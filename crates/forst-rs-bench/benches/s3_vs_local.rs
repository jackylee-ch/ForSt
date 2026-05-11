// Copyright 2026 The ForSt-RS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! B-Prod-followup-S3-perf — Direct comparison of `DbImpl::open_remote`
//! against a MinIO-backed S3 bucket vs `DbImpl::open_with_fs` against a
//! local tempdir, on the SAME workloads and the SAME hardware. Answers
//! VP question 6 quantitatively.
//!
//! ## Design notes
//!
//! 1. Criterion's `iter_with_setup` would pay the ~5 s MinIO container
//!    boot cost on every sample, swamping the actual measurement. So
//!    the MinIO container is started ONCE per bench session, lazily via
//!    [`once_cell::sync::Lazy`]-style static (a plain `std::sync::OnceLock`)
//!    and reused across every workload. The container lives until the
//!    bench process exits.
//!
//! 2. For the warm-cache point-lookup workload the DB is fully populated,
//!    flushed, and warmed ONCE outside `b.iter`; the timed region is a
//!    1000-key read loop, identical between the local-FS and S3
//!    variants.
//!
//! 3. For the sequential-write workload we use `iter_batched` with
//!    `BatchSize::PerIteration` so each iteration gets a fresh DB at a
//!    fresh prefix (avoiding cumulative file-system state) but the
//!    MinIO container is reused.
//!
//! ## Workloads
//!
//! - `point_lookup_warm_cache/1000` — 1000 keys all cache-hot. The S3
//!   variant should be ~comparable to local-FS because the LocalCache
//!   serves every read from the local FS layer.
//! - `sequential_write_then_flush/1000` — write 1000 keys + force a
//!   flush. Measures the upload + manifest latency (every flushed SST
//!   is pushed to S3 before the call returns).
//!
//! ## Why no "cold cache" workload
//!
//! A cold-cache read workload would require opening a fresh DB at an
//! existing S3 prefix and recovering the manifest + SSTs from remote.
//! ForSt-RS today does not implement that recovery path — `open_with_fs`
//! always starts a fresh manifest; persistence boundaries live at the
//! checkpoint/restore API, not at the open path. We therefore intentionally
//! restrict this bench to the two paths that ARE supported in a
//! single-process lifetime: warm-cache reads (cache-served) and
//! write-then-flush (the upload cost).
//!
//! ## Running
//!
//! ```text
//! cargo bench -p forst-rs-bench --bench s3_vs_local --features s3-it
//! ```
//!
//! Skipped unless the `s3-it` feature is enabled AND Docker is running.
//! The CI workflow `s3-perf-bench.yml` pulls the MinIO image, builds
//! the bench, and uploads the criterion output as an artifact.

#![allow(clippy::expect_used)]

#[cfg(not(feature = "s3-it"))]
fn main() {
    eprintln!("s3_vs_local bench skipped: enable with --features s3-it (requires Docker daemon)");
}

#[cfg(feature = "s3-it")]
mod bench {
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};

    use criterion::{criterion_group, BatchSize, BenchmarkId, Criterion, Throughput};
    use forst_rs_common::EngineOptions;
    use forst_rs_engine::DbImpl;
    use forst_rs_io::{FileSystem, LocalFileSystem};
    use tempfile::TempDir;
    use testcontainers::core::{ContainerPort, WaitFor};
    use testcontainers::{runners::SyncRunner, GenericImage, ImageExt};

    // Match the version pin used in `crates/forst-rs-engine/tests/s3_remote_storage_it.rs`
    // so a local repro and a CI run drink from the same well.
    const MINIO_TAG: &str = "RELEASE.2025-02-28T09-55-16Z";
    const BUCKET: &str = "forst-rs-bench";

    /// Shared MinIO state: the container handle (kept alive for the
    /// process), the OpenDAL config map we hand to `open_remote`, and
    /// a small counter used to mint fresh DB prefixes per iteration.
    struct MinioCtx {
        /// Keep the handle alive — dropping it stops the container.
        /// `testcontainers::ContainerAsync<_>` is `Send + Sync`; the
        /// blocking variant is also `Send + Sync` so it sits in a
        /// static fine.
        #[allow(dead_code)]
        container: testcontainers::Container<GenericImage>,
        opendal_cfg: HashMap<String, String>,
        next_db_id: std::sync::atomic::AtomicU64,
    }

    fn minio() -> &'static MinioCtx {
        static MINIO: OnceLock<MinioCtx> = OnceLock::new();
        MINIO.get_or_init(start_minio)
    }

    fn start_minio() -> MinioCtx {
        // Pre-create the bucket as `/data/<bucket>` before booting the
        // server, exactly like the engine S3 integration test.
        let bootstrap = format!("mkdir -p /data/{BUCKET} && exec minio server /data");
        let container = GenericImage::new("minio/minio", MINIO_TAG)
            .with_exposed_port(ContainerPort::Tcp(9000))
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_entrypoint("sh")
            .with_env_var("MINIO_ROOT_USER", "minioadmin")
            .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
            .with_cmd(["-c", &bootstrap])
            .start()
            .expect("MinIO container failed to start (is Docker running?)");

        let host = container.get_host().expect("get_host").to_string();
        let api_port = container
            .get_host_port_ipv4(9000)
            .expect("get_host_port_ipv4(9000)");
        let endpoint = format!("http://{host}:{api_port}");

        let mut opendal_cfg = HashMap::new();
        opendal_cfg.insert("region".to_string(), "us-east-1".to_string());
        opendal_cfg.insert("endpoint".to_string(), endpoint);
        opendal_cfg.insert("access_key_id".to_string(), "minioadmin".to_string());
        opendal_cfg.insert("secret_access_key".to_string(), "minioadmin".to_string());

        MinioCtx {
            container,
            opendal_cfg,
            next_db_id: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Mints a fresh DB prefix (different `/db-<n>` for each iteration)
    /// so that successive iterations of the same bench don't accumulate
    /// SSTs / manifests under the same prefix.
    fn fresh_db_path(ctx: &MinioCtx) -> String {
        let n = ctx
            .next_db_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("/db-bench-{n}")
    }

    fn engine_options(db_path: &str) -> EngineOptions {
        EngineOptions {
            db_path: db_path.to_string(),
            // Small write buffer so a 1000-key write actually triggers
            // at least one SST flush rather than sitting entirely in
            // the active memtable (which would make S3 a no-op).
            write_buffer_size: 64 * 1024,
            ..EngineOptions::default()
        }
    }

    // ---------- DB factories ----------

    fn open_local(_ctx: &MinioCtx) -> (Arc<DbImpl>, TempDir, Option<TempDir>) {
        let dir = TempDir::new().expect("local tempdir");
        let opts = EngineOptions {
            db_path: dir.path().to_str().expect("tempdir utf-8").to_string(),
            write_buffer_size: 64 * 1024,
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        let db = DbImpl::open_with_fs(opts, fs).expect("open_with_fs(local)");
        // Return the TempDir twice-shaped (the second slot is unused
        // for local) so the caller has a uniform tuple shape with the
        // S3 variant. Drop happens when the tuple goes out of scope.
        (db, dir, None)
    }

    fn open_s3(ctx: &MinioCtx) -> (Arc<DbImpl>, TempDir, Option<TempDir>) {
        let db_path = fresh_db_path(ctx);
        let cache_dir = TempDir::new().expect("cache tempdir");
        let uri = format!("s3://{BUCKET}/");
        let db = DbImpl::open_remote(
            engine_options(&db_path),
            &uri,
            ctx.opendal_cfg.clone(),
            cache_dir.path(),
            64 * 1024 * 1024, // 64 MiB local cache
        )
        .expect("open_remote against MinIO");
        // Return the cache_dir as the "primary" tempdir slot so it gets
        // dropped with the DB.
        (db, cache_dir, None)
    }

    // ---------- Workloads ----------

    /// Reads `keys.len()` keys against `db`. Returns the number of bytes
    /// read so that the optimiser can't elide the loop.
    fn read_all(
        db: &Arc<DbImpl>,
        cf: &forst_rs_engine::ColumnFamilyHandle,
        keys: &[Vec<u8>],
    ) -> usize {
        let mut total = 0usize;
        for k in keys {
            let v = db.get(cf, k.as_slice()).expect("get");
            total += v.map(|b| b.len()).unwrap_or(0);
        }
        total
    }

    fn write_all(db: &Arc<DbImpl>, cf: &forst_rs_engine::ColumnFamilyHandle, n: u32) {
        for i in 0..n {
            db.put(
                cf,
                format!("k{i:05}").as_bytes(),
                format!("v{i}").as_bytes(),
            )
            .expect("put");
        }
    }

    // ---------- bench: warm-cache point lookup ----------

    fn bench_point_lookup_warm_cache(c: &mut Criterion) {
        let ctx = minio();
        let mut group = c.benchmark_group("s3_vs_local__point_lookup_warm_cache");
        group.throughput(Throughput::Elements(1000));
        // Default 100 samples is fine for warm cache; container is shared.

        // ----- Local-FS -----
        let (local_db, _local_dir, _) = open_local(ctx);
        let local_cf = local_db.default_cf();
        write_all(&local_db, &local_cf, 1000);
        local_db.flush_all().expect("flush local");
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("k{i:05}").into_bytes())
            .collect();
        // Warm: read every key once before measuring.
        let _ = read_all(&local_db, &local_cf, &keys);

        group.bench_with_input(BenchmarkId::new("local-fs", 1000), &1000u32, |b, _| {
            b.iter(|| read_all(&local_db, &local_cf, &keys));
        });

        // ----- S3 (MinIO) -----
        let (s3_db, _cache_dir, _) = open_s3(ctx);
        let s3_cf = s3_db.default_cf();
        write_all(&s3_db, &s3_cf, 1000);
        s3_db.flush_all().expect("flush s3");
        // Warm the local cache by reading every key once before
        // measuring; after this point every read should be a cache hit
        // and S3 should not be touched.
        let _ = read_all(&s3_db, &s3_cf, &keys);

        group.bench_with_input(BenchmarkId::new("s3-minio-warm", 1000), &1000u32, |b, _| {
            b.iter(|| read_all(&s3_db, &s3_cf, &keys));
        });

        group.finish();
    }

    // ---------- bench: sequential write + flush ----------

    fn bench_sequential_write_then_flush(c: &mut Criterion) {
        let ctx = minio();
        let mut group = c.benchmark_group("s3_vs_local__sequential_write_then_flush");
        group.throughput(Throughput::Elements(1000));
        group.sample_size(10); // write workload is bigger; fewer samples is fine

        group.bench_function("local-fs/1k-writes-then-flush", |b| {
            b.iter_batched(
                || open_local(ctx),
                |(db, _dir, _)| {
                    let cf = db.default_cf();
                    write_all(&db, &cf, 1000);
                    db.flush_all().expect("flush local");
                },
                BatchSize::PerIteration,
            );
        });

        group.bench_function("s3-minio/1k-writes-then-flush", |b| {
            b.iter_batched(
                || open_s3(ctx),
                |(db, _cache, _)| {
                    let cf = db.default_cf();
                    write_all(&db, &cf, 1000);
                    db.flush_all().expect("flush s3");
                },
                BatchSize::PerIteration,
            );
        });

        group.finish();
    }

    criterion_group!(
        s3_vs_local_benches,
        bench_point_lookup_warm_cache,
        bench_sequential_write_then_flush
    );

    /// Public re-export so the file's top-level `criterion_main!` can
    /// reach the group through a path the macro accepts. `criterion_group!`
    /// itself does not accept a `pub` keyword.
    pub use s3_vs_local_benches as benches;
}

#[cfg(feature = "s3-it")]
criterion::criterion_main!(bench::benches);

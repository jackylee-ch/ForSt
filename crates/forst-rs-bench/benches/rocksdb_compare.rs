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

//! Side-by-side ForSt-RS vs RocksDB v8.x baseline benchmarks.
//!
//! Gated behind the `rocksdb-baseline` Cargo feature because the `rocksdb`
//! crate vendors the C++ sources and takes ~15 minutes to build cold.
//!
//! Run via:
//! ```bash
//! cargo bench -p forst-rs-bench --features rocksdb-baseline --bench rocksdb_compare
//! ```
//!
//! Each benchmark exposes a paired `forst_rs/<scenario>` and `rocksdb/<scenario>`
//! group so criterion can compute the perf delta directly.
//!
//! Reference: v3.2 §2.4 (3-5× perf KPI) and `docs/superpowers/planning/v3.2/reports/N2_nexmark_plan.md`
//! Lane B (in-process micro-baseline) for the methodology this file implements.

#![cfg(feature = "rocksdb-baseline")]

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::{create_cf, open_in_memory, seed_sequential};
use forst_rs_engine::WriteBatch as FrsWriteBatch;
use rocksdb::{
    ColumnFamilyDescriptor as RocksCfDesc, Options as RocksOpts, WriteBatch as RocksWriteBatch,
    DB as RocksDb,
};
use tempfile::TempDir;

/// BM-1.1 — point lookup latency. Compares 100k pre-loaded entries.
fn bench_point_lookup(c: &mut Criterion) {
    const N: u32 = 100_000;
    let mut group = c.benchmark_group("point_lookup");
    group.throughput(Throughput::Elements(1));

    // forst-rs setup
    let frs_db = open_in_memory(64 * 1024 * 1024);
    let frs_cf = create_cf(&frs_db, "default-cf");
    seed_sequential(&frs_db, &frs_cf, N, "k{:08}", "v{:08}");

    // rocksdb setup
    let tmp = TempDir::new().expect("tempdir");
    let mut rocks_opts = RocksOpts::default();
    rocks_opts.create_if_missing(true);
    rocks_opts.create_missing_column_families(true);
    let rocks_db = Arc::new(
        RocksDb::open_cf_descriptors(
            &rocks_opts,
            tmp.path(),
            vec![RocksCfDesc::new("default", RocksOpts::default())],
        )
        .expect("rocksdb open"),
    );
    let rocks_cf = rocks_db.cf_handle("default").expect("rocksdb cf");
    for i in 0..N {
        let k = format!("k{:08}", i);
        let v = format!("v{:08}", i);
        rocks_db
            .put_cf(&rocks_cf, k.as_bytes(), v.as_bytes())
            .expect("rocksdb put");
    }

    // Probe a fixed mid-range key for stable measurement.
    let probe_key = b"k00050000";

    group.bench_function(BenchmarkId::new("forst_rs", N), |b| {
        b.iter(|| {
            let v = frs_db
                .get(&frs_cf, probe_key)
                .expect("frs get")
                .expect("frs hit");
            std::hint::black_box(v);
        })
    });

    group.bench_function(BenchmarkId::new("rocksdb", N), |b| {
        b.iter(|| {
            let v = rocks_db
                .get_cf(&rocks_cf, probe_key)
                .expect("rocksdb get")
                .expect("rocksdb hit");
            std::hint::black_box(v);
        })
    });

    group.finish();
}

/// BM-1.3 — single-key sequential put throughput. Measures cost per put.
fn bench_sequential_put(c: &mut Criterion) {
    const N: u32 = 10_000;
    let mut group = c.benchmark_group("sequential_put");
    group.throughput(Throughput::Elements(N as u64));

    group.bench_function(BenchmarkId::new("forst_rs", N), |b| {
        b.iter_batched(
            || {
                let db = open_in_memory(64 * 1024 * 1024);
                let cf = create_cf(&db, "bench-cf");
                (db, cf)
            },
            |(db, cf)| {
                for i in 0..N {
                    let k = format!("k{:08}", i);
                    let v = format!("v{:08}", i);
                    db.put(&cf, k.as_bytes(), v.as_bytes()).expect("put");
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("rocksdb", N), |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().expect("tempdir");
                let mut opts = RocksOpts::default();
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                let db = RocksDb::open_cf_descriptors(
                    &opts,
                    tmp.path(),
                    vec![RocksCfDesc::new("bench", RocksOpts::default())],
                )
                .expect("rocksdb open");
                (db, tmp)
            },
            |(db, _tmp)| {
                let cf = db.cf_handle("bench").expect("rocksdb cf");
                for i in 0..N {
                    let k = format!("k{:08}", i);
                    let v = format!("v{:08}", i);
                    db.put_cf(&cf, k.as_bytes(), v.as_bytes()).expect("put");
                }
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// BM-1.4 — batched put throughput.
///
/// Mirrors the Java JMH `batchedPut` workload: BATCH_SIZE rows per WriteBatch,
/// per timing iteration. This is the realistic write hot path for production
/// state backends (Flink emits WriteBatches at checkpoint barriers); the
/// per-row LSM cost is amortized over the batch and the RocksDB-vs-ForSt-RS
/// engine ratio compresses to the steady-state SST-write ratio.
fn bench_batched_put(c: &mut Criterion) {
    const N: u32 = 1_000;
    let mut group = c.benchmark_group("batched_put");
    group.throughput(Throughput::Elements(N as u64));

    // Pre-generate the batch payload once; the per-iter cost is just batch
    // construction + apply, no key/value allocation in the hot loop.
    let payload: Vec<(Vec<u8>, Vec<u8>)> = (0..N)
        .map(|i| {
            let k = format!("bk{:010}", i).into_bytes();
            let v = format!("bv{:010}", i).into_bytes();
            (k, v)
        })
        .collect();

    group.bench_function(BenchmarkId::new("forst_rs", N), |b| {
        b.iter_batched(
            || {
                let db = open_in_memory(64 * 1024 * 1024);
                let cf = create_cf(&db, "bench-cf");
                (db, cf)
            },
            |(db, cf)| {
                let mut wb = FrsWriteBatch::with_capacity(N as usize);
                for (k, v) in &payload {
                    wb.put(&cf, k, v);
                }
                db.batch_write(wb).expect("frs batch_write");
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("rocksdb", N), |b| {
        b.iter_batched(
            || {
                let tmp = TempDir::new().expect("tempdir");
                let mut opts = RocksOpts::default();
                opts.create_if_missing(true);
                opts.create_missing_column_families(true);
                let db = RocksDb::open_cf_descriptors(
                    &opts,
                    tmp.path(),
                    vec![RocksCfDesc::new("bench", RocksOpts::default())],
                )
                .expect("rocksdb open");
                (db, tmp)
            },
            |(db, _tmp)| {
                let cf = db.cf_handle("bench").expect("rocksdb cf");
                let mut wb = RocksWriteBatch::default();
                for (k, v) in &payload {
                    wb.put_cf(&cf, k, v);
                }
                db.write(wb).expect("rocksdb write");
            },
            criterion::BatchSize::LargeInput,
        );
    });

    group.finish();
}

/// BM-1.5 — full-range scan over a MULTI-LEVEL (overlapping-L0) CF.
///
/// This is the engine path behind the NexMark range-scan-heavy queries
/// (q9/q19/q20 and the q11 timer drain): `DbImpl::scan_iter_owned_arc_*` /
/// RocksDB's forward iterator, where the lazy k-way merge must dedup the same
/// key range across many overlapping SSTs (read-amplification). We seed the
/// SAME key range in `WAVES` flushed batches so each wave is one overlapping L0
/// SST — the merge must compare `WAVES` cursors per emitted key. Measures the
/// per-entry merge+decode+value-resolution cost that gates the diffuse
/// per-record floor (see 2026-06-07-value-carrying-range-scan-design.md).
fn bench_range_scan_multilevel(c: &mut Criterion) {
    const N: u32 = 20_000;
    const WAVES: u32 = 8;
    let mut group = c.benchmark_group("range_scan_multilevel");
    group.throughput(Throughput::Elements((N as u64) * 0 + N as u64));

    // forst-rs: WAVES overlapping L0 SSTs over the same key range.
    let frs_db = open_in_memory(8 * 1024 * 1024);
    let frs_cf = create_cf(&frs_db, "scan-cf");
    for w in 0..WAVES {
        for i in 0..N {
            let k = format!("k{:08}", i);
            let v = format!("v{:08}-w{}", i, w);
            frs_db
                .put(&frs_cf, k.as_bytes(), v.as_bytes())
                .expect("put");
        }
        let _ = frs_db.switch_and_flush(&frs_cf);
    }

    // rocksdb: same shape — WAVES flushed memtables over the same range.
    let tmp = TempDir::new().expect("tempdir");
    let mut rocks_opts = RocksOpts::default();
    rocks_opts.create_if_missing(true);
    rocks_opts.create_missing_column_families(true);
    // Disable auto-compaction so the WAVES L0 files persist (match forst-rs).
    rocks_opts.set_disable_auto_compactions(true);
    let rocks_db = Arc::new(
        RocksDb::open_cf_descriptors(
            &rocks_opts,
            tmp.path(),
            vec![RocksCfDesc::new("default", RocksOpts::default())],
        )
        .expect("rocksdb open"),
    );
    let rocks_cf = rocks_db.cf_handle("default").expect("rocksdb cf");
    for w in 0..WAVES {
        for i in 0..N {
            let k = format!("k{:08}", i);
            let v = format!("v{:08}-w{}", i, w);
            rocks_db
                .put_cf(&rocks_cf, k.as_bytes(), v.as_bytes())
                .expect("rocksdb put");
        }
        rocks_db.flush_cf(&rocks_cf).expect("rocksdb flush");
    }

    group.bench_function(BenchmarkId::new("forst_rs", N), |b| {
        b.iter(|| {
            let slot = Arc::new(std::sync::Mutex::new(None));
            let it = frs_db
                .scan_iter_owned_arc_with_error_slot(&frs_cf, b"", None, slot)
                .expect("frs scan open");
            let mut count = 0u64;
            for r in it {
                let (_k, _v) = r.expect("frs row");
                count += 1;
            }
            std::hint::black_box(count);
        })
    });

    group.bench_function(BenchmarkId::new("rocksdb", N), |b| {
        b.iter(|| {
            let it = rocks_db.iterator_cf(&rocks_cf, rocksdb::IteratorMode::Start);
            let mut count = 0u64;
            for item in it {
                let (_k, _v) = item.expect("rocksdb row");
                count += 1;
            }
            std::hint::black_box(count);
        })
    });

    group.finish();
}

/// BM-1.6 — prefix scan over a CHURNED hot prefix (q9/q19 TopN pattern).
///
/// The 3-backend NexMark sweep showed forst-rs LOSES q9/q19 to BOTH RocksDB and
/// ForSt-Java, yet BM-1.5 (static overlapping layout) showed forst-rs FASTER.
/// The difference q9/q19 add is CHURN: a TopN keeps a small (~N) sorted buffer
/// per key and rewrites+evicts it on every input row, so the prefix accumulates
/// many versions + tombstones across SSTs until compaction. This bench replays
/// that: N live keys, ROUNDS cycles of delete-all + re-put (each cycle a new
/// version + a tombstone per key), flushed each cycle so the prefix spans ROUNDS
/// SSTs of churn. Then it prefix-scans the N live keys — the read must merge the
/// live value past ROUNDS-1 stale versions/tombstones per key. If forst-rs is
/// slower HERE (unlike BM-1.5), the q9/q19 gap is churn-merge read-amp.
fn bench_hot_prefix_churn(c: &mut Criterion) {
    const N: u32 = 1_000;
    const ROUNDS: u32 = 64;
    let mut group = c.benchmark_group("hot_prefix_churn");
    group.throughput(Throughput::Elements(N as u64));

    let frs_db = open_in_memory(8 * 1024 * 1024);
    let frs_cf = create_cf(&frs_db, "churn-cf");
    for r in 0..ROUNDS {
        for i in 0..N {
            let k = format!("p{:08}", i);
            if r > 0 {
                frs_db.delete(&frs_cf, k.as_bytes()).expect("del");
            }
            let v = format!("v{:08}-r{}", i, r);
            frs_db
                .put(&frs_cf, k.as_bytes(), v.as_bytes())
                .expect("put");
        }
        let _ = frs_db.switch_and_flush(&frs_cf);
    }

    let tmp = TempDir::new().expect("tempdir");
    let mut rocks_opts = RocksOpts::default();
    rocks_opts.create_if_missing(true);
    rocks_opts.create_missing_column_families(true);
    rocks_opts.set_disable_auto_compactions(true);
    let rocks_db = Arc::new(
        RocksDb::open_cf_descriptors(
            &rocks_opts,
            tmp.path(),
            vec![RocksCfDesc::new("default", RocksOpts::default())],
        )
        .expect("rocksdb open"),
    );
    let rocks_cf = rocks_db.cf_handle("default").expect("rocksdb cf");
    for r in 0..ROUNDS {
        for i in 0..N {
            let k = format!("p{:08}", i);
            if r > 0 {
                rocks_db.delete_cf(&rocks_cf, k.as_bytes()).expect("del");
            }
            let v = format!("v{:08}-r{}", i, r);
            rocks_db
                .put_cf(&rocks_cf, k.as_bytes(), v.as_bytes())
                .expect("put");
        }
        rocks_db.flush_cf(&rocks_cf).expect("flush");
    }

    group.bench_function(BenchmarkId::new("forst_rs", N), |b| {
        b.iter(|| {
            let slot = Arc::new(std::sync::Mutex::new(None));
            let it = frs_db
                .scan_iter_owned_arc_with_error_slot(&frs_cf, b"", None, slot)
                .expect("frs scan");
            let mut count = 0u64;
            for r in it {
                let _ = r.expect("row");
                count += 1;
            }
            std::hint::black_box(count);
        })
    });

    group.bench_function(BenchmarkId::new("rocksdb", N), |b| {
        b.iter(|| {
            let it = rocks_db.iterator_cf(&rocks_cf, rocksdb::IteratorMode::Start);
            let mut count = 0u64;
            for item in it {
                let _ = item.expect("row");
                count += 1;
            }
            std::hint::black_box(count);
        })
    });

    group.finish();
}

criterion_group!(
    rocksdb_compare,
    bench_point_lookup,
    bench_sequential_put,
    bench_batched_put,
    bench_range_scan_multilevel,
    bench_hot_prefix_churn
);
criterion_main!(rocksdb_compare);

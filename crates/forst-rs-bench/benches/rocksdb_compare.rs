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
use rocksdb::{ColumnFamilyDescriptor as RocksCfDesc, Options as RocksOpts, DB as RocksDb};
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

criterion_group!(rocksdb_compare, bench_point_lookup, bench_sequential_put);
criterion_main!(rocksdb_compare);

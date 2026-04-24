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

//! BM-1.1 Point lookup latency & throughput.
//!
//! Measures `DbImpl::get` against a pre-populated in-memory engine. The
//! engine is sized so every entry stays in the active memtable (best-case
//! cache hit).

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::{open_in_memory, seed_sequential};

fn bench_point_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup_memtable");
    for &n in &[1_000u32, 10_000, 100_000] {
        // Use an 8 MB buffer so everything fits in the active memtable.
        let db = open_in_memory(8 * 1024 * 1024);
        let cf = db.default_cf();
        seed_sequential(&db, &cf, n, "k{:08}", "v{:08}");

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &count| {
            b.iter(|| {
                for i in 0..count {
                    let k = format!("k{:08}", i);
                    let _ = db.get(&cf, k.as_bytes()).expect("get");
                }
            });
        });
    }
    group.finish();
}

fn bench_point_lookup_with_compaction(c: &mut Criterion) {
    // Entries are split across several flushed L0 files and one L1 file so
    // the read path must walk the SST layer.
    let mut group = c.benchmark_group("point_lookup_after_flush");
    for &n in &[1_000u32, 10_000] {
        let db = open_in_memory(8 * 1024 * 1024);
        let cf = db.default_cf();
        seed_sequential(&db, &cf, n, "k{:08}", "v{:08}");
        db.switch_and_flush(&cf).expect("flush").unwrap();
        db.compact_l0(&cf).expect("compact").unwrap();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &count| {
            b.iter(|| {
                for i in 0..count {
                    let k = format!("k{:08}", i);
                    let _ = db.get(&cf, k.as_bytes()).expect("get");
                }
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_point_lookup,
    bench_point_lookup_with_compaction
);
criterion_main!(benches);

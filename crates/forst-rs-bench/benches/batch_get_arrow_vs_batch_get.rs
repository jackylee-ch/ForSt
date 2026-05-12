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

//! Micro-bench: `batch_get_arrow` (zero-copy Arrow return) vs `batch_get`
//! (Vec<Option<Vec<u8>>> return) at the engine boundary.
//!
//! Both arms look up the same 64-key batch from a pre-populated memtable.
//! The Arrow arm builds the BinaryArray of keys ONCE at setup and re-uses
//! it on every iteration (matching the FFM call site where the Java side
//! passes the same Arrow buffer). The legacy arm builds a Vec<&[u8]> on
//! every iteration.
//!
//! Goal: the Arrow path eliminates per-value Vec<u8> allocation on the
//! return side, yielding measurable throughput improvement for batch sizes
//! typical of Flink state access (64 events/batch).

use arrow::array::BinaryArray;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::open_in_memory;

const KEY_POOL: u32 = 10_000;

fn bench_batch_get_arrow_vs_batch_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_get_arrow_vs_batch_get");

    let pool: Vec<Vec<u8>> = (0..KEY_POOL)
        .map(|i| format!("k{:08}", i).into_bytes())
        .collect();

    for &batch_size in &[16usize, 64, 256, 1024] {
        let db = open_in_memory(64 * 1024 * 1024);
        let cf = db.default_cf();
        // Pre-populate so every lookup hits.
        for k in &pool {
            db.put(&cf, k.as_slice(), b"value-payload-16b")
                .expect("put");
        }

        // --- Legacy batch_get (Vec<Option<Vec<u8>>>) ---
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_get", batch_size),
            &batch_size,
            |b, &size| {
                let mut start = 0usize;
                b.iter(|| {
                    let keys: Vec<&[u8]> = (0..size)
                        .map(|i| pool[(start + i) % pool.len()].as_slice())
                        .collect();
                    let _ = db.batch_get(&cf, &keys).expect("batch_get");
                    start = start.wrapping_add(size);
                });
            },
        );

        // --- Arrow batch_get_arrow (RecordBatch return) ---
        group.bench_with_input(
            BenchmarkId::new("batch_get_arrow", batch_size),
            &batch_size,
            |b, &size| {
                let mut start = 0usize;
                b.iter(|| {
                    let keys = BinaryArray::from(
                        (0..size)
                            .map(|i| pool[(start + i) % pool.len()].as_slice())
                            .collect::<Vec<&[u8]>>(),
                    );
                    let _ = db.batch_get_arrow(&cf, &keys).expect("batch_get_arrow");
                    start = start.wrapping_add(size);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_batch_get_arrow_vs_batch_get);
criterion_main!(benches);

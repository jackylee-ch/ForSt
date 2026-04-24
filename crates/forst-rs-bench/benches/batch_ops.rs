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

//! BM-1.2 Batch put / batch get scaling.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::WriteBatch;

fn bench_batch_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_put");
    for &batch_size in &[1usize, 10, 100, 1_000] {
        let db = open_in_memory(64 * 1024 * 1024);
        let cf = db.default_cf();

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let mut counter = 0u32;
                b.iter(|| {
                    let mut wb = WriteBatch::with_capacity(size);
                    for _ in 0..size {
                        let k = format!("k{:08}", counter);
                        wb.put(&cf, k.as_bytes(), b"v");
                        counter += 1;
                    }
                    db.batch_write(wb).expect("batch_write");
                });
            },
        );
    }
    group.finish();
}

fn bench_batch_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_get");
    for &batch_size in &[1usize, 10, 100, 1_000] {
        let db = open_in_memory(64 * 1024 * 1024);
        let cf = db.default_cf();
        // Pre-populate enough entries.
        for i in 0..10_000u32 {
            let k = format!("k{:08}", i);
            db.put(&cf, k.as_bytes(), b"v").expect("put");
        }

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let mut start = 0u32;
                b.iter(|| {
                    let keys_owned: Vec<String> = (0..size)
                        .map(|i| format!("k{:08}", (start + i as u32) % 10_000))
                        .collect();
                    let keys: Vec<&[u8]> = keys_owned.iter().map(|s| s.as_bytes()).collect();
                    let _ = db.batch_get(&cf, &keys).expect("batch_get");
                    start = start.wrapping_add(size as u32);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_batch_put, bench_batch_get);
criterion_main!(benches);

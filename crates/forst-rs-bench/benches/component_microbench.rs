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

//! P0.4 Component microbench: Rust-side batch API cost isolation (no FFI overhead).
//!
//! Measures engine::DbImpl batch_put / batch_get round-trip latency in isolation,
//! serving as the Rust-side counterpart to the Java JMH bench harness for the P0
//! component-boundary validation gate (umbrella spec Appendix).
//!
//! This bench uses an in-memory filesystem to remove storage I/O noise and focuses
//! purely on the engine's bookkeeping overhead for batch operations.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::WriteBatch;

/// Generates key/value pairs with consistent formatting for bench repeatability.
fn make_keys_vals(n: usize, val_bytes: usize) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let keys: Vec<Vec<u8>> = (0..n)
        .map(|i| format!("bench-key-{:08}", i).into_bytes())
        .collect();
    let vals: Vec<Vec<u8>> = (0..n).map(|_| vec![0xABu8; val_bytes]).collect();
    (keys, vals)
}

/// P0.4a: Measure WriteBatch::put() construction + batch_write() round-trip,
/// isolating the engine-side batch put cost.
///
/// Methodology: Pre-generate N (key, value) pairs with fixed size (256B),
/// then measure the time to construct a WriteBatch and commit it via batch_write().
fn bench_engine_batch_put_256b(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_batch_put_256b");

    for &batch_size in &[1usize, 4, 16, 64] {
        let db = open_in_memory(64 * 1024 * 1024);
        let cf = db.default_cf();
        let (keys, vals) = make_keys_vals(batch_size, 256);

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("size_{}", batch_size)),
            &batch_size,
            |b, &_size| {
                let mut counter = 0usize;
                b.iter(|| {
                    let mut wb = WriteBatch::with_capacity(batch_size);
                    for (i, k) in keys.iter().enumerate() {
                        wb.put(&cf, k.as_slice(), vals[i].as_slice());
                    }
                    let _ = db.batch_write(black_box(wb)).expect("batch_write");
                    counter = counter.wrapping_add(1);
                    let _ = black_box(counter);
                });
            },
        );
    }
    group.finish();
}

/// P0.4b: Measure batch_get() round-trip in isolation.
///
/// Methodology: Pre-populate the engine with N (key, value) pairs of fixed size (256B),
/// then measure the time to issue a batch_get() call for those keys.
fn bench_engine_batch_get_256b(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_batch_get_256b");

    for &batch_size in &[1usize, 4, 16, 64] {
        let db = open_in_memory(64 * 1024 * 1024);
        let cf = db.default_cf();
        let (keys, vals) = make_keys_vals(batch_size, 256);

        // Pre-populate with the keys we will query.
        for (k, v) in keys.iter().zip(vals.iter()) {
            db.put(&cf, k.as_slice(), v.as_slice()).expect("put");
        }

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("size_{}", batch_size)),
            &batch_size,
            |b, &_size| {
                let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
                b.iter(|| {
                    let _ = db.batch_get(&cf, black_box(&key_refs)).expect("batch_get");
                });
            },
        );
    }
    group.finish();
}

/// P0.4c: Measure put() single-op cost as a baseline for batch comparison.
///
/// Methodology: Issue sequential put() calls (not in a batch) to measure the
/// per-operation overhead of the non-batched path. This serves as a control
/// to validate that batch_write() provides a speedup.
fn bench_engine_single_put_256b(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_single_put_256b");

    let db = open_in_memory(64 * 1024 * 1024);
    let cf = db.default_cf();
    let (keys, vals) = make_keys_vals(64, 256);

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_put_256b", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            let k = &keys[counter % keys.len()];
            let v = &vals[counter % vals.len()];
            let _ = db.put(&cf, black_box(k.as_slice()), black_box(v.as_slice()))
                .expect("put");
            counter = counter.wrapping_add(1);
        });
    });

    group.finish();
}

/// P0.4d: Measure get() single-op cost as a baseline for batch_get() comparison.
///
/// Methodology: Pre-populate the engine, then issue sequential get() calls
/// to measure the per-operation overhead of the non-batched path.
fn bench_engine_single_get_256b(c: &mut Criterion) {
    let mut group = c.benchmark_group("engine_single_get_256b");

    let db = open_in_memory(64 * 1024 * 1024);
    let cf = db.default_cf();
    let (keys, vals) = make_keys_vals(64, 256);

    // Pre-populate.
    for (k, v) in keys.iter().zip(vals.iter()) {
        db.put(&cf, k.as_slice(), v.as_slice()).expect("put");
    }

    group.throughput(Throughput::Elements(1));
    group.bench_function("single_get_256b", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            let k = &keys[counter % keys.len()];
            let _ = db.get(&cf, black_box(k.as_slice())).expect("get");
            counter = counter.wrapping_add(1);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_engine_batch_put_256b,
    bench_engine_batch_get_256b,
    bench_engine_single_put_256b,
    bench_engine_single_get_256b,
);
criterion_main!(benches);

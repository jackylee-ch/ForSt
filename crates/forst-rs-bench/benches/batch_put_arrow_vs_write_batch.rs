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

//! C1 micro-bench: zero-copy `batch_put_arrow` vs the legacy `WriteBatch`
//! path at the engine boundary.
//!
//! Both arms simulate the FFM bench's `batchedPutArrow` workload: a
//! pre-built 1000-row batch (key=12B, value=12B, op_type=Put) is dispatched
//! to a fresh in-memory engine 100 times per timing window. The Arrow arm
//! constructs the `RecordBatch` ONCE at setup and re-passes it on every
//! iteration (matching the FFM call site, which re-uses the same staged
//! arrays). The WriteBatch arm builds a fresh `WriteBatch` from
//! pre-allocated `Vec<u8>` payloads on every iteration — same data, same
//! semantics, just the legacy decode → re-borrow round-trip the FFI used to
//! pay before C1.
//!
//! Goal: the Arrow path should be 1.5–2× faster at the engine level by
//! avoiding the per-row `Vec<u8>` allocation in `WriteBatch::put` and the
//! subsequent `Vec<&[u8]>` re-borrow in `batch_write`.

use std::sync::Arc;

use arrow::array::{BinaryBuilder, RecordBatch, StructArray, UInt8Builder};
use arrow::datatypes::{DataType, Field};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::WriteBatch;

const BATCH_SIZE: usize = 1000;
const BATCHES_PER_ITER: usize = 100;

fn build_payloads() -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let keys: Vec<Vec<u8>> = (0..BATCH_SIZE as u32)
        .map(|i| format!("bk{:010}", i).into_bytes())
        .collect();
    let values: Vec<Vec<u8>> = (0..BATCH_SIZE as u32)
        .map(|i| format!("bv{:010}", i).into_bytes())
        .collect();
    (keys, values)
}

fn build_arrow_batch(keys: &[Vec<u8>], values: &[Vec<u8>]) -> RecordBatch {
    let mut kb = BinaryBuilder::new();
    let mut vb = BinaryBuilder::new();
    let mut ob = UInt8Builder::new();
    for i in 0..keys.len() {
        kb.append_value(&keys[i]);
        vb.append_value(&values[i]);
        ob.append_value(0); // Put
    }
    let s = StructArray::from(vec![
        (
            Arc::new(Field::new("key", DataType::Binary, false)),
            Arc::new(kb.finish()) as arrow::array::ArrayRef,
        ),
        (
            Arc::new(Field::new("value", DataType::Binary, true)),
            Arc::new(vb.finish()) as arrow::array::ArrayRef,
        ),
        (
            Arc::new(Field::new("op_type", DataType::UInt8, false)),
            Arc::new(ob.finish()) as arrow::array::ArrayRef,
        ),
    ]);
    s.into()
}

fn bench_batch_put_arrow(c: &mut Criterion) {
    let (keys, values) = build_payloads();
    let batch = build_arrow_batch(&keys, &values);

    let mut group = c.benchmark_group("c1_batch_put_engine");
    // Throughput is rows/s — BATCH_SIZE * BATCHES_PER_ITER per iter.
    group.throughput(Throughput::Elements((BATCH_SIZE * BATCHES_PER_ITER) as u64));

    group.bench_with_input(
        BenchmarkId::new("arrow_zero_copy", BATCH_SIZE),
        &batch,
        |b, batch| {
            // Each iter starts on a fresh engine so we measure the actual
            // memtable-insert cost, not the steady-state amortised flush
            // cost. A 256 MiB write_buffer keeps everything in one memtable
            // for the whole timing window (BATCH_SIZE * BATCHES_PER_ITER *
            // ~24B ≈ 2.4 MiB).
            b.iter_batched(
                || {
                    let db = open_in_memory(256 * 1024 * 1024);
                    let cf = db.default_cf();
                    (db, cf)
                },
                |(db, cf)| {
                    for _ in 0..BATCHES_PER_ITER {
                        db.batch_put_arrow(&cf, batch).expect("batch_put_arrow");
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.bench_with_input(
        BenchmarkId::new("write_batch_legacy", BATCH_SIZE),
        &(keys.clone(), values.clone()),
        |b, (keys, values)| {
            b.iter_batched(
                || {
                    let db = open_in_memory(256 * 1024 * 1024);
                    let cf = db.default_cf();
                    (db, cf)
                },
                |(db, cf)| {
                    for _ in 0..BATCHES_PER_ITER {
                        let mut wb = WriteBatch::with_capacity(BATCH_SIZE);
                        for i in 0..BATCH_SIZE {
                            wb.put(&cf, &keys[i], &values[i]);
                        }
                        db.batch_write(wb).expect("batch_write");
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

criterion_group!(c1, bench_batch_put_arrow);
criterion_main!(c1);

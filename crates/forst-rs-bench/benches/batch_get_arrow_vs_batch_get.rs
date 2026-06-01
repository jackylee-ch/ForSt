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

/// V10 (spec §3 D10) — SST-tiered bench. The active-memtable bench above
/// hits the fast path on every key and cannot exercise the vectorized
/// SST grouping. This bench flushes the key pool down to multiple L0
/// SSTs so every lookup falls through to phase 5 of
/// `batch_get_vectorized`, where the per-batch hoists (one version
/// snapshot, one live-files HashSet, one reader-open per SST instead
/// of per (key, SST)) deliver the projected 4.2x speedup.
fn bench_batch_get_sst_tier(c: &mut Criterion) {
    use forst_rs_bench::open_in_memory;
    let mut group = c.benchmark_group("batch_get_sst_tier");

    // Build a pool whose keys span multiple L0 SSTs by repeatedly
    // flushing the active memtable.
    let key_pool_size = 4096u32;
    let pool: Vec<Vec<u8>> = (0..key_pool_size)
        .map(|i| format!("k{:08}", i).into_bytes())
        .collect();

    for &batch_size in &[16usize, 64, 256, 1024] {
        // Small write buffer + manual flushes ⇒ multiple L0 SSTs.
        let db = open_in_memory(4 * 1024 * 1024);
        let cf = db.default_cf();
        // Flush every 512 keys so we end up with ~8 L0 SSTs.
        for chunk in pool.chunks(512) {
            for k in chunk {
                db.put(&cf, k.as_slice(), b"value-payload-16b")
                    .expect("put");
            }
            db.switch_and_flush(&cf).expect("switch_and_flush");
        }
        // Drop active memtable so reads ALWAYS go to SST.

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_get_vectorized", batch_size),
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

        // Per-key naive baseline (what the old `frs_vectorized_batch_get`
        // and pre-V10 `batch_get` effectively did — N independent
        // `db.get` calls). Each call independently snapshots the version,
        // builds the live-files HashSet, and walks the resident list.
        // V10's vectorized variant amortizes these to one-per-batch.
        group.bench_with_input(
            BenchmarkId::new("get_per_key", batch_size),
            &batch_size,
            |b, &size| {
                let mut start = 0usize;
                b.iter(|| {
                    for i in 0..size {
                        let k = pool[(start + i) % pool.len()].as_slice();
                        let _ = db.get(&cf, k).expect("get");
                    }
                    start = start.wrapping_add(size);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_batch_get_arrow_vs_batch_get,
    bench_batch_get_sst_tier
);
criterion_main!(benches);

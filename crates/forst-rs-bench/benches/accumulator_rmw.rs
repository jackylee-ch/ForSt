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

//! Windowed-agg accumulator-RMW microbench (Approach 2 / V-C pre-build gate).
//!
//! Confirms — WITHOUT NexMark — the batch-execution-violation **V-C** the
//! omnipotent-rethink §2 names: the keyed-window / OVER accumulator RMW today
//! round-trips Java↔engine per record (`get accumulator → fold in Java → put
//! accumulator`), which is TWO ops per record AND a dependent get→put chain.
//! The in-engine path submits ONLY the delta as a real engine `Merge` operand,
//! folded by the CF's merge operator at read/flush/compaction time — ONE op per
//! record and NO dependent read.
//!
//! This isolates the engine-side cost of the two strategies (the FFI-crossing
//! count is structural: Arm A is 2 crossings/record, Arm B is 1). The absolute
//! ns/record here is the engine work behind each crossing; the FFM crossing
//! itself adds a further fixed per-call tax in the real JNI path that this micro
//! does not pay, so the measured Arm-A:Arm-B gap is a LOWER bound on the
//! production win.
//!
//! Three measurements (spec §4.2):
//! 1. `accumulator_rmw/get_fold_put` (Arm A) vs `accumulator_rmw/merge` (Arm B):
//!    ns/record for K records folded into the SAME key (the q12/q17 hot key).
//! 2. `accumulator_rmw_distinct/*`: the q8/q11-shape — K records spread across
//!    many distinct keys (one fold each), Arm A vs Arm B.
//! 3. `accumulator_read_cost/*`: the read-cost guard — after K in-engine merges,
//!    the cost of READING the accumulator WITHOUT a flush (walks the operand
//!    chain, O(K)) vs WITH a flush-collapse (partial/full-merge bounds it,
//!    O(1)). This is the q20-regression falsifier, measured before any NexMark.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::{ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl};
use forst_rs_storage::merge_operator::NumericAddBeMergeOperator;

/// Big-endian i64 = Flink `DataOutputSerializer.writeLong` order; the bytes the
/// `NumericAddBeMergeOperator` folds and the bytes a Java `Long` accumulator
/// would serialize to.
fn be(v: i64) -> [u8; 8] {
    v.to_be_bytes()
}

/// A CF whose accumulator fold runs IN-ENGINE (BE wrapping i64 add = Java
/// `long +` over network-order bytes).
fn merge_cf(db: &Arc<DbImpl>, name: &str) -> ColumnFamilyHandle {
    db.create_column_family(
        ColumnFamilyDescriptor::new(name)
            .with_merge_operator(Arc::new(NumericAddBeMergeOperator::new())),
    )
    .expect("create merge cf")
}

/// 8-byte BE accumulator key for accumulator slot `i`.
fn acc_key(i: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[4..].copy_from_slice(&i.to_be_bytes());
    k
}

/// ARM A — the V-C round-trip: read the accumulator, fold the delta in
/// caller-space (stands in for the Java reduce), write it back. Two engine ops
/// per record + a dependent get→put. This is what q8/q11/q12/q17 do today.
fn arm_a_get_fold_put(db: &Arc<DbImpl>, cf: &ColumnFamilyHandle, key: &[u8], delta: i64) {
    let cur = match db.get(cf, key).expect("get accumulator") {
        Some(v) => i64::from_be_bytes(v.as_slice().try_into().expect("8-byte accumulator")),
        None => 0,
    };
    let next = cur.wrapping_add(delta); // Java `long +`
    db.put(cf, key, &be(next)).expect("put accumulator");
}

/// ARM B — in-engine merge: submit ONLY the delta as a `Merge` operand. One
/// engine op per record, no dependent read; the engine folds at read time.
fn arm_b_merge(db: &Arc<DbImpl>, cf: &ColumnFamilyHandle, key: &[u8], delta: i64) {
    db.merge(cf, key, &be(delta)).expect("merge delta");
}

/// Measurement 1 + 2: ns/record for the two arms, on a single hot key (q12/q17
/// shape) and on many distinct keys (q8/q11 shape).
fn bench_accumulator_rmw(c: &mut Criterion) {
    // --- Single hot key: every record folds into the same accumulator. ---
    {
        let mut group = c.benchmark_group("accumulator_rmw");
        let key = acc_key(0);

        group.bench_function(BenchmarkId::from_parameter("get_fold_put"), |b| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = merge_cf(&db, "accA");
            let mut d = 1i64;
            b.iter(|| {
                arm_a_get_fold_put(&db, &cf, &key, d);
                d = d.wrapping_add(1);
                black_box(d);
            });
        });

        group.bench_function(BenchmarkId::from_parameter("merge"), |b| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = merge_cf(&db, "accB");
            let mut d = 1i64;
            b.iter(|| {
                arm_b_merge(&db, &cf, &key, d);
                d = d.wrapping_add(1);
                black_box(d);
            });
        });
        group.finish();
    }

    // --- Distinct keys: q8/q11 shape — one fold per key, rotating keys. ---
    {
        let num_keys = 4096u32;
        let mut group = c.benchmark_group("accumulator_rmw_distinct");

        group.bench_function(BenchmarkId::from_parameter("get_fold_put"), |b| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = merge_cf(&db, "accDistA");
            let mut i = 0u32;
            b.iter(|| {
                arm_a_get_fold_put(&db, &cf, &acc_key(i % num_keys), 1);
                i = i.wrapping_add(1);
                black_box(i);
            });
        });

        group.bench_function(BenchmarkId::from_parameter("merge"), |b| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = merge_cf(&db, "accDistB");
            let mut i = 0u32;
            b.iter(|| {
                arm_b_merge(&db, &cf, &acc_key(i % num_keys), 1);
                i = i.wrapping_add(1);
                black_box(i);
            });
        });
        group.finish();
    }
}

/// Measurement 3 — the read-cost guard (q20-regression falsifier). After K
/// in-engine merges into one key, READ the accumulator in three states:
///   (a) `no_flush`  — chain resident in the memtable (read walks all K operands);
///   (b) `flushed`   — chain written to ONE L0 SST by a flush (flush does NOT
///       fold the chain — it persists each operand, so the read still walks K);
///   (c) `compacted` — chain collapsed by a compaction (full/partial-merge folds
///       it to ONE value, so the read is O(1)).
///
/// NOTE on the `compacted` arm: the DEFINITIVE bound proof is the engine unit
/// test `test_accumulator_merge_read_cost_chain_collapses_on_compaction`, which
/// asserts the operand chain collapses to a SINGLE entry (num_entries == 1)
/// after compaction in the fixed-level layout — O(1) read, byte-identical value.
/// This bench's `compacted` arm CANNOT trigger that collapse from the bench
/// crate: `force_fixed_levels()` is `pub(crate)`, and in the default
/// dynamic-level mode a single-key L0 set rolls to L1 as a trivial move without
/// folding, so the arm stays ≈ `no_flush` (an artifact of the bench's reach, not
/// of the engine). The HONEST takeaway the bench shows: a flush alone does NOT
/// bound the chain (`flushed` ≈ `no_flush`); compaction is what collapses it
/// (proven by the unit test). A heavy between-compaction burst transiently pays
/// the chain — the documented no-flush artifact / q20 read-cost guard.
fn bench_accumulator_read_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("accumulator_read_cost");
    let key = acc_key(0);

    for &k in &[16u32, 256, 4096] {
        // (a) chain resident in the memtable — read walks K operands.
        group.bench_with_input(BenchmarkId::new("no_flush", k), &k, |b, &k| {
            let db = open_in_memory(512 * 1024 * 1024); // big buffer: no auto-flush
            let cf = merge_cf(&db, &format!("readNF_{k}"));
            for i in 0..k {
                db.merge(&cf, &key, &be(i as i64)).expect("merge");
            }
            b.iter(|| {
                let v = db.get(&cf, &key).expect("get").expect("present");
                black_box(v);
            });
        });

        // (b) chain persisted to ONE L0 SST by a flush — flush does NOT fold.
        group.bench_with_input(BenchmarkId::new("flushed", k), &k, |b, &k| {
            let db = open_in_memory(512 * 1024 * 1024);
            let cf = merge_cf(&db, &format!("readF_{k}"));
            for i in 0..k {
                db.merge(&cf, &key, &be(i as i64)).expect("merge");
            }
            db.flush_cf(&cf).expect("flush");
            b.iter(|| {
                let v = db.get(&cf, &key).expect("get").expect("present");
                black_box(v);
            });
        });

        // (c) chain collapsed by a compaction — read sees ONE folded value.
        // The collapse fires when the chain spans MULTIPLE L0 files (the
        // production flush-cadence shape): a flush per chunk produces several
        // L0 SSTs, then compact_l0 folds the whole chain across them via
        // full/partial-merge down to a single entry.
        group.bench_with_input(BenchmarkId::new("compacted", k), &k, |b, &k| {
            let db = open_in_memory(512 * 1024 * 1024);
            let cf = merge_cf(&db, &format!("readC_{k}"));
            // Spread the K merges across ~K/4 flushes (>=2 L0 files) so the
            // chain is not a single-SST trivial move.
            let chunk = (k / 4).max(1);
            for i in 0..k {
                db.merge(&cf, &key, &be(i as i64)).expect("merge");
                if (i + 1) % chunk == 0 {
                    db.flush_cf(&cf).expect("flush");
                }
            }
            db.flush_cf(&cf).expect("final flush");
            db.compact_l0(&cf).expect("compact collapses the chain");
            b.iter(|| {
                let v = db.get(&cf, &key).expect("get").expect("present");
                black_box(v);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_accumulator_rmw, bench_accumulator_read_cost);
criterion_main!(benches);

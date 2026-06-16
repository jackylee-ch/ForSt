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

//! FRS-MEM-MANAGER OPTIMAL-CONFIG-OPTIMIZES guard (PMC-1, 2026-06-16).
//!
//! The unified memory controller's caps must only BIND under pressure — at a
//! GENEROUS (ample) config the per-op write/merge path must be perf-neutral vs
//! the manager OFF (no spurious capping: no early WBM stall, no windowed-ceiling
//! stall, no early force-flush, no compaction-admission wait). This micro proves
//! that on the engine write path WITHOUT NexMark.
//!
//! # Why this is a valid neutrality proof
//!
//! The controller's hot-path cost when armed-but-ample is: one `OnceLock`-cached
//! cap read per consumer (resolved once), the windowed force-flush floor compare
//! (a `usize` compare), the windowed-ceiling compare (an atomic load + compare),
//! and the compaction-admission CAS (only on the bg thread). At an ample budget
//! every cap is far above the working set, so every gate's "over?" predicate is
//! FALSE and the writer proceeds exactly as in the OFF path — the only added cost
//! is the (cached) compares. This bench measures the steady-state ns/op of a
//! merge-CF (q5-shape) and a plain put-CF (q4-shape) under:
//!
//!   * `*/off`         — manager OFF (env unset): today's behaviour, the baseline.
//!   * `*/armed_ample` — manager ON at a HUGE cgroup (256 g) so no cap binds.
//!
//! A perf-neutral controller ⇒ `armed_ample` ns/op ≈ `off` ns/op (within noise).
//! A REGRESSION here (armed_ample materially slower) would mean a cap binds too
//! early at the optimal config and the budget split must be fixed.
//!
//! NOTE on process-global state: `manager_enabled()` caches ARMED=true once set,
//! and the engine-native budget caches its first successful computation. So the
//! OFF arms MUST run before arming. Criterion runs benches in registration order
//! within a group; this file arms the manager AFTER the OFF group completes, with
//! a 256 g cgroup pinned so the cached budget is ample for the armed arms.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::{ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl};
use forst_rs_storage::merge_operator::RawConcatMergeOperator;

const N: u32 = 20_000;

fn merge_cf(db: &Arc<DbImpl>, name: &str) -> ColumnFamilyHandle {
    db.create_column_family(
        ColumnFamilyDescriptor::new(name)
            .with_merge_operator(Arc::new(RawConcatMergeOperator::new())),
    )
    .expect("create merge cf")
}

/// q5-shape: a windowed merge-CF accumulating operands across distinct panes.
/// Exercises the windowed force-flush floor + the windowed-ceiling gate.
fn run_merge_workload(db: &Arc<DbImpl>, cf: &ColumnFamilyHandle) {
    for i in 0..N {
        let key = (i % 4096).to_be_bytes(); // 4096 distinct panes (breadth)
        let val = i.to_le_bytes();
        db.merge(cf, &key, &val).expect("merge");
    }
}

/// q4-shape: a plain put-CF. Exercises the WBM soft/hard-cap gate.
fn run_put_workload(db: &Arc<DbImpl>, cf: &ColumnFamilyHandle) {
    for i in 0..N {
        let key = i.to_be_bytes();
        db.put(cf, &key, b"payload-32-bytes-padding-aaaaaaa")
            .expect("put");
    }
}

fn bench_off(c: &mut Criterion) {
    // Ensure a clean OFF environment for the baseline arms.
    std::env::remove_var("FRS_MEM_MANAGER");
    std::env::remove_var("FRS_MEM_CGROUP_MB");
    std::env::remove_var("FRS_JVM_RESERVED_MB");

    let mut g = c.benchmark_group("mem_manager_overhead");
    g.sample_size(10);

    g.bench_with_input(BenchmarkId::new("merge", "off"), &N, |b, _| {
        b.iter(|| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = merge_cf(&db, "win");
            run_merge_workload(&db, &cf);
            black_box(&db);
        });
    });
    g.bench_with_input(BenchmarkId::new("put", "off"), &N, |b, _| {
        b.iter(|| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = db.default_cf();
            run_put_workload(&db, &cf);
            black_box(&db);
        });
    });
    g.finish();
}

fn bench_armed_ample(c: &mut Criterion) {
    // Arm the controller at an AMPLE (256 g) budget so NO cap can bind. The
    // budget OnceLock caches this first successful computation for the run.
    std::env::set_var("FRS_MEM_MANAGER", "1");
    std::env::set_var("FRS_MEM_CGROUP_MB", "262144"); // 256 GiB cgroup
    std::env::set_var("FRS_JVM_RESERVED_MB", "8192"); //   8 GiB JVM → huge native

    let mut g = c.benchmark_group("mem_manager_overhead");
    g.sample_size(10);

    g.bench_with_input(BenchmarkId::new("merge", "armed_ample"), &N, |b, _| {
        b.iter(|| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = merge_cf(&db, "win");
            run_merge_workload(&db, &cf);
            black_box(&db);
        });
    });
    g.bench_with_input(BenchmarkId::new("put", "armed_ample"), &N, |b, _| {
        b.iter(|| {
            let db = open_in_memory(64 * 1024 * 1024);
            let cf = db.default_cf();
            run_put_workload(&db, &cf);
            black_box(&db);
        });
    });
    g.finish();
}

criterion_group!(benches, bench_off, bench_armed_ample);
criterion_main!(benches);

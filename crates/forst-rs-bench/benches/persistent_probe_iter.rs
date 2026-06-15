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

//! APPROACH-1 (2026-06-15 omnipotent rethink §4.1) mini-bench: PERSISTENT
//! seekable probe iterator vs the per-probe REBUILT source set.
//!
//! Two decisive measurements, NEITHER is NexMark:
//!
//! 1. **Read-amp / p50 (the q7 win).** Same key-group probed K times. Arm A =
//!    `prefix_scan_iter_owned_arc` (rebuilds the version snapshot + resident
//!    clone + overlapping-SST locate per probe). Arm B = one
//!    `PersistentProbeIter::seek_drain` reused across all K probes (amortizes
//!    the source-set construction). PASS = B's ns/probe drops materially vs A on
//!    the deep multi-source cell — the same within-key-group pattern q7/q9/q20
//!    issue.
//!
//! 2. **Memory additive (the q9 fix direction).** A counting global allocator
//!    measures BYTES ALLOCATED PER PROBE for each arm. The rebuild arm's
//!    per-probe additive = version snapshot + resident clone + locate Vec; the
//!    persistent arm reuses them, so its per-probe additive drops toward the
//!    irreducible per-source cursor/prefetcher floor. Printed as a table so the
//!    eliminated additive is exactly sized.
//!
//! Contention-robust: in-memory FS, single thread, no NexMark, fixed work.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_bench::open_in_memory;
use forst_rs_common::config::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, MemoryFileSystem};

// ---------------------------------------------------------------------------
// Counting allocator: only counts allocations while ARMED (the per-probe
// window), so setup/teardown noise is excluded. Net of the irreducible
// criterion harness churn by measuring both arms identically.
// ---------------------------------------------------------------------------
struct CountingAlloc;
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
static ARMED: AtomicBool = AtomicBool::new(false);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ARMED.load(Ordering::Relaxed) {
            ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn key(jk: u32, entry: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[..4].copy_from_slice(&jk.to_be_bytes());
    k[4..].copy_from_slice(&entry.to_be_bytes());
    k
}
fn prefix(jk: u32) -> [u8; 4] {
    jk.to_be_bytes()
}

/// Build a DB whose state is scattered across `rounds` flushed L0 SSTs, each
/// spanning the full join-key range (coarse range-skip cannot prune — the
/// adversarial deep multi-source probe). Tiny write buffer + high L0 triggers
/// so the fan-out persists at probe time.
fn build_scattered(rounds: u32, num_keys: u32) -> Arc<DbImpl> {
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "1000000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "1000000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "1000000");
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 4096,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    let cf = db.default_cf();
    for round in 0..rounds {
        for jk in 0..num_keys {
            let v = vec![0xCDu8; 48];
            db.put(&cf, &key(jk, round), &v).expect("put");
        }
        db.flush_cf(&cf).expect("flush");
    }
    std::env::remove_var("FRS_L0_COMPACTION_TRIGGER");
    std::env::remove_var("FRS_L0_STOP_TRIGGER");
    std::env::remove_var("FRS_L0_SLOWDOWN_TRIGGER");
    let _ = open_in_memory; // keep the import (other benches share the helper)
    db
}

/// (1) Read-amp / p50: the same key-group probed repeatedly.
fn bench_read_amp(c: &mut Criterion) {
    let num_keys = 64u32;
    let mut group = c.benchmark_group("persistent_probe_read_amp");
    for &rounds in &[8u32, 32, 64] {
        let db = build_scattered(rounds, num_keys);
        let cf = db.default_cf();

        // Arm A: rebuild per probe (today's default path).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("rebuild_ssts_{}", rounds)),
            &rounds,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    let it = db
                        .prefix_scan_iter_owned_arc(&cf, &prefix(jk % num_keys))
                        .expect("rebuild open");
                    let mut n = 0u64;
                    for item in it {
                        let _ = item.expect("row");
                        n += 1;
                    }
                    jk = jk.wrapping_add(1);
                    black_box(n);
                });
            },
        );

        // Arm B: persistent seek reuse — ONE handle, reused across probes.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("persistent_ssts_{}", rounds)),
            &rounds,
            |b, &_n| {
                let mut ppi = db
                    .open_persistent_probe_iter(&cf, &prefix(0))
                    .expect("open ppi");
                let mut jk = 0u32;
                b.iter(|| {
                    let (n, _bytes) = ppi.seek_drain(&prefix(jk % num_keys)).expect("seek_drain");
                    jk = jk.wrapping_add(1);
                    black_box(n);
                });
            },
        );
    }
    group.finish();
}

/// (2) Memory additive: bytes allocated PER PROBE for each arm. The difference
/// is the eliminated per-probe additive (version snapshot + resident clone +
/// locate Vec) — the structure the spec ties to q9's resident additive.
fn bench_memory_additive(c: &mut Criterion) {
    // Run once outside criterion's timing loop and PRINT the table — the
    // allocator counter is the measurement, not wall time.
    let num_keys = 64u32;
    let probes_per_key = 50u64;

    println!("\n=== APPROACH-1 per-probe allocation additive (q9 memory fix) ===");
    println!(
        "{:<10} {:>16} {:>16} {:>14}",
        "ssts", "rebuild B/probe", "persist B/probe", "eliminated"
    );
    for &rounds in &[8u32, 32, 64] {
        let db = build_scattered(rounds, num_keys);
        let cf = db.default_cf();

        // Arm A: rebuild — measure allocated bytes over many probes.
        ALLOC_BYTES.store(0, Ordering::Relaxed);
        ARMED.store(true, Ordering::Relaxed);
        let mut total_rows_a = 0u64;
        for jk in 0..num_keys {
            for _ in 0..probes_per_key {
                let it = db
                    .prefix_scan_iter_owned_arc(&cf, &prefix(jk))
                    .expect("open");
                for item in it {
                    let _ = item.expect("row");
                    total_rows_a += 1;
                }
            }
        }
        ARMED.store(false, Ordering::Relaxed);
        let probes = num_keys as u64 * probes_per_key;
        let rebuild_per_probe = ALLOC_BYTES.load(Ordering::Relaxed) / probes;

        // Arm B: persistent — one handle, reused across all probes.
        ALLOC_BYTES.store(0, Ordering::Relaxed);
        let mut ppi = db
            .open_persistent_probe_iter(&cf, &prefix(0))
            .expect("open ppi");
        ARMED.store(true, Ordering::Relaxed);
        let mut total_rows_b = 0u64;
        for jk in 0..num_keys {
            for _ in 0..probes_per_key {
                let (n, _b) = ppi.seek_drain(&prefix(jk)).expect("seek_drain");
                total_rows_b += n;
            }
        }
        ARMED.store(false, Ordering::Relaxed);
        let persist_per_probe = ALLOC_BYTES.load(Ordering::Relaxed) / probes;

        assert_eq!(
            total_rows_a, total_rows_b,
            "row count must match across arms"
        );
        let eliminated = rebuild_per_probe.saturating_sub(persist_per_probe);
        println!(
            "{:<10} {:>16} {:>16} {:>13}B  ({:.0}% reuses)",
            rounds,
            rebuild_per_probe,
            persist_per_probe,
            eliminated,
            100.0 * ppi.located_reuses() as f64 / probes as f64,
        );
    }
    println!("================================================================\n");

    // Keep criterion happy with a trivial timed cell so the bench registers.
    c.bench_function("persistent_probe_memory_additive_noop", |b| {
        b.iter(|| black_box(1u64))
    });
}

criterion_group!(benches, bench_read_amp, bench_memory_additive);
criterion_main!(benches);

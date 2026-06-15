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

//! Join-probe prefix-iterator OPEN microbench.
//!
//! Reproduces the q7/q9/q16/q20 hot path the jstack profiler flagged
//! (`VectorizedExecutor.executeIters → ForStRsDBIterRequest.openVecIterIntoBuf
//! → frsVecIterPrefixOpen`, 61/85 executor samples): a streaming join issues
//! ONE prefix scan per incoming record to find the matching records under the
//! join key. Each scan OPENS a fresh prefix iterator = a lazy k-way merge over
//! (active memtable + immutable memtables + resident-flushed memtables + every
//! overlapping L0 SST).
//!
//! The adversarial-but-realistic shape: records arrive round-robin across many
//! join keys and the engine flushes periodically (checkpoint cadence). Each
//! flushed SST therefore spans the FULL join-key range, so the coarse
//! smallest/largest_key range-skip in `build_lazy_prefix_key_stream` CANNOT
//! prune any SST — every probe must consider every SST + resident memtable.
//! This isolates the per-open bookkeeping cost (live-SST-set build, resident
//! visible-entry clone, per-source seek) as a function of LSM depth.

use std::sync::{Arc, Mutex};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::{DbImpl, FillOutcome, RowSink};

/// Join-key namespace prefix for join key `jk`: 8-byte big-endian so prefixes
/// sort in the same order as the keys, mirroring the Flink composite-key
/// layout `[keygroup][namespace][user-key]`.
fn ns_prefix(jk: u32) -> [u8; 8] {
    let mut p = [0u8; 8];
    p[..4].copy_from_slice(&jk.to_be_bytes());
    p
}

/// Full state key: `[join-key BE][entry-id BE]`.
fn state_key(jk: u32, entry: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[..4].copy_from_slice(&jk.to_be_bytes());
    k[4..].copy_from_slice(&entry.to_be_bytes());
    k
}

/// Builds an engine whose join state is spread across `num_ssts` flushed L0
/// SSTs, **each spanning the full join-key range** (so the coarse range-skip
/// can't prune any of them — the adversarial long-running-join case).
///
/// One round = one full pass writing entry `r` to EVERY join key, then a
/// flush. So round `r` produces SST `r` covering keys `[0 .. num_keys)`, and a
/// probe for any join key finds one matching entry in every SST. `num_ssts`
/// rounds → `num_ssts` overlapping SSTs.
fn build_db(num_keys: u32, num_ssts: u32) -> Arc<DbImpl> {
    // 64 MiB buffer: large enough that only our explicit flushes cut SSTs.
    let db = open_in_memory(64 * 1024 * 1024);
    let cf = db.default_cf();
    let val = vec![0xCDu8; 64];

    for round in 0..num_ssts {
        for jk in 0..num_keys {
            db.put(&cf, &state_key(jk, round), &val).expect("put");
        }
        // Each full pass becomes one SST spanning the entire join-key range.
        db.flush_cf(&cf).expect("flush");
    }
    db
}

/// Measures the cost of OPENING a prefix iterator (+ draining its few entries),
/// which is exactly what a join probe does per record. We rotate the probed
/// join key each iteration to defeat any per-key caching and to average over
/// the key space.
fn bench_join_probe_open(c: &mut Criterion) {
    let num_keys = 4096u32;

    let mut group = c.benchmark_group("join_probe_open");
    // Sweep LSM depth: 1 SST (best case, fits one) → 128 SSTs (deep L0 fan-out
    // like a long-running join between checkpoints/compactions). Each SST spans
    // the full join-key range, so coarse range-skip cannot prune.
    for &num_ssts in &[1u32, 8, 32, 64, 128] {
        let db = build_db(num_keys, num_ssts);
        let cf = db.default_cf();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("ssts_{}", num_ssts)),
            &num_ssts,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    let prefix = ns_prefix(jk % num_keys);
                    // OPEN + drain — the join-probe unit of work.
                    let iter = db
                        .prefix_scan_iter_owned_arc(&cf, &prefix)
                        .expect("open prefix iter");
                    let mut n = 0usize;
                    for item in iter {
                        let (_k, _v) = item.expect("iter item");
                        n += 1;
                    }
                    jk = jk.wrapping_add(1);
                    black_box(n);
                });
            },
        );
    }
    group.finish();
}

/// S2 stage-2 (work-order §6.1/§6.3): the SAME probe shape driven through the
/// raw push-style `PrefixScanStream::fill_into` (pinned mode forced, sink =
/// byte-counting no-op) — measured ALONGSIDE the continuity series above
/// (which drains via `prefix_scan_iter_owned_arc` and therefore measures the
/// Arc-pair COMPAT ADAPTER when the flag is ON), so the adapter tax is itself
/// observable. Do not rewire the cells above.
fn bench_join_probe_fill_into(c: &mut Criterion) {
    /// Byte-counting no-op sink (no copies, no allocs).
    struct CountBytes(u64);
    impl RowSink for CountBytes {
        fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
            self.0 += (key.len() + value.len()) as u64;
            true
        }
    }

    let num_keys = 4096u32;
    let mut group = c.benchmark_group("join_probe_open_fill_into");
    for &num_ssts in &[1u32, 8, 32, 64, 128] {
        let db = build_db(num_keys, num_ssts);
        let cf = db.default_cf();

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("ssts_{}", num_ssts)),
            &num_ssts,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    let prefix = ns_prefix(jk % num_keys);
                    let mut stream = db
                        .prefix_scan_stream_with_mode(
                            &cf,
                            &prefix,
                            Arc::new(Mutex::new(None)),
                            true, // pinned (the S2 path under test)
                        )
                        .expect("open prefix stream");
                    let mut sink = CountBytes(0);
                    let outcome = stream.fill_into(&mut sink).expect("fill_into");
                    assert_eq!(outcome, FillOutcome::Exhausted);
                    jk = jk.wrapping_add(1);
                    black_box(sink.0);
                });
            },
        );
    }
    group.finish();
}

/// R1 (2026-06-14, repair design §3-R1): per-scan fan-out-ADAPTIVE S2 gate.
///
/// The `build_db` fixture above lets background compaction collapse the L0
/// fan-out down to ~1 SST during criterion's steady-state window, so it cannot
/// exercise a SUSTAINED deep probe (the q7/q9/q20 long-running-join regime
/// where forst-rs holds L0 at 40-64). This group raises
/// `FRS_L0_COMPACTION_TRIGGER`/`FRS_L0_STOP_TRIGGER` so the overlapping SSTs
/// PERSIST at scan time, then measures THREE arms on the IDENTICAL fixture:
///   * `legacy`   — forced OFF (today's path);
///   * `pinned`   — forced ON (the full S2 loser-tree win, the ceiling);
///   * `adaptive` — the R1 selector (threshold 8) — should match `pinned` on
///     the deep cells (n_overlap >= 8 → loser tree) and `legacy` on the shallow
///     cell (n_overlap == 1 < 8 → no pinned tax). One config, no shallow tax,
///     full deep win.
fn bench_join_probe_adaptive(c: &mut Criterion) {
    use forst_rs_engine::{
        set_s2_adaptive_fanout_min_override, set_s2_pinned_override, FillOutcome, RowSink,
    };

    struct CountBytes(u64);
    impl RowSink for CountBytes {
        fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
            self.0 += (key.len() + value.len()) as u64;
            true
        }
    }

    // Sustain the L0 fan-out for the lifetime of this group: keep auto-compaction
    // from collapsing the per-round SSTs so a deep probe actually sees them.
    // SAFETY: single-threaded bench setup; read fresh per DB open (env_u32).
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "100000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "100000");

    let drain = |db: &Arc<forst_rs_engine::DbImpl>, prefix: &[u8]| {
        let cf = db.default_cf();
        let mut stream = db
            .prefix_scan_stream_with_error_slot(&cf, prefix, Arc::new(Mutex::new(None)))
            .expect("open prefix stream");
        let mut sink = CountBytes(0);
        let outcome = stream.fill_into(&mut sink).expect("fill_into");
        assert_eq!(outcome, FillOutcome::Exhausted);
        sink.0
    };

    // Deep-fan-out fixture: TINY write buffer so every round flushes a real
    // SST, with the high trigger above so they are NOT compacted away — a
    // 4-byte (join-key-only) prefix probe then overlaps ~rounds SSTs (the
    // sustained long-running-join regime). (Diagnosed: 64 MiB buffer → 1
    // source; 4 KiB buffer + trigger 100000 → sources≈rounds.)
    let deep_keys = 64u32;
    let build_deep = |rounds: u32| -> Arc<forst_rs_engine::DbImpl> {
        let db = open_in_memory(4096);
        let cf = db.default_cf();
        for round in 0..rounds {
            for jk in 0..deep_keys {
                let mut k = [0u8; 8];
                k[..4].copy_from_slice(&jk.to_be_bytes());
                k[4..].copy_from_slice(&round.to_be_bytes());
                let v = vec![0xCDu8; 64];
                db.put(&cf, &k, &v).expect("put");
            }
            db.flush_cf(&cf).expect("flush");
        }
        db
    };
    let prefix4 = |jk: u32| -> [u8; 4] { jk.to_be_bytes() };

    let mut group = c.benchmark_group("join_probe_open_adaptive");
    for &rounds in &[1u32, 8, 32, 64, 128] {
        let db = build_deep(rounds);

        // legacy (forced OFF)
        set_s2_pinned_override(Some(false));
        set_s2_adaptive_fanout_min_override(None);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("legacy_ssts_{}", rounds)),
            &rounds,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    black_box(drain(&db, &prefix4(jk % deep_keys)));
                    jk = jk.wrapping_add(1);
                });
            },
        );

        // pinned (forced ON — the ceiling)
        set_s2_pinned_override(Some(true));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("pinned_ssts_{}", rounds)),
            &rounds,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    black_box(drain(&db, &prefix4(jk % deep_keys)));
                    jk = jk.wrapping_add(1);
                });
            },
        );

        // adaptive (R1 selector, threshold 8)
        set_s2_pinned_override(None);
        set_s2_adaptive_fanout_min_override(Some(8));
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("adaptive_ssts_{}", rounds)),
            &rounds,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    black_box(drain(&db, &prefix4(jk % deep_keys)));
                    jk = jk.wrapping_add(1);
                });
            },
        );
    }
    group.finish();

    // Restore default global state for any later bench in the same process.
    set_s2_pinned_override(None);
    set_s2_adaptive_fanout_min_override(None);
    std::env::remove_var("FRS_L0_COMPACTION_TRIGGER");
    std::env::remove_var("FRS_L0_STOP_TRIGGER");
    std::env::remove_var("FRS_L0_SLOWDOWN_TRIGGER");
}

/// APPROACH 1 (2026-06-15 omnipotent-rethink §4.1 + the q7 probe-open prune
/// next-cycle candidate): **leveled vs tiered** per-probe source count.
///
/// The size-tiered L0 layout is the read-amp root: a long-running join holds
/// `rounds` overlapping SSTs, each spanning the full join-key range, so EVERY
/// probe fans out over all `rounds` sources (the `tiered` arm — identical fixture
/// to `bench_join_probe_adaptive`). A true LEVELED bottom level holds ONE
/// non-overlapping run, so a probe locates ≤1 source regardless of `rounds`. We
/// approximate the leveled bottom level with `compact_range` (a full-range
/// compaction that collapses the overlapping L0 SSTs into a single sorted run),
/// then measure the SAME probe.
///
/// PASS (the gate for building leveled-on-hot-CFs): the `leveled` arm's per-probe
/// time is ~FLAT in `rounds` (source count bounded to ≈#levels) while `tiered`
/// rises ~linearly — i.e. "leveled-N ≈ tiered-1". This complements MR-1: MR-1
/// cuts the WASTED cold opens of bloom-negative sources; leveled cuts the source
/// COUNT itself. Run BEFORE any leveled-compaction build (NOT NexMark).
fn bench_join_probe_leveled_vs_tiered(c: &mut Criterion) {
    // Sustain the L0 fan-out so the `tiered` arm actually sees `rounds` sources.
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "100000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "100000");

    let deep_keys = 64u32;
    let build_deep = |rounds: u32| -> Arc<DbImpl> {
        let db = open_in_memory(4096);
        let cf = db.default_cf();
        for round in 0..rounds {
            for jk in 0..deep_keys {
                let mut k = [0u8; 8];
                k[..4].copy_from_slice(&jk.to_be_bytes());
                k[4..].copy_from_slice(&round.to_be_bytes());
                db.put(&cf, &k, &[0xCDu8; 64]).expect("put");
            }
            // switch_and_flush forces one SST per round (tiered fan-out fixture).
            db.switch_and_flush(&cf).expect("switch+flush");
        }
        db
    };
    let prefix4 = |jk: u32| -> [u8; 4] { jk.to_be_bytes() };
    let drain = |db: &Arc<DbImpl>, prefix: &[u8]| {
        let cf = db.default_cf();
        let it = db
            .prefix_scan_iter_owned_arc(&cf, prefix)
            .expect("open prefix iter");
        let mut n = 0usize;
        for item in it {
            let _ = item.expect("row");
            n += 1;
        }
        n
    };

    let mut group = c.benchmark_group("join_probe_leveled_vs_tiered");
    for &rounds in &[8u32, 32, 64, 128] {
        // tiered: `rounds` overlapping L0 SSTs (today's layout).
        let db_tiered = build_deep(rounds);
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("tiered_ssts_{rounds}")),
            &rounds,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    black_box(drain(&db_tiered, &prefix4(jk % deep_keys)));
                    jk = jk.wrapping_add(1);
                });
            },
        );

        // leveled: collapse the overlapping L0 into ONE non-overlapping run via a
        // full-range compaction (the leveled-bottom-level approximation). A probe
        // now locates ≤1 source regardless of `rounds`.
        let db_leveled = build_deep(rounds);
        let cf = db_leveled.default_cf();
        // Collapse repeatedly until the run is non-overlapping (one full-range
        // pass merges all current inputs into a single sorted output).
        let _ = db_leveled.compact_range(&cf).expect("compact_range");
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("leveled_ssts_{rounds}")),
            &rounds,
            |b, &_n| {
                let mut jk = 0u32;
                b.iter(|| {
                    black_box(drain(&db_leveled, &prefix4(jk % deep_keys)));
                    jk = jk.wrapping_add(1);
                });
            },
        );
    }
    group.finish();

    std::env::remove_var("FRS_L0_COMPACTION_TRIGGER");
    std::env::remove_var("FRS_L0_STOP_TRIGGER");
    std::env::remove_var("FRS_L0_SLOWDOWN_TRIGGER");
}

/// APPROACH 1 (shipped, `FRS_RS_LEVELED_HOT_CF`): the per-probe located-SOURCE-
/// COUNT collapse, measured through the REAL stream API (`debug_source_count`).
///
/// The `bench_join_probe_leveled_vs_tiered` group above measures per-probe TIME;
/// this group quantifies the underlying lever directly: the number of tier
/// sources a probe's k-way merge fans out over. `tiered` = today's deep L0 (a
/// probe locates ~`rounds` overlapping SSTs); `leveled` = after the bottom level
/// is collapsed to one non-overlapping run (`compact_range`, the steady state an
/// ARMED hot CF converges to once its tightened L0 trigger rolls the tail down) =
/// a probe locates ≈#levels sources regardless of join duration. This is the
/// "source count leveled vs tiered" number the design's read-amp argument rests
/// on (it should match the KV-sep 69→6 file-count collapse the omnipotent-rethink
/// §1.A measured). Printed once per `rounds`, then a no-op timed body so the group
/// also records that locating ≤few sources is ~flat in `rounds`.
fn bench_join_probe_leveled_source_count(c: &mut Criterion) {
    use std::sync::Mutex;

    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "100000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "100000");

    let deep_keys = 64u32;
    let build_deep = |rounds: u32| -> Arc<DbImpl> {
        let db = open_in_memory(4096);
        let cf = db.default_cf();
        for round in 0..rounds {
            for jk in 0..deep_keys {
                let mut k = [0u8; 8];
                k[..4].copy_from_slice(&jk.to_be_bytes());
                k[4..].copy_from_slice(&round.to_be_bytes());
                db.put(&cf, &k, &[0xCDu8; 64]).expect("put");
            }
            db.switch_and_flush(&cf).expect("switch+flush");
        }
        db
    };
    let prefix4 = |jk: u32| -> [u8; 4] { jk.to_be_bytes() };
    let source_count = |db: &Arc<DbImpl>, prefix: &[u8]| -> usize {
        let cf = db.default_cf();
        let s = db
            .prefix_scan_stream_with_error_slot(&cf, prefix, Arc::new(Mutex::new(None)))
            .expect("open stream");
        s.debug_source_count()
    };

    let mut group = c.benchmark_group("join_probe_leveled_source_count");
    for &rounds in &[8u32, 32, 64, 128] {
        let db_tiered = build_deep(rounds);
        let tiered_n = source_count(&db_tiered, &prefix4(7));

        let db_leveled = build_deep(rounds);
        let _ = db_leveled
            .compact_range(&db_leveled.default_cf())
            .expect("compact_range");
        let leveled_n = source_count(&db_leveled, &prefix4(7));

        eprintln!(
            "APPROACH-1 source-count rounds={rounds:3}: tiered={tiered_n:3} leveled={leveled_n:3} \
             collapse={:.1}x",
            tiered_n as f64 / leveled_n.max(1) as f64
        );

        group.bench_with_input(
            BenchmarkId::from_parameter(format!("leveled_locate_{rounds}")),
            &rounds,
            |b, &_n| {
                b.iter(|| black_box(source_count(&db_leveled, &prefix4(7))));
            },
        );
    }
    group.finish();

    std::env::remove_var("FRS_L0_COMPACTION_TRIGGER");
    std::env::remove_var("FRS_L0_STOP_TRIGGER");
    std::env::remove_var("FRS_L0_SLOWDOWN_TRIGGER");
}

criterion_group!(
    benches,
    bench_join_probe_open,
    bench_join_probe_fill_into,
    bench_join_probe_adaptive,
    bench_join_probe_leveled_vs_tiered,
    bench_join_probe_leveled_source_count
);
criterion_main!(benches);

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

//! MR-1 mini-bench: COLD per-probe reader-open cost for BLOOM-NEGATIVE SSTs,
//! and the realized win of the metadata-resident prefix-bloom prune.
//!
//! Motivation (q7 H1, `2026-06-12-forst-architecture-q7-analysis.md` §3 +
//! `2026-06-15-q7-probe-open-prune-design.md`): on the interval-join probe path,
//! `build_lazy_prefix_key_stream_sel` (db.rs ~10479) currently calls
//! `get_or_open_sst_reader(sst)` — a COLD open that reads + decodes the SST
//! footer / index / prefix-bloom from the filesystem — **BEFORE** the
//! prefix-bloom prune (`reader.may_contain_prefix`) can reject the SST. So every
//! range-overlapping SST that the bloom would reject still pays a full cold open.
//! In the q7 disk-saturated regime (state spilled past the block cache, readers
//! evicted), this is wasted footer/index I/O — the iostat-confirmed read-amp
//! component (q7-analysis §1 marker Q7P32: NVMe 98-99% util, reads + writes
//! saturated).
//!
//! Four arms on the SAME fixture (N range-overlapping SSTs, of which only the
//! `present_*` SSTs actually contain the probed prefix):
//!   * `cold_all` — open ALL N SSTs cold, then drain (today's path: the bloom
//!     rejects the absent ones AFTER paying their cold open). FLAG OFF.
//!   * `cold_hit` — open ONLY the SSTs that contain the prefix cold, then drain
//!     (the MR-1 *ceiling*: a fixture containing only the bloom-positive SSTs).
//!     `cold_all - cold_hit` = the wasted open I/O MR-1 reclaims.
//!   * `mr1_on` — the SAME full `cold_all` fixture, FLAG ON, with the bloom meta
//!     cache primed once (so a later cold probe prunes). This is the REALIZED
//!     win: it must approach `cold_hit` at high N (the prune fires) and not
//!     regress `cold_all` at low N (no shallow tax).
//!   * `warm_all` — all readers cached (no cold open), today's path — the lower
//!     bound that proves the cost IS the cold open, not the merge/drain.
//!
//! In-memory FS removes physical-disk latency, so the measured delta is a
//! CONSERVATIVE lower bound (footer/index/bloom decode CPU only); on a real
//! NVMe the delta is the additional pread I/O — strictly larger.

use std::sync::{Arc, Mutex};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::{DbImpl, FillOutcome, RowSink};

/// Byte-counting no-op sink (no copies, no allocs) — isolates open + merge.
struct CountBytes(u64);
impl RowSink for CountBytes {
    fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.0 += (key.len() + value.len()) as u64;
        true
    }
}

/// 17-byte state key `[join-key BE u64][round BE u64][entry u8]`. The probe uses
/// the FULL 16-byte `[join-key][round]` prefix so the v3 prefix bloom
/// (PREFIX_BLOOM_LEN = 16) actually engages — the MR-1 prune only fires for
/// probe prefixes >= the filter length (the join-key regime).
fn key(jk: u64, round: u64, entry: u8) -> [u8; 17] {
    let mut k = [0u8; 17];
    k[..8].copy_from_slice(&jk.to_be_bytes());
    k[8..16].copy_from_slice(&round.to_be_bytes());
    k[16] = entry;
    k
}

/// The 16-byte probe prefix selecting one (jk, round) target.
fn prefix16(jk: u64, round: u64) -> [u8; 16] {
    let mut p = [0u8; 16];
    p[..8].copy_from_slice(&jk.to_be_bytes());
    p[8..].copy_from_slice(&round.to_be_bytes());
    p
}

/// Builds a deep-fan-out engine of `num_ssts` persisted L0 SSTs. Every SST spans
/// the FULL join-key range (jk=0 and jk=MAX endpoints written in each) so the
/// coarse smallest/largest_key range-skip cannot prune any of them — they are all
/// "overlapping" for the probe. The probed target `(target_jk, round=0)` is
/// written into only the first `present` SSTs, so those are bloom-POSITIVE and the
/// other `num_ssts - present` SSTs are bloom-NEGATIVE for the probe (they hold
/// only the range-boundary keys) — exactly the MR-1 prune target.
fn build(num_ssts: u64, target_jk: u64, present: u64) -> Arc<DbImpl> {
    // Tiny buffer + (env) high compaction triggers => every round is its own SST
    // and they are NOT compacted away (the sustained long-running-join regime).
    let db = open_in_memory(4096);
    let cf = db.default_cf();
    let val = vec![0xCDu8; 64];
    for round in 0..num_ssts {
        // Range endpoints in EVERY SST (force range overlap).
        for e in 0..2u8 {
            db.put(&cf, &key(0, round, e), &val).expect("put lo");
            db.put(&cf, &key(u64::MAX, round, e), &val).expect("put hi");
        }
        // Target only in the first `present` SSTs.
        if round < present {
            for e in 0..3u8 {
                db.put(&cf, &key(target_jk, 0, e), &val)
                    .expect("put target");
            }
        }
        // switch_and_flush forces ONE SST per round (a bare flush_cf only drains
        // the imm queue, letting the tiny rounds accumulate in one memtable).
        db.switch_and_flush(&cf).expect("switch+flush");
    }
    db
}

/// Builds a fixture identical to `build` but containing ONLY the `present`
/// SSTs that hold the target key — the MR-1 "ceiling" where the metadata bloom
/// pruned every absent SST without opening it.
fn build_hit_only(target_jk: u64, present: u64) -> Arc<DbImpl> {
    let db = open_in_memory(4096);
    let cf = db.default_cf();
    let val = vec![0xCDu8; 64];
    for round in 0..present {
        for e in 0..2u8 {
            db.put(&cf, &key(0, round, e), &val).expect("put lo");
            db.put(&cf, &key(u64::MAX, round, e), &val).expect("put hi");
        }
        for e in 0..3u8 {
            db.put(&cf, &key(target_jk, 0, e), &val)
                .expect("put target");
        }
        db.switch_and_flush(&cf).expect("switch+flush");
    }
    db
}

fn drain(db: &Arc<DbImpl>, prefix: &[u8]) -> u64 {
    let cf = db.default_cf();
    let mut stream = db
        .prefix_scan_stream_with_error_slot(&cf, prefix, Arc::new(Mutex::new(None)))
        .expect("open prefix stream");
    let mut sink = CountBytes(0);
    let outcome = stream.fill_into(&mut sink).expect("fill_into");
    assert_eq!(outcome, FillOutcome::Exhausted);
    sink.0
}

fn bench_probe_open_bloom_prune(c: &mut Criterion) {
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "100000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "100000");

    let target_jk = 7u64;
    let prefix = prefix16(target_jk, 0);
    // present = 2: a realistic interval-join where the band matches a handful of
    // SSTs out of a deep L0 — the bloom rejects the rest.
    let present = 2u64;

    let mut group = c.benchmark_group("probe_open_bloom_prune");
    for &num_ssts in &[8u64, 32, 64, 128] {
        let db_all = build(num_ssts, target_jk, present);
        let db_hit = build_hit_only(target_jk, present);

        // cold_all: today's path (FLAG OFF) — open ALL range-overlapping SSTs
        // cold, bloom rejects the absent ones AFTER their cold open.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("cold_all_ssts_{num_ssts}")),
            &num_ssts,
            |b, &_n| {
                std::env::remove_var("FRS_RS_PROBE_BLOOM_PRUNE");
                b.iter(|| {
                    db_all.evict_all_sst_readers();
                    black_box(drain(&db_all, &prefix));
                });
            },
        );

        // cold_hit: MR-1 ceiling — only the bloom-POSITIVE SSTs are opened cold.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("cold_hit_ssts_{num_ssts}")),
            &num_ssts,
            |b, &_n| {
                std::env::remove_var("FRS_RS_PROBE_BLOOM_PRUNE");
                b.iter(|| {
                    db_hit.evict_all_sst_readers();
                    black_box(drain(&db_hit, &prefix));
                });
            },
        );

        // mr1_on: REALIZED win — FLAG ON on the full fixture. Prime the bloom
        // meta cache once (opens + caches every SST's bloom), then per-iter evict
        // readers (cold regime) but KEEP the sticky blooms, so the prune fires and
        // opens only the bloom-positive SSTs.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("mr1_on_ssts_{num_ssts}")),
            &num_ssts,
            |b, &_n| {
                std::env::set_var("FRS_RS_PROBE_BLOOM_PRUNE", "1");
                // Prime the bloom cache (a full open with the flag on populates it).
                black_box(drain(&db_all, &prefix));
                b.iter(|| {
                    db_all.evict_all_sst_readers();
                    black_box(drain(&db_all, &prefix));
                });
                std::env::remove_var("FRS_RS_PROBE_BLOOM_PRUNE");
            },
        );

        // warm_all: readers cached — proves the gap is the cold OPEN, not merge.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("warm_all_ssts_{num_ssts}")),
            &num_ssts,
            |b, &_n| {
                std::env::remove_var("FRS_RS_PROBE_BLOOM_PRUNE");
                // Prime once.
                black_box(drain(&db_all, &prefix));
                b.iter(|| {
                    black_box(drain(&db_all, &prefix));
                });
            },
        );
    }
    group.finish();

    std::env::remove_var("FRS_L0_COMPACTION_TRIGGER");
    std::env::remove_var("FRS_L0_STOP_TRIGGER");
    std::env::remove_var("FRS_L0_SLOWDOWN_TRIGGER");
}

criterion_group!(benches, bench_probe_open_bloom_prune);
criterion_main!(benches);

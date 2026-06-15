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

//! MR-1 mini-bench: COLD per-probe reader-open cost for BLOOM-NEGATIVE SSTs.
//!
//! Motivation (q7 H1, `2026-06-12-forst-architecture-q7-analysis.md` §3 +
//! `2026-06-15-q7-probe-open-prune-design.md`): on the interval-join probe path,
//! `build_lazy_prefix_key_stream` (db.rs:10445-10491) currently calls
//! `get_or_open_sst_reader(sst)` — a COLD open that reads + decodes the SST
//! footer / index / prefix-bloom from the filesystem — **BEFORE** the
//! prefix-bloom prune (`reader.may_contain_prefix`, db.rs:10486) can reject the
//! SST. So every range-overlapping SST that the bloom would reject still pays a
//! full cold open. In the q7 disk-saturated regime (state spilled past the
//! block cache, readers evicted), this is wasted footer/index I/O — the
//! iostat-confirmed read-amp component (q7-analysis §1 marker Q7P32: NVMe 98-99%
//! util, reads + writes saturated).
//!
//! This bench QUANTIFIES that wasted work as a function of the bloom-reject
//! ratio, on the COLD regime (`evict_all_sst_readers` before each probe), so the
//! projected win of an MR-1 metadata-resident prefix-bloom prune (skip the open
//! for bloom-negative SSTs) is measured BEFORE the lever is implemented.
//!
//! Three arms on the SAME fixture (N range-overlapping SSTs, of which only the
//! `present_*` SSTs actually contain the probed prefix):
//!   * `cold_all` — open ALL N SSTs cold, then drain (today's path: the bloom
//!     rejects the absent ones AFTER paying their cold open).
//!   * `cold_hit` — open ONLY the SSTs that contain the prefix cold, then drain
//!     (the MR-1 ceiling: metadata bloom pruned the rest with no open).
//!     `cold_all - cold_hit` = the wasted open I/O MR-1 reclaims.
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

/// State key `[join-key BE][round BE]`. A probe uses the 4-byte join-key prefix.
fn key(jk: u32, round: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[..4].copy_from_slice(&jk.to_be_bytes());
    k[4..].copy_from_slice(&round.to_be_bytes());
    k
}

/// Builds a deep-fan-out engine of `num_ssts` persisted L0 SSTs. Every SST spans
/// the FULL join-key range (so the coarse smallest/largest_key range-skip cannot
/// prune any of them — they are all "overlapping"), BUT for the probed join key
/// `target_jk` only `present` of the SSTs actually contain an entry. The other
/// `num_ssts - present` SSTs contain the range's boundary keys (jk 0 and
/// `num_keys-1`) so they range-overlap the target prefix yet hold NO key for it
/// — exactly the prefix-bloom-NEGATIVE case the MR-1 prune targets.
fn build(num_keys: u32, num_ssts: u32, target_jk: u32, present: u32) -> Arc<DbImpl> {
    // Tiny buffer + (env) high compaction triggers => every round is its own SST
    // and they are NOT compacted away (the sustained long-running-join regime).
    let db = open_in_memory(4096);
    let cf = db.default_cf();
    let val = vec![0xCDu8; 64];
    for round in 0..num_ssts {
        let has_target = round < present;
        for jk in 0..num_keys {
            // Always write the range endpoints so the SST's [smallest,largest]
            // spans the whole key space (range-overlaps every probe).
            let is_endpoint = jk == 0 || jk == num_keys - 1;
            // Only the first `present` SSTs carry the target join key.
            let is_target = jk == target_jk && has_target;
            if is_endpoint || is_target {
                db.put(&cf, &key(jk, round), &val).expect("put");
            }
        }
        db.flush_cf(&cf).expect("flush");
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

/// Builds a fixture identical to `build` but containing ONLY the `present`
/// SSTs that hold the target key — the MR-1 "ceiling" where the metadata bloom
/// pruned every absent SST without opening it.
fn build_hit_only(num_keys: u32, present: u32, target_jk: u32) -> Arc<DbImpl> {
    let db = open_in_memory(4096);
    let cf = db.default_cf();
    let val = vec![0xCDu8; 64];
    for round in 0..present {
        for jk in 0..num_keys {
            let is_endpoint = jk == 0 || jk == num_keys - 1;
            let is_target = jk == target_jk;
            if is_endpoint || is_target {
                db.put(&cf, &key(jk, round), &val).expect("put");
            }
        }
        db.flush_cf(&cf).expect("flush");
    }
    db
}

fn bench_probe_open_bloom_prune(c: &mut Criterion) {
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "100000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "100000");

    let num_keys = 64u32;
    let target_jk = 7u32;
    let prefix = target_jk.to_be_bytes();
    // present = 2: a realistic interval-join where the band matches a handful of
    // SSTs out of a deep L0 — the bloom rejects the rest.
    let present = 2u32;

    let mut group = c.benchmark_group("probe_open_bloom_prune");
    for &num_ssts in &[8u32, 32, 64, 128] {
        let db_all = build(num_keys, num_ssts, target_jk, present);
        let db_hit = build_hit_only(num_keys, present, target_jk);

        // cold_all: today's path — open ALL range-overlapping SSTs cold, bloom
        // rejects the absent ones AFTER their cold open.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("cold_all_ssts_{}", num_ssts)),
            &num_ssts,
            |b, &_n| {
                b.iter(|| {
                    db_all.evict_all_sst_readers();
                    black_box(drain(&db_all, &prefix));
                });
            },
        );

        // cold_hit: MR-1 ceiling — only the bloom-POSITIVE SSTs are opened cold.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("cold_hit_ssts_{}", num_ssts)),
            &num_ssts,
            |b, &_n| {
                b.iter(|| {
                    db_hit.evict_all_sst_readers();
                    black_box(drain(&db_hit, &prefix));
                });
            },
        );

        // warm_all: readers cached — proves the gap is the cold OPEN, not merge.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("warm_all_ssts_{}", num_ssts)),
            &num_ssts,
            |b, &_n| {
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

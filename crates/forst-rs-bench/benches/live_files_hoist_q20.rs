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

//! PMC-1 q20 LIVE-FILES-HOIST mini-bench (`2026-06-18-q20-join-readpath-31pct-decompose.md`).
//!
//! Sizes the per-probe `Version::live_sst_file_numbers()` HashSet build that the
//! prefix-scan-open path paid on EVERY probe even though, with the resident
//! shadow OFF (the default uniform config), its result was discarded. The cost
//! grows O(total live SSTs) — the unbounded q20 join state (~92M entries → many
//! SSTs in steady state) is the regime where it bites (profile
//! `Version::live_sst_file_numbers` 1.42% self).
//!
//! The bench reproduces that regime: `num_ssts` overlapping L0 SSTs (large state),
//! resident shadow OFF, then measures the per-probe prefix-scan-open + full drain.
//! A/B is the db.rs hoist itself (run on the parent commit vs this branch) — the
//! delta at high SST counts is the reclaimed dead work.

use std::sync::{Arc, Mutex};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_bench::open_in_memory;
use forst_rs_engine::{DbImpl, FillOutcome, RowSink};

struct CountBytes(u64);
impl RowSink for CountBytes {
    fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.0 += (key.len() + value.len()) as u64;
        true
    }
}

/// ~100-byte composite key in the q20 no-UK MapState regime:
/// `[join-key BE u64][round BE u64][~84B body]`. The probe prefix is the
/// 16-byte `[join-key][round]` so the v3 prefix bloom engages.
fn key(jk: u64, round: u64, tag: u8) -> Vec<u8> {
    let mut k = Vec::with_capacity(100);
    k.extend_from_slice(&jk.to_be_bytes());
    k.extend_from_slice(&round.to_be_bytes());
    k.extend_from_slice(&[tag; 84]);
    k
}

fn prefix16(jk: u64, round: u64) -> [u8; 16] {
    let mut p = [0u8; 16];
    p[..8].copy_from_slice(&jk.to_be_bytes());
    p[8..].copy_from_slice(&round.to_be_bytes());
    p
}

/// Build `num_ssts` overlapping L0 SSTs (every SST spans the full join-key range
/// so none is range-pruned). The target `(target_jk, round=0)` lives in only the
/// first SST — a realistic shallow per-probe result over a LARGE live-SST set.
fn build(num_ssts: u64, target_jk: u64) -> Arc<DbImpl> {
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "1000000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "1000000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "1000000");
    let db = open_in_memory(4096);
    let cf = db.default_cf();
    for round in 0..num_ssts {
        db.put(&cf, &key(0, round, 1), b"v").expect("put lo");
        db.put(&cf, &key(u64::MAX, round, 1), b"v").expect("put hi");
        if round == 0 {
            for e in 0..3u8 {
                db.put(&cf, &key(target_jk, 0, e), b"v")
                    .expect("put target");
            }
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

fn bench_live_files_hoist(c: &mut Criterion) {
    // Resident shadow OFF (default) — the regime where live_files is dead work.
    std::env::remove_var("FRS_RESIDENT_SHADOW");
    let target_jk = 0x5151_5151_5151_5151u64;
    let mut g = c.benchmark_group("live_files_hoist_q20_probe_open");
    for &num_ssts in &[64u64, 256, 512] {
        let db = build(num_ssts, target_jk);
        let p = prefix16(target_jk, 0);
        // Warm the readers so the measured delta is the per-probe open
        // bookkeeping (incl. live_sst_file_numbers), not cold SST decode.
        let _ = drain(&db, &p);
        g.bench_with_input(BenchmarkId::from_parameter(num_ssts), &num_ssts, |b, _| {
            b.iter(|| black_box(drain(&db, &p)));
        });
    }
    g.finish();
}

criterion_group!(benches, bench_live_files_hoist);
criterion_main!(benches);

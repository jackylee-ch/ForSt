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

//! B3 — `compaction_throughput` (2026-06-12 local FFI/flush/compaction bench
//! design §2-B3): the repeatable reproduction target for the recorded
//! "3 ns/byte isolated vs 21 ns/byte under live load" compaction number
//! (memory: resume 2026-06-05, q4 binder = bandwidth-bound compaction merge).
//!
//! Build phase: write + `switch_and_flush` until L0 holds `--ssts` files of
//! `--sst-mib` each, with controllable cross-file key overlap and tombstone
//! fraction. Measure phase: time `compact_l0`; report ns/byte-live, MB/s,
//! and reclaimed-bytes %. `--with-read-load` runs a sidecar point-get thread
//! at max rate against the same CF during the compaction — the isolated/live
//! pair quantifies compaction↔foreground interference (the L4
//! compaction-windowed + cache-policy lever's before/after harness).
//!
//! Methodology (design §3): real LocalFileSystem I/O on a scratch dir under
//! `target/` (NOT /tmp), deterministic xorshift keys, machine-readable JSON
//! line per cell, ≥1 compaction asserted per measured phase. Mac numbers are
//! system-allocator numbers — same-box A/B only.
//!
//! Run:
//! ```bash
//! cargo run -p forst-rs-bench --release --bin compaction_throughput -- \
//!     [--ssts 8] [--sst-mib 64] [--value-bytes 256] [--overlap-pct 50] \
//!     [--tombstone-pct 0] [--with-read-load] [--smoke]
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;

/// Deterministic xorshift64* generator (same pattern as merge_operator UTs).
struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

struct Args {
    ssts: u32,
    sst_mib: u64,
    value_bytes: usize,
    overlap_pct: u32,
    tombstone_pct: u32,
    with_read_load: bool,
    smoke: bool,
}

fn parse_args() -> Args {
    let mut a = Args {
        ssts: 8,
        sst_mib: 64,
        value_bytes: 256,
        overlap_pct: 50,
        tombstone_pct: 0,
        with_read_load: false,
        smoke: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let mut take = |a: &mut u64| {
            *a = it
                .next()
                .expect("missing value")
                .parse()
                .expect("numeric arg");
        };
        let mut tmp = 0u64;
        match flag.as_str() {
            "--ssts" => {
                take(&mut tmp);
                a.ssts = tmp as u32;
            }
            "--sst-mib" => take(&mut a.sst_mib),
            "--value-bytes" => {
                take(&mut tmp);
                a.value_bytes = tmp as usize;
            }
            "--overlap-pct" => {
                take(&mut tmp);
                a.overlap_pct = tmp as u32;
            }
            "--tombstone-pct" => {
                take(&mut tmp);
                a.tombstone_pct = tmp as u32;
            }
            "--with-read-load" => a.with_read_load = true,
            "--smoke" => a.smoke = true,
            other => panic!("unknown flag: {other}"),
        }
    }
    if a.smoke {
        a.ssts = 4.min(a.ssts);
        a.sst_mib = 4.min(a.sst_mib);
    }
    assert!(a.overlap_pct <= 100 && a.tombstone_pct <= 100);
    a
}

fn rss_mib() -> u64 {
    let pid = std::process::id();
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid.to_string()])
        .output()
        .ok();
    out.and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|kib| kib / 1024)
        .unwrap_or(0)
}

/// Key layout: `[16-byte zero-padded decimal id]`. Overlap control: with
/// `overlap_pct = p`, a fraction p of each file's keys are drawn from a
/// SHARED key range (rewritten by every file → garbage versions) and the
/// rest from a per-file DISJOINT range (live forever).
fn make_key(buf: &mut [u8; 16], id: u64) {
    // Hand-rolled zero-padded decimal, no per-key format! alloc.
    let mut x = id;
    for i in (0..16).rev() {
        buf[i] = b'0' + (x % 10) as u8;
        x /= 10;
    }
}

fn main() {
    // Determinism: background L0→L1 auto-compaction (FRS_L0_COMPACTION_TRIGGER,
    // default 4 — write_controller.rs:93) would drain L0 DURING the build phase,
    // leaving `compact_l0` a nondeterministic (possibly empty) input set. Raise
    // the trigger so the measured job is exactly the `--ssts` files we built.
    // Must happen before `DbImpl::open` reads the controller config.
    if std::env::var("FRS_L0_COMPACTION_TRIGGER").is_err() {
        std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    }
    let args = parse_args();
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target")
        .join(format!("b3-compaction-scratch-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).expect("create scratch dir");
    let db_path = scratch.join("db").to_string_lossy().into_owned();

    println!(
        "# B3 compaction_throughput | ssts={} sst_mib={} value_bytes={} overlap_pct={} \
         tombstone_pct={} read_load={} smoke={}",
        args.ssts,
        args.sst_mib,
        args.value_bytes,
        args.overlap_pct,
        args.tombstone_pct,
        args.with_read_load,
        args.smoke
    );
    println!(
        "# RULE: Mac numbers are system-allocator numbers; same-box A/B regressions only. \
         Linux runs for absolute footprints."
    );

    // Write buffer larger than one SST so only explicit switch_and_flush cuts files.
    let opts = EngineOptions {
        db_path,
        write_buffer_size: (args.sst_mib as usize + 64) * 1024 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open(opts).expect("open local db");
    let cf = db.default_cf();

    // ---- Build phase -------------------------------------------------
    let row_bytes = 16 + args.value_bytes;
    let rows_per_sst = (args.sst_mib * 1024 * 1024) as usize / row_bytes;
    let shared_rows = rows_per_sst * args.overlap_pct as usize / 100;
    // Incompressible value pool: random bytes, per-row random slice — so lz4
    // cannot collapse the SSTs and byte-rate metrics stay honest.
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let mut pool = vec![0u8; args.value_bytes + 65536];
    for chunk in pool.chunks_mut(8) {
        let bytes = rng.next().to_le_bytes();
        let n = chunk.len();
        chunk.copy_from_slice(&bytes[..n]);
    }
    let mut key = [0u8; 16];
    let build_t0 = Instant::now();
    let mut logical_bytes = 0u64;
    let mut tombstones = 0u64;
    for file in 0..args.ssts {
        for r in 0..rows_per_sst {
            let id = if r < shared_rows {
                // Shared range [0, shared_rows) — rewritten by every file.
                (rng.next() as usize % shared_rows.max(1)) as u64
            } else {
                // Disjoint per-file range.
                10_000_000_000 + (file as u64) * rows_per_sst as u64 + r as u64
            };
            make_key(&mut key, id);
            if args.tombstone_pct > 0 && (rng.next() % 100) < args.tombstone_pct as u64 {
                db.delete(&cf, &key).expect("delete");
                tombstones += 1;
                logical_bytes += 16;
            } else {
                let off = (rng.next() % 65536) as usize;
                db.put(&cf, &key, &pool[off..off + args.value_bytes])
                    .expect("put");
                logical_bytes += row_bytes as u64;
            }
        }
        db.switch_and_flush(&cf).expect("flush");
    }
    let build_secs = build_t0.elapsed().as_secs_f64();
    let input_bytes_est = args.ssts as u64 * args.sst_mib * 1024 * 1024;
    println!(
        "# build: {} files x {} rows (tombstones {}) in {:.1}s, RSS {} MiB",
        args.ssts,
        rows_per_sst,
        tombstones,
        build_secs,
        rss_mib()
    );

    // ---- Optional sidecar read load ----------------------------------
    let stop = Arc::new(AtomicBool::new(false));
    let gets_done = Arc::new(AtomicU64::new(0));
    let sidecar = if args.with_read_load {
        let db2 = Arc::clone(&db);
        let cf2 = cf.clone();
        let stop2 = Arc::clone(&stop);
        let gets2 = Arc::clone(&gets_done);
        let shared = shared_rows.max(1) as u64;
        Some(std::thread::spawn(move || {
            let mut rng = XorShift(0xDEAD_BEEF_CAFE_F00D);
            let mut key = [0u8; 16];
            while !stop2.load(Ordering::Relaxed) {
                make_key(&mut key, rng.next() % shared);
                let _ = db2.get(&cf2, &key);
                gets2.fetch_add(1, Ordering::Relaxed);
            }
        }))
    } else {
        None
    };

    // ---- Measure phase ------------------------------------------------
    let t0 = Instant::now();
    let out_meta = db.compact_l0(&cf).expect("compact_l0");
    let secs = t0.elapsed().as_secs_f64();
    assert!(out_meta.is_some(), "compaction must run (design rule 3)");
    stop.store(true, Ordering::Relaxed);
    if let Some(h) = sidecar {
        h.join().expect("sidecar join");
    }

    let out_bytes = out_meta.map(|m| m.file_size).unwrap_or(0);
    let live_bytes = out_bytes.max(1);
    let ns_per_byte_live = secs * 1e9 / live_bytes as f64;
    let mb_per_s_in = input_bytes_est as f64 / 1024.0 / 1024.0 / secs;
    let reclaimed_pct = 100.0 * (1.0 - out_bytes as f64 / input_bytes_est as f64);
    let gets = gets_done.load(Ordering::Relaxed);

    println!(
        "# compact_l0: {:.3}s | out {} MiB | {:.2} ns/byte-live | input {:.0} MB/s | \
         reclaimed {:.1}% | sidecar gets {} ({:.0}/s) | RSS {} MiB",
        secs,
        out_bytes / 1024 / 1024,
        ns_per_byte_live,
        mb_per_s_in,
        reclaimed_pct,
        gets,
        gets as f64 / secs,
        rss_mib()
    );
    println!(
        "{{\"bench\":\"compaction_throughput\",\"ssts\":{},\"sst_mib\":{},\"value_bytes\":{},\
         \"overlap_pct\":{},\"tombstone_pct\":{},\"read_load\":{},\"secs\":{:.3},\
         \"out_bytes\":{},\"ns_per_byte_live\":{:.2},\"input_mb_per_s\":{:.1},\
         \"reclaimed_pct\":{:.1},\"sidecar_gets_per_s\":{:.0},\"logical_bytes\":{}}}",
        args.ssts,
        args.sst_mib,
        args.value_bytes,
        args.overlap_pct,
        args.tombstone_pct,
        args.with_read_load,
        secs,
        out_bytes,
        ns_per_byte_live,
        mb_per_s_in,
        reclaimed_pct,
        gets as f64 / secs,
        logical_bytes
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&scratch);
}

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

//! FRS-PHASE2 pillar 6b — remote-compaction offload mini-bench
//! (`docs/superpowers/specs/2026-06-13-remote-compaction-design.md` §4.2).
//!
//! Builds a fixed compaction (overlapping L0 SSTs over a Put-overwrite
//! workload so the k-way merge does real per-byte CPU), then runs it through
//! the in-process `LocalCompactionExecutor` vs the locally-emulated
//! `RemoteEmulatedCompactionExecutor`, measuring the CALLING (TaskManager)
//! thread's CPU time (`CLOCK_THREAD_CPUTIME_ID`) across `compact_all`.
//!
//! The offload claim: under the Remote executor the CALLING thread does ~0
//! compaction CPU (it blocks on the result channel while a separate pool
//! thread runs the merge), whereas under Local it burns the full merge CPU.
//! Wall time is comparable (the merge bytes are the same; on this dev box the
//! pool thread runs on another core).
//!
//! Methodology (matches `compaction_throughput.rs`): same-session A/B only,
//! n≥`--runs` per cell, never compare across machines; macOS = system
//! allocator. The number that matters is the RATIO of caller-thread CPU
//! (Local ≫ Remote), which is machine-independent in direction.
//!
//! ```bash
//! cargo run -p forst-rs-bench --release --bin remote_compaction_offload -- --smoke
//! cargo run -p forst-rs-bench --release --bin remote_compaction_offload -- --runs 5
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::EngineOptions;
use forst_rs_engine::compaction_executor::{
    LocalCompactionExecutor, RemoteEmulatedCompactionExecutor,
};
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};

/// Calling-thread CPU time in nanoseconds (`CLOCK_THREAD_CPUTIME_ID` — POSIX,
/// works on macOS 10.12+ and Linux). Measures ONLY the current thread's CPU,
/// so time spent blocked on a channel `recv` does NOT count.
fn thread_cpu_nanos() -> u128 {
    // SAFETY: zeroed timespec is a valid POSIX struct; clock_gettime fills it.
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime(CLOCK_THREAD_CPUTIME_ID) failed");
    (ts.tv_sec as u128) * 1_000_000_000 + (ts.tv_nsec as u128)
}

struct Cfg {
    runs: usize,
    keys: usize,
    rounds: usize,
    write_buffer: usize,
}

fn parse_args() -> Cfg {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");
    let get = |flag: &str, default: usize| -> usize {
        args.iter()
            .position(|a| a == flag)
            .and_then(|i| args.get(i + 1))
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    if smoke {
        Cfg {
            runs: 1,
            keys: 5_000,
            rounds: 6,
            // Large buffer so each round flushes ~once ⇒ L0 count ≈ rounds
            // (kept well under the stall triggers; the merge happens in
            // compact_all, not in many mid-round auto-flushes).
            write_buffer: 8 * 1024 * 1024,
        }
    } else {
        Cfg {
            runs: get("--runs", 3),
            keys: get("--keys", 50_000),
            rounds: get("--rounds", 10),
            write_buffer: get("--wbuf", 32 * 1024 * 1024),
        }
    }
}

fn open(dir: &std::path::Path, write_buffer: usize) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: dir.to_string_lossy().into_owned(),
        write_buffer_size: write_buffer,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open engine")
}

/// Write `rounds` overlapping full-keyspace bursts (every key overwritten each
/// round) so compaction must consolidate `rounds` versions per key — real
/// per-byte merge CPU.
fn populate(db: &Arc<DbImpl>, keys: usize, rounds: usize) {
    let cf = db.default_cf();
    for round in 0..rounds {
        for i in 0..keys {
            let key = format!("key_{i:08}");
            let val = format!("val_{i:08}_round_{round:02}_padding_padding_padding");
            db.put(&cf, key.as_bytes(), val.as_bytes()).expect("put");
        }
        // One flush per round ⇒ one L0 SST per round, all overlapping the full
        // keyspace. compact_all then merges `rounds` files into one.
        db.flush_all().expect("flush");
    }
}

struct CellResult {
    wall_ms: f64,
    caller_cpu_ms: f64,
}

static RUN_SEQ: AtomicU64 = AtomicU64::new(0);

fn run_cell(remote: bool, cfg: &Cfg) -> CellResult {
    let seq = RUN_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::path::PathBuf::from("target").join(format!(
        "remote_compaction_offload_bench_{}_{}",
        std::process::id(),
        seq
    ));
    std::fs::create_dir_all(&dir).expect("create run dir");
    let db = open(&dir, cfg.write_buffer);
    if remote {
        db.set_compaction_executor(Arc::new(RemoteEmulatedCompactionExecutor::new()));
    } else {
        db.set_compaction_executor(Arc::new(LocalCompactionExecutor));
    }
    populate(&db, cfg.keys, cfg.rounds);

    // Measure the CALLING thread's CPU + wall across compact_all.
    let cpu0 = thread_cpu_nanos();
    let wall0 = Instant::now();
    db.compact_all().expect("compact_all");
    let wall = wall0.elapsed();
    let cpu = thread_cpu_nanos() - cpu0;

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);

    CellResult {
        wall_ms: wall.as_secs_f64() * 1e3,
        caller_cpu_ms: cpu as f64 / 1e6,
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn main() {
    // Suppress background auto-compaction AND the write-stall triggers so the
    // ENTIRE merge runs inside the measured `compact_all` window (otherwise the
    // flush-triggered bg worker does the merge during populate and compact_all
    // finds a balanced LSM, measuring nothing; and a low stop trigger stalls
    // writes once L0 grows). With all three above the produced L0 count, the
    // only compaction is the explicit `compact_all` — Local runs it on the
    // calling thread, Remote offloads it to the pool. Set before any engine
    // opens (the controller reads these at open).
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "1000000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "1000000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "1000000");

    let cfg = parse_args();
    println!(
        "== remote-compaction offload (pillar 6b) — {} keys × {} rounds, wbuf {} KiB, n={} ==",
        cfg.keys,
        cfg.rounds,
        cfg.write_buffer / 1024,
        cfg.runs
    );
    println!("(caller_cpu = CALLING/TM-thread CPU during compact_all; offload ⇒ ~0)\n");

    for (label, remote) in [("Local   ", false), ("Remote  ", true)] {
        let mut walls = Vec::new();
        let mut cpus = Vec::new();
        for _ in 0..cfg.runs {
            let r = run_cell(remote, &cfg);
            walls.push(r.wall_ms);
            cpus.push(r.caller_cpu_ms);
        }
        println!(
            "{label}  wall_ms(med)={:8.1}   caller_cpu_ms(med)={:8.1}",
            median(walls),
            median(cpus),
        );
    }

    // Headline ratio: caller CPU Local vs Remote.
    let local_cpu = median(
        (0..cfg.runs)
            .map(|_| run_cell(false, &cfg).caller_cpu_ms)
            .collect(),
    );
    let remote_cpu = median(
        (0..cfg.runs)
            .map(|_| run_cell(true, &cfg).caller_cpu_ms)
            .collect(),
    );
    let ratio = if remote_cpu > 0.01 {
        local_cpu / remote_cpu
    } else {
        f64::INFINITY
    };
    println!(
        "\nHEADLINE: caller-thread compaction CPU  Local={:.1} ms  Remote={:.1} ms  \
         => offload cut {:.0}x (TM does ~0 compaction work under Remote)",
        local_cpu, remote_cpu, ratio
    );
}

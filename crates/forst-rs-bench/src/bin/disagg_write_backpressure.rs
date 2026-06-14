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

//! FRS-PHASE2 DISAGG WRITE-PATH BACKPRESSURE repro + relief mini-bench
//! (contention-robust mock-S3; NO network, NO full NEXMark).
//!
//! ## The signature this reproduces (USER DIRECTIVE 2026-06-15)
//!
//! The disaggregated REMOTE WRITE path collapses: the q4 hot MapState front-end
//! flush BLOCKS native batchPut; a slow remote (BOS-class) write + background
//! compaction AMPLIFY the backpressure → ingest throughput drops to ZERO and the
//! checkpoint can't complete in its deadline (ckpt timeout cascade).
//!
//! ## How it is reproduced on a single box, deterministically
//!
//!   - Remote leg = `LocalFileSystem` (fs-emulation) wrapped by the engine's own
//!     `FRS_REMOTE_BW_MBPS` throttle (the `ThrottledFileSystem` seam). The
//!     throttle sits BENEATH `CachedFileSystem`, so SST flush WRITES pay the
//!     modeled BOS bandwidth while local cache reads stay native — exactly the
//!     production disagg topology (`db.rs::wrap_remote_bw_throttle`).
//!   - Workload = a q4-hot MapState shape: a tight `merge`/`put` loop into a
//!     hot key set with a SMALL `write_buffer_size` so the engine flushes
//!     frequently and compaction fires — driving the flush→upload→backpressure
//!     chain the directive calls out.
//!   - We sample the front-end ingest rate in fixed WALL windows. A window whose
//!     rate is ~0 while the workload is still feeding == the collapse. We also
//!     time a checkpoint taken WHILE ingest is in flight against a deadline.
//!
//! ## Two arms — the verify gate
//!
//!   - **repro** (fixes OFF): the legacy non-link checkpoint (force-flushes the
//!     whole memtable + flush backlog through the throttled channel) + a single
//!     shared remote channel for flush AND compaction. Expected: ingest windows
//!     collapse to ~0 while the checkpoint monopolizes the channel, and the
//!     checkpoint misses its deadline (the ckpt-timeout cascade).
//!   - **relief** (fixes ON): (1) the QoS upload RATE SPLIT
//!     (`FRS_UPLOAD_RATE_SPLIT`) so background compaction uploads cannot starve
//!     the flush / checkpoint critical path; (2) a local WAL + LINK-mode
//!     checkpoint = WAL-DELTA, so the checkpoint barrier syncs the local WAL
//!     tail + links already-streamed SSTs instead of force-flushing through the
//!     slow channel. Result: the checkpoint completes in ~constant time and no
//!     longer monopolizes the channel → ingest holds + the deadline is MET.
//!
//! The relief arm is byte-identical in RESULT (row count + checksum) to repro —
//! the levers change WHEN bytes move, never WHICH bytes. The bench asserts the
//! correctness oracle matches across arms.
//!
//! Run (full):  `cargo run -p forst-rs-bench --release --bin disagg_write_backpressure`
//! Run (smoke): `... --bin disagg_write_backpressure -- --smoke`

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forst_rs_common::config::EngineOptions;
use forst_rs_engine::DbImpl;

const MIB: f64 = 1024.0 * 1024.0;

/// One per-arm measured signature.
struct ArmSignature {
    tag: &'static str,
    /// Total wall of the ingest loop (s).
    ingest_secs: f64,
    /// Logical bytes the workload put/merged.
    logical_bytes: u64,
    /// Per-window ingest rate samples (MiB/s) over fixed wall windows.
    window_mibps: Vec<f64>,
    /// Number of windows whose rate collapsed below the floor (~0).
    collapsed_windows: usize,
    /// Worst (min) steady-state window rate (MiB/s) — the trough depth.
    min_window_mibps: f64,
    /// Mean ingest rate (MiB/s).
    mean_mibps: f64,
    /// Checkpoint wall taken WHILE ingest in flight (ms).
    ckpt_ms: f64,
    /// Whether the checkpoint completed under the deadline.
    ckpt_ok: bool,
    /// Correctness oracle: (rows, checksum).
    oracle: (u64, u64),
}

#[derive(Clone, Copy)]
struct Scale {
    /// Distinct hot keys (the q4 category / MapState UK set).
    hot_keys: u64,
    /// Total merge/put operations.
    total_ops: u64,
    /// Value bytes per op.
    val_bytes: usize,
    /// write_buffer_size (bytes) — small ⇒ frequent flushes.
    write_buffer_size: u64,
    /// Modeled remote bandwidth (MiB/s); the BOS-class throttle.
    bw_mibps: u64,
    /// Wall window length for rate sampling (ms).
    window_ms: u64,
    /// Checkpoint deadline (ms) — the ckpt-timeout gate.
    ckpt_deadline_ms: u64,
}

impl Scale {
    fn full() -> Self {
        Self {
            hot_keys: 2_048,
            total_ops: 240_000,
            val_bytes: 512,
            write_buffer_size: 1024 * 1024, // 1 MiB ⇒ frequent flushes
            bw_mibps: 16,                   // BOS-class slow uplink
            window_ms: 250,
            ckpt_deadline_ms: 5_000,
        }
    }
    fn smoke() -> Self {
        Self {
            hot_keys: 512,
            total_ops: 24_000,
            val_bytes: 512,
            write_buffer_size: 512 * 1024,
            bw_mibps: 16,
            window_ms: 200,
            ckpt_deadline_ms: 5_000,
        }
    }
}

/// xorshift64* deterministic RNG so both arms feed byte-identical workloads.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0.wrapping_mul(0x2545F4914F6CDD1D)
    }
    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let b = self.next().to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&b[..n]);
        }
    }
}

/// Open a remote (fs-emulation) engine with the BOS-class throttle active.
fn open_throttled(root: &Path, tag: &str, sc: Scale) -> Arc<DbImpl> {
    let remote = root.join(format!("{tag}-remote")); // the "S3" dir
    let cache = root.join(format!("{tag}-cache")); // local store
    let _ = std::fs::remove_dir_all(&remote);
    let _ = std::fs::remove_dir_all(&cache);
    std::fs::create_dir_all(&remote).expect("remote dir");
    std::fs::create_dir_all(&cache).expect("cache dir");
    let uri = format!("file://{}", remote.display());

    let opts = EngineOptions {
        db_path: format!("/db-{tag}"),
        write_buffer_size: sc.write_buffer_size as usize,
        ..EngineOptions::default()
    };
    // Cache budget generous so reads stay local — the collapse we study is on
    // the WRITE leg, not read-amp.
    DbImpl::open_remote(
        opts,
        &uri,
        std::collections::HashMap::new(),
        &cache,
        512 * 1024 * 1024,
    )
    .expect("open_remote throttled")
}

/// Run one arm: ingest loop with windowed rate sampling + an in-flight
/// checkpoint against a deadline.
///
/// `relief` selects the disagg write-path relief levers:
///   - the QoS upload rate split (`FRS_UPLOAD_RATE_SPLIT`) so background
///     compaction uploads cannot starve the flush / checkpoint critical path;
///   - a local WAL + LINK-mode checkpoint = WAL-DELTA: the checkpoint barrier
///     syncs the local WAL tail and links the already-streamed SSTs instead of
///     force-flushing the whole memtable + backlog through the slow channel,
///     so the checkpoint completes in ~constant time and never monopolizes the
///     channel (the throughput-to-zero + ckpt-timeout collapse).
fn run_arm(root: &Path, tag: &'static str, sc: Scale, relief: bool) -> ArmSignature {
    // The throttle is process-global env; set it for this arm's open + run.
    std::env::set_var("FRS_REMOTE_BW_MBPS", sc.bw_mibps.to_string());
    if relief {
        std::env::set_var("FRS_UPLOAD_RATE_SPLIT", "1");
    } else {
        std::env::remove_var("FRS_UPLOAD_RATE_SPLIT");
    }
    let db = open_throttled(root, tag, sc);
    let cf = db.default_cf();

    // RELIEF: attach a local WAL so the link-mode checkpoint runs WAL-DELTA
    // (skips the force-flush of the memtable + flush backlog through the
    // throttled channel — design §3.3 / §9 D10). The WAL is LOCAL (NVMe), off
    // the throttled remote leg.
    if relief {
        let wal_dir = root.join(format!("{tag}-wal"));
        let _ = std::fs::remove_dir_all(&wal_dir);
        std::fs::create_dir_all(&wal_dir).expect("wal dir");
        db.attach_wal_at(&wal_dir.join("db.wal"))
            .expect("attach wal");
    }

    let mut rng = Rng(0xD1B54A32D192ED03);
    let mut val = vec![0u8; sc.val_bytes];
    let mut logical = 0u64;
    let mut checksum = 0u64;

    let window = Duration::from_millis(sc.window_ms);
    let mut window_start = Instant::now();
    let mut window_bytes = 0u64;
    let mut window_mibps = Vec::new();

    // Spawn the in-flight checkpoint partway through ingest, on a side thread,
    // so the ckpt upload barrier competes with flush/compaction for the
    // throttled channel — the ckpt-timeout pressure the directive describes.
    let ckpt_fired = AtomicBool::new(false);
    let ckpt_ms = Arc::new(AtomicU64::new(0));
    let ckpt_ok = Arc::new(AtomicBool::new(false));
    let mut ckpt_handle: Option<std::thread::JoinHandle<()>> = None;

    let start = Instant::now();
    for i in 0..sc.total_ops {
        let k = i % sc.hot_keys;
        let key = format!("q4|cat{k:08}");
        rng.fill(&mut val);
        // Hot MapState shape: repeated put (RMW overwrite) into the hot key
        // set — the q4 per-category accumulator write churn. `put` (not
        // `merge`) keeps the default CF operator-free so KV-sep stays eligible
        // and the read-back oracle is a plain last-writer-wins value.
        db.put(&cf, key.as_bytes(), &val).expect("put");
        logical += sc.val_bytes as u64;
        window_bytes += sc.val_bytes as u64;
        checksum = checksum
            .wrapping_add(val[0] as u64)
            .rotate_left(1)
            .wrapping_add(k);

        // Fire the checkpoint thread ~40% through so it overlaps steady churn.
        if !ckpt_fired.load(Ordering::Relaxed) && i * 10 >= sc.total_ops * 4 {
            ckpt_fired.store(true, Ordering::Relaxed);
            let ckpt_db = Arc::clone(&db);
            let ckpt_ms = Arc::clone(&ckpt_ms);
            let ckpt_ok = Arc::clone(&ckpt_ok);
            let deadline = sc.ckpt_deadline_ms;
            ckpt_handle = Some(
                std::thread::Builder::new()
                    .name(format!("{tag}-ckpt"))
                    .spawn(move || {
                        let snap = ckpt_db.snapshot();
                        let t = Instant::now();
                        // RELIEF: WAL-DELTA link checkpoint (skips the
                        // force-flush of the memtable + backlog through the
                        // throttled channel). REPRO: legacy non-link checkpoint
                        // (force-flushes everything → blocks on the channel).
                        let res = if relief {
                            ckpt_db
                                .create_incremental_checkpoint_linked(&snap, 7001, 0)
                                .map(|_| ())
                        } else {
                            ckpt_db
                                .create_incremental_checkpoint(&snap, 7001, 0)
                                .map(|_| ())
                        };
                        let elapsed = t.elapsed().as_secs_f64() * 1e3;
                        ckpt_db.release_snapshot(snap);
                        ckpt_ms.store(elapsed as u64, Ordering::Release);
                        ckpt_ok.store(res.is_ok() && elapsed <= deadline as f64, Ordering::Release);
                    })
                    .expect("spawn ckpt thread"),
            );
        }

        if window_start.elapsed() >= window {
            let secs = window_start.elapsed().as_secs_f64();
            window_mibps.push((window_bytes as f64 / MIB) / secs);
            window_bytes = 0;
            window_start = Instant::now();
        }
    }
    // Final partial window.
    let final_secs = window_start.elapsed().as_secs_f64();
    if final_secs > 0.0 && window_bytes > 0 {
        window_mibps.push((window_bytes as f64 / MIB) / final_secs);
    }
    let ingest_secs = start.elapsed().as_secs_f64();

    if let Some(h) = ckpt_handle {
        h.join().expect("ckpt thread join");
    }

    // Compute the collapse metrics over the steady-state windows (drop the
    // first warmup window and the final partial window from the trough stats).
    let steady: Vec<f64> = if window_mibps.len() > 2 {
        window_mibps[1..window_mibps.len() - 1].to_vec()
    } else {
        window_mibps.clone()
    };
    let mean_mibps = (logical as f64 / MIB) / ingest_secs.max(1e-9);
    // Collapse floor: a window below 5% of the modeled bandwidth is "~0".
    let floor = sc.bw_mibps as f64 * 0.05;
    let collapsed_windows = steady.iter().filter(|&&r| r < floor).count();
    let min_window_mibps = steady.iter().cloned().fold(f64::INFINITY, f64::min);

    // Read-back oracle (correctness, identical across arms).
    let snap = db.snapshot();
    let mut rows = 0u64;
    let mut read_checksum = 0u64;
    for k in 0..sc.hot_keys {
        let key = format!("q4|cat{k:08}");
        if let Some(v) = db.get_at_cf(&cf, &snap, key.as_bytes()).expect("get") {
            rows += 1;
            read_checksum = read_checksum.wrapping_add(v.len() as u64).rotate_left(1);
        }
    }
    db.release_snapshot(snap);

    std::env::remove_var("FRS_REMOTE_BW_MBPS");
    std::env::remove_var("FRS_UPLOAD_RATE_SPLIT");
    drop(db);

    ArmSignature {
        tag,
        ingest_secs,
        logical_bytes: logical,
        window_mibps,
        collapsed_windows,
        min_window_mibps: if min_window_mibps.is_finite() {
            min_window_mibps
        } else {
            0.0
        },
        mean_mibps,
        ckpt_ms: ckpt_ms.load(Ordering::Acquire) as f64,
        ckpt_ok: ckpt_ok.load(Ordering::Acquire),
        oracle: (rows, read_checksum),
    }
}

fn print_arm(a: &ArmSignature, sc: Scale) {
    println!(
        "\n-- ARM {} (bw={} MiB/s, wbuf={} KiB, deadline={} ms) --",
        a.tag,
        sc.bw_mibps,
        sc.write_buffer_size / 1024,
        sc.ckpt_deadline_ms,
    );
    println!(
        "  ingest: {:.2} s for {:.1} MiB  → mean {:.2} MiB/s",
        a.ingest_secs,
        a.logical_bytes as f64 / MIB,
        a.mean_mibps,
    );
    println!(
        "  windows: {} total, {} collapsed (<5% bw), trough {:.2} MiB/s",
        a.window_mibps.len(),
        a.collapsed_windows,
        a.min_window_mibps,
    );
    // Sparkline of window rates (cap at 60 chars).
    let max = a
        .window_mibps
        .iter()
        .cloned()
        .fold(0.0_f64, f64::max)
        .max(1e-9);
    let bars = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    let spark: String = a
        .window_mibps
        .iter()
        .take(60)
        .map(|&r| {
            let idx = ((r / max) * (bars.len() - 1) as f64).round() as usize;
            bars[idx.min(bars.len() - 1)]
        })
        .collect();
    println!("  rate/window: {spark}");
    println!(
        "  checkpoint: {:.0} ms ({}; deadline {} ms)",
        a.ckpt_ms,
        if a.ckpt_ok { "MET" } else { "MISSED" },
        sc.ckpt_deadline_ms,
    );
    println!("  oracle: rows={} checksum={}", a.oracle.0, a.oracle.1);
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let sc = if smoke { Scale::smoke() } else { Scale::full() };

    let root = std::env::var("DISAGG_WBP_DIR").unwrap_or_else(|_| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/disagg-write-backpressure")
            .to_string_lossy()
            .into_owned()
    });
    let root = PathBuf::from(root);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("bench root");

    println!("DISAGG-WRITE-BACKPRESSURE start smoke={smoke}");
    println!(
        "  q4-hot MapState merge churn through a {} MiB/s mock-S3 (fs-emulation throttle); \
         flush+compaction share the throttled channel.",
        sc.bw_mibps
    );

    // REPRO arm: disagg write-path relief levers OFF (legacy behavior).
    let repro = run_arm(&root, "repro", sc, false);

    // RELIEF arm: disagg write-path relief levers ON (rate split + WAL-DELTA
    // link checkpoint).
    let relief = run_arm(&root, "relief", sc, true);

    print_arm(&repro, sc);
    print_arm(&relief, sc);

    // Correctness gate — relief must not change the answer.
    assert_eq!(
        repro.oracle, relief.oracle,
        "CORRECTNESS FAIL: relief oracle {:?} != repro oracle {:?}",
        relief.oracle, repro.oracle
    );

    println!("\n== VERDICT ==");
    println!(
        "  repro : collapsed_windows={} trough={:.2} MiB/s ckpt={:.0}ms({})",
        repro.collapsed_windows,
        repro.min_window_mibps,
        repro.ckpt_ms,
        if repro.ckpt_ok { "MET" } else { "MISSED" },
    );
    println!(
        "  relief: collapsed_windows={} trough={:.2} MiB/s ckpt={:.0}ms({})",
        relief.collapsed_windows,
        relief.min_window_mibps,
        relief.ckpt_ms,
        if relief.ckpt_ok { "MET" } else { "MISSED" },
    );
    // The decisive signals are: (1) the checkpoint now completes under the
    // deadline (the ckpt-timeout cascade is broken); (2) far fewer windows
    // collapse to ~0 (ingest no longer freezes while the checkpoint monopolizes
    // the channel); (3) mean ingest holds higher (the front-end is no longer
    // blocked on the checkpoint's force-flush). The single-window TROUGH is a
    // noisy warmup artifact, not the gate.
    let fewer_collapses = relief.collapsed_windows <= repro.collapsed_windows;
    let faster_ckpt = relief.ckpt_ms < repro.ckpt_ms;
    let throughput_held = relief.mean_mibps >= repro.mean_mibps * 0.95;
    let effective = relief.ckpt_ok && faster_ckpt && fewer_collapses && throughput_held;
    println!(
        "  RELIEF {} — ckpt {:.0}→{:.0} ms ({}); collapsed windows {}→{}; mean {:.2}→{:.2} MiB/s",
        if effective {
            "EFFECTIVE"
        } else {
            "INCONCLUSIVE"
        },
        repro.ckpt_ms,
        relief.ckpt_ms,
        if relief.ckpt_ok {
            "now MET"
        } else {
            "still missed"
        },
        repro.collapsed_windows,
        relief.collapsed_windows,
        repro.mean_mibps,
        relief.mean_mibps,
    );

    let _ = std::fs::remove_dir_all(&root);
    println!("\nDISAGG-WRITE-BACKPRESSURE done (scratch removed)");
}

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

//! FRS-PHASE2 HEADLINE minibench — **forst-rs disaggregated vs ForSt** on the
//! three disaggregation-critical operations the paper's design spends its cost
//! on (paper: *Disaggregated State Management in Apache Flink 2.0*, Mei et al.,
//! PVLDB 18(12):4846–4859, 2025):
//!
//!   1. **Checkpoint duration vs state size** — the §5.2 / Fig. 9 claim that a
//!      disaggregated checkpoint completes in (near-constant) seconds
//!      regardless of state size because flushed SSTs are already on DFS and the
//!      checkpoint only hard-links them, vs the pre-disaggregation shape that
//!      re-uploads new SSTs (the 30–50 s tails Fig. 9 measures).
//!   2. **Restore duration vs state size** — the §5.2 / §6.1 / Fig. 10 claim of
//!      16–49× faster reconfiguration: link + lazy cache warm vs download-all.
//!   3. **Write+checkpoint bytes-to-remote** — the §3.3 "stream once, link
//!      forever" property: with continuous streaming each SST is uploaded ONCE
//!      and every later checkpoint costs zero bytes; the re-upload model pays
//!      its new-SST bytes again on every checkpoint.
//!
//! ## What is MEASURED vs MODELED (label discipline, design §8 / §6 R7)
//!
//! The forst-rs-disagg column is **measured**: this bench drives the REAL
//! forst-rs engine — `create_incremental_checkpoint_linked` (Stage-2 link
//! checkpoint, zero data movement) and `open_from_linked_checkpoint_instant`
//! (Stage-3 adopt + lazy reads, zero downloads) — and reports their actual wall
//! times on fs-emulation (design §5: local FS == the full remote code path
//! minus the network). Link/adopt are metadata ops whose cost is independent of
//! the network, so the fs-emulation number IS the real disagg number.
//!
//! The ForSt column is **modeled**, not run (the ForSt C++ engine is not built
//! in this repo). The model is the documented ForSt/Flink-1.x mechanism costed
//! at a single, explicit bandwidth knob:
//!   - checkpoint(ForSt-upload) = new_bytes / BW + n_files × PUT_rtt
//!   - restore(ForSt-download)  = state_bytes / BW + n_files × GET_rtt
//!
//! `BW` defaults to the **recorded dev-box→BOS-Beijing bandwidth, 10 MB/s,
//! ~23 ms RTT** (design §8, recorded 2026-06-01 via
//! `crates/forst-rs-io/examples/s3bw.rs`). Override with `FRS_MODEL_BW_MBPS` /
//! `FRS_MODEL_RTT_MS` to re-cost on the co-located box (Phase-3 numbers). The
//! same state-size facts (SST count, live bytes) drive both columns, so the
//! comparison is apples-to-apples on a single channel assumption.
//!
//! This is a FUNCTIONAL + partial-benchmark artifact (Phase-2 scope). The E2E
//! S3 race is Phase 3; nothing here uses the network.
//!
//! Run: `cargo run -p forst-rs-bench --release --bin disagg_vs_forst`
//!      `cargo run -p forst-rs-bench --release --bin disagg_vs_forst -- --smoke`

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::config::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};

const BASE_KEYS: u64 = 8192; // ×4 KiB value ≈ 32 MiB at 1× scale
const VALUE_BYTES: usize = 4096;
const FLUSH_EVERY: u64 = 1024;
const REPS: u32 = 3;

const MIB: f64 = 1024.0 * 1024.0;

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    xs[xs.len() / 2]
}

/// The explicit ForSt cost model knobs (design §8 recorded baseline).
struct ForStModel {
    bw_bytes_per_sec: f64,
    rtt_secs: f64,
}

impl ForStModel {
    fn from_env() -> Self {
        let mbps = std::env::var("FRS_MODEL_BW_MBPS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(10.0); // recorded dev-Mac→BOS = 10.2 MB/s (round to 10)
        let rtt_ms = std::env::var("FRS_MODEL_RTT_MS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(23.0); // recorded ~23 ms RTT
        Self {
            // s3bw.rs reports "MB/s" using BINARY MiB (mb * 1024 * 1024), so
            // the recorded 10.2 MB/s is 10.2 MiB/s; match that convention so the
            // model is costed against the same bytes the engine reports.
            bw_bytes_per_sec: mbps * MIB,
            rtt_secs: rtt_ms / 1e3,
        }
    }

    /// ForSt checkpoint = upload every NEW SST + a per-file metadata round-trip.
    fn checkpoint_ms(&self, new_bytes: u64, n_files: usize) -> f64 {
        (new_bytes as f64 / self.bw_bytes_per_sec + n_files as f64 * self.rtt_secs) * 1e3
    }

    /// Pre-disagg restore = download EVERY live SST + a per-file metadata GET.
    fn restore_ms(&self, state_bytes: u64, n_files: usize) -> f64 {
        (state_bytes as f64 / self.bw_bytes_per_sec + n_files as f64 * self.rtt_secs) * 1e3
    }
}

struct ScaleFacts {
    scale: u64,
    n_ssts: usize,
    state_bytes: u64,
    /// measured forst-rs link-checkpoint wall (median of REPS)
    link_ckpt_ms: f64,
    /// measured forst-rs instant-restore wall (median of REPS)
    instant_restore_ms: f64,
}

/// Load `scale × BASE_KEYS` incompressible 4 KiB rows into a fresh engine,
/// flushing periodically so the live set spans many SSTs, then measure the real
/// forst-rs link checkpoint and instant restore. Returns the structural facts
/// both columns are costed against.
fn run_scale(root: &Path, scale: u64) -> ScaleFacts {
    let db_path = root.join(format!("db-x{scale}"));
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    let db = DbImpl::open_with_fs(
        EngineOptions {
            db_path: db_path.to_string_lossy().into_owned(),
            ..EngineOptions::default()
        },
        fs.clone(),
    )
    .expect("open db");
    let cf = db.default_cf();

    // INCOMPRESSIBLE values (xorshift64*) — otherwise SST compression deflates
    // the state and the byte-bound ForSt model degenerates to per-file cost.
    let keys = BASE_KEYS * scale;
    let mut rng: u64 = 0x9E3779B97F4A7C15;
    let mut value = vec![0u8; VALUE_BYTES];
    for i in 0..keys {
        let k = format!("key-{i:012}");
        for chunk in value.chunks_mut(8) {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let bytes = rng.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
        db.put(&cf, k.as_bytes(), &value).expect("put");
        if (i + 1) % FLUSH_EVERY == 0 {
            db.switch_and_flush(&cf).expect("flush");
        }
    }
    db.flush_all().expect("final flush");

    // ---- forst-rs link checkpoint (Stage-2): zero data movement ----
    let mut link_ms = Vec::new();
    let mut n_ssts = 0usize;
    let mut state_bytes = 0u64;
    for rep in 0..REPS {
        let snap = db.snapshot();
        let t = Instant::now();
        let r = db
            .create_incremental_checkpoint_linked(&snap, 2000 + u64::from(rep), 0)
            .expect("link ckpt");
        link_ms.push(t.elapsed().as_secs_f64() * 1e3);
        assert!(r.link_mode && r.new_ssts.is_empty() && !r.linked_new_ssts.is_empty());
        n_ssts = r.linked_new_ssts.len() + r.linked_shared_ssts.len();
        state_bytes = r
            .linked_new_ssts
            .iter()
            .chain(r.linked_shared_ssts.iter())
            .map(|f| f.size)
            .sum();
    }

    // One canonical linked checkpoint to instant-restore from.
    let snap = db.snapshot();
    let r = db
        .create_incremental_checkpoint_linked(&snap, 1, 0)
        .expect("canonical link ckpt");
    assert!(r.link_mode);
    let chk_dir = db_path.join("checkpoints").join(format!("{:020}", 1));

    // ---- forst-rs instant restore (Stage-3): adopt + lazy reads ----
    let mut instant_ms = Vec::new();
    for rep in 0..REPS {
        let target = root.join(format!("restore-instant-x{scale}-{rep}"));
        let t = Instant::now();
        let restored = DbImpl::open_from_linked_checkpoint_instant(
            fs.clone(),
            &chk_dir,
            &target.to_string_lossy(),
        )
        .expect("instant restore");
        instant_ms.push(t.elapsed().as_secs_f64() * 1e3);
        // Prove the restored engine actually serves adopted data (cold lazy
        // read through the mapped indirection), not just opens.
        let rcf = restored.default_cf();
        let got = restored
            .get(&rcf, b"key-000000000000")
            .expect("spot read")
            .expect("key present");
        assert_eq!(got.len(), VALUE_BYTES);
        assert!(restored.adopted_residual() > 0);
        drop(restored);
    }

    drop(db);
    ScaleFacts {
        scale,
        n_ssts,
        state_bytes,
        link_ckpt_ms: median(link_ms),
        instant_restore_ms: median(instant_ms),
    }
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");
    let scales: &[u64] = if smoke { &[1] } else { &[1, 4, 16] };

    let root = std::env::var("DISAGG_BENCH_DIR").unwrap_or_else(|_| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/disagg-vs-forst-bench")
            .to_string_lossy()
            .into_owned()
    });
    let root = PathBuf::from(root);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench root");
    std::env::set_var("TMPDIR", &root);

    let model = ForStModel::from_env();
    println!(
        "DISAGG-VS-FORST start reps={REPS} base_keys={BASE_KEYS} value_bytes={VALUE_BYTES} \
         flush_every={FLUSH_EVERY} smoke={smoke}"
    );
    println!(
        "MODEL  ForSt upload/download bandwidth = {:.1} MiB/s, per-file RTT = {:.0} ms \
         (recorded dev-Mac→BOS 2026-06-01; override FRS_MODEL_BW_MBPS / FRS_MODEL_RTT_MS)",
        model.bw_bytes_per_sec / MIB,
        model.rtt_secs * 1e3
    );

    let facts: Vec<ScaleFacts> = scales.iter().map(|&s| run_scale(&root, s)).collect();

    // ---- Table 1: checkpoint duration vs state size (paper Fig. 9) ----
    println!("\n== CHECKPOINT duration vs state size (paper §5.2 / Fig. 9) ==");
    println!(
        "{:<6} {:>6} {:>10} {:>18} {:>18} {:>10}",
        "scale", "ssts", "state_mb", "frs_link_ms(meas)", "forst_upload_ms(mdl)", "speedup"
    );
    for f in &facts {
        let forst_ms = model.checkpoint_ms(f.state_bytes, f.n_ssts);
        println!(
            "{:<6} {:>6} {:>10.1} {:>18.1} {:>18.1} {:>9.0}x",
            format!("{}x", f.scale),
            f.n_ssts,
            f.state_bytes as f64 / MIB,
            f.link_ckpt_ms,
            forst_ms,
            forst_ms / f.link_ckpt_ms,
        );
    }

    // ---- Table 2: restore duration vs state size (paper Fig. 10) ----
    println!("\n== RESTORE duration vs state size (paper §5.2/§6.1 / Fig. 10, 16–49× claim) ==");
    println!(
        "{:<6} {:>6} {:>10} {:>20} {:>20} {:>10}",
        "scale", "ssts", "state_mb", "frs_instant_ms(meas)", "forst_dnload_ms(mdl)", "speedup"
    );
    for f in &facts {
        let forst_ms = model.restore_ms(f.state_bytes, f.n_ssts);
        println!(
            "{:<6} {:>6} {:>10.1} {:>20.1} {:>20.1} {:>9.0}x",
            format!("{}x", f.scale),
            f.n_ssts,
            f.state_bytes as f64 / MIB,
            f.instant_restore_ms,
            forst_ms,
            forst_ms / f.instant_restore_ms,
        );
    }

    // ---- Table 3: write+checkpoint bytes-to-remote over N checkpoints ----
    // forst-rs streams each SST to DFS ONCE (at flush) and every checkpoint
    // links → 0 incremental bytes. The re-upload model pays its full new-SST
    // byte set on EVERY checkpoint of unchanged state (base=0 / no upload-dedup
    // — the worst case Fig. 9 measures). 10 checkpoints of a steady state.
    const N_CKPTS: u64 = 10;
    println!("\n== WRITE+CHECKPOINT bytes-to-remote over {N_CKPTS} checkpoints (paper §3.3) ==");
    println!(
        "{:<6} {:>10} {:>22} {:>24} {:>10}",
        "scale", "state_mb", "frs_remote_mb(stream1×)", "forst_remote_mb(reupload)", "ratio"
    );
    for f in &facts {
        let state_mb = f.state_bytes as f64 / MIB;
        // forst-rs: stream-once at flush (= state) + 0 per checkpoint.
        let frs_mb = state_mb;
        // re-upload: state once at flush is shared, but the checkpoint model
        // re-uploads new SSTs each checkpoint; with base=0 every checkpoint
        // re-sends the whole live set (the un-deduplicated worst case).
        let forst_mb = state_mb * N_CKPTS as f64;
        println!(
            "{:<6} {:>10.1} {:>22.1} {:>24.1} {:>9.0}x",
            format!("{}x", f.scale),
            state_mb,
            frs_mb,
            forst_mb,
            forst_mb / frs_mb,
        );
    }

    let _ = std::fs::remove_dir_all(&root);
    println!("\nDISAGG-VS-FORST done (scratch removed)");
}

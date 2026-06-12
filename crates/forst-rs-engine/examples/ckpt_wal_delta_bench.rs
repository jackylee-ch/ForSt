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

//! FRS-PHASE2-S4/C2U3 partial bench: per-checkpoint cost in both link-mode
//! durability modes — FLUSH-on-barrier vs WAL-DELTA — across UNFLUSHED state
//! size, for BOTH the first checkpoint (cold: every byte still needs its
//! one-time capture/flush) and the **second checkpoint after a small fixed
//! tail** (steady state: the Phase-5 sealed-segment claim is that THIS cost
//! is flat — the prior tail was sealed + re-homed by checkpoint 1 and is
//! only LINKED by checkpoint 2; v1 re-copied the whole accumulated tail
//! every time).
//!
//! Method: per scale ∈ {1×, 4×, 16×}, REPS fresh DBs per cell on the LOCAL
//! filesystem; load `scale × 1024` keys with 4 KiB incompressible values
//! (≈ scale × 4 MiB) WITHOUT flushing; measure ckpt-1; append a FIXED
//! 64-key (256 KiB) tail; measure ckpt-2. WAL cell attaches per-DB via
//! `attach_wal_at` (the supported env-free route).
//!
//! Run: `cargo run -p forst-rs-engine --release --example ckpt_wal_delta_bench`

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::config::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};

const BASE_KEYS: u64 = 1024;
const VALUE_BYTES: usize = 4096;
const SECOND_TAIL_KEYS: u64 = 64;
const REPS: u32 = 3;

fn median_ms(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    xs[xs.len() / 2]
}

fn fill(db: &Arc<DbImpl>, prefix: &str, start: u64, count: u64, rep: u32) {
    let cf = db.default_cf();
    let mut rng: u64 = 0x9E3779B97F4A7C15 ^ u64::from(rep) ^ start;
    let mut value = vec![0u8; VALUE_BYTES];
    for i in start..start + count {
        let k = format!("{prefix}-{i:012}");
        for chunk in value.chunks_mut(8) {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let bytes = rng.wrapping_mul(0x2545F4914F6CDD1D).to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&bytes[..n]);
        }
        db.put(&cf, k.as_bytes(), &value).expect("put");
    }
}

/// Returns (ckpt1_ms, ckpt2_ms) medians for one cell.
fn run_cell(root: &Path, label: &str, scale: u64, wal: bool) -> (f64, f64) {
    let mut t1 = Vec::new();
    let mut t2 = Vec::new();
    for rep in 0..REPS {
        let db_path = root.join(format!("db-{label}-x{scale}-{rep}"));
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        let db = DbImpl::open_with_fs(
            EngineOptions {
                db_path: db_path.to_string_lossy().into_owned(),
                ..EngineOptions::default()
            },
            fs,
        )
        .expect("open db");
        if wal {
            let wal_dir = root.join(format!("wal-{label}-x{scale}-{rep}"));
            std::fs::create_dir_all(&wal_dir).expect("wal dir");
            db.attach_wal_at(&wal_dir.join("db.wal")).expect("attach");
        }

        // Cold tail: scale × 4 MiB unflushed.
        fill(&db, "key", 0, BASE_KEYS * scale, rep);
        let snap = db.snapshot();
        let t = Instant::now();
        let r = db
            .create_incremental_checkpoint_linked(&snap, 1, 0)
            .expect("linked ckpt 1");
        t1.push(t.elapsed().as_secs_f64() * 1e3);
        assert!(r.link_mode);

        // Steady state: FIXED small tail, then checkpoint 2. Phase-5 claim:
        // this cost is flat in `scale` for WAL-DELTA (prior segment LINKED,
        // only the 256 KiB tail is sealed + re-homed).
        fill(&db, "tail", 0, SECOND_TAIL_KEYS, rep);
        let snap2 = db.snapshot();
        let t = Instant::now();
        let r2 = db
            .create_incremental_checkpoint_linked(&snap2, 2, 1)
            .expect("linked ckpt 2");
        t2.push(t.elapsed().as_secs_f64() * 1e3);
        assert!(r2.link_mode);
        drop(db);
    }
    (median_ms(t1), median_ms(t2))
}

fn main() {
    let root = std::env::var("WALDELTA_BENCH_DIR").unwrap_or_else(|_| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/wal-delta-bench")
            .to_string_lossy()
            .into_owned()
    });
    let root = PathBuf::from(root);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench root");
    std::env::set_var("TMPDIR", &root);

    println!(
        "WALDELTABENCH start reps={REPS} base_keys={BASE_KEYS} value_bytes={VALUE_BYTES} \
         second_tail_keys={SECOND_TAIL_KEYS} root={}",
        root.display()
    );
    println!(
        "{:<6} {:>10} {:>13} {:>13} {:>11} {:>11}",
        "scale", "state_mb", "flush_ck1_ms", "flush_ck2_ms", "wal_ck1_ms", "wal_ck2_ms"
    );

    for scale in [1u64, 4, 16] {
        let mem_mb = (BASE_KEYS * scale) as f64 * VALUE_BYTES as f64 / (1024.0 * 1024.0);
        let (f1, f2) = run_cell(&root, "flush", scale, false);
        let (w1, w2) = run_cell(&root, "waldelta", scale, true);
        println!(
            "{:<6} {:>10.1} {:>13.1} {:>13.1} {:>11.1} {:>11.1}",
            format!("{scale}x"),
            mem_mb,
            f1,
            f2,
            w1,
            w2,
        );
    }

    let _ = std::fs::remove_dir_all(&root);
    println!("WALDELTABENCH done (scratch removed)");
}

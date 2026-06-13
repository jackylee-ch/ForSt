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

//! FRS-PHASE2-S2 partial bench (design §5 Stage-2): checkpoint duration vs
//! state size — LINK mode must be ~flat (metadata-bound) while COPY mode is
//! linear in state bytes (the paper's Fig. 9 claim, reproduced on
//! fs-emulation).
//!
//! Method: per scale ∈ {1×, 4×, 16×}, load `scale × 8192` keys with 4 KiB
//! values (≈ scale × 32 MiB) into a fresh DB on the LOCAL filesystem
//! (fs-emulation per design §5 — full engine code path minus network), flush
//! every 1024 keys so the live set holds many SSTs, then take 3 checkpoints
//! in COPY mode (`create_incremental_checkpoint`, base 0 ⇒ every SST staged,
//! today's full-upload shape) and 3 in LINK mode
//! (`create_incremental_checkpoint_linked`, base 0 ⇒ every SST linked,
//! zero data movement). Report the median of 3 per cell.
//!
//! Scratch lives under `target/ckpt-flat-bench` (override: CKPT_BENCH_DIR);
//! TMPDIR is pointed there too so copy-mode staging stays off /tmp.
//!
//! Run: `cargo run -p forst-rs-engine --release --example ckpt_link_flat_bench`

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::config::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};

const BASE_KEYS: u64 = 8192;
const VALUE_BYTES: usize = 4096;
const FLUSH_EVERY: u64 = 1024;
const REPS: u32 = 3;

fn median_ms(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    xs[xs.len() / 2]
}

fn main() {
    let root = std::env::var("CKPT_BENCH_DIR").unwrap_or_else(|_| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/ckpt-flat-bench")
            .to_string_lossy()
            .into_owned()
    });
    let root = PathBuf::from(root);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench root");
    // Keep copy-mode staging (std::env::temp_dir) inside the bench root.
    std::env::set_var("TMPDIR", &root);

    println!(
        "CKPTBENCH start reps={REPS} base_keys={BASE_KEYS} value_bytes={VALUE_BYTES} \
         flush_every={FLUSH_EVERY} root={}",
        root.display()
    );
    println!(
        "{:<6} {:>6} {:>10} {:>16} {:>16} {:>9}",
        "scale", "ssts", "state_mb", "copy_median_ms", "link_median_ms", "ratio"
    );

    for scale in [1u64, 4, 16] {
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

        // Load: deterministic keys, INCOMPRESSIBLE values (xorshift64* —
        // otherwise SST compression deflates the state and copy-mode cost
        // degenerates to per-file overhead), periodic flush so the live set
        // spans many SSTs.
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

        // COPY mode (today's path): base 0 ⇒ every live SST staged per rep.
        let mut copy_ms = Vec::new();
        let mut n_ssts = 0usize;
        let mut state_bytes = 0u64;
        for rep in 0..REPS {
            let snap = db.snapshot();
            let t = Instant::now();
            let r = db
                .create_incremental_checkpoint(&snap, 1000 + u64::from(rep), 0)
                .expect("copy ckpt");
            copy_ms.push(t.elapsed().as_secs_f64() * 1e3);
            assert!(!r.link_mode && !r.new_ssts.is_empty());
            n_ssts = r.new_ssts.len() + r.shared_ssts.len();
            state_bytes = r
                .new_ssts
                .iter()
                .chain(r.shared_ssts.iter())
                .map(|f| f.size)
                .sum();
        }

        // LINK mode: base 0 ⇒ every live SST linked; zero data movement.
        let mut link_ms = Vec::new();
        for rep in 0..REPS {
            let snap = db.snapshot();
            let t = Instant::now();
            let r = db
                .create_incremental_checkpoint_linked(&snap, 2000 + u64::from(rep), 0)
                .expect("link ckpt");
            link_ms.push(t.elapsed().as_secs_f64() * 1e3);
            assert!(r.link_mode && r.new_ssts.is_empty() && !r.linked_new_ssts.is_empty());
        }

        let copy_med = median_ms(copy_ms.clone());
        let link_med = median_ms(link_ms.clone());
        println!(
            "{:<6} {:>6} {:>10.1} {:>16.1} {:>16.1} {:>8.0}x  copy_raw={:?} link_raw={:?}",
            format!("{scale}x"),
            n_ssts,
            state_bytes as f64 / (1024.0 * 1024.0),
            copy_med,
            link_med,
            copy_med / link_med,
            copy_ms
                .iter()
                .map(|x| (x * 10.0).round() / 10.0)
                .collect::<Vec<_>>(),
            link_ms
                .iter()
                .map(|x| (x * 10.0).round() / 10.0)
                .collect::<Vec<_>>(),
        );
        drop(db);
    }

    let _ = std::fs::remove_dir_all(&root);
    println!("CKPTBENCH done (scratch removed)");
}

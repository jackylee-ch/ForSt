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

//! FRS-PHASE2-S3 partial bench (design §5 Stage-3): restore wall-time vs
//! state size — INSTANT-link restore (`open_from_linked_checkpoint_instant`,
//! adopt + lazy reads, zero copies) must be ~flat (metadata-bound, the
//! paper's Fig. 10 shape) while the COPY restore
//! (`open_from_linked_checkpoint`, materialize-by-copy = the download-restore
//! stand-in) is linear in state bytes.
//!
//! Method: per scale ∈ {1×, 4×, 16×}, load `scale × 8192` keys with 4 KiB
//! incompressible values (≈ scale × 32 MiB) on the LOCAL filesystem
//! (fs-emulation per design §5), flush every 1024 keys, take ONE linked
//! checkpoint, then restore from it 3× per mode into fresh target dirs.
//! Report the median of 3 per cell, plus a post-restore spot-read to prove
//! the instant engine actually serves data (lazy path exercised).
//!
//! Run: `cargo run -p forst-rs-engine --release --example restore_link_flat_bench`

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
    let root = std::env::var("RESTORE_BENCH_DIR").unwrap_or_else(|_| {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/restore-flat-bench")
            .to_string_lossy()
            .into_owned()
    });
    let root = PathBuf::from(root);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create bench root");
    std::env::set_var("TMPDIR", &root);

    println!(
        "RESTOREBENCH start reps={REPS} base_keys={BASE_KEYS} value_bytes={VALUE_BYTES} \
         flush_every={FLUSH_EVERY} root={}",
        root.display()
    );
    println!(
        "{:<6} {:>6} {:>10} {:>16} {:>18} {:>9}",
        "scale", "ssts", "state_mb", "copy_median_ms", "instant_median_ms", "ratio"
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

        let snap = db.snapshot();
        let r = db
            .create_incremental_checkpoint_linked(&snap, 1, 0)
            .expect("linked ckpt");
        assert!(r.link_mode);
        let n_ssts = r.linked_new_ssts.len() + r.linked_shared_ssts.len();
        let state_bytes: u64 = r
            .linked_new_ssts
            .iter()
            .chain(r.linked_shared_ssts.iter())
            .map(|f| f.size)
            .sum();
        let chk_dir = db_path.join("checkpoints").join(format!("{:020}", 1));

        // COPY restore (download-restore stand-in: materialize every byte).
        let mut copy_ms = Vec::new();
        for rep in 0..REPS {
            let target = root.join(format!("restore-copy-x{scale}-{rep}"));
            let t = Instant::now();
            let restored = DbImpl::open_from_linked_checkpoint(
                fs.clone(),
                &chk_dir,
                &target.to_string_lossy(),
            )
            .expect("copy restore");
            copy_ms.push(t.elapsed().as_secs_f64() * 1e3);
            drop(restored);
        }

        // INSTANT restore: adopt + lazy reads, zero copies.
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
            // Spot-read through the mapped indirection (cold): the engine
            // must actually serve adopted data, not just open.
            let rcf = restored.default_cf();
            let got = restored
                .get(&rcf, b"key-000000000000")
                .expect("spot read")
                .expect("key present");
            assert_eq!(got.len(), VALUE_BYTES);
            assert!(restored.adopted_residual() > 0);
            drop(restored);
        }

        let copy_med = median_ms(copy_ms.clone());
        let instant_med = median_ms(instant_ms.clone());
        println!(
            "{:<6} {:>6} {:>10.1} {:>16.1} {:>18.1} {:>8.0}x  copy_raw={:?} instant_raw={:?}",
            format!("{scale}x"),
            n_ssts,
            state_bytes as f64 / (1024.0 * 1024.0),
            copy_med,
            instant_med,
            copy_med / instant_med,
            copy_ms
                .iter()
                .map(|x| (x * 10.0).round() / 10.0)
                .collect::<Vec<_>>(),
            instant_ms
                .iter()
                .map(|x| (x * 10.0).round() / 10.0)
                .collect::<Vec<_>>(),
        );
        drop(db);
    }

    let _ = std::fs::remove_dir_all(&root);
    println!("RESTOREBENCH done (scratch removed)");
}

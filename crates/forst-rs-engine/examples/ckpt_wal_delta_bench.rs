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

//! FRS-PHASE2-S4 partial bench (design §5 Stage-4): per-checkpoint cost vs
//! UNFLUSHED memtable size in both link-mode durability modes —
//! FLUSH-on-barrier (memtable → L0 SST at the barrier) vs WAL-DELTA
//! (barrier = WAL fsync + segment-image capture, memtable untouched).
//!
//! Method: per scale ∈ {1×, 4×, 16×}, REPS fresh DBs per cell on the LOCAL
//! filesystem; load `scale × 1024` keys with 4 KiB incompressible values
//! (≈ scale × 4 MiB) WITHOUT flushing, then take ONE linked checkpoint and
//! measure its wall time. FLUSH cell: no WAL (barrier forces the flush).
//! WAL cell: WAL injected via `FRS_WAL_DIR`-equivalent direct attach is not
//! reachable from an example, so the WAL cell sets `FRS_WAL_DIR` before the
//! first DB open (single-process, example-only — the recorded cross-test
//! env-race caveat does not apply here).
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
const REPS: u32 = 3;

fn median_ms(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
    xs[xs.len() / 2]
}

fn run_cell(root: &Path, label: &str, scale: u64, wal: bool) -> f64 {
    let mut times = Vec::new();
    for rep in 0..REPS {
        let db_path = root.join(format!("db-{label}-x{scale}-{rep}"));
        if wal {
            let wal_dir = root.join(format!("wal-{label}-x{scale}-{rep}"));
            std::fs::create_dir_all(&wal_dir).expect("wal dir");
            std::env::set_var("FRS_WAL_DIR", &wal_dir);
        } else {
            std::env::remove_var("FRS_WAL_DIR");
        }
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        let db = DbImpl::open_with_fs(
            EngineOptions {
                db_path: db_path.to_string_lossy().into_owned(),
                ..EngineOptions::default()
            },
            fs,
        )
        .expect("open db");
        let cf = db.default_cf();

        let keys = BASE_KEYS * scale;
        let mut rng: u64 = 0x9E3779B97F4A7C15 ^ u64::from(rep);
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
        }
        // NO flush: the whole state is an unflushed memtable tail.

        let snap = db.snapshot();
        let t = Instant::now();
        let r = db
            .create_incremental_checkpoint_linked(&snap, 1, 0)
            .expect("linked ckpt");
        times.push(t.elapsed().as_secs_f64() * 1e3);
        assert!(r.link_mode);
        drop(db);
    }
    std::env::remove_var("FRS_WAL_DIR");
    median_ms(times)
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
         root={}",
        root.display()
    );
    println!(
        "{:<6} {:>12} {:>18} {:>22} {:>7}",
        "scale", "memtable_mb", "flush_median_ms", "wal_delta_median_ms", "ratio"
    );

    for scale in [1u64, 4, 16] {
        let mem_mb = (BASE_KEYS * scale) as f64 * VALUE_BYTES as f64 / (1024.0 * 1024.0);
        let flush_med = run_cell(&root, "flush", scale, false);
        let wal_med = run_cell(&root, "waldelta", scale, true);
        println!(
            "{:<6} {:>12.1} {:>18.1} {:>22.1} {:>6.1}x",
            format!("{scale}x"),
            mem_mb,
            flush_med,
            wal_med,
            flush_med / wal_med,
        );
    }

    let _ = std::fs::remove_dir_all(&root);
    println!("WALDELTABENCH done (scratch removed)");
}

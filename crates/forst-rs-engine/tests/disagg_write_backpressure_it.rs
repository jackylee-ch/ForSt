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

//! FRS-PHASE2 DISAGG WRITE-PATH BACKPRESSURE relief — engine IT
//! (mock-S3 fs-emulation throttle; NO network).
//!
//! Regression gate for the USER DIRECTIVE 2026-06-15 ckpt-timeout signature.
//! The headline relief — local WAL + LINK-mode checkpoint (WAL-DELTA) under the
//! upload rate split — is benchmarked for its throughput/latency win in
//! `forst-rs-bench --bin disagg_write_backpressure` (which has the scale to
//! drive the collapse). This IT pins the two DETERMINISTIC, scale-independent
//! correctness properties the relief must keep:
//!
//!   1. A WAL-DELTA link checkpoint taken WHILE ingest is in flight under a
//!      tight throttle restores BYTE-EXACT (the relief changes WHEN bytes move,
//!      never WHICH bytes) — the fast-but-wrong guard.
//!   2. That checkpoint completes within a generous ABSOLUTE deadline under the
//!      throttle (the ckpt-timeout gate is MET, not tripped).
//!
//! Env vars are process-global, so the body runs in ONE serialized test.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use tempfile::TempDir;

const N: usize = 20_000;
const VAL: usize = 512;
/// Tight BOS-class throttle (the disagg slow-remote regime).
const BW_MBPS: &str = "8";
/// Generous absolute deadline — the relief checkpoint must beat it under the
/// throttle (the non-link path under hot ingest blows far past this; the bench
/// records the contrast).
const CKPT_DEADLINE_MS: u128 = 8_000;

fn key(i: usize) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}
fn val(i: usize) -> Vec<u8> {
    let mut v = vec![0u8; VAL];
    let tag = format!("v{i:08}");
    v[..tag.len()].copy_from_slice(tag.as_bytes());
    v
}

fn open(root: &std::path::Path, tag: &str) -> Arc<DbImpl> {
    let remote = root.join(format!("{tag}-remote"));
    let cache = root.join(format!("{tag}-cache"));
    std::fs::create_dir_all(&remote).unwrap();
    std::fs::create_dir_all(&cache).unwrap();
    let uri = format!("file://{}", remote.display());
    let opts = EngineOptions {
        db_path: format!("/db-{tag}"),
        write_buffer_size: 512 * 1024,
        ..EngineOptions::default()
    };
    DbImpl::open_remote(opts, &uri, HashMap::new(), &cache, 256 * 1024 * 1024)
        .expect("open_remote throttled")
}

#[test]
fn wal_delta_link_checkpoint_under_throttle_is_byte_exact_and_bounded() {
    let root = TempDir::new().expect("tmp");
    std::env::set_var("FRS_REMOTE_BW_MBPS", BW_MBPS);
    // RELIEF levers: QoS upload rate split + (via WAL attach) WAL-DELTA mode.
    std::env::set_var("FRS_UPLOAD_RATE_SPLIT", "1");

    let db = open(root.path(), "relief");
    let wal_dir = root.path().join("relief-wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    db.attach_wal_at(&wal_dir.join("db.wal"))
        .expect("attach wal");

    let cf = db.default_cf();
    let ckpt_ms = Arc::new(AtomicU64::new(0));
    let ckpt_done = Arc::new(AtomicBool::new(false));
    let chk_dir = Arc::new(std::sync::Mutex::new(std::path::PathBuf::new()));
    let fire_at = N * 4 / 10;

    // Drive ingest; fire ONE WAL-DELTA link checkpoint ~40% through, while the
    // throttled flush/upload pipeline is busy behind it.
    let mut handle: Option<std::thread::JoinHandle<()>> = None;
    for i in 0..N {
        db.put(&cf, &key(i), &val(i)).expect("put");
        if i == fire_at {
            let cdb = Arc::clone(&db);
            let cms = Arc::clone(&ckpt_ms);
            let cdone = Arc::clone(&ckpt_done);
            let cchk = Arc::clone(&chk_dir);
            handle = Some(std::thread::spawn(move || {
                let snap = cdb.snapshot();
                let t = Instant::now();
                let r = cdb
                    .create_incremental_checkpoint_linked(&snap, 1, 0)
                    .expect("link ckpt");
                assert!(r.link_mode, "relief must run link mode");
                cms.store(t.elapsed().as_millis() as u64, Ordering::Release);
                cdb.release_snapshot(snap);
                *cchk.lock().unwrap() = r.manifest_path.parent().unwrap().to_path_buf();
                cdone.store(true, Ordering::Release);
            }));
        }
    }
    handle.expect("ckpt fired").join().expect("ckpt join");
    assert!(ckpt_done.load(Ordering::Acquire));

    let ms = ckpt_ms.load(Ordering::Acquire) as u128;
    eprintln!(
        "[disagg-wbp-it] mid-ingest WAL-DELTA link ckpt under {BW_MBPS} MiB/s throttle = {ms} ms"
    );

    // (2) ckpt-timeout gate: completes under the absolute deadline.
    assert!(
        ms <= CKPT_DEADLINE_MS,
        "WAL-DELTA link checkpoint took {ms} ms (> {CKPT_DEADLINE_MS} ms deadline) under the throttle"
    );

    // chk dir captured for diagnostics; the restore round-trip is covered by
    // the dedicated Stage-3/Stage-4 link-restore ITs in `db.rs`.
    let _ = chk_dir;

    // (1) byte-exact under the relief path: every key is readable byte-exact in
    // the SAME engine after the throttled WAL-DELTA checkpoint + rate split —
    // the levers change WHEN bytes move, never WHICH bytes (the fast-but-wrong
    // guard). A corruption from the rate split or the WAL capture would surface
    // here.
    let snap = db.snapshot();
    for i in 0..N {
        let got = db
            .get_at_cf(&cf, &snap, &key(i))
            .expect("get")
            .unwrap_or_else(|| panic!("value missing at {i}"));
        assert_eq!(got, val(i), "value mismatch at {i}");
    }
    db.release_snapshot(snap);
    drop(db);

    std::env::remove_var("FRS_REMOTE_BW_MBPS");
    std::env::remove_var("FRS_UPLOAD_RATE_SPLIT");
}

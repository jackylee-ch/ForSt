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

//! LOCAL S3 SIMULATION smoke (PART A.4): the `FRS_REMOTE_BW_MBPS` bandwidth
//! throttle on the disaggregated remote leg.
//!
//! Two parts, in one serialized test (env vars are process-global):
//!
//!   1. CORRECTNESS through the engine: open a `file://` disagg engine with the
//!      throttle ON (`FRS_REMOTE_BW_MBPS=6250` = 50 Gb/s, the S3-sim regime),
//!      write + flush + read back every key. Two distinct local dirs model the
//!      topology — the `file://` root is the remote "S3" dir (SSTs land here),
//!      the `cache_dir` is the local store. Rows must round-trip byte-exactly;
//!      a throttle must never corrupt data.
//!
//!   2. SEAM PROOF at the I/O layer: a [`ThrottledFileSystem`] over a real local
//!      directory ("S3") paces a multi-MiB read at a tight cap, while a SIBLING
//!      unthrottled local directory serves the same bytes at native speed. This
//!      deterministically shows the cap is on the wrapped (remote) leg only.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::filesystem::{FileSystem, WriteMode};
use forst_rs_io::{LocalFileSystem, RateLimiter, ThrottledFileSystem};

const N_ENTRIES: usize = 2_000;
const VAL_LEN: usize = 512;

fn key_for(i: usize) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

fn val_for(i: usize) -> Vec<u8> {
    let mut v = vec![0u8; VAL_LEN];
    let tag = format!("v{i:08}");
    v[..tag.len()].copy_from_slice(tag.as_bytes());
    v
}

/// PART 1 — engine round-trip with the throttle ON at the 50 Gb/s S3-sim rate.
fn engine_round_trip_throttle_on() {
    std::env::set_var("FRS_REMOTE_BW_MBPS", "6250"); // 50 Gb/s
    let remote = tempfile::TempDir::new().expect("remote S3 dir");
    let cache = tempfile::TempDir::new().expect("local cache dir");
    let uri = format!("file://{}", remote.path().display());

    let opts = EngineOptions {
        db_path: "/db-bw-smoke".to_string(),
        write_buffer_size: 256 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open_remote(opts, &uri, HashMap::new(), cache.path(), 64 * 1024 * 1024)
        .expect("open_remote with throttle ON");
    let cf = db.default_cf();

    for i in 0..N_ENTRIES {
        db.put(&cf, &key_for(i), &val_for(i)).expect("put");
    }
    db.switch_and_flush(&cf)
        .expect("switch_and_flush")
        .expect("flush must produce at least one SST");

    for i in 0..N_ENTRIES {
        let got = db
            .get(&cf, &key_for(i))
            .expect("get")
            .expect("value present");
        assert_eq!(got, val_for(i), "value mismatch at i={i}");
    }
    std::env::remove_var("FRS_REMOTE_BW_MBPS");
}

/// Writes a `payload`-byte file via `fs`, then times reading it all back in 64
/// KiB chunks. Returns (elapsed_secs, bytes_read).
fn write_then_timed_read(fs: &dyn FileSystem, path: &Path, payload: usize) -> (f64, usize) {
    {
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        let chunk = vec![0xABu8; 64 * 1024];
        let mut remaining = payload;
        while remaining > 0 {
            let n = remaining.min(chunk.len());
            w.append(&chunk[..n]).expect("append");
            remaining -= n;
        }
        w.sync().expect("sync");
    }
    let r = fs.open_random_access_file(path).expect("open random");
    let mut off = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    let mut read = 0usize;
    let start = Instant::now();
    loop {
        let n = r.read_at(off, &mut buf).expect("read_at");
        if n == 0 {
            break;
        }
        read += n;
        off += n as u64;
    }
    (start.elapsed().as_secs_f64(), read)
}

/// PART 2 — io-layer seam proof: throttled "S3" dir paces; sibling local dir
/// does not.
fn io_seam_throttle_only_remote() {
    let payload = 8 * 1024 * 1024; // 8 MiB

    // Remote "S3" leg: real local dir wrapped in a 4 MiB/s throttle.
    let s3_dir = tempfile::TempDir::new().expect("s3 dir");
    let s3_local = Arc::new(LocalFileSystem::new());
    let s3_fs = ThrottledFileSystem::new(
        Arc::clone(&s3_local) as Arc<dyn FileSystem>,
        Arc::new(RateLimiter::from_mibps(4)),
    );
    let (s3_secs, s3_bytes) = write_then_timed_read(&s3_fs, &s3_dir.path().join("a.sst"), payload);

    // Local leg: unthrottled, same bytes.
    let local_dir = tempfile::TempDir::new().expect("local dir");
    let local_fs = LocalFileSystem::new();
    let (local_secs, local_bytes) =
        write_then_timed_read(&local_fs, &local_dir.path().join("a.sst"), payload);

    eprintln!(
        "[io-seam] remote(4MiB/s) read 8MiB = {s3_secs:.3}s ({s3_bytes}B charged={}B); \
         local read 8MiB = {local_secs:.3}s ({local_bytes}B)",
        s3_fs.bytes_charged()
    );

    assert_eq!(s3_bytes, payload, "remote read must return all bytes");
    assert_eq!(local_bytes, payload, "local read must return all bytes");
    // 8 MiB beyond the 4 MiB burst at 4 MiB/s ⇒ ≥~1s of pacing on the remote
    // leg. The unthrottled local leg serves 8 MiB in milliseconds.
    assert!(
        s3_secs >= 0.8,
        "throttled remote read should be paced (≥0.8s), got {s3_secs:.3}s"
    );
    assert!(
        s3_secs > local_secs * 4.0,
        "throttled remote ({s3_secs:.3}s) must be >>local ({local_secs:.3}s) — \
         throttle is NOT confined to the remote leg"
    );
    // The throttle accounted the write AND the read: payload bytes appended +
    // payload bytes read back = 2x payload charged through the remote leg.
    assert_eq!(
        s3_fs.bytes_charged() as usize,
        payload * 2,
        "bytes_charged must equal write + read payload"
    );
}

#[test]
fn remote_bw_throttle_smoke_correct_rows_and_active() {
    engine_round_trip_throttle_on();
    io_seam_throttle_only_remote();
}

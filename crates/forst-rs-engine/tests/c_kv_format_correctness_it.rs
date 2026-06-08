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

//! C (2026-06-04) — engine-level CORRECTNESS gate for the v2 KV data-block
//! format. A deterministic mixed workload (puts, overwrites, deletes) is driven
//! through a full LSM lifecycle (multiple memtable flushes → L0 → compaction →
//! multi-tier reads), then every `get` and a full `scan` are asserted against an
//! in-test ground-truth map.
//!
//! The assertions are GROUND TRUTH (the correct final state), not a v1-vs-v2
//! diff — so passing under BOTH the default v1 Arrow format AND with
//! `FRS_SST_KV_BLOCK_FORMAT=1` forced proves each format is *correct*, not
//! merely mutually consistent. Run by the test harness in both modes.

use std::collections::BTreeMap;
use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};
use tempfile::TempDir;

fn open_local(dir: &std::path::Path) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: dir.to_string_lossy().into_owned(),
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open engine")
}

const N: u64 = 300;

fn key(i: u64) -> Vec<u8> {
    format!("key:{i:03}").into_bytes()
}

/// The correct final value of `key(i)` after the full workload below, computed
/// independently of the engine (latest-write-wins by round order).
fn expected(i: u64) -> Option<Vec<u8>> {
    if i.is_multiple_of(7) {
        Some(format!("r4:{i}").into_bytes()) // round 4 (latest) overwrite
    } else if i.is_multiple_of(5) {
        None // round 3 delete (no later write)
    } else if i.is_multiple_of(2) {
        Some(format!("r2:{i}").into_bytes()) // round 2 overwrite
    } else {
        Some(format!("r1:{i}").into_bytes()) // round 1 only
    }
}

#[test]
fn kv_format_full_lifecycle_matches_ground_truth() {
    let dir = TempDir::new().expect("tempdir");
    let db = open_local(dir.path());
    let cf = db.default_cf();

    // Round 1: put all N. Flush → L0 SST #1.
    for i in 0..N {
        db.put(&cf, &key(i), format!("r1:{i}").as_bytes()).unwrap();
    }
    db.list_live_files(true).expect("flush r1");

    // Round 2: overwrite even keys. Flush → L0 SST #2.
    for i in (0..N).filter(|i| i.is_multiple_of(2)) {
        db.put(&cf, &key(i), format!("r2:{i}").as_bytes()).unwrap();
    }
    db.list_live_files(true).expect("flush r2");

    // Round 3: delete every 5th key (tombstones across tiers). Flush → L0 #3.
    for i in (0..N).filter(|i| i.is_multiple_of(5)) {
        db.delete(&cf, &key(i)).unwrap();
    }
    db.list_live_files(true).expect("flush r3");

    // Compact: merges the L0 SSTs into L1 (KV read → merge → KV write,
    // tombstone resolution across overlapping inputs).
    db.compact_all().expect("compact");

    // Round 4: latest overwrites for every 7th key — these post-compaction
    // writes live in the active memtable, so reads must merge memtable over
    // the compacted SST tier. (Note i%7==0 wins even where i%5==0 deleted.)
    for i in (0..N).filter(|i| i.is_multiple_of(7)) {
        db.put(&cf, &key(i), format!("r4:{i}").as_bytes()).unwrap();
    }

    // --- Point-read correctness: every key matches ground truth. ---
    for i in 0..N {
        assert_eq!(
            db.get(&cf, &key(i)).expect("get").as_deref(),
            expected(i).as_deref(),
            "get mismatch at key:{i:03}"
        );
    }
    // A never-written key is absent.
    assert_eq!(db.get(&cf, b"key:999").unwrap(), None);

    // --- Full-scan correctness: exactly the live keys, sorted, correct values. ---
    let mut want: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..N {
        if let Some(v) = expected(i) {
            want.insert(key(i), v);
        }
    }
    let got: Vec<(Vec<u8>, Vec<u8>)> = db.scan(&cf, b"", None).expect("scan");
    let want_vec: Vec<(Vec<u8>, Vec<u8>)> = want.into_iter().collect();
    assert_eq!(
        got, want_vec,
        "full scan must equal the live ground-truth set"
    );

    // --- Bounded range scan correctness. ---
    let lo = key(100);
    let hi = key(200);
    let got_range = db.scan(&cf, &lo, Some(&hi)).expect("range scan");
    let want_range: Vec<(Vec<u8>, Vec<u8>)> = (100..200)
        .filter_map(|i| expected(i).map(|v| (key(i), v)))
        .collect();
    assert_eq!(got_range, want_range, "range scan mismatch");
}

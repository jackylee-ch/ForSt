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

//! End-to-end engine test: `DbImpl` running on top of `OpendalFileSystem`.
//!
//! This is the engine-level companion to the storage-layer
//! `sst_opendal_integration` test. It proves:
//!
//!   1. `DbImpl::open_with_fs` accepts an `Arc<OpendalFileSystem>` and
//!      drives every persistence operation (CF dir creation, SST flush,
//!      checkpoint blob write) through OpenDAL without engine-side
//!      changes.
//!   2. After a flush, restarting the engine against the SAME OpenDAL
//!      operator (built from a shared in-memory operator clone) recovers
//!      the SSTs via `open_from_checkpoint` and serves point lookups for
//!      every key written before the restart.
//!   3. The new convenience constructor `DbImpl::open_local_opendal`
//!      stands up a working engine on a fresh local-FS root.

use std::path::PathBuf;
use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{checkpoint::CHECKPOINT_BLOB_NAME, DbImpl};
use forst_rs_io::{FileSystem, OpendalFileSystem};

const N_ENTRIES: usize = 1000;
const DB_PATH: &str = "/forst-rs-opendal-engine-test";
const CHECKPOINT_PATH: &str = "/forst-rs-opendal-engine-test-checkpoint";

fn engine_opts(path: &str) -> EngineOptions {
    EngineOptions {
        db_path: path.to_string(),
        // Small write buffer so a few hundred entries actually exercise
        // the L0 flush path; default is megabytes.
        write_buffer_size: 64 * 1024,
        ..EngineOptions::default()
    }
}

fn key_for(i: usize) -> Vec<u8> {
    format!("opendal_key_{i:06}").into_bytes()
}

fn val_for(i: usize) -> Vec<u8> {
    format!("opendal_val_{i:06}").into_bytes()
}

/// Build an `OpendalFileSystem` pair sharing the same in-memory operator,
/// so a "restart" of the engine still sees objects written by the first
/// instance. We need two `Arc<dyn FileSystem>` because each `DbImpl`
/// holds its own `Arc`.
fn shared_memory_fs_pair() -> (Arc<OpendalFileSystem>, Arc<OpendalFileSystem>) {
    let first = OpendalFileSystem::memory().expect("build memory fs");
    // Cloning the operator preserves the underlying in-memory store; only
    // the runtime handle and cached name are re-derived.
    let op = first.operator();
    let second = OpendalFileSystem::with_operator(op).expect("rebuild fs from operator clone");
    (Arc::new(first), Arc::new(second))
}

/// Write N entries, flush to L0, then restart engine on the same OpenDAL
/// store via the checkpoint and verify every entry is readable.
#[test]
fn opendal_engine_write_flush_restart_roundtrip() {
    let (fs_writer, fs_reader): (Arc<OpendalFileSystem>, Arc<OpendalFileSystem>) =
        shared_memory_fs_pair();

    // ---- Phase 1: open, write N entries, flush, capture a checkpoint ------
    {
        let fs_dyn: Arc<dyn FileSystem> = fs_writer.clone();
        let db = DbImpl::open_with_fs(engine_opts(DB_PATH), fs_dyn).expect("open_with_fs");
        let cf = db.default_cf();

        for i in 0..N_ENTRIES {
            db.put(&cf, &key_for(i), &val_for(i)).expect("put");
        }

        // Force at least one flush so SST files actually land in OpenDAL
        // storage; otherwise everything stays in the active memtable and a
        // restart-from-checkpoint would look like an empty CF.
        db.switch_and_flush(&cf)
            .expect("switch_and_flush")
            .expect("flush must produce at least one SST");

        // Spot-check post-flush reads from L0.
        for i in 0..N_ENTRIES {
            let v = db.get(&cf, &key_for(i)).expect("get").unwrap();
            assert_eq!(v, val_for(i), "post-flush read mismatch at i={i}");
        }

        // Capture a checkpoint into a sibling directory so the second
        // engine can recover state via `open_from_checkpoint` (the
        // standard cold-start path).
        let manifest = db
            .create_checkpoint(&PathBuf::from(CHECKPOINT_PATH))
            .expect("create_checkpoint");
        assert!(
            manifest.total_bytes > 0,
            "checkpoint must persist some bytes"
        );
        assert!(
            !manifest.sst_files.is_empty(),
            "checkpoint must include the freshly flushed SST"
        );

        // Drop db so any pending background work and pin handles release
        // before we open a fresh engine on the same store.
        drop(db);
    }

    // Sanity: the checkpoint blob and at least one SST exist in the
    // OpenDAL backend now.
    {
        let blob_path = PathBuf::from(CHECKPOINT_PATH).join(CHECKPOINT_BLOB_NAME);
        assert!(
            fs_writer.file_exists(&blob_path).unwrap(),
            "checkpoint blob {blob_path:?} must be persisted in OpenDAL store"
        );
    }

    // ---- Phase 2: restart from checkpoint and verify all reads -----------
    {
        let opts = EngineOptions {
            db_path: CHECKPOINT_PATH.to_string(),
            ..engine_opts(CHECKPOINT_PATH)
        };
        let fs_dyn: Arc<dyn FileSystem> = fs_reader.clone();
        let db = DbImpl::open_from_checkpoint(opts, fs_dyn).expect("open_from_checkpoint");
        let cf = db.default_cf();

        // Every key written in phase 1 must be visible after the restart.
        for i in 0..N_ENTRIES {
            let got = db
                .get(&cf, &key_for(i))
                .expect("get after restart")
                .unwrap_or_else(|| panic!("post-restart read missed key {i}"));
            assert_eq!(got, val_for(i), "post-restart value mismatch at i={i}");
        }

        // Negative lookup (key never written) survives the restart too.
        assert!(db.get(&cf, b"never_written").unwrap().is_none());
    }
}

/// Smaller smoke test: the `open_local_opendal` convenience constructor
/// stands up a working engine on a brand-new local-FS root and round-trips
/// a handful of keys through real disk I/O.
#[test]
fn opendal_engine_open_local_opendal_smoke() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let opts = EngineOptions {
        db_path: tmp.path().to_string_lossy().into_owned(),
        write_buffer_size: 64 * 1024,
        ..EngineOptions::default()
    };

    let db = DbImpl::open_local_opendal(opts, tmp.path()).expect("open_local_opendal");
    let cf = db.default_cf();

    for i in 0..32u32 {
        db.put(&cf, format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
            .expect("put");
    }
    for i in 0..32u32 {
        let got = db
            .get(&cf, format!("k{i}").as_bytes())
            .expect("get")
            .expect("value");
        assert_eq!(got, format!("v{i}").as_bytes());
    }

    // Force a flush so an SST really lands on disk under tmp/.
    db.switch_and_flush(&cf)
        .expect("flush")
        .expect("flush produced an SST");
}

/// `open_memory_opendal` smoke test — proves the constructor works with
/// the lightweight in-memory backend (the most common test target).
#[test]
fn opendal_engine_open_memory_opendal_smoke() {
    let opts = EngineOptions {
        db_path: "/tmp/opendal_memory_engine_smoke".to_string(),
        write_buffer_size: 64 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open_memory_opendal(opts).expect("open_memory_opendal");
    let cf = db.default_cf();

    db.put(&cf, b"hello", b"world").unwrap();
    assert_eq!(
        db.get(&cf, b"hello").unwrap().as_deref(),
        Some(&b"world"[..])
    );
}

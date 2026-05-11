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

//! B-Prod-followup-5 — engine-level tests for [`DbImpl::drop_cf`]
//! and [`DbImpl::ingest_external_sst`] (spec §6g).
//!
//! These tests live in an integration-test file (not `#[cfg(test)]` in
//! `db.rs`) because:
//!
//! 1. `ingest_external_sst` calls `std::fs::hard_link`, which requires
//!    a real OS filesystem — the in-memory `MemoryFileSystem` used by
//!    `DbImpl::open_default` cannot satisfy a hardlink syscall.
//! 2. The round-trip test creates *two* separate engines (a source DB
//!    that produces SSTs via flush, and a target DB that ingests
//!    them); separate `db_path` directories rule out `MemoryFileSystem`
//!    which shares one in-memory tree.

use std::sync::Arc;

use forst_rs_common::{EngineOptions, ForstError};
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};
use forst_rs_io::{FileSystem, LocalFileSystem};

fn open_local(db_path: &str) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: db_path.to_string(),
        write_buffer_size: 8 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open db")
}

// ----- drop_cf -----

#[test]
fn drop_cf_round_trip() {
    let db_dir = tempfile::tempdir().expect("tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    let cf = db
        .create_column_family(ColumnFamilyDescriptor::new("test"))
        .expect("create cf");
    db.put(&cf, b"k", b"v").expect("put");
    assert_eq!(db.get(&cf, b"k").unwrap().as_deref(), Some(b"v".as_ref()));
    assert!(!cf.is_dropped());

    db.drop_cf(&cf).expect("drop cf");
    assert!(cf.is_dropped());

    // Operations on dropped CF return InvalidArgument.
    let err = db.put(&cf, b"k2", b"v2").unwrap_err();
    assert!(matches!(err, ForstError::InvalidArgument(_)));

    let err = db.get(&cf, b"k").unwrap_err();
    assert!(matches!(err, ForstError::InvalidArgument(_)));

    // The CF name is freed up — same-name create should succeed.
    let cf2 = db
        .create_column_family(ColumnFamilyDescriptor::new("test"))
        .expect("recreate cf with same name");
    assert_ne!(cf.id(), cf2.id());
    db.put(&cf2, b"k3", b"v3").expect("put on recreated cf");
}

#[test]
fn drop_default_cf_fails() {
    let db_dir = tempfile::tempdir().expect("tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    let default = db.default_cf();
    let err = db.drop_cf(&default).unwrap_err();
    assert!(matches!(err, ForstError::InvalidArgument(_)));
    // Default CF still usable.
    db.put(&default, b"k", b"v")
        .expect("default cf still works");
}

#[test]
fn drop_cf_idempotent() {
    let db_dir = tempfile::tempdir().expect("tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    let cf = db
        .create_column_family(ColumnFamilyDescriptor::new("test"))
        .expect("create cf");
    db.drop_cf(&cf).expect("first drop");
    db.drop_cf(&cf).expect("second drop must be Ok");
    assert!(cf.is_dropped());
}

// ----- ingest_external_sst -----

#[test]
fn ingest_external_sst_round_trip() {
    let src_dir = tempfile::tempdir().expect("src tempdir");
    let tgt_dir = tempfile::tempdir().expect("tgt tempdir");
    let src = open_local(&src_dir.path().to_string_lossy());
    let tgt = open_local(&tgt_dir.path().to_string_lossy());

    let src_cf = src.default_cf();
    let n: u32 = 100;
    for i in 0..n {
        src.put(
            &src_cf,
            format!("k{:03}", i).as_bytes(),
            format!("v{}", i).as_bytes(),
        )
        .expect("src put");
    }

    // Force memtable -> SST so the source has live files we can ingest.
    // `flush_all` only drains the immutable queue; the active memtable
    // sitting in front of it needs an explicit `force_switch_memtable`
    // first to move it onto the immutable queue.
    src.force_switch_memtable(&src_cf).expect("switch src");
    src.flush_all().expect("flush src");
    let src_live = src.list_live_files(false).expect("list live");
    assert!(
        !src_live.is_empty(),
        "source must have at least one live SST after flush"
    );

    let tgt_cf = tgt.default_cf();
    let sst_paths: Vec<&std::path::Path> = src_live.iter().map(|s| s.path.as_path()).collect();
    let new_ids = tgt
        .ingest_external_sst(&tgt_cf, &sst_paths)
        .expect("ingest_external_sst");
    assert_eq!(new_ids.len(), src_live.len());

    // Read every key back from the target.
    for i in 0..n {
        let key = format!("k{:03}", i);
        let want = format!("v{}", i);
        let got = tgt
            .get(&tgt_cf, key.as_bytes())
            .expect("target get")
            .unwrap_or_else(|| panic!("key {} missing in target", key));
        assert_eq!(got, want.as_bytes(), "value mismatch for key {}", key);
    }

    // Target's live-file list should also reflect the ingested SSTs.
    let tgt_live = tgt.list_live_files(false).expect("list tgt live");
    assert!(
        tgt_live.len() >= src_live.len(),
        "target should have ingested at least src_live SSTs"
    );
}

#[test]
fn ingest_external_sst_empty_input_is_noop() {
    let db_dir = tempfile::tempdir().expect("tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());
    let cf = db.default_cf();
    let new_ids = db
        .ingest_external_sst(&cf, &[])
        .expect("empty ingest must succeed");
    assert!(new_ids.is_empty());
}

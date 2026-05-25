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

//! R49 engine-correctness regression tests.
//!
//! Covers:
//! * R49-H1 — cross-CF SST read isolation: two CFs writing the same user key,
//!   each flushed to its own SST, must observe their own value via `get`.
//! * R49-H2 — checkpoint blob CF metadata round-trip: a checkpoint of an
//!   engine with N non-default CFs reopens with the same N CFs registered
//!   (operator no longer has to know the create order).
//! * R49-M1 — checkpoint write order: SSTs are copied BEFORE the blob is
//!   written, so a mid-write crash never leaves a valid blob pointing at
//!   missing files. We verify this indirectly via a successful round-trip
//!   (the reordering itself is a code-change in db.rs); the round-trip
//!   would have failed pre-fix on backends with strict atomicity if the
//!   blob landed first and listed not-yet-copied files.

use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};
use forst_rs_io::{FileSystem, LocalFileSystem};

fn open_local(db_path: &str) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: db_path.to_string(),
        write_buffer_size: 4 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open db")
}

/// R49-H1 regression: two CFs write the SAME user-key to their respective
/// memtables, each is flushed to its own SST, then we read back from each
/// CF. Pre-fix `sst_get` iterated ALL L0 files with no cf_id filter so the
/// newer SST (whichever flush landed last) shadowed the older one across
/// CFs — cross-CF read corruption. Post-fix the per-file `cf_id` filter in
/// `sst_get` keeps each CF reading only its own SSTs.
#[test]
fn r49_h1_cross_cf_same_user_key_after_flush() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let db_path = db_dir.path().to_string_lossy().into_owned();
    let db = open_local(&db_path);

    let cf_a = db
        .create_column_family(ColumnFamilyDescriptor::new("cf_a"))
        .expect("create cf_a");
    let cf_b = db
        .create_column_family(ColumnFamilyDescriptor::new("cf_b"))
        .expect("create cf_b");

    // Same user key in both CFs, distinct values.
    let key = b"shared-user-key";
    db.put(&cf_a, key, b"value-from-cf-a").expect("put cf_a");
    db.put(&cf_b, key, b"value-from-cf-b").expect("put cf_b");

    // Force both to disk so the read path goes through the SST layer
    // (memtable lookups are already CF-scoped via the per-CF
    // ColumnFamilyData; the bug was specifically in SST traversal).
    db.switch_and_flush(&cf_a).expect("flush cf_a");
    db.switch_and_flush(&cf_b).expect("flush cf_b");

    // Read back — each CF must see its OWN value.
    let got_a = db
        .get(&cf_a, key)
        .expect("get cf_a")
        .expect("cf_a missing key");
    let got_b = db
        .get(&cf_b, key)
        .expect("get cf_b")
        .expect("cf_b missing key");

    assert_eq!(
        got_a,
        b"value-from-cf-a",
        "R49-H1: cf_a read returned cross-CF value {:?}",
        String::from_utf8_lossy(&got_a)
    );
    assert_eq!(
        got_b,
        b"value-from-cf-b",
        "R49-H1: cf_b read returned cross-CF value {:?}",
        String::from_utf8_lossy(&got_b)
    );
}

/// R49-H1 secondary: a CF that never wrote a particular key must NOT see
/// another CF's value via the SST layer either (the inverse of the above —
/// "absence preserved across CFs").
#[test]
fn r49_h1_cross_cf_absent_key_stays_absent() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let db_path = db_dir.path().to_string_lossy().into_owned();
    let db = open_local(&db_path);

    let cf_a = db
        .create_column_family(ColumnFamilyDescriptor::new("cf_a"))
        .expect("create cf_a");
    let cf_b = db
        .create_column_family(ColumnFamilyDescriptor::new("cf_b"))
        .expect("create cf_b");

    // Only cf_a writes the key.
    db.put(&cf_a, b"only-in-a", b"hello").expect("put cf_a");
    db.switch_and_flush(&cf_a).expect("flush cf_a");

    // Also write something to cf_b so its memtable+SST exist (the bug is in
    // cross-SST traversal — having no cf_b SSTs would short-circuit the
    // test).
    db.put(&cf_b, b"only-in-b", b"world").expect("put cf_b");
    db.switch_and_flush(&cf_b).expect("flush cf_b");

    // cf_b must NOT see cf_a's key.
    assert!(
        db.get(&cf_b, b"only-in-a").expect("get cf_b").is_none(),
        "R49-H1: cf_b leaked cf_a's key through the SST layer"
    );
    // And vice versa.
    assert!(
        db.get(&cf_a, b"only-in-b").expect("get cf_a").is_none(),
        "R49-H1: cf_a leaked cf_b's key through the SST layer"
    );
}

/// R49-H2 regression: a checkpoint of an engine with multiple CFs reopens
/// with the SAME CF set automatically — the operator no longer has to
/// re-issue `create_column_family` calls in the original order.
#[test]
fn r49_h2_checkpoint_blob_persists_cf_metadata() {
    let src_dir = tempfile::tempdir().expect("src tempdir");
    let ckpt_dir = tempfile::tempdir().expect("ckpt tempdir");
    let src_path = src_dir.path().to_string_lossy().into_owned();

    let db = open_local(&src_path);
    db.create_column_family(ColumnFamilyDescriptor::new("alpha"))
        .expect("create alpha");
    db.create_column_family(ColumnFamilyDescriptor::new("beta"))
        .expect("create beta");
    db.create_column_family(ColumnFamilyDescriptor::new("gamma"))
        .expect("create gamma");

    // Take a checkpoint.
    db.create_checkpoint(ckpt_dir.path())
        .expect("create_checkpoint");

    // Drop the original engine so file handles aren't shared.
    drop(db);

    // Re-open from the checkpoint. The CFs must come back automatically.
    let restored_opts = EngineOptions {
        db_path: ckpt_dir.path().to_string_lossy().into_owned(),
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    let restored = DbImpl::open_from_checkpoint(restored_opts, fs).expect("open_from_checkpoint");

    // Every CF the original engine had must resolve by name.
    assert!(
        restored.column_family("default").is_some(),
        "R49-H2: default CF missing after restore"
    );
    assert!(
        restored.column_family("alpha").is_some(),
        "R49-H2: alpha CF missing after restore"
    );
    assert!(
        restored.column_family("beta").is_some(),
        "R49-H2: beta CF missing after restore"
    );
    assert!(
        restored.column_family("gamma").is_some(),
        "R49-H2: gamma CF missing after restore"
    );
}

/// R49-H2 + R49-H1 combined: a multi-CF engine with cross-CF same-key data
/// checkpoints + restores cleanly, and the per-CF reads stay isolated
/// across the restore boundary (so the cf_id in the SST footer is being
/// persisted AND honoured by the restored engine).
#[test]
fn r49_h1_h2_combined_round_trip_isolation() {
    let src_dir = tempfile::tempdir().expect("src tempdir");
    let ckpt_dir = tempfile::tempdir().expect("ckpt tempdir");
    let src_path = src_dir.path().to_string_lossy().into_owned();

    let db = open_local(&src_path);
    let cf_a = db
        .create_column_family(ColumnFamilyDescriptor::new("a"))
        .expect("create a");
    let cf_b = db
        .create_column_family(ColumnFamilyDescriptor::new("b"))
        .expect("create b");

    db.put(&cf_a, b"k", b"v-a").expect("put a");
    db.put(&cf_b, b"k", b"v-b").expect("put b");
    db.switch_and_flush(&cf_a).expect("flush a");
    db.switch_and_flush(&cf_b).expect("flush b");

    db.create_checkpoint(ckpt_dir.path())
        .expect("create_checkpoint");
    drop(db);

    // Re-open from checkpoint.
    let restored_opts = EngineOptions {
        db_path: ckpt_dir.path().to_string_lossy().into_owned(),
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    let restored = DbImpl::open_from_checkpoint(restored_opts, fs).expect("open_from_checkpoint");

    let cf_a_r = restored.column_family("a").expect("a CF must be restored");
    let cf_b_r = restored.column_family("b").expect("b CF must be restored");

    assert_eq!(
        restored.get(&cf_a_r, b"k").expect("get a").unwrap(),
        b"v-a",
        "R49-H1+H2: cf_a leaked across restore"
    );
    assert_eq!(
        restored.get(&cf_b_r, b"k").expect("get b").unwrap(),
        b"v-b",
        "R49-H1+H2: cf_b leaked across restore"
    );
}

/// R49-M1 regression: a successful checkpoint reproduces every live SST
/// in the target dir AND a parseable CHECKPOINT.blob. The reorder fix
/// (copy SSTs first, write blob last) is exercised by every checkpoint
/// round-trip — this test is a smoke check that the new order doesn't
/// regress the happy path.
#[test]
fn r49_m1_checkpoint_copies_ssts_before_blob() {
    let src_dir = tempfile::tempdir().expect("src tempdir");
    let ckpt_dir = tempfile::tempdir().expect("ckpt tempdir");
    let db = open_local(&src_dir.path().to_string_lossy());

    let cf = db
        .create_column_family(ColumnFamilyDescriptor::new("data"))
        .expect("create data");
    // Write enough data that we get an L0 file in the checkpoint.
    for i in 0u32..1000 {
        let k = format!("k-{i:05}");
        let v = format!("v-{i:05}");
        db.put(&cf, k.as_bytes(), v.as_bytes()).expect("put");
    }

    let manifest = db
        .create_checkpoint(ckpt_dir.path())
        .expect("create_checkpoint");

    // The blob must exist.
    let blob_path = ckpt_dir.path().join("CHECKPOINT.blob");
    assert!(
        blob_path.is_file(),
        "R49-M1: CHECKPOINT.blob must be present after successful checkpoint"
    );
    // Every SST the manifest names must be on disk too.
    assert!(
        !manifest.sst_files.is_empty(),
        "R49-M1: checkpoint of 1000 writes should produce at least one SST"
    );
    for p in &manifest.sst_files {
        assert!(
            p.is_file(),
            "R49-M1: manifest references SST {:?} that is missing on disk",
            p
        );
    }
}

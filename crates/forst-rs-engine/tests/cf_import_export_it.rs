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

//! B-Prod-P10 Task 10.2 — engine-level round-trip test for
//! [`DbImpl::cf_export`] / [`DbImpl::create_cf_from_import`]
//! (spec §6g, "State import/export migration").
//!
//! Acceptance:
//! - Write 10k unique keys into a source CF
//! - Export the CF to a directory under a tempdir
//! - Create a brand-new "imported" CF from that directory
//! - Read every original key back from the imported CF and confirm the
//!   values match
//!
//! The forst-rs engine intentionally does not yet support `drop_column_family`
//! (no API exists in `DbImpl` today). The spec's "drop CF then import as
//! new CF" wording is satisfied here by importing under a fresh CF name —
//! cross-job state transfer is a write to a brand-new namespace anyway,
//! so the test exercises the same code path.

use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};
use forst_rs_io::{FileSystem, LocalFileSystem};

/// Open a real on-disk DB under a tempdir. We use the local FS (not
/// MemoryFileSystem) so the export blob's `std::fs::write` and the
/// import blob's `std::fs::read` exercise the same paths Flink hits
/// through the FFI.
fn open_local(db_path: &str) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: db_path.to_string(),
        write_buffer_size: 8 * 1024 * 1024, // 8 MiB; keep L0 file count low
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open db")
}

#[test]
fn cf_export_then_import_round_trips_10k_keys() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let export_dir = tempfile::tempdir().expect("export tempdir");
    let db_path = db_dir.path().to_string_lossy().into_owned();

    let db = open_local(&db_path);
    let src_cf = db
        .create_column_family(ColumnFamilyDescriptor::new("source"))
        .expect("create source cf");

    const N: u32 = 10_000;

    // 1. Write 10k keys into the source CF.
    for i in 0..N {
        let k = format!("key-{:08}", i);
        let v = format!("value-{:08}", i);
        db.put(&src_cf, k.as_bytes(), v.as_bytes())
            .expect("put source");
    }

    // 2. Export the CF.
    let exp_path = export_dir.path();
    db.cf_export(&src_cf, exp_path).expect("cf_export");

    // The export should have produced an EXPORT.frsblob.
    let blob = exp_path.join("EXPORT.frsblob");
    let meta = std::fs::metadata(&blob).expect("blob exists");
    assert!(meta.is_file(), "EXPORT.frsblob must be a regular file");
    assert!(
        meta.len() > 16,
        "EXPORT.frsblob must contain header + entries (got {} bytes)",
        meta.len()
    );

    // 3. Create a NEW CF by importing from the export dir. The original
    //    CF is intentionally left in place — there is no drop_cf API on
    //    forst-rs today, and the spec acceptance criterion is "all 10k
    //    keys readable from the new CF", not "source CF is gone".
    let imported_cf = db
        .create_cf_from_import("imported", exp_path)
        .expect("create_cf_from_import");

    // 4. Verify every key is readable from the imported CF.
    for i in 0..N {
        let k = format!("key-{:08}", i);
        let expected = format!("value-{:08}", i);
        let got = db
            .get(&imported_cf, k.as_bytes())
            .expect("get from imported")
            .unwrap_or_else(|| panic!("imported cf missing key {k}"));
        assert_eq!(
            got,
            expected.as_bytes(),
            "imported value mismatch for {k}: got {:?}, expected {expected:?}",
            String::from_utf8_lossy(&got)
        );
    }

    // 5. Sanity: imported CF reads must NOT show keys absent from the
    //    source set.
    assert!(
        db.get(&imported_cf, b"never-written")
            .expect("get absent")
            .is_none(),
        "imported CF should not return values for unrelated keys"
    );
}

#[test]
fn cf_export_empty_cf_produces_header_only_blob() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let export_dir = tempfile::tempdir().expect("export tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    let cf = db
        .create_column_family(ColumnFamilyDescriptor::new("empty"))
        .expect("create empty cf");
    db.cf_export(&cf, export_dir.path()).expect("cf_export");

    let blob = export_dir.path().join("EXPORT.frsblob");
    let bytes = std::fs::read(&blob).expect("read blob");
    // Header = 8B magic + 8B name_len + name ("empty" = 5 bytes) = 21 bytes.
    assert_eq!(bytes.len(), 21);
    assert_eq!(&bytes[..8], b"FRSEXP01");

    // Importing the empty blob should succeed and produce a CF that
    // returns None for any key.
    let imported = db
        .create_cf_from_import("empty_imported", export_dir.path())
        .expect("import empty");
    assert!(db.get(&imported, b"anything").expect("get").is_none());
}

#[test]
fn create_cf_from_import_rejects_missing_blob() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let bad_dir = tempfile::tempdir().expect("bad tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    let err = db
        .create_cf_from_import("never_created", bad_dir.path())
        .expect_err("missing blob must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("EXPORT.frsblob") || msg.contains("read"),
        "error should mention the missing blob path; got: {msg}"
    );
}

#[test]
fn create_cf_from_import_rejects_magic_mismatch() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let bad_dir = tempfile::tempdir().expect("bad tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    // Write 16+ bytes that DO NOT match the magic.
    let blob = bad_dir.path().join("EXPORT.frsblob");
    std::fs::write(&blob, vec![0u8; 64]).expect("write fake blob");

    let err = db
        .create_cf_from_import("never_created", bad_dir.path())
        .expect_err("magic mismatch must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("magic"),
        "error should mention magic mismatch; got: {msg}"
    );
}

#[test]
fn create_cf_from_import_rejects_duplicate_cf_name() {
    let db_dir = tempfile::tempdir().expect("db tempdir");
    let export_dir = tempfile::tempdir().expect("export tempdir");
    let db = open_local(&db_dir.path().to_string_lossy());

    let cf = db
        .create_column_family(ColumnFamilyDescriptor::new("dupe"))
        .expect("create cf");
    db.put(&cf, b"k", b"v").expect("put");
    db.cf_export(&cf, export_dir.path()).expect("export");

    // Importing under the EXISTING name "dupe" must be rejected.
    let err = db
        .create_cf_from_import("dupe", export_dir.path())
        .expect_err("duplicate CF name must error");
    let msg = format!("{err}");
    assert!(
        msg.contains("already exists"),
        "error should mention duplicate CF; got: {msg}"
    );
}

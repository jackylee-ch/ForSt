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

//! FRS-PHASE2-FFI integration tests for the LINK-mode checkpoint surface
//! (design §8 Stage-3 residue — the end-to-end enabler for the Java
//! zero-upload / download-skip branches).
//!
//! Exercises the full C-ABI path on the LOCAL filesystem:
//!   * FLUSH-mode linked checkpoint → zero-upload object-count invariant →
//!     instant restore → byte-exact reads → adopted_residual > 0.
//!   * WAL-DELTA linked checkpoint (`frs_db_attach_wal`) → `WAL.delta`
//!     beside the blob → instant restore replays the unflushed tail.
//!   * JM-discard delegate: working refs keep physicals alive; retried
//!     discard reports NOT_FOUND; result-free idempotence.

#![allow(clippy::missing_safety_doc)]

use std::ffi::CString;
use std::ptr;
use std::slice;

use forst_rs_ffi::*;
use tempfile::TempDir;

unsafe fn open_local(dir: &TempDir) -> (FrsDb, FrsCfHandle) {
    let base_path = CString::new(dir.path().to_string_lossy().into_owned()).unwrap();
    let mut db: FrsDb = ptr::null_mut();
    assert_eq!(frs_db_open(base_path.as_ptr(), &mut db), FRS_STATUS_OK);
    let mut cf: FrsCfHandle = ptr::null_mut();
    assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
    (db, cf)
}

unsafe fn put_kv(db: FrsDb, cf: FrsCfHandle, key: &str, val: &str) {
    assert_eq!(
        frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()),
        FRS_STATUS_OK
    );
}

unsafe fn assert_get(db: FrsDb, cf: FrsCfHandle, key: &str, expect: &str) {
    let mut out = FrsBytes::default();
    assert_eq!(
        frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
        FRS_STATUS_OK
    );
    assert!(!out.data.is_null(), "missing key {key}");
    let val = slice::from_raw_parts(out.data, out.len);
    assert_eq!(val, expect.as_bytes(), "wrong value for {key}");
    frs_bytes_free(&mut out);
}

/// Takes a linked checkpoint at a fresh snapshot and returns the result
/// (caller frees) plus the checkpoint directory (parent of the manifest).
unsafe fn linked_ckpt(
    db: FrsDb,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
) -> (FrsLinkedCheckpointResult, std::path::PathBuf) {
    let mut snap: FrsSnapshot = ptr::null_mut();
    assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);
    let mut result = std::mem::MaybeUninit::<FrsLinkedCheckpointResult>::uninit();
    assert_eq!(
        frs_create_incremental_checkpoint_linked(
            db,
            snap,
            checkpoint_id,
            base_checkpoint_id,
            result.as_mut_ptr()
        ),
        FRS_STATUS_OK
    );
    assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
    let result = result.assume_init();
    let manifest = std::ffi::CStr::from_ptr(result.manifest_path)
        .to_str()
        .unwrap()
        .to_string();
    let ckpt_dir = std::path::Path::new(&manifest)
        .parent()
        .expect("manifest blob lives in the chk dir")
        .to_path_buf();
    (result, ckpt_dir)
}

/// FLUSH-mode round trip: zero-upload invariant + instant restore +
/// adopted residual + restored-engine writability.
#[test]
fn linked_checkpoint_flush_mode_instant_restore_round_trip() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let (db, cf) = open_local(&dir);
        for i in 0..50u32 {
            put_kv(db, cf, &format!("k{i:03}"), &format!("v{i:03}"));
        }

        let (mut result, ckpt_dir) = linked_ckpt(db, 1, 0);

        // First checkpoint: every linked SST is "new", nothing shared.
        let new_list = &*result.linked_new_ssts;
        let shared_list = &*result.linked_shared_ssts;
        assert!(new_list.count >= 1, "flush-on-barrier must produce SSTs");
        assert_eq!(shared_list.count, 0, "no base ⇒ empty shared list");

        // D1 zero-upload object-count invariant: the chk dir physically
        // contains exactly CHECKPOINT.blob — linked paths are metadata-only.
        let names: Vec<String> = std::fs::read_dir(&ckpt_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["CHECKPOINT.blob"], "chk dir = blob ONLY");
        // The linked logical paths point inside the chk dir but have no bytes.
        let files = slice::from_raw_parts(new_list.files, new_list.count);
        for f in files {
            let p = std::ffi::CStr::from_ptr(f.path).to_str().unwrap();
            assert!(p.starts_with(ckpt_dir.to_str().unwrap()));
            assert!(!std::path::Path::new(p).exists(), "metadata-only path");
            assert!(f.size > 0, "logical size reported from the live set");
        }

        // Instant restore into a fresh target — downloads/copies nothing.
        let target = TempDir::new().unwrap();
        let target_str = target.path().join("restored");
        let ckpt_c = CString::new(ckpt_dir.to_str().unwrap()).unwrap();
        let target_c = CString::new(target_str.to_str().unwrap()).unwrap();
        let mut restored: FrsDb = ptr::null_mut();
        assert_eq!(
            frs_db_open_from_linked_checkpoint_instant(
                ckpt_c.as_ptr(),
                target_c.as_ptr(),
                &mut restored
            ),
            FRS_STATUS_OK
        );
        // Target dir holds only blob + journal — no SST bytes copied.
        let mut tnames: Vec<String> = std::fs::read_dir(&target_str)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        tnames.sort();
        assert_eq!(tnames, vec!["CHECKPOINT.blob", "MAPPING.journal"]);

        let mut rcf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(restored, &mut rcf), FRS_STATUS_OK);
        for i in 0..50u32 {
            assert_get(restored, rcf, &format!("k{i:03}"), &format!("v{i:03}"));
        }

        // CLAIM discipline signal: live SSTs still resolve to the source
        // checkpoint's physicals.
        let mut residual: u64 = 0;
        assert_eq!(
            frs_db_adopted_residual(restored, &mut residual),
            FRS_STATUS_OK
        );
        assert!(residual > 0, "freshly restored engine is not weaned yet");
        // The SOURCE db has no adopted physicals.
        let mut src_residual: u64 = 7;
        assert_eq!(
            frs_db_adopted_residual(db, &mut src_residual),
            FRS_STATUS_OK
        );
        assert_eq!(src_residual, 0);

        // Restored engine is writable.
        put_kv(restored, rcf, "post-restore", "ok");
        assert_get(restored, rcf, "post-restore", "ok");

        assert_eq!(
            frs_db_linked_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        // Idempotent double-free.
        assert_eq!(
            frs_db_linked_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        assert_eq!(frs_db_close(restored), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}

/// WAL-DELTA mode: `frs_db_attach_wal` flips the linked checkpoint to
/// capture the unflushed tail into `WAL.delta` (no forced flush); the
/// instant restore replays it byte-exact above the flushed floor.
#[test]
fn linked_checkpoint_wal_delta_mode_replays_unflushed_tail() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let (db, cf) = open_local(&dir);

        // Flushed floor: a batch that lands in SSTs.
        for i in 0..20u32 {
            put_kv(db, cf, &format!("floor{i:02}"), &format!("f{i:02}"));
        }
        assert_eq!(frs_flush(db), FRS_STATUS_OK);

        // Opt into WAL-DELTA (per-DB, env-free).
        let wal_dir = TempDir::new().unwrap();
        let wal_path = wal_dir.path().join("db.wal");
        let wal_c = CString::new(wal_path.to_str().unwrap()).unwrap();
        assert_eq!(frs_db_attach_wal(db, wal_c.as_ptr()), FRS_STATUS_OK);
        // Double-attach is rejected.
        assert_eq!(
            frs_db_attach_wal(db, wal_c.as_ptr()),
            FRS_STATUS_INVALID_ARGUMENT
        );

        // Unflushed tail: inserts + an overwrite, logged to the WAL.
        for i in 0..15u32 {
            put_kv(db, cf, &format!("tail{i:02}"), &format!("t{i:02}"));
        }
        put_kv(db, cf, "floor00", "overwritten");

        let (mut result, ckpt_dir) = linked_ckpt(db, 5, 0);

        // WAL-DELTA object-count invariant (Phase-5 rotation): the chk dir is
        // physically blob-ONLY; the sealed tail was re-homed to `<db>/wal/`
        // and linked as metadata.
        let names: Vec<String> = std::fs::read_dir(&ckpt_dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["CHECKPOINT.blob"]);
        assert!(
            dir.path().join("wal").join("WAL-000000.seg").exists(),
            "sealed tail re-homed under <db>/wal/"
        );

        // A write AFTER the barrier must not be part of the checkpoint.
        put_kv(db, cf, "after-barrier", "nope");

        let target = TempDir::new().unwrap();
        let target_str = target.path().join("restored");
        let ckpt_c = CString::new(ckpt_dir.to_str().unwrap()).unwrap();
        let target_c = CString::new(target_str.to_str().unwrap()).unwrap();
        let mut restored: FrsDb = ptr::null_mut();
        assert_eq!(
            frs_db_open_from_linked_checkpoint_instant(
                ckpt_c.as_ptr(),
                target_c.as_ptr(),
                &mut restored
            ),
            FRS_STATUS_OK
        );
        let mut rcf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(restored, &mut rcf), FRS_STATUS_OK);

        // Floor rows (minus the overwrite), tail rows, and the overwrite
        // itself are all present; the post-barrier write is absent.
        for i in 1..20u32 {
            assert_get(restored, rcf, &format!("floor{i:02}"), &format!("f{i:02}"));
        }
        for i in 0..15u32 {
            assert_get(restored, rcf, &format!("tail{i:02}"), &format!("t{i:02}"));
        }
        assert_get(restored, rcf, "floor00", "overwritten");
        let mut out = FrsBytes::default();
        let key = "after-barrier";
        assert_eq!(
            frs_get(restored, rcf, key.as_ptr(), key.len(), &mut out),
            FRS_STATUS_OK
        );
        assert!(
            out.data.is_null(),
            "post-barrier write leaked into the checkpoint"
        );

        assert_eq!(
            frs_db_linked_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        assert_eq!(frs_db_close(restored), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}

/// JM-discard delegate: unlink counts reported, working refs keep
/// physicals alive, retried discard → NOT_FOUND.
#[test]
fn discard_linked_checkpoint_reports_and_is_not_retriable() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let (db, cf) = open_local(&dir);
        for i in 0..10u32 {
            put_kv(db, cf, &format!("k{i}"), &format!("v{i}"));
        }
        let (mut result, _ckpt_dir) = linked_ckpt(db, 1, 0);
        let linked_count = (*result.linked_new_ssts).count as u64;
        assert!(linked_count >= 1);

        let mut unlinked: u64 = 0;
        let mut deleted: u64 = 99;
        assert_eq!(
            frs_db_discard_linked_checkpoint(db, 1, &mut unlinked, &mut deleted),
            FRS_STATUS_OK
        );
        assert_eq!(unlinked, linked_count, "every linked path unlinked");
        assert_eq!(
            deleted, 0,
            "working-dir refs still hold every physical — nothing deleted"
        );
        // The working DB still reads its state (physicals alive).
        assert_get(db, cf, "k0", "v0");

        // Retried discard: blob is gone → NOT_FOUND.
        assert_eq!(
            frs_db_discard_linked_checkpoint(db, 1, &mut unlinked, &mut deleted),
            FRS_STATUS_NOT_FOUND
        );

        // Null-arg checks.
        assert_eq!(
            frs_db_discard_linked_checkpoint(ptr::null_mut(), 1, &mut unlinked, &mut deleted),
            FRS_STATUS_NULL_ARG
        );
        let mut out_handle: FrsDb = ptr::null_mut();
        assert_eq!(
            frs_db_open_from_linked_checkpoint_instant(ptr::null(), ptr::null(), &mut out_handle),
            FRS_STATUS_NULL_ARG
        );

        assert_eq!(
            frs_db_linked_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}

/// Startup sweep (D5 crash window a) through the C ABI: an abandoned
/// link-mode checkpoint (blob lost; links durable in the journal) is reaped
/// when its id is not in the JM-live set; working refs keep every physical.
#[test]
fn sweep_abandoned_checkpoints_reaps_journal_only_links() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let (db, cf) = open_local(&dir);
        for i in 0..10u32 {
            put_kv(db, cf, &format!("k{i}"), &format!("v{i}"));
        }
        let (mut result, ckpt_dir) = linked_ckpt(db, 3, 0);
        let linked_count = (*result.linked_new_ssts).count as u64;

        // Simulate the crash window: the blob never made it / the id was
        // never acked — the links live only in the mapping journal.
        std::fs::remove_file(ckpt_dir.join("CHECKPOINT.blob")).unwrap();

        let mut unlinked: u64 = 0;
        let mut deleted: u64 = 99;
        assert_eq!(
            frs_db_sweep_abandoned_checkpoints(db, ptr::null(), 0, &mut unlinked, &mut deleted),
            FRS_STATUS_OK
        );
        assert_eq!(unlinked, linked_count, "abandoned chk-3 links reaped");
        assert_eq!(deleted, 0, "working refs hold every physical");
        // The working DB still reads its state.
        assert_get(db, cf, "k0", "v0");

        // Idempotent.
        assert_eq!(
            frs_db_sweep_abandoned_checkpoints(db, ptr::null(), 0, &mut unlinked, &mut deleted),
            FRS_STATUS_OK
        );
        assert_eq!(unlinked, 0);

        // Live-set protection: a fresh checkpoint id=4 survives a sweep that
        // lists it live.
        let (mut r4, _d4) = linked_ckpt(db, 4, 0);
        let live = [4u64];
        assert_eq!(
            frs_db_sweep_abandoned_checkpoints(db, live.as_ptr(), 1, &mut unlinked, &mut deleted),
            FRS_STATUS_OK
        );
        assert_eq!(unlinked, 0, "live id untouched");

        assert_eq!(
            frs_db_linked_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        assert_eq!(frs_db_linked_checkpoint_result_free(&mut r4), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}

unsafe fn put_raw(db: FrsDb, cf: FrsCfHandle, key: &[u8], val: &[u8]) {
    assert_eq!(
        frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()),
        FRS_STATUS_OK
    );
}

unsafe fn get_raw_present(db: FrsDb, cf: FrsCfHandle, key: &[u8]) -> bool {
    let mut out = FrsBytes::default();
    assert_eq!(
        frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
        FRS_STATUS_OK
    );
    !out.data.is_null()
}

/// FRS-PHASE2-C2U3 (rescale-by-clip) FFI round trip: build a checkpoint
/// spanning four 2-byte big-endian "key-group" prefixes, then restore only
/// the `[0x0001, 0x0003)` sub-range. Keys in groups 1–2 survive; groups 0
/// and 3 are clipped out (file-level + read-path). Mirrors the backend's
/// composite-key encoding `[kg_be16][user-key]`.
#[test]
fn linked_checkpoint_instant_clipped_adopts_only_assigned_range() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let (db, cf) = open_local(&dir);
        // Four groups, distinct big-endian prefixes; flush so they land in
        // SSTs (the file-level clip operates on the restored Version).
        for kg in 0u16..4 {
            for i in 0..8u32 {
                let mut key = kg.to_be_bytes().to_vec();
                key.extend_from_slice(format!("-user{i:02}").as_bytes());
                put_raw(db, cf, &key, format!("val-{kg}-{i}").as_bytes());
            }
        }
        assert_eq!(frs_flush(db), FRS_STATUS_OK);

        let (mut result, ckpt_dir) = linked_ckpt(db, 1, 0);

        // Clip to assigned key-group sub-range [1, 3): start = 0x0001,
        // end (exclusive) = 0x0003.
        let clip_start = 1u16.to_be_bytes();
        let clip_end = 3u16.to_be_bytes();

        let target = TempDir::new().unwrap();
        let target_str = target.path().join("restored-clip");
        let ckpt_c = CString::new(ckpt_dir.to_str().unwrap()).unwrap();
        let target_c = CString::new(target_str.to_str().unwrap()).unwrap();
        let mut restored: FrsDb = ptr::null_mut();
        assert_eq!(
            frs_db_open_from_linked_checkpoint_instant_clipped(
                ckpt_c.as_ptr(),
                target_c.as_ptr(),
                clip_start.as_ptr(),
                clip_start.len(),
                clip_end.as_ptr(),
                clip_end.len(),
                &mut restored
            ),
            FRS_STATUS_OK
        );

        let mut rcf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(restored, &mut rcf), FRS_STATUS_OK);

        // Groups 1 and 2 are present; groups 0 and 3 are clipped out.
        for kg in 0u16..4 {
            let in_range = kg == 1 || kg == 2;
            for i in 0..8u32 {
                let mut key = kg.to_be_bytes().to_vec();
                key.extend_from_slice(format!("-user{i:02}").as_bytes());
                assert_eq!(
                    get_raw_present(restored, rcf, &key),
                    in_range,
                    "kg={kg} i={i} expected present={in_range}"
                );
            }
        }

        // Empty clip (start >= end) is rejected at the FFI boundary.
        let mut bad: FrsDb = ptr::null_mut();
        let empty_lo = 3u16.to_be_bytes();
        let empty_hi = 1u16.to_be_bytes();
        assert_eq!(
            frs_db_open_from_linked_checkpoint_instant_clipped(
                ckpt_c.as_ptr(),
                target_c.as_ptr(),
                empty_lo.as_ptr(),
                empty_lo.len(),
                empty_hi.as_ptr(),
                empty_hi.len(),
                &mut bad
            ),
            FRS_STATUS_INVALID_ARGUMENT
        );

        assert_eq!(
            frs_db_linked_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        assert_eq!(frs_db_close(restored), FRS_STATUS_OK);
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
    }
}

/// `frs_set_env` plumbs a process env var the engine reads (the backend's
/// feature-flag bridge). Null args and malformed names are rejected without
/// aborting; a valid set is observable via `std::env::var`.
#[test]
fn set_env_plumbs_and_rejects_bad_input() {
    unsafe {
        // Use a test-private name to avoid perturbing the engine's cached
        // feature OnceLocks in this shared test process.
        let name = CString::new("FRS_FFI_SET_ENV_PROBE").unwrap();
        let value = CString::new("hello").unwrap();
        assert_eq!(frs_set_env(name.as_ptr(), value.as_ptr()), FRS_STATUS_OK);
        assert_eq!(std::env::var("FRS_FFI_SET_ENV_PROBE").unwrap(), "hello");

        // Null pointers.
        assert_eq!(
            frs_set_env(ptr::null(), value.as_ptr()),
            FRS_STATUS_NULL_ARG
        );
        assert_eq!(frs_set_env(name.as_ptr(), ptr::null()), FRS_STATUS_NULL_ARG);

        // Empty name and `=`-bearing name are rejected (would panic set_var).
        let empty = CString::new("").unwrap();
        assert_eq!(
            frs_set_env(empty.as_ptr(), value.as_ptr()),
            FRS_STATUS_INVALID_ARGUMENT
        );
        let eq_name = CString::new("BAD=NAME").unwrap();
        assert_eq!(
            frs_set_env(eq_name.as_ptr(), value.as_ptr()),
            FRS_STATUS_INVALID_ARGUMENT
        );
    }
}

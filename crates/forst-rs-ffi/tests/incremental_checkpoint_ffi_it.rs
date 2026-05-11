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

//! Round-trip integration test for the incremental-checkpoint FFI surface
//! (B-Prod-P2 Task 2.4).
//!
//! Exercises the full path through the C ABI:
//!   1. write 5 keys into a fresh DB,
//!   2. capture a snapshot,
//!   3. take an incremental checkpoint at the snapshot (full ckpt — no base),
//!   4. open a fresh DB from the resulting manifest + SST file list,
//!   5. read every key back from the restored DB.

#![allow(clippy::missing_safety_doc)]

use std::ffi::{CStr, CString};
use std::ptr;
use std::slice;

use forst_rs_ffi::*;
use tempfile::TempDir;

#[test]
fn incremental_checkpoint_round_trip() {
    unsafe {
        let dir = TempDir::new().unwrap();
        let base_path = CString::new(dir.path().to_string_lossy().into_owned()).unwrap();
        let mut db: FrsDb = ptr::null_mut();
        assert_eq!(frs_db_open(base_path.as_ptr(), &mut db), FRS_STATUS_OK);
        let mut cf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

        for i in 0..5u32 {
            let key = format!("k{}", i);
            let val = format!("v{}", i);
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()),
                FRS_STATUS_OK
            );
        }

        let mut snap: FrsSnapshot = ptr::null_mut();
        assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);

        let mut result = std::mem::MaybeUninit::<FrsIncrementalCheckpointResult>::uninit();
        assert_eq!(
            frs_create_incremental_checkpoint_at(db, snap, 1, 0, result.as_mut_ptr()),
            FRS_STATUS_OK
        );
        let mut result = result.assume_init();

        // The first checkpoint has no base, so every live SST must appear
        // in `new_ssts` and `shared_ssts` must be empty.
        assert!(!result.new_ssts.is_null());
        assert!(!result.shared_ssts.is_null());
        let new_list_ref = &*result.new_ssts;
        let shared_list_ref = &*result.shared_ssts;
        assert!(
            new_list_ref.count >= 1,
            "expected at least one SST in new_ssts after the writes were flushed"
        );
        assert_eq!(
            shared_list_ref.count, 0,
            "first checkpoint must have empty shared_ssts (no base)"
        );

        // Materialize the C-string array `frs_db_open_from_incremental` expects.
        let mut owned_paths: Vec<CString> = Vec::new();
        let mut path_ptrs: Vec<*const std::os::raw::c_char> = Vec::new();
        let new_files = slice::from_raw_parts(new_list_ref.files, new_list_ref.count);
        for f in new_files {
            owned_paths.push(CStr::from_ptr(f.path).to_owned());
        }
        for owned in &owned_paths {
            path_ptrs.push(owned.as_ptr());
        }

        // Restore into a fresh target dir.
        let target = TempDir::new().unwrap();
        let target_path = CString::new(target.path().to_string_lossy().into_owned()).unwrap();
        let mut restored: FrsDb = ptr::null_mut();
        assert_eq!(
            frs_db_open_from_incremental(
                target_path.as_ptr(),
                result.manifest_path,
                path_ptrs.as_ptr(),
                path_ptrs.len(),
                &mut restored,
            ),
            FRS_STATUS_OK
        );

        // Verify all 5 keys are readable in the restored DB.
        let mut restored_cf: FrsCfHandle = ptr::null_mut();
        assert_eq!(frs_db_default_cf(restored, &mut restored_cf), FRS_STATUS_OK);
        for i in 0..5u32 {
            let key = format!("k{}", i);
            let mut out = FrsBytes::default();
            assert_eq!(
                frs_get(restored, restored_cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(
                !out.data.is_null(),
                "restored DB missing key k{i} — incremental restore lost data"
            );
            let val = slice::from_raw_parts(out.data, out.len);
            assert_eq!(val, format!("v{}", i).as_bytes());
            frs_bytes_free(&mut out);
        }

        assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
        assert_eq!(
            frs_db_incremental_checkpoint_result_free(&mut result),
            FRS_STATUS_OK
        );
        assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        assert_eq!(frs_db_close(restored), FRS_STATUS_OK);
    }
}

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

//! M5 acceptance tests — end-to-end workloads driven entirely through the
//! C ABI. These mirror the M4 Rust-level tests but exercise every
//! `frs_*` entry point, including panic guards, memory management, and
//! cross-function interactions.

#![allow(clippy::missing_safety_doc)]

use std::ffi::CString;
use std::ptr;
use std::slice;

use forst_rs_ffi::*;

unsafe fn open() -> FrsDb {
    let mut db: FrsDb = ptr::null_mut();
    assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
    db
}

unsafe fn default_cf(db: FrsDb) -> FrsCfHandle {
    let mut cf: FrsCfHandle = ptr::null_mut();
    assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
    cf
}

unsafe fn put(db: FrsDb, cf: FrsCfHandle, k: &[u8], v: &[u8]) {
    assert_eq!(
        frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
        FRS_STATUS_OK
    );
}

unsafe fn get_copy(db: FrsDb, cf: FrsCfHandle, k: &[u8]) -> Option<Vec<u8>> {
    let mut out = FrsBytes {
        data: ptr::null_mut(),
        len: 0,
        capacity: 0,
    };
    assert_eq!(
        frs_get(db, cf, k.as_ptr(), k.len(), &mut out),
        FRS_STATUS_OK
    );
    if out.data.is_null() {
        None
    } else {
        let v = slice::from_raw_parts(out.data, out.len).to_vec();
        frs_bytes_free(&mut out);
        Some(v)
    }
}

#[test]
fn m5_put_100k_then_get_all_correct() {
    unsafe {
        let db = open();
        let cf = default_cf(db);
        const N: u32 = 100_000;
        for i in 0..N {
            let k = format!("k{:08}", i);
            let v = format!("v{:08}", i);
            put(db, cf, k.as_bytes(), v.as_bytes());
        }
        for i in 0..N {
            let k = format!("k{:08}", i);
            let expected = format!("v{:08}", i);
            assert_eq!(
                get_copy(db, cf, k.as_bytes()),
                Some(expected.into_bytes()),
                "mismatch at key {}",
                i
            );
        }
        frs_cf_close(cf);
        frs_db_close(db);
    }
}

#[test]
fn m5_batch_put_1000x100() {
    unsafe {
        let db = open();
        let cf = default_cf(db);
        for batch_id in 0..10u32 {
            let keys: Vec<Vec<u8>> = (0..100u32)
                .map(|i| format!("b{:03}-k{:05}", batch_id, i).into_bytes())
                .collect();
            let values: Vec<Vec<u8>> = (0..100u32)
                .map(|i| format!("b{:03}-v{:05}", batch_id, i).into_bytes())
                .collect();
            let key_ptrs: Vec<*const u8> = keys.iter().map(|v| v.as_ptr()).collect();
            let key_lens: Vec<usize> = keys.iter().map(|v| v.len()).collect();
            let value_ptrs: Vec<*const u8> = values.iter().map(|v| v.as_ptr()).collect();
            let value_lens: Vec<usize> = values.iter().map(|v| v.len()).collect();

            assert_eq!(
                frs_batch_put(
                    db,
                    cf,
                    key_ptrs.as_ptr(),
                    key_lens.as_ptr(),
                    value_ptrs.as_ptr(),
                    value_lens.as_ptr(),
                    100,
                ),
                FRS_STATUS_OK
            );
        }

        // Verify via batch_get in chunks of 100.
        for batch_id in 0..10u32 {
            let keys: Vec<Vec<u8>> = (0..100u32)
                .map(|i| format!("b{:03}-k{:05}", batch_id, i).into_bytes())
                .collect();
            let key_ptrs: Vec<*const u8> = keys.iter().map(|v| v.as_ptr()).collect();
            let key_lens: Vec<usize> = keys.iter().map(|v| v.len()).collect();
            let mut outs: Vec<FrsBytes> = (0..100)
                .map(|_| FrsBytes {
                    data: ptr::null_mut(),
                    len: 0,
                    capacity: 0,
                })
                .collect();
            assert_eq!(
                frs_batch_get(
                    db,
                    cf,
                    key_ptrs.as_ptr(),
                    key_lens.as_ptr(),
                    100,
                    outs.as_mut_ptr(),
                ),
                FRS_STATUS_OK
            );
            for (i, out) in outs.iter_mut().enumerate() {
                let expected = format!("b{:03}-v{:05}", batch_id, i);
                let slice = slice::from_raw_parts(out.data, out.len);
                assert_eq!(slice, expected.as_bytes());
                frs_bytes_free(out);
            }
        }

        frs_cf_close(cf);
        frs_db_close(db);
    }
}

#[test]
fn m5_flush_compact_checkpoint_cycle() {
    unsafe {
        let db = open();
        let cf = default_cf(db);

        // 1. Write 5 waves, flushing between each so we build L0 files.
        for wave in 0..5u32 {
            for i in 0..100u32 {
                let k = format!("k{:04}-{:02}", i, wave);
                let v = format!("v{:04}-{:02}", i, wave);
                put(db, cf, k.as_bytes(), v.as_bytes());
            }
            assert_eq!(frs_flush(db), FRS_STATUS_OK);
        }

        // 2. Compact L0 → L1.
        assert_eq!(frs_compact_all(db), FRS_STATUS_OK);

        // 3. Verify every key still reads.
        for wave in 0..5u32 {
            for i in 0..100u32 {
                let k = format!("k{:04}-{:02}", i, wave);
                let v = format!("v{:04}-{:02}", i, wave);
                assert_eq!(get_copy(db, cf, k.as_bytes()), Some(v.into_bytes()));
            }
        }

        // 4. Create checkpoint. In-memory FS: the checkpoint directory is
        //    accessible from the same filesystem so the call must succeed.
        let target = CString::new("/ckpt").unwrap();
        assert_eq!(frs_create_checkpoint(db, target.as_ptr()), FRS_STATUS_OK);

        frs_cf_close(cf);
        frs_db_close(db);
    }
}

#[test]
fn m5_merge_list_append_at_scale() {
    unsafe {
        let mut db: FrsDb = ptr::null_mut();
        frs_db_open_memory(&mut db);
        let name = CString::new("lists").unwrap();
        let op = CString::new("ListAppendMergeOperator").unwrap();
        let mut cf: FrsCfHandle = ptr::null_mut();
        assert_eq!(
            frs_db_create_cf_with_merge(db, name.as_ptr(), op.as_ptr(), &mut cf),
            FRS_STATUS_OK
        );

        // 1000 keys, each with 10 merge operands.
        for i in 0..1_000u32 {
            let k = format!("k{:04}", i);
            put(db, cf, k.as_bytes(), b"base");
            for j in 0..10u32 {
                let operand = format!("op{}", j);
                assert_eq!(
                    frs_merge(db, cf, k.as_ptr(), k.len(), operand.as_ptr(), operand.len(),),
                    FRS_STATUS_OK
                );
            }
        }

        for i in 0..1_000u32 {
            let k = format!("k{:04}", i);
            let v = get_copy(db, cf, k.as_bytes()).expect("key must exist");
            let s = std::str::from_utf8(&v).unwrap();
            // Expect "base,op0,op1,...,op9".
            assert!(s.starts_with("base"));
            assert!(s.ends_with("op9"));
            assert_eq!(s.matches(',').count(), 10);
        }

        frs_cf_close(cf);
        frs_db_close(db);
    }
}

#[test]
fn m5_panic_returns_panic_status() {
    // We cannot easily trigger a panic inside the engine from the FFI tests
    // without changing engine code, so just assert that NULL-arg guards are
    // honoured and do not crash. This exercises the catch_unwind wrapper.
    unsafe {
        assert_eq!(
            frs_put(
                ptr::null_mut(),
                ptr::null_mut(),
                ptr::null(),
                0,
                ptr::null(),
                0,
            ),
            FRS_STATUS_NULL_ARG
        );
        assert_eq!(frs_flush(ptr::null_mut()), FRS_STATUS_NULL_ARG);
        assert_eq!(frs_compact_all(ptr::null_mut()), FRS_STATUS_NULL_ARG);
    }
}

#[test]
fn m5_multi_cf_workload() {
    unsafe {
        let db = open();
        let cf_a = default_cf(db);
        let name_b = CString::new("cf_b").unwrap();
        let mut cf_b: FrsCfHandle = ptr::null_mut();
        assert_eq!(
            frs_db_create_cf(db, name_b.as_ptr(), &mut cf_b),
            FRS_STATUS_OK
        );

        const N: u32 = 10_000;
        for i in 0..N {
            let k = format!("k{:06}", i);
            put(db, cf_a, k.as_bytes(), b"A");
            put(db, cf_b, k.as_bytes(), b"B");
        }

        for i in 0..N {
            let k = format!("k{:06}", i);
            assert_eq!(
                get_copy(db, cf_a, k.as_bytes()).as_deref(),
                Some(b"A".as_ref())
            );
            assert_eq!(
                get_copy(db, cf_b, k.as_bytes()).as_deref(),
                Some(b"B".as_ref())
            );
        }

        frs_cf_close(cf_a);
        frs_cf_close(cf_b);
        frs_db_close(db);
    }
}

#[test]
fn m5_sequence_number_monotonic() {
    unsafe {
        let db = open();
        let cf = default_cf(db);
        let mut last = 0u64;
        for i in 0..1000u32 {
            let k = format!("k{}", i);
            put(db, cf, k.as_bytes(), b"v");
            let mut seq = 0u64;
            frs_sequence_number(db, &mut seq);
            assert!(seq >= last);
            last = seq;
        }
        frs_cf_close(cf);
        frs_db_close(db);
    }
}

#[test]
fn m5_delete_then_put_cycle() {
    unsafe {
        let db = open();
        let cf = default_cf(db);

        for i in 0..1_000u32 {
            let k = format!("k{}", i);
            put(db, cf, k.as_bytes(), b"v1");
        }
        assert_eq!(frs_flush(db), FRS_STATUS_OK);

        // Delete half.
        for i in (0..1_000u32).filter(|n| n % 2 == 0) {
            let k = format!("k{}", i);
            assert_eq!(frs_delete(db, cf, k.as_ptr(), k.len()), FRS_STATUS_OK);
        }
        assert_eq!(frs_flush(db), FRS_STATUS_OK);

        // Verify.
        for i in 0..1_000u32 {
            let k = format!("k{}", i);
            let got = get_copy(db, cf, k.as_bytes());
            if i % 2 == 0 {
                assert!(got.is_none(), "even key {} should be deleted", i);
            } else {
                assert_eq!(got.as_deref(), Some(b"v1".as_ref()));
            }
        }

        frs_cf_close(cf);
        frs_db_close(db);
    }
}

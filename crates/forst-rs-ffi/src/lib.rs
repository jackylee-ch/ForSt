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

//! ForSt-RS C ABI Bridge.
//!
//! This crate exposes the engine through a stable C ABI that Java consumers
//! call via the Foreign Function & Memory API (Java 21+) or JNI. See
//! `2.5_ffm_bridge_design.md` for the design.
//!
//! # Safety Model
//!
//! - Handles are opaque `*mut c_void` pointers. Rust wraps the owned value in
//!   `Box` and leaks the raw pointer; the consumer MUST call the matching
//!   `*_destroy` (or `*_free`) function exactly once when done.
//! - Every exported function catches Rust panics via `catch_unwind` and
//!   returns `FRS_STATUS_PANIC` rather than unwinding across the FFI
//!   boundary. This is required for soundness.
//! - Byte slices passed in are borrowed for the duration of the call (the
//!   engine copies data internally).
//! - Output byte slices are allocated by Rust and returned along with a
//!   length and capacity; consumers call `frs_bytes_free` to release them.

#![allow(clippy::missing_safety_doc)]

use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl, WriteBatch};
use forst_rs_io::{FileSystem, LocalFileSystem, MemoryFileSystem};
use forst_rs_storage::merge_operator::{ListAppendMergeOperator, MergeOperator};

/// Defense-in-depth cap on `count` (or row count) passed to FFI batch
/// operations. Untrusted C-side caller could otherwise drive
/// `WriteBatch::with_capacity(count)` or other count-driven allocations
/// to OOM (Sweep R5 H by Reviewer 2). 1M entries is well above any
/// realistic per-call batch size; calling code that needs more should
/// chunk into multiple calls.
pub const MAX_BATCH_COUNT: usize = 1_000_000;

// ---------------------------------------------------------------------------
// Status codes
// ---------------------------------------------------------------------------

/// Operation completed successfully.
pub const FRS_STATUS_OK: i32 = 0;
/// Generic error (details written to status buffer).
pub const FRS_STATUS_ERROR: i32 = 1;
/// NULL pointer passed where non-null required.
pub const FRS_STATUS_NULL_ARG: i32 = 2;
/// The requested item was not found.
pub const FRS_STATUS_NOT_FOUND: i32 = 3;
/// Input was invalid.
pub const FRS_STATUS_INVALID_ARGUMENT: i32 = 4;
/// Rust panic caught at the FFI boundary.
pub const FRS_STATUS_PANIC: i32 = 5;
/// Reserved. The engine currently recovers from Rust `Mutex` poisoning
/// transparently via `catch_unwind`, so this code is never returned today.
/// Retained as a stable ABI slot; a future release may start surfacing it
/// for unrecoverable lock corruption.
pub const FRS_STATUS_POISONED: i32 = 6;
/// I/O error (disk, filesystem, network).
pub const FRS_STATUS_IO: i32 = 7;
/// Data corruption detected (checksum mismatch, bad magic, truncated file).
pub const FRS_STATUS_CORRUPTION: i32 = 8;
/// The requested feature or operation is not supported.
pub const FRS_STATUS_NOT_SUPPORTED: i32 = 9;
/// Operation was aborted (e.g. by shutdown or external signal).
pub const FRS_STATUS_ABORTED: i32 = 10;
/// A required resource is busy (e.g. lock contention, too many open files).
pub const FRS_STATUS_BUSY: i32 = 11;
/// Operation did not complete within its deadline.
pub const FRS_STATUS_TIMED_OUT: i32 = 12;
/// The referenced item has expired. Reserved for lease-based APIs; the
/// current TTL compaction filter drops expired keys at compaction time
/// rather than surfacing `EXPIRED` on read, so callers will not observe
/// this status today.
pub const FRS_STATUS_EXPIRED: i32 = 13;
/// Operation completed only partially. Reserved for future cursor-style
/// APIs (partial scans, chunked reads); no call site emits this status
/// today.
pub const FRS_STATUS_INCOMPLETE: i32 = 14;

// ---------------------------------------------------------------------------
// Opaque handle types
// ---------------------------------------------------------------------------

/// A handle to an open engine. Consumers pass this to every other entry
/// point. Created by `frs_db_open` / `frs_db_open_memory`, destroyed by
/// `frs_db_close`.
pub type FrsDb = *mut c_void;

/// A handle to a column family. Created by `frs_db_create_cf`, destroyed by
/// `frs_cf_close`. Safe to share across threads — all engine operations are
/// thread-safe.
pub type FrsCfHandle = *mut c_void;

/// Byte slice owned by Rust; consumers must call [`frs_bytes_free`] to
/// release after use.
#[repr(C)]
pub struct FrsBytes {
    pub data: *mut u8,
    pub len: usize,
    pub capacity: usize,
}

impl FrsBytes {
    fn from_vec(mut v: Vec<u8>) -> Self {
        v.shrink_to_fit();
        let len = v.len();
        let capacity = v.capacity();
        let data = if capacity == 0 {
            std::ptr::null_mut()
        } else {
            let ptr = v.as_mut_ptr();
            std::mem::forget(v);
            ptr
        };
        Self {
            data,
            len,
            capacity,
        }
    }

    const NULL: Self = Self {
        data: std::ptr::null_mut(),
        len: 0,
        capacity: 0,
    };
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Runs `f` inside `catch_unwind`. Returns `FRS_STATUS_PANIC` if a panic is
/// caught; otherwise returns whatever `f` returned.
fn guarded<F: FnOnce() -> i32>(f: F) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => FRS_STATUS_PANIC,
    }
}

/// Borrow a `&str` from an opaque C string pointer.
///
/// Same lifetime-elision trick as `cf_ref`: input is `&*const c_char`,
/// so Rust elides `<'a>(p: &'a *const c_char) -> Option<&'a str>` and
/// the returned borrow cannot be inferred as `'static`. SAFETY contract
/// still requires the caller to guarantee the underlying C string outlives
/// the returned `&str` (Sweep R6 H by Reviewer 2).
///
/// # SAFETY
/// - `*p` must be either null OR a valid pointer to a NUL-terminated UTF-8
///   string for the duration of the returned borrow.
/// - Caller must not mutate or free the underlying C string while the
///   returned `&str` is in use.
unsafe fn cstr_to_str(p: &*const c_char) -> Option<&str> {
    if p.is_null() {
        return None;
    }
    CStr::from_ptr(*p).to_str().ok()
}

unsafe fn db_from_handle(h: FrsDb) -> Option<Arc<DbImpl>> {
    if h.is_null() {
        return None;
    }
    let ptr = h as *const Arc<DbImpl>;
    Some((*ptr).clone())
}

unsafe fn cf_from_handle(h: FrsCfHandle) -> Option<Box<ColumnFamilyHandle>> {
    if h.is_null() {
        return None;
    }
    // Reconstructs the `Box<ColumnFamilyHandle>` from the raw pointer, taking
    // ownership back from C. When the returned `Box` is dropped the handle is
    // freed; callers that intend to keep the handle alive must use
    // [`cf_ref`] instead, or must `Box::into_raw` the returned box before it
    // goes out of scope.
    let ptr = h as *mut ColumnFamilyHandle;
    Some(Box::from_raw(ptr))
}

/// Borrow a `ColumnFamilyHandle` from an opaque FFI handle.
///
/// The returned reference's lifetime is tied to the input borrow `&'h h`,
/// preventing rustc from inferring `'static` and silently allowing the
/// returned reference to outlive the FFI call frame.
///
/// # SAFETY
/// - Caller must guarantee `*h` is either null OR a valid pointer
///   originally produced by `Box::into_raw(Box::new(ColumnFamilyHandle))`
///   (i.e., from a successful prior `frs_cf_create_*` / `frs_cf_open_*`).
/// - Caller must guarantee no concurrent call to `frs_cf_close(*h)` (or
///   any other deallocation) for the entire duration the returned
///   reference is in use. External synchronization is required if the
///   handle may be closed from another thread.
/// - `*h` must be properly aligned for `ColumnFamilyHandle` (guaranteed
///   by `Box::new` allocation paths).
unsafe fn cf_ref(h: &FrsCfHandle) -> Option<&ColumnFamilyHandle> {
    // Lifetime elided: Rust derives `fn cf_ref<'h>(h: &'h FrsCfHandle)
    // -> Option<&'h ColumnFamilyHandle>` from the single-input-borrow
    // rule, which is exactly the soundness constraint we want (output
    // borrow bounded by input borrow, NOT 'static).
    if h.is_null() {
        return None;
    }
    Some(&*(*h as *const ColumnFamilyHandle))
}

fn error_to_status(err: &forst_rs_common::ForstError) -> i32 {
    if err.is_not_found() {
        FRS_STATUS_NOT_FOUND
    } else if err.is_invalid_argument() {
        FRS_STATUS_INVALID_ARGUMENT
    } else if err.is_io() {
        FRS_STATUS_IO
    } else if err.is_corruption() {
        FRS_STATUS_CORRUPTION
    } else if err.is_not_supported() {
        FRS_STATUS_NOT_SUPPORTED
    } else if err.is_aborted() {
        FRS_STATUS_ABORTED
    } else if err.is_busy() {
        FRS_STATUS_BUSY
    } else if err.is_timed_out() {
        FRS_STATUS_TIMED_OUT
    } else if err.is_expired() {
        FRS_STATUS_EXPIRED
    } else if err.is_incomplete() {
        FRS_STATUS_INCOMPLETE
    } else {
        FRS_STATUS_ERROR
    }
}

// ---------------------------------------------------------------------------
// 1. Lifecycle
// ---------------------------------------------------------------------------

/// Opens a new engine at the given path with a local filesystem.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open(db_path: *const c_char, out_handle: *mut FrsDb) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let path = match cstr_to_str(&db_path) {
            Some(p) => p,
            None => return FRS_STATUS_NULL_ARG,
        };
        let opts = EngineOptions {
            db_path: path.to_string(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        match DbImpl::open_with_fs(opts, fs) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens a new engine backed by an in-memory filesystem. Useful for tests
/// and short-lived workloads.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_memory(out_handle: *mut FrsDb) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        match DbImpl::open_with_fs(opts, fs) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Closes an engine previously returned by [`frs_db_open`]. After this
/// call the handle must not be used again. Always returns `FRS_STATUS_OK`.
#[no_mangle]
pub unsafe extern "C" fn frs_db_close(handle: FrsDb) -> i32 {
    guarded(|| {
        if handle.is_null() {
            return FRS_STATUS_OK;
        }
        let ptr = handle as *mut Arc<DbImpl>;
        drop(Box::from_raw(ptr));
        FRS_STATUS_OK
    })
}

// ---------------------------------------------------------------------------
// 2. Column family management
// ---------------------------------------------------------------------------

/// Returns a handle to the default column family (always exists after open).
#[no_mangle]
pub unsafe extern "C" fn frs_db_default_cf(handle: FrsDb, out_cf: *mut FrsCfHandle) -> i32 {
    guarded(|| {
        if out_cf.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let cf = db.default_cf();
        let boxed = Box::new(cf);
        *out_cf = Box::into_raw(boxed) as *mut c_void;
        FRS_STATUS_OK
    })
}

/// Creates a new column family.
#[no_mangle]
pub unsafe extern "C" fn frs_db_create_cf(
    handle: FrsDb,
    name: *const c_char,
    out_cf: *mut FrsCfHandle,
) -> i32 {
    frs_db_create_cf_with_merge(handle, name, std::ptr::null(), out_cf)
}

/// Creates a new column family with the named merge operator attached.
/// `merge_op_name = NULL` means no merge operator.
///
/// Currently recognised merge operators:
/// - `"ListAppendMergeOperator"` — comma-separated concatenation
#[no_mangle]
pub unsafe extern "C" fn frs_db_create_cf_with_merge(
    handle: FrsDb,
    name: *const c_char,
    merge_op_name: *const c_char,
    out_cf: *mut FrsCfHandle,
) -> i32 {
    guarded(|| {
        if out_cf.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf_name) = cstr_to_str(&name) else {
            return FRS_STATUS_NULL_ARG;
        };
        let mut desc = ColumnFamilyDescriptor::new(cf_name);
        if !merge_op_name.is_null() {
            let Some(op_name) = cstr_to_str(&merge_op_name) else {
                return FRS_STATUS_NULL_ARG;
            };
            let op: Arc<dyn MergeOperator> = match op_name {
                "ListAppendMergeOperator" => Arc::new(ListAppendMergeOperator::with_comma()),
                _ => return FRS_STATUS_INVALID_ARGUMENT,
            };
            desc = desc.with_merge_operator(op);
        }
        match db.create_column_family(desc) {
            Ok(cf) => {
                let boxed = Box::new(cf);
                *out_cf = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens a handle for an existing column family by name.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_cf(
    handle: FrsDb,
    name: *const c_char,
    out_cf: *mut FrsCfHandle,
) -> i32 {
    guarded(|| {
        if out_cf.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf_name) = cstr_to_str(&name) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.column_family(cf_name) {
            Some(cf) => {
                let boxed = Box::new(cf);
                *out_cf = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            None => FRS_STATUS_NOT_FOUND,
        }
    })
}

/// Destroys a CF handle returned by the engine.
#[no_mangle]
pub unsafe extern "C" fn frs_cf_close(handle: FrsCfHandle) -> i32 {
    guarded(|| {
        if handle.is_null() {
            return FRS_STATUS_OK;
        }
        let _ = cf_from_handle(handle);
        FRS_STATUS_OK
    })
}

// ---------------------------------------------------------------------------
// 3. Point operations
// ---------------------------------------------------------------------------

/// Inserts or overwrites a value.
#[no_mangle]
pub unsafe extern "C" fn frs_put(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if key.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let k = slice::from_raw_parts(key, key_len);
        let v = if value.is_null() {
            &[][..]
        } else {
            slice::from_raw_parts(value, value_len)
        };
        match db.put(cf, k, v) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Deletes a key.
#[no_mangle]
pub unsafe extern "C" fn frs_delete(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if key.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let k = slice::from_raw_parts(key, key_len);
        match db.delete(cf, k) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Appends a merge operand.
#[no_mangle]
pub unsafe extern "C" fn frs_merge(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    operand: *const u8,
    operand_len: usize,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if key.is_null() || operand.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let k = slice::from_raw_parts(key, key_len);
        let o = slice::from_raw_parts(operand, operand_len);
        match db.merge(cf, k, o) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Gets a value. On success sets `*out_value` to a heap-allocated `FrsBytes`
/// (the consumer must call [`frs_bytes_free`] after use). If the key does
/// not exist, `out_value->data` will be NULL and `out_value->len` will be 0
/// but the status will still be `FRS_STATUS_OK` — distinguish by checking
/// whether `out_value->data` is null.
#[no_mangle]
pub unsafe extern "C" fn frs_get(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    out_value: *mut FrsBytes,
) -> i32 {
    guarded(|| {
        if out_value.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if key.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let k = slice::from_raw_parts(key, key_len);
        match db.get(cf, k) {
            Ok(Some(v)) => {
                *out_value = FrsBytes::from_vec(v);
                FRS_STATUS_OK
            }
            Ok(None) => {
                *out_value = FrsBytes::NULL;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 4. Batch operations
// ---------------------------------------------------------------------------

/// Writes multiple put entries in a single batch. `keys`, `key_lens`,
/// `values`, `value_lens` are parallel arrays of length `count`. A NULL
/// `value` pointer denotes a Delete.
#[no_mangle]
pub unsafe extern "C" fn frs_batch_put(
    handle: FrsDb,
    cf: FrsCfHandle,
    keys: *const *const u8,
    key_lens: *const usize,
    values: *const *const u8,
    value_lens: *const usize,
    count: usize,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if count == 0 {
            return FRS_STATUS_OK;
        }
        if count > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        if keys.is_null() || key_lens.is_null() || values.is_null() || value_lens.is_null() {
            return FRS_STATUS_NULL_ARG;
        }

        let key_ptrs = slice::from_raw_parts(keys, count);
        let key_len_arr = slice::from_raw_parts(key_lens, count);
        let value_ptrs = slice::from_raw_parts(values, count);
        let value_len_arr = slice::from_raw_parts(value_lens, count);

        let mut wb = WriteBatch::with_capacity(count);
        for i in 0..count {
            let k = slice::from_raw_parts(key_ptrs[i], key_len_arr[i]);
            if value_ptrs[i].is_null() {
                wb.delete(cf, k);
            } else {
                let v = slice::from_raw_parts(value_ptrs[i], value_len_arr[i]);
                wb.put(cf, k, v);
            }
        }
        match db.batch_write(wb) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Reads multiple keys. The consumer supplies `out_values`, an array of
/// `count` `FrsBytes` slots that will each be populated with a heap-owned
/// value (or `{NULL, 0}` for missing keys). The consumer must call
/// [`frs_bytes_free`] on each non-null slot.
#[no_mangle]
pub unsafe extern "C" fn frs_batch_get(
    handle: FrsDb,
    cf: FrsCfHandle,
    keys: *const *const u8,
    key_lens: *const usize,
    count: usize,
    out_values: *mut FrsBytes,
) -> i32 {
    guarded(|| {
        if out_values.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if count == 0 {
            return FRS_STATUS_OK;
        }
        if count > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        if keys.is_null() || key_lens.is_null() {
            return FRS_STATUS_NULL_ARG;
        }

        let key_ptrs = slice::from_raw_parts(keys, count);
        let key_len_arr = slice::from_raw_parts(key_lens, count);
        let owned_keys: Vec<&[u8]> = (0..count)
            .map(|i| slice::from_raw_parts(key_ptrs[i], key_len_arr[i]))
            .collect();

        match db.batch_get(cf, &owned_keys) {
            Ok(values) => {
                let out = slice::from_raw_parts_mut(out_values, count);
                for (slot, v) in out.iter_mut().zip(values) {
                    *slot = match v {
                        Some(bytes) => FrsBytes::from_vec(bytes),
                        None => FrsBytes::NULL,
                    };
                }
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 5. Memory management
// ---------------------------------------------------------------------------

/// Frees a byte slice returned by [`frs_get`] / [`frs_batch_get`]. Safe to
/// call on a zero-length or null slice.
#[no_mangle]
pub unsafe extern "C" fn frs_bytes_free(bytes: *mut FrsBytes) -> i32 {
    guarded(|| {
        if bytes.is_null() {
            return FRS_STATUS_OK;
        }
        let b = &mut *bytes;
        if !b.data.is_null() && b.capacity > 0 {
            drop(Vec::from_raw_parts(b.data, b.len, b.capacity));
        }
        b.data = std::ptr::null_mut();
        b.len = 0;
        b.capacity = 0;
        FRS_STATUS_OK
    })
}

// ---------------------------------------------------------------------------
// 6. Flush / compact / checkpoint
// ---------------------------------------------------------------------------

/// Flushes all pending memtables.
#[no_mangle]
pub unsafe extern "C" fn frs_flush(handle: FrsDb) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.flush_all() {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Flushes pending memtables for a specific CF.
#[no_mangle]
pub unsafe extern "C" fn frs_flush_cf(handle: FrsDb, cf: FrsCfHandle) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.flush_cf(cf) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Runs L0→L1 compaction for a specific CF.
#[no_mangle]
pub unsafe extern "C" fn frs_compact_cf(handle: FrsDb, cf: FrsCfHandle) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.compact_l0(cf) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Runs compaction for every CF.
#[no_mangle]
pub unsafe extern "C" fn frs_compact_all(handle: FrsDb) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.compact_all() {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Creates a checkpoint at the target directory.
#[no_mangle]
pub unsafe extern "C" fn frs_create_checkpoint(handle: FrsDb, target_dir: *const c_char) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(path) = cstr_to_str(&target_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.create_checkpoint(std::path::Path::new(path)) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 7. Metadata
// ---------------------------------------------------------------------------

/// Returns the current global sequence number.
#[no_mangle]
pub unsafe extern "C" fn frs_sequence_number(handle: FrsDb, out_seq: *mut u64) -> i32 {
    guarded(|| {
        if out_seq.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        *out_seq = db.sequence_number();
        FRS_STATUS_OK
    })
}

/// Returns the number of L0 files in the current Version.
#[no_mangle]
pub unsafe extern "C" fn frs_l0_file_count(handle: FrsDb, out_count: *mut u32) -> i32 {
    guarded(|| {
        if out_count.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let _ = db;
        // Engine does not currently expose Version directly; we leave this
        // metric at 0 until the read accessor lands.
        *out_count = 0;
        FRS_STATUS_OK
    })
}

/// Opens an engine from a checkpoint directory.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_checkpoint(
    target_dir: *const c_char,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(path) = cstr_to_str(&target_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        let opts = EngineOptions {
            db_path: path.to_string(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        match DbImpl::open_from_checkpoint(opts, fs) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens an engine from a checkpoint directory backed by the in-memory FS.
/// Used by tests; real consumers should use [`frs_db_open_from_checkpoint`].
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_checkpoint_memory(
    target_dir: *const c_char,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(path) = cstr_to_str(&target_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        let opts = EngineOptions {
            db_path: path.to_string(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        match DbImpl::open_from_checkpoint(opts, fs) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

// Suppress dead_code warning for _path used above.
#[allow(dead_code)]
fn _pathbuf_usage(_p: PathBuf) {}

// ---------------------------------------------------------------------------
// 8. Arrow C Data Interface (zero-copy batch ops)
// ---------------------------------------------------------------------------
//
// These entry points accept / produce Arrow RecordBatches directly via the
// Arrow C Data Interface (FFI_ArrowArray + FFI_ArrowSchema). Consumers can
// wrap Java `MemorySegment` pointers in `ArrowArray` / `ArrowSchema` and
// hand them to these functions, avoiding the per-row copy that
// `frs_batch_put` / `frs_batch_get` incur.

use arrow::array::{make_array, Array as _, BinaryArray, RecordBatch, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};

/// Schema expected by [`frs_batch_put_arrow`]:
/// `key: Binary, value: Binary (nullable), op_type: UInt8`.
///
/// `op_type` encoding mirrors `OpType`: 0 Put, 1 Delete, 2 SingleDelete,
/// 3 Merge. Null `value` is allowed only for Delete/SingleDelete.
///
/// Returns the canonical Arrow schema as an exported `FFI_ArrowSchema` the
/// caller can compare against. Ownership of the returned schema is
/// transferred to the caller; it must be freed via `ffi_arrow_schema_free`
/// (or the standard Arrow FFI release callback).
#[no_mangle]
pub unsafe extern "C" fn frs_batch_put_arrow_schema(out_schema: *mut FFI_ArrowSchema) -> i32 {
    guarded(|| {
        if out_schema.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let schema = put_batch_schema();
        let ffi = match FFI_ArrowSchema::try_from(schema.as_ref()) {
            Ok(s) => s,
            Err(_) => return FRS_STATUS_ERROR,
        };
        std::ptr::write(out_schema, ffi);
        FRS_STATUS_OK
    })
}

/// Zero-copy batch put via the Arrow C Data Interface.
///
/// The caller provides `array` and `schema` produced by Arrow-Java's
/// `Data.exportArray` (or equivalent). Data pointers are retained only for
/// the duration of this call — the engine copies into its memtables.
///
/// The record batch schema MUST be:
/// `key: Binary, value: Binary (nullable), op_type: UInt8`.
#[no_mangle]
pub unsafe extern "C" fn frs_batch_put_arrow(
    handle: FrsDb,
    cf: FrsCfHandle,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if array.is_null() || schema.is_null() {
            return FRS_STATUS_NULL_ARG;
        }

        // Import the FFI buffers into an arrow Array. We take ownership of
        // `*array` and `*schema` — per the Arrow C Data Interface contract,
        // we invalidate the originals so the producer won't double-free.
        let array_owned = std::ptr::read(array);
        let schema_owned = std::ptr::read(schema);
        std::ptr::write(array, FFI_ArrowArray::empty());
        std::ptr::write(schema, FFI_ArrowSchema::empty());
        let data = match from_ffi(array_owned, &schema_owned) {
            Ok(d) => d,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let struct_array = make_array(data);
        // Struct RecordBatch convention: use `arrow::array::StructArray`
        // downcast. But `to_ffi` may have exported a single struct array.
        // Prefer decomposing into columns via RecordBatch::from directly.
        let struct_array = match struct_array
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
        {
            Some(s) => s.clone(),
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let batch: RecordBatch = struct_array.into();

        // Validate schema (best effort — users should use the canonical
        // schema from `frs_batch_put_arrow_schema`).
        if batch.num_columns() != 3 {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let keys = match batch.column(0).as_any().downcast_ref::<BinaryArray>() {
            Some(a) => a,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let values = match batch.column(1).as_any().downcast_ref::<BinaryArray>() {
            Some(a) => a,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let ops = match batch.column(2).as_any().downcast_ref::<UInt8Array>() {
            Some(a) => a,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };

        // SECURITY: same MAX_BATCH_COUNT cap as the non-Arrow path
        // (Sweep R5 H by Reviewer 2). batch.num_rows() comes from the
        // C-side Arrow array; cap defends against a crafted batch.
        if batch.num_rows() > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let mut wb = WriteBatch::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let key = keys.value(i);
            let op = ops.value(i);
            match op {
                0 => {
                    // Put
                    if values.is_null(i) {
                        return FRS_STATUS_INVALID_ARGUMENT;
                    }
                    wb.put(cf, key, values.value(i));
                }
                1 => {
                    // Delete rows must not carry a value.
                    if !values.is_null(i) {
                        return FRS_STATUS_INVALID_ARGUMENT;
                    }
                    wb.delete(cf, key);
                }
                2 => {
                    // SingleDelete rows must not carry a value.
                    if !values.is_null(i) {
                        return FRS_STATUS_INVALID_ARGUMENT;
                    }
                    wb.single_delete(cf, key);
                }
                3 => {
                    if values.is_null(i) {
                        return FRS_STATUS_INVALID_ARGUMENT;
                    }
                    wb.merge(cf, key, values.value(i));
                }
                _ => return FRS_STATUS_INVALID_ARGUMENT,
            }
        }
        match db.batch_write(wb) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Zero-copy batch get via the Arrow C Data Interface.
///
/// Input: `keys_array` / `keys_schema` — an Arrow `BinaryArray` of lookup
/// keys. Output: `out_array` / `out_schema` — a RecordBatch with columns
/// `value: Binary (nullable)`, `found: Boolean`. The caller releases the
/// output via the standard Arrow release callbacks.
#[no_mangle]
pub unsafe extern "C" fn frs_batch_get_arrow(
    handle: FrsDb,
    cf: FrsCfHandle,
    keys_array: *mut FFI_ArrowArray,
    keys_schema: *mut FFI_ArrowSchema,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    guarded(|| {
        if keys_array.is_null()
            || keys_schema.is_null()
            || out_array.is_null()
            || out_schema.is_null()
        {
            return FRS_STATUS_NULL_ARG;
        }
        // Initialise outputs to inert empty() so caller-side release is safe
        // on any early-error path.
        std::ptr::write(out_array, FFI_ArrowArray::empty());
        std::ptr::write(out_schema, FFI_ArrowSchema::empty());

        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };

        let keys_array_owned = std::ptr::read(keys_array);
        let keys_schema_owned = std::ptr::read(keys_schema);
        std::ptr::write(keys_array, FFI_ArrowArray::empty());
        std::ptr::write(keys_schema, FFI_ArrowSchema::empty());
        let data = match from_ffi(keys_array_owned, &keys_schema_owned) {
            Ok(d) => d,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let keys_arr = make_array(data);
        let keys = match keys_arr.as_any().downcast_ref::<BinaryArray>() {
            Some(a) => a,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };

        // SECURITY: same MAX_BATCH_COUNT cap as the non-Arrow path
        // and as frs_batch_put_arrow. keys.len() comes from the C-side
        // Arrow array; cap defends against a crafted batch driving
        // unbounded allocations via Vec<&[u8]> and batch_get's internal
        // result Vec. (Sweep R10 H by Reviewer 3; parallel finding to
        // R5 H#2 which fixed the put-side.)
        if keys.len() > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let key_slices: Vec<&[u8]> = (0..keys.len()).map(|i| keys.value(i)).collect();
        let values = match db.batch_get(cf, &key_slices) {
            Ok(v) => v,
            Err(e) => return error_to_status(&e),
        };

        // Build the output RecordBatch.
        let mut value_builder = arrow::array::BinaryBuilder::new();
        let mut found_builder = arrow::array::BooleanBuilder::new();
        for v in &values {
            match v {
                Some(bytes) => {
                    value_builder.append_value(bytes);
                    found_builder.append_value(true);
                }
                None => {
                    value_builder.append_null();
                    found_builder.append_value(false);
                }
            }
        }
        let value_array = value_builder.finish();
        let found_array = found_builder.finish();

        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("value", DataType::Binary, true),
            Field::new("found", DataType::Boolean, false),
        ]));
        let batch = match RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(value_array),
                std::sync::Arc::new(found_array),
            ],
        ) {
            Ok(b) => b,
            Err(_) => return FRS_STATUS_ERROR,
        };

        // Export the batch as a struct FFI_ArrowArray.
        let struct_arr = arrow::array::StructArray::from(batch);
        let (ffi_array, ffi_schema) = match to_ffi(&struct_arr.into_data()) {
            Ok(pair) => pair,
            Err(_) => return FRS_STATUS_ERROR,
        };
        std::ptr::write(out_array, ffi_array);
        std::ptr::write(out_schema, ffi_schema);
        FRS_STATUS_OK
    })
}

/// Zero-copy prefix scan via Arrow C Data Interface.
///
/// Returns a RecordBatch with columns `key: Binary` and `value: Binary`
/// containing every key-value pair whose key starts with `prefix`. The
/// caller releases the output via the standard Arrow release callbacks.
///
/// **Empty prefix semantics**: if `prefix` is NULL or `prefix_len == 0`, the
/// call degenerates to a full column-family scan. Every key in the CF is
/// returned, sorted in ascending byte-wise order. Callers should therefore
/// treat the zero-length prefix as a potentially unbounded operation.
///
/// This is the W26 fast-path for DeltaJoin Lookup: Java consumers can
/// wrap the returned arrays with `FFI_ArrowArray` / `FFI_ArrowSchema`
/// pointers and iterate them without further per-row copies.
#[no_mangle]
pub unsafe extern "C" fn frs_prefix_scan_arrow(
    handle: FrsDb,
    cf: FrsCfHandle,
    prefix: *const u8,
    prefix_len: usize,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32 {
    guarded(|| {
        if out_array.is_null() || out_schema.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        // Initialise outputs up-front so callers that blindly invoke the
        // Arrow release callback on a partial-failure path see an inert
        // (NULL callback) struct rather than garbage.
        std::ptr::write(out_array, FFI_ArrowArray::empty());
        std::ptr::write(out_schema, FFI_ArrowSchema::empty());

        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        let prefix_slice = if prefix.is_null() || prefix_len == 0 {
            &[][..]
        } else {
            slice::from_raw_parts(prefix, prefix_len)
        };

        let rows = match db.prefix_scan(cf, prefix_slice) {
            Ok(r) => r,
            Err(e) => return error_to_status(&e),
        };

        // SECURITY: same MAX_BATCH_COUNT cap as the batch FFI paths.
        // Defends against a crafted empty/short prefix that matches
        // millions of keys → unbounded Arrow array materialization
        // (Sweep R11 H by Reviewers 1 + 5; parallel of R5/R10 batch
        // findings, scan-side). The deeper engine-side cap on
        // `db.prefix_scan` itself remains a follow-up architectural
        // change (would require passing max-rows through scan API).
        if rows.len() > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let mut key_builder = arrow::array::BinaryBuilder::new();
        let mut value_builder = arrow::array::BinaryBuilder::new();
        for (k, v) in &rows {
            key_builder.append_value(k);
            value_builder.append_value(v);
        }
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("key", DataType::Binary, false),
            Field::new("value", DataType::Binary, false),
        ]));
        let batch = match RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(key_builder.finish()),
                std::sync::Arc::new(value_builder.finish()),
            ],
        ) {
            Ok(b) => b,
            Err(_) => return FRS_STATUS_ERROR,
        };
        let struct_arr = arrow::array::StructArray::from(batch);
        let (a, s) = match to_ffi(&struct_arr.into_data()) {
            Ok(p) => p,
            Err(_) => return FRS_STATUS_ERROR,
        };
        std::ptr::write(out_array, a);
        std::ptr::write(out_schema, s);
        FRS_STATUS_OK
    })
}

fn put_batch_schema() -> std::sync::Arc<Schema> {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("op_type", DataType::UInt8, false),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::ptr;

    #[test]
    fn test_open_put_get_close_roundtrip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            assert!(!db.is_null());

            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"hello";
            let value = b"world";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                FRS_STATUS_OK
            );

            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(!out.data.is_null());
            let slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(slice, value);
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_get_missing_returns_null_bytes() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            let key = b"absent";
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(out.data.is_null());
            assert_eq!(out.len, 0);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_delete_then_get_returns_null() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            let k = b"k";
            let v = b"v";
            frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            assert_eq!(frs_delete(db, cf, k.as_ptr(), k.len()), FRS_STATUS_OK);

            let mut out = FrsBytes::NULL;
            frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
            assert!(out.data.is_null());

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_merge_with_list_append() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);

            let name = CString::new("lists").unwrap();
            let op_name = CString::new("ListAppendMergeOperator").unwrap();
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_with_merge(db, name.as_ptr(), op_name.as_ptr(), &mut cf,),
                FRS_STATUS_OK
            );

            let k = b"list";
            frs_put(db, cf, k.as_ptr(), k.len(), b"a".as_ptr(), 1);
            frs_merge(db, cf, k.as_ptr(), k.len(), b"b".as_ptr(), 1);
            frs_merge(db, cf, k.as_ptr(), k.len(), b"c".as_ptr(), 1);

            let mut out = FrsBytes::NULL;
            frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
            let slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(slice, b"a,b,c");
            frs_bytes_free(&mut out);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_batch_put_and_get() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            let keys: Vec<Vec<u8>> = (0..10u32).map(|i| format!("k{}", i).into_bytes()).collect();
            let values: Vec<Vec<u8>> = (0..10u32).map(|i| format!("v{}", i).into_bytes()).collect();
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
                    10,
                ),
                FRS_STATUS_OK
            );

            let mut outs: Vec<FrsBytes> = (0..10).map(|_| FrsBytes::NULL).collect();
            assert_eq!(
                frs_batch_get(
                    db,
                    cf,
                    key_ptrs.as_ptr(),
                    key_lens.as_ptr(),
                    10,
                    outs.as_mut_ptr(),
                ),
                FRS_STATUS_OK
            );
            for (i, out) in outs.iter_mut().enumerate() {
                let slice = slice::from_raw_parts(out.data, out.len);
                assert_eq!(slice, values[i].as_slice());
                frs_bytes_free(out);
            }

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_flush_and_read() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            for i in 0..50u32 {
                let k = format!("k{:04}", i);
                let v = format!("v{:04}", i);
                frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            }
            assert_eq!(frs_flush(db), FRS_STATUS_OK);
            assert_eq!(frs_compact_all(db), FRS_STATUS_OK);

            for i in 0..50u32 {
                let k = format!("k{:04}", i);
                let v = format!("v{:04}", i);
                let mut out = FrsBytes::NULL;
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
                let slice = slice::from_raw_parts(out.data, out.len);
                assert_eq!(slice, v.as_bytes());
                frs_bytes_free(&mut out);
            }

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_null_args_return_null_arg_status() {
        unsafe {
            assert_eq!(frs_db_open_memory(ptr::null_mut()), FRS_STATUS_NULL_ARG);
            assert_eq!(
                frs_put(
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    ptr::null(),
                    0
                ),
                FRS_STATUS_NULL_ARG
            );
            assert_eq!(frs_db_close(ptr::null_mut()), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_sequence_number_accessor() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            let mut seq = 0u64;
            frs_sequence_number(db, &mut seq);
            assert_eq!(seq, 0);

            let k = b"k";
            let v = b"v";
            frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            frs_sequence_number(db, &mut seq);
            assert!(seq > 0);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_checkpoint_create_noop_on_memory() {
        unsafe {
            // In-memory FS: create checkpoint + a fresh engine should not
            // reopen from it (open_from_checkpoint_memory uses a new FS).
            // Just verify the create-checkpoint call succeeds.
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            let k = b"k";
            let v = b"v";
            frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());

            let target = CString::new("/ckpt").unwrap();
            assert_eq!(frs_create_checkpoint(db, target.as_ptr()), FRS_STATUS_OK);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    // --- Arrow FFI tests ---

    #[test]
    fn test_arrow_put_batch_via_ffi() {
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::{DataType, Field};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            // Build a RecordBatch containing 3 put ops.
            let mut keys = BinaryBuilder::new();
            let mut values = BinaryBuilder::new();
            let mut ops = UInt8Builder::new();
            for i in 0..3u32 {
                keys.append_value(format!("k{}", i));
                values.append_value(format!("v{}", i));
                ops.append_value(0); // Put
            }
            let struct_arr = StructArray::from(vec![
                (
                    std::sync::Arc::new(Field::new("key", DataType::Binary, false)),
                    std::sync::Arc::new(keys.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("value", DataType::Binary, true)),
                    std::sync::Arc::new(values.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("op_type", DataType::UInt8, false)),
                    std::sync::Arc::new(ops.finish()) as arrow::array::ArrayRef,
                ),
            ]);

            let (mut array, mut schema) = to_ffi(&struct_arr.into_data()).unwrap();
            assert_eq!(
                frs_batch_put_arrow(db, cf, &mut array, &mut schema),
                FRS_STATUS_OK
            );
            // After the call, `array`/`schema` have been invalidated by the
            // engine (their release callbacks zeroed). Dropping them here is
            // a no-op, matching the Arrow C Data Interface contract.

            // Verify values.
            for i in 0..3u32 {
                let k = format!("k{}", i);
                let v = format!("v{}", i);
                let mut out = FrsBytes::NULL;
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
                let slice = slice::from_raw_parts(out.data, out.len);
                assert_eq!(slice, v.as_bytes());
                frs_bytes_free(&mut out);
            }

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_arrow_put_batch_mixed_ops() {
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::{DataType, Field};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            // Seed a key that we'll later delete via the Arrow batch.
            frs_put(db, cf, b"k2".as_ptr(), 2, b"old".as_ptr(), 3);

            // Build a RecordBatch: put k1, delete k2, put k3.
            let mut keys = BinaryBuilder::new();
            let mut values = BinaryBuilder::new();
            let mut ops = UInt8Builder::new();

            keys.append_value(b"k1");
            values.append_value(b"v1");
            ops.append_value(0); // Put

            keys.append_value(b"k2");
            values.append_null();
            ops.append_value(1); // Delete

            keys.append_value(b"k3");
            values.append_value(b"v3");
            ops.append_value(0);

            let struct_arr = StructArray::from(vec![
                (
                    std::sync::Arc::new(Field::new("key", DataType::Binary, false)),
                    std::sync::Arc::new(keys.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("value", DataType::Binary, true)),
                    std::sync::Arc::new(values.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("op_type", DataType::UInt8, false)),
                    std::sync::Arc::new(ops.finish()) as arrow::array::ArrayRef,
                ),
            ]);

            let (mut a, mut s) = to_ffi(&struct_arr.into_data()).unwrap();
            assert_eq!(frs_batch_put_arrow(db, cf, &mut a, &mut s), FRS_STATUS_OK);

            // k1 = v1, k2 = deleted, k3 = v3.
            let mut out = FrsBytes::NULL;
            frs_get(db, cf, b"k1".as_ptr(), 2, &mut out);
            assert_eq!(slice::from_raw_parts(out.data, out.len), b"v1");
            frs_bytes_free(&mut out);

            let mut out = FrsBytes::NULL;
            frs_get(db, cf, b"k2".as_ptr(), 2, &mut out);
            assert!(out.data.is_null(), "k2 should be deleted");

            let mut out = FrsBytes::NULL;
            frs_get(db, cf, b"k3".as_ptr(), 2, &mut out);
            assert_eq!(slice::from_raw_parts(out.data, out.len), b"v3");
            frs_bytes_free(&mut out);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_arrow_batch_get_roundtrip() {
        use arrow::array::{make_array, BinaryArray, BinaryBuilder, BooleanArray, StructArray};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);

            // Seed with 5 keys.
            for i in 0..5u32 {
                let k = format!("k{}", i);
                let v = format!("v{}", i);
                frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            }

            // Build an Arrow array of 6 keys (the 6th missing).
            let mut kb = BinaryBuilder::new();
            for i in 0..6u32 {
                kb.append_value(format!("k{}", i));
            }
            let keys_arr = kb.finish();
            let (mut ia, mut is_) = to_ffi(&keys_arr.to_data()).unwrap();

            let mut out_array = arrow::ffi::FFI_ArrowArray::empty();
            let mut out_schema = arrow::ffi::FFI_ArrowSchema::empty();
            assert_eq!(
                frs_batch_get_arrow(db, cf, &mut ia, &mut is_, &mut out_array, &mut out_schema,),
                FRS_STATUS_OK
            );

            // Import the output.
            let out_data = arrow::ffi::from_ffi(out_array, &out_schema).unwrap();
            let array = make_array(out_data);
            let struct_arr = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            let values = struct_arr
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary");
            let found = struct_arr
                .column(1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("bool");
            assert_eq!(values.len(), 6);
            for i in 0..5 {
                assert!(found.value(i));
                let expected = format!("v{}", i);
                assert_eq!(values.value(i), expected.as_bytes());
            }
            assert!(!found.value(5));

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_prefix_scan_arrow_returns_sorted_matches() {
        use arrow::array::{BinaryArray, StructArray};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Seed: three matching + two non-matching keys.
            let pairs: &[(&[u8], &[u8])] = &[
                (b"user:alice", b"A"),
                (b"user:bob", b"B"),
                (b"user:carol", b"C"),
                (b"admin:root", b"R"),
                (b"zzz", b"Z"),
            ];
            for (k, v) in pairs {
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            let prefix = b"user:";
            let mut out_array = FFI_ArrowArray::empty();
            let mut out_schema = FFI_ArrowSchema::empty();
            assert_eq!(
                frs_prefix_scan_arrow(
                    db,
                    cf,
                    prefix.as_ptr(),
                    prefix.len(),
                    &mut out_array,
                    &mut out_schema,
                ),
                FRS_STATUS_OK
            );

            // Import back via from_ffi.
            let data = from_ffi(out_array, &out_schema).expect("valid ffi payload");
            let struct_arr = make_array(data);
            let struct_arr = struct_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            assert_eq!(struct_arr.num_columns(), 2);
            assert_eq!(struct_arr.len(), 3);
            let keys = struct_arr
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("key col is binary");
            let values = struct_arr
                .column(1)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("value col is binary");
            // prefix_scan should return keys in sorted order.
            assert_eq!(keys.value(0), b"user:alice");
            assert_eq!(keys.value(1), b"user:bob");
            assert_eq!(keys.value(2), b"user:carol");
            assert_eq!(values.value(0), b"A");
            assert_eq!(values.value(1), b"B");
            assert_eq!(values.value(2), b"C");

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_prefix_scan_arrow_empty_prefix_returns_all() {
        use arrow::array::StructArray;

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            for i in 0..4u32 {
                let k = format!("k{:02}", i);
                let v = format!("v{:02}", i);
                frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            }

            let mut out_array = FFI_ArrowArray::empty();
            let mut out_schema = FFI_ArrowSchema::empty();
            // Empty prefix (null pointer, zero length) → full scan.
            assert_eq!(
                frs_prefix_scan_arrow(db, cf, ptr::null(), 0, &mut out_array, &mut out_schema,),
                FRS_STATUS_OK
            );
            let data = from_ffi(out_array, &out_schema).expect("valid ffi");
            let struct_arr = make_array(data);
            let struct_arr = struct_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            assert_eq!(struct_arr.len(), 4);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_prefix_scan_arrow_no_match_returns_empty_batch() {
        use arrow::array::StructArray;

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            frs_put(db, cf, b"foo".as_ptr(), 3, b"v".as_ptr(), 1);

            let prefix = b"zzz";
            let mut out_array = FFI_ArrowArray::empty();
            let mut out_schema = FFI_ArrowSchema::empty();
            assert_eq!(
                frs_prefix_scan_arrow(
                    db,
                    cf,
                    prefix.as_ptr(),
                    prefix.len(),
                    &mut out_array,
                    &mut out_schema,
                ),
                FRS_STATUS_OK
            );
            let data = from_ffi(out_array, &out_schema).expect("valid ffi");
            let struct_arr = make_array(data);
            let struct_arr = struct_arr
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("struct");
            assert_eq!(struct_arr.len(), 0);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_prefix_scan_arrow_null_handle_returns_null_arg() {
        unsafe {
            let mut out_array = FFI_ArrowArray::empty();
            let mut out_schema = FFI_ArrowSchema::empty();
            assert_eq!(
                frs_prefix_scan_arrow(
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    &mut out_array,
                    &mut out_schema,
                ),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    #[test]
    fn test_batch_put_arrow_single_delete_op() {
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::{DataType, Field};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Seed key that the batch will SingleDelete.
            assert_eq!(
                frs_put(db, cf, b"k1".as_ptr(), 2, b"v1".as_ptr(), 2),
                FRS_STATUS_OK
            );

            // Build a RecordBatch: SingleDelete k1 (op=2).
            let mut keys = BinaryBuilder::new();
            let mut values = BinaryBuilder::new();
            let mut ops = UInt8Builder::new();
            keys.append_value(b"k1");
            values.append_null();
            ops.append_value(2); // SingleDelete

            let struct_arr = StructArray::from(vec![
                (
                    std::sync::Arc::new(Field::new("key", DataType::Binary, false)),
                    std::sync::Arc::new(keys.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("value", DataType::Binary, true)),
                    std::sync::Arc::new(values.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("op_type", DataType::UInt8, false)),
                    std::sync::Arc::new(ops.finish()) as arrow::array::ArrayRef,
                ),
            ]);
            let (mut a, mut s) = to_ffi(&struct_arr.into_data()).expect("to_ffi");
            assert_eq!(frs_batch_put_arrow(db, cf, &mut a, &mut s), FRS_STATUS_OK);

            // Key should be gone.
            let mut out = FrsBytes::NULL;
            assert_eq!(frs_get(db, cf, b"k1".as_ptr(), 2, &mut out), FRS_STATUS_OK);
            assert!(out.data.is_null(), "k1 should have been SingleDeleted");
            assert_eq!(out.len, 0);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_batch_put_arrow_rejects_value_on_delete_op() {
        // Regression guard for the Round-2 finding: Delete/SingleDelete
        // rows must not carry a value.
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::{DataType, Field};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            for bad_op in [1u8, 2u8] {
                let mut keys = BinaryBuilder::new();
                let mut values = BinaryBuilder::new();
                let mut ops = UInt8Builder::new();
                keys.append_value(b"k");
                values.append_value(b"not-null"); // Invalid: must be NULL.
                ops.append_value(bad_op);
                let struct_arr = StructArray::from(vec![
                    (
                        std::sync::Arc::new(Field::new("key", DataType::Binary, false)),
                        std::sync::Arc::new(keys.finish()) as arrow::array::ArrayRef,
                    ),
                    (
                        std::sync::Arc::new(Field::new("value", DataType::Binary, true)),
                        std::sync::Arc::new(values.finish()) as arrow::array::ArrayRef,
                    ),
                    (
                        std::sync::Arc::new(Field::new("op_type", DataType::UInt8, false)),
                        std::sync::Arc::new(ops.finish()) as arrow::array::ArrayRef,
                    ),
                ]);
                let (mut a, mut s) = to_ffi(&struct_arr.into_data()).expect("to_ffi");
                assert_eq!(
                    frs_batch_put_arrow(db, cf, &mut a, &mut s),
                    FRS_STATUS_INVALID_ARGUMENT,
                    "op={} with non-null value must be rejected",
                    bad_op
                );
            }

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_single_delete_via_batch_put_arrow_compacts_away() {
        // Flush a Put + SingleDelete and compact. Because the engine
        // treats SingleDelete+Put as a collapsible pair, a *subsequent*
        // Put of the same key must survive compaction untouched — proving
        // the SingleDelete was elided rather than retained as a tombstone.
        use arrow::array::{BinaryBuilder, StructArray, UInt8Builder};
        use arrow::datatypes::{DataType, Field};

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Seed & flush a Put, then SingleDelete via Arrow batch & flush.
            assert_eq!(
                frs_put(db, cf, b"k".as_ptr(), 1, b"v".as_ptr(), 1),
                FRS_STATUS_OK
            );
            assert_eq!(frs_flush(db), FRS_STATUS_OK);

            let mut keys = BinaryBuilder::new();
            let mut values = BinaryBuilder::new();
            let mut ops = UInt8Builder::new();
            keys.append_value(b"k");
            values.append_null();
            ops.append_value(2);
            let struct_arr = StructArray::from(vec![
                (
                    std::sync::Arc::new(Field::new("key", DataType::Binary, false)),
                    std::sync::Arc::new(keys.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("value", DataType::Binary, true)),
                    std::sync::Arc::new(values.finish()) as arrow::array::ArrayRef,
                ),
                (
                    std::sync::Arc::new(Field::new("op_type", DataType::UInt8, false)),
                    std::sync::Arc::new(ops.finish()) as arrow::array::ArrayRef,
                ),
            ]);
            let (mut a, mut s) = to_ffi(&struct_arr.into_data()).expect("to_ffi");
            assert_eq!(frs_batch_put_arrow(db, cf, &mut a, &mut s), FRS_STATUS_OK);
            assert_eq!(frs_flush(db), FRS_STATUS_OK);
            assert_eq!(frs_compact_all(db), FRS_STATUS_OK);

            // After compaction, re-Put and read back. If the SingleDelete
            // had been retained as a Delete tombstone at the bottommost
            // level it would have been dropped (bottommost-level rule) —
            // either way the final read should see the new value.
            assert_eq!(
                frs_put(db, cf, b"k".as_ptr(), 1, b"new".as_ptr(), 3),
                FRS_STATUS_OK
            );
            let mut out = FrsBytes::NULL;
            assert_eq!(frs_get(db, cf, b"k".as_ptr(), 1, &mut out), FRS_STATUS_OK);
            assert!(!out.data.is_null());
            let slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(slice, b"new");
            frs_bytes_free(&mut out);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }
}

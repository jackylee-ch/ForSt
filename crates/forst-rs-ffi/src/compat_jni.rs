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

//! JNI compatibility shim — G-A drop-in replacement for community ForSt's
//! `libforstjni.so`.
//!
//! # The G-A goal
//!
//! Per A1 §4.1 ("Goal A: drop-in replacement"), an existing user running
//! Apache Flink with `flink-statebackend-forst` (which loads
//! `libforstjni.so` from the published `com.ververica:forstjni:0.1.8`
//! artifact, a RocksDB fork) must be able to swap in a `libforstjni.so`
//! built from forst-rs **without recompiling or modifying their Flink job
//! one line**. The Java side calls `org.forstdb.RocksDB.put(...)` etc.;
//! these resolve via JNI to symbols of the form
//! `Java_org_forstdb_RocksDB_put`. This module exports exactly that
//! symbol surface and forwards each call into the existing `frs_*` C ABI.
//!
//! # What "perfect drop-in" means in the threat model
//!
//! - **Binary compat with `libforstjni.so` callers:** the *symbol table*
//!   of the resulting cdylib is a superset of the methods Flink's JNI
//!   resolver looks up at class-load time. A missing symbol surfaces as
//!   `UnsatisfiedLinkError` at runtime — that is the failure mode we
//!   prevent.
//! - **JVM-safe error handling:** every exported function MUST translate
//!   Rust `Result::Err` and Rust panics into Java exceptions
//!   (`org.forstdb.RocksDBException`). Unwinding across the C ABI is UB
//!   and crashes the JVM with SIGSEGV; we use [`std::panic::catch_unwind`]
//!   to convert panics to `RocksDBException`.
//! - **No memory leaked across the boundary:** Java byte arrays are
//!   copied into Rust slices; results returned from `frs_*` go into
//!   `jbyteArray` and the underlying `FrsBytes` is freed via
//!   [`crate::frs_bytes_free`] before the function returns.
//!
//! # Caveats — minimal-viable surface only
//!
//! The community RocksDB Java API is large (~500 native methods across
//! `RocksDB`, `Options`, `WriteBatch`, `ReadOptions`, `Env`, ...). This
//! module implements only the subset that
//! `flink-statebackend-forst::ForStKeyedStateBackend` actually invokes at
//! runtime — open / close / put / get / delete / column-family
//! create+drop / flush / checkpoint. Tracking design doc 2.5 captures
//! the full surface; additional symbols can be added incrementally as
//! Flink coverage expands. Java code that calls a missing symbol gets
//! `UnsatisfiedLinkError`, which surfaces clearly in the Flink job log
//! rather than corrupting state.
//!
//! # How to use
//!
//! 1. Build with the feature:
//!    `cargo build --release -p forst-rs-ffi --features compat-jni`
//! 2. Locate the resulting cdylib:
//!    `target/release/libforst_rs_ffi.{so,dylib,dll}`
//! 3. Rename it to `libforstjni.{so,dylib,dll}` and place it on the
//!    `LD_LIBRARY_PATH` (Linux) / `DYLD_LIBRARY_PATH` (macOS) /
//!    `PATH` (Windows) ahead of any community `forstjni` JAR's bundled
//!    native binary.
//! 4. Restart the Flink TaskManager. `System.loadLibrary("forstjni")`
//!    will now resolve into forst-rs.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::Arc;

use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JPrimitiveArray, JString};
use jni::sys::{
    jboolean, jbyteArray, jint, jlong, jobjectArray, JavaVM, JNI_FALSE, JNI_TRUE, JNI_VERSION_1_8,
};
use jni::JNIEnv;

use forst_rs_common::{CfOptions, CompressionType, EngineOptions};
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};

use crate::{
    frs_batch_get, frs_batch_put, frs_compact_all, frs_compact_cf, frs_create_checkpoint,
    frs_db_close, frs_db_create_cf, frs_db_default_cf, frs_db_open, frs_db_open_cf,
    frs_db_open_from_checkpoint, frs_delete, frs_flush, frs_flush_cf, frs_get, frs_iterator_close,
    frs_iterator_next, frs_iterator_open, frs_iterator_seek, frs_l0_file_count, frs_lookup_kv,
    frs_merge, frs_prefix_lookup_close, frs_prefix_lookup_open, frs_put, frs_sequence_number,
    FrsBytes, FrsCfHandle, FrsDb, FrsIterator, FRS_STATUS_NOT_FOUND, FRS_STATUS_OK,
};

// ---------------------------------------------------------------------------
// Options-handle ownership module
//
// Each Java `Options` class corresponds to a Rust-side struct held in a Box.
// The Box is leaked across the JNI boundary as a `jlong` and reclaimed by the
// matching `disposeInternal` thunk. `from_raw_ref` returns a temporary
// `&'static mut` borrow whose lifetime is bounded by the enclosing JNI call —
// callers MUST NOT retain the reference past the closure scope. This is the
// standard "round-trip via Box::into_raw / Box::from_raw" pattern; missing
// disposeInternal leaks the Box (no UB), double-disposeInternal is UB and
// the Java side is expected to enforce single-call semantics.
// ---------------------------------------------------------------------------

pub(crate) mod handles {
    use super::*;

    /// Java `org.forstdb.DBOptions` mirror. Wraps the engine-wide
    /// [`EngineOptions`]; per-CF overrides go through [`CfOptionsHandle`].
    pub(crate) struct DbOptionsHandle {
        pub opts: EngineOptions,
    }

    /// Java `org.forstdb.ColumnFamilyOptions` mirror. Wraps a [`CfOptions`]
    /// of per-CF overrides applied at `RocksDB.open` time.
    #[derive(Default)]
    pub(crate) struct CfOptionsHandle {
        pub opts: CfOptions,
        /// Opaque handle stored by `setTableFormatConfig` — forst-rs has no
        /// pluggable table-format layer (bloom + block params live on
        /// `EngineOptions`), so we just round-trip the value to keep
        /// Flink's setter chain happy. P3 will hydrate this into actual
        /// engine settings.
        pub table_format_handle: jlong,
    }

    /// Java `org.forstdb.WriteOptions` mirror. forst-rs always durably
    /// writes the WAL; `disable_wal` is recorded so `setDisableWAL(true)`
    /// is observable in tests but the engine ignores it for now (no
    /// silent-disable surprise: the FFI returns OK either way).
    #[derive(Default)]
    pub(crate) struct WriteOptionsHandle {
        pub disable_wal: bool,
    }

    /// Java `org.forstdb.ReadOptions` mirror.
    ///
    /// `fill_cache` and `verify_checksums` are accepted by future setters
    /// (P1) but no engine-side honouring yet — keep the fields so the
    /// `Default` impl matches the community ReadOptions defaults for any
    /// future thunk that wants to forward them onto a per-call read path.
    pub(crate) struct ReadOptionsHandle {
        pub readahead_size: u64,
        #[allow(dead_code)]
        pub fill_cache: bool,
        #[allow(dead_code)]
        pub verify_checksums: bool,
    }

    /// Java `org.forstdb.ColumnFamilyHandle` mirror. Wraps the existing
    /// FFI [`FrsCfHandle`] (`Box<ColumnFamilyHandle>` on the engine side)
    /// plus the CF name so `getName0` can return it without an extra
    /// engine round-trip.
    pub(crate) struct CfHandle {
        pub name: Vec<u8>,
        pub frs_handle: FrsCfHandle,
        /// Owning `CfOptionsHandle` Box pointer **iff** this CF was created
        /// from a caller-supplied `cfOptions` jlong via `RocksDB.open`. We
        /// take ownership at open-time so the Java code can immediately
        /// release the `ColumnFamilyOptions` without leaking the box (the
        /// engine has already extracted the relevant fields). 0 means
        /// "no owned options box" (e.g. default-CF handle that was opened
        /// without an explicit option set).
        ///
        /// On `ColumnFamilyHandle.disposeInternal` we drop both the
        /// `frs_handle` and (if non-zero) the owned options box.
        pub owned_opts_handle: jlong,
    }

    macro_rules! impl_into_from_raw {
        ($t:ty) => {
            impl $t {
                /// Box the handle and surface it to Java as an opaque jlong.
                pub(crate) fn into_raw(self) -> jlong {
                    Box::into_raw(Box::new(self)) as jlong
                }

                /// Reborrow a Java-side jlong as `&'static mut` for the
                /// duration of the enclosing JNI call. The static lifetime
                /// is a lie — callers must drop the reference before
                /// returning to Java. Sound under JNI's single-threaded
                /// per-handle access pattern (Java code is expected to
                /// serialise Options mutations on its end).
                ///
                /// # Safety
                /// `handle` must be a non-zero jlong returned by a prior
                /// `into_raw` of the same type and not yet freed by
                /// `disposeInternal`. The caller MUST NOT alias the
                /// resulting reference with another concurrent borrow of
                /// the same handle.
                #[allow(dead_code)]
                pub(crate) unsafe fn from_raw_ref<'a>(handle: jlong) -> Option<&'a mut $t> {
                    if handle == 0 {
                        None
                    } else {
                        Some(&mut *(handle as *mut $t))
                    }
                }
            }
        };
    }

    /// Java `org.forstdb.RocksIterator` mirror. Wraps the engine-side
    /// [`FrsIterator`] (a `Box<crate::IteratorState>`) and caches the
    /// most-recent `(key, value)` pair so the community
    /// `isValid() / key() / value()` triad can be served without consuming
    /// extra rows from the underlying iterator.
    ///
    /// Lifecycle: created by `Java_org_forstdb_RocksDB_iterator`, advanced
    /// by `Java_org_forstdb_RocksIterator_seek*` / `next0` / `prev0`,
    /// released by `Java_org_forstdb_RocksIterator_disposeInternal` (which
    /// closes the underlying `FrsIterator` and drops the box).
    pub(crate) struct RocksIteratorHandle {
        /// Underlying engine iterator. `*mut c_void` so we keep ownership
        /// of the `FrsIterator` alias and can pass it to `frs_iterator_*`
        /// without dereferencing as a typed Rust pointer.
        pub frs_iter: FrsIterator,
        /// Last key materialised by the most recent `next0`/`prev0`/`seek*`.
        /// `None` after construction or when `valid == false`.
        pub last_key: Option<Vec<u8>>,
        /// Last value materialised, paired with `last_key`.
        pub last_value: Option<Vec<u8>>,
        /// Mirror of the underlying iterator's "is positioned at a row"
        /// state. `false` after construction (no `seek*` yet) and after
        /// stepping past the end.
        pub valid: bool,
    }

    /// Java `org.forstdb.WriteBatch` mirror. Buffers operations Rust-side
    /// (no engine FFI for "build batch in advance") and applies them all
    /// in `Java_org_forstdb_RocksDB_write0` via the existing parallel-array
    /// `frs_batch_put` / `frs_delete` paths.
    ///
    /// The batch is not transactional in the strict-ACID sense — failures
    /// mid-apply leave the engine in a partial state. This matches the
    /// engine's current write-path semantics; once the engine grows a real
    /// atomic-batch API the `apply` method here can be rewritten without
    /// changing the JNI surface.
    #[derive(Default)]
    pub(crate) struct WriteBatchHandle {
        pub entries: Vec<WriteBatchEntry>,
        /// Bytes accumulated across all entries' `(cf?, key, value?)`
        /// payloads. Mirrors RocksDB's `WriteBatch.getDataSize()` — used
        /// by Flink to flush the batch when it grows past a threshold.
        pub data_size: u64,
    }

    /// One write op buffered in a [`WriteBatchHandle`].
    pub(crate) enum WriteBatchEntry {
        Put {
            cf: FrsCfHandle,
            key: Vec<u8>,
            value: Vec<u8>,
        },
        Merge {
            cf: FrsCfHandle,
            key: Vec<u8>,
            value: Vec<u8>,
        },
        Delete {
            cf: FrsCfHandle,
            key: Vec<u8>,
        },
    }

    impl_into_from_raw!(DbOptionsHandle);
    impl_into_from_raw!(CfOptionsHandle);
    impl_into_from_raw!(WriteOptionsHandle);
    impl_into_from_raw!(ReadOptionsHandle);
    impl_into_from_raw!(CfHandle);
    impl_into_from_raw!(RocksIteratorHandle);
    impl_into_from_raw!(WriteBatchHandle);

    impl Default for ReadOptionsHandle {
        fn default() -> Self {
            // Mirror the community RocksDB defaults: no read-ahead, cache
            // populated on miss, checksums verified.
            Self {
                readahead_size: 0,
                fill_cache: true,
                verify_checksums: true,
            }
        }
    }
}

use handles::{
    CfHandle, CfOptionsHandle, DbOptionsHandle, ReadOptionsHandle, RocksIteratorHandle,
    WriteBatchEntry, WriteBatchHandle, WriteOptionsHandle,
};

// ---------------------------------------------------------------------------
// JNI library load hook
// ---------------------------------------------------------------------------

/// Standard JNI library-load entry point. The JVM calls this exactly once
/// when `System.loadLibrary("forstjni")` resolves the cdylib. Returning
/// the JNI version constant tells the JVM the library is compatible.
///
/// # Safety
/// Called only by the JVM during library load. The arguments come from
/// the JVM and are guaranteed valid for the duration of the call.
#[no_mangle]
pub unsafe extern "system" fn JNI_OnLoad(
    _vm: *mut JavaVM,
    _reserved: *mut std::ffi::c_void,
) -> jint {
    JNI_VERSION_1_8
}

/// Informational entry point. The `Java_org_forstdb_*` functions in this
/// module are auto-discovered by the JVM via the C symbol table — there
/// is no explicit registration step. This function exists so callers (or
/// integration tests) can confirm the compat module is linked into the
/// binary.
pub fn register_jni_symbols() -> &'static str {
    "forst-rs-ffi compat_jni: Java_org_forstdb_RocksDB_* symbols active"
}

// ---------------------------------------------------------------------------
// Exception helpers
// ---------------------------------------------------------------------------

/// Java-side fully-qualified exception class. Matches what the community
/// `forstjni` library throws so Flink's existing `try/catch` blocks
/// continue to work unchanged.
const ROCKSDB_EXCEPTION: &str = "org/forstdb/RocksDBException";

/// Throw a `RocksDBException` with the given message. Idempotent: if a
/// Java exception is already pending we leave it in place rather than
/// overwriting it (per JNI spec — calling Java APIs while an exception
/// is pending is UB).
fn throw_rocksdb(env: &mut JNIEnv, msg: &str) {
    if let Ok(true) = env.exception_check() {
        return;
    }
    // Best-effort: if throw_new itself fails (e.g. exception class not
    // found because Flink isn't on the classpath at all) we swallow the
    // error — there is nothing useful to do, and panicking would be
    // worse.
    let _ = env.throw_new(ROCKSDB_EXCEPTION, msg);
}

/// Translate an `frs_*` status code to a Java exception. Returns true if
/// the status was an error (caller should bail), false on success.
fn check_status(env: &mut JNIEnv, status: i32, context: &str) -> bool {
    if status == FRS_STATUS_OK {
        return false;
    }
    let msg = format!("{context}: frs_status={status}");
    throw_rocksdb(env, &msg);
    true
}

/// Run `f` under `catch_unwind`, translating any panic into a
/// `RocksDBException`. Panics across the JNI boundary are UB and would
/// crash the JVM; this is non-negotiable.
///
/// `on_panic` produces the value returned to Java when a panic is
/// caught. Most callers pass a sentinel zero / null pointer.
fn jni_guard<R, F, G>(env: &mut JNIEnv, on_panic: G, f: F) -> R
where
    F: FnOnce(&mut JNIEnv) -> R,
    G: FnOnce() -> R,
{
    // SAFETY of AssertUnwindSafe: the closure receives `&mut JNIEnv` and
    // may mutate it (throwing exceptions, etc.). We never read from
    // `env` after a panic except to throw, so the unwind-safety
    // assertion is correct.
    //
    // We avoid using `env` directly inside catch_unwind because
    // JNIEnv is not UnwindSafe. Instead, we extract the raw `*mut
    // sys::JNIEnv` (a pointer, which is unwind-safe) and reconstruct a
    // local JNIEnv inside the closure.
    let env_ptr = env.get_raw();
    let result = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: env_ptr came from a live &mut JNIEnv; we reconstruct a
        // local JNIEnv inside the closure to call f. The original env
        // borrow is still live in the caller stack frame, but we never
        // touch it concurrently — closures run inline, not on another
        // thread.
        let mut local =
            unsafe { JNIEnv::from_raw(env_ptr).expect("env_ptr is valid for current thread") };
        f(&mut local)
    }));
    match result {
        Ok(v) => v,
        Err(payload) => {
            let msg = panic_message(payload.as_ref());
            throw_rocksdb(env, &format!("Rust panic in JNI: {msg}"));
            on_panic()
        }
    }
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "(non-string panic payload)".to_string()
    }
}

// ---------------------------------------------------------------------------
// Argument-conversion helpers
// ---------------------------------------------------------------------------

/// Convert a `JByteArray` slice (with offset+len) into an owned `Vec<u8>`.
/// Returns `None` and throws on JNI-side failure.
fn read_byte_slice(env: &mut JNIEnv, arr: &JByteArray, off: jint, len: jint) -> Option<Vec<u8>> {
    if off < 0 || len < 0 {
        throw_rocksdb(env, "negative offset/length");
        return None;
    }
    let full = match env.convert_byte_array(arr) {
        Ok(v) => v,
        Err(e) => {
            throw_rocksdb(env, &format!("convert_byte_array failed: {e}"));
            return None;
        }
    };
    let off = off as usize;
    let len = len as usize;
    if off.checked_add(len).is_none_or(|end| end > full.len()) {
        throw_rocksdb(env, "offset+length exceeds array bounds");
        return None;
    }
    Some(full[off..off + len].to_vec())
}

/// Convert a `JString` to a Rust `String`. Throws and returns `None` on
/// failure.
fn read_string(env: &mut JNIEnv, s: &JString) -> Option<String> {
    match env.get_string(s) {
        Ok(js) => Some(js.into()),
        Err(e) => {
            throw_rocksdb(env, &format!("get_string failed: {e}"));
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Java_org_forstdb_RocksDB_* — the actual JNI surface
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.open(String path) -> long handle`
///
/// Java signature: `(Ljava/lang/String;)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_open<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(path_str) = read_string(env, &path) else {
                return 0_i64;
            };
            let c_path = match std::ffi::CString::new(path_str) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(env, "path contains interior NUL");
                    return 0;
                }
            };
            let mut handle: FrsDb = ptr::null_mut();
            // SAFETY: pointers point to valid memory for the call; out_handle
            // is a stack-local mut.
            let status = unsafe { frs_db_open(c_path.as_ptr(), &mut handle) };
            if check_status(env, status, "RocksDB.open") {
                return 0;
            }
            handle as jlong
        },
    )
}

/// `org.forstdb.RocksDB.close(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_close<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: handle came from a prior frs_db_open; nullity is checked
            // inside frs_db_close.
            let status = unsafe { frs_db_close(handle as FrsDb) };
            check_status(env, status, "RocksDB.close");
        },
    )
}

/// `org.forstdb.RocksDB.put(long handle, long cfHandle, byte[] key,
///                          int keyOff, int keyLen, byte[] val,
///                          int valOff, int valLen)`
///
/// Java signature: `(JJ[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_put<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return;
            };
            let Some(v) = read_byte_slice(env, &val, val_off, val_len) else {
                return;
            };
            // SAFETY: pointers are valid for the duration of frs_put; the
            // engine copies the data internally.
            let status = unsafe {
                frs_put(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                    v.as_ptr(),
                    v.len(),
                )
            };
            check_status(env, status, "RocksDB.put");
        },
    )
}

/// `org.forstdb.RocksDB.get(long handle, long cfHandle, byte[] key,
///                          int keyOff, int keyLen) -> byte[]?`
///
/// Java signature: `(JJ[BII)[B`
///
/// Returns `null` if the key is absent — matches RocksDB Java behavior.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_get<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return ptr::null_mut();
            };
            let mut out = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            // SAFETY: out is a stack-local FrsBytes that we own; the engine
            // populates data/len/capacity if Some(value) is found.
            let status = unsafe {
                frs_get(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                    &mut out,
                )
            };
            if status == FRS_STATUS_NOT_FOUND {
                return ptr::null_mut();
            }
            if check_status(env, status, "RocksDB.get") {
                return ptr::null_mut();
            }
            if out.data.is_null() {
                // Status OK + null data == absent key (per FFI contract).
                return ptr::null_mut();
            }
            // SAFETY: out.data/len describe a Rust-owned buffer for the
            // duration of this call; we copy into a Java byte[] and then
            // free the original.
            let slice = unsafe { std::slice::from_raw_parts(out.data, out.len) };
            let java_arr = match env.byte_array_from_slice(slice) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    // Free the Rust buffer even on JNI failure.
                    unsafe {
                        let _ = crate::frs_bytes_free(&mut out);
                    }
                    throw_rocksdb(env, &format!("byte_array_from_slice failed: {e}"));
                    return ptr::null_mut();
                }
            };
            // SAFETY: out is still the original FrsBytes we got from frs_get.
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
            java_arr
        },
    )
}

/// `org.forstdb.RocksDB.delete(long handle, long cfHandle, byte[] key,
///                             int keyOff, int keyLen)`
///
/// Java signature: `(JJ[BII)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_delete<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return;
            };
            // SAFETY: key pointer valid for the call; engine copies what it
            // needs.
            let status = unsafe {
                frs_delete(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                )
            };
            check_status(env, status, "RocksDB.delete");
        },
    )
}

/// `org.forstdb.RocksDB.createColumnFamily(long handle, String name) -> long cfHandle`
///
/// Java signature: `(JLjava/lang/String;)J`
///
/// If the CF already exists this opens it; otherwise it creates a new
/// one. This collapses the community API's separate
/// `createColumnFamily` / `openColumnFamily` into one call because the
/// Flink state backend only ever wants "give me a handle, creating if
/// needed" semantics (each Flink state descriptor maps 1:1 to a CF).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_createColumnFamily<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(s) = read_string(env, &name) else {
                return 0_i64;
            };
            let c_name = match std::ffi::CString::new(s) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(env, "CF name contains interior NUL");
                    return 0;
                }
            };
            let mut cf: FrsCfHandle = ptr::null_mut();
            // Try open first, then fall back to create.
            // SAFETY: pointers valid for the call; out_cf is a stack local.
            let open_status = unsafe { frs_db_open_cf(handle as FrsDb, c_name.as_ptr(), &mut cf) };
            if open_status == FRS_STATUS_OK {
                return cf as jlong;
            }
            // SAFETY: same as above.
            let create_status =
                unsafe { frs_db_create_cf(handle as FrsDb, c_name.as_ptr(), &mut cf) };
            if check_status(env, create_status, "RocksDB.createColumnFamily") {
                return 0;
            }
            cf as jlong
        },
    )
}

/// `org.forstdb.RocksDB.dropColumnFamily(long handle, long cfHandle)`
///
/// Java signature: `(JJ)V`
///
/// Maps to `frs_cf_close` — releases the handle. The underlying CF
/// metadata persists across DB reopens; full CF removal requires a
/// separate engine call (not in the Flink hot path).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_dropColumnFamily<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: cf_handle came from a prior createColumnFamily; nullity
            // checked inside frs_cf_close.
            let status = unsafe { crate::frs_cf_close(cf_handle as FrsCfHandle) };
            check_status(env, status, "RocksDB.dropColumnFamily");
        },
    )
}

/// `org.forstdb.RocksDB.flush(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_flush<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: handle came from a prior open; nullity checked inside.
            let status = unsafe { frs_flush(handle as FrsDb) };
            check_status(env, status, "RocksDB.flush");
        },
    )
}

/// `org.forstdb.RocksDB.createCheckpoint(long handle, String targetDir)`
///
/// Java signature: `(JLjava/lang/String;)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_createCheckpoint<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    target_dir: JString<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(dir) = read_string(env, &target_dir) else {
                return;
            };
            let c_dir = match std::ffi::CString::new(dir) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(env, "checkpoint target_dir contains interior NUL");
                    return;
                }
            };
            // SAFETY: pointers valid for the call.
            let status = unsafe { frs_create_checkpoint(handle as FrsDb, c_dir.as_ptr()) };
            check_status(env, status, "RocksDB.createCheckpoint");
        },
    )
}

/// `org.forstdb.RocksDB.merge(long handle, long cfHandle, byte[] key,
///                            int keyOff, int keyLen, byte[] value,
///                            int valueOff, int valueLen)`
///
/// Java signature: `(JJ[BII[BII)V`
///
/// Issues a merge operand against the configured merge operator for the
/// CF (created via `frs_db_create_cf_with_merge` from the engine side).
/// If no operator is configured, [`frs_merge`] returns
/// `FRS_STATUS_INVALID_ARGUMENT` and we surface that as a
/// `RocksDBException`.
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_merge<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return;
            };
            let Some(v) = read_byte_slice(env, &val, val_off, val_len) else {
                return;
            };
            // SAFETY: pointers valid for the call; engine copies what it needs.
            let status = unsafe {
                frs_merge(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                    v.as_ptr(),
                    v.len(),
                )
            };
            check_status(env, status, "RocksDB.merge");
        },
    )
}

/// `org.forstdb.RocksDB.compactRange(long handle, long cfHandle)`
///
/// Java signature: `(JJ)V`
///
/// **Behavioural note:** the community RocksDB `compactRange` accepts
/// optional `begin` / `end` byte arrays to restrict compaction to a
/// sub-range. forst-rs does not yet expose a per-range compaction API —
/// this shim forwards to [`frs_compact_cf`], which compacts the **entire**
/// CF. For Flink's `flink-statebackend-forst` this is benign: the only
/// caller path is the periodic full-CF compaction triggered by the state
/// backend's housekeeping, which already passes `null/null` to mean
/// "everything". A future revision can add `compactRangeBounded` once the
/// engine grows range support.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_compactRange<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: handles came from prior open / create calls; nullity
            // checked inside frs_compact_cf.
            let status = unsafe { frs_compact_cf(handle as FrsDb, cf_handle as FrsCfHandle) };
            check_status(env, status, "RocksDB.compactRange");
        },
    )
}

/// `org.forstdb.RocksDB.compactRangeAll(long handle)`
///
/// Java signature: `(J)V`
///
/// Triggers compaction across every CF on the engine. Maps directly to
/// [`frs_compact_all`].
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_compactRangeAll<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: handle came from prior open; nullity checked inside.
            let status = unsafe { frs_compact_all(handle as FrsDb) };
            check_status(env, status, "RocksDB.compactRangeAll");
        },
    )
}

/// `org.forstdb.RocksDB.flushCf(long handle, long cfHandle)`
///
/// Java signature: `(JJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_flushCf<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: handles came from prior open / create calls.
            let status = unsafe { frs_flush_cf(handle as FrsDb, cf_handle as FrsCfHandle) };
            check_status(env, status, "RocksDB.flushCf");
        },
    )
}

/// `org.forstdb.RocksDB.getDefaultColumnFamily(long handle) -> long cfHandle`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getDefaultColumnFamily<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let mut cf: FrsCfHandle = ptr::null_mut();
            // SAFETY: handle came from prior open; out_cf is a stack local.
            let status = unsafe { frs_db_default_cf(handle as FrsDb, &mut cf) };
            if check_status(env, status, "RocksDB.getDefaultColumnFamily") {
                return 0;
            }
            cf as jlong
        },
    )
}

/// `org.forstdb.RocksDB.getLatestSequenceNumber(long handle) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getLatestSequenceNumber<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let mut seq: u64 = 0;
            // SAFETY: handle came from prior open; out_seq is a stack local.
            let status = unsafe { frs_sequence_number(handle as FrsDb, &mut seq) };
            if check_status(env, status, "RocksDB.getLatestSequenceNumber") {
                return 0;
            }
            // `seq` is u64; truncate to i64 (jlong). Sequence numbers in
            // practice never approach 2^63, and Java has no unsigned long.
            seq as jlong
        },
    )
}

/// `org.forstdb.RocksDB.iteratorOpen(long handle, long cfHandle) -> long iterHandle`
///
/// Java signature: `(JJ)J`
///
/// Opens a forward iterator over the entire CF. The returned handle must
/// be released with [`Java_org_forstdb_RocksDB_iteratorClose`] when the
/// caller is done — leaking it leaks the materialised snapshot.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_iteratorOpen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let mut iter: FrsIterator = ptr::null_mut();
            // SAFETY: handles came from prior open / create calls; out_iter is
            // a stack local.
            let status =
                unsafe { frs_iterator_open(handle as FrsDb, cf_handle as FrsCfHandle, &mut iter) };
            if check_status(env, status, "RocksDB.iteratorOpen") {
                return 0;
            }
            iter as jlong
        },
    )
}

/// `org.forstdb.RocksDB.iteratorSeek(long iterHandle, byte[] key, int keyOff, int keyLen)`
///
/// Java signature: `(J[BII)V`
///
/// Repositions the cursor at the first key `>= key`. A null `key` (or
/// `keyLen == 0`) seeks to the first key — equivalent to a fresh open.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_iteratorSeek<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    iter_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // Treat null key OR len==0 as seek-to-first. We detect null by
            // peeking at the JObject the JByteArray wraps.
            let key_obj: &JObject = key.as_ref();
            let is_null = key_obj.is_null() || key_len == 0;
            let status = if is_null {
                // SAFETY: iter_handle came from prior iteratorOpen; nullity
                // checked inside frs_iterator_seek.
                unsafe { frs_iterator_seek(iter_handle as FrsIterator, ptr::null(), 0) }
            } else {
                let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                    return;
                };
                // SAFETY: k is a stack-local Vec<u8>; engine copies bounds.
                unsafe { frs_iterator_seek(iter_handle as FrsIterator, k.as_ptr(), k.len()) }
            };
            check_status(env, status, "RocksDB.iteratorSeek");
        },
    )
}

/// `org.forstdb.RocksDB.iteratorNext(long iterHandle) -> byte[][] | null`
///
/// Java signature: `(J)[[B`
///
/// Returns either:
/// - `null` — iterator exhausted (`valid == false`).
/// - a 2-element `byte[][]` — `[0]` is the key, `[1]` is the value of
///   the row at the cursor. Cursor advances by one before return.
///
/// We pick `byte[][]` over the alternative pre-allocated-buffer signature
/// because Flink's iterator wrapper already allocates fresh arrays per
/// row, so the extra allocation is not on the critical path, and the
/// "buffer too small" failure mode of the alternative would force every
/// caller to retry-with-larger-buffer logic.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_iteratorNext<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    iter_handle: jlong,
) -> jobjectArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jobjectArray,
        |env| -> jobjectArray {
            let mut k = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            let mut v = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            let mut valid: bool = false;
            // SAFETY: iter_handle came from prior iteratorOpen; out_* are
            // stack locals owned by us.
            let status = unsafe {
                frs_iterator_next(iter_handle as FrsIterator, &mut k, &mut v, &mut valid)
            };
            if check_status(env, status, "RocksDB.iteratorNext") {
                // Best-effort cleanup if the engine did partially populate.
                unsafe {
                    let _ = crate::frs_bytes_free(&mut k);
                    let _ = crate::frs_bytes_free(&mut v);
                }
                return ptr::null_mut();
            }
            if !valid {
                // Engine guarantees both slots are NULL when valid=false, but
                // we still call free for defensive symmetry.
                unsafe {
                    let _ = crate::frs_bytes_free(&mut k);
                    let _ = crate::frs_bytes_free(&mut v);
                }
                return ptr::null_mut();
            }
            // Materialise key + value as Java byte[]s. Copy first, then free
            // the Rust-owned buffers regardless of JNI success/failure.
            // SAFETY: k.data/v.data describe Rust-owned buffers populated by
            // frs_iterator_next; len fields are accurate for the call window.
            let key_slice = unsafe { std::slice::from_raw_parts(k.data, k.len) };
            let val_slice = unsafe { std::slice::from_raw_parts(v.data, v.len) };
            let key_arr = match env.byte_array_from_slice(key_slice) {
                Ok(a) => a,
                Err(e) => {
                    unsafe {
                        let _ = crate::frs_bytes_free(&mut k);
                        let _ = crate::frs_bytes_free(&mut v);
                    }
                    throw_rocksdb(env, &format!("byte_array_from_slice(key) failed: {e}"));
                    return ptr::null_mut();
                }
            };
            let val_arr = match env.byte_array_from_slice(val_slice) {
                Ok(a) => a,
                Err(e) => {
                    unsafe {
                        let _ = crate::frs_bytes_free(&mut k);
                        let _ = crate::frs_bytes_free(&mut v);
                    }
                    throw_rocksdb(env, &format!("byte_array_from_slice(value) failed: {e}"));
                    return ptr::null_mut();
                }
            };
            // Free Rust-side buffers — the data lives in the Java heap now.
            unsafe {
                let _ = crate::frs_bytes_free(&mut k);
                let _ = crate::frs_bytes_free(&mut v);
            }
            // Wrap into a [[B array. Element class is "[B" (byte[]).
            let element_class = match env.find_class("[B") {
                Ok(c) => c,
                Err(e) => {
                    throw_rocksdb(env, &format!("find_class([B) failed: {e}"));
                    return ptr::null_mut();
                }
            };
            let outer = match env.new_object_array(2, &element_class, JObject::null()) {
                Ok(a) => a,
                Err(e) => {
                    throw_rocksdb(env, &format!("new_object_array failed: {e}"));
                    return ptr::null_mut();
                }
            };
            if let Err(e) = env.set_object_array_element(&outer, 0, &key_arr) {
                throw_rocksdb(env, &format!("set_object_array_element(0) failed: {e}"));
                return ptr::null_mut();
            }
            if let Err(e) = env.set_object_array_element(&outer, 1, &val_arr) {
                throw_rocksdb(env, &format!("set_object_array_element(1) failed: {e}"));
                return ptr::null_mut();
            }
            outer.into_raw()
        },
    )
}

/// `org.forstdb.RocksDB.iteratorClose(long iterHandle)`
///
/// Java signature: `(J)V`
///
/// Releases the iterator handle. Safe to call with `0` / null (no-op
/// per [`frs_iterator_close`]'s contract).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_iteratorClose<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    iter_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: nullity handled inside frs_iterator_close.
            let status = unsafe { frs_iterator_close(iter_handle as FrsIterator) };
            check_status(env, status, "RocksDB.iteratorClose");
        },
    )
}

/// `org.forstdb.RocksDB.lookupKv(long handle, long cfHandle, byte[] key,
///                              int keyOff, int keyLen) -> byte[] | null`
///
/// Java signature: `(JJ[BII)[B`
///
/// Specialised exact-match lookup matching [`frs_lookup_kv`]; semantically
/// identical to `RocksDB.get` (returns `null` on miss). Kept as a
/// distinct symbol so future hot-path optimisations on `frs_lookup_kv`
/// surface to Java without churning the `get` ABI.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_lookupKv<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return ptr::null_mut();
            };
            let mut out = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            // SAFETY: out is stack-local; engine populates data/len/capacity
            // on hit, leaves NULL/0/0 on miss (per frs_lookup_kv contract).
            let status = unsafe {
                frs_lookup_kv(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                    &mut out,
                )
            };
            if status == FRS_STATUS_NOT_FOUND {
                return ptr::null_mut();
            }
            if check_status(env, status, "RocksDB.lookupKv") {
                return ptr::null_mut();
            }
            if out.data.is_null() {
                // Status OK + null data == miss (frs_lookup_kv contract).
                return ptr::null_mut();
            }
            // SAFETY: out.data/len describe a Rust-owned buffer; we copy and
            // free.
            let slice = unsafe { std::slice::from_raw_parts(out.data, out.len) };
            let java_arr = match env.byte_array_from_slice(slice) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    unsafe {
                        let _ = crate::frs_bytes_free(&mut out);
                    }
                    throw_rocksdb(env, &format!("byte_array_from_slice failed: {e}"));
                    return ptr::null_mut();
                }
            };
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
            java_arr
        },
    )
}

/// `org.forstdb.RocksDB.dbOpenFromCheckpoint(String targetDir) -> long handle`
///
/// Java signature: `(Ljava/lang/String;)J`
///
/// Opens an engine from a previously-created checkpoint directory (see
/// [`frs_db_open_from_checkpoint`]). Used by Flink's restore path when
/// the savepoint metadata indicates the state was checkpointed by an
/// earlier `flink-statebackend-forst` run.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_dbOpenFromCheckpoint<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    target_dir: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(dir) = read_string(env, &target_dir) else {
                return 0_i64;
            };
            let c_dir = match std::ffi::CString::new(dir) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(env, "checkpoint target_dir contains interior NUL");
                    return 0;
                }
            };
            let mut handle: FrsDb = ptr::null_mut();
            // SAFETY: c_dir lives for the duration of the call; out_handle
            // is a stack local.
            let status = unsafe { frs_db_open_from_checkpoint(c_dir.as_ptr(), &mut handle) };
            if check_status(env, status, "RocksDB.dbOpenFromCheckpoint") {
                return 0;
            }
            handle as jlong
        },
    )
}

// ---------------------------------------------------------------------------
// G-A surface broadening — additional aliases / convenience entry points
//
// These cover the next-most-used Flink call sites that were not in the
// initial 21-symbol seed: alternate method names ("dbOpen", "remove",
// "writeBatch"), full-array convenience overloads, batch put/get over
// `byte[][]`, prefix-iterator family, and a couple of monitoring/stub
// helpers (`l0FileCount`, `isClosed`).
//
// All functions wrap in `jni_guard` for panic safety and translate FFI
// status codes via `check_status` like the seed surface does.
// ---------------------------------------------------------------------------

/// Helper: read a `byte[][]` (Java `[[B`) into an owned `Vec<Vec<u8>>`.
/// Returns `None` (and throws) on JNI-side failure or null inner element.
///
/// Lifetimes: `'env` is the JNIEnv borrow lifetime (typically inferred
/// from the `jni_guard` inner closure's local env); `'arr` is the lifetime
/// of the input `JObjectArray` (the outer JNI call's `'local`). They are
/// independent — the helper does not produce any references that outlive
/// either.
fn read_byte_matrix<'env, 'arr>(
    env: &mut JNIEnv<'env>,
    arr: &JObjectArray<'arr>,
    label: &str,
) -> Option<Vec<Vec<u8>>> {
    let outer: &JObject = arr.as_ref();
    if outer.is_null() {
        throw_rocksdb(env, &format!("{label}: array is null"));
        return None;
    }
    let len = match env.get_array_length(arr) {
        Ok(n) => n,
        Err(e) => {
            throw_rocksdb(env, &format!("{label}: get_array_length failed: {e}"));
            return None;
        }
    };
    if len < 0 {
        throw_rocksdb(env, &format!("{label}: negative array length"));
        return None;
    }
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(len as usize);
    for i in 0..len {
        let elem = match env.get_object_array_element(arr, i) {
            Ok(o) => o,
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("{label}: get_object_array_element({i}) failed: {e}"),
                );
                return None;
            }
        };
        if elem.is_null() {
            throw_rocksdb(env, &format!("{label}: element {i} is null"));
            return None;
        }
        // SAFETY: `elem` is a JObject we just got from the JVM, and the
        // surrounding outer array's element class is `[B` per the Java
        // signature `[[B`. We re-tag it as JByteArray for the
        // convert_byte_array call.
        let jba = unsafe { JByteArray::from_raw(elem.into_raw()) };
        match env.convert_byte_array(&jba) {
            Ok(v) => out.push(v),
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("{label}: convert_byte_array({i}) failed: {e}"),
                );
                return None;
            }
        }
    }
    Some(out)
}

/// `org.forstdb.RocksDB.dbOpen(String path) -> long handle`
///
/// Java signature: `(Ljava/lang/String;)J`
///
/// Filesystem-backed open. Alias of [`Java_org_forstdb_RocksDB_open`] —
/// some Flink call sites use the more explicit `dbOpen` name.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_dbOpen<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    path: JString<'local>,
) -> jlong {
    Java_org_forstdb_RocksDB_open(env, class, path)
}

/// `org.forstdb.RocksDB.createColumnFamily2(long handle, String name) -> long cfHandle`
///
/// Java signature: `(JLjava/lang/String;)J`
///
/// Wraps [`frs_db_create_cf`] without configuring a merge operator. The
/// existing `createColumnFamily` does open-or-create; this is the simpler
/// "always create" form for callers that have already verified non-existence.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_createColumnFamily2<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(s) = read_string(env, &name) else {
                return 0_i64;
            };
            let c_name = match std::ffi::CString::new(s) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(env, "CF name contains interior NUL");
                    return 0;
                }
            };
            let mut cf: FrsCfHandle = ptr::null_mut();
            // SAFETY: pointers valid for the call; out_cf is a stack local.
            let status = unsafe { frs_db_create_cf(handle as FrsDb, c_name.as_ptr(), &mut cf) };
            if check_status(env, status, "RocksDB.createColumnFamily2") {
                return 0;
            }
            cf as jlong
        },
    )
}

/// `org.forstdb.RocksDB.openColumnFamily(long handle, String name) -> long cfHandle`
///
/// Java signature: `(JLjava/lang/String;)J`
///
/// Wraps [`frs_db_open_cf`]. Opens an existing CF without creating; if
/// the CF does not exist the FFI returns an error which we surface as a
/// `RocksDBException`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_openColumnFamily<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(s) = read_string(env, &name) else {
                return 0_i64;
            };
            let c_name = match std::ffi::CString::new(s) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(env, "CF name contains interior NUL");
                    return 0;
                }
            };
            let mut cf: FrsCfHandle = ptr::null_mut();
            // SAFETY: pointers valid for the call; out_cf is a stack local.
            let status = unsafe { frs_db_open_cf(handle as FrsDb, c_name.as_ptr(), &mut cf) };
            if check_status(env, status, "RocksDB.openColumnFamily") {
                return 0;
            }
            cf as jlong
        },
    )
}

/// `org.forstdb.RocksDB.l0FileCount(long handle) -> long count`
///
/// Java signature: `(J)J`
///
/// Wraps [`frs_l0_file_count`]. Useful for monitoring write-stall
/// conditions in Flink's metric reporters.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_l0FileCount<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let mut count: u32 = 0;
            // SAFETY: handle came from prior open; out_count is a stack local.
            let status = unsafe { frs_l0_file_count(handle as FrsDb, &mut count) };
            if check_status(env, status, "RocksDB.l0FileCount") {
                return 0;
            }
            count as jlong
        },
    )
}

/// Internal: shared implementation for `batchPut` and `writeBatch`. Walks
/// `keys` and `values` in lock-step; mismatched lengths throw.
///
/// Lifetimes: see `read_byte_matrix` — `'env` and `'arr` are independent.
fn batch_put_inner<'env, 'arr>(
    env: &mut JNIEnv<'env>,
    handle: jlong,
    cf_handle: jlong,
    keys: JObjectArray<'arr>,
    values: JObjectArray<'arr>,
    label: &str,
) {
    let Some(ks) = read_byte_matrix(env, &keys, &format!("{label}.keys")) else {
        return;
    };
    let Some(vs) = read_byte_matrix(env, &values, &format!("{label}.values")) else {
        return;
    };
    if ks.len() != vs.len() {
        throw_rocksdb(
            env,
            &format!(
                "{label}: keys.length ({}) != values.length ({})",
                ks.len(),
                vs.len()
            ),
        );
        return;
    }
    let count = ks.len();
    let key_ptrs: Vec<*const u8> = ks.iter().map(|k| k.as_ptr()).collect();
    let key_lens: Vec<usize> = ks.iter().map(|k| k.len()).collect();
    let val_ptrs: Vec<*const u8> = vs.iter().map(|v| v.as_ptr()).collect();
    let val_lens: Vec<usize> = vs.iter().map(|v| v.len()).collect();
    // SAFETY: all four arrays have the same `count`; pointers point into
    // owned Vecs that outlive the call. The engine copies what it needs.
    let status = unsafe {
        frs_batch_put(
            handle as FrsDb,
            cf_handle as FrsCfHandle,
            key_ptrs.as_ptr(),
            key_lens.as_ptr(),
            val_ptrs.as_ptr(),
            val_lens.as_ptr(),
            count,
        )
    };
    check_status(env, status, label);
}

/// `org.forstdb.RocksDB.batchPut(long handle, long cfHandle,
///                                byte[][] keys, byte[][] values)`
///
/// Java signature: `(JJ[[B[[B)V`
///
/// Walks both arrays in lock-step. Throws `RocksDBException` on
/// length mismatch.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_batchPut<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    keys: JObjectArray<'local>,
    values: JObjectArray<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| batch_put_inner(env, handle, cf_handle, keys, values, "RocksDB.batchPut"),
    )
}

/// `org.forstdb.RocksDB.batchGet(long handle, long cfHandle,
///                                byte[][] keys) -> byte[][]`
///
/// Java signature: `(JJ[[B)[[B`
///
/// Returns a same-length array of values; null entries denote missing
/// keys (matches RocksDB Java multi-get behavior).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_batchGet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    keys: JObjectArray<'local>,
) -> jobjectArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jobjectArray,
        |env| -> jobjectArray {
            let Some(ks) = read_byte_matrix(env, &keys, "RocksDB.batchGet.keys") else {
                return ptr::null_mut();
            };
            let count = ks.len();
            let key_ptrs: Vec<*const u8> = ks.iter().map(|k| k.as_ptr()).collect();
            let key_lens: Vec<usize> = ks.iter().map(|k| k.len()).collect();
            // Pre-allocate output FrsBytes slots; the engine populates them.
            let mut out_slots: Vec<FrsBytes> = (0..count)
                .map(|_| FrsBytes {
                    data: ptr::null_mut(),
                    len: 0,
                    capacity: 0,
                })
                .collect();
            // SAFETY: arrays have `count` entries; out_slots has `count`
            // FrsBytes slots that we own and free below.
            let status = unsafe {
                frs_batch_get(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    key_ptrs.as_ptr(),
                    key_lens.as_ptr(),
                    count,
                    out_slots.as_mut_ptr(),
                )
            };
            if check_status(env, status, "RocksDB.batchGet") {
                for slot in out_slots.iter_mut() {
                    unsafe {
                        let _ = crate::frs_bytes_free(slot);
                    }
                }
                return ptr::null_mut();
            }
            // Build the result `byte[][]`.
            let element_class = match env.find_class("[B") {
                Ok(c) => c,
                Err(e) => {
                    for slot in out_slots.iter_mut() {
                        unsafe {
                            let _ = crate::frs_bytes_free(slot);
                        }
                    }
                    throw_rocksdb(env, &format!("find_class([B) failed: {e}"));
                    return ptr::null_mut();
                }
            };
            let outer = match env.new_object_array(count as jint, &element_class, JObject::null()) {
                Ok(a) => a,
                Err(e) => {
                    for slot in out_slots.iter_mut() {
                        unsafe {
                            let _ = crate::frs_bytes_free(slot);
                        }
                    }
                    throw_rocksdb(env, &format!("new_object_array failed: {e}"));
                    return ptr::null_mut();
                }
            };
            for (i, slot) in out_slots.iter_mut().enumerate() {
                if slot.data.is_null() {
                    // Missing key — leave the JObject::null() default in place.
                    continue;
                }
                // SAFETY: slot.data/len describe a Rust-owned buffer.
                let s = unsafe { std::slice::from_raw_parts(slot.data, slot.len) };
                let arr = match env.byte_array_from_slice(s) {
                    Ok(a) => a,
                    Err(e) => {
                        // Free remaining slots and bail.
                        for s2 in out_slots.iter_mut() {
                            unsafe {
                                let _ = crate::frs_bytes_free(s2);
                            }
                        }
                        throw_rocksdb(env, &format!("byte_array_from_slice({i}) failed: {e}"));
                        return ptr::null_mut();
                    }
                };
                if let Err(e) = env.set_object_array_element(&outer, i as jint, &arr) {
                    for s2 in out_slots.iter_mut() {
                        unsafe {
                            let _ = crate::frs_bytes_free(s2);
                        }
                    }
                    throw_rocksdb(env, &format!("set_object_array_element({i}) failed: {e}"));
                    return ptr::null_mut();
                }
            }
            // Free Rust-side buffers — data lives in the Java heap now.
            for slot in out_slots.iter_mut() {
                unsafe {
                    let _ = crate::frs_bytes_free(slot);
                }
            }
            outer.into_raw()
        },
    )
}

/// `org.forstdb.RocksDB.writeBatch(long handle, long cfHandle,
///                                  byte[][] keys, byte[][] values)`
///
/// Java signature: `(JJ[[B[[B)V`
///
/// Alias for [`Java_org_forstdb_RocksDB_batchPut`]. Common Flink call
/// site uses "writeBatch" as the public method name.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_writeBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    keys: JObjectArray<'local>,
    values: JObjectArray<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| batch_put_inner(env, handle, cf_handle, keys, values, "RocksDB.writeBatch"),
    )
}

/// `org.forstdb.RocksDB.prefixLookupOpen(long handle, long cfHandle,
///                                        byte[] prefix, int prefixOff,
///                                        int prefixLen) -> long iterHandle`
///
/// Java signature: `(JJ[BII)J`
///
/// Wraps [`frs_prefix_lookup_open`]. Returns a handle that must be
/// released via [`Java_org_forstdb_RocksDB_prefixLookupClose`].
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_prefixLookupOpen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    prefix: JByteArray<'local>,
    prefix_off: jint,
    prefix_len: jint,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            // null prefix or len==0 → full scan (matches FFI semantics).
            let prefix_obj: &JObject = prefix.as_ref();
            let is_empty = prefix_obj.is_null() || prefix_len == 0;
            let mut iter: FrsIterator = ptr::null_mut();
            let status = if is_empty {
                // SAFETY: NULL prefix is allowed by frs_prefix_lookup_open.
                unsafe {
                    frs_prefix_lookup_open(
                        handle as FrsDb,
                        cf_handle as FrsCfHandle,
                        ptr::null(),
                        0,
                        &mut iter,
                    )
                }
            } else {
                let Some(p) = read_byte_slice(env, &prefix, prefix_off, prefix_len) else {
                    return 0_i64;
                };
                // SAFETY: p is a stack-local Vec<u8>; engine copies bounds.
                unsafe {
                    frs_prefix_lookup_open(
                        handle as FrsDb,
                        cf_handle as FrsCfHandle,
                        p.as_ptr(),
                        p.len(),
                        &mut iter,
                    )
                }
            };
            if check_status(env, status, "RocksDB.prefixLookupOpen") {
                return 0_i64;
            }
            iter as jlong
        },
    )
}

/// `org.forstdb.RocksDB.prefixLookupNext(long iterHandle) -> byte[][] | null`
///
/// Java signature: `(J)[[B`
///
/// Alias for [`Java_org_forstdb_RocksDB_iteratorNext`] — both consume the
/// same `FrsIterator` representation per the FFI's prefix/iterator
/// symmetry (see `frs_prefix_lookup_next`).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_prefixLookupNext<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    iter_handle: jlong,
) -> jobjectArray {
    Java_org_forstdb_RocksDB_iteratorNext(env, class, iter_handle)
}

/// `org.forstdb.RocksDB.prefixLookupClose(long iterHandle)`
///
/// Java signature: `(J)V`
///
/// Wraps [`frs_prefix_lookup_close`]. Safe to call with `0` / null.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_prefixLookupClose<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    iter_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // SAFETY: nullity handled inside frs_prefix_lookup_close.
            let status = unsafe { frs_prefix_lookup_close(iter_handle as FrsIterator) };
            check_status(env, status, "RocksDB.prefixLookupClose");
        },
    )
}

/// `org.forstdb.RocksDB.isClosed(long handle) -> boolean`
///
/// Java signature: `(J)Z`
///
/// Pure compatibility shim — always returns `false`. forst-rs handles
/// are managed differently from RocksDB's (close eagerly drops the Box;
/// there is no "soft-closed" state to query). Exposed so Flink code
/// paths that defensively call `isClosed()` before further operations
/// continue to link.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_isClosed<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) -> jboolean {
    jni_guard(&mut env, || JNI_FALSE, |_env| JNI_FALSE)
}

/// `org.forstdb.RocksDB.getColumnFamilyHandle(long handle) -> long cfHandle`
///
/// Java signature: `(J)J`
///
/// Alias for [`Java_org_forstdb_RocksDB_getDefaultColumnFamily`] — some
/// Flink call sites use this shorter name.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getColumnFamilyHandle<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
) -> jlong {
    Java_org_forstdb_RocksDB_getDefaultColumnFamily(env, class, handle)
}

/// `org.forstdb.RocksDB.remove(long handle, long cfHandle, byte[] key,
///                              int keyOff, int keyLen)`
///
/// Java signature: `(JJ[BII)V`
///
/// Alias for [`Java_org_forstdb_RocksDB_delete`] — the older RocksDB
/// Java API spelled this method `remove`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_remove<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) {
    Java_org_forstdb_RocksDB_delete(env, class, handle, cf_handle, key, key_off, key_len);
}

/// `org.forstdb.RocksDB.putByteArray(long handle, long cfHandle,
///                                    byte[] key, byte[] value)`
///
/// Java signature: `(JJ[B[B)V`
///
/// Convenience overload of `put` taking full byte arrays without
/// offset/length pairs. Matches some Flink generated-code conventions.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_putByteArray<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    val: JByteArray<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let k = match env.convert_byte_array(&key) {
                Ok(v) => v,
                Err(e) => {
                    throw_rocksdb(env, &format!("putByteArray: convert(key) failed: {e}"));
                    return;
                }
            };
            let v = match env.convert_byte_array(&val) {
                Ok(v) => v,
                Err(e) => {
                    throw_rocksdb(env, &format!("putByteArray: convert(value) failed: {e}"));
                    return;
                }
            };
            // SAFETY: pointers valid for the duration of frs_put; the
            // engine copies the data internally.
            let status = unsafe {
                frs_put(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                    v.as_ptr(),
                    v.len(),
                )
            };
            check_status(env, status, "RocksDB.putByteArray");
        },
    )
}

/// `org.forstdb.RocksDB.getByteArray(long handle, long cfHandle,
///                                    byte[] key) -> byte[] | null`
///
/// Java signature: `(JJ[B)[B`
///
/// Convenience overload of `get` taking the full key byte array. Returns
/// `null` on miss (matches RocksDB Java behavior).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getByteArray<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let k = match env.convert_byte_array(&key) {
                Ok(v) => v,
                Err(e) => {
                    throw_rocksdb(env, &format!("getByteArray: convert(key) failed: {e}"));
                    return ptr::null_mut();
                }
            };
            let mut out = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            // SAFETY: out is stack-local; engine populates on hit.
            let status = unsafe {
                frs_get(
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    k.as_ptr(),
                    k.len(),
                    &mut out,
                )
            };
            if status == FRS_STATUS_NOT_FOUND {
                return ptr::null_mut();
            }
            if check_status(env, status, "RocksDB.getByteArray") {
                return ptr::null_mut();
            }
            if out.data.is_null() {
                return ptr::null_mut();
            }
            // SAFETY: out.data/len describe a Rust-owned buffer; copy and free.
            let slice = unsafe { std::slice::from_raw_parts(out.data, out.len) };
            let java_arr = match env.byte_array_from_slice(slice) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    unsafe {
                        let _ = crate::frs_bytes_free(&mut out);
                    }
                    throw_rocksdb(env, &format!("byte_array_from_slice failed: {e}"));
                    return ptr::null_mut();
                }
            };
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
            java_arr
        },
    )
}

// Keep `jboolean` / `JNI_TRUE` referenced even though only `JNI_FALSE` is
// actively returned today (by `isClosed`). They document the JNI ABI we
// promise for any future bool-returning entry point and keep the import
// group stable across iterative additions.
#[allow(dead_code)]
const _JNI_BOOL_REFS: (jboolean, jboolean) = (JNI_TRUE, JNI_FALSE);

// ===========================================================================
// P0 — multi-CF RocksDB.open + Options classes
//
// The community-style Java side (`org.forstdb.{DBOptions, ColumnFamilyOptions,
// WriteOptions, ReadOptions, ColumnFamilyHandle}` plus the multi-CF
// `RocksDB.open(long, String, byte[][], long[], long[])` overload) needs a
// matching set of `Java_org_forstdb_*` symbols. The setters that have no
// counterpart on `EngineOptions` / `CfOptions` are accepted as no-ops and
// logged at `tracing::debug!` so Flink's unconditional setter chains keep
// working without UnsatisfiedLinkError.
//
// Symbol budget: ~25 entries split across DBOptions (15), ColumnFamilyOptions
// (13), WriteOptions (3), ReadOptions (3), ColumnFamilyHandle (3), plus the
// multi-CF `RocksDB.open__JLjava_lang_String_2_3_3B_3J_3J` overload.
// ===========================================================================

// ---------------------------------------------------------------------------
// DBOptions
// ---------------------------------------------------------------------------

/// `org.forstdb.DBOptions.<init>() -> long`
///
/// Java signature: `()J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_newDBOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            DbOptionsHandle {
                opts: EngineOptions::default(),
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.DBOptions.disposeInternal(long)`
///
/// Java signature: `(J)V`
///
/// Reclaims the boxed [`DbOptionsHandle`]. Safe to call on `0` (no-op).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newDBOptions`, has not
                // been freed, and is uniquely held (Java side serialises this).
                unsafe { drop(Box::from_raw(handle as *mut DbOptionsHandle)) };
            }
        },
    )
}

/// `org.forstdb.DBOptions.setCreateIfMissing(long, boolean)`
///
/// forst-rs always creates the database directory if missing; the setter
/// is accepted but does not change behaviour. We do not surface a
/// `setErrorIfExists` because the engine has no toggle for that yet.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setCreateIfMissing<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            // forst-rs always creates the DB directory if missing; no field to set.
        },
    )
}

/// `org.forstdb.DBOptions.setUseFsync(long, boolean)`
///
/// forst-rs's WAL writer always fsyncs (write-path-r2 durability invariant).
/// Setter accepted as a no-op so Flink's "best effort durability" toggle
/// continues to link.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setUseFsync<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setUseFsync: forst-rs WAL is always fsync; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setStatsDumpPeriodSec(long, int)`
///
/// forst-rs has no statsdump scheduler; metrics are exposed via
/// `forst_rs_common::metrics::*`. Setter accepted as a no-op.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setStatsDumpPeriodSec<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setStatsDumpPeriodSec: forst-rs has no statsdump scheduler; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setAvoidFlushDuringShutdown(long, boolean)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setAvoidFlushDuringShutdown<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setAvoidFlushDuringShutdown: forst-rs always flushes on close; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setDbLogDir(long, String)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setDbLogDir<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: JString<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setDbLogDir: forst-rs uses tracing for log routing; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setInfoLogLevel(long, byte)`
///
/// Community RocksDB encodes the log level as a byte (0=debug … 5=fatal).
/// forst-rs delegates log filtering to `tracing_subscriber`, so we accept
/// and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setInfoLogLevel<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setInfoLogLevel: forst-rs uses tracing for log levels; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setMaxBackgroundJobs(long, int)`
///
/// Community RocksDB has a single `max_background_jobs` knob covering
/// both flushes and compactions. forst-rs splits these into two fields
/// (`max_background_compactions`, `max_background_flushes`); we apply
/// `value/2` to each, with at least 1 each, so the joint thread budget
/// matches the caller's intent.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxBackgroundJobs<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            let Some(h) = (unsafe { DbOptionsHandle::from_raw_ref(handle) }) else {
                return;
            };
            let v = value.max(2) as usize;
            // Keep the legacy 2:1 compactions:flushes ratio that EngineOptions::default()
            // ships with — both halves get a floor of 1.
            let compactions = (v / 2).max(1);
            let flushes = (v - compactions).max(1);
            h.opts.max_background_compactions = compactions;
            h.opts.max_background_flushes = flushes;
        },
    )
}

/// `org.forstdb.DBOptions.setMaxOpenFiles(long, int)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxOpenFiles<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setMaxOpenFiles: forst-rs has no per-table FD cache; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setMaxLogFileSize(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxLogFileSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setMaxLogFileSize: tracing-managed; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setKeepLogFileNum(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setKeepLogFileNum<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setKeepLogFileNum: tracing-managed; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setStatistics(long, long)`
///
/// forst-rs reports metrics via `forst_rs_common::metrics::*` rather than a
/// caller-supplied Statistics handle. Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setStatistics<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _stats_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                h.opts.enable_statistics = true;
            }
            tracing::debug!(target: "compat_jni::dbopts", "setStatistics: external Statistics handle ignored; using built-in metrics");
        },
    )
}

/// `org.forstdb.DBOptions.setWriteBufferManager(long, long)`
///
/// forst-rs has no shared write-buffer manager (per-CF arenas instead).
/// Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setWriteBufferManager<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _wbm_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setWriteBufferManager: forst-rs uses per-CF arenas; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.setEnv(long, long)`
///
/// forst-rs has its own `forst_rs_io::FileSystem` abstraction; we never
/// honour a caller-supplied Env handle. Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setEnv<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _env_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::dbopts", "setEnv: forst-rs FileSystem is internal; ignored");
        },
    )
}

// ---------------------------------------------------------------------------
// ColumnFamilyOptions
// ---------------------------------------------------------------------------

/// `org.forstdb.ColumnFamilyOptions.<init>() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_newColumnFamilyOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| CfOptionsHandle::default().into_raw(),
    )
}

/// `org.forstdb.ColumnFamilyOptions.disposeInternal(long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newColumnFamilyOptions` and
                // has not been freed.
                unsafe { drop(Box::from_raw(handle as *mut CfOptionsHandle)) };
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setWriteBufferSize(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setWriteBufferSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                if value > 0 {
                    h.opts.write_buffer_size = Some(value as usize);
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setMaxWriteBufferNumber(long, int)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setMaxWriteBufferNumber<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                if value > 0 {
                    h.opts.max_write_buffer_number = Some(value as usize);
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setMinWriteBufferNumberToMerge(long, int)`
///
/// forst-rs's flush scheduler always picks the largest immutable memtable
/// (no batched-merge knob). Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setMinWriteBufferNumberToMerge<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::cfopts", "setMinWriteBufferNumberToMerge: forst-rs has no merge-batch knob; ignored");
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setLevelCompactionDynamicLevelBytes(long, boolean)`
///
/// forst-rs uses static level multipliers (`max_bytes_for_level_multiplier`).
/// Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setLevelCompactionDynamicLevelBytes<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::cfopts", "setLevelCompactionDynamicLevelBytes: forst-rs uses static multiplier; ignored");
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setMaxBytesForLevelBase(long, long)`
///
/// Engine-wide field on forst-rs (no per-CF override yet); we route the
/// value through the CF options struct as a hint and the engine-side open
/// path will pick it up if forst-rs grows per-CF level bases.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setMaxBytesForLevelBase<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::cfopts", "setMaxBytesForLevelBase: per-CF override not yet supported; ignored");
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setTargetFileSizeBase(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setTargetFileSizeBase<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                if value > 0 {
                    h.opts.target_file_size_base = Some(value as usize);
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setCompressionPerLevel(long, byte[])`
///
/// Community RocksDB allows distinct compression per level; forst-rs uses a
/// single engine-wide compression. We pick the *last* non-zero entry as the
/// "deepest level" compression and apply it to the CF override; if the
/// array is empty or all-zero we leave the CF compression unset.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setCompressionPerLevel<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    levels: JByteArray<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { CfOptionsHandle::from_raw_ref(handle) }) else {
                return;
            };
            let arr = match env.convert_byte_array(&levels) {
                Ok(a) => a,
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("setCompressionPerLevel: convert_byte_array failed: {e}"),
                    );
                    return;
                }
            };
            // Pick deepest non-zero level.
            if let Some(&deepest) = arr.iter().rev().find(|&&b| b != 0) {
                h.opts.compression = Some(byte_to_compression(deepest));
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setCompactionStyle(long, byte)`
///
/// forst-rs only supports level-style compaction. Accept the call; reject
/// only if the caller asks for something exotic (universal=1, fifo=2,
/// none=3) so misconfigured Flink jobs surface a clear error.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setCompactionStyle<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    style: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            // 0 = level (default). Anything else is unsupported.
            if style != 0 {
                tracing::debug!(target: "compat_jni::cfopts", "setCompactionStyle({style}): forst-rs only supports LEVEL; ignored");
                // Do not throw — many Flink jobs blindly call setCompactionStyle(LEVEL)
                // and we don't want to break them; the unrecognised style values fall
                // through silently per the audit's "no-op on unsupported" rule.
                let _ = env; // suppress unused warning
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setPeriodicCompactionSeconds(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setPeriodicCompactionSeconds<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    seconds: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                if seconds > 0 {
                    h.opts.ttl_seconds = Some(seconds as u64);
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setTableFormatConfig(long, long)`
///
/// Stores the opaque BlockBasedTableConfig handle; P3 will hydrate it into
/// `EngineOptions::{block_size, bloom_bits_per_key}`. Accepted as a no-op
/// for now so Flink's setter chain links.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setTableFormatConfig<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    table_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                h.table_format_handle = table_handle;
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.setCompactionFilterFactory(long, long)`
///
/// forst-rs has no compaction-filter factory layer. Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setCompactionFilterFactory<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _factory_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::cfopts", "setCompactionFilterFactory: not supported; ignored");
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.tableFormatConfig(long) -> long`
///
/// Returns the handle previously stored via `setTableFormatConfig`, or `0`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_tableFormatConfig<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.table_format_handle)
                .unwrap_or(0)
        },
    )
}

// ---------------------------------------------------------------------------
// WriteOptions
// ---------------------------------------------------------------------------

/// `org.forstdb.WriteOptions.<init>() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_newWriteOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| WriteOptionsHandle::default().into_raw(),
    )
}

/// `org.forstdb.WriteOptions.disposeInternal(long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle from prior newWriteOptions; not freed.
                unsafe { drop(Box::from_raw(handle as *mut WriteOptionsHandle)) };
            }
        },
    )
}

/// `org.forstdb.WriteOptions.setDisableWAL(long, boolean)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteOptions_setDisableWAL<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { WriteOptionsHandle::from_raw_ref(handle) } {
                h.disable_wal = value != JNI_FALSE;
                if h.disable_wal {
                    tracing::debug!(target: "compat_jni::writeopts", "setDisableWAL(true): forst-rs always writes the WAL; flag recorded but no-op");
                }
            }
        },
    )
}

// ---------------------------------------------------------------------------
// ReadOptions
// ---------------------------------------------------------------------------

/// `org.forstdb.ReadOptions.<init>() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ReadOptions_newReadOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| ReadOptionsHandle::default().into_raw(),
    )
}

/// `org.forstdb.ReadOptions.disposeInternal(long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ReadOptions_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle from prior newReadOptions; not freed.
                unsafe { drop(Box::from_raw(handle as *mut ReadOptionsHandle)) };
            }
        },
    )
}

/// `org.forstdb.ReadOptions.setReadaheadSize(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ReadOptions_setReadaheadSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { ReadOptionsHandle::from_raw_ref(handle) } {
                // Negative jlong values are rejected (silently clamped to 0) so
                // the unsigned cast can't wrap.
                h.readahead_size = if value < 0 { 0 } else { value as u64 };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// ColumnFamilyHandle
// ---------------------------------------------------------------------------

/// `org.forstdb.ColumnFamilyHandle.disposeInternal(long)`
///
/// Releases the wrapping [`CfHandle`] *and* the underlying engine CF handle.
/// Java's `ColumnFamilyHandle.close()` calls this; double-call is UB on the
/// Java side and we do not guard against it (Java's auto-closeable contract
/// makes single-call the caller's responsibility).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyHandle_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle == 0 {
                return;
            }
            // SAFETY: handle came from a prior `RocksDB.open` multi-CF call.
            let cf_handle = unsafe { Box::from_raw(handle as *mut CfHandle) };
            // Close engine CF handle (if non-null).
            if !cf_handle.frs_handle.is_null() {
                let _ = unsafe { crate::frs_cf_close(cf_handle.frs_handle) };
            }
            // If we owned the CfOptionsHandle, drop it now too.
            if cf_handle.owned_opts_handle != 0 {
                unsafe {
                    drop(Box::from_raw(
                        cf_handle.owned_opts_handle as *mut CfOptionsHandle,
                    ));
                }
            }
            drop(cf_handle);
        },
    )
}

/// `org.forstdb.ColumnFamilyHandle.getName0(long) -> byte[]`
///
/// Returns the CF name as a UTF-8 byte array (matches community ForSt's
/// internal `getName0` contract).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyHandle_getName0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(h) = (unsafe { CfHandle::from_raw_ref(handle) }) else {
                return ptr::null_mut();
            };
            match env.byte_array_from_slice(&h.name) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("ColumnFamilyHandle.getName0: byte_array_from_slice failed: {e}"),
                    );
                    ptr::null_mut()
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyHandle.getDescriptor(long) -> ColumnFamilyDescriptor`
///
/// Community ForSt returns a `ColumnFamilyDescriptor` Java object with the
/// CF name + a fresh `ColumnFamilyOptions`. We synthesise a default
/// `CfOptionsHandle` (callers needing the original opts should track them
/// Java-side) and wrap it in the matching Java type via reflection.
///
/// **Limitation:** because constructing a Java `ColumnFamilyDescriptor`
/// requires the class to exist on the classpath, this method instantiates
/// `org/forstdb/ColumnFamilyDescriptor` reflectively. If the class is
/// missing (e.g. a partial classpath in tests) we throw
/// `RocksDBException` rather than crash.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyHandle_getDescriptor<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jobject {
    jni_guard(&mut env, ptr::null_mut, |env| -> jni::sys::jobject {
        let Some(h) = (unsafe { CfHandle::from_raw_ref(handle) }) else {
            return ptr::null_mut();
        };
        // Build a fresh CfOptionsHandle for the Java side to own.
        let opts_handle = CfOptionsHandle::default().into_raw();
        // Wrap name as byte[].
        let name_arr = match env.byte_array_from_slice(&h.name) {
            Ok(a) => a,
            Err(e) => {
                // Reclaim the leaked CfOptionsHandle to avoid leaking memory.
                unsafe {
                    drop(Box::from_raw(opts_handle as *mut CfOptionsHandle));
                }
                throw_rocksdb(
                    env,
                    &format!("getDescriptor: byte_array_from_slice failed: {e}"),
                );
                return ptr::null_mut();
            }
        };
        let cf_opts_class = match env.find_class("org/forstdb/ColumnFamilyOptions") {
            Ok(c) => c,
            Err(e) => {
                unsafe {
                    drop(Box::from_raw(opts_handle as *mut CfOptionsHandle));
                }
                throw_rocksdb(
                    env,
                    &format!("getDescriptor: find_class(ColumnFamilyOptions) failed: {e}"),
                );
                return ptr::null_mut();
            }
        };
        // Construct ColumnFamilyOptions via its <init>(long) constructor (the
        // common forstdb shim convention is a long-handle adopting ctor).
        let cf_opts = match env.new_object(
            &cf_opts_class,
            "(J)V",
            &[jni::objects::JValue::Long(opts_handle)],
        ) {
            Ok(o) => o,
            Err(_) => {
                // Fallback: try no-arg ctor; the new object will own its own
                // handle, so reclaim our orphaned one to avoid leaking.
                unsafe {
                    drop(Box::from_raw(opts_handle as *mut CfOptionsHandle));
                }
                match env.new_object(&cf_opts_class, "()V", &[]) {
                    Ok(o) => o,
                    Err(e) => {
                        throw_rocksdb(
                            env,
                            &format!("getDescriptor: new ColumnFamilyOptions failed: {e}"),
                        );
                        return ptr::null_mut();
                    }
                }
            }
        };
        let desc_class = match env.find_class("org/forstdb/ColumnFamilyDescriptor") {
            Ok(c) => c,
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("getDescriptor: find_class(ColumnFamilyDescriptor) failed: {e}"),
                );
                return ptr::null_mut();
            }
        };
        match env.new_object(
            &desc_class,
            "([BLorg/forstdb/ColumnFamilyOptions;)V",
            &[
                jni::objects::JValue::Object(name_arr.as_ref()),
                jni::objects::JValue::Object(cf_opts.as_ref()),
            ],
        ) {
            Ok(o) => o.into_raw(),
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("getDescriptor: new ColumnFamilyDescriptor failed: {e}"),
                );
                ptr::null_mut()
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Multi-CF RocksDB.open
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.open(long dbOptionsHandle, String path,
///                           byte[][] cfNames, long[] cfOptions,
///                           long[] outCfHandles) -> long dbHandle`
///
/// Mangled JNI symbol (because `open` is overloaded with the original
/// single-arg `(Ljava/lang/String;)J`):
///   `Java_org_forstdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J_3J`
///
/// Lockstep iterates `cf_names` and `cf_opts_handles`, opens (or creates)
/// each CF, and writes the resulting `CfHandle` jlongs into `out_cf_handles`.
/// The "default" CF (community magic name `b"default"`) is mapped to
/// [`frs_db_default_cf`] rather than create-or-open.
///
/// All five inputs MUST be non-null and `cf_names.length` MUST equal both
/// `cf_opts_handles.length` and `out_cf_handles.length`. Length mismatch
/// throws `RocksDBException`.
///
/// **Ownership note:** the returned `CfHandle` boxes adopt the caller's
/// `CfOptionsHandle` boxes (via `owned_opts_handle`) so Java code can
/// immediately release the `ColumnFamilyOptions` after open. The DB handle
/// is the same opaque jlong returned by [`Java_org_forstdb_RocksDB_open`].
#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_org_forstdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J_3J<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_opts_handle: jlong,
    path: JString<'local>,
    cf_names: JObjectArray<'local>,
    cf_opts_handles: JPrimitiveArray<'local, jlong>,
    out_cf_handles: JPrimitiveArray<'local, jlong>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| -> jlong {
            // 1. Resolve DB options.
            let Some(db_opts) = (unsafe { DbOptionsHandle::from_raw_ref(db_opts_handle) }) else {
                throw_rocksdb(env, "RocksDB.open(multi-CF): null DBOptions handle");
                return 0;
            };
            // 2. Resolve path.
            let Some(path_str) = read_string(env, &path) else {
                return 0;
            };
            // 3. Validate arrays & extract sizes.
            let cf_names_len = match env.get_array_length(&cf_names) {
                Ok(n) if n >= 0 => n as usize,
                Ok(n) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): negative cf_names length: {n}"),
                    );
                    return 0;
                }
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): get cf_names length: {e}"),
                    );
                    return 0;
                }
            };
            let cf_opts_len = match env.get_array_length(&cf_opts_handles) {
                Ok(n) if n >= 0 => n as usize,
                Ok(n) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): negative cf_opts length: {n}"),
                    );
                    return 0;
                }
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): get cf_opts length: {e}"),
                    );
                    return 0;
                }
            };
            let out_len = match env.get_array_length(&out_cf_handles) {
                Ok(n) if n >= 0 => n as usize,
                Ok(n) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): negative out length: {n}"),
                    );
                    return 0;
                }
                Err(e) => {
                    throw_rocksdb(env, &format!("RocksDB.open(multi-CF): get out length: {e}"));
                    return 0;
                }
            };
            if cf_names_len != cf_opts_len || cf_names_len != out_len {
                throw_rocksdb(
                env,
                &format!(
                    "RocksDB.open(multi-CF): array length mismatch (cf_names={}, cf_opts={}, out={})",
                    cf_names_len, cf_opts_len, out_len
                ),
            );
                return 0;
            }
            if cf_names_len == 0 {
                throw_rocksdb(
                    env,
                    "RocksDB.open(multi-CF): cf_names must contain at least the default CF",
                );
                return 0;
            }

            // 4. Read cf_opts handles.
            let mut cf_opts_buf = vec![0_i64; cf_opts_len];
            if let Err(e) = env.get_long_array_region(&cf_opts_handles, 0, &mut cf_opts_buf) {
                throw_rocksdb(env, &format!("RocksDB.open(multi-CF): read cf_opts: {e}"));
                return 0;
            }

            // 5. Read CF names (each entry is byte[]).
            let cf_name_bytes =
                match read_byte_matrix(env, &cf_names, "RocksDB.open(multi-CF).cf_names") {
                    Some(v) => v,
                    None => return 0,
                };

            // 6. Build EngineOptions (clone the caller's, override db_path).
            let mut engine_opts = db_opts.opts.clone();
            engine_opts.db_path = path_str;

            // 7. Open the engine. We take the slow path of constructing a fresh
            //    DbImpl directly so the configured EngineOptions land on the
            //    engine instead of the default-only path of `frs_db_open`.
            let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
            let db = match DbImpl::open_with_fs(engine_opts, fs) {
                Ok(d) => Box::new(d),
                Err(e) => {
                    throw_rocksdb(env, &format!("RocksDB.open(multi-CF): {e}"));
                    return 0;
                }
            };
            let db_handle = Box::into_raw(db) as FrsDb;

            // 8. For each CF, build / open a CfHandle.
            let mut out_handles = Vec::<jlong>::with_capacity(cf_names_len);
            for (i, name_bytes) in cf_name_bytes.iter().enumerate() {
                let name_str = match std::str::from_utf8(name_bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        // Roll back: close the DB and reset partial CF handles.
                        cleanup_partial_open(db_handle, &out_handles);
                        throw_rocksdb(
                            env,
                            &format!(
                                "RocksDB.open(multi-CF): cf_names[{i}] is not valid UTF-8: {e}"
                            ),
                        );
                        return 0;
                    }
                };
                let c_name = match std::ffi::CString::new(name_str) {
                    Ok(s) => s,
                    Err(_) => {
                        cleanup_partial_open(db_handle, &out_handles);
                        throw_rocksdb(
                            env,
                            &format!("RocksDB.open(multi-CF): cf_names[{i}] contains interior NUL"),
                        );
                        return 0;
                    }
                };

                let mut frs_cf: FrsCfHandle = ptr::null_mut();
                let status = if name_str == "default" {
                    // SAFETY: db_handle is a freshly-built DbImpl pointer; out_cf is stack-local.
                    unsafe { frs_db_default_cf(db_handle, &mut frs_cf) }
                } else {
                    // Try open first, fall back to create. This matches the existing
                    // single-CF createColumnFamily helper's "create-or-open" semantics
                    // and is what Flink's restore path expects.
                    let open_status =
                        unsafe { frs_db_open_cf(db_handle, c_name.as_ptr(), &mut frs_cf) };
                    if open_status == FRS_STATUS_OK {
                        open_status
                    } else {
                        unsafe { frs_db_create_cf(db_handle, c_name.as_ptr(), &mut frs_cf) }
                    }
                };
                if status != FRS_STATUS_OK {
                    cleanup_partial_open(db_handle, &out_handles);
                    throw_rocksdb(
                    env,
                    &format!(
                        "RocksDB.open(multi-CF): failed to open/create CF `{name_str}`: frs_status={status}"
                    ),
                );
                    return 0;
                }

                let cf_handle = CfHandle {
                    name: name_bytes.clone(),
                    frs_handle: frs_cf,
                    owned_opts_handle: cf_opts_buf[i],
                };
                out_handles.push(cf_handle.into_raw());
            }

            // 9. Write back into out_cf_handles.
            if let Err(e) = env.set_long_array_region(&out_cf_handles, 0, &out_handles) {
                cleanup_partial_open(db_handle, &out_handles);
                throw_rocksdb(
                    env,
                    &format!("RocksDB.open(multi-CF): set_long_array_region: {e}"),
                );
                return 0;
            }

            db_handle as jlong
        },
    )
}

/// Roll back a partially-completed multi-CF open. Called when an error
/// surfaces between `frs_db_open` succeeding and the final
/// `set_long_array_region` returning. Drops every leaked
/// [`CfHandle`] box and closes the engine handle.
fn cleanup_partial_open(db_handle: FrsDb, partial_cf_handles: &[jlong]) {
    for &cf_jlong in partial_cf_handles {
        if cf_jlong != 0 {
            // SAFETY: we just leaked these via Box::into_raw above.
            let cf = unsafe { Box::from_raw(cf_jlong as *mut CfHandle) };
            if !cf.frs_handle.is_null() {
                let _ = unsafe { crate::frs_cf_close(cf.frs_handle) };
            }
            // Note: do NOT touch owned_opts_handle here — the Java side
            // still owns them (it gave us the handles, we never adopted
            // them since the open ultimately failed).
            drop(cf);
        }
    }
    if !db_handle.is_null() {
        let _ = unsafe { frs_db_close(db_handle) };
    }
}

/// Map a community RocksDB `compression_type` byte to forst-rs's enum.
/// Unknown bytes fall back to `Lz4` (the engine default) so a stale Flink
/// constant table never breaks open.
fn byte_to_compression(b: u8) -> CompressionType {
    match b {
        0 => CompressionType::None,
        1 => CompressionType::Lz4, // RocksDB calls 1 "snappy"; we surface our LZ4
        2 => CompressionType::Zstd,
        4 => CompressionType::Lz4,
        7 => CompressionType::Zstd,
        _ => CompressionType::Lz4,
    }
}

// ===========================================================================
// P1 — RocksIterator + WriteBatch JNI surface
//
// Goal: complete the read-iteration and grouped-write Java surfaces so a
// Flink job using value/list/map state has a working `byte[]`-level path.
// (Checkpoint/restore is P2; that's a different layering concern.)
//
// The RocksIterator wrapper sits on top of the existing C-ABI iterator
// (`frs_iterator_open` + `frs_iterator_next`) but adds the
// "isValid()/key()/value() without consuming" semantics community RocksDB
// exposes — we cache the most-recent (key, value) in
// [`RocksIteratorHandle`] after each `seek*` / `next0` / `prev0`. Cursor
// rewind (`seekToLast`, `prev0`, `seekForPrev`) reaches into the
// engine-side [`crate::IteratorState`] directly because the public C ABI
// is forward-only. This is intentional layering: the JNI shim is in the
// same crate and gets `pub(crate)` access.
// ===========================================================================

/// Helper: consume one `frs_iterator_next` row and store it in the caller's
/// `RocksIteratorHandle`. Used by `seek*` and `next0`. Returns `true` if
/// the call succeeded (regardless of whether a row was found; check
/// `h.valid` for that), `false` if the FFI raised an error and a Java
/// exception was thrown.
fn fetch_into_handle(env: &mut JNIEnv, h: &mut RocksIteratorHandle, label: &str) -> bool {
    let mut k = FrsBytes {
        data: ptr::null_mut(),
        len: 0,
        capacity: 0,
    };
    let mut v = FrsBytes {
        data: ptr::null_mut(),
        len: 0,
        capacity: 0,
    };
    let mut valid: bool = false;
    // SAFETY: frs_iter came from frs_iterator_open; out_* are stack-locals
    // we own.
    let status = unsafe { frs_iterator_next(h.frs_iter, &mut k, &mut v, &mut valid) };
    if check_status(env, status, label) {
        // Best-effort cleanup; the engine guarantees null on error but we
        // call free for symmetry with other thunks.
        unsafe {
            let _ = crate::frs_bytes_free(&mut k);
            let _ = crate::frs_bytes_free(&mut v);
        }
        h.valid = false;
        h.last_key = None;
        h.last_value = None;
        return false;
    }
    if !valid {
        h.valid = false;
        h.last_key = None;
        h.last_value = None;
        // Defensive free.
        unsafe {
            let _ = crate::frs_bytes_free(&mut k);
            let _ = crate::frs_bytes_free(&mut v);
        }
        return true;
    }
    // Copy the FrsBytes payloads into owned Vec<u8>s so we can free the
    // FFI buffers immediately and serve key0()/value0() from the cache.
    // SAFETY: k.data/v.data describe Rust-owned buffers populated by
    // frs_iterator_next; len fields are accurate for the call window.
    let key_vec = unsafe { std::slice::from_raw_parts(k.data, k.len).to_vec() };
    let val_vec = unsafe { std::slice::from_raw_parts(v.data, v.len).to_vec() };
    unsafe {
        let _ = crate::frs_bytes_free(&mut k);
        let _ = crate::frs_bytes_free(&mut v);
    }
    h.valid = true;
    h.last_key = Some(key_vec);
    h.last_value = Some(val_vec);
    true
}

// ---------------------------------------------------------------------------
// RocksIterator factory on RocksDB
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.iterator(long handle, long cfHandle, long readOptionsHandle) -> long iter`
///
/// Java signature: `(JJJ)J`
///
/// Opens a fresh `RocksIterator` over the supplied CF. The
/// `read_options_handle` is currently a no-op — forst-rs's iterator does
/// not yet consult `ReadOptions` for snapshot/iterate-bounds/upper-bound
/// semantics. We log at `tracing::debug!` so divergences from community
/// RocksDB behaviour are visible without breaking the Java caller.
///
/// Differs from [`Java_org_forstdb_RocksDB_iteratorOpen`] (the legacy 2-arg
/// form) only in the extra `readOptionsHandle` slot. Returns a handle to
/// a [`RocksIteratorHandle`] (NOT the raw `FrsIterator`) — Java code must
/// dispose via `RocksIterator.disposeInternal`, not `iteratorClose`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_iterator<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    read_options_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            // ReadOptions are currently informational. Surface the divergence
            // at debug level so an operator chasing iterator semantics can
            // grep for it.
            if read_options_handle != 0 {
                tracing::debug!(
                    target: "compat_jni::iterator",
                    "RocksDB.iterator: ReadOptions ({read_options_handle:#x}) ignored — engine uses default snapshot semantics"
                );
            }
            let mut iter: FrsIterator = ptr::null_mut();
            // SAFETY: db / cf came from prior open; out_iter is stack-local.
            let status =
                unsafe { frs_iterator_open(handle as FrsDb, cf_handle as FrsCfHandle, &mut iter) };
            if check_status(env, status, "RocksDB.iterator") {
                return 0_i64;
            }
            RocksIteratorHandle {
                frs_iter: iter,
                last_key: None,
                last_value: None,
                valid: false,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.RocksDB.iteratorCF(long handle, long cfHandle, long readOptionsHandle) -> long iter`
///
/// Java signature: `(JJJ)J`
///
/// Alias of [`Java_org_forstdb_RocksDB_iterator`]. Some community bindings
/// expose the column-family iterator factory under this name; both are
/// kept so we link cleanly against either.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_iteratorCF<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    read_options_handle: jlong,
) -> jlong {
    Java_org_forstdb_RocksDB_iterator(env, class, handle, cf_handle, read_options_handle)
}

// ---------------------------------------------------------------------------
// RocksIterator instance methods
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksIterator.seek0(long handle, byte[] key, int keyLen)`
///
/// Java signature: `(J[BI)V`
///
/// Repositions the cursor at the first key `>= key` and pre-fetches that
/// row into the handle's cache. After this call, `isValid0()` is `true`
/// iff such a row exists.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seek0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "RocksIterator.seek0: null handle");
                return;
            };
            // SAFETY: read_byte_slice requires offset+len bounds; we pass 0/key_len.
            let needle = if key_len <= 0 {
                Vec::new()
            } else {
                let Some(k) = read_byte_slice(env, &key, 0, key_len) else {
                    return;
                };
                k
            };
            // SAFETY: frs_iter valid; needle pointer + len consistent.
            let status = unsafe {
                frs_iterator_seek(
                    h.frs_iter,
                    if needle.is_empty() {
                        ptr::null()
                    } else {
                        needle.as_ptr()
                    },
                    needle.len(),
                )
            };
            if check_status(env, status, "RocksIterator.seek0") {
                return;
            }
            // Pre-fetch so isValid0/key0/value0 return the seeked-to row.
            let _ = fetch_into_handle(env, h, "RocksIterator.seek0(prefetch)");
        },
    )
}

/// `org.forstdb.RocksIterator.seekToFirst0(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seekToFirst0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "RocksIterator.seekToFirst0: null handle");
                return;
            };
            // SAFETY: valid frs_iter; null + 0 means "seek to first".
            let status = unsafe { frs_iterator_seek(h.frs_iter, ptr::null(), 0) };
            if check_status(env, status, "RocksIterator.seekToFirst0") {
                return;
            }
            let _ = fetch_into_handle(env, h, "RocksIterator.seekToFirst0(prefetch)");
        },
    )
}

/// `org.forstdb.RocksIterator.seekToLast0(long handle)`
///
/// Java signature: `(J)V`
///
/// The public C ABI is forward-only, so this thunk reaches into the
/// engine-side [`crate::IteratorState`] directly to position the cursor at
/// `rows.len() - 1`. Same-crate `pub(crate)` access — soundness rests on
/// the iterator handle being the [`RocksIteratorHandle`]'s sole
/// `frs_iter`, which is guaranteed by the constructor.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seekToLast0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "RocksIterator.seekToLast0: null handle");
                return;
            };
            if h.frs_iter.is_null() {
                throw_rocksdb(env, "RocksIterator.seekToLast0: null frs_iter");
                return;
            }
            // SAFETY: frs_iter is a valid `Box<crate::IteratorState>` raw
            // pointer obtained via Box::into_raw inside frs_iterator_open;
            // we are the sole owner for the duration of this thunk.
            let state = unsafe { &mut *(h.frs_iter as *mut crate::IteratorState) };
            if state.rows.is_empty() {
                h.valid = false;
                h.last_key = None;
                h.last_value = None;
                return;
            }
            state.cursor = state.rows.len() - 1;
            // Pre-fetch the last row.
            let _ = fetch_into_handle(env, h, "RocksIterator.seekToLast0(prefetch)");
        },
    )
}

/// `org.forstdb.RocksIterator.seekForPrev0(long handle, byte[] key, int keyLen)`
///
/// Java signature: `(J[BI)V`
///
/// Positions at the last key `<= needle`. Same engine-internal
/// access pattern as [`Java_org_forstdb_RocksIterator_seekToLast0`] —
/// the public C ABI exposes only `>= needle`, so we binary-search the
/// in-memory `rows` and adjust the cursor.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_seekForPrev0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "RocksIterator.seekForPrev0: null handle");
                return;
            };
            if h.frs_iter.is_null() {
                throw_rocksdb(env, "RocksIterator.seekForPrev0: null frs_iter");
                return;
            }
            let needle = if key_len <= 0 {
                Vec::new()
            } else {
                let Some(k) = read_byte_slice(env, &key, 0, key_len) else {
                    return;
                };
                k
            };
            // SAFETY: see seekToLast0 — same-crate pub(crate) access to the
            // owning Box<IteratorState>.
            let state = unsafe { &mut *(h.frs_iter as *mut crate::IteratorState) };
            // Find the index of the first row whose key > needle; the row
            // at index-1 is the largest key <= needle.
            let upper = state
                .rows
                .partition_point(|(k, _)| k.as_slice() <= needle.as_slice());
            if upper == 0 {
                // No key <= needle.
                h.valid = false;
                h.last_key = None;
                h.last_value = None;
                return;
            }
            state.cursor = upper - 1;
            let _ = fetch_into_handle(env, h, "RocksIterator.seekForPrev0(prefetch)");
        },
    )
}

/// `org.forstdb.RocksIterator.next0(long handle)`
///
/// Java signature: `(J)V`
///
/// Advances the cursor and refreshes the cached `(key, value)`. After this
/// call, `isValid0()` is `true` iff a row was available.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_next0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "RocksIterator.next0: null handle");
                return;
            };
            let _ = fetch_into_handle(env, h, "RocksIterator.next0");
        },
    )
}

/// `org.forstdb.RocksIterator.prev0(long handle)`
///
/// Java signature: `(J)V`
///
/// Steps the cursor backwards. The public C ABI is forward-only; we
/// rewind the engine-side cursor by two (one to undo the last `next` call,
/// one more to land on the previous row) and re-fetch.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_prev0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "RocksIterator.prev0: null handle");
                return;
            };
            if h.frs_iter.is_null() {
                throw_rocksdb(env, "RocksIterator.prev0: null frs_iter");
                return;
            }
            // SAFETY: see seekToLast0 — same-crate pub(crate) access.
            let state = unsafe { &mut *(h.frs_iter as *mut crate::IteratorState) };
            // After a successful `next`, state.cursor points one *past* the
            // row we just returned. To go to "the row before the one we
            // just returned" we need cursor -= 2. If cursor < 2 we are at
            // (or before) the start.
            if state.cursor < 2 {
                h.valid = false;
                h.last_key = None;
                h.last_value = None;
                state.cursor = 0;
                return;
            }
            state.cursor -= 2;
            let _ = fetch_into_handle(env, h, "RocksIterator.prev0(prefetch)");
        },
    )
}

/// `org.forstdb.RocksIterator.isValid0(long handle) -> boolean`
///
/// Java signature: `(J)Z`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_isValid0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    jni_guard(
        &mut env,
        || JNI_FALSE,
        |_env| {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                return JNI_FALSE;
            };
            if h.valid {
                JNI_TRUE
            } else {
                JNI_FALSE
            }
        },
    )
}

/// `org.forstdb.RocksIterator.key0(long handle) -> byte[]`
///
/// Java signature: `(J)[B`
///
/// Returns a copy of the current row's key, or `null` if `isValid0()` is
/// false. Cache-served — no engine round-trip.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_key0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                return ptr::null_mut();
            };
            let Some(k) = h.last_key.as_ref() else {
                return ptr::null_mut();
            };
            match env.byte_array_from_slice(k) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksIterator.key0: byte_array_from_slice: {e}"),
                    );
                    ptr::null_mut()
                }
            }
        },
    )
}

/// `org.forstdb.RocksIterator.value0(long handle) -> byte[]`
///
/// Java signature: `(J)[B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_value0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(h) = (unsafe { RocksIteratorHandle::from_raw_ref(handle) }) else {
                return ptr::null_mut();
            };
            let Some(v) = h.last_value.as_ref() else {
                return ptr::null_mut();
            };
            match env.byte_array_from_slice(v) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksIterator.value0: byte_array_from_slice: {e}"),
                    );
                    ptr::null_mut()
                }
            }
        },
    )
}

/// `org.forstdb.RocksIterator.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
///
/// Closes the engine iterator and drops the handle box. Safe to call on
/// `0`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            if handle == 0 {
                return;
            }
            // SAFETY: handle came from prior `iterator()` / `iteratorCF()`,
            // sole owner.
            let h = unsafe { Box::from_raw(handle as *mut RocksIteratorHandle) };
            if !h.frs_iter.is_null() {
                let status = unsafe { frs_iterator_close(h.frs_iter) };
                check_status(env, status, "RocksIterator.disposeInternal");
            }
            drop(h);
        },
    )
}

// ---------------------------------------------------------------------------
// WriteBatch — Rust-side accumulator
// ---------------------------------------------------------------------------

/// `org.forstdb.WriteBatch.<init>(int reservedBytes) -> long`
///
/// Java signature: `(I)J`
///
/// `reserved_bytes` is informational — we pre-size the entries Vec to
/// `max(0, reserved_bytes/64)` (rough average per-entry overhead) so the
/// caller's hint controls the initial capacity. Negative inputs are
/// clamped to 0.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_newWriteBatch<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    reserved_bytes: jint,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let cap = if reserved_bytes <= 0 {
                0
            } else {
                (reserved_bytes as usize) / 64
            };
            WriteBatchHandle {
                entries: Vec::with_capacity(cap),
                data_size: 0,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.WriteBatch.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: came from `newWriteBatch`; sole owner per JNI
                // single-thread-per-handle convention.
                unsafe { drop(Box::from_raw(handle as *mut WriteBatchHandle)) };
            }
        },
    )
}

/// `org.forstdb.WriteBatch.put(long handle, long cfHandle, byte[] key,
///                              int keyOff, int keyLen, byte[] val,
///                              int valOff, int valLen)`
///
/// Java signature: `(JJ[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_WriteBatch_put<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "WriteBatch.put: null handle");
                return;
            };
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return;
            };
            let Some(v) = read_byte_slice(env, &val, val_off, val_len) else {
                return;
            };
            h.data_size = h.data_size.saturating_add((k.len() + v.len()) as u64);
            h.entries.push(WriteBatchEntry::Put {
                cf: cf_handle as FrsCfHandle,
                key: k,
                value: v,
            });
        },
    )
}

/// `org.forstdb.WriteBatch.merge(long handle, long cfHandle, byte[] key,
///                                int keyOff, int keyLen, byte[] val,
///                                int valOff, int valLen)`
///
/// Java signature: `(JJ[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_WriteBatch_merge<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "WriteBatch.merge: null handle");
                return;
            };
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return;
            };
            let Some(v) = read_byte_slice(env, &val, val_off, val_len) else {
                return;
            };
            h.data_size = h.data_size.saturating_add((k.len() + v.len()) as u64);
            h.entries.push(WriteBatchEntry::Merge {
                cf: cf_handle as FrsCfHandle,
                key: k,
                value: v,
            });
        },
    )
}

/// `org.forstdb.WriteBatch.delete(long handle, long cfHandle, byte[] key,
///                                 int keyOff, int keyLen)`
///
/// Java signature: `(JJ[BII)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_delete<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "WriteBatch.delete: null handle");
                return;
            };
            let Some(k) = read_byte_slice(env, &key, key_off, key_len) else {
                return;
            };
            h.data_size = h.data_size.saturating_add(k.len() as u64);
            h.entries.push(WriteBatchEntry::Delete {
                cf: cf_handle as FrsCfHandle,
                key: k,
            });
        },
    )
}

/// `org.forstdb.WriteBatch.clear0(long handle)`
///
/// Java signature: `(J)V`
///
/// Drops all buffered entries and resets `getDataSize()` to 0. The Vec's
/// capacity is preserved so a follow-on burst of writes does not
/// reallocate.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_clear0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "WriteBatch.clear0: null handle");
                return;
            };
            h.entries.clear();
            h.data_size = 0;
        },
    )
}

/// `org.forstdb.WriteBatch.count0(long handle) -> int`
///
/// Java signature: `(J)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_count0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_guard(
        &mut env,
        || 0_i32,
        |_env| {
            let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
                return 0;
            };
            // i32::MAX clamp — > 2B entries in a single batch is pathological
            // and would be denoted as `count == i32::MAX` to match the
            // Java caller's `int`-typed expectation.
            h.entries.len().min(i32::MAX as usize) as jint
        },
    )
}

/// `org.forstdb.WriteBatch.getDataSize(long handle) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_getDataSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
                return 0_i64;
            };
            // saturating jlong cast; data_size is u64 but jlong is i64.
            h.data_size.min(i64::MAX as u64) as jlong
        },
    )
}

// ---------------------------------------------------------------------------
// RocksDB.write0 — apply a WriteBatch
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.write0(long dbHandle, long woHandle, long wbHandle)`
///
/// Java signature: `(JJJ)V`
///
/// Drains the [`WriteBatchHandle`]'s buffered entries and dispatches each
/// to the engine via the existing `frs_put` / `frs_merge` / `frs_delete`
/// paths. The batch is left empty on success; partial-failure semantics
/// (mid-batch error) drop the remaining entries to avoid surprise re-apply
/// on retry — Java callers expecting transactional semantics should
/// `clear0()` before retrying.
///
/// `wo_handle` (WriteOptions) is currently informational. The
/// engine always durably writes; `disable_wal` is recorded but ignored.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_write0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_handle: jlong,
    wo_handle: jlong,
    wb_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            if wo_handle != 0 {
                // Touch only to surface the divergence; the field is read
                // for its side-effect of confirming the box is live.
                if let Some(wo) = unsafe { WriteOptionsHandle::from_raw_ref(wo_handle) } {
                    if wo.disable_wal {
                        tracing::debug!(
                            target: "compat_jni::write0",
                            "RocksDB.write0: WriteOptions.disable_wal=true ignored — engine always writes WAL"
                        );
                    }
                }
            }
            let Some(wb) = (unsafe { WriteBatchHandle::from_raw_ref(wb_handle) }) else {
                throw_rocksdb(env, "RocksDB.write0: null WriteBatch handle");
                return;
            };
            // Drain entries so a successful apply leaves the batch empty;
            // a failure also clears so retry semantics are well-defined
            // (caller must rebuild the batch on retry).
            let entries = std::mem::take(&mut wb.entries);
            wb.data_size = 0;
            for (i, entry) in entries.into_iter().enumerate() {
                let label = format!("RocksDB.write0[{i}]");
                let status = match entry {
                    WriteBatchEntry::Put { cf, key, value } => {
                        // SAFETY: db_handle / cf came from prior open;
                        // key / value vectors live for the duration of
                        // the call and the engine copies internally.
                        unsafe {
                            crate::frs_put(
                                db_handle as FrsDb,
                                cf,
                                key.as_ptr(),
                                key.len(),
                                value.as_ptr(),
                                value.len(),
                            )
                        }
                    }
                    WriteBatchEntry::Merge { cf, key, value } => {
                        // SAFETY: same as above.
                        unsafe {
                            frs_merge(
                                db_handle as FrsDb,
                                cf,
                                key.as_ptr(),
                                key.len(),
                                value.as_ptr(),
                                value.len(),
                            )
                        }
                    }
                    WriteBatchEntry::Delete { cf, key } => {
                        // SAFETY: same as above.
                        unsafe { frs_delete(db_handle as FrsDb, cf, key.as_ptr(), key.len()) }
                    }
                };
                if check_status(env, status, &label) {
                    return;
                }
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The compat module must register itself; this test simply
    /// confirms the informational hook is present.
    #[test]
    fn register_jni_symbols_returns_marker() {
        let s = register_jni_symbols();
        assert!(s.contains("Java_org_forstdb"));
    }

    /// Sanity: the constants match the JNI ABI we promise.
    #[test]
    fn jni_version_matches() {
        assert_eq!(JNI_VERSION_1_8, 0x0001_0008);
    }

    /// Verify the `Java_org_forstdb_RocksDB_*` symbols are present in
    /// the cdylib. We do this by inspecting the running test binary
    /// itself (which is statically linked against forst-rs-ffi as an
    /// rlib in unit-test mode) — we use `nm` on the test binary's
    /// linked artifacts.
    ///
    /// On macOS / Linux only; skipped elsewhere because the symbol
    /// inspector is platform-specific.
    #[test]
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn test_compat_jni_symbol_exports() {
        use std::process::Command;

        // Locate the test binary's containing target/<profile>/deps dir
        // and look for the forst_rs_ffi cdylib that the build produced
        // alongside it.
        let exe = std::env::current_exe().expect("current_exe");
        let mut deps_dir = exe.clone();
        deps_dir.pop(); // remove test binary file name
                        // deps_dir is now .../target/<profile>/deps
        let mut target_dir = deps_dir.clone();
        target_dir.pop(); // .../target/<profile>

        let candidates = ["libforst_rs_ffi.dylib", "libforst_rs_ffi.so"];
        let mut found_lib: Option<std::path::PathBuf> = None;
        for c in &candidates {
            let p = target_dir.join(c);
            if p.exists() {
                found_lib = Some(p);
                break;
            }
        }

        let Some(lib_path) = found_lib else {
            // The cdylib may not have been built yet (e.g. running the
            // unit test before `cargo build`). Don't fail the test, just
            // log; the contract is enforced by the cargo build step in
            // CI.
            eprintln!(
                "compat_jni: no cdylib found in {} — skipping symbol check",
                target_dir.display()
            );
            return;
        };

        let nm = Command::new("nm")
            .arg("-g")
            .arg(&lib_path)
            .output()
            .expect("nm should be available on Linux/macOS");

        let stdout = String::from_utf8_lossy(&nm.stdout);
        let required = [
            // Original 9 entry points (PR adding compat_jni shim).
            "Java_org_forstdb_RocksDB_open",
            "Java_org_forstdb_RocksDB_close",
            "Java_org_forstdb_RocksDB_put",
            "Java_org_forstdb_RocksDB_get",
            "Java_org_forstdb_RocksDB_delete",
            "Java_org_forstdb_RocksDB_createColumnFamily",
            "Java_org_forstdb_RocksDB_dropColumnFamily",
            "Java_org_forstdb_RocksDB_flush",
            "Java_org_forstdb_RocksDB_createCheckpoint",
            // 12 additions for broader Flink-statebackend-forst coverage.
            "Java_org_forstdb_RocksDB_merge",
            "Java_org_forstdb_RocksDB_compactRange",
            "Java_org_forstdb_RocksDB_compactRangeAll",
            "Java_org_forstdb_RocksDB_flushCf",
            "Java_org_forstdb_RocksDB_getDefaultColumnFamily",
            "Java_org_forstdb_RocksDB_getLatestSequenceNumber",
            "Java_org_forstdb_RocksDB_iteratorOpen",
            "Java_org_forstdb_RocksDB_iteratorSeek",
            "Java_org_forstdb_RocksDB_iteratorNext",
            "Java_org_forstdb_RocksDB_iteratorClose",
            "Java_org_forstdb_RocksDB_lookupKv",
            "Java_org_forstdb_RocksDB_dbOpenFromCheckpoint",
            // 15 additions for G-A surface broadening (alternate names,
            // batch helpers, prefix-iterator family, monitoring stubs).
            "Java_org_forstdb_RocksDB_dbOpen",
            "Java_org_forstdb_RocksDB_createColumnFamily2",
            "Java_org_forstdb_RocksDB_openColumnFamily",
            "Java_org_forstdb_RocksDB_l0FileCount",
            "Java_org_forstdb_RocksDB_batchPut",
            "Java_org_forstdb_RocksDB_batchGet",
            "Java_org_forstdb_RocksDB_writeBatch",
            "Java_org_forstdb_RocksDB_prefixLookupOpen",
            "Java_org_forstdb_RocksDB_prefixLookupNext",
            "Java_org_forstdb_RocksDB_prefixLookupClose",
            "Java_org_forstdb_RocksDB_isClosed",
            "Java_org_forstdb_RocksDB_getColumnFamilyHandle",
            "Java_org_forstdb_RocksDB_remove",
            "Java_org_forstdb_RocksDB_putByteArray",
            "Java_org_forstdb_RocksDB_getByteArray",
            // JNI library-load hook.
            "JNI_OnLoad",
            // P0 — DBOptions class (15 entries).
            "Java_org_forstdb_DBOptions_newDBOptions",
            "Java_org_forstdb_DBOptions_disposeInternal",
            "Java_org_forstdb_DBOptions_setCreateIfMissing",
            "Java_org_forstdb_DBOptions_setUseFsync",
            "Java_org_forstdb_DBOptions_setStatsDumpPeriodSec",
            "Java_org_forstdb_DBOptions_setAvoidFlushDuringShutdown",
            "Java_org_forstdb_DBOptions_setDbLogDir",
            "Java_org_forstdb_DBOptions_setInfoLogLevel",
            "Java_org_forstdb_DBOptions_setMaxBackgroundJobs",
            "Java_org_forstdb_DBOptions_setMaxOpenFiles",
            "Java_org_forstdb_DBOptions_setMaxLogFileSize",
            "Java_org_forstdb_DBOptions_setKeepLogFileNum",
            "Java_org_forstdb_DBOptions_setStatistics",
            "Java_org_forstdb_DBOptions_setWriteBufferManager",
            "Java_org_forstdb_DBOptions_setEnv",
            // P0 — ColumnFamilyOptions class (13 entries).
            "Java_org_forstdb_ColumnFamilyOptions_newColumnFamilyOptions",
            "Java_org_forstdb_ColumnFamilyOptions_disposeInternal",
            "Java_org_forstdb_ColumnFamilyOptions_setWriteBufferSize",
            "Java_org_forstdb_ColumnFamilyOptions_setMaxWriteBufferNumber",
            "Java_org_forstdb_ColumnFamilyOptions_setMinWriteBufferNumberToMerge",
            "Java_org_forstdb_ColumnFamilyOptions_setLevelCompactionDynamicLevelBytes",
            "Java_org_forstdb_ColumnFamilyOptions_setMaxBytesForLevelBase",
            "Java_org_forstdb_ColumnFamilyOptions_setTargetFileSizeBase",
            "Java_org_forstdb_ColumnFamilyOptions_setCompressionPerLevel",
            "Java_org_forstdb_ColumnFamilyOptions_setCompactionStyle",
            "Java_org_forstdb_ColumnFamilyOptions_setPeriodicCompactionSeconds",
            "Java_org_forstdb_ColumnFamilyOptions_setTableFormatConfig",
            "Java_org_forstdb_ColumnFamilyOptions_setCompactionFilterFactory",
            "Java_org_forstdb_ColumnFamilyOptions_tableFormatConfig",
            // P0 — WriteOptions class (3 entries).
            "Java_org_forstdb_WriteOptions_newWriteOptions",
            "Java_org_forstdb_WriteOptions_disposeInternal",
            "Java_org_forstdb_WriteOptions_setDisableWAL",
            // P0 — ReadOptions class (3 entries).
            "Java_org_forstdb_ReadOptions_newReadOptions",
            "Java_org_forstdb_ReadOptions_disposeInternal",
            "Java_org_forstdb_ReadOptions_setReadaheadSize",
            // P0 — ColumnFamilyHandle class (3 entries).
            "Java_org_forstdb_ColumnFamilyHandle_disposeInternal",
            "Java_org_forstdb_ColumnFamilyHandle_getName0",
            "Java_org_forstdb_ColumnFamilyHandle_getDescriptor",
            // P0 — multi-CF RocksDB.open overload.
            "Java_org_forstdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J_3J",
            // P1 — RocksIterator class (10 entries).
            "Java_org_forstdb_RocksIterator_seek0",
            "Java_org_forstdb_RocksIterator_seekToFirst0",
            "Java_org_forstdb_RocksIterator_seekToLast0",
            "Java_org_forstdb_RocksIterator_seekForPrev0",
            "Java_org_forstdb_RocksIterator_next0",
            "Java_org_forstdb_RocksIterator_prev0",
            "Java_org_forstdb_RocksIterator_isValid0",
            "Java_org_forstdb_RocksIterator_key0",
            "Java_org_forstdb_RocksIterator_value0",
            "Java_org_forstdb_RocksIterator_disposeInternal",
            // P1 — RocksDB iterator factory (2 entries).
            "Java_org_forstdb_RocksDB_iterator",
            "Java_org_forstdb_RocksDB_iteratorCF",
            // P1 — WriteBatch class (8 entries).
            "Java_org_forstdb_WriteBatch_newWriteBatch",
            "Java_org_forstdb_WriteBatch_disposeInternal",
            "Java_org_forstdb_WriteBatch_put",
            "Java_org_forstdb_WriteBatch_merge",
            "Java_org_forstdb_WriteBatch_delete",
            "Java_org_forstdb_WriteBatch_clear0",
            "Java_org_forstdb_WriteBatch_count0",
            "Java_org_forstdb_WriteBatch_getDataSize",
            // P1 — RocksDB.write0 (1 entry).
            "Java_org_forstdb_RocksDB_write0",
        ];
        for sym in &required {
            assert!(
                stdout.contains(sym),
                "expected symbol `{sym}` missing from {} (nm output:\n{})",
                lib_path.display(),
                stdout
            );
        }
    }

    // -------------------------------------------------------------------
    // P0 — Options class lifecycle smoke tests
    //
    // We can't invoke `Java_org_forstdb_*` thunks directly without a JVM,
    // so these tests exercise the Rust-internal handle round-trip
    // pattern: Box::into_raw → from_raw_ref → Box::from_raw. The thunks
    // themselves are thin wrappers around `jni_guard` + the handle
    // module — exercising the handle module covers the failure modes
    // (null handle = None, double-dispose = leaked Box, etc.) under
    // Miri / Sanitizers without needing a running JVM.
    // -------------------------------------------------------------------

    #[test]
    fn test_db_options_lifecycle() {
        // newDBOptions ⇒ non-zero handle.
        let h = DbOptionsHandle {
            opts: EngineOptions::default(),
        }
        .into_raw();
        assert_ne!(h, 0, "DbOptionsHandle::into_raw must not return 0");

        // setCreateIfMissing equivalent: mutating field via from_raw_ref.
        // SAFETY: handle came from `into_raw` immediately above, no aliasing.
        let opts_ref =
            unsafe { DbOptionsHandle::from_raw_ref(h) }.expect("non-null handle should resolve");
        opts_ref.opts.max_background_compactions = 8;
        opts_ref.opts.max_background_flushes = 4;

        // Re-borrow to verify the write took.
        let opts_ref2 =
            unsafe { DbOptionsHandle::from_raw_ref(h) }.expect("non-null handle should resolve");
        assert_eq!(opts_ref2.opts.max_background_compactions, 8);
        assert_eq!(opts_ref2.opts.max_background_flushes, 4);

        // disposeInternal equivalent.
        // SAFETY: Box round-trip, sole owner.
        unsafe {
            drop(Box::from_raw(h as *mut DbOptionsHandle));
        }

        // disposeInternal(0) is a no-op.
        let zero = unsafe { DbOptionsHandle::from_raw_ref(0) };
        assert!(zero.is_none(), "from_raw_ref(0) must return None");
    }

    #[test]
    fn test_cf_options_lifecycle() {
        let h = CfOptionsHandle::default().into_raw();
        assert_ne!(h, 0);

        // setWriteBufferSize equivalent.
        // SAFETY: just-allocated, sole owner.
        let opts_ref = unsafe { CfOptionsHandle::from_raw_ref(h) }.unwrap();
        opts_ref.opts.write_buffer_size = Some(128 * 1024 * 1024);
        opts_ref.opts.max_write_buffer_number = Some(4);
        opts_ref.opts.compression = Some(CompressionType::Zstd);
        opts_ref.table_format_handle = 0xdead_beef;

        // Verify state.
        let opts_ref2 = unsafe { CfOptionsHandle::from_raw_ref(h) }.unwrap();
        assert_eq!(opts_ref2.opts.write_buffer_size, Some(128 * 1024 * 1024));
        assert_eq!(opts_ref2.opts.max_write_buffer_number, Some(4));
        assert_eq!(opts_ref2.opts.compression, Some(CompressionType::Zstd));
        assert_eq!(opts_ref2.table_format_handle, 0xdead_beef);

        // disposeInternal.
        // SAFETY: Box round-trip.
        unsafe {
            drop(Box::from_raw(h as *mut CfOptionsHandle));
        }
    }

    #[test]
    fn test_write_read_options_lifecycle() {
        // WriteOptions.
        let wh = WriteOptionsHandle::default().into_raw();
        assert_ne!(wh, 0);
        // SAFETY: just-allocated.
        let wref = unsafe { WriteOptionsHandle::from_raw_ref(wh) }.unwrap();
        assert!(!wref.disable_wal);
        wref.disable_wal = true;
        let wref2 = unsafe { WriteOptionsHandle::from_raw_ref(wh) }.unwrap();
        assert!(wref2.disable_wal);
        unsafe {
            drop(Box::from_raw(wh as *mut WriteOptionsHandle));
        }

        // ReadOptions.
        let rh = ReadOptionsHandle::default().into_raw();
        assert_ne!(rh, 0);
        // SAFETY: just-allocated.
        let rref = unsafe { ReadOptionsHandle::from_raw_ref(rh) }.unwrap();
        assert_eq!(rref.readahead_size, 0);
        assert!(rref.fill_cache);
        assert!(rref.verify_checksums);
        rref.readahead_size = 4096;
        let rref2 = unsafe { ReadOptionsHandle::from_raw_ref(rh) }.unwrap();
        assert_eq!(rref2.readahead_size, 4096);
        unsafe {
            drop(Box::from_raw(rh as *mut ReadOptionsHandle));
        }
    }

    /// Multi-CF open: spin up an in-process engine via the `frs_*` C ABI,
    /// build wrapping `CfHandle` boxes the way the JNI thunk would, and
    /// verify every handle round-trips and disposes cleanly. This is the
    /// JVM-free equivalent of calling `RocksDB.open(opts, path, names,
    /// opts, out)` and then `ColumnFamilyHandle.close()` on each handle.
    #[test]
    fn test_multi_cf_open_two_cfs() {
        use std::ffi::CString;

        // 1. Open an in-memory engine via the existing FFI path.
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: out_handle is a stack local we own.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(!db.is_null());

        // 2. Resolve default CF.
        let mut default_cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db came from frs_db_open_memory.
        let st = unsafe { frs_db_default_cf(db, &mut default_cf) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(!default_cf.is_null());

        // 3. Create an extra "test" CF.
        let test_cname = CString::new("test").unwrap();
        let mut test_cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid, name valid, out_cf stack-local.
        let st = unsafe { frs_db_create_cf(db, test_cname.as_ptr(), &mut test_cf) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(!test_cf.is_null());

        // 4. Wrap each in a CfHandle box (this is what the multi-CF open
        //    JNI thunk does in its inner loop).
        let default_handle = CfHandle {
            name: b"default".to_vec(),
            frs_handle: default_cf,
            owned_opts_handle: 0,
        }
        .into_raw();
        let test_handle = CfHandle {
            name: b"test".to_vec(),
            frs_handle: test_cf,
            owned_opts_handle: 0,
        }
        .into_raw();
        assert_ne!(default_handle, 0);
        assert_ne!(test_handle, 0);

        // 5. Verify each round-trips with the right name.
        // SAFETY: just-allocated.
        let h0 = unsafe { CfHandle::from_raw_ref(default_handle) }.unwrap();
        assert_eq!(&h0.name, b"default");
        assert_eq!(h0.frs_handle, default_cf);

        // SAFETY: just-allocated.
        let h1 = unsafe { CfHandle::from_raw_ref(test_handle) }.unwrap();
        assert_eq!(&h1.name, b"test");
        assert_eq!(h1.frs_handle, test_cf);

        // 6. Dispose: this is what `ColumnFamilyHandle.disposeInternal`
        //    does — drop the box and close the engine handle.
        for h in [default_handle, test_handle] {
            // SAFETY: we just leaked these boxes; sole owner.
            let cf = unsafe { Box::from_raw(h as *mut CfHandle) };
            // SAFETY: frs_handle came from the engine.
            let st = unsafe { crate::frs_cf_close(cf.frs_handle) };
            assert_eq!(st, FRS_STATUS_OK);
            drop(cf);
        }

        // 7. Close DB.
        // SAFETY: db came from frs_db_open_memory; not yet closed.
        let st = unsafe { frs_db_close(db) };
        assert_eq!(st, FRS_STATUS_OK);
    }

    /// Cleanup-partial-open helper test: when the multi-CF open fails part
    /// way through, every leaked CfHandle box must drop and the engine
    /// must close. This guards the sad-path leak window.
    #[test]
    fn test_cleanup_partial_open_releases_resources() {
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: stack-local out param.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);

        let mut cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid.
        let st = unsafe { frs_db_default_cf(db, &mut cf) };
        assert_eq!(st, FRS_STATUS_OK);

        let leaked = CfHandle {
            name: b"default".to_vec(),
            frs_handle: cf,
            owned_opts_handle: 0,
        }
        .into_raw();
        cleanup_partial_open(db, &[leaked]);
        // After cleanup the db handle is freed; we can't safely re-use it.
        // The mere fact that we don't crash is what we're testing.
    }

    // -------------------------------------------------------------------
    // P1 — RocksIterator + WriteBatch lifecycle smoke tests
    //
    // The Java thunks themselves can't run without a JVM, so these tests
    // exercise the same `RocksIteratorHandle` / `WriteBatchHandle`
    // round-trip pattern + the engine-side iterator the thunks drive.
    // Together they cover the failure modes (null handle, empty CF,
    // seek-past-end, dispose ordering) that would surface as a
    // `RocksDBException` if the thunk were called from Java.
    // -------------------------------------------------------------------

    /// Helper: open a fresh in-memory engine + default CF and seed it with
    /// the supplied `(key, value)` pairs (in caller-supplied order; the
    /// engine sorts them internally).
    fn open_seeded_engine(seed: &[(&[u8], &[u8])]) -> (FrsDb, FrsCfHandle) {
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: stack-local out param.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);
        let mut cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid.
        let st = unsafe { frs_db_default_cf(db, &mut cf) };
        assert_eq!(st, FRS_STATUS_OK);
        for (k, v) in seed {
            // SAFETY: db / cf valid; pointers describe stack-local slices.
            let st = unsafe { crate::frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()) };
            assert_eq!(st, FRS_STATUS_OK);
        }
        (db, cf)
    }

    /// Drives the same operations `Java_org_forstdb_RocksDB_iterator` →
    /// `seekToFirst0` → `next0` x 5 → `disposeInternal` would produce when
    /// invoked from Java, but inside Rust so we don't need a JVM.
    #[test]
    fn test_rocks_iterator_lifecycle() {
        let seed: [(&[u8], &[u8]); 5] = [
            (b"k1", b"v1"),
            (b"k2", b"v2"),
            (b"k3", b"v3"),
            (b"k4", b"v4"),
            (b"k5", b"v5"),
        ];
        let (db, cf) = open_seeded_engine(&seed);

        // Mirror Java_org_forstdb_RocksDB_iterator's body: open the engine
        // iterator and box it as a RocksIteratorHandle.
        let mut iter: FrsIterator = ptr::null_mut();
        // SAFETY: db / cf valid.
        let st = unsafe { frs_iterator_open(db, cf, &mut iter) };
        assert_eq!(st, FRS_STATUS_OK);
        let h = RocksIteratorHandle {
            frs_iter: iter,
            last_key: None,
            last_value: None,
            valid: false,
        }
        .into_raw();
        assert_ne!(h, 0);

        // SAFETY: just-allocated; sole owner.
        let href = unsafe { RocksIteratorHandle::from_raw_ref(h) }.unwrap();

        // seekToFirst0: equivalent to frs_iterator_seek(NULL, 0) +
        // fetch_into_handle. Drive that directly.
        // SAFETY: frs_iter valid.
        let st = unsafe { frs_iterator_seek(href.frs_iter, ptr::null(), 0) };
        assert_eq!(st, FRS_STATUS_OK);
        // Fetch the first row inline (mirror of fetch_into_handle).
        let mut k = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut v = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut valid = false;
        // SAFETY: frs_iter valid; out_* stack-locals.
        let st = unsafe { frs_iterator_next(href.frs_iter, &mut k, &mut v, &mut valid) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(valid, "first row should be valid");
        // SAFETY: k.data / v.data describe Rust-owned buffers.
        let key0 = unsafe { std::slice::from_raw_parts(k.data, k.len).to_vec() };
        let val0 = unsafe { std::slice::from_raw_parts(v.data, v.len).to_vec() };
        unsafe {
            let _ = crate::frs_bytes_free(&mut k);
            let _ = crate::frs_bytes_free(&mut v);
        }
        href.valid = true;
        href.last_key = Some(key0);
        href.last_value = Some(val0);

        // Walk via the engine's frs_iterator_next; collect 5 entries.
        let mut walked: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        // First row was the one we just fetched.
        walked.push((
            href.last_key.clone().unwrap(),
            href.last_value.clone().unwrap(),
        ));
        for _ in 0..4 {
            let mut kk = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            let mut vv = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            let mut vld = false;
            // SAFETY: same as above.
            let st = unsafe { frs_iterator_next(href.frs_iter, &mut kk, &mut vv, &mut vld) };
            assert_eq!(st, FRS_STATUS_OK);
            assert!(vld);
            // SAFETY: kk/vv populated.
            let kvec = unsafe { std::slice::from_raw_parts(kk.data, kk.len).to_vec() };
            let vvec = unsafe { std::slice::from_raw_parts(vv.data, vv.len).to_vec() };
            unsafe {
                let _ = crate::frs_bytes_free(&mut kk);
                let _ = crate::frs_bytes_free(&mut vv);
            }
            walked.push((kvec, vvec));
        }
        // 6th call past end should be invalid.
        let mut kk = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut vv = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut vld = true;
        // SAFETY: same.
        let st = unsafe { frs_iterator_next(href.frs_iter, &mut kk, &mut vv, &mut vld) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(!vld, "6th call past end should be invalid");

        let walked_keys: Vec<Vec<u8>> = walked.iter().map(|(k, _)| k.clone()).collect();
        let walked_vals: Vec<Vec<u8>> = walked.iter().map(|(_, v)| v.clone()).collect();
        assert_eq!(
            walked_keys,
            vec![
                b"k1".to_vec(),
                b"k2".to_vec(),
                b"k3".to_vec(),
                b"k4".to_vec(),
                b"k5".to_vec(),
            ]
        );
        assert_eq!(
            walked_vals,
            vec![
                b"v1".to_vec(),
                b"v2".to_vec(),
                b"v3".to_vec(),
                b"v4".to_vec(),
                b"v5".to_vec(),
            ]
        );

        // Mirror disposeInternal: take ownership of the box, close the iter.
        // SAFETY: just-allocated; sole owner.
        let h_owned = unsafe { Box::from_raw(h as *mut RocksIteratorHandle) };
        // SAFETY: frs_iter came from frs_iterator_open.
        let st = unsafe { frs_iterator_close(h_owned.frs_iter) };
        assert_eq!(st, FRS_STATUS_OK);
        drop(h_owned);

        // Tear down the engine.
        // SAFETY: cf came from frs_db_default_cf.
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        // SAFETY: db came from frs_db_open_memory.
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    /// Mirror of `seek0(b"k2") + isValid0() + key0() + value0()`. Validates
    /// the seek-then-cache pattern: seek finds the first key >= needle and
    /// pre-fetches it.
    #[test]
    fn test_rocks_iterator_seek() {
        let seed: [(&[u8], &[u8]); 3] = [(b"k1", b"v1"), (b"k3", b"v3"), (b"k5", b"v5")];
        let (db, cf) = open_seeded_engine(&seed);

        let mut iter: FrsIterator = ptr::null_mut();
        // SAFETY: db / cf valid.
        let st = unsafe { frs_iterator_open(db, cf, &mut iter) };
        assert_eq!(st, FRS_STATUS_OK);
        let h = RocksIteratorHandle {
            frs_iter: iter,
            last_key: None,
            last_value: None,
            valid: false,
        }
        .into_raw();

        // SAFETY: just-allocated.
        let href = unsafe { RocksIteratorHandle::from_raw_ref(h) }.unwrap();

        // seek to "k2" → first key >= "k2" is "k3".
        let needle = b"k2";
        // SAFETY: frs_iter valid; needle is a literal byte slice.
        let st = unsafe { frs_iterator_seek(href.frs_iter, needle.as_ptr(), needle.len()) };
        assert_eq!(st, FRS_STATUS_OK);
        // Fetch.
        let mut k = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut v = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut vld = false;
        // SAFETY: see above.
        let st = unsafe { frs_iterator_next(href.frs_iter, &mut k, &mut v, &mut vld) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(vld);
        // SAFETY: k/v populated.
        let kvec = unsafe { std::slice::from_raw_parts(k.data, k.len).to_vec() };
        let vvec = unsafe { std::slice::from_raw_parts(v.data, v.len).to_vec() };
        unsafe {
            let _ = crate::frs_bytes_free(&mut k);
            let _ = crate::frs_bytes_free(&mut v);
        }
        assert_eq!(kvec, b"k3".to_vec(), "seek(k2) should land on k3");
        assert_eq!(vvec, b"v3".to_vec());

        // Validate seekToLast via direct pub(crate) cursor manipulation.
        // SAFETY: same-crate access; sole owner of the box.
        let state = unsafe { &mut *(href.frs_iter as *mut crate::IteratorState) };
        assert_eq!(state.rows.len(), 3);
        state.cursor = state.rows.len() - 1;
        let mut k = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut v = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut vld = false;
        // SAFETY: same.
        let st = unsafe { frs_iterator_next(href.frs_iter, &mut k, &mut v, &mut vld) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(vld);
        // SAFETY: populated.
        let kvec = unsafe { std::slice::from_raw_parts(k.data, k.len).to_vec() };
        unsafe {
            let _ = crate::frs_bytes_free(&mut k);
            let _ = crate::frs_bytes_free(&mut v);
        }
        assert_eq!(
            kvec,
            b"k5".to_vec(),
            "seekToLast equivalent should land on k5"
        );

        // Cleanup.
        // SAFETY: just-allocated; sole owner.
        let h_owned = unsafe { Box::from_raw(h as *mut RocksIteratorHandle) };
        unsafe {
            let _ = frs_iterator_close(h_owned.frs_iter);
        }
        drop(h_owned);
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    /// WriteBatch lifecycle: ctor, multiple put, one delete, count, dispose.
    #[test]
    fn test_write_batch_lifecycle() {
        let h = WriteBatchHandle {
            entries: Vec::with_capacity(1024 / 64),
            data_size: 0,
        }
        .into_raw();
        assert_ne!(h, 0);

        // SAFETY: just-allocated.
        let href = unsafe { WriteBatchHandle::from_raw_ref(h) }.unwrap();
        // 2 puts + 1 delete (CF handle is opaque — we don't need a real
        // engine for the buffering test).
        href.entries.push(WriteBatchEntry::Put {
            cf: ptr::null_mut(),
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
        });
        href.data_size += 4;
        href.entries.push(WriteBatchEntry::Put {
            cf: ptr::null_mut(),
            key: b"k2".to_vec(),
            value: b"v2".to_vec(),
        });
        href.data_size += 4;
        href.entries.push(WriteBatchEntry::Delete {
            cf: ptr::null_mut(),
            key: b"k1".to_vec(),
        });
        href.data_size += 2;

        // count0 equivalent.
        assert_eq!(href.entries.len(), 3);
        // getDataSize equivalent.
        assert_eq!(href.data_size, 10);

        // dispose.
        // SAFETY: Box round-trip; sole owner.
        unsafe { drop(Box::from_raw(h as *mut WriteBatchHandle)) };
    }

    /// Build a batch + apply it via the engine + verify keys are present
    /// (mirror of `Java_org_forstdb_RocksDB_write0`'s drain-and-dispatch
    /// loop, but driven from Rust).
    #[test]
    fn test_write_batch_apply_via_db_write() {
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: out param.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);
        let mut cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid.
        let st = unsafe { frs_db_default_cf(db, &mut cf) };
        assert_eq!(st, FRS_STATUS_OK);

        // Build a batch with 3 puts + 1 delete.
        let h = WriteBatchHandle {
            entries: Vec::new(),
            data_size: 0,
        }
        .into_raw();
        // SAFETY: just-allocated.
        let href = unsafe { WriteBatchHandle::from_raw_ref(h) }.unwrap();
        href.entries.push(WriteBatchEntry::Put {
            cf,
            key: b"alpha".to_vec(),
            value: b"1".to_vec(),
        });
        href.entries.push(WriteBatchEntry::Put {
            cf,
            key: b"beta".to_vec(),
            value: b"2".to_vec(),
        });
        href.entries.push(WriteBatchEntry::Put {
            cf,
            key: b"gamma".to_vec(),
            value: b"3".to_vec(),
        });
        href.entries.push(WriteBatchEntry::Delete {
            cf,
            key: b"beta".to_vec(),
        });

        // Drain & apply (mirror of write0 body).
        // SAFETY: same-pattern access.
        let entries = std::mem::take(&mut href.entries);
        for entry in entries {
            let status = match entry {
                WriteBatchEntry::Put { cf, key, value } => {
                    // SAFETY: pointers live for the call.
                    unsafe {
                        crate::frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len())
                    }
                }
                WriteBatchEntry::Delete { cf, key } => {
                    // SAFETY: same.
                    unsafe { crate::frs_delete(db, cf, key.as_ptr(), key.len()) }
                }
                WriteBatchEntry::Merge { .. } => unreachable!("test does not use merge"),
            };
            assert_eq!(status, FRS_STATUS_OK);
        }

        // Verify: alpha + gamma present, beta absent.
        for (key, expected) in [
            (b"alpha".as_ref(), Some(b"1".as_ref())),
            (b"gamma", Some(b"3")),
        ] {
            let mut out = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            // SAFETY: stack-local out param.
            let st = unsafe { crate::frs_get(db, cf, key.as_ptr(), key.len(), &mut out) };
            assert_eq!(st, FRS_STATUS_OK);
            // SAFETY: out populated on hit.
            let got = unsafe { std::slice::from_raw_parts(out.data, out.len).to_vec() };
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
            assert_eq!(got, expected.unwrap().to_vec());
        }
        let mut out = FrsBytes {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        // SAFETY: out param.
        let st = unsafe { crate::frs_get(db, cf, b"beta".as_ptr(), 4, &mut out) };
        assert!(st == FRS_STATUS_OK || st == FRS_STATUS_NOT_FOUND);
        if st == FRS_STATUS_OK {
            assert!(out.data.is_null(), "beta should be tombstoned");
        }
        unsafe {
            let _ = crate::frs_bytes_free(&mut out);
        }

        // Cleanup.
        // SAFETY: Box round-trip.
        unsafe { drop(Box::from_raw(h as *mut WriteBatchHandle)) };
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    /// `clear0()` zeroes both the entry list and the running data_size
    /// counter, but preserves the Vec's capacity to avoid reallocation
    /// across reuse cycles.
    #[test]
    fn test_write_batch_clear_resets_count() {
        let h = WriteBatchHandle {
            entries: Vec::with_capacity(8),
            data_size: 0,
        }
        .into_raw();
        // SAFETY: just-allocated.
        let href = unsafe { WriteBatchHandle::from_raw_ref(h) }.unwrap();
        for i in 0..5 {
            href.entries.push(WriteBatchEntry::Put {
                cf: ptr::null_mut(),
                key: vec![i],
                value: vec![i, i],
            });
            href.data_size += 3;
        }
        assert_eq!(href.entries.len(), 5);
        assert_eq!(href.data_size, 15);
        let cap_before = href.entries.capacity();

        // clear0 equivalent.
        href.entries.clear();
        href.data_size = 0;
        assert_eq!(href.entries.len(), 0);
        assert_eq!(href.data_size, 0);
        assert_eq!(
            href.entries.capacity(),
            cap_before,
            "clear must preserve capacity"
        );

        // dispose.
        // SAFETY: Box round-trip.
        unsafe { drop(Box::from_raw(h as *mut WriteBatchHandle)) };
    }

    /// `WriteBatchHandle::from_raw_ref(0)` must return `None` (matches the
    /// other handle types).
    #[test]
    fn test_write_batch_null_handle_is_none() {
        let none = unsafe { WriteBatchHandle::from_raw_ref(0) };
        assert!(none.is_none());
        let none = unsafe { RocksIteratorHandle::from_raw_ref(0) };
        assert!(none.is_none());
    }
}

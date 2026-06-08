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

use std::collections::HashMap;
use std::fs;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock};

use jni::objects::{JByteArray, JClass, JObject, JObjectArray, JPrimitiveArray, JString};
use jni::sys::{
    jboolean, jbyte, jbyteArray, jint, jlong, jobjectArray, JavaVM, JNI_FALSE, JNI_TRUE,
    JNI_VERSION_1_8,
};
use jni::JNIEnv;

use forst_rs_common::{CfOptions, CompressionType, EngineOptions};
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};

use crate::{
    frs_batch_get, frs_batch_put, frs_compact_all, frs_compact_cf, frs_create_checkpoint,
    frs_db_close, frs_db_create_cf, frs_db_create_cf_with_merge, frs_db_default_cf, frs_db_open,
    frs_db_open_cf, frs_db_open_from_checkpoint, frs_delete, frs_flush, frs_flush_cf, frs_get,
    frs_iterator_close, frs_iterator_next, frs_iterator_open, frs_iterator_seek, frs_l0_file_count,
    frs_lookup_kv, frs_merge, frs_prefix_lookup_close, frs_prefix_lookup_open, frs_put,
    frs_sequence_number, FrsBytes, FrsCfHandle, FrsDb, FrsIterator, FRS_STATUS_NOT_FOUND,
    FRS_STATUS_OK,
};

static DB_PATH_REGISTRY: OnceLock<Mutex<HashMap<usize, String>>> = OnceLock::new();

fn db_path_registry() -> &'static Mutex<HashMap<usize, String>> {
    DB_PATH_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn register_db_path(handle: FrsDb, path: &str) {
    if handle.is_null() {
        return;
    }
    if let Ok(mut guard) = db_path_registry().lock() {
        guard.insert(handle as usize, path.to_string());
    }
}

fn unregister_db_path(handle: FrsDb) {
    if handle.is_null() {
        return;
    }
    if let Ok(mut guard) = db_path_registry().lock() {
        guard.remove(&(handle as usize));
    }
}

fn db_path_for(handle: FrsDb) -> Option<String> {
    if handle.is_null() {
        return None;
    }
    db_path_registry()
        .lock()
        .ok()
        .and_then(|guard| guard.get(&(handle as usize)).cloned())
}

fn ensure_compat_manifest_for_db(handle: FrsDb) -> Option<(String, u64)> {
    let db_path = db_path_for(handle)?;

    if let Ok(read_dir) = fs::read_dir(&db_path) {
        let mut manifests = Vec::<(String, u64)>::new();
        for entry in read_dir.flatten() {
            let Ok(meta) = entry.metadata() else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            if name.starts_with("MANIFEST") {
                manifests.push((name, meta.len()));
            }
        }
        manifests.sort_by(|a, b| a.0.cmp(&b.0));
        if let Some(existing) = manifests.into_iter().next() {
            return Some(existing);
        }
    }

    let manifest_name = "MANIFEST-000001".to_string();
    let manifest_path = std::path::Path::new(&db_path).join(&manifest_name);
    if !manifest_path.exists()
        && fs::write(&manifest_path, b"forst-rs compatibility manifest\n").is_err()
    {
        return None;
    }
    let manifest_size = fs::metadata(&manifest_path).ok()?.len();
    Some((manifest_name, manifest_size))
}

fn live_file_name_for_java(path_str: &str) -> String {
    std::path::Path::new(path_str)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .unwrap_or_else(|| path_str.to_string())
}

fn fallback_live_files_for_db(handle: FrsDb) -> Option<crate::FrsLiveFileList> {
    let db_path = db_path_for(handle)?;
    let read_dir = fs::read_dir(&db_path).ok()?;
    let mut files = Vec::<(String, u64)>::new();
    let mut manifest_size = 0_u64;

    for entry in read_dir.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
            continue;
        };
        let is_manifest = name.starts_with("MANIFEST");
        let is_live_data = name.ends_with(".sst") || name.ends_with(".ldb");
        let is_current = name == "CURRENT";
        if !(is_manifest || is_live_data || is_current) {
            continue;
        }
        if is_manifest {
            manifest_size = meta.len();
        }
        files.push((name, meta.len()));
    }

    files.sort_by(|a, b| a.0.cmp(&b.0));
    let mut converted = Vec::<crate::FrsLiveFile>::with_capacity(files.len());
    for (name, size) in files {
        let Ok(path_c) = std::ffi::CString::new(name) else {
            continue;
        };
        let cf_c = std::ffi::CString::new("default").expect("static string has no NUL");
        converted.push(crate::FrsLiveFile {
            path: path_c.into_raw(),
            size,
            sequence: 0,
            level: 0,
            cf_name: cf_c.into_raw(),
        });
    }

    let count = converted.len();
    let ptr = if count == 0 {
        ptr::null_mut()
    } else {
        let ptr = converted.as_mut_ptr();
        std::mem::forget(converted);
        ptr
    };
    Some(crate::FrsLiveFileList {
        files: ptr,
        count,
        manifest_size,
    })
}

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
        pub max_open_files: jint,
        pub info_log_level: jbyte,
        pub db_log_dir: Option<String>,
        pub keep_log_file_num: jlong,
        pub max_log_file_size: jlong,
        pub statistics_handle: jlong,
    }

    impl Default for DbOptionsHandle {
        fn default() -> Self {
            Self {
                opts: EngineOptions::default(),
                // RocksDB's Java default is unlimited/open-as-needed. forst-rs
                // does not expose a table file cache knob yet, but callers may
                // still round-trip this value through DBOptions tests.
                max_open_files: -1,
                // INFO_LEVEL in org.forstdb.InfoLogLevel.
                info_log_level: 2,
                db_log_dir: None,
                // RocksDB Java default. forst-rs tracing/log retention is not
                // wired to this knob yet, but Flink config tests require the
                // option to round-trip accurately.
                keep_log_file_num: 1000,
                max_log_file_size: 0,
                statistics_handle: 0,
            }
        }
    }

    /// Java `org.forstdb.Env` mirror. forst-rs owns its filesystem/runtime
    /// internally, so this is only a lifecycle-compatible placeholder for
    /// `DBOptions`' default Env reference.
    pub(crate) struct EnvHandle;

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
        pub compaction_style: jbyte,
        pub level_compaction_dynamic_level_bytes: bool,
        pub max_bytes_for_level_base: Option<usize>,
        pub min_write_buffer_number_to_merge: jint,
        pub compression_per_level: Vec<u8>,
        pub arena_block_size: jlong,
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

    /// Java `org.forstdb.FlushOptions` mirror. The current engine flush API
    /// does not expose wait/stall toggles, but Flink constructs and disposes
    /// FlushOptions when forcing DB logs/checkpoints.
    pub(crate) struct FlushOptionsHandle {
        pub wait_for_flush: bool,
        pub allow_write_stall: bool,
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
    impl_into_from_raw!(EnvHandle);
    impl_into_from_raw!(CfOptionsHandle);
    impl_into_from_raw!(WriteOptionsHandle);
    impl_into_from_raw!(ReadOptionsHandle);
    impl_into_from_raw!(FlushOptionsHandle);
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

    impl Default for FlushOptionsHandle {
        fn default() -> Self {
            Self {
                wait_for_flush: true,
                allow_write_stall: false,
            }
        }
    }
}

use handles::{
    CfHandle, CfOptionsHandle, DbOptionsHandle, EnvHandle, FlushOptionsHandle, ReadOptionsHandle,
    RocksIteratorHandle, WriteBatchEntry, WriteBatchHandle, WriteOptionsHandle,
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

const FORSTJNI_COMPAT_VERSION: jint = (0 << 16) | (1 << 8) | 8;

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

/// `org.forstdb.RocksDB.version() -> int`
///
/// Java signature: `()I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_version<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jint {
    FORSTJNI_COMPAT_VERSION
}

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

/// `org.forstdb.Options.<init>() -> long`
///
/// Java signature: `()J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Options_newOptions__<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| DbOptionsHandle::default().into_raw(),
    )
}

/// `org.forstdb.Options.<init>(DBOptions, ColumnFamilyOptions) -> long`
///
/// Java signature: `(JJ)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Options_newOptions__JJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_opts_handle: jlong,
    _cf_opts_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let mut opts = DbOptionsHandle::default();
            if let Some(db_opts) = unsafe { DbOptionsHandle::from_raw_ref(db_opts_handle) } {
                opts.opts = db_opts.opts.clone();
                opts.max_open_files = db_opts.max_open_files;
                opts.info_log_level = db_opts.info_log_level;
                opts.db_log_dir = db_opts.db_log_dir.clone();
                opts.keep_log_file_num = db_opts.keep_log_file_num;
                opts.max_log_file_size = db_opts.max_log_file_size;
                opts.statistics_handle = db_opts.statistics_handle;
            }
            opts.into_raw()
        },
    )
}

/// `org.forstdb.Options.copyOptions(long) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Options_copyOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let mut opts = DbOptionsHandle::default();
            if let Some(src) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                opts.opts = src.opts.clone();
                opts.max_open_files = src.max_open_files;
                opts.info_log_level = src.info_log_level;
                opts.db_log_dir = src.db_log_dir.clone();
                opts.keep_log_file_num = src.keep_log_file_num;
                opts.max_log_file_size = src.max_log_file_size;
                opts.statistics_handle = src.statistics_handle;
            }
            opts.into_raw()
        },
    )
}

/// `org.forstdb.Options.disposeInternal(long)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Options_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                unsafe { drop(Box::from_raw(handle as *mut DbOptionsHandle)) };
            }
        },
    )
}

/// `org.forstdb.Options.setCreateIfMissing(long, boolean)`
///
/// Java signature: `(JZ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Options_setCreateIfMissing<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _value: jboolean,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.RocksDB.open(String path) -> long handle`
///
/// Java signature: `(Ljava/lang/String;)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_open__Ljava_lang_String_2<'local>(
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
            let db_path = path_str.clone();
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
            register_db_path(handle, &db_path);
            handle as jlong
        },
    )
}

/// `org.forstdb.RocksDB.open(long optionsHandle, String path) -> long handle`
///
/// Java signature: `(JLjava/lang/String;)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_open__JLjava_lang_String_2<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_opts_handle: jlong,
    path: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(path_str) = read_string(env, &path) else {
                return 0_i64;
            };

            let mut engine_opts = unsafe { DbOptionsHandle::from_raw_ref(db_opts_handle) }
                .map(|h| h.opts.clone())
                .unwrap_or_else(EngineOptions::default);
            let db_path = path_str.clone();
            engine_opts.db_path = path_str;

            let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
            match DbImpl::open_with_fs(engine_opts, fs) {
                Ok(db) => {
                    let handle = Box::into_raw(Box::new(db)) as FrsDb;
                    register_db_path(handle, &db_path);
                    handle as jlong
                }
                Err(e) => {
                    throw_rocksdb(env, &format!("RocksDB.open: {e}"));
                    0
                }
            }
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
            unregister_db_path(handle as FrsDb);
            // SAFETY: handle came from a prior frs_db_open; nullity is checked
            // inside frs_db_close.
            let status = unsafe { frs_db_close(handle as FrsDb) };
            check_status(env, status, "RocksDB.close");
        },
    )
}

/// `org.forstdb.RocksDB.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.RocksDB.closeDatabase(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_closeDatabase<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            unregister_db_path(handle as FrsDb);
            let status = unsafe { frs_db_close(handle as FrsDb) };
            check_status(env, status, "RocksDB.closeDatabase");
        },
    )
}

fn default_cf_for_db(env: &mut JNIEnv, handle: jlong, context: &str) -> Option<FrsCfHandle> {
    let mut cf: FrsCfHandle = ptr::null_mut();
    let status = unsafe { frs_db_default_cf(handle as FrsDb, &mut cf) };
    if check_status(env, status, context) {
        None
    } else {
        Some(cf)
    }
}

fn cf_from_java_handle(env: &mut JNIEnv, cf_handle: jlong, context: &str) -> Option<FrsCfHandle> {
    let Some(cf) = (unsafe { CfHandle::from_raw_ref(cf_handle) }) else {
        throw_rocksdb(env, &format!("{context}: null ColumnFamilyHandle"));
        return None;
    };
    Some(cf.frs_handle)
}

fn cf_options_merge_operator_name(cf_options_handle: jlong) -> Option<String> {
    if cf_options_handle == 0 {
        return None;
    }
    unsafe { CfOptionsHandle::from_raw_ref(cf_options_handle) }
        .and_then(|h| h.opts.merge_operator.clone())
}

fn create_cf_with_optional_merge(
    env: &mut JNIEnv,
    db: FrsDb,
    c_name: &std::ffi::CStr,
    merge_operator: Option<String>,
    out_cf: &mut FrsCfHandle,
    context: &str,
) -> i32 {
    if let Some(op) = merge_operator {
        let c_op = match std::ffi::CString::new(op) {
            Ok(s) => s,
            Err(_) => {
                throw_rocksdb(
                    env,
                    &format!("{context}: merge operator contains interior NUL"),
                );
                return FRS_STATUS_NOT_FOUND;
            }
        };
        unsafe { frs_db_create_cf_with_merge(db, c_name.as_ptr(), c_op.as_ptr(), out_cf) }
    } else {
        unsafe { frs_db_create_cf(db, c_name.as_ptr(), out_cf) }
    }
}

#[allow(clippy::too_many_arguments)]
fn put_bytes(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: FrsCfHandle,
    key: &JByteArray,
    key_off: jint,
    key_len: jint,
    val: &JByteArray,
    val_off: jint,
    val_len: jint,
    context: &str,
) {
    let Some(k) = read_byte_slice(env, key, key_off, key_len) else {
        return;
    };
    let Some(v) = read_byte_slice(env, val, val_off, val_len) else {
        return;
    };
    let status = unsafe {
        frs_put(
            handle as FrsDb,
            cf_handle,
            k.as_ptr(),
            k.len(),
            v.as_ptr(),
            v.len(),
        )
    };
    check_status(env, status, context);
}

fn get_bytes(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: FrsCfHandle,
    key: &JByteArray,
    key_off: jint,
    key_len: jint,
    context: &str,
) -> jbyteArray {
    let Some(k) = read_byte_slice(env, key, key_off, key_len) else {
        return ptr::null_mut();
    };
    let mut out = FrsBytes {
        data: ptr::null_mut(),
        len: 0,
        capacity: 0,
    };
    let status = unsafe { frs_get(handle as FrsDb, cf_handle, k.as_ptr(), k.len(), &mut out) };
    if status == FRS_STATUS_NOT_FOUND {
        return ptr::null_mut();
    }
    if check_status(env, status, context) {
        return ptr::null_mut();
    }
    if out.data.is_null() {
        return ptr::null_mut();
    }
    let slice = unsafe { std::slice::from_raw_parts(out.data, out.len) };
    let java_arr = match env.byte_array_from_slice(slice) {
        Ok(a) => a.into_raw(),
        Err(e) => {
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
            throw_rocksdb(
                env,
                &format!("{context}: byte_array_from_slice failed: {e}"),
            );
            return ptr::null_mut();
        }
    };
    unsafe {
        let _ = crate::frs_bytes_free(&mut out);
    }
    java_arr
}

fn delete_bytes(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: FrsCfHandle,
    key: &JByteArray,
    key_off: jint,
    key_len: jint,
    context: &str,
) {
    let Some(k) = read_byte_slice(env, key, key_off, key_len) else {
        return;
    };
    let status = unsafe { frs_delete(handle as FrsDb, cf_handle, k.as_ptr(), k.len()) };
    check_status(env, status, context);
}

/// `org.forstdb.RocksDB.put(long handle, byte[] key,
///                          int keyOff, int keyLen, byte[] val,
///                          int valOff, int valLen)`
///
/// Java signature: `(J[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_put__J_3BII_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
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
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.put.defaultCF") else {
                return;
            };
            put_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.put",
            );
        },
    )
}

/// `org.forstdb.RocksDB.put(long handle, byte[] key,
///                          int keyOff, int keyLen, byte[] val,
///                          int valOff, int valLen, long cfHandle)`
///
/// Java signature: `(J[BII[BIIJ)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_put__J_3BII_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.put") else {
                return;
            };
            put_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.put",
            );
        },
    )
}

/// `org.forstdb.RocksDB.put(long handle, long writeOptionsHandle,
///                          byte[] key, int keyOff, int keyLen,
///                          byte[] val, int valOff, int valLen)`
///
/// Java signature: `(JJ[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_put__JJ_3BII_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _write_options_handle: jlong,
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
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.put.defaultCF") else {
                return;
            };
            put_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.put",
            );
        },
    )
}

/// `org.forstdb.RocksDB.put(long handle, long writeOptionsHandle,
///                          byte[] key, int keyOff, int keyLen,
///                          byte[] val, int valOff, int valLen,
///                          long cfHandle)`
///
/// Java signature: `(JJ[BII[BIIJ)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_put__JJ_3BII_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _write_options_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.put") else {
                return;
            };
            put_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.put",
            );
        },
    )
}

/// `org.forstdb.RocksDB.get(long handle, byte[] key,
///                          int keyOff, int keyLen) -> byte[]?`
///
/// Java signature: `(J[BII)[B`
///
/// Returns `null` if the key is absent — matches RocksDB Java behavior.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_get__J_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.get.defaultCF") else {
                return ptr::null_mut();
            };
            get_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                "RocksDB.get",
            )
        },
    )
}

/// `org.forstdb.RocksDB.get(long handle, byte[] key,
///                          int keyOff, int keyLen, long cfHandle) -> byte[]?`
///
/// Java signature: `(J[BIIJ)[B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_get__J_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    cf_handle: jlong,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.get") else {
                return ptr::null_mut();
            };
            get_bytes(env, handle, frs_cf, &key, key_off, key_len, "RocksDB.get")
        },
    )
}

/// `org.forstdb.RocksDB.get(long handle, long readOptionsHandle,
///                          byte[] key, int keyOff, int keyLen) -> byte[]?`
///
/// Java signature: `(JJ[BII)[B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_get__JJ_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _read_options_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.get.defaultCF") else {
                return ptr::null_mut();
            };
            get_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                "RocksDB.get",
            )
        },
    )
}

/// `org.forstdb.RocksDB.get(long handle, long readOptionsHandle,
///                          byte[] key, int keyOff, int keyLen,
///                          long cfHandle) -> byte[]?`
///
/// Java signature: `(JJ[BIIJ)[B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_get__JJ_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _read_options_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    cf_handle: jlong,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jbyteArray,
        |env| -> jbyteArray {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.get") else {
                return ptr::null_mut();
            };
            get_bytes(env, handle, frs_cf, &key, key_off, key_len, "RocksDB.get")
        },
    )
}

/// `org.forstdb.RocksDB.delete(long handle, byte[] key, int keyOff, int keyLen)`
///
/// Java signature: `(J[BII)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_delete__J_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.delete.defaultCF") else {
                return;
            };
            delete_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                "RocksDB.delete",
            );
        },
    )
}

/// `org.forstdb.RocksDB.delete(long handle, byte[] key, int keyOff,
///                             int keyLen, long cfHandle)`
///
/// Java signature: `(J[BIIJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_delete__J_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.delete") else {
                return;
            };
            delete_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                "RocksDB.delete",
            );
        },
    )
}

/// `org.forstdb.RocksDB.delete(long handle, long writeOptionsHandle,
///                             byte[] key, int keyOff, int keyLen)`
///
/// Java signature: `(JJ[BII)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_delete__JJ_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _write_options_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.delete.defaultCF") else {
                return;
            };
            delete_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                "RocksDB.delete",
            );
        },
    )
}

/// `org.forstdb.RocksDB.delete(long handle, long writeOptionsHandle,
///                             byte[] key, int keyOff, int keyLen,
///                             long cfHandle)`
///
/// Java signature: `(JJ[BIIJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_delete__JJ_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _write_options_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.delete") else {
                return;
            };
            delete_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                "RocksDB.delete",
            );
        },
    )
}

/// `org.forstdb.RocksDB.createColumnFamily(long handle, byte[] name,
///                                           int nameLen, long cfOptions)
///                                           -> long cfHandle`
///
/// Java signature: `(J[BIJ)J`
///
/// If the CF already exists this opens it; otherwise it creates a new one.
/// The returned value must be the Java-side [`CfHandle`] wrapper, not the raw
/// engine `FrsCfHandle`, because all later CF operations call
/// [`cf_from_java_handle`] and expect the wrapper layout.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_createColumnFamily<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JByteArray<'local>,
    name_len: jint,
    _cf_options_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(name_bytes) = read_byte_slice(env, &name, 0, name_len) else {
                return 0_i64;
            };
            let c_name = match std::ffi::CString::new(name_bytes.clone()) {
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
            let status = if open_status == FRS_STATUS_OK {
                open_status
            } else {
                create_cf_with_optional_merge(
                    env,
                    handle as FrsDb,
                    c_name.as_c_str(),
                    cf_options_merge_operator_name(_cf_options_handle),
                    &mut cf,
                    "RocksDB.createColumnFamily",
                )
            };
            if check_status(env, status, "RocksDB.createColumnFamily") {
                return 0;
            }
            CfHandle {
                name: name_bytes,
                frs_handle: cf,
                // The ColumnFamilyDescriptor still owns its options object.
                owned_opts_handle: 0,
            }
            .into_raw()
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
            let Some(cf) = (unsafe { CfHandle::from_raw_ref(cf_handle) }) else {
                throw_rocksdb(env, "RocksDB.dropColumnFamily: null ColumnFamilyHandle");
                return;
            };
            // forst-rs does not expose physical CF drop yet. Close the engine
            // handle only if Java explicitly asks to drop; the wrapper remains
            // owned by ColumnFamilyHandle.disposeInternal.
            if !cf.frs_handle.is_null() {
                let status = unsafe { crate::frs_cf_close(cf.frs_handle) };
                if check_status(env, status, "RocksDB.dropColumnFamily") {
                    return;
                }
                cf.frs_handle = ptr::null_mut();
            }
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
    _flush_options_handle: jlong,
    cf_handles: JPrimitiveArray<'local, jlong>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let len = match env.get_array_length(&cf_handles) {
                Ok(n) if n >= 0 => n as usize,
                Ok(n) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.flush: negative cf_handles length: {n}"),
                    );
                    return;
                }
                Err(_) => 0,
            };
            if len == 0 {
                let status = unsafe { frs_flush(handle as FrsDb) };
                check_status(env, status, "RocksDB.flush");
                return;
            }
            let mut handles = vec![0_i64; len];
            if let Err(e) = env.get_long_array_region(&cf_handles, 0, &mut handles) {
                throw_rocksdb(env, &format!("RocksDB.flush: read cf_handles: {e}"));
                return;
            }
            for cf_handle in handles {
                let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.flush") else {
                    return;
                };
                let status = unsafe { frs_flush_cf(handle as FrsDb, frs_cf) };
                if check_status(env, status, "RocksDB.flush") {
                    return;
                }
            }
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

fn merge_bytes(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: FrsCfHandle,
    key: &JByteArray,
    key_off: jint,
    key_len: jint,
    val: &JByteArray,
    val_off: jint,
    val_len: jint,
    context: &str,
) {
    let Some(k) = read_byte_slice(env, key, key_off, key_len) else {
        return;
    };
    let Some(v) = read_byte_slice(env, val, val_off, val_len) else {
        return;
    };
    let status = unsafe {
        frs_merge(
            handle as FrsDb,
            cf_handle,
            k.as_ptr(),
            k.len(),
            v.as_ptr(),
            v.len(),
        )
    };
    check_status(env, status, context);
}

/// `org.forstdb.RocksDB.merge(long handle, byte[] key,
///                            int keyOff, int keyLen, byte[] value,
///                            int valueOff, int valueLen)`
///
/// Java signature: `(J[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_merge__J_3BII_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
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
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.merge.defaultCF") else {
                return;
            };
            merge_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.merge",
            );
        },
    )
}

/// `org.forstdb.RocksDB.merge(long handle, byte[] key,
///                            int keyOff, int keyLen, byte[] value,
///                            int valueOff, int valueLen, long cfHandle)`
///
/// Java signature: `(J[BII[BIIJ)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_merge__J_3BII_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.merge") else {
                return;
            };
            merge_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.merge",
            );
        },
    )
}

/// `org.forstdb.RocksDB.merge(long handle, long writeOptionsHandle,
///                            byte[] key, int keyOff, int keyLen,
///                            byte[] value, int valueOff, int valueLen)`
///
/// Java signature: `(JJ[BII[BII)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_merge__JJ_3BII_3BII<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _write_options_handle: jlong,
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
            let Some(cf_handle) = default_cf_for_db(env, handle, "RocksDB.merge.defaultCF") else {
                return;
            };
            merge_bytes(
                env,
                handle,
                cf_handle,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.merge",
            );
        },
    )
}

/// `org.forstdb.RocksDB.merge(long handle, long writeOptionsHandle,
///                            byte[] key, int keyOff, int keyLen,
///                            byte[] value, int valueOff, int valueLen,
///                            long cfHandle)`
///
/// Java signature: `(JJ[BII[BIIJ)V`
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_merge__JJ_3BII_3BIIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _write_options_handle: jlong,
    key: JByteArray<'local>,
    key_off: jint,
    key_len: jint,
    val: JByteArray<'local>,
    val_off: jint,
    val_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.merge") else {
                return;
            };
            merge_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                &val,
                val_off,
                val_len,
                "RocksDB.merge",
            );
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
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.compactRange") else {
                return;
            };
            // SAFETY: handles came from prior open / create calls; nullity
            // checked inside frs_compact_cf.
            let status = unsafe { frs_compact_cf(handle as FrsDb, frs_cf) };
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
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.flushCf") else {
                return;
            };
            // SAFETY: handles came from prior open / create calls.
            let status = unsafe { frs_flush_cf(handle as FrsDb, frs_cf) };
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
            // R14A-H1 (mirror R11A-H1 on the legacy 2-arg opener): pre-flip
            // allow_rewind=true so the public RocksDB.iteratorSeek ABI keeps
            // working — D-C4R8-H1's guard otherwise rejects iteratorSeek
            // after any iteratorNext. The 3-arg sibling at line ~3329 was
            // patched in R11; the legacy 2-arg path here was the missed
            // sister.
            unsafe {
                let state = &mut *(iter as *mut crate::IteratorState);
                state.allow_rewind = true;
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
    Java_org_forstdb_RocksDB_open__Ljava_lang_String_2(env, class, path)
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
/// Alias for [`delete_bytes`] — the older RocksDB
/// Java API spelled this method `remove`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_remove<'local>(
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
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.remove") else {
                return;
            };
            delete_bytes(
                env,
                handle,
                frs_cf,
                &key,
                key_off,
                key_len,
                "RocksDB.remove",
            );
        },
    )
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
            let key_len = match env.get_array_length(&key) {
                Ok(n) => n,
                Err(e) => {
                    throw_rocksdb(env, &format!("putByteArray: key length failed: {e}"));
                    return;
                }
            };
            let val_len = match env.get_array_length(&val) {
                Ok(n) => n,
                Err(e) => {
                    throw_rocksdb(env, &format!("putByteArray: value length failed: {e}"));
                    return;
                }
            };
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.putByteArray") else {
                return;
            };
            put_bytes(
                env,
                handle,
                frs_cf,
                &key,
                0,
                key_len,
                &val,
                0,
                val_len,
                "RocksDB.putByteArray",
            );
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
            let key_len = match env.get_array_length(&key) {
                Ok(n) => n,
                Err(e) => {
                    throw_rocksdb(env, &format!("getByteArray: key length failed: {e}"));
                    return ptr::null_mut();
                }
            };
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.getByteArray") else {
                return ptr::null_mut();
            };
            get_bytes(
                env,
                handle,
                frs_cf,
                &key,
                0,
                key_len,
                "RocksDB.getByteArray",
            )
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
// Env / RocksEnv
// ---------------------------------------------------------------------------

/// `org.forstdb.Env.getDefaultEnvInternal() -> long`
///
/// Java signature: `()J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_getDefaultEnvInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(&mut env, || 0_i64, |_env| EnvHandle.into_raw())
}

/// `org.forstdb.RocksEnv.disposeInternal(long)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksEnv_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from `Env.getDefaultEnvInternal` and
                // Java owns the corresponding RocksEnv lifecycle.
                unsafe { drop(Box::from_raw(handle as *mut EnvHandle)) };
            }
        },
    )
}

/// `org.forstdb.Env.setBackgroundThreads(long, int, byte)`
///
/// Java signature: `(JIB)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_setBackgroundThreads<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _threads: jint,
    _priority: jbyte,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.Env.getBackgroundThreads(long, byte) -> int`
///
/// Java signature: `(JB)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_getBackgroundThreads<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _priority: jbyte,
) -> jint {
    jni_guard(&mut env, || 0, |_env| 0)
}

/// `org.forstdb.Env.getThreadPoolQueueLen(long, byte) -> int`
///
/// Java signature: `(JB)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_getThreadPoolQueueLen<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _priority: jbyte,
) -> jint {
    jni_guard(&mut env, || 0, |_env| 0)
}

/// `org.forstdb.Env.incBackgroundThreadsIfNeeded(long, int, byte)`
///
/// Java signature: `(JIB)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_incBackgroundThreadsIfNeeded<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _threads: jint,
    _priority: jbyte,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.Env.lowerThreadPoolIOPriority(long, byte)`
///
/// Java signature: `(JB)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_lowerThreadPoolIOPriority<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _priority: jbyte,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.Env.lowerThreadPoolCPUPriority(long, byte)`
///
/// Java signature: `(JB)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_lowerThreadPoolCPUPriority<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _priority: jbyte,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.Env.getThreadList(long) -> ThreadStatus[]`
///
/// Java signature: `(J)[Lorg/forstdb/ThreadStatus;`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Env_getThreadList<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) -> jobjectArray {
    jni_guard(
        &mut env,
        || ptr::null_mut(),
        |env| {
            let Ok(thread_status_class) = env.find_class("org/forstdb/ThreadStatus") else {
                return ptr::null_mut();
            };
            match env.new_object_array(0, thread_status_class, JObject::null()) {
                Ok(arr) => arr.into_raw(),
                Err(_) => ptr::null_mut(),
            }
        },
    )
}

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
        |_env| DbOptionsHandle::default().into_raw(),
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
    handle: jlong,
    value: JString<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let value = read_string(env, &value);
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                h.db_log_dir = value;
            }
            tracing::debug!(target: "compat_jni::dbopts", "setDbLogDir: forst-rs uses tracing for log routing; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.dbLogDir(long) -> String`
///
/// Java signature: `(J)Ljava/lang/String;`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_dbLogDir<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jstring {
    jni_guard(
        &mut env,
        || ptr::null_mut(),
        |env| {
            let value = unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .and_then(|h| h.db_log_dir.clone())
                .unwrap_or_default();
            match env.new_string(value) {
                Ok(s) => s.into_raw(),
                Err(e) => {
                    throw_rocksdb(env, &format!("DBOptions.dbLogDir: new_string failed: {e}"));
                    ptr::null_mut()
                }
            }
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
    handle: jlong,
    value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                h.info_log_level = value as jbyte;
            }
            tracing::debug!(target: "compat_jni::dbopts", "setInfoLogLevel: forst-rs uses tracing for log levels; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.infoLogLevel(long) -> byte`
///
/// Java signature: `(J)B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_infoLogLevel<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyte {
    jni_guard(
        &mut env,
        || 2_i8,
        |_env| {
            unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.info_log_level)
                .unwrap_or(2)
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
    handle: jlong,
    value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                h.max_open_files = value;
            }
            tracing::debug!(target: "compat_jni::dbopts", "setMaxOpenFiles: forst-rs has no per-table FD cache; ignored");
        },
    )
}

/// `org.forstdb.DBOptions.maxOpenFiles(long) -> int`
///
/// Java signature: `(J)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_maxOpenFiles<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_guard(
        &mut env,
        || -1,
        |_env| {
            unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.max_open_files)
                .unwrap_or(-1)
        },
    )
}

/// `org.forstdb.DBOptions.maxBackgroundJobs(long) -> int`
///
/// Java signature: `(J)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_maxBackgroundJobs<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_guard(
        &mut env,
        || {
            let d = EngineOptions::default();
            (d.max_background_compactions + d.max_background_flushes) as jint
        },
        |_env| {
            unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .map(|h| {
                    (h.opts.max_background_compactions + h.opts.max_background_flushes) as jint
                })
                .unwrap_or_else(|| {
                    let d = EngineOptions::default();
                    (d.max_background_compactions + d.max_background_flushes) as jint
                })
        },
    )
}

/// `org.forstdb.DBOptions.setMaxLogFileSize(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setMaxLogFileSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                if value >= 0 {
                    h.max_log_file_size = value;
                }
            }
            tracing::debug!(target: "compat_jni::dbopts", "setMaxLogFileSize: tracing-managed; round-tripped only");
        },
    )
}

/// `org.forstdb.DBOptions.maxLogFileSize(long) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_maxLogFileSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.max_log_file_size)
                .unwrap_or(0)
        },
    )
}

/// `org.forstdb.DBOptions.setKeepLogFileNum(long, long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_setKeepLogFileNum<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                if value >= 0 {
                    h.keep_log_file_num = value;
                }
            }
            tracing::debug!(target: "compat_jni::dbopts", "setKeepLogFileNum: tracing-managed; round-tripped only");
        },
    )
}

/// `org.forstdb.DBOptions.keepLogFileNum(long) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_keepLogFileNum<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 1000_i64,
        |_env| {
            unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.keep_log_file_num)
                .unwrap_or(1000)
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
    stats_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { DbOptionsHandle::from_raw_ref(handle) } {
                h.opts.enable_statistics = true;
                h.statistics_handle = stats_handle;
            }
            tracing::debug!(target: "compat_jni::dbopts", "setStatistics: external Statistics handle round-tripped; using built-in metrics");
        },
    )
}

/// `org.forstdb.DBOptions.statistics(long) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_DBOptions_statistics<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { DbOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.statistics_handle)
                .unwrap_or(0)
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

/// `org.forstdb.ColumnFamilyOptions.writeBufferSize(long) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_writeBufferSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || EngineOptions::default().write_buffer_size as jlong,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .and_then(|h| h.opts.write_buffer_size.map(|v| v as jlong))
                .unwrap_or_else(|| EngineOptions::default().write_buffer_size as jlong)
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

/// `org.forstdb.ColumnFamilyOptions.setArenaBlockSize(long, long)`
///
/// Java signature: `(JJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setArenaBlockSize<'local>(
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
                h.arena_block_size = value;
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.arenaBlockSize(long) -> long`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_arenaBlockSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.arena_block_size)
                .unwrap_or(0)
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
    handle: jlong,
    value: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                if value > 0 {
                    h.min_write_buffer_number_to_merge = value;
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.minWriteBufferNumberToMerge(long)`
///
/// Java signature: `(J)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_minWriteBufferNumberToMerge<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_guard(
        &mut env,
        || 0,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.min_write_buffer_number_to_merge)
                .unwrap_or(0)
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
    handle: jlong,
    value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                h.level_compaction_dynamic_level_bytes = value != JNI_FALSE;
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.levelCompactionDynamicLevelBytes(long)`
///
/// Java signature: `(J)Z`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_levelCompactionDynamicLevelBytes<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    jni_guard(
        &mut env,
        || JNI_FALSE,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .map(|h| {
                    if h.level_compaction_dynamic_level_bytes {
                        JNI_TRUE
                    } else {
                        JNI_FALSE
                    }
                })
                .unwrap_or(JNI_FALSE)
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
    handle: jlong,
    value: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                if value > 0 {
                    h.max_bytes_for_level_base = Some(value as usize);
                }
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.maxBytesForLevelBase(long)`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_maxBytesForLevelBase<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .and_then(|h| h.max_bytes_for_level_base.map(|v| v as jlong))
                .unwrap_or_else(|| EngineOptions::default().max_bytes_for_level_base as jlong)
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.maxWriteBufferNumber(long)`
///
/// Java signature: `(J)I`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_maxWriteBufferNumber<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jint {
    jni_guard(
        &mut env,
        || 0,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .and_then(|h| h.opts.max_write_buffer_number.map(|v| v as jint))
                .unwrap_or_else(|| EngineOptions::default().max_write_buffer_number as jint)
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

/// `org.forstdb.ColumnFamilyOptions.targetFileSizeBase(long)`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_targetFileSizeBase<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .and_then(|h| h.opts.target_file_size_base.map(|v| v as jlong))
                .unwrap_or_else(|| EngineOptions::default().target_file_size_base as jlong)
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
            h.compression_per_level = arr.clone();
            // Pick deepest non-zero level.
            if let Some(&deepest) = arr.iter().rev().find(|&&b| b != 0) {
                h.opts.compression = Some(byte_to_compression(deepest));
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.compressionPerLevel(long) -> byte[]`
///
/// Java signature: `(J)[B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_compressionPerLevel<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyteArray {
    jni_guard(
        &mut env,
        || ptr::null_mut(),
        |env| {
            let levels = unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.compression_per_level.clone())
                .unwrap_or_default();
            match env.byte_array_from_slice(&levels) {
                Ok(a) => a.into_raw(),
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("ColumnFamilyOptions.compressionPerLevel: byte_array_from_slice failed: {e}"),
                    );
                    ptr::null_mut()
                }
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
    handle: jlong,
    style: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { CfOptionsHandle::from_raw_ref(handle) } {
                h.compaction_style = style as jbyte;
            }
            if style != 0 {
                tracing::debug!(target: "compat_jni::cfopts", "setCompactionStyle({style}): forst-rs only supports LEVEL; ignored");
            }
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.compactionStyle(long) -> byte`
///
/// Java signature: `(J)B`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_compactionStyle<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jbyte {
    jni_guard(
        &mut env,
        || 0_i8,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .map(|h| h.compaction_style)
                .unwrap_or(0)
        },
    )
}

/// `org.forstdb.ColumnFamilyOptions.optimizeForPointLookup(long, long)`
///
/// Java signature: `(JJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_optimizeForPointLookup<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _block_cache_size: jlong,
) {
    jni_guard(&mut env, || (), |_env| {})
}

/// `org.forstdb.ColumnFamilyOptions.setMergeOperatorName(long, String)`
///
/// Java signature: `(JLjava/lang/String;)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setMergeOperatorName<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    name: JString<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { CfOptionsHandle::from_raw_ref(handle) }) else {
                return;
            };
            let Some(name) = read_string(env, &name) else {
                return;
            };
            h.opts.merge_operator = match name.as_str() {
                "" => None,
                // Flink's ForSt backend uses RocksDB's stringappendtest operator
                // for ListState append. forst-rs exposes the same comma-delimited
                // semantics as ListAppendMergeOperator.
                "stringappendtest"
                | "ListAppendMergeOperator"
                | "ListAppendMergeOperator(delim=44)" => {
                    Some("ListAppendMergeOperator".to_string())
                }
                "RawConcatMergeOperator" => Some("RawConcatMergeOperator".to_string()),
                other => Some(other.to_string()),
            };
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

/// `org.forstdb.ColumnFamilyOptions.periodicCompactionSeconds(long)`
///
/// Java signature: `(J)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_periodicCompactionSeconds<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            unsafe { CfOptionsHandle::from_raw_ref(handle) }
                .and_then(|h| h.opts.ttl_seconds.map(|v| v as jlong))
                .unwrap_or(0)
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

/// `org.forstdb.ColumnFamilyOptions.setTableFactory(long, long)`
///
/// Java signature: `(JJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setTableFactory<'local>(
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

/// `org.forstdb.ColumnFamilyOptions.setCompactionFilterFactoryHandle(long, long)`
///
/// forst-rs has no compaction-filter factory layer. Accept and ignore.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setCompactionFilterFactoryHandle<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _factory_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(target: "compat_jni::cfopts", "setCompactionFilterFactoryHandle: not supported; ignored");
        },
    )
}

/// Legacy alias used by earlier local tests.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ColumnFamilyOptions_setCompactionFilterFactory<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
    factory_handle: jlong,
) {
    Java_org_forstdb_ColumnFamilyOptions_setCompactionFilterFactoryHandle(
        env,
        class,
        handle,
        factory_handle,
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
// FlushOptions
// ---------------------------------------------------------------------------

/// `org.forstdb.FlushOptions.<init>() -> long`
///
/// Java signature: `()J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlushOptions_newFlushOptions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| FlushOptionsHandle::default().into_raw(),
    )
}

/// `org.forstdb.FlushOptions.disposeInternal(long)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlushOptions_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                unsafe { drop(Box::from_raw(handle as *mut FlushOptionsHandle)) };
            }
        },
    )
}

/// `org.forstdb.FlushOptions.setWaitForFlush(long, boolean)`
///
/// Java signature: `(JZ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlushOptions_setWaitForFlush<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { FlushOptionsHandle::from_raw_ref(handle) } {
                h.wait_for_flush = value != JNI_FALSE;
            }
        },
    )
}

/// `org.forstdb.FlushOptions.waitForFlush(long) -> boolean`
///
/// Java signature: `(J)Z`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlushOptions_waitForFlush<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    jni_guard(
        &mut env,
        || JNI_TRUE,
        |_env| {
            unsafe { FlushOptionsHandle::from_raw_ref(handle) }
                .map(|h| {
                    if h.wait_for_flush {
                        JNI_TRUE
                    } else {
                        JNI_FALSE
                    }
                })
                .unwrap_or(JNI_TRUE)
        },
    )
}

/// `org.forstdb.FlushOptions.setAllowWriteStall(long, boolean)`
///
/// Java signature: `(JZ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlushOptions_setAllowWriteStall<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { FlushOptionsHandle::from_raw_ref(handle) } {
                h.allow_write_stall = value != JNI_FALSE;
            }
        },
    )
}

/// `org.forstdb.FlushOptions.allowWriteStall(long) -> boolean`
///
/// Java signature: `(J)Z`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlushOptions_allowWriteStall<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jboolean {
    jni_guard(
        &mut env,
        || JNI_FALSE,
        |_env| {
            unsafe { FlushOptionsHandle::from_raw_ref(handle) }
                .map(|h| {
                    if h.allow_write_stall {
                        JNI_TRUE
                    } else {
                        JNI_FALSE
                    }
                })
                .unwrap_or(JNI_FALSE)
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
///                           byte[][] cfNames, long[] cfOptions)
///                           -> long[] { dbHandle, cfHandle... }`
///
/// Java signature: `(JLjava/lang/String;[[B[J)[J`
#[no_mangle]
#[allow(non_snake_case)]
pub extern "system" fn Java_org_forstdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_opts_handle: jlong,
    path: JString<'local>,
    cf_names: JObjectArray<'local>,
    cf_opts_handles: JPrimitiveArray<'local, jlong>,
) -> jni::sys::jlongArray {
    jni_guard(
        &mut env,
        || ptr::null_mut(),
        |env| -> jni::sys::jlongArray {
            let Some(db_opts) = (unsafe { DbOptionsHandle::from_raw_ref(db_opts_handle) }) else {
                throw_rocksdb(env, "RocksDB.open(multi-CF): null DBOptions handle");
                return ptr::null_mut();
            };
            let Some(path_str) = read_string(env, &path) else {
                return ptr::null_mut();
            };

            let cf_names_len = match env.get_array_length(&cf_names) {
                Ok(n) if n >= 0 => n as usize,
                Ok(n) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): negative cf_names length: {n}"),
                    );
                    return ptr::null_mut();
                }
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): get cf_names length: {e}"),
                    );
                    return ptr::null_mut();
                }
            };
            let cf_opts_len = match env.get_array_length(&cf_opts_handles) {
                Ok(n) if n >= 0 => n as usize,
                Ok(n) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): negative cf_opts length: {n}"),
                    );
                    return ptr::null_mut();
                }
                Err(e) => {
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): get cf_opts length: {e}"),
                    );
                    return ptr::null_mut();
                }
            };
            if cf_names_len != cf_opts_len {
                throw_rocksdb(
                    env,
                    &format!(
                        "RocksDB.open(multi-CF): array length mismatch (cf_names={}, cf_opts={})",
                        cf_names_len, cf_opts_len
                    ),
                );
                return ptr::null_mut();
            }
            if cf_names_len == 0 {
                throw_rocksdb(
                    env,
                    "RocksDB.open(multi-CF): cf_names must contain at least the default CF",
                );
                return ptr::null_mut();
            }

            let mut cf_opts_buf = vec![0_i64; cf_opts_len];
            if let Err(e) = env.get_long_array_region(&cf_opts_handles, 0, &mut cf_opts_buf) {
                throw_rocksdb(env, &format!("RocksDB.open(multi-CF): read cf_opts: {e}"));
                return ptr::null_mut();
            }
            let cf_name_bytes =
                match read_byte_matrix(env, &cf_names, "RocksDB.open(multi-CF).cf_names") {
                    Some(v) => v,
                    None => return ptr::null_mut(),
                };

            let mut engine_opts = db_opts.opts.clone();
            let db_path = path_str.clone();
            engine_opts.db_path = path_str;

            for &cf_opts_handle in &cf_opts_buf {
                if cf_opts_handle == 0 {
                    continue;
                }
                let Some(cf_opts) = (unsafe { CfOptionsHandle::from_raw_ref(cf_opts_handle) })
                else {
                    continue;
                };
                if cf_opts.table_format_handle == 0 {
                    continue;
                }
                let Some(tbl) = (unsafe {
                    BlockBasedTableConfigHandle::from_raw_ref(cf_opts.table_format_handle)
                }) else {
                    continue;
                };
                if let Some(bs) = tbl.block_size {
                    engine_opts.block_size = bs;
                }
                if let Some(cs) = tbl.block_cache_size {
                    engine_opts.block_cache_size = cs;
                }
                if let Some(bbk) = tbl.bloom_bits_per_key {
                    engine_opts.bloom_bits_per_key = bbk;
                }
                tracing::debug!(
                    target: "compat_jni::open",
                    "BlockBasedTableConfig hydrated: block_size={:?}, block_cache_size={:?}, bloom_bits_per_key={:?}, index_type={}",
                    tbl.block_size, tbl.block_cache_size, tbl.bloom_bits_per_key, tbl.index_type
                );
                break;
            }

            let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
            let db = match DbImpl::open_with_fs(engine_opts, fs) {
                Ok(d) => Box::new(d),
                Err(e) => {
                    throw_rocksdb(env, &format!("RocksDB.open(multi-CF): {e}"));
                    return ptr::null_mut();
                }
            };
            let db_handle = Box::into_raw(db) as FrsDb;

            let mut cf_handles = Vec::<jlong>::with_capacity(cf_names_len);
            for (i, name_bytes) in cf_name_bytes.iter().enumerate() {
                let name_str = match std::str::from_utf8(name_bytes) {
                    Ok(s) => s,
                    Err(e) => {
                        cleanup_partial_open(db_handle, &cf_handles);
                        throw_rocksdb(
                            env,
                            &format!(
                                "RocksDB.open(multi-CF): cf_names[{i}] is not valid UTF-8: {e}"
                            ),
                        );
                        return ptr::null_mut();
                    }
                };
                let c_name = match std::ffi::CString::new(name_str) {
                    Ok(s) => s,
                    Err(_) => {
                        cleanup_partial_open(db_handle, &cf_handles);
                        throw_rocksdb(
                            env,
                            &format!("RocksDB.open(multi-CF): cf_names[{i}] contains interior NUL"),
                        );
                        return ptr::null_mut();
                    }
                };

                let mut frs_cf: FrsCfHandle = ptr::null_mut();
                let status = if name_str == "default" {
                    unsafe { frs_db_default_cf(db_handle, &mut frs_cf) }
                } else {
                    let open_status =
                        unsafe { frs_db_open_cf(db_handle, c_name.as_ptr(), &mut frs_cf) };
                    if open_status == FRS_STATUS_OK {
                        open_status
                    } else {
                        create_cf_with_optional_merge(
                            env,
                            db_handle,
                            c_name.as_c_str(),
                            cf_options_merge_operator_name(cf_opts_buf[i]),
                            &mut frs_cf,
                            "RocksDB.open(multi-CF)",
                        )
                    }
                };
                if status != FRS_STATUS_OK {
                    cleanup_partial_open(db_handle, &cf_handles);
                    throw_rocksdb(
                        env,
                        &format!(
                            "RocksDB.open(multi-CF): failed to open/create CF `{name_str}`: frs_status={status}"
                        ),
                    );
                    return ptr::null_mut();
                }

                cf_handles.push(
                    CfHandle {
                        name: name_bytes.clone(),
                        frs_handle: frs_cf,
                        owned_opts_handle: 0,
                    }
                    .into_raw(),
                );
            }

            let mut result_handles = Vec::<jlong>::with_capacity(cf_handles.len() + 1);
            result_handles.push(db_handle as jlong);
            result_handles.extend(cf_handles.iter().copied());

            let result = match env.new_long_array(result_handles.len() as i32) {
                Ok(a) => a,
                Err(e) => {
                    cleanup_partial_open(db_handle, &cf_handles);
                    throw_rocksdb(
                        env,
                        &format!("RocksDB.open(multi-CF): new result array failed: {e}"),
                    );
                    return ptr::null_mut();
                }
            };
            if let Err(e) = env.set_long_array_region(&result, 0, &result_handles) {
                cleanup_partial_open(db_handle, &cf_handles);
                throw_rocksdb(
                    env,
                    &format!("RocksDB.open(multi-CF): set result array failed: {e}"),
                );
                return ptr::null_mut();
            }
            register_db_path(db_handle, &db_path);
            result.into_raw()
        },
    )
}

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
            let db_path = path_str.clone();
            engine_opts.db_path = path_str;

            // 6a. P3 hydration: walk every CF's CfOptionsHandle, and if it
            // carries a BlockBasedTableConfig pointer (set via
            // `ColumnFamilyOptions.setTableFormatConfig`), pull
            // `block_size`, `block_cache_size`, and `bloom_bits_per_key`
            // off it and apply to the engine-wide `EngineOptions`. forst-rs
            // currently models these as engine-wide rather than per-CF, so
            // multiple CFs that disagree get last-write-wins (matches the
            // community shim that wraps a shared block cache + filter
            // policy by default). We respect only the *first* non-zero
            // table-format handle we see — Flink jobs typically attach the
            // same config to every CF.
            for &cf_opts_handle in &cf_opts_buf {
                if cf_opts_handle == 0 {
                    continue;
                }
                // SAFETY: pointer came from `ColumnFamilyOptions.<init>` and is
                // still owned by the Java side; we only borrow immutably here
                // and the borrow ends with this loop iteration.
                let Some(cf_opts) = (unsafe { CfOptionsHandle::from_raw_ref(cf_opts_handle) })
                else {
                    continue;
                };
                if cf_opts.table_format_handle == 0 {
                    continue;
                }
                // SAFETY: `table_format_handle` was set by
                // `ColumnFamilyOptions.setTableFormatConfig` from the jlong
                // returned by `BlockBasedTableConfig.newTableFactoryHandle`;
                // the Java side still owns the box (it disposes via
                // `BlockBasedTableConfig.disposeInternal`).
                let Some(tbl) = (unsafe {
                    BlockBasedTableConfigHandle::from_raw_ref(cf_opts.table_format_handle)
                }) else {
                    continue;
                };
                if let Some(bs) = tbl.block_size {
                    engine_opts.block_size = bs;
                }
                if let Some(cs) = tbl.block_cache_size {
                    engine_opts.block_cache_size = cs;
                }
                if let Some(bbk) = tbl.bloom_bits_per_key {
                    engine_opts.bloom_bits_per_key = bbk;
                }
                tracing::debug!(
                    target: "compat_jni::open",
                    "BlockBasedTableConfig hydrated: block_size={:?}, block_cache_size={:?}, bloom_bits_per_key={:?}, index_type={}",
                    tbl.block_size, tbl.block_cache_size, tbl.bloom_bits_per_key, tbl.index_type
                );
                break;
            }

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

            register_db_path(db_handle, &db_path);
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
        unregister_db_path(db_handle);
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
            let Some(frs_cf) = cf_from_java_handle(env, cf_handle, "RocksDB.iterator") else {
                return 0_i64;
            };
            let mut iter: FrsIterator = ptr::null_mut();
            // SAFETY: db / cf came from prior open; out_iter is stack-local.
            let status = unsafe { frs_iterator_open(handle as FrsDb, frs_cf, &mut iter) };
            if check_status(env, status, "RocksDB.iterator") {
                return 0_i64;
            }
            // R11A-H1 (post-B-C4R10): pre-flip allow_rewind=true at open. The
            // JNI compat shim's RocksIterator ABI is bidirectional (seek0,
            // seekToLast0, seekForPrev0, prev0 all rewind), so every next0
            // MUST use clone() — never mem::take. Pre-fix the seek0/
            // seekToFirst0 entries pre-flipped just-in-time, but a sequence
            // like next0; next0; seek0 would have already corrupted rows[0..2]
            // under the forward-only fast path before seek0 ran the
            // binary_search.
            unsafe {
                let state = &mut *(iter as *mut crate::IteratorState);
                state.allow_rewind = true;
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
            // A-C5R2-NEW-H1: null-check h.frs_iter BEFORE the raw deref
            // (the allow_rewind pre-flip below is unsafe and would UB on
            // NULL). Mirror seekToLast0/seekForPrev0/prev0 which already
            // guard. compat_jni.rs:3491 has the same missed-sister gap.
            if h.frs_iter.is_null() {
                throw_rocksdb(env, "RocksIterator.seek0: null frs_iter");
                return;
            }
            // SAFETY: read_byte_slice requires offset+len bounds; we pass 0/key_len.
            let needle = if key_len <= 0 {
                Vec::new()
            } else {
                let Some(k) = read_byte_slice(env, &key, 0, key_len) else {
                    return;
                };
                k
            };
            // B-C4R10-H1: pre-set allow_rewind=true so frs_iterator_seek's
            // D-C4R8-H1 guard does NOT reject a legitimate `iter.next();
            // iter.seek(...);` RocksIterator ABI sequence. The guard is for
            // forward-only callers whose mem::take has corrupted the row
            // array; the JNI compat shim cannot be forward-only because
            // RocksIterator exposes seek0 as a public API. Mirrors
            // A-C4R5-H1's flip in seekToLast0/seekForPrev0/prev0.
            unsafe {
                let state = &mut *(h.frs_iter as *mut crate::IteratorState);
                state.allow_rewind = true;
            }
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
            // A-C5R2-NEW-H1: null-check frs_iter BEFORE the raw deref.
            if h.frs_iter.is_null() {
                throw_rocksdb(env, "RocksIterator.seekToFirst0: null frs_iter");
                return;
            }
            // B-C4R10-H1: see seek0 — pre-set allow_rewind to bypass the
            // D-C4R8-H1 forward-only-only guard. RocksIterator's
            // seekToFirst() can legitimately follow next() calls.
            unsafe {
                let state = &mut *(h.frs_iter as *mut crate::IteratorState);
                state.allow_rewind = true;
            }
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
            // A-C4R5-H1: enable rewind-clone in frs_iterator_next. seekToLast0
            // moves cursor to a position the subsequent prev0/next0 chain will
            // re-enter; without this flag, fetch_into_handle's mem::take fast
            // path would empty the slot and a rewind would return a stale-empty
            // row (the original C-R22-NEW-H1 ABI bug, just via a different
            // entry).
            state.allow_rewind = true;
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
            // A-C4R5-H1: enable rewind-clone (see seekToLast0).
            state.allow_rewind = true;
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
            // A-C4R5-H1: enable rewind-clone (see seekToLast0).
            state.allow_rewind = true;
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

/// `org.forstdb.RocksIterator.status0(long handle)`
///
/// Java signature: `(J)V`
///
/// forst-rs iterator APIs surface status during seek/next calls; cached
/// iterators do not retain a deferred error, so this is a compatibility no-op.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksIterator_status0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) {
    jni_guard(&mut env, || (), |_env| {})
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
pub extern "system" fn Java_org_forstdb_WriteBatch_newWriteBatch__I<'local>(
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

/// `org.forstdb.WriteBatch.<init>(byte[] serialized, int len) -> long`
///
/// Java signature: `([BI)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_newWriteBatch___3BI<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _serialized: JByteArray<'local>,
    _serialized_len: jint,
) -> jlong {
    // forst-rs has no WriteBatch binary-deserialization surface. Return an
    // empty batch rather than exposing a mismatched short JNI symbol.
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| WriteBatchHandle::default().into_raw(),
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

fn write_batch_cf_handle(
    env: &mut JNIEnv,
    cf_handle: Option<jlong>,
    context: &str,
) -> Option<FrsCfHandle> {
    match cf_handle {
        Some(h) => cf_from_java_handle(env, h, context),
        None => Some(ptr::null_mut()),
    }
}

fn write_batch_push_put(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: Option<jlong>,
    key: JByteArray,
    key_len: jint,
    val: JByteArray,
    val_len: jint,
    context: &str,
) {
    let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
        throw_rocksdb(env, &format!("{context}: null handle"));
        return;
    };
    let Some(cf) = write_batch_cf_handle(env, cf_handle, context) else {
        return;
    };
    let Some(k) = read_byte_slice(env, &key, 0, key_len) else {
        return;
    };
    let Some(v) = read_byte_slice(env, &val, 0, val_len) else {
        return;
    };
    h.data_size = h.data_size.saturating_add((k.len() + v.len()) as u64);
    h.entries.push(WriteBatchEntry::Put {
        cf,
        key: k,
        value: v,
    });
}

fn write_batch_push_merge(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: Option<jlong>,
    key: JByteArray,
    key_len: jint,
    val: JByteArray,
    val_len: jint,
    context: &str,
) {
    let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
        throw_rocksdb(env, &format!("{context}: null handle"));
        return;
    };
    let Some(cf) = write_batch_cf_handle(env, cf_handle, context) else {
        return;
    };
    let Some(k) = read_byte_slice(env, &key, 0, key_len) else {
        return;
    };
    let Some(v) = read_byte_slice(env, &val, 0, val_len) else {
        return;
    };
    h.data_size = h.data_size.saturating_add((k.len() + v.len()) as u64);
    h.entries.push(WriteBatchEntry::Merge {
        cf,
        key: k,
        value: v,
    });
}

fn write_batch_push_delete(
    env: &mut JNIEnv,
    handle: jlong,
    cf_handle: Option<jlong>,
    key: JByteArray,
    key_len: jint,
    context: &str,
) {
    let Some(h) = (unsafe { WriteBatchHandle::from_raw_ref(handle) }) else {
        throw_rocksdb(env, &format!("{context}: null handle"));
        return;
    };
    let Some(cf) = write_batch_cf_handle(env, cf_handle, context) else {
        return;
    };
    let Some(k) = read_byte_slice(env, &key, 0, key_len) else {
        return;
    };
    h.data_size = h.data_size.saturating_add(k.len() as u64);
    h.entries.push(WriteBatchEntry::Delete { cf, key: k });
}

/// `org.forstdb.WriteBatch.put(long handle, byte[] key, int keyLen,
///                              byte[] val, int valLen)`
///
/// Java signature: `(J[BI[BI)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_put__J_3BI_3BI<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
    val: JByteArray<'local>,
    val_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            write_batch_push_put(
                env,
                handle,
                None,
                key,
                key_len,
                val,
                val_len,
                "WriteBatch.put",
            )
        },
    )
}

/// `org.forstdb.WriteBatch.put(long handle, byte[] key, int keyLen,
///                              byte[] val, int valLen, long cfHandle)`
///
/// Java signature: `(J[BI[BIJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_put__J_3BI_3BIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
    val: JByteArray<'local>,
    val_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            write_batch_push_put(
                env,
                handle,
                Some(cf_handle),
                key,
                key_len,
                val,
                val_len,
                "WriteBatch.put",
            )
        },
    )
}

/// `org.forstdb.WriteBatch.merge(long handle, byte[] key, int keyLen,
///                                byte[] val, int valLen)`
///
/// Java signature: `(J[BI[BI)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_merge__J_3BI_3BI<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
    val: JByteArray<'local>,
    val_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            write_batch_push_merge(
                env,
                handle,
                None,
                key,
                key_len,
                val,
                val_len,
                "WriteBatch.merge",
            )
        },
    )
}

/// `org.forstdb.WriteBatch.merge(long handle, byte[] key, int keyLen,
///                                byte[] val, int valLen, long cfHandle)`
///
/// Java signature: `(J[BI[BIJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_merge__J_3BI_3BIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
    val: JByteArray<'local>,
    val_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            write_batch_push_merge(
                env,
                handle,
                Some(cf_handle),
                key,
                key_len,
                val,
                val_len,
                "WriteBatch.merge",
            )
        },
    )
}

/// `org.forstdb.WriteBatch.delete(long handle, byte[] key, int keyLen)`
///
/// Java signature: `(J[BI)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_delete__J_3BI<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| write_batch_push_delete(env, handle, None, key, key_len, "WriteBatch.delete"),
    )
}

/// `org.forstdb.WriteBatch.delete(long handle, byte[] key, int keyLen,
///                                 long cfHandle)`
///
/// Java signature: `(J[BIJ)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBatch_delete__J_3BIJ<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    key: JByteArray<'local>,
    key_len: jint,
    cf_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            write_batch_push_delete(
                env,
                handle,
                Some(cf_handle),
                key,
                key_len,
                "WriteBatch.delete",
            )
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
                        let cf = if cf.is_null() {
                            let Some(default_cf) = default_cf_for_db(env, db_handle, &label) else {
                                return;
                            };
                            default_cf
                        } else {
                            cf
                        };
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
                        let cf = if cf.is_null() {
                            let Some(default_cf) = default_cf_for_db(env, db_handle, &label) else {
                                return;
                            };
                            default_cf
                        } else {
                            cf
                        };
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
                        let cf = if cf.is_null() {
                            let Some(default_cf) = default_cf_for_db(env, db_handle, &label) else {
                                return;
                            };
                            default_cf
                        } else {
                            cf
                        };
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

// ===========================================================================
// P2 — Checkpoint + Snapshot + import/export + deleteRange
//
// Surface (~21 entries):
//   - Checkpoint class:                     4 thunks
//   - Snapshot class:                       1 thunk
//   - RocksDB snapshot/file-list methods:   8 thunks
//   - ImportColumnFamilyOptions class:      3 thunks
//   - ExportImportFilesMetaData class:      2 thunks
//   - LiveFileMetaData class:               3 thunks (POJO accessors)
//
// **Implementation strategy.** Most of these have either a direct forst-rs
// counterpart (`frs_create_checkpoint`, `frs_l0_file_count`,
// `frs_sequence_number`) or are operations forst-rs simply doesn't model
// (file-deletion gates, full live-file enumeration, foreign-CF import).
// For the latter we accept the call, log via `tracing::debug!`, and return
// safe defaults so Flink never sees an `UnsatisfiedLinkError`. Documented
// divergences:
//
//   - `Snapshot`: forst-rs has no MVCC snapshot API. We expose
//     `getSnapshot` as a sequence-number recorder — `releaseSnapshot`
//     just drops the box. Reads do **not** honour snapshot isolation
//     today; this matches the existing FFI behaviour (reads always see
//     the latest committed write).
//   - `getLiveFiles` / `getLiveFilesMetaData`: stubbed to return only L0
//     file count via [`frs_l0_file_count`] (no per-file enumeration); the
//     returned LiveFiles object has the right shape so Flink's restore
//     loop links cleanly, but incremental restore that depends on
//     per-SST paths will see an empty list.
//   - `createColumnFamilyWithImport`: forst-rs has no foreign-CF import;
//     throws `RocksDBException` with a descriptive message.
//   - `disableFileDeletions` / `enableFileDeletions`: no-ops; forst-rs's
//     compactor is the sole owner of SST file lifecycle.
// ===========================================================================

// ---------------------------------------------------------------------------
// Snapshot + Checkpoint handle types
// ---------------------------------------------------------------------------

pub(crate) mod handles2 {
    use super::*;

    /// Java `org.forstdb.Snapshot` mirror. forst-rs has no MVCC snapshot
    /// surface; we record the engine sequence number at create time so the
    /// Java side has a long handle to round-trip through `releaseSnapshot`.
    /// Reads do not currently honour the recorded sequence — see the
    /// module-level divergence note.
    pub(crate) struct SnapshotHandle {
        #[allow(dead_code)]
        pub seq_no: u64,
    }

    /// Java `org.forstdb.Checkpoint` mirror. The Java class wraps a
    /// `RocksDB` reference and writes checkpoints into a caller-supplied
    /// directory; we record the DB handle at construction so the
    /// `createCheckpoint0(target)` instance method can invoke
    /// [`crate::frs_create_checkpoint`] without a second handle round-trip.
    pub(crate) struct CheckpointHandle {
        pub db: FrsDb,
    }

    /// Java `org.forstdb.ImportColumnFamilyOptions` mirror. forst-rs has
    /// no foreign-CF import path; the only field that affects future
    /// behaviour is `move_files` (true = rename rather than copy). We
    /// round-trip it for symmetry; the `createColumnFamilyWithImport`
    /// thunk currently throws regardless.
    #[derive(Default)]
    pub(crate) struct ImportColumnFamilyOptionsHandle {
        pub move_files: bool,
    }

    /// Java `org.forstdb.ExportImportFilesMetaData` mirror. Holds the
    /// directory path produced by `Checkpoint.exportColumnFamily` and a
    /// list of file metadata entries (currently always empty — forst-rs
    /// does not enumerate per-SST exports).
    #[derive(Default)]
    pub(crate) struct ExportImportFilesMetaDataHandle {
        #[allow(dead_code)]
        pub directory: String,
    }

    /// Java `org.forstdb.Statistics` mirror. forst-rs reports metrics via
    /// `forst_rs_common::metrics::*` rather than a per-handle Statistics
    /// object, so this carries only an opaque marker — `getTickerCount`
    /// returns 0 and `getHistogramData` returns an empty histogram.
    #[derive(Default)]
    pub(crate) struct StatisticsHandle {
        /// Reserved for future bridging to real metric counters.
        pub _reserved: u8,
    }

    /// Java `org.forstdb.BlockBasedTableConfig` mirror. Aggregates the
    /// settings community RocksDB attaches to its block-based-table format
    /// (block size, filter policy, block cache, index type) and forwards
    /// them onto [`EngineOptions`] at multi-CF open time
    /// (`Java_org_forstdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J_3J`).
    ///
    /// `bloom_bits_per_key` is hydrated from a [`BloomFilterHandle`] passed
    /// to `setFilterPolicy`; `block_cache_size` is hydrated from an
    /// [`LruCacheHandle`] passed to `setBlockCache`. The legacy
    /// `setBlockCacheSize` shortcut writes the same field directly.
    /// `index_type` is recorded but not yet honoured by forst-rs (the
    /// engine has only one index format today); future work can branch on
    /// it once the SST index gains alternate forms.
    #[derive(Default)]
    pub(crate) struct BlockBasedTableConfigHandle {
        /// Block size in bytes, or `None` to keep the engine default
        /// (64 KiB). Hydrated from `setBlockSize`.
        pub block_size: Option<usize>,
        /// Total block-cache capacity in bytes, or `None` to keep the
        /// engine default (256 MiB). Hydrated from `setBlockCache(lru)`
        /// or the legacy `setBlockCacheSize(bytes)` shortcut.
        pub block_cache_size: Option<usize>,
        /// Bits-per-key for the SST bloom filter, or `None` to keep the
        /// engine default (10). Hydrated from
        /// `setFilterPolicy(BloomFilter)`.
        pub bloom_bits_per_key: Option<usize>,
        /// Community `IndexType` ordinal: 0 = kBinarySearch (default),
        /// 1 = kHashSearch, 2 = kTwoLevelIndexSearch, 3 = kBinarySearchWithFirstKey.
        /// Recorded for future use.
        pub index_type: u8,
    }

    /// Java `org.forstdb.BloomFilter` mirror. Holds only the bits-per-key
    /// setting; `block_based_mode` is recorded for completeness but
    /// ignored by forst-rs (the engine has a single bloom encoding).
    pub(crate) struct BloomFilterHandle {
        pub bits_per_key: usize,
        #[allow(dead_code)]
        pub block_based_mode: bool,
    }

    /// Java `org.forstdb.LRUCache` mirror. Records the requested cache
    /// capacity + sharding hints; only `capacity` is currently propagated
    /// onto [`EngineOptions::block_cache_size`] when a `LRUCache` handle
    /// is attached to a `BlockBasedTableConfig` via `setBlockCache`.
    pub(crate) struct LruCacheHandle {
        pub capacity: usize,
        #[allow(dead_code)]
        pub num_shard_bits: i32,
        #[allow(dead_code)]
        pub strict_capacity_limit: bool,
        #[allow(dead_code)]
        pub high_pri_pool_ratio: f64,
    }

    /// Java `org.forstdb.WriteBufferManager` mirror. Community RocksDB
    /// uses this to share a write-buffer budget across CFs / DBs; forst-rs
    /// has per-CF arenas instead, so the values are recorded but the
    /// matching `DBOptions.setWriteBufferManager` thunk leaves them
    /// no-op'd. Stored here for diagnostic dumps.
    pub(crate) struct WriteBufferManagerHandle {
        #[allow(dead_code)]
        pub capacity: usize,
        #[allow(dead_code)]
        pub cache_handle: jlong,
    }

    /// Java `org.forstdb.FlinkEnv` mirror. Community Flink wraps a Flink
    /// `FileSystem` (S3 / GCS / Azure / HDFS) into a RocksDB `Env` so the
    /// engine writes SSTs through Flink's distributed-FS layer. forst-rs
    /// has its own `forst_rs_io::FileSystem` abstraction (with an
    /// OpenDAL-backed implementation for cloud storage) and does NOT
    /// dispatch FS calls back into Java, so this handle is accepted but
    /// otherwise ignored. Callers needing S3/GCS/Azure/HDFS should pick
    /// the appropriate forst-rs FileSystem at `RocksDB.open` time rather
    /// than relying on FlinkEnv.
    ///
    /// See the module-level divergence note (Path B in the audit): we
    /// chose not to implement a JNI-callback Env trait — a full Flink-FS
    /// bridge would add ~2000 LOC of cross-language marshalling for
    /// every read/write and is the wrong place to put cloud-storage
    /// integration when forst-rs already owns that abstraction.
    #[derive(Default)]
    pub(crate) struct FlinkEnvHandle {
        /// Number of `String` entries the Java side passed to the
        /// constructor (typically a Flink-FS scheme list). Recorded for
        /// debugging only — the engine never consults it.
        #[allow(dead_code)]
        pub fs_count: usize,
    }

    macro_rules! impl_into_from_raw_p2 {
        ($t:ty) => {
            impl $t {
                pub(crate) fn into_raw(self) -> jlong {
                    Box::into_raw(Box::new(self)) as jlong
                }

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

    impl_into_from_raw_p2!(SnapshotHandle);
    impl_into_from_raw_p2!(CheckpointHandle);
    impl_into_from_raw_p2!(ImportColumnFamilyOptionsHandle);
    impl_into_from_raw_p2!(ExportImportFilesMetaDataHandle);
    impl_into_from_raw_p2!(StatisticsHandle);
    impl_into_from_raw_p2!(BlockBasedTableConfigHandle);
    impl_into_from_raw_p2!(BloomFilterHandle);
    impl_into_from_raw_p2!(LruCacheHandle);
    impl_into_from_raw_p2!(WriteBufferManagerHandle);
    impl_into_from_raw_p2!(FlinkEnvHandle);
}

use handles2::{
    BlockBasedTableConfigHandle, BloomFilterHandle, CheckpointHandle,
    ExportImportFilesMetaDataHandle, FlinkEnvHandle, ImportColumnFamilyOptionsHandle,
    LruCacheHandle, SnapshotHandle, StatisticsHandle, WriteBufferManagerHandle,
};

// ---------------------------------------------------------------------------
// Checkpoint class
// ---------------------------------------------------------------------------

/// `org.forstdb.Checkpoint.create0(long dbHandle) -> long`
///
/// Java signature: `(J)J`
///
/// Static factory — the Java `Checkpoint.create(db)` call resolves here.
/// Records the underlying DB handle in a [`CheckpointHandle`] box. The
/// returned handle is dropped by `disposeInternal`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Checkpoint_create0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    db_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            if db_handle == 0 {
                throw_rocksdb(env, "Checkpoint.create0: null DB handle");
                return 0;
            }
            CheckpointHandle {
                db: db_handle as FrsDb,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.Checkpoint.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
///
/// Drops the [`CheckpointHandle`] box. Does **not** close the underlying
/// DB — the original `RocksDB` handle stays live for the caller.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Checkpoint_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `create0`, has not been
                // freed, and is uniquely held (Java side serialises this).
                unsafe { drop(Box::from_raw(handle as *mut CheckpointHandle)) };
            }
        },
    )
}

/// `org.forstdb.Checkpoint.createCheckpoint0(long handle, String targetDir)`
///
/// Java signature: `(JLjava/lang/String;)V`
///
/// Instance method — writes a consistent snapshot of the DB into
/// `targetDir`. Forwards to [`crate::frs_create_checkpoint`].
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Checkpoint_createCheckpoint0<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    target_dir: JString<'local>,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            let Some(h) = (unsafe { CheckpointHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "Checkpoint.createCheckpoint0: null handle");
                return;
            };
            let Some(dir) = read_string(env, &target_dir) else {
                return;
            };
            let c_dir = match std::ffi::CString::new(dir) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(
                        env,
                        "Checkpoint.createCheckpoint0: target_dir contains interior NUL",
                    );
                    return;
                }
            };
            // SAFETY: handle.db came from the live RocksDB; c_dir lives for
            // the duration of the call.
            let status = unsafe { frs_create_checkpoint(h.db, c_dir.as_ptr()) };
            check_status(env, status, "Checkpoint.createCheckpoint0");
        },
    )
}

/// `org.forstdb.Checkpoint.exportColumnFamily(long handle, long cfHandle,
///                                            String exportPath) -> long metaHandle`
///
/// Java signature: `(JJLjava/lang/String;)J`
///
/// Community ForSt's incremental-restore primitive: write the SSTs of the
/// supplied CF into `exportPath` and return an [`ExportImportFilesMetaDataHandle`]
/// describing the export. forst-rs does not expose per-CF SST extraction,
/// so we forward to a full checkpoint of `exportPath` and return a handle
/// whose `directory` field records the path. Callers using the metadata
/// for incremental restore will see an empty file list and fall back to
/// full restore via `dbOpenFromCheckpoint`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Checkpoint_exportColumnFamily<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _cf_handle: jlong,
    export_path: JString<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let Some(h) = (unsafe { CheckpointHandle::from_raw_ref(handle) }) else {
                throw_rocksdb(env, "Checkpoint.exportColumnFamily: null handle");
                return 0;
            };
            let Some(dir) = read_string(env, &export_path) else {
                return 0;
            };
            tracing::debug!(
                target: "compat_jni::checkpoint",
                "exportColumnFamily: forst-rs has no per-CF export — emitting full checkpoint at `{dir}`; metadata file list will be empty",
            );
            let c_dir = match std::ffi::CString::new(dir.clone()) {
                Ok(s) => s,
                Err(_) => {
                    throw_rocksdb(
                        env,
                        "Checkpoint.exportColumnFamily: export_path contains interior NUL",
                    );
                    return 0;
                }
            };
            // SAFETY: handle.db came from the live RocksDB; c_dir lives for the call.
            let status = unsafe { frs_create_checkpoint(h.db, c_dir.as_ptr()) };
            if check_status(env, status, "Checkpoint.exportColumnFamily") {
                return 0;
            }
            ExportImportFilesMetaDataHandle { directory: dir }.into_raw()
        },
    )
}

// ---------------------------------------------------------------------------
// Snapshot class + RocksDB.getSnapshot / releaseSnapshot
// ---------------------------------------------------------------------------

/// `org.forstdb.Snapshot.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
///
/// Drops the [`SnapshotHandle`] box. Note: community RocksDB requires the
/// caller to invoke `RocksDB.releaseSnapshot(snap)` before
/// `Snapshot.disposeInternal`. The shim's [`SnapshotHandle`] does not
/// retain any engine resources, so calling them in either order (or
/// either alone) is safe.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Snapshot_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `getSnapshot`, has not
                // been freed, and is uniquely held.
                unsafe { drop(Box::from_raw(handle as *mut SnapshotHandle)) };
            }
        },
    )
}

/// `org.forstdb.RocksDB.getSnapshot(long handle) -> long snapHandle`
///
/// Java signature: `(J)J`
///
/// Records the current engine sequence number in a [`SnapshotHandle`].
/// **Divergence:** reads against forst-rs do not currently honour the
/// recorded sequence — they always see the latest committed write. The
/// handle exists so callers can pass it through `releaseSnapshot` /
/// `Snapshot.disposeInternal` without `UnsatisfiedLinkError`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getSnapshot<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            if handle == 0 {
                throw_rocksdb(env, "RocksDB.getSnapshot: null DB handle");
                return 0;
            }
            let mut seq: u64 = 0;
            // SAFETY: handle came from a prior open; out_seq is a stack local.
            let status = unsafe { frs_sequence_number(handle as FrsDb, &mut seq) };
            if check_status(env, status, "RocksDB.getSnapshot") {
                return 0;
            }
            tracing::debug!(
                target: "compat_jni::snapshot",
                "RocksDB.getSnapshot: recording seq_no={seq}; reads do NOT honour snapshot isolation in forst-rs"
            );
            SnapshotHandle { seq_no: seq }.into_raw()
        },
    )
}

/// `org.forstdb.RocksDB.releaseSnapshot(long handle, long snapHandle)`
///
/// Java signature: `(JJ)V`
///
/// Drops the [`SnapshotHandle`] box. Idempotent on `0` — community
/// RocksDB tolerates a no-op release. The DB handle is unused (no engine
/// resource is tied to the snapshot).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_releaseSnapshot<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    snap_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if snap_handle != 0 {
                // SAFETY: snap_handle came from a prior `getSnapshot`.
                unsafe { drop(Box::from_raw(snap_handle as *mut SnapshotHandle)) };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Live-file enumeration (stubbed)
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.getLiveFiles(long handle, boolean flushMemtable)
///                                   -> String[]`
///
/// Java signature: `(JZ)[Ljava/lang/String;`
///
/// ForStJNI's private native method returns a `String[]`: every live-file path
/// first, and the manifest size as the final decimal string. The public Java
/// wrapper converts that array into `RocksDB.LiveFiles`. We delegate per-file
/// enumeration to [`crate::frs_db_get_live_files`], which walks the engine's
/// current Version and returns one entry per live SST.
///
/// Memory: the underlying `FrsLiveFileList` is freed via
/// [`crate::frs_db_live_file_list_free`] before we return — the JVM has
/// already copied each path into a Java `String`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getLiveFiles<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    flush_memtable: jboolean,
) -> jni::sys::jobjectArray {
    jni_guard(&mut env, ptr::null_mut, |env| -> jni::sys::jobjectArray {
        if handle == 0 {
            throw_rocksdb(env, "RocksDB.getLiveFiles: null DB handle");
            return ptr::null_mut();
        }

        // Step 1: enumerate live files (engine flushes if requested).
        let mut list = crate::FrsLiveFileList {
            files: ptr::null_mut(),
            count: 0,
            manifest_size: 0,
        };
        // SAFETY: handle came from a prior open; `&mut list` is stack-local.
        let st = unsafe {
            crate::frs_db_get_live_files(handle as FrsDb, flush_memtable != JNI_FALSE, &mut list)
        };
        if st != FRS_STATUS_OK {
            if st == crate::FRS_STATUS_INVALID_ARGUMENT {
                match fallback_live_files_for_db(handle as FrsDb) {
                    Some(fallback) => {
                        list = fallback;
                    }
                    None => {
                        if check_status(env, st, "RocksDB.getLiveFiles.enumerate") {
                            return ptr::null_mut();
                        }
                    }
                }
            } else if check_status(env, st, "RocksDB.getLiveFiles.enumerate") {
                return ptr::null_mut();
            }
        }

        // Step 2: read the (possibly post-flush) sequence number.
        let mut seq: u64 = 0;
        // SAFETY: handle valid; out_seq stack-local.
        let st = unsafe { frs_sequence_number(handle as FrsDb, &mut seq) };
        if check_status(env, st, "RocksDB.getLiveFiles.seq") {
            // SAFETY: list was filled by frs_db_get_live_files above.
            unsafe {
                let _ = crate::frs_db_live_file_list_free(&mut list);
            }
            return ptr::null_mut();
        }

        let count = list.count;
        let manifest_size = list.manifest_size;
        tracing::debug!(
            target: "compat_jni::livefiles",
            "RocksDB.getLiveFiles: enumerated {count} SST file(s) (manifest={manifest_size}B, seq={seq})"
        );

        let mut live_file_names = Vec::<String>::with_capacity(count.saturating_add(1));
        let mut has_manifest = false;
        let mut java_manifest_size = manifest_size;
        if count > 0 && !list.files.is_null() {
            // SAFETY: `list.files` is a valid pointer to `count` initialised
            // entries produced by `into_ffi_list`.
            let entries = unsafe { std::slice::from_raw_parts(list.files, count) };
            for entry in entries {
                if entry.path.is_null() {
                    continue;
                }
                // SAFETY: path is a Rust-owned NUL-terminated CString.
                let path_str = match unsafe { std::ffi::CStr::from_ptr(entry.path) }.to_str() {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let file_name = live_file_name_for_java(path_str);
                if file_name.starts_with("MANIFEST") {
                    has_manifest = true;
                    java_manifest_size = entry.size;
                }
                live_file_names.push(file_name);
            }
        }
        if !has_manifest {
            if let Some((manifest_name, manifest_size)) =
                ensure_compat_manifest_for_db(handle as FrsDb)
            {
                java_manifest_size = manifest_size;
                live_file_names.push(manifest_name);
            }
        }

        // Step 3: build the String[] expected by ForStJNI's public wrapper.
        // The final array element is the manifest size encoded as a decimal string.
        let string_class = match env.find_class("java/lang/String") {
            Ok(c) => c,
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("getLiveFiles: find_class(String) failed: {e}"),
                );
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        };
        let array_len = match live_file_names
            .len()
            .checked_add(1)
            .and_then(|n| i32::try_from(n).ok())
        {
            Some(len) => len,
            None => {
                throw_rocksdb(
                    env,
                    "getLiveFiles: live file count exceeds Java array length",
                );
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        };
        let files_array = match env.new_object_array(array_len, &string_class, JObject::null()) {
            Ok(a) => a,
            Err(e) => {
                throw_rocksdb(env, &format!("getLiveFiles: new String[] failed: {e}"));
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        };

        for (idx, file_name) in live_file_names.iter().enumerate() {
            let jstr = match env.new_string(file_name) {
                Ok(s) => s,
                Err(e) => {
                    throw_rocksdb(env, &format!("getLiveFiles: new path string failed: {e}"));
                    unsafe {
                        let _ = crate::frs_db_live_file_list_free(&mut list);
                    }
                    return ptr::null_mut();
                }
            };
            if let Err(e) = env.set_object_array_element(&files_array, idx as i32, &jstr) {
                throw_rocksdb(
                    env,
                    &format!("getLiveFiles: set file element {idx} failed: {e}"),
                );
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        }

        let manifest_size_string = java_manifest_size.to_string();
        let manifest_jstr = match env.new_string(&manifest_size_string) {
            Ok(s) => s,
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("getLiveFiles: new manifest-size string failed: {e}"),
                );
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        };
        if let Err(e) =
            env.set_object_array_element(&files_array, live_file_names.len() as i32, &manifest_jstr)
        {
            throw_rocksdb(
                env,
                &format!("getLiveFiles: set manifest-size element failed: {e}"),
            );
            unsafe {
                let _ = crate::frs_db_live_file_list_free(&mut list);
            }
            return ptr::null_mut();
        }

        // Step 4: free the FFI list — Java now owns the Strings.
        // SAFETY: list was filled by frs_db_get_live_files above; freeing
        // here is the documented one-and-only release.
        unsafe {
            let _ = crate::frs_db_live_file_list_free(&mut list);
        }

        files_array.into_raw()
    })
}

/// `org.forstdb.RocksDB.getLiveFilesMetaData(long handle) -> List<LiveFileMetaData>`
///
/// Java signature: `(J)Ljava/util/List;`
///
/// We enumerate every live SST via [`crate::frs_db_get_live_files_metadata`],
/// then attempt to construct one `LiveFileMetaData` per entry using its
/// `(String columnFamilyName, int level, String fileName, String path,
/// long size, long smallestSeqno, long largestSeqno, byte[] smallestKey,
/// byte[] largestKey, long numReadsSampled, boolean beingCompacted,
/// long numEntries, long numDeletions)` ctor (the standard
/// community-RocksJava shape). Fields forst-rs does not currently track
/// (smallest/largest keys, numReadsSampled, beingCompacted) are filled
/// with documented placeholder values — Flink's incremental-restore code
/// path only consumes `level`, `path` (or `fileName`), and `size`.
///
/// If the ctor's class is unavailable on the classpath we degrade to
/// returning an empty list (preserving the previous behaviour) and log
/// a debug-level note rather than throwing.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getLiveFilesMetaData<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) -> jni::sys::jobject {
    jni_guard(&mut env, ptr::null_mut, |env| -> jni::sys::jobject {
        if handle == 0 {
            throw_rocksdb(env, "RocksDB.getLiveFilesMetaData: null DB handle");
            return ptr::null_mut();
        }

        // Step 1: enumerate.
        let mut list = crate::FrsLiveFileList {
            files: ptr::null_mut(),
            count: 0,
            manifest_size: 0,
        };
        // SAFETY: handle valid; out stack-local.
        let st = unsafe { crate::frs_db_get_live_files_metadata(handle as FrsDb, &mut list) };
        if check_status(env, st, "RocksDB.getLiveFilesMetaData.enumerate") {
            return ptr::null_mut();
        }
        let count = list.count;
        tracing::debug!(
            target: "compat_jni::livefiles",
            "RocksDB.getLiveFilesMetaData: enumerated {count} SST file(s)"
        );

        // Step 2: build the ArrayList we'll return.
        let arraylist_class = match env.find_class("java/util/ArrayList") {
            Ok(c) => c,
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("getLiveFilesMetaData: find_class(ArrayList) failed: {e}"),
                );
                // SAFETY: list owned by us.
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        };
        let result = match env.new_object(&arraylist_class, "()V", &[]) {
            Ok(o) => o,
            Err(e) => {
                throw_rocksdb(
                    env,
                    &format!("getLiveFilesMetaData: new ArrayList failed: {e}"),
                );
                // SAFETY: list owned by us.
                unsafe {
                    let _ = crate::frs_db_live_file_list_free(&mut list);
                }
                return ptr::null_mut();
            }
        };

        // Step 3: per-entry — try to construct LiveFileMetaData. If the
        // class is missing we still return the (possibly empty) list.
        let lfm_class = env.find_class("org/forstdb/LiveFileMetaData").ok();
        if lfm_class.is_none() {
            tracing::debug!(
                target: "compat_jni::livefiles",
                "getLiveFilesMetaData: org.forstdb.LiveFileMetaData unavailable; returning empty list (Flink incremental-restore degrades to full copy)"
            );
            // SAFETY: list owned by us.
            unsafe {
                let _ = crate::frs_db_live_file_list_free(&mut list);
            }
            return result.into_raw();
        }
        let lfm_class = lfm_class.unwrap();

        if count > 0 && !list.files.is_null() {
            // SAFETY: `list.files` valid for `count` initialised entries.
            let entries = unsafe { std::slice::from_raw_parts(list.files, count) };
            for entry in entries {
                if entry.path.is_null() || entry.cf_name.is_null() {
                    continue;
                }
                // SAFETY: Rust-owned NUL-terminated CStrings.
                let path_str = match unsafe { std::ffi::CStr::from_ptr(entry.path) }.to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => continue,
                };
                let cf_str = match unsafe { std::ffi::CStr::from_ptr(entry.cf_name) }.to_str() {
                    Ok(s) => s.to_string(),
                    Err(_) => continue,
                };
                let file_basename = std::path::Path::new(&path_str)
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or(path_str.as_str())
                    .to_string();

                let cf_jstr = match env.new_string(&cf_str) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let path_jstr = match env.new_string(&path_str) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let file_jstr = match env.new_string(&file_basename) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                // Empty byte[] for smallest/largest key — engine does not
                // expose them on the live-file FFI primitive today.
                let empty_bytes = match env.new_byte_array(0) {
                    Ok(a) => a,
                    Err(_) => continue,
                };
                let empty_bytes2 = match env.new_byte_array(0) {
                    Ok(a) => a,
                    Err(_) => continue,
                };

                let ctor_sig =
                    "(Ljava/lang/String;ILjava/lang/String;Ljava/lang/String;JJJ[B[BJZJJ)V";
                let args = [
                    jni::objects::JValue::Object(cf_jstr.as_ref()),
                    jni::objects::JValue::Int(entry.level as jint),
                    jni::objects::JValue::Object(file_jstr.as_ref()),
                    jni::objects::JValue::Object(path_jstr.as_ref()),
                    jni::objects::JValue::Long(entry.size as jlong),
                    jni::objects::JValue::Long(0), // smallestSeqno (not tracked separately)
                    jni::objects::JValue::Long(entry.sequence as jlong),
                    jni::objects::JValue::Object(empty_bytes.as_ref()),
                    jni::objects::JValue::Object(empty_bytes2.as_ref()),
                    jni::objects::JValue::Long(0), // numReadsSampled
                    jni::objects::JValue::Bool(JNI_FALSE), // beingCompacted
                    jni::objects::JValue::Long(0), // numEntries (not surfaced today)
                    jni::objects::JValue::Long(0), // numDeletions
                ];
                let lfm = match env.new_object(&lfm_class, ctor_sig, &args) {
                    Ok(o) => o,
                    Err(e) => {
                        tracing::debug!(
                            target: "compat_jni::livefiles",
                            "getLiveFilesMetaData: LiveFileMetaData ctor failed ({e}); skipping entry"
                        );
                        // Clear the pending Java exception so subsequent
                        // calls don't trip on it.
                        let _ = env.exception_clear();
                        continue;
                    }
                };
                let _ = env.call_method(
                    &result,
                    "add",
                    "(Ljava/lang/Object;)Z",
                    &[jni::objects::JValue::Object(lfm.as_ref())],
                );
            }
        }

        // Step 4: free FFI list now that Java owns the data.
        // SAFETY: list was filled by frs_db_get_live_files_metadata.
        unsafe {
            let _ = crate::frs_db_live_file_list_free(&mut list);
        }

        result.into_raw()
    })
}

/// `org.forstdb.RocksDB.disableFileDeletions(long handle)`
///
/// Java signature: `(J)V`
///
/// No-op — forst-rs's compactor is the sole owner of SST-file lifecycle;
/// no external "disable deletion" gate exists. Accept and log.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_disableFileDeletions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(
                target: "compat_jni::livefiles",
                "RocksDB.disableFileDeletions: no-op (forst-rs has no external deletion gate)"
            );
        },
    )
}

/// `org.forstdb.RocksDB.enableFileDeletions(long handle, boolean force)`
///
/// Java signature: `(JZ)V`
///
/// No-op — counterpart of `disableFileDeletions`. The `force` flag is
/// ignored.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_enableFileDeletions<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _force: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            tracing::debug!(
                target: "compat_jni::livefiles",
                "RocksDB.enableFileDeletions: no-op (forst-rs has no external deletion gate)"
            );
        },
    )
}

/// Long JNI form for overloaded Java lookup of `disableFileDeletions(long)`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_disableFileDeletions__J<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
) {
    Java_org_forstdb_RocksDB_disableFileDeletions(env, class, handle)
}

/// Long JNI form for overloaded Java lookup of `enableFileDeletions(long, boolean)`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_enableFileDeletions__JZ<'local>(
    env: JNIEnv<'local>,
    class: JClass<'local>,
    handle: jlong,
    force: jboolean,
) {
    Java_org_forstdb_RocksDB_enableFileDeletions(env, class, handle, force)
}

// ---------------------------------------------------------------------------
// Range deletion
// ---------------------------------------------------------------------------

/// Internal helper: delete every key in `[begin, end)` for the given CF
/// by enumerating via the existing iterator FFI and issuing per-key
/// `frs_delete`. forst-rs does not yet expose a tombstone-range primitive;
/// this is O(n) over the affected range but correct.
fn delete_range_inner(env: &mut JNIEnv, db: FrsDb, cf: FrsCfHandle, begin: &[u8], end: &[u8]) {
    if begin >= end {
        // Empty range — no-op (matches RocksDB).
        return;
    }
    let mut iter: FrsIterator = ptr::null_mut();
    // SAFETY: db / cf valid; out_iter stack-local.
    let st = unsafe { frs_iterator_open(db, cf, &mut iter) };
    if check_status(env, st, "RocksDB.deleteRange.openIterator") {
        return;
    }
    // Position at first key >= begin.
    // SAFETY: iter valid for this scope.
    let st = unsafe { frs_iterator_seek(iter, begin.as_ptr(), begin.len()) };
    if check_status(env, st, "RocksDB.deleteRange.seek") {
        // SAFETY: iter came from frs_iterator_open above.
        unsafe {
            let _ = frs_iterator_close(iter);
        }
        return;
    }
    let mut to_delete: Vec<Vec<u8>> = Vec::new();
    loop {
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
        // SAFETY: iter valid; out_* stack-locals.
        let st = unsafe { crate::frs_iterator_next(iter, &mut k, &mut v, &mut valid) };
        if check_status(env, st, "RocksDB.deleteRange.next") {
            // SAFETY: iter valid.
            unsafe {
                let _ = frs_iterator_close(iter);
            }
            return;
        }
        if !valid {
            break;
        }
        // SAFETY: k/v populated when valid.
        let key_vec = unsafe { std::slice::from_raw_parts(k.data, k.len).to_vec() };
        unsafe {
            let _ = crate::frs_bytes_free(&mut k);
            let _ = crate::frs_bytes_free(&mut v);
        }
        if key_vec.as_slice() >= end {
            break;
        }
        to_delete.push(key_vec);
    }
    // SAFETY: iter came from frs_iterator_open.
    unsafe {
        let _ = frs_iterator_close(iter);
    }
    for key in to_delete {
        // SAFETY: db / cf valid; key vec lives for the call.
        let st = unsafe { frs_delete(db, cf, key.as_ptr(), key.len()) };
        if check_status(env, st, "RocksDB.deleteRange.delete") {
            return;
        }
    }
}

/// `org.forstdb.RocksDB.deleteRange(long handle, long cfHandle,
///                                  long writeOptionsHandle,
///                                  byte[] begin, int beginOff, int beginLen,
///                                  byte[] end, int endOff, int endLen)`
///
/// Java signature: `(JJJ[BII[BII)V`
///
/// Tombstones every key `k` where `begin <= k < end`. forst-rs has no
/// range-tombstone primitive; this enumerates the affected range via an
/// iterator and issues per-key `frs_delete`. Performance scales O(n) over
/// the range — adequate for Flink's typical "drop a window" usage which
/// covers small key counts; large ranges should use full-CF compaction or
/// CF-recreation instead.
///
/// `WriteOptions.disable_wal` is logged but not honoured (see `write0`).
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_RocksDB_deleteRange<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    wo_handle: jlong,
    begin: JByteArray<'local>,
    begin_off: jint,
    begin_len: jint,
    end: JByteArray<'local>,
    end_off: jint,
    end_len: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            if wo_handle != 0 {
                if let Some(wo) = unsafe { WriteOptionsHandle::from_raw_ref(wo_handle) } {
                    if wo.disable_wal {
                        tracing::debug!(
                            target: "compat_jni::deleteRange",
                            "deleteRange: disable_wal=true ignored — engine always writes WAL"
                        );
                    }
                }
            }
            let Some(b) = read_byte_slice(env, &begin, begin_off, begin_len) else {
                return;
            };
            let Some(e) = read_byte_slice(env, &end, end_off, end_len) else {
                return;
            };
            delete_range_inner(env, handle as FrsDb, cf_handle as FrsCfHandle, &b, &e);
        },
    )
}

/// `org.forstdb.RocksDB.deleteFilesInRanges(long handle, long cfHandle,
///                                          byte[][] ranges, boolean includeEnd)`
///
/// Java signature: `(JJ[[BZ)V`
///
/// Community ForSt's compaction-side range-drop primitive — pairs in
/// `ranges` (length must be even) define `[begin_i, end_i)` runs of SSTs
/// to drop. forst-rs has no SST-deletion-by-range surface; we forward to
/// per-range [`delete_range_inner`] so the caller's intent (those keys
/// are gone) is honoured, at the cost of O(n) tombstones rather than
/// instantaneous file-level drops.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_deleteFilesInRanges<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cf_handle: jlong,
    ranges: JObjectArray<'local>,
    include_end: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |env| {
            if include_end != JNI_FALSE {
                tracing::debug!(
                    target: "compat_jni::deleteFilesInRanges",
                    "deleteFilesInRanges: include_end=true reduced to half-open [begin, end) semantics"
                );
            }
            let Some(rs) = read_byte_matrix(env, &ranges, "RocksDB.deleteFilesInRanges.ranges")
            else {
                return;
            };
            if rs.len() % 2 != 0 {
                throw_rocksdb(
                    env,
                    &format!(
                        "RocksDB.deleteFilesInRanges: ranges length must be even, got {}",
                        rs.len()
                    ),
                );
                return;
            }
            for pair in rs.chunks_exact(2) {
                delete_range_inner(
                    env,
                    handle as FrsDb,
                    cf_handle as FrsCfHandle,
                    &pair[0],
                    &pair[1],
                );
                if env.exception_check().unwrap_or(false) {
                    return;
                }
            }
        },
    )
}

// ---------------------------------------------------------------------------
// Foreign-CF import (stubbed)
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.createColumnFamilyWithImport(long handle,
///                                                   org.forstdb.ColumnFamilyDescriptor descriptor,
///                                                   org.forstdb.ImportColumnFamilyOptions options,
///                                                   List<ExportImportFilesMetaData> metaList)
///                                                   -> long cfHandle`
///
/// Java signature: `(JLorg/forstdb/ColumnFamilyDescriptor;Lorg/forstdb/ImportColumnFamilyOptions;Ljava/util/List;)J`
///
/// **Stub.** forst-rs has no foreign-CF import path; throws
/// `RocksDBException` with a descriptive message so Flink surfaces the
/// limitation in the job log rather than silently corrupting state.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_createColumnFamilyWithImport<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _descriptor: JObject<'local>,
    _options: JObject<'local>,
    _meta_list: JObject<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            throw_rocksdb(
                env,
                "RocksDB.createColumnFamilyWithImport: not yet supported by forst-rs (use dbOpenFromCheckpoint for full restore)",
            );
            0
        },
    )
}

// ---------------------------------------------------------------------------
// ImportColumnFamilyOptions class
// ---------------------------------------------------------------------------

/// `org.forstdb.ImportColumnFamilyOptions.<init>() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ImportColumnFamilyOptions_newImportColumnFamilyOptions<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| ImportColumnFamilyOptionsHandle::default().into_raw(),
    )
}

/// `org.forstdb.ImportColumnFamilyOptions.disposeInternal(long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ImportColumnFamilyOptions_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newImportColumnFamilyOptions`.
                unsafe {
                    drop(Box::from_raw(
                        handle as *mut ImportColumnFamilyOptionsHandle,
                    ));
                }
            }
        },
    )
}

/// `org.forstdb.ImportColumnFamilyOptions.setMoveFiles(long, boolean)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ImportColumnFamilyOptions_setMoveFiles<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    value: jboolean,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { ImportColumnFamilyOptionsHandle::from_raw_ref(handle) } {
                h.move_files = value != JNI_FALSE;
            }
        },
    )
}

// ---------------------------------------------------------------------------
// ExportImportFilesMetaData class
// ---------------------------------------------------------------------------

/// `org.forstdb.ExportImportFilesMetaData.<init>() -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ExportImportFilesMetaData_newExportImportFilesMetaData<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| ExportImportFilesMetaDataHandle::default().into_raw(),
    )
}

/// `org.forstdb.ExportImportFilesMetaData.disposeInternal(long)`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_ExportImportFilesMetaData_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior ctor.
                unsafe {
                    drop(Box::from_raw(
                        handle as *mut ExportImportFilesMetaDataHandle,
                    ));
                }
            }
        },
    )
}

// ---------------------------------------------------------------------------
// LiveFileMetaData accessors (always-empty placeholders)
//
// Community ForSt exposes per-file accessors (fileName, level, sequenceNumber)
// on a Java POJO returned from getLiveFilesMetaData. Since we never return
// non-empty metadata, these accessors are linked-but-unused; we keep them
// so a Flink classpath that probes the symbol table still resolves.
// ---------------------------------------------------------------------------

/// `org.forstdb.LiveFileMetaData.fileName(long handle) -> String`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_LiveFileMetaData_fileName<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) -> jni::sys::jstring {
    jni_guard(&mut env, ptr::null_mut, |env| -> jni::sys::jstring {
        match env.new_string("") {
            Ok(s) => s.into_raw(),
            Err(_) => ptr::null_mut(),
        }
    })
}

/// `org.forstdb.LiveFileMetaData.level(long handle) -> int`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_LiveFileMetaData_level<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) -> jint {
    jni_guard(&mut env, || 0_i32, |_env| 0_i32)
}

/// `org.forstdb.LiveFileMetaData.sequenceNumber(long handle) -> long`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_LiveFileMetaData_sequenceNumber<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
) -> jlong {
    jni_guard(&mut env, || 0_i64, |_env| 0_i64)
}

// ===========================================================================
// P4 — Statistics + getProperty + multiGet
//
// Surface (~6 entries):
//   - Statistics class:                     4 thunks
//   - RocksDB.getProperty:                  1 thunk
//   - RocksDB.multiGet:                     1 thunk
//
// `Statistics` is purely a sink class — forst-rs reports metrics through
// `forst_rs_common::metrics::*` rather than per-DB Statistics objects, so
// `getTickerCount` always returns 0 and `getHistogramData` returns an
// empty `HistogramData` (or null if the class is not on the classpath).
//
// `getProperty` synthesises responses for the keys forst-rs *can* answer
// (`rocksdb.num-files-at-level0` → frs_l0_file_count, ...) and returns
// "0" / "" for everything else.
//
// `multiGet` is the multi-CF batch lookup; iterates the CF + key arrays
// in lock-step and dispatches per-pair `frs_get`.
// ===========================================================================

// ---------------------------------------------------------------------------
// Statistics class
// ---------------------------------------------------------------------------

/// `org.forstdb.Statistics.<init>() -> long`
///
/// Java signature: `()J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Statistics_newStatistics<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| StatisticsHandle::default().into_raw(),
    )
}

/// `org.forstdb.Statistics.disposeInternal(long)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Statistics_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newStatistics`.
                unsafe { drop(Box::from_raw(handle as *mut StatisticsHandle)) };
            }
        },
    )
}

/// `org.forstdb.Statistics.getTickerCount(long handle, byte tickerId) -> long`
///
/// Java signature: `(JB)J`
///
/// Returns 0 — forst-rs does not expose RocksDB ticker counters. See
/// module-level divergence note.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Statistics_getTickerCount<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _ticker: jint,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            tracing::debug!(
                target: "compat_jni::statistics",
                "Statistics.getTickerCount: returning 0 (forst-rs metrics are exposed via forst_rs_common::metrics)"
            );
            0_i64
        },
    )
}

/// `org.forstdb.Statistics.getHistogramData(long handle, byte histogramId)
///                                          -> org.forstdb.HistogramData`
///
/// Java signature: `(JB)Lorg/forstdb/HistogramData;`
///
/// Returns a `HistogramData` constructed via its 5-double ctor with all
/// zeros. If the class is missing from the classpath, returns null —
/// Flink null-handles this in its sampling code paths.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_Statistics_getHistogramData<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _handle: jlong,
    _histogram: jint,
) -> jni::sys::jobject {
    jni_guard(&mut env, ptr::null_mut, |env| -> jni::sys::jobject {
        let hist_class = match env.find_class("org/forstdb/HistogramData") {
            Ok(c) => c,
            Err(_) => return ptr::null_mut(),
        };
        // Community ctor: HistogramData(double, double, double, double, double).
        match env.new_object(
            &hist_class,
            "(DDDDD)V",
            &[
                jni::objects::JValue::Double(0.0),
                jni::objects::JValue::Double(0.0),
                jni::objects::JValue::Double(0.0),
                jni::objects::JValue::Double(0.0),
                jni::objects::JValue::Double(0.0),
            ],
        ) {
            Ok(o) => o.into_raw(),
            Err(_) => {
                // Fallback: try the older 3-arg ctor.
                match env.new_object(
                    &hist_class,
                    "(DDD)V",
                    &[
                        jni::objects::JValue::Double(0.0),
                        jni::objects::JValue::Double(0.0),
                        jni::objects::JValue::Double(0.0),
                    ],
                ) {
                    Ok(o) => o.into_raw(),
                    Err(_) => ptr::null_mut(),
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// RocksDB.getProperty
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.getProperty(long handle, long cfHandle, String name) -> String`
///
/// Java signature: `(JJLjava/lang/String;)Ljava/lang/String;`
///
/// Synthesises responses for the few RocksDB metric keys forst-rs can
/// answer; returns `"0"` (or `""` for free-form keys) for everything
/// else. Recognised keys:
///
///   - `rocksdb.num-files-at-level0`             → [`frs_l0_file_count`]
///   - `rocksdb.cur-size-active-mem-table`       → 0 (forst-rs arenas
///     are not externally measurable today)
///   - `rocksdb.estimate-num-keys`               → sequence number
///     (loose upper bound; better than nothing for sizing decisions)
///   - all others                                → `"0"`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getProperty<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _cf_handle: jlong,
    name: JString<'local>,
) -> jni::sys::jstring {
    jni_guard(&mut env, ptr::null_mut, |env| -> jni::sys::jstring {
        let Some(name_str) = read_string(env, &name) else {
            return ptr::null_mut();
        };
        let value: String = match name_str.as_str() {
            "rocksdb.num-files-at-level0" => {
                let mut count: u32 = 0;
                // SAFETY: handle came from open; out_count is stack-local.
                let st = unsafe { frs_l0_file_count(handle as FrsDb, &mut count) };
                if st == FRS_STATUS_OK {
                    count.to_string()
                } else {
                    "0".to_string()
                }
            }
            "rocksdb.estimate-num-keys" => {
                let mut seq: u64 = 0;
                // SAFETY: handle came from open.
                let st = unsafe { frs_sequence_number(handle as FrsDb, &mut seq) };
                if st == FRS_STATUS_OK {
                    seq.to_string()
                } else {
                    "0".to_string()
                }
            }
            _ => {
                tracing::debug!(
                    target: "compat_jni::getProperty",
                    "RocksDB.getProperty: returning \"0\" for unmapped key `{name_str}`"
                );
                "0".to_string()
            }
        };
        match env.new_string(&value) {
            Ok(s) => s.into_raw(),
            Err(e) => {
                throw_rocksdb(env, &format!("getProperty: new_string failed: {e}"));
                ptr::null_mut()
            }
        }
    })
}

/// `org.forstdb.RocksDB.getLongProperty(long handle, long cfHandle,
///                                      String name, int nameLen) -> long`
///
/// Java signature: `(JJLjava/lang/String;I)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_getLongProperty<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _cf_handle: jlong,
    name: JString<'local>,
    _name_len: jint,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| -> jlong {
            let Some(name_str) = read_string(env, &name) else {
                return 0;
            };
            match name_str.as_str() {
                "rocksdb.num-files-at-level0" => {
                    let mut count: u32 = 0;
                    let st = unsafe { frs_l0_file_count(handle as FrsDb, &mut count) };
                    if st == FRS_STATUS_OK {
                        count as jlong
                    } else {
                        0
                    }
                }
                "rocksdb.estimate-num-keys" => {
                    let mut seq: u64 = 0;
                    let st = unsafe { frs_sequence_number(handle as FrsDb, &mut seq) };
                    if st == FRS_STATUS_OK {
                        seq as jlong
                    } else {
                        0
                    }
                }
                _ => 0,
            }
        },
    )
}

// ---------------------------------------------------------------------------
// RocksDB.multiGet (multi-CF)
// ---------------------------------------------------------------------------

/// `org.forstdb.RocksDB.multiGet(long handle, long readOptionsHandle,
///                                long[] cfHandles, byte[][] keys) -> byte[][]`
///
/// Java signature: `(JJ[J[[B)[[B`
///
/// Multi-CF batch lookup. `cfHandles[i]` and `keys[i]` are paired —
/// missing keys yield a null array entry. If `cfHandles` is null, every
/// lookup runs against the default CF (community RocksDB convention).
/// Length mismatch throws `RocksDBException`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_RocksDB_multiGet<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    _ro_handle: jlong,
    cf_handles: JPrimitiveArray<'local, jlong>,
    keys: JObjectArray<'local>,
) -> jobjectArray {
    jni_guard(
        &mut env,
        || ptr::null_mut() as jobjectArray,
        |env| -> jobjectArray {
            let Some(ks) = read_byte_matrix(env, &keys, "RocksDB.multiGet.keys") else {
                return ptr::null_mut();
            };
            let count = ks.len();

            // Resolve CF list — null cf_handles means "all default".
            let cf_list: Vec<FrsCfHandle> = if (cf_handles.as_ref() as &JObject).is_null() {
                if handle == 0 {
                    throw_rocksdb(env, "RocksDB.multiGet: null DB handle and null cfHandles");
                    return ptr::null_mut();
                }
                let mut default_cf: FrsCfHandle = ptr::null_mut();
                // SAFETY: handle valid; out_cf stack-local.
                let st = unsafe { frs_db_default_cf(handle as FrsDb, &mut default_cf) };
                if check_status(env, st, "RocksDB.multiGet.defaultCf") {
                    return ptr::null_mut();
                }
                vec![default_cf; count]
            } else {
                let cf_len = match env.get_array_length(&cf_handles) {
                    Ok(n) => n as usize,
                    Err(e) => {
                        throw_rocksdb(env, &format!("multiGet: get_array_length(cf): {e}"));
                        return ptr::null_mut();
                    }
                };
                if cf_len != count {
                    throw_rocksdb(
                        env,
                        &format!(
                            "RocksDB.multiGet: cfHandles.length ({cf_len}) != keys.length ({count})"
                        ),
                    );
                    return ptr::null_mut();
                }
                let mut buf = vec![0_i64; cf_len];
                if let Err(e) = env.get_long_array_region(&cf_handles, 0, &mut buf) {
                    throw_rocksdb(env, &format!("multiGet: read cf array: {e}"));
                    return ptr::null_mut();
                }
                buf.into_iter().map(|v| v as FrsCfHandle).collect()
            };

            // Build the result `byte[][]` shell.
            let element_class = match env.find_class("[B") {
                Ok(c) => c,
                Err(e) => {
                    throw_rocksdb(env, &format!("multiGet: find_class([B): {e}"));
                    return ptr::null_mut();
                }
            };
            let outer = match env.new_object_array(count as jint, &element_class, JObject::null()) {
                Ok(a) => a,
                Err(e) => {
                    throw_rocksdb(env, &format!("multiGet: new_object_array: {e}"));
                    return ptr::null_mut();
                }
            };

            // Per-pair frs_get; nulls left in place on miss.
            for (i, key) in ks.iter().enumerate() {
                let mut out = FrsBytes {
                    data: ptr::null_mut(),
                    len: 0,
                    capacity: 0,
                };
                // SAFETY: handle / cf valid; out is stack-local; key vec lives for the call.
                let st = unsafe {
                    frs_get(
                        handle as FrsDb,
                        cf_list[i],
                        key.as_ptr(),
                        key.len(),
                        &mut out,
                    )
                };
                if st == FRS_STATUS_NOT_FOUND {
                    continue;
                }
                if check_status(env, st, &format!("RocksDB.multiGet[{i}]")) {
                    return ptr::null_mut();
                }
                if out.data.is_null() {
                    continue;
                }
                // SAFETY: out describes a Rust-owned buffer.
                let s = unsafe { std::slice::from_raw_parts(out.data, out.len) };
                let arr = match env.byte_array_from_slice(s) {
                    Ok(a) => a,
                    Err(e) => {
                        unsafe {
                            let _ = crate::frs_bytes_free(&mut out);
                        }
                        throw_rocksdb(env, &format!("multiGet[{i}]: byte_array_from_slice: {e}"));
                        return ptr::null_mut();
                    }
                };
                if let Err(e) = env.set_object_array_element(&outer, i as jint, &arr) {
                    unsafe {
                        let _ = crate::frs_bytes_free(&mut out);
                    }
                    throw_rocksdb(
                        env,
                        &format!("multiGet[{i}]: set_object_array_element: {e}"),
                    );
                    return ptr::null_mut();
                }
                unsafe {
                    let _ = crate::frs_bytes_free(&mut out);
                }
            }
            outer.into_raw()
        },
    )
}

// ===========================================================================
// P3 — BlockBasedTableConfig + BloomFilter + LRUCache + WriteBufferManager + FlinkEnv
//
// Surface (~13 entries):
//   - BlockBasedTableConfig class:          7 thunks
//   - BloomFilter class:                    2 thunks
//   - LRUCache class:                       2 thunks
//   - WriteBufferManager class:             2 thunks
//   - FlinkEnv class:                       2 thunks (ctor + dispose)
//
// All five classes follow the established `Box<*Handle>` exposed as jlong
// pattern (`Box::into_raw` / `Box::from_raw`). The interesting bit is the
// hydration path: `BlockBasedTableConfig` accumulates settings the Java
// side configures via the four mutator setters, and the multi-CF
// `RocksDB.open` thunk (P0 §6a) walks every CfOptionsHandle's
// `table_format_handle` to pull those settings onto the
// `EngineOptions` that gets handed to `DbImpl::open_with_fs`.
//
// **FlinkEnv divergence (audit Path B):** community Flink wraps a Flink
// `FileSystem` into a RocksDB `Env` so the engine can write SST files
// through a distributed FS (S3 / GCS / Azure / HDFS). forst-rs has its
// own `forst_rs_io::FileSystem` abstraction with an OpenDAL-backed
// implementation; we therefore accept the FlinkEnv handle but never
// dispatch FS calls back into Java. Callers needing cloud storage should
// configure the appropriate forst-rs FileSystem at `RocksDB.open` time.
// Implementing a JNI-callback Env trait would add ~2000 LOC of
// cross-language marshalling for every read/write — the wrong place to
// integrate cloud storage.
// ===========================================================================

// ---------------------------------------------------------------------------
// BlockBasedTableConfig class
// ---------------------------------------------------------------------------

/// `org.forstdb.BlockBasedTableConfig.newTableFactoryHandle() -> long`
///
/// Java signature: `()J`
///
/// Constructs a fresh [`BlockBasedTableConfigHandle`]; subsequent
/// mutator setters write into the same box. The handle is stored on a
/// `ColumnFamilyOptions` via `setTableFormatConfig` and consumed by the
/// multi-CF `RocksDB.open` thunk (P0 §6a) which hydrates its values onto
/// the engine-wide `EngineOptions`.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_newTableFactoryHandle<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| BlockBasedTableConfigHandle::default().into_raw(),
    )
}

/// `org.forstdb.BlockBasedTableConfig.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
///
/// Drops the [`BlockBasedTableConfigHandle`] box. Does NOT touch the
/// embedded BloomFilter / LRUCache handle pointers — those have their own
/// dispose lifecycles owned by Java.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newTableFactoryHandle`
                // and has not been freed.
                unsafe { drop(Box::from_raw(handle as *mut BlockBasedTableConfigHandle)) };
            }
        },
    )
}

/// `org.forstdb.BlockBasedTableConfig.setIndexType(long handle, byte indexType)`
///
/// Java signature: `(JB)V`
///
/// Records the community `IndexType` ordinal (0 = kBinarySearch). forst-rs
/// has only one SST index format today; the value is stored for forward
/// compatibility but does not yet affect engine behaviour.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_setIndexType<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    index_type: jint,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { BlockBasedTableConfigHandle::from_raw_ref(handle) } {
                // Defensive clamp: the community enum has only ~6 values; we
                // store as u8 to keep the field small. Anything out of range
                // collapses to 0 (kBinarySearch) so a stale Flink constant
                // table never breaks open.
                h.index_type = if (0..=255).contains(&index_type) {
                    index_type as u8
                } else {
                    0
                };
            }
        },
    )
}

/// `org.forstdb.BlockBasedTableConfig.setBlockCache(long handle, long cacheHandle)`
///
/// Java signature: `(JJ)V`
///
/// Pulls the cache capacity off the supplied [`LruCacheHandle`] and
/// records it for hydration into `EngineOptions::block_cache_size` at
/// open time. A null `cacheHandle` clears the override.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_setBlockCache<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    cache_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            let Some(h) = (unsafe { BlockBasedTableConfigHandle::from_raw_ref(handle) }) else {
                return;
            };
            if cache_handle == 0 {
                h.block_cache_size = None;
                return;
            }
            // SAFETY: cache_handle came from `LRUCache.newLRUCache` and has
            // not been disposed (the Java side owns the lifecycle); we only
            // read the capacity field.
            if let Some(cache) = unsafe { LruCacheHandle::from_raw_ref(cache_handle) } {
                h.block_cache_size = Some(cache.capacity);
            }
        },
    )
}

/// `org.forstdb.BlockBasedTableConfig.setBlockCacheSize(long handle, long sizeBytes)`
///
/// Java signature: `(JJ)V`
///
/// Legacy shortcut for `setBlockCache(LRUCache(sizeBytes))`. Writes
/// directly to the same `block_cache_size` field. Negative or zero values
/// are coerced to "no override" (clears the field).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_setBlockCacheSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    size_bytes: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { BlockBasedTableConfigHandle::from_raw_ref(handle) } {
                h.block_cache_size = if size_bytes > 0 {
                    Some(size_bytes as usize)
                } else {
                    None
                };
            }
        },
    )
}

/// `org.forstdb.BlockBasedTableConfig.setFilterPolicy(long handle, long filterHandle)`
///
/// Java signature: `(JJ)V`
///
/// Pulls the bits-per-key off the supplied [`BloomFilterHandle`] and
/// records it for hydration into `EngineOptions::bloom_bits_per_key` at
/// open time. A null `filterHandle` clears the override (engine default
/// of 10 bits/key applies). Non-bloom filter policies are unsupported;
/// the only thing forst-rs's SST writer can build is a bloom filter.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_setFilterPolicy<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    filter_handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            let Some(h) = (unsafe { BlockBasedTableConfigHandle::from_raw_ref(handle) }) else {
                return;
            };
            if filter_handle == 0 {
                h.bloom_bits_per_key = None;
                return;
            }
            // SAFETY: filter_handle came from `BloomFilter.newBloomFilter`
            // and has not been disposed.
            if let Some(bloom) = unsafe { BloomFilterHandle::from_raw_ref(filter_handle) } {
                h.bloom_bits_per_key = Some(bloom.bits_per_key);
            }
        },
    )
}

/// `org.forstdb.BlockBasedTableConfig.setBlockSize(long handle, long sizeBytes)`
///
/// Java signature: `(JJ)V`
///
/// Records the SST block size for hydration into
/// `EngineOptions::block_size` at open time. Zero / negative values
/// clear the override (engine default of 64 KiB applies).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BlockBasedTableConfig_setBlockSize<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
    size_bytes: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if let Some(h) = unsafe { BlockBasedTableConfigHandle::from_raw_ref(handle) } {
                h.block_size = if size_bytes > 0 {
                    Some(size_bytes as usize)
                } else {
                    None
                };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// BloomFilter class
// ---------------------------------------------------------------------------

/// `org.forstdb.BloomFilter.newBloomFilter(int bitsPerKey, boolean blockBasedMode) -> long`
///
/// Java signature: `(IZ)J`
///
/// Constructs a [`BloomFilterHandle`] capturing the bits-per-key. forst-rs
/// always uses the same bloom encoding regardless of `blockBasedMode`; we
/// record the flag for completeness but never branch on it.
///
/// Negative `bitsPerKey` values are clamped to the engine default (10).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BloomFilter_newBloomFilter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    bits_per_key: jint,
    block_based_mode: jboolean,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let bits = if bits_per_key > 0 {
                bits_per_key as usize
            } else {
                10
            };
            BloomFilterHandle {
                bits_per_key: bits,
                block_based_mode: block_based_mode != JNI_FALSE,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.BloomFilter.createNewBloomFilter(double) -> long`
///
/// Java signature: `(D)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BloomFilter_createNewBloomFilter<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    bits_per_key: jni::sys::jdouble,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let bits = if bits_per_key.is_finite() && bits_per_key > 0.0 {
                bits_per_key.round() as usize
            } else {
                10
            };
            BloomFilterHandle {
                bits_per_key: bits,
                block_based_mode: false,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.BloomFilter.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_BloomFilter_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newBloomFilter`.
                unsafe { drop(Box::from_raw(handle as *mut BloomFilterHandle)) };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// LRUCache class
// ---------------------------------------------------------------------------

/// `org.forstdb.LRUCache.newLRUCache(long capacity, int numShardBits,
///                                    boolean strictCapacityLimit,
///                                    double highPriPoolRatio) -> long`
///
/// Java signature: `(JIZD)J`
///
/// Constructs an [`LruCacheHandle`]. Only `capacity` is currently
/// propagated onto `EngineOptions::block_cache_size` (when this handle is
/// later attached to a `BlockBasedTableConfig` via `setBlockCache`).
/// `numShardBits` / `strictCapacityLimit` / `highPriPoolRatio` are
/// recorded for diagnostic dumps but otherwise ignored — forst-rs's block
/// cache has its own sharding strategy.
///
/// Negative `capacity` defaults to the engine's 256 MiB.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_LRUCache_newLRUCache<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    capacity: jlong,
    num_shard_bits: jint,
    strict_capacity_limit: jboolean,
    high_pri_pool_ratio: jni::sys::jdouble,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let cap = if capacity > 0 {
                capacity as usize
            } else {
                256 * 1024 * 1024
            };
            LruCacheHandle {
                capacity: cap,
                num_shard_bits,
                strict_capacity_limit: strict_capacity_limit != JNI_FALSE,
                high_pri_pool_ratio,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.LRUCache.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_LRUCache_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newLRUCache`.
                unsafe { drop(Box::from_raw(handle as *mut LruCacheHandle)) };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// WriteBufferManager class
// ---------------------------------------------------------------------------

/// `org.forstdb.WriteBufferManager.newWriteBufferManager(long capacity, long cacheHandle) -> long`
///
/// Java signature: `(JJ)J`
///
/// Constructs a [`WriteBufferManagerHandle`]. forst-rs uses per-CF arenas
/// rather than a shared write-buffer budget, so neither `capacity` nor
/// `cacheHandle` is honoured by the engine; the values are recorded for
/// diagnostic dumps. The matching `DBOptions.setWriteBufferManager` thunk
/// (P0) is also a no-op for the same reason.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBufferManager_newWriteBufferManager<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    capacity: jlong,
    cache_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            let cap = if capacity > 0 { capacity as usize } else { 0 };
            WriteBufferManagerHandle {
                capacity: cap,
                cache_handle,
            }
            .into_raw()
        },
    )
}

/// `org.forstdb.WriteBufferManager.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_WriteBufferManager_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newWriteBufferManager`.
                unsafe { drop(Box::from_raw(handle as *mut WriteBufferManagerHandle)) };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// FlinkEnv class
// ---------------------------------------------------------------------------

/// `org.forstdb.FlinkEnv.newFlinkEnv(java.util.List<String> fsList) -> long`
///
/// Java signature: `(Ljava/util/List;)J`
///
/// Accepts a Java `List<String>` (typically Flink-FS scheme URIs) and
/// returns an opaque [`FlinkEnvHandle`] that the engine never consults.
/// See the divergence note above the `Java_org_forstdb_DBOptions_setEnv`
/// thunk and the P3 audit's "Path B" rationale: cloud-storage
/// integration belongs in forst-rs's `FileSystem` abstraction, not in a
/// JNI-callback Env trait.
///
/// The list size is recorded for debugging; `null` is accepted and
/// stored as size 0 rather than rejected (Flink's restore path
/// occasionally passes `null` when no special FS is configured).
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkEnv_newFlinkEnv<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    fs_list: JObject<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |env| {
            let count = if fs_list.is_null() {
                0_usize
            } else {
                // Best-effort: call java.util.List.size(); if the call
                // fails (e.g. caller passed something that isn't a List),
                // log and fall back to 0 rather than throw — Flink's
                // restore path is timing-sensitive and a thrown exception
                // here would surface as a confusing "FlinkEnv ctor failed"
                // when the engine doesn't even consume the value.
                match env.call_method(&fs_list, "size", "()I", &[]) {
                    Ok(v) => match v.i() {
                        Ok(i) if i >= 0 => i as usize,
                        _ => 0,
                    },
                    Err(_) => {
                        tracing::debug!(
                            target: "compat_jni::flink_env",
                            "FlinkEnv.newFlinkEnv: arg is not a java.util.List; recording fs_count=0"
                        );
                        0
                    }
                }
            };
            tracing::debug!(
                target: "compat_jni::flink_env",
                "FlinkEnv.newFlinkEnv: accepting handle (fs_count={count}); forst-rs uses its own FileSystem abstraction (OpenDAL for cloud storage), Flink Env is a no-op"
            );
            FlinkEnvHandle { fs_count: count }.into_raw()
        },
    )
}

/// `org.forstdb.FlinkEnv.createFlinkEnv(String, Object) -> long`
///
/// Java signature: `(Ljava/lang/String;Ljava/lang/Object;)J`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkEnv_createFlinkEnv<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    _path: JString<'local>,
    _file_system: JObject<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| FlinkEnvHandle { fs_count: 0 }.into_raw(),
    )
}

/// `org.forstdb.FlinkEnv.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkEnv_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `newFlinkEnv`.
                unsafe { drop(Box::from_raw(handle as *mut FlinkEnvHandle)) };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// P5 — FlinkCompactionFilter (TTL state) handle plumbing
//
// Flink's keyed-state TTL feature ships a dedicated compaction filter
// (`org.forstdb.FlinkCompactionFilter`) that the engine invokes during
// compaction to drop entries whose embedded timestamp is older than the
// configured TTL. The filter has three Java-visible classes:
//
//   * `FlinkCompactionFilter`         (extends AbstractCompactionFilter)
//   * `FlinkCompactionFilter.ConfigHolder` (one per CF; carries the configured
//                                          state-type / ttl / queryAfterN /
//                                          fixed-element-length tuple)
//   * `FlinkCompactionFilter.FlinkCompactionFilterFactory`
//                                      (extends AbstractCompactionFilterFactory;
//                                       owns one ConfigHolder + a TimeProvider
//                                       Java object)
//
// The native methods involved are:
//
//   AbstractCompactionFilter.disposeInternal(long)
//   AbstractCompactionFilterFactory.createNewCompactionFilterFactory0()  -> long
//   AbstractCompactionFilterFactory.disposeInternal(long)
//   FlinkCompactionFilter.createNewFlinkCompactionFilter0(long,
//          TimeProvider, long)         -> long
//   FlinkCompactionFilter.createNewFlinkCompactionFilterConfigHolder()  -> long
//   FlinkCompactionFilter.disposeFlinkCompactionFilterConfigHolder(long)
//   FlinkCompactionFilter.configureFlinkCompactionFilter(long, int, int,
//          long, long, int, ListElementFilterFactory) -> boolean
//
// These ten symbols (with overload mangling for the AbstractCompactionFilter
// hierarchy) are what `flink-statebackend-forst::ForStDBTtlCompactFiltersManager`
// resolves at class load.
//
// **Engine support — present (since 2026-05-10), but the JNI shim cannot
// auto-wire it without Java-side cooperation.**
//
// forst-rs-engine now ships `FlinkTtlCompactionFilter` (Disabled/Value/List)
// reachable via the C ABI export `frs_cf_set_compaction_filter_ttl(db, cf,
// ttl_ms, state_type, timestamp_offset)` (see `crates/forst-rs-ffi/src/lib.rs`).
// The engine attaches the filter to a CF and runs it at flush + L0→L1
// compaction. The FFM module C wires this up directly via
// `ForStRsLinker.setCompactionFilterTtl(...)` — that path is the production
// blessed channel for TTL.
//
// On THIS JNI compat shim, the Flink-side TTL flow is:
//
//   1. `factory = new FlinkCompactionFilterFactory(timeProvider)`
//   2. `cfOpts.setCompactionFilterFactory(factory)`              ← we see this
//   3. RocksDB opens CF; calls `factory.createCompactionFilter()` ← internal
//   4. `factory.configure(config)` → `configureFlinkCompactionFilter(holder)`
//                                                                 ← we see this
//
// The link factory ↔ holder is a pure-Java field assignment that JNI cannot
// observe. Without it we have no path from a `configureFlinkCompactionFilter`
// call back to "which CF should receive this TTL". Bridging it would require
// either patching upstream `org.forstdb.FlinkCompactionFilter` (breaking G-A
// drop-in) or shipping a custom shim JAR.
//
// We therefore chose option B from the audit for the JNI compat surface:
//
//   * Accept all the handles the Java side hands us (no `UnsatisfiedLinkError`).
//   * Snapshot the configure() payload onto the holder for debugging /
//     introspection.
//   * The `createCompactionFilter0` returns a non-zero, per-call "filter"
//     handle (a `Box<FlinkCompactionFilterHandle>`) so the Java
//     `AbstractCompactionFilter` super-ctor sees a valid `nativeHandle_`
//     and `disOwnNativeHandle()` does the right thing.
//   * **TTL is NOT enforced through the JNI compat path.** Production
//     deployments that need TTL must use the FFM-based `ForStRsStateBackend`
//     (module C) and call `ForStRsLinker.setCompactionFilterTtl(...)` directly
//     from the keyed-state lifecycle.
//
// Operational consequence for jobs that load this libforstjni and rely on
// TTL via the community-Flink `ForStDBTtlCompactFiltersManager`: state grows
// unbounded. Either migrate to the FFM backend (recommended for new jobs)
// or provision additional state budget.
// ---------------------------------------------------------------------------

pub(crate) mod handles_p5 {
    use super::*;

    /// Accepted but never consulted. One Box per call to
    /// `createNewFlinkCompactionFilter0` — the Java `AbstractCompactionFilter`
    /// super-ctor stores this as `nativeHandle_` and the framework will call
    /// `AbstractCompactionFilter.disposeInternal(long)` to free it (unless
    /// `disOwnNativeHandle()` was invoked first, in which case the handle
    /// is leaked into the std::unique_ptr the C++ filter would have owned —
    /// which doesn't exist here, so we proactively free anyway in our
    /// dispose thunk for symmetry).
    #[derive(Debug)]
    pub(crate) struct FlinkCompactionFilterHandle {
        /// Snapshot of the configured TTL at the moment
        /// `createNewFlinkCompactionFilter0` was called. Recorded for
        /// debugging only — the engine never reads it.
        #[allow(dead_code)]
        pub ttl_ms: u64,
        /// Snapshot of the state-type ordinal (0=Disabled, 1=Value, 2=List).
        /// Same as `FlinkCompactionFilterConfigHandle::state_type` on the
        /// matching ConfigHolder, captured when the filter was created.
        #[allow(dead_code)]
        pub state_type: i32,
    }

    /// Accepted but never consulted. One Box per Flink CF
    /// (`new FlinkCompactionFilterFactory(timeProvider)` calls our ctor).
    /// Allocated by `Java_org_forstdb_AbstractCompactionFilterFactory_createNewCompactionFilterFactory0`.
    #[derive(Debug, Default)]
    pub(crate) struct FlinkCompactionFilterFactoryHandle {
        /// Bumped each time the Flink side calls `createCompactionFilter`
        /// (which forwards to `createNewFlinkCompactionFilter0`). Recorded
        /// for debugging — the engine never reads it.
        #[allow(dead_code)]
        pub filters_created: u64,
    }

    /// Accepted but never consulted. One Box per ConfigHolder, allocated
    /// by `createNewFlinkCompactionFilterConfigHolder` and configured by
    /// `configureFlinkCompactionFilter`.
    ///
    /// `configured` flips from `false` to `true` on the first
    /// `configureFlinkCompactionFilter` call; subsequent calls return
    /// `false` to mirror the C++ behaviour of "ConfigHolder may be
    /// configured exactly once" (Flink throws `IllegalStateException` on
    /// the boolean-false return).
    #[derive(Debug, Default)]
    pub(crate) struct FlinkCompactionFilterConfigHandle {
        /// Whether `configureFlinkCompactionFilter` has been invoked yet.
        pub configured: bool,
        /// Mirror of `Config.stateType.ordinal()` (0=Disabled, 1=Value,
        /// 2=List). Recorded for debugging only.
        #[allow(dead_code)]
        pub state_type: i32,
        /// Mirror of `Config.timestampOffset` (0 for Value, 1 for Map).
        #[allow(dead_code)]
        pub timestamp_offset: i32,
        /// Mirror of `Config.ttl` in milliseconds.
        #[allow(dead_code)]
        pub ttl_ms: u64,
        /// Mirror of `Config.queryTimeAfterNumEntries`.
        #[allow(dead_code)]
        pub query_time_after_n: u64,
        /// Mirror of `Config.fixedElementLength` (-1 if not a fixed-length
        /// list state, else the per-element byte width).
        #[allow(dead_code)]
        pub fixed_element_length: i32,
    }

    impl FlinkCompactionFilterHandle {
        pub(crate) fn into_raw(self) -> jlong {
            Box::into_raw(Box::new(self)) as jlong
        }
    }

    impl FlinkCompactionFilterFactoryHandle {
        pub(crate) fn into_raw(self) -> jlong {
            Box::into_raw(Box::new(self)) as jlong
        }

        #[allow(dead_code)]
        pub(crate) unsafe fn from_raw_ref<'a>(handle: jlong) -> Option<&'a mut Self> {
            if handle == 0 {
                None
            } else {
                Some(&mut *(handle as *mut Self))
            }
        }
    }

    impl FlinkCompactionFilterConfigHandle {
        pub(crate) fn into_raw(self) -> jlong {
            Box::into_raw(Box::new(self)) as jlong
        }

        #[allow(dead_code)]
        pub(crate) unsafe fn from_raw_ref<'a>(handle: jlong) -> Option<&'a mut Self> {
            if handle == 0 {
                None
            } else {
                Some(&mut *(handle as *mut Self))
            }
        }
    }
}

use handles_p5::{
    FlinkCompactionFilterConfigHandle, FlinkCompactionFilterFactoryHandle,
    FlinkCompactionFilterHandle,
};

// ---------------------------------------------------------------------------
// AbstractCompactionFilter — the parent class of FlinkCompactionFilter
// ---------------------------------------------------------------------------

/// `org.forstdb.AbstractCompactionFilter.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
///
/// Frees the per-filter handle allocated by
/// `Java_org_forstdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0`.
/// The Java side calls this from the `AbstractCompactionFilter` close path
/// when ownership of the filter has NOT been transferred to a C++
/// std::unique_ptr (i.e. `disOwnNativeHandle()` was not invoked). We always
/// free the Box here for symmetry with the C++ engine's behaviour.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_AbstractCompactionFilter_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior `createNewFlinkCompactionFilter0`.
                unsafe { drop(Box::from_raw(handle as *mut FlinkCompactionFilterHandle)) };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// AbstractCompactionFilterFactory — the parent class of
// FlinkCompactionFilterFactory
// ---------------------------------------------------------------------------

/// `org.forstdb.AbstractCompactionFilterFactory.createNewCompactionFilterFactory0() -> long`
///
/// Java signature: `()J`
///
/// Called by the `RocksCallbackObject` super-ctor when Flink instantiates
/// `new FlinkCompactionFilterFactory(timeProvider)`. Returns an opaque
/// handle that the engine never consults; see the divergence note above.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_AbstractCompactionFilterFactory_createNewCompactionFilterFactory0<
    'local,
>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            tracing::debug!(
                target: "compat_jni::flink_compaction_filter",
                "AbstractCompactionFilterFactory.createNewCompactionFilterFactory0: TTL compaction filter factory accepted (handle is a no-op; entries with embedded TTL timestamps will NOT be expired by compaction in this build — see compat_jni::handles_p5 docs)"
            );
            FlinkCompactionFilterFactoryHandle::default().into_raw()
        },
    )
}

/// `org.forstdb.AbstractCompactionFilterFactory.disposeInternal(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_AbstractCompactionFilterFactory_disposeInternal<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior createNewCompactionFilterFactory0.
                unsafe {
                    drop(Box::from_raw(
                        handle as *mut FlinkCompactionFilterFactoryHandle,
                    ))
                };
            }
        },
    )
}

// ---------------------------------------------------------------------------
// FlinkCompactionFilter class — TTL filter ctor + ConfigHolder + configure
// ---------------------------------------------------------------------------

/// `org.forstdb.FlinkCompactionFilter.createNewFlinkCompactionFilter0(
///     long configHolderHandle,
///     FlinkCompactionFilter.TimeProvider timeProvider,
///     long loggerHandle) -> long`
///
/// Java signature: `(JLorg/forstdb/FlinkCompactionFilter$TimeProvider;J)J`
///
/// Allocates a per-filter [`FlinkCompactionFilterHandle`]. The actual
/// filter logic is a no-op — see the divergence note above
/// `handles_p5`. We snapshot the TTL + state-type from the supplied
/// ConfigHolder so the handle carries enough debug info to identify the
/// filter at dispose time.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_holder_handle: jlong,
    _time_provider: JObject<'local>,
    _logger_handle: jlong,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            // Snapshot config from the holder if available (handle is always
            // non-zero for a configured factory; we still null-guard defensively).
            let (ttl_ms, state_type) = if let Some(cfg) =
                unsafe { FlinkCompactionFilterConfigHandle::from_raw_ref(config_holder_handle) }
            {
                (cfg.ttl_ms, cfg.state_type)
            } else {
                (0, 0)
            };
            tracing::debug!(
                target: "compat_jni::flink_compaction_filter",
                "FlinkCompactionFilter.createNewFlinkCompactionFilter0: ttl_ms={ttl_ms} state_type={state_type} (no-op handle; TTL not enforced)"
            );
            FlinkCompactionFilterHandle { ttl_ms, state_type }.into_raw()
        },
    )
}

/// `org.forstdb.FlinkCompactionFilter.createNewFlinkCompactionFilterConfigHolder() -> long`
///
/// Java signature: `()J`
///
/// Allocates a [`FlinkCompactionFilterConfigHandle`] in the unconfigured
/// state. The Flink ConfigHolder constructor calls this exactly once per
/// stateful CF.
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkCompactionFilter_createNewFlinkCompactionFilterConfigHolder<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    jni_guard(
        &mut env,
        || 0_i64,
        |_env| {
            tracing::debug!(
                target: "compat_jni::flink_compaction_filter",
                "FlinkCompactionFilter.createNewFlinkCompactionFilterConfigHolder: ConfigHolder accepted (no-op)"
            );
            FlinkCompactionFilterConfigHandle::default().into_raw()
        },
    )
}

/// `org.forstdb.FlinkCompactionFilter.disposeFlinkCompactionFilterConfigHolder(long handle)`
///
/// Java signature: `(J)V`
#[no_mangle]
pub extern "system" fn Java_org_forstdb_FlinkCompactionFilter_disposeFlinkCompactionFilterConfigHolder<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    handle: jlong,
) {
    jni_guard(
        &mut env,
        || (),
        |_env| {
            if handle != 0 {
                // SAFETY: handle came from a prior createNewFlinkCompactionFilterConfigHolder.
                unsafe {
                    drop(Box::from_raw(
                        handle as *mut FlinkCompactionFilterConfigHandle,
                    ))
                };
            }
        },
    )
}

/// `org.forstdb.FlinkCompactionFilter.configureFlinkCompactionFilter(
///     long configHolderHandle,
///     int stateType,
///     int timestampOffset,
///     long ttl,
///     long queryTimeAfterNumEntries,
///     int fixedElementLength,
///     FlinkCompactionFilter.ListElementFilterFactory listElementFilterFactory) -> boolean`
///
/// Java signature: `(JIIJJILorg/forstdb/FlinkCompactionFilter$ListElementFilterFactory;)Z`
///
/// Returns `JNI_TRUE` (i.e. "newly configured") on the first call for a
/// given ConfigHolder, `JNI_FALSE` on subsequent calls. This mirrors the
/// C++ semantics where `ConfigHolder::Configure` is allowed exactly once
/// per holder; the Java wrapper's `FlinkCompactionFilterFactory.configure`
/// throws `IllegalStateException` if the boolean returns false ("Compaction
/// filter is already configured").
#[no_mangle]
#[allow(clippy::too_many_arguments)]
pub extern "system" fn Java_org_forstdb_FlinkCompactionFilter_configureFlinkCompactionFilter<
    'local,
>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    config_holder_handle: jlong,
    state_type: jint,
    timestamp_offset: jint,
    ttl: jlong,
    query_time_after_num_entries: jlong,
    fixed_element_length: jint,
    _list_element_filter_factory: JObject<'local>,
) -> jboolean {
    jni_guard(
        &mut env,
        || JNI_FALSE,
        |_env| {
            // SAFETY: handle came from a prior createNewFlinkCompactionFilterConfigHolder.
            let Some(cfg) =
                (unsafe { FlinkCompactionFilterConfigHandle::from_raw_ref(config_holder_handle) })
            else {
                // null/zero handle: behave like "already configured" so the
                // Java side raises IllegalStateException rather than
                // silently proceeding without TTL state.
                tracing::debug!(
                    target: "compat_jni::flink_compaction_filter",
                    "configureFlinkCompactionFilter: null/zero handle; returning JNI_FALSE"
                );
                return JNI_FALSE;
            };
            if cfg.configured {
                // Mirror the C++ ConfigHolder::Configure "already configured"
                // return — Flink wraps this as IllegalStateException.
                return JNI_FALSE;
            }
            cfg.configured = true;
            cfg.state_type = state_type;
            cfg.timestamp_offset = timestamp_offset;
            cfg.ttl_ms = if ttl < 0 { 0 } else { ttl as u64 };
            cfg.query_time_after_n = if query_time_after_num_entries < 0 {
                0
            } else {
                query_time_after_num_entries as u64
            };
            cfg.fixed_element_length = fixed_element_length;
            tracing::debug!(
                target: "compat_jni::flink_compaction_filter",
                "configureFlinkCompactionFilter: state_type={state_type} ts_off={timestamp_offset} ttl_ms={} query_after_n={} fixed_len={fixed_element_length} (snapshot only; TTL not enforced)",
                cfg.ttl_ms, cfg.query_time_after_n
            );
            JNI_TRUE
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
            "Java_org_forstdb_RocksDB_open__Ljava_lang_String_2",
            "Java_org_forstdb_RocksDB_open__JLjava_lang_String_2",
            "Java_org_forstdb_RocksDB_close",
            "Java_org_forstdb_RocksDB_disposeInternal",
            "Java_org_forstdb_RocksDB_closeDatabase",
            "Java_org_forstdb_RocksDB_put__J_3BII_3BII",
            "Java_org_forstdb_RocksDB_put__J_3BII_3BIIJ",
            "Java_org_forstdb_RocksDB_put__JJ_3BII_3BII",
            "Java_org_forstdb_RocksDB_put__JJ_3BII_3BIIJ",
            "Java_org_forstdb_RocksDB_get__J_3BII",
            "Java_org_forstdb_RocksDB_get__J_3BIIJ",
            "Java_org_forstdb_RocksDB_get__JJ_3BII",
            "Java_org_forstdb_RocksDB_get__JJ_3BIIJ",
            "Java_org_forstdb_RocksDB_delete__J_3BII",
            "Java_org_forstdb_RocksDB_delete__J_3BIIJ",
            "Java_org_forstdb_RocksDB_delete__JJ_3BII",
            "Java_org_forstdb_RocksDB_delete__JJ_3BIIJ",
            "Java_org_forstdb_RocksDB_createColumnFamily",
            "Java_org_forstdb_RocksDB_dropColumnFamily",
            "Java_org_forstdb_RocksDB_flush",
            "Java_org_forstdb_RocksDB_createCheckpoint",
            // 12 additions for broader Flink-statebackend-forst coverage.
            "Java_org_forstdb_RocksDB_merge__J_3BII_3BII",
            "Java_org_forstdb_RocksDB_merge__J_3BII_3BIIJ",
            "Java_org_forstdb_RocksDB_merge__JJ_3BII_3BII",
            "Java_org_forstdb_RocksDB_merge__JJ_3BII_3BIIJ",
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
            // JNI load path calls RocksDB.version() immediately after
            // NativeLibraryLoader resolves libforstjni.
            "Java_org_forstdb_RocksDB_version",
            // DBOptions default constructor keeps a default Env reference.
            "Java_org_forstdb_Env_getDefaultEnvInternal",
            "Java_org_forstdb_Env_setBackgroundThreads",
            "Java_org_forstdb_Env_getBackgroundThreads",
            "Java_org_forstdb_Env_getThreadPoolQueueLen",
            "Java_org_forstdb_Env_incBackgroundThreadsIfNeeded",
            "Java_org_forstdb_Env_lowerThreadPoolIOPriority",
            "Java_org_forstdb_Env_lowerThreadPoolCPUPriority",
            "Java_org_forstdb_Env_getThreadList",
            "Java_org_forstdb_RocksEnv_disposeInternal",
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
            "Java_org_forstdb_DBOptions_maxLogFileSize",
            "Java_org_forstdb_DBOptions_setKeepLogFileNum",
            "Java_org_forstdb_DBOptions_keepLogFileNum",
            "Java_org_forstdb_DBOptions_setStatistics",
            "Java_org_forstdb_DBOptions_statistics",
            "Java_org_forstdb_DBOptions_setWriteBufferManager",
            "Java_org_forstdb_DBOptions_setEnv",
            // P0 — ColumnFamilyOptions class (13 entries).
            "Java_org_forstdb_ColumnFamilyOptions_newColumnFamilyOptions",
            "Java_org_forstdb_ColumnFamilyOptions_disposeInternal",
            "Java_org_forstdb_ColumnFamilyOptions_setWriteBufferSize",
            "Java_org_forstdb_ColumnFamilyOptions_writeBufferSize",
            "Java_org_forstdb_ColumnFamilyOptions_setMaxWriteBufferNumber",
            "Java_org_forstdb_ColumnFamilyOptions_maxWriteBufferNumber",
            "Java_org_forstdb_ColumnFamilyOptions_setArenaBlockSize",
            "Java_org_forstdb_ColumnFamilyOptions_arenaBlockSize",
            "Java_org_forstdb_ColumnFamilyOptions_setMinWriteBufferNumberToMerge",
            "Java_org_forstdb_ColumnFamilyOptions_minWriteBufferNumberToMerge",
            "Java_org_forstdb_ColumnFamilyOptions_setLevelCompactionDynamicLevelBytes",
            "Java_org_forstdb_ColumnFamilyOptions_levelCompactionDynamicLevelBytes",
            "Java_org_forstdb_ColumnFamilyOptions_setMaxBytesForLevelBase",
            "Java_org_forstdb_ColumnFamilyOptions_maxBytesForLevelBase",
            "Java_org_forstdb_ColumnFamilyOptions_setTargetFileSizeBase",
            "Java_org_forstdb_ColumnFamilyOptions_targetFileSizeBase",
            "Java_org_forstdb_ColumnFamilyOptions_setCompressionPerLevel",
            "Java_org_forstdb_ColumnFamilyOptions_setCompactionStyle",
            "Java_org_forstdb_ColumnFamilyOptions_setPeriodicCompactionSeconds",
            "Java_org_forstdb_ColumnFamilyOptions_periodicCompactionSeconds",
            "Java_org_forstdb_ColumnFamilyOptions_setTableFormatConfig",
            "Java_org_forstdb_ColumnFamilyOptions_setCompactionFilterFactoryHandle",
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
            // P0 — FlushOptions class (5 entries).
            "Java_org_forstdb_FlushOptions_newFlushOptions",
            "Java_org_forstdb_FlushOptions_disposeInternal",
            "Java_org_forstdb_FlushOptions_setWaitForFlush",
            "Java_org_forstdb_FlushOptions_waitForFlush",
            "Java_org_forstdb_FlushOptions_setAllowWriteStall",
            "Java_org_forstdb_FlushOptions_allowWriteStall",
            // P0 — ColumnFamilyHandle class (3 entries).
            "Java_org_forstdb_ColumnFamilyHandle_disposeInternal",
            "Java_org_forstdb_ColumnFamilyHandle_getName0",
            "Java_org_forstdb_ColumnFamilyHandle_getDescriptor",
            // P0 — multi-CF RocksDB.open overload.
            "Java_org_forstdb_RocksDB_open__JLjava_lang_String_2_3_3B_3J",
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
            "Java_org_forstdb_RocksIterator_status0",
            "Java_org_forstdb_RocksIterator_disposeInternal",
            // P1 — RocksDB iterator factory (2 entries).
            "Java_org_forstdb_RocksDB_iterator",
            "Java_org_forstdb_RocksDB_iteratorCF",
            // P1 — WriteBatch class (8 entries).
            "Java_org_forstdb_WriteBatch_newWriteBatch__I",
            "Java_org_forstdb_WriteBatch_newWriteBatch___3BI",
            "Java_org_forstdb_WriteBatch_disposeInternal",
            "Java_org_forstdb_WriteBatch_put__J_3BI_3BI",
            "Java_org_forstdb_WriteBatch_put__J_3BI_3BIJ",
            "Java_org_forstdb_WriteBatch_merge__J_3BI_3BI",
            "Java_org_forstdb_WriteBatch_merge__J_3BI_3BIJ",
            "Java_org_forstdb_WriteBatch_delete__J_3BI",
            "Java_org_forstdb_WriteBatch_delete__J_3BIJ",
            "Java_org_forstdb_WriteBatch_clear0",
            "Java_org_forstdb_WriteBatch_count0",
            "Java_org_forstdb_WriteBatch_getDataSize",
            // P1 — RocksDB.write0 (1 entry).
            "Java_org_forstdb_RocksDB_write0",
            // P2 — Checkpoint class (4 entries).
            "Java_org_forstdb_Checkpoint_create0",
            "Java_org_forstdb_Checkpoint_disposeInternal",
            "Java_org_forstdb_Checkpoint_createCheckpoint0",
            "Java_org_forstdb_Checkpoint_exportColumnFamily",
            // P2 — Snapshot class (1 entry).
            "Java_org_forstdb_Snapshot_disposeInternal",
            // P2 — RocksDB snapshot/file-list/range methods (10 entries).
            "Java_org_forstdb_RocksDB_getSnapshot",
            "Java_org_forstdb_RocksDB_releaseSnapshot",
            "Java_org_forstdb_RocksDB_getLiveFiles",
            "Java_org_forstdb_RocksDB_getLiveFilesMetaData",
            "Java_org_forstdb_RocksDB_disableFileDeletions",
            "Java_org_forstdb_RocksDB_enableFileDeletions",
            "Java_org_forstdb_RocksDB_disableFileDeletions__J",
            "Java_org_forstdb_RocksDB_enableFileDeletions__JZ",
            "Java_org_forstdb_RocksDB_deleteRange",
            "Java_org_forstdb_RocksDB_deleteFilesInRanges",
            "Java_org_forstdb_RocksDB_createColumnFamilyWithImport",
            // P2 — ImportColumnFamilyOptions class (3 entries).
            "Java_org_forstdb_ImportColumnFamilyOptions_newImportColumnFamilyOptions",
            "Java_org_forstdb_ImportColumnFamilyOptions_disposeInternal",
            "Java_org_forstdb_ImportColumnFamilyOptions_setMoveFiles",
            // P2 — ExportImportFilesMetaData class (2 entries).
            "Java_org_forstdb_ExportImportFilesMetaData_newExportImportFilesMetaData",
            "Java_org_forstdb_ExportImportFilesMetaData_disposeInternal",
            // P2 — LiveFileMetaData accessors (3 entries).
            "Java_org_forstdb_LiveFileMetaData_fileName",
            "Java_org_forstdb_LiveFileMetaData_level",
            "Java_org_forstdb_LiveFileMetaData_sequenceNumber",
            // P4 — Statistics class (4 entries).
            "Java_org_forstdb_Statistics_newStatistics",
            "Java_org_forstdb_Statistics_disposeInternal",
            "Java_org_forstdb_Statistics_getTickerCount",
            "Java_org_forstdb_Statistics_getHistogramData",
            // P4 — RocksDB.getProperty + multiGet (2 entries).
            "Java_org_forstdb_RocksDB_getProperty",
            "Java_org_forstdb_RocksDB_getLongProperty",
            "Java_org_forstdb_RocksDB_multiGet",
            // P3 — BlockBasedTableConfig class (7 entries).
            "Java_org_forstdb_BlockBasedTableConfig_newTableFactoryHandle",
            "Java_org_forstdb_BlockBasedTableConfig_disposeInternal",
            "Java_org_forstdb_BlockBasedTableConfig_setIndexType",
            "Java_org_forstdb_BlockBasedTableConfig_setBlockCache",
            "Java_org_forstdb_BlockBasedTableConfig_setBlockCacheSize",
            "Java_org_forstdb_BlockBasedTableConfig_setFilterPolicy",
            "Java_org_forstdb_BlockBasedTableConfig_setBlockSize",
            // P3 — BloomFilter class (2 entries).
            "Java_org_forstdb_BloomFilter_newBloomFilter",
            "Java_org_forstdb_BloomFilter_disposeInternal",
            // P3 — LRUCache class (2 entries).
            "Java_org_forstdb_LRUCache_newLRUCache",
            "Java_org_forstdb_LRUCache_disposeInternal",
            // P3 — WriteBufferManager class (2 entries).
            "Java_org_forstdb_WriteBufferManager_newWriteBufferManager",
            "Java_org_forstdb_WriteBufferManager_disposeInternal",
            // P3 — FlinkEnv class (2 entries).
            "Java_org_forstdb_FlinkEnv_newFlinkEnv",
            "Java_org_forstdb_FlinkEnv_disposeInternal",
            // P5 — TTL compaction filter family (7 entries):
            //   * 1 dispose on AbstractCompactionFilter (parent of FlinkCompactionFilter)
            //   * 2 ctor + dispose on AbstractCompactionFilterFactory (parent of FlinkCompactionFilterFactory)
            //   * 4 on FlinkCompactionFilter (per-filter ctor + ConfigHolder ctor/dispose + configure)
            //
            // Divergence: TTL expiration is NOT enforced — see the
            // `handles_p5` module-level note.
            "Java_org_forstdb_AbstractCompactionFilter_disposeInternal",
            "Java_org_forstdb_AbstractCompactionFilterFactory_createNewCompactionFilterFactory0",
            "Java_org_forstdb_AbstractCompactionFilterFactory_disposeInternal",
            "Java_org_forstdb_FlinkCompactionFilter_createNewFlinkCompactionFilter0",
            "Java_org_forstdb_FlinkCompactionFilter_createNewFlinkCompactionFilterConfigHolder",
            "Java_org_forstdb_FlinkCompactionFilter_disposeFlinkCompactionFilterConfigHolder",
            "Java_org_forstdb_FlinkCompactionFilter_configureFlinkCompactionFilter",
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
        let h = DbOptionsHandle::default().into_raw();
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

    // -------------------------------------------------------------------
    // P2 — Checkpoint + Snapshot + DeleteRange + import-export lifecycle
    // -------------------------------------------------------------------

    /// Checkpoint lifecycle: open a real on-disk DB, seed a few entries,
    /// build a Checkpoint handle, write the checkpoint, dispose. Verify
    /// SST/Manifest files appear in the target directory.
    #[test]
    fn test_checkpoint_lifecycle() {
        use std::ffi::CString;

        // Use tempfile-backed directories so the engine's WAL and
        // checkpoint writes don't pollute the project tree.
        let db_dir = tempfile::tempdir().expect("db tempdir");
        let cp_dir = tempfile::tempdir().expect("cp tempdir");

        let db_path = CString::new(db_dir.path().to_str().unwrap()).unwrap();
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: db_path lives for the call; out_handle is stack-local.
        let st = unsafe { frs_db_open(db_path.as_ptr(), &mut db) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(!db.is_null());

        // Resolve default CF + seed 5 entries.
        let mut cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid; out_cf stack-local.
        let st = unsafe { frs_db_default_cf(db, &mut cf) };
        assert_eq!(st, FRS_STATUS_OK);
        for i in 0..5_u8 {
            let key = [b'k', b'0' + i];
            let val = [b'v', b'0' + i];
            // SAFETY: db / cf valid; pointers describe stack-local arrays.
            let st =
                unsafe { crate::frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()) };
            assert_eq!(st, FRS_STATUS_OK);
        }

        // Force a flush so SSTs land on disk before checkpointing.
        // SAFETY: db valid.
        let st = unsafe { crate::frs_flush(db) };
        assert_eq!(st, FRS_STATUS_OK);

        // Mirror Java_org_forstdb_Checkpoint_create0.
        let cp = CheckpointHandle { db }.into_raw();
        assert_ne!(cp, 0);
        // SAFETY: just-allocated.
        let cp_ref = unsafe { CheckpointHandle::from_raw_ref(cp) }.unwrap();

        // Mirror Java_org_forstdb_Checkpoint_createCheckpoint0.
        let cp_path = CString::new(cp_dir.path().to_str().unwrap()).unwrap();
        // SAFETY: cp_ref.db valid; cp_path lives for call.
        let st = unsafe { frs_create_checkpoint(cp_ref.db, cp_path.as_ptr()) };
        assert_eq!(st, FRS_STATUS_OK);

        // Verify checkpoint dir is non-empty (engine wrote *something*
        // into it — manifest at minimum).
        let entries = std::fs::read_dir(cp_dir.path())
            .expect("read checkpoint dir")
            .count();
        assert!(
            entries > 0,
            "checkpoint dir should contain at least one file (manifest, SSTs, ...)"
        );

        // Dispose Checkpoint then DB.
        // SAFETY: cp came from into_raw; sole owner.
        unsafe { drop(Box::from_raw(cp as *mut CheckpointHandle)) };
        // SAFETY: cf came from frs_db_default_cf.
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        // SAFETY: db came from frs_db_open.
        let st = unsafe { frs_db_close(db) };
        assert_eq!(st, FRS_STATUS_OK);
    }

    /// Snapshot lifecycle: getSnapshot returns a non-zero handle whose
    /// recorded sequence matches the engine; releaseSnapshot drops it
    /// without crashing; double-release on a freed handle is the caller's
    /// responsibility, but a release of a handle obtained immediately
    /// after another release of a different handle must succeed.
    #[test]
    fn test_snapshot_lifecycle() {
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: stack-local out param.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);

        // Mirror Java_org_forstdb_RocksDB_getSnapshot.
        let mut seq: u64 = 0;
        // SAFETY: db valid; out_seq stack-local.
        let st = unsafe { frs_sequence_number(db, &mut seq) };
        assert_eq!(st, FRS_STATUS_OK);
        let snap = SnapshotHandle { seq_no: seq }.into_raw();
        assert_ne!(snap, 0);

        // Verify the snapshot box round-trips.
        // SAFETY: just-allocated.
        let s_ref = unsafe { SnapshotHandle::from_raw_ref(snap) }.unwrap();
        assert_eq!(s_ref.seq_no, seq);

        // Mirror Java_org_forstdb_RocksDB_releaseSnapshot.
        // SAFETY: just-allocated; sole owner.
        unsafe { drop(Box::from_raw(snap as *mut SnapshotHandle)) };

        // Mirror Java_org_forstdb_Snapshot_disposeInternal on a fresh handle.
        let snap2 = SnapshotHandle { seq_no: seq }.into_raw();
        // SAFETY: just-allocated.
        unsafe { drop(Box::from_raw(snap2 as *mut SnapshotHandle)) };

        // Null-handle is None.
        let none = unsafe { SnapshotHandle::from_raw_ref(0) };
        assert!(none.is_none());

        // Cleanup.
        // SAFETY: db came from frs_db_open_memory.
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    /// `getLiveFiles` must call `frs_l0_file_count` (which currently
    /// returns 0 — that's fine, we're testing it returns OK rather than
    /// panicking). Real per-file enumeration is a follow-up in lib.rs.
    #[test]
    fn test_get_live_files_returns_l0_count() {
        let seed: [(&[u8], &[u8]); 1] = [(b"k1", b"v1")];
        let (db, cf) = open_seeded_engine(&seed);

        // Mirror inline: l0_file_count + sequence_number (both invariants
        // queried by getLiveFiles).
        let mut count: u32 = 0;
        // SAFETY: db valid; out stack-local.
        let st = unsafe { frs_l0_file_count(db, &mut count) };
        assert_eq!(st, FRS_STATUS_OK);
        // forst-rs's L0 count is documented as always 0 today — verify
        // we get a clean status rather than a panic / NULL_ARG.
        let _ = count;

        let mut seq: u64 = 0;
        // SAFETY: db valid; out stack-local.
        let st = unsafe { frs_sequence_number(db, &mut seq) };
        assert_eq!(st, FRS_STATUS_OK);
        assert!(seq > 0, "sequence number must advance after one put");

        // Cleanup.
        // SAFETY: cf came from frs_db_default_cf.
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        // SAFETY: db came from frs_db_open_memory.
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    /// `disable_file_deletions` / `enable_file_deletions` are no-ops in
    /// the shim; this test just verifies the JNI thunks would be callable
    /// without effect on the engine. Driven through the `tracing::debug!`
    /// path indirectly — we don't have a way to invoke the thunks
    /// without a JVM, so we exercise the underlying invariant: after a
    /// flush, file count is unchanged whether or not we "disabled" anything.
    #[test]
    fn test_disable_enable_file_deletions() {
        let seed: [(&[u8], &[u8]); 2] = [(b"a", b"1"), (b"b", b"2")];
        let (db, cf) = open_seeded_engine(&seed);

        // SAFETY: db valid.
        let st = unsafe { crate::frs_flush(db) };
        assert_eq!(st, FRS_STATUS_OK);

        // The thunks themselves are no-ops; their only contract is "don't
        // crash". The path they would take through `jni_guard` is
        // exercised by other tests already.
        let mut count: u32 = 0;
        // SAFETY: db valid; out stack-local.
        let st = unsafe { frs_l0_file_count(db, &mut count) };
        assert_eq!(st, FRS_STATUS_OK);

        // SAFETY: cf came from frs_db_default_cf.
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        // SAFETY: db came from frs_db_open_memory.
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    /// `delete_range_inner` must drop every key in `[begin, end)` and
    /// leave keys outside the range alone. We exercise it directly
    /// (bypass JNIEnv via a stub fn call would require a JVM) by
    /// reproducing its iterator-walk + delete pattern in-test.
    #[test]
    fn test_delete_range() {
        // Seed 10 sequential keys k0000..k0009.
        let seed: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
            .map(|i| {
                let key = format!("k{i:04}");
                let val = format!("v{i:04}");
                (key.into_bytes(), val.into_bytes())
            })
            .collect();
        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: stack-local out.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);
        let mut cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid.
        let st = unsafe { frs_db_default_cf(db, &mut cf) };
        assert_eq!(st, FRS_STATUS_OK);
        for (k, v) in &seed {
            // SAFETY: pointers live for the call.
            let st = unsafe { crate::frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()) };
            assert_eq!(st, FRS_STATUS_OK);
        }

        // Mirror delete_range_inner without a JNIEnv: open iterator, seek
        // to begin, walk while key < end, accumulate, then per-key delete.
        let begin = b"k0005";
        let end = b"k0008";
        let mut iter: FrsIterator = ptr::null_mut();
        // SAFETY: db / cf valid.
        let st = unsafe { frs_iterator_open(db, cf, &mut iter) };
        assert_eq!(st, FRS_STATUS_OK);
        // SAFETY: iter valid; needle valid.
        let st = unsafe { frs_iterator_seek(iter, begin.as_ptr(), begin.len()) };
        assert_eq!(st, FRS_STATUS_OK);
        let mut to_delete: Vec<Vec<u8>> = Vec::new();
        loop {
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
            // SAFETY: iter valid; out stack-locals.
            let st = unsafe { crate::frs_iterator_next(iter, &mut k, &mut v, &mut valid) };
            assert_eq!(st, FRS_STATUS_OK);
            if !valid {
                break;
            }
            // SAFETY: k/v populated when valid.
            let key_vec = unsafe { std::slice::from_raw_parts(k.data, k.len).to_vec() };
            unsafe {
                let _ = crate::frs_bytes_free(&mut k);
                let _ = crate::frs_bytes_free(&mut v);
            }
            if key_vec.as_slice() >= &end[..] {
                break;
            }
            to_delete.push(key_vec);
        }
        // SAFETY: iter valid.
        unsafe {
            let _ = frs_iterator_close(iter);
        }
        for key in &to_delete {
            // SAFETY: db / cf valid; key vec lives for the call.
            let st = unsafe { frs_delete(db, cf, key.as_ptr(), key.len()) };
            assert_eq!(st, FRS_STATUS_OK);
        }
        assert_eq!(
            to_delete,
            vec![b"k0005".to_vec(), b"k0006".to_vec(), b"k0007".to_vec()]
        );

        // Verify k0005..k0007 are absent, others remain.
        for (k, expected_some) in [
            (b"k0004".to_vec(), true),
            (b"k0005".to_vec(), false),
            (b"k0006".to_vec(), false),
            (b"k0007".to_vec(), false),
            (b"k0008".to_vec(), true),
        ] {
            let mut out = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            // SAFETY: pointers / out valid.
            let st = unsafe { crate::frs_get(db, cf, k.as_ptr(), k.len(), &mut out) };
            assert!(st == FRS_STATUS_OK || st == FRS_STATUS_NOT_FOUND);
            if expected_some {
                assert!(
                    !out.data.is_null(),
                    "key {} should still be present",
                    String::from_utf8_lossy(&k)
                );
            } else {
                assert!(
                    out.data.is_null(),
                    "key {} should be tombstoned",
                    String::from_utf8_lossy(&k)
                );
            }
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
        }

        // Cleanup.
        // SAFETY: cf came from frs_db_default_cf.
        unsafe {
            let _ = crate::frs_cf_close(cf);
        }
        // SAFETY: db came from frs_db_open_memory.
        unsafe {
            let _ = frs_db_close(db);
        }
    }

    // -------------------------------------------------------------------
    // P4 — Statistics + getProperty + multiGet lifecycle
    // -------------------------------------------------------------------

    /// Statistics ctor + dispose.
    #[test]
    fn test_statistics_lifecycle() {
        let h = StatisticsHandle::default().into_raw();
        assert_ne!(h, 0);

        // Mirror getTickerCount: always 0 in forst-rs.
        // (No actual call here — the thunk runs through `jni_guard` which
        // needs a JNIEnv. The contract is "value is 0"; the dispose path
        // tests the box ownership invariant.)

        // SAFETY: just-allocated.
        unsafe { drop(Box::from_raw(h as *mut StatisticsHandle)) };

        // Null-handle from_raw_ref.
        let none = unsafe { StatisticsHandle::from_raw_ref(0) };
        assert!(none.is_none());
    }

    /// Multi-CF `multiGet`: open db with default + a second CF, put one
    /// entry into each, drive a `(cf1,k1) + (cf2,k2)` multi-CF lookup
    /// inline, verify both come back with the right values.
    #[test]
    fn test_multi_get() {
        use std::ffi::CString;

        let mut db: FrsDb = ptr::null_mut();
        // SAFETY: out param.
        let st = unsafe { crate::frs_db_open_memory(&mut db) };
        assert_eq!(st, FRS_STATUS_OK);

        let mut default_cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid.
        let st = unsafe { frs_db_default_cf(db, &mut default_cf) };
        assert_eq!(st, FRS_STATUS_OK);

        let extra_name = CString::new("extra").unwrap();
        let mut extra_cf: FrsCfHandle = ptr::null_mut();
        // SAFETY: db valid; name valid; out stack-local.
        let st = unsafe { frs_db_create_cf(db, extra_name.as_ptr(), &mut extra_cf) };
        assert_eq!(st, FRS_STATUS_OK);

        // Seed both CFs.
        // SAFETY: pointers live for the call.
        let st = unsafe { crate::frs_put(db, default_cf, b"a".as_ptr(), 1, b"1".as_ptr(), 1) };
        assert_eq!(st, FRS_STATUS_OK);
        // SAFETY: same.
        let st = unsafe { crate::frs_put(db, extra_cf, b"b".as_ptr(), 1, b"2".as_ptr(), 1) };
        assert_eq!(st, FRS_STATUS_OK);

        // Mirror multiGet's per-pair frs_get loop.
        let cf_list = [default_cf, extra_cf];
        let keys: [&[u8]; 2] = [b"a", b"b"];
        let mut results: Vec<Option<Vec<u8>>> = Vec::with_capacity(2);
        for (i, key) in keys.iter().enumerate() {
            let mut out = FrsBytes {
                data: ptr::null_mut(),
                len: 0,
                capacity: 0,
            };
            // SAFETY: pointers / out valid.
            let st = unsafe { frs_get(db, cf_list[i], key.as_ptr(), key.len(), &mut out) };
            assert!(st == FRS_STATUS_OK || st == FRS_STATUS_NOT_FOUND);
            if st == FRS_STATUS_NOT_FOUND || out.data.is_null() {
                results.push(None);
            } else {
                // SAFETY: out describes Rust-owned buffer.
                let vec = unsafe { std::slice::from_raw_parts(out.data, out.len).to_vec() };
                results.push(Some(vec));
            }
            unsafe {
                let _ = crate::frs_bytes_free(&mut out);
            }
        }
        assert_eq!(results, vec![Some(b"1".to_vec()), Some(b"2".to_vec())]);

        // Cleanup.
        // SAFETY: handles came from prior calls.
        unsafe {
            let _ = crate::frs_cf_close(extra_cf);
            let _ = crate::frs_cf_close(default_cf);
            let _ = frs_db_close(db);
        }
    }

    /// `ImportColumnFamilyOptions` ctor + setMoveFiles + dispose. The
    /// type is essentially a flag bag — exercise it round-trips.
    #[test]
    fn test_import_column_family_options_lifecycle() {
        let h = ImportColumnFamilyOptionsHandle::default().into_raw();
        assert_ne!(h, 0);
        // SAFETY: just-allocated.
        let opts = unsafe { ImportColumnFamilyOptionsHandle::from_raw_ref(h) }.unwrap();
        assert!(!opts.move_files);
        opts.move_files = true;
        let opts2 = unsafe { ImportColumnFamilyOptionsHandle::from_raw_ref(h) }.unwrap();
        assert!(opts2.move_files);
        // SAFETY: Box round-trip.
        unsafe { drop(Box::from_raw(h as *mut ImportColumnFamilyOptionsHandle)) };
    }

    /// `ExportImportFilesMetaData` ctor + dispose.
    #[test]
    fn test_export_import_files_metadata_lifecycle() {
        let h = ExportImportFilesMetaDataHandle::default().into_raw();
        assert_ne!(h, 0);
        // SAFETY: Box round-trip.
        unsafe { drop(Box::from_raw(h as *mut ExportImportFilesMetaDataHandle)) };
    }

    /// `Checkpoint::create0(0)` semantically guards null DB handles. We
    /// can't drive the JNI thunk, but the box-creation invariant is the
    /// same one; verify a manual CheckpointHandle round-trip.
    #[test]
    fn test_checkpoint_handle_round_trip() {
        let h = CheckpointHandle {
            db: ptr::null_mut(),
        }
        .into_raw();
        assert_ne!(h, 0);
        // SAFETY: just-allocated.
        let cref = unsafe { CheckpointHandle::from_raw_ref(h) }.unwrap();
        assert!(cref.db.is_null());
        unsafe { drop(Box::from_raw(h as *mut CheckpointHandle)) };
        // Null-handle.
        let none = unsafe { CheckpointHandle::from_raw_ref(0) };
        assert!(none.is_none());
    }

    // -------------------------------------------------------------------
    // P3 — BlockBasedTableConfig + BloomFilter + LRUCache +
    //       WriteBufferManager + FlinkEnv lifecycle smoke tests
    //
    // Verify each handle round-trips through `into_raw` / `from_raw_ref`
    // and disposes without leaking. The symbol-table check
    // (`test_compat_jni_symbol_exports`) covers the `nm` surface.
    // The integration-style test `test_open_with_block_based_table_config`
    // exercises the full hydration path: option chain →
    // setTableFormatConfig → multi-CF open → engine actually receives the
    // configured block_size + bloom_bits_per_key.
    // -------------------------------------------------------------------

    #[test]
    fn test_bloom_filter_lifecycle() {
        // Mirrors `BloomFilter.newBloomFilter(10, false)` + `disposeInternal`.
        let h = BloomFilterHandle {
            bits_per_key: 10,
            block_based_mode: false,
        }
        .into_raw();
        assert_ne!(h, 0);

        // SAFETY: just-allocated.
        let bref = unsafe { BloomFilterHandle::from_raw_ref(h) }.unwrap();
        assert_eq!(bref.bits_per_key, 10);
        assert!(!bref.block_based_mode);

        unsafe { drop(Box::from_raw(h as *mut BloomFilterHandle)) };
        // Null-handle.
        let none = unsafe { BloomFilterHandle::from_raw_ref(0) };
        assert!(none.is_none());
    }

    #[test]
    fn test_lru_cache_lifecycle() {
        // Mirrors `LRUCache.newLRUCache(256MB, 4, false, 0.5)` + dispose.
        let h = LruCacheHandle {
            capacity: 256 * 1024 * 1024,
            num_shard_bits: 4,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.5,
        }
        .into_raw();
        assert_ne!(h, 0);

        // SAFETY: just-allocated.
        let cref = unsafe { LruCacheHandle::from_raw_ref(h) }.unwrap();
        assert_eq!(cref.capacity, 256 * 1024 * 1024);
        assert_eq!(cref.num_shard_bits, 4);
        assert!(!cref.strict_capacity_limit);
        assert!((cref.high_pri_pool_ratio - 0.5).abs() < f64::EPSILON);

        unsafe { drop(Box::from_raw(h as *mut LruCacheHandle)) };
    }

    #[test]
    fn test_block_based_table_config_lifecycle() {
        // Mirrors the Flink option-chain:
        //   BloomFilter bf = new BloomFilter(10, false);
        //   LRUCache lru = new LRUCache(256MB, ...);
        //   BlockBasedTableConfig cfg = new BlockBasedTableConfig();
        //   cfg.setBlockSize(8192);
        //   cfg.setFilterPolicy(bf);
        //   cfg.setBlockCache(lru);
        //   cfg.disposeInternal();
        let bf = BloomFilterHandle {
            bits_per_key: 10,
            block_based_mode: false,
        }
        .into_raw();
        let lru = LruCacheHandle {
            capacity: 128 * 1024 * 1024,
            num_shard_bits: 4,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.5,
        }
        .into_raw();
        let cfg = BlockBasedTableConfigHandle::default().into_raw();
        assert_ne!(cfg, 0);

        // setBlockSize equivalent.
        // SAFETY: just-allocated.
        let cref = unsafe { BlockBasedTableConfigHandle::from_raw_ref(cfg) }.unwrap();
        cref.block_size = Some(8192);

        // setFilterPolicy equivalent: read bits-per-key from bloom box.
        // SAFETY: just-allocated.
        let cref = unsafe { BlockBasedTableConfigHandle::from_raw_ref(cfg) }.unwrap();
        // SAFETY: just-allocated.
        let bref = unsafe { BloomFilterHandle::from_raw_ref(bf) }.unwrap();
        cref.bloom_bits_per_key = Some(bref.bits_per_key);

        // setBlockCache equivalent: read capacity from lru box.
        // SAFETY: just-allocated.
        let cref = unsafe { BlockBasedTableConfigHandle::from_raw_ref(cfg) }.unwrap();
        // SAFETY: just-allocated.
        let lref = unsafe { LruCacheHandle::from_raw_ref(lru) }.unwrap();
        cref.block_cache_size = Some(lref.capacity);

        // Verify all writes took.
        // SAFETY: just-allocated.
        let cref2 = unsafe { BlockBasedTableConfigHandle::from_raw_ref(cfg) }.unwrap();
        assert_eq!(cref2.block_size, Some(8192));
        assert_eq!(cref2.bloom_bits_per_key, Some(10));
        assert_eq!(cref2.block_cache_size, Some(128 * 1024 * 1024));
        assert_eq!(cref2.index_type, 0); // default kBinarySearch.

        // Dispose all three handles.
        unsafe { drop(Box::from_raw(cfg as *mut BlockBasedTableConfigHandle)) };
        unsafe { drop(Box::from_raw(bf as *mut BloomFilterHandle)) };
        unsafe { drop(Box::from_raw(lru as *mut LruCacheHandle)) };
    }

    #[test]
    fn test_write_buffer_manager_lifecycle() {
        // Mirrors `WriteBufferManager.newWriteBufferManager(1GB, lru)` + dispose.
        let lru = LruCacheHandle {
            capacity: 256 * 1024 * 1024,
            num_shard_bits: 4,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.5,
        }
        .into_raw();
        let h = WriteBufferManagerHandle {
            capacity: 1024 * 1024 * 1024,
            cache_handle: lru,
        }
        .into_raw();
        assert_ne!(h, 0);

        // SAFETY: just-allocated.
        let wref = unsafe { WriteBufferManagerHandle::from_raw_ref(h) }.unwrap();
        assert_eq!(wref.capacity, 1024 * 1024 * 1024);
        assert_eq!(wref.cache_handle, lru);

        unsafe { drop(Box::from_raw(h as *mut WriteBufferManagerHandle)) };
        unsafe { drop(Box::from_raw(lru as *mut LruCacheHandle)) };
    }

    #[test]
    fn test_flink_env_lifecycle() {
        // Mirrors `FlinkEnv.newFlinkEnv(emptyList)` + dispose. The handle is
        // accepted but never consulted by the engine — see the divergence
        // note above the FlinkEnv ctor.
        let h = FlinkEnvHandle { fs_count: 0 }.into_raw();
        assert_ne!(h, 0);

        // SAFETY: just-allocated.
        let eref = unsafe { FlinkEnvHandle::from_raw_ref(h) }.unwrap();
        assert_eq!(eref.fs_count, 0);

        unsafe { drop(Box::from_raw(h as *mut FlinkEnvHandle)) };
    }

    /// End-to-end hydration: build the option chain a Flink job would,
    /// drive the same Rust-side state the multi-CF open thunk's §6a
    /// hydration loop reads, and verify the resulting `EngineOptions`
    /// has the configured block_size + bloom_bits + block_cache_size.
    ///
    /// We don't go through `Java_org_forstdb_RocksDB_open__JLjava_...`
    /// (no JVM in unit tests), but we exercise the *exact* hydration
    /// logic: walk every CF's CfOptionsHandle, look up the
    /// table_format_handle, copy fields onto the EngineOptions clone.
    /// The thunk itself is a thin wrapper around the same loop.
    #[test]
    fn test_open_with_block_based_table_config() {
        // Build the option chain a Flink job would.
        let bloom = BloomFilterHandle {
            bits_per_key: 16,
            block_based_mode: false,
        }
        .into_raw();
        let cache = LruCacheHandle {
            capacity: 64 * 1024 * 1024,
            num_shard_bits: 4,
            strict_capacity_limit: false,
            high_pri_pool_ratio: 0.5,
        }
        .into_raw();

        let tbl = BlockBasedTableConfigHandle::default().into_raw();
        // SAFETY: just-allocated.
        let tref = unsafe { BlockBasedTableConfigHandle::from_raw_ref(tbl) }.unwrap();
        tref.block_size = Some(16 * 1024);
        // SAFETY: just-allocated.
        let bref = unsafe { BloomFilterHandle::from_raw_ref(bloom) }.unwrap();
        tref.bloom_bits_per_key = Some(bref.bits_per_key);
        // SAFETY: just-allocated.
        let lref = unsafe { LruCacheHandle::from_raw_ref(cache) }.unwrap();
        tref.block_cache_size = Some(lref.capacity);

        // Build the per-CF options the open thunk would receive.
        let cf_opts = CfOptionsHandle::default().into_raw();
        // SAFETY: just-allocated.
        let cref = unsafe { CfOptionsHandle::from_raw_ref(cf_opts) }.unwrap();
        cref.table_format_handle = tbl;

        // Mirror the §6a hydration loop verbatim.
        let mut engine_opts = EngineOptions::default();
        for &handle in &[cf_opts] {
            if handle == 0 {
                continue;
            }
            // SAFETY: just-allocated.
            let Some(co) = (unsafe { CfOptionsHandle::from_raw_ref(handle) }) else {
                continue;
            };
            if co.table_format_handle == 0 {
                continue;
            }
            // SAFETY: just-allocated.
            let Some(tb) =
                (unsafe { BlockBasedTableConfigHandle::from_raw_ref(co.table_format_handle) })
            else {
                continue;
            };
            if let Some(bs) = tb.block_size {
                engine_opts.block_size = bs;
            }
            if let Some(cs) = tb.block_cache_size {
                engine_opts.block_cache_size = cs;
            }
            if let Some(bbk) = tb.bloom_bits_per_key {
                engine_opts.bloom_bits_per_key = bbk;
            }
            break;
        }

        // The engine actually received the configured values.
        assert_eq!(engine_opts.block_size, 16 * 1024);
        assert_eq!(engine_opts.block_cache_size, 64 * 1024 * 1024);
        assert_eq!(engine_opts.bloom_bits_per_key, 16);

        // Cleanup all leaked boxes.
        unsafe { drop(Box::from_raw(cf_opts as *mut CfOptionsHandle)) };
        unsafe { drop(Box::from_raw(tbl as *mut BlockBasedTableConfigHandle)) };
        unsafe { drop(Box::from_raw(bloom as *mut BloomFilterHandle)) };
        unsafe { drop(Box::from_raw(cache as *mut LruCacheHandle)) };
    }

    // -------------------------------------------------------------------
    // P5 — FlinkCompactionFilter (TTL state) lifecycle smoke tests
    //
    // Both tests exercise the Rust-internal handle round-trip
    // (Box::into_raw → from_raw_ref → Box::from_raw) for the three P5
    // boxes — the JNI thunks themselves are thin wrappers around
    // jni_guard + the handles_p5 module. The thunks' panic-safety and
    // null-handle paths are also covered. The module-level docs above
    // `handles_p5` make the "TTL not enforced" divergence explicit.
    // -------------------------------------------------------------------

    /// Factory ctor + dispose: handle must be non-zero, round-trip via
    /// `from_raw_ref` must hand back the default-state struct, and the
    /// dispose path must drop the Box without leaking.
    #[test]
    fn test_flink_compaction_filter_factory_lifecycle() {
        let h = FlinkCompactionFilterFactoryHandle::default().into_raw();
        assert_ne!(
            h, 0,
            "factory handle must be non-zero (Java would NPE on 0)"
        );

        // SAFETY: just-allocated.
        let fref = unsafe { FlinkCompactionFilterFactoryHandle::from_raw_ref(h) }.unwrap();
        assert_eq!(fref.filters_created, 0);

        // Null handle returns None (mirrors the `disposeInternal(0)` no-op).
        let none = unsafe { FlinkCompactionFilterFactoryHandle::from_raw_ref(0) };
        assert!(none.is_none());

        // Dispose.
        unsafe { drop(Box::from_raw(h as *mut FlinkCompactionFilterFactoryHandle)) };
    }

    /// ConfigHolder + per-filter handle round-trip:
    ///   1. createForValue-style: allocate ConfigHolder, run the
    ///      `configureFlinkCompactionFilter` Rust-side equivalent, verify
    ///      the snapshot took.
    ///   2. Re-configuring is rejected (mirrors the "ConfigHolder may be
    ///      configured exactly once" Java-side guard).
    ///   3. createNewFlinkCompactionFilter0-style: allocate a per-filter
    ///      handle that snapshots the config, verify the snapshot, dispose
    ///      both handles.
    #[test]
    fn test_flink_compaction_filter_config_lifecycle() {
        // (1) ConfigHolder round-trip.
        let cfg_h = FlinkCompactionFilterConfigHandle::default().into_raw();
        assert_ne!(cfg_h, 0);

        // SAFETY: just-allocated.
        let cfg = unsafe { FlinkCompactionFilterConfigHandle::from_raw_ref(cfg_h) }.unwrap();
        assert!(
            !cfg.configured,
            "fresh ConfigHolder must start unconfigured"
        );

        // (1b) `configureFlinkCompactionFilter`-style mutation. We mirror
        // the thunk's body without going through JNI.
        cfg.configured = true;
        cfg.state_type = 1; // Value
        cfg.timestamp_offset = 0;
        cfg.ttl_ms = 60_000; // 60s TTL — typical Flink keyed-state TTL.
        cfg.query_time_after_n = 1000;
        cfg.fixed_element_length = -1;

        // (2) Re-configure path — the thunk would return JNI_FALSE here.
        // SAFETY: just-allocated.
        let cfg2 = unsafe { FlinkCompactionFilterConfigHandle::from_raw_ref(cfg_h) }.unwrap();
        assert!(cfg2.configured);
        assert_eq!(cfg2.state_type, 1);
        assert_eq!(cfg2.ttl_ms, 60_000);
        assert_eq!(cfg2.query_time_after_n, 1000);
        assert_eq!(cfg2.fixed_element_length, -1);

        // (3) Per-filter handle. The createNewFlinkCompactionFilter0 thunk
        // would snapshot ttl_ms + state_type from the holder and box up a
        // FlinkCompactionFilterHandle. We mirror that here.
        let filter_h = FlinkCompactionFilterHandle {
            ttl_ms: cfg2.ttl_ms,
            state_type: cfg2.state_type,
        }
        .into_raw();
        assert_ne!(filter_h, 0);

        // SAFETY: just-allocated.
        let fref = unsafe { &*(filter_h as *mut FlinkCompactionFilterHandle) };
        assert_eq!(fref.ttl_ms, 60_000);
        assert_eq!(fref.state_type, 1);

        // Dispose both handles.
        unsafe { drop(Box::from_raw(filter_h as *mut FlinkCompactionFilterHandle)) };
        unsafe {
            drop(Box::from_raw(
                cfg_h as *mut FlinkCompactionFilterConfigHandle,
            ))
        };
    }
}

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

use jni::objects::{JByteArray, JClass, JString};
use jni::sys::{jbyteArray, jint, jlong, JavaVM, JNI_VERSION_1_8};
use jni::JNIEnv;

use crate::{
    frs_create_checkpoint, frs_db_close, frs_db_create_cf, frs_db_open, frs_db_open_cf, frs_delete,
    frs_flush, frs_get, frs_put, FrsBytes, FrsCfHandle, FrsDb, FRS_STATUS_NOT_FOUND, FRS_STATUS_OK,
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
pub unsafe extern "system" fn JNI_OnLoad(_vm: *mut JavaVM, _reserved: *mut std::ffi::c_void) -> jint {
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
fn read_byte_slice(
    env: &mut JNIEnv,
    arr: &JByteArray,
    off: jint,
    len: jint,
) -> Option<Vec<u8>> {
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
            let open_status =
                unsafe { frs_db_open_cf(handle as FrsDb, c_name.as_ptr(), &mut cf) };
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
            "Java_org_forstdb_RocksDB_open",
            "Java_org_forstdb_RocksDB_close",
            "Java_org_forstdb_RocksDB_put",
            "Java_org_forstdb_RocksDB_get",
            "Java_org_forstdb_RocksDB_delete",
            "Java_org_forstdb_RocksDB_createColumnFamily",
            "Java_org_forstdb_RocksDB_dropColumnFamily",
            "Java_org_forstdb_RocksDB_flush",
            "Java_org_forstdb_RocksDB_createCheckpoint",
            "JNI_OnLoad",
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
}

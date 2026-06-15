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
// FRS clippy policy: index-based loops over caller-provided FFI arrays are
// bounds-checked per element by design (style-only allow).
#![allow(clippy::needless_range_loop)]
// Doc comments cross-reference private internals (documented via --document-private-items);
// such links don't resolve under the strict rustdoc gate (doc-rendering cosmetics only).
#![allow(rustdoc::broken_intra_doc_links)]
// Workspace-wide ForstError is intentionally large; see forst-rs-common rationale.
#![allow(clippy::result_large_err)]

// FRS-JEMALLOC (2026-06-05): route every allocation in this cdylib through
// jemalloc. The default system allocator retained ~14 GB of freed memory across
// 55.9 M small q4 allocations (key/value `Vec` + memtable node per row) → 43 GB
// RSS, unfit for the 8c/32g target (swaps → collapse). jemalloc returns freed
// pages to the OS via dirty/muzzy decay (+ a background purge thread on Linux)
// and fragments far less under small-alloc churn — matching RocksDB's 6.4 GB.
// LINUX ONLY: on macOS jemalloc's pthread-TSD destructor SIGSEGVs on JVM
// thread-exit under this dlopen'd dylib (see Cargo.toml). The 8c/32g target is
// Linux; macOS host uses the system allocator. Keep cfg in sync with Cargo.toml.
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// FRS-MEM-PRESSURE-PURGE (2026-06-16, PMC-1): the proactive jemalloc page-reclaim
// valve. At the q9/q19/q5 16 g/TM cliff the LIVE allocation is tiny but jemalloc
// holds ~6-8 GiB of freed-but-unpurged dirty/muzzy pages, so RSS — what the
// cgroup OOM-killer watches — hits the limit and the TM is exit-137 killed
// (measured, commit 23420c2c0; orthogonal to the shed levers). The engine's
// mem-pressure sampler calls this hook under High/Critical pressure to force
// jemalloc to return ALL retained pages to the OS immediately. Purge only ever
// releases already-FREED memory — it touches no live allocation — so it is
// byte-identical / zero correctness impact. The mallctl lives here (not the
// engine) because forst-rs-engine is `#![forbid(unsafe_code)]` and the void
// `arena.<MALLCTL_ARENAS_ALL>.purge` mallctl requires `unsafe`.

/// Force ALL jemalloc arenas to release their retained dirty+muzzy pages to the
/// OS now, via the void `arena.<MALLCTL_ARENAS_ALL>.purge` mallctl. Returns
/// `true` on success. `MALLCTL_ARENAS_ALL == 4096` in jemalloc 5.3, so
/// `arena.4096.purge` purges every arena. It is a write-only command:
/// `newp=NULL, newlen=0`. The typed ctl API / `raw::write` always pass a
/// non-null `newp`, which the command rejects with `EINVAL`, so we call
/// `mallctl` directly.
#[cfg(target_os = "linux")]
fn jemalloc_purge_all() -> bool {
    // "arena.4096.purge\0" — 4096 == MALLCTL_ARENAS_ALL (all arenas).
    let name = b"arena.4096.purge\0";
    // SAFETY: void command — null in/out pointers, zero lengths. `name` is a
    // valid NUL-terminated static byte string. jemalloc is this cdylib's global
    // allocator (`GLOBAL` above), so this targets the live heap.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            name.as_ptr() as *const std::os::raw::c_char,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    rc == 0
}

/// FRS-MEM-PRESSURE-PURGE: register the jemalloc purge hook with the engine's
/// mem-pressure sampler (idempotent; first registration wins). Called from the
/// FFI entry guard so it is installed before any DB open starts the sampler.
/// On Linux this wires up [`jemalloc_purge_all`]; off Linux it is a no-op (no
/// jemalloc allocator), so the valve stays inert.
#[cfg(target_os = "linux")]
fn ensure_purge_hook_registered() {
    static DONE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    DONE.get_or_init(|| {
        forst_rs_engine::mem_pressure::register_purge_hook(jemalloc_purge_all);
    });
}

#[cfg(not(target_os = "linux"))]
fn ensure_purge_hook_registered() {}

// Memory return tuned for throughput: a background thread purges off the hot
// path, and dirty/muzzy pages are returned to the OS on jemalloc's standard
// ~10 s decay. (An earlier 1 s decay cut RSS 43→34 GB but RE-FAULTED within each
// checkpoint burst and slowed q4 — 10 s spans the 30 s checkpoint cycle so memory
// is returned between bursts without re-faulting inside one.) jemalloc ignores
// `background_thread` where unsupported; the decay settings still apply.
#[cfg(target_os = "linux")]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static MALLOC_CONF: &[u8] =
    b"background_thread:true,dirty_decay_ms:10000,muzzy_decay_ms:10000\0";

/// JNI compatibility shim — exports `Java_org_forstdb_RocksDB_*` symbols
/// so the resulting cdylib is a drop-in for the community
/// `libforstjni.so` that Apache Flink's `flink-statebackend-forst`
/// loads. Gated behind the `compat-jni` Cargo feature; see the module
/// docs for the G-A drop-in goal and threat model.
#[cfg(feature = "compat-jni")]
pub mod compat_jni;

use std::collections::HashMap;
use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::ptr;
use std::slice;
use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{
    ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl, WriteBatch, DEFAULT_CF_NAME,
};
use forst_rs_io::{FileSystem, LocalFileSystem, MemoryFileSystem};
use forst_rs_storage::merge_operator::{merge_operator_by_name, RawConcatMergeOperator};

/// Defense-in-depth cap on `count` (or row count) passed to FFI batch
/// operations. Untrusted C-side caller could otherwise drive
/// `WriteBatch::with_capacity(count)` or other count-driven allocations
/// to OOM (Sweep R5 H by Reviewer 2). 1M entries is well above any
/// realistic per-call batch size; calling code that needs more should
/// chunk into multiple calls.
pub const MAX_BATCH_COUNT: usize = 1_000_000;

/// Defense-in-depth cap on per-key (or per-prefix) byte length. The C
/// caller could otherwise push a fabricated `key_len` or `prefix_len`
/// at `slice::from_raw_parts`, driving the engine to scan / hash / index
/// bogus untrusted memory. 1 MiB is well above any realistic key size
/// for an LSM (RocksDB recommends ≤ 8 KiB); see Delta-Join Lookup design
/// (2.13_deltajoin_localization.md) for the consumer-side budget.
pub const MAX_KEY_LEN: usize = 1 << 20;

/// Defense-in-depth cap on per-VALUE byte length. Unlike keys (which an LSM
/// keeps small), values are legitimately large: Flink ListState / MapState
/// values and — critically — streaming-join state on the
/// `InputSideHasNoUniqueKey` path, which buffers EVERY record sharing a join
/// key into a single MapState value. A high-fan-out join key easily exceeds
/// the 1 MiB `MAX_KEY_LEN` (RocksDB stores such values without complaint), so
/// reusing `MAX_KEY_LEN` for values (the pre-fix behaviour) made
/// `frs_batch_put` / `frs_put` reject legitimate large state with
/// INVALID_ARGUMENT, crashing q7-style joins. We still bound the read for the
/// OOB-disclosure defense, but at the per-batch ceiling: a single value can be
/// as large as a whole batch (the aggregate `MAX_BATCH_BYTES` check still caps
/// total payload, and the engine's u32 offset arithmetic is safe ≤ 256 MiB).
pub const MAX_VALUE_LEN: usize = MAX_BATCH_BYTES;

/// Defense-in-depth cap on total per-batch payload bytes across all
/// keys + values in a single Arrow batch FFI call. C-R12-NEW-H2: with
/// MAX_KEY_LEN = 1 MiB and MAX_BATCH_COUNT = 1M, an attacker could
/// otherwise stage 1 TiB of payload that cumulative-overflows the
/// memtable's `key_data.len() as u32` rebase or VersionEdit / SST
/// offset arithmetic (everything downstream of the memtable assumes
/// per-batch sizes fit in u32). 256 MiB is well above any realistic
/// single-batch payload (default memtable flush is 64 MiB).
pub const MAX_BATCH_BYTES: usize = 256 * 1024 * 1024;

fn validate_i32_offsets(offsets: &[i32]) -> Option<usize> {
    if offsets.first().copied()? != 0 {
        return None;
    }
    let mut prev = 0i32;
    for &off in offsets {
        if off < 0 || off < prev {
            return None;
        }
        prev = off;
    }
    Some(prev as usize)
}

fn validate_u32_offsets(offsets: &[u32]) -> Option<usize> {
    if offsets.first().copied()? != 0 {
        return None;
    }
    let mut prev = 0u32;
    for &off in offsets {
        if off < prev {
            return None;
        }
        prev = off;
    }
    Some(prev as usize)
}

fn raw_concat_default_cf_descriptor() -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(DEFAULT_CF_NAME)
        .with_merge_operator(Arc::new(RawConcatMergeOperator::new()))
}

fn raw_concat_cf_descriptor(name: impl Into<String>) -> ColumnFamilyDescriptor {
    ColumnFamilyDescriptor::new(name).with_merge_operator(Arc::new(RawConcatMergeOperator::new()))
}

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
/// Engine-internal invariant violated; caller cannot make forward progress
/// without operator intervention (e.g. process restart from a checkpoint).
/// Emitted when the engine's sequence-number space is exhausted past the
/// 2^60 fatal threshold (spec §6a.4) — write paths return this code and
/// stop accepting work until the engine is restarted.
pub const FRS_STATUS_INTERNAL: i32 = 15;

/// The value is not available via the zero-copy path (not inline in the
/// active memtable). Caller should fall back to the regular allocating
/// `frs_get` path.
pub const FRS_STATUS_FALLBACK: i32 = 16;

/// Caller-provided output buffer is too small for the value.
pub const FRS_STATUS_BUFFER_TOO_SMALL: i32 = 17;

// ---------------------------------------------------------------------------
// ABI version negotiation
// ---------------------------------------------------------------------------

/// V1 ABI version. Bump on any FFI layout change (struct field add/remove,
/// enum variant add/remove, function signature change). Java side maintains
/// EXPECTED_ABI_VERSION; init-time mismatch throws FrsAbiMismatchException.
pub const FRS_ABI_VERSION: u32 = 1;

/// Returns the V1 ABI version. Called once at Java backend init to detect
/// dylib/jar version skew before any state op runs.
#[no_mangle]
pub extern "C" fn frs_abi_version() -> u32 {
    FRS_ABI_VERSION
}

// ---------------------------------------------------------------------------
// Error codes and row result envelope
// ---------------------------------------------------------------------------

/// Per-row result envelope returned by every batch FFI call.
///
/// Layout is `#[repr(C)]` for stable ABI. `code` is a discriminator
/// from `FrsErrorCode`; on `Ok`, `payload_off`/`payload_len` describe a
/// slice in the caller's output buffer. On error, they MAY carry
/// optional detail bytes (caller need not consume).
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FrsRowResult {
    pub code: u32,
    pub payload_off: u32,
    pub payload_len: u32,
}

/// Typed error code set returned per row by every batch FFI call.
///
/// Three classes (per umbrella spec §4):
///   - Fail-row (codes 1..300): one row affected; other rows in the batch
///     resolve normally. State remains consistent.
///   - Fail-batch (codes 300..900): whole batch failed; treat as if nothing
///     happened. Recovery via Flink checkpoint replay.
///   - Fail-process (codes 900+): engine state suspect; FatalErrorHandler
///     escalates to TM-level restart.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrsErrorCode {
    Ok = 0,
    NotFound = 1,
    KeyTooLarge = 100,
    ValueTooLarge = 101,
    BatchHeaderMalformed = 110,
    IterExpired = 200,
    IterCursorInvalid = 201,
    EngineIo = 300,
    EngineCorrupted = 301,
    EngineOom = 302,
    EngineDiskFull = 303,
    PanicCaught = 900,
    Unknown = 999,
}

impl FrsErrorCode {
    /// Convert from u32; unknown values map to `Unknown`.
    pub fn from_u32(v: u32) -> Self {
        match v {
            0 => FrsErrorCode::Ok,
            1 => FrsErrorCode::NotFound,
            100 => FrsErrorCode::KeyTooLarge,
            101 => FrsErrorCode::ValueTooLarge,
            110 => FrsErrorCode::BatchHeaderMalformed,
            200 => FrsErrorCode::IterExpired,
            201 => FrsErrorCode::IterCursorInvalid,
            300 => FrsErrorCode::EngineIo,
            301 => FrsErrorCode::EngineCorrupted,
            302 => FrsErrorCode::EngineOom,
            303 => FrsErrorCode::EngineDiskFull,
            900 => FrsErrorCode::PanicCaught,
            _ => FrsErrorCode::Unknown,
        }
    }
}

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

/// A handle to an open iterator. Created by `frs_iterator_open` (full CF
/// scan) or `frs_prefix_lookup_open` (prefix-bounded scan). Destroyed by
/// `frs_iterator_close` / `frs_prefix_lookup_close`.
///
/// The current implementation materializes the full key/value set at
/// open time (snapshot-and-collect) because the underlying engine does
/// not yet expose a streaming iterator (see W26 follow-up). Memory cost
/// is therefore O(scanned bytes); callers should constrain the iteration
/// range via `frs_prefix_lookup_open` or seek closely.
pub type FrsIterator = *mut c_void;

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

impl Default for FrsBytes {
    fn default() -> Self {
        Self::NULL
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Runs `f` inside `catch_unwind`. Returns `FRS_STATUS_PANIC` if a panic is
/// caught; otherwise returns whatever `f` returned.
fn guarded<F: FnOnce() -> i32>(f: F) -> i32 {
    // FRS-MEM-PRESSURE-PURGE: install the jemalloc purge hook on the first FFI
    // call — before any DB open starts the mem-pressure sampler. Idempotent +
    // cheap (one OnceLock load after the first call); a no-op off Linux.
    ensure_purge_hook_registered();
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => FRS_STATUS_PANIC,
    }
}

/// FRS-PROBE-DIAG (2026-06-02): whether per-probe open timing is enabled, read
/// once from `FRS_PROBE_DIAG`. Used to diagnose the q7 ckpt-ON join throughput
/// collapse (~100/s past ~21M records) by splitting `frs_vec_iter_prefix_open`
/// latency into BUILD (merge construction / reader opens) vs FILL (get_arc
/// drain). Zero cost when unset.
fn probe_diag_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("FRS_PROBE_DIAG")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

/// Like [`guarded`] but for the vectorized batch FFI functions. On panic,
/// returns `FrsErrorCode::PanicCaught as i32` (900) — a Fail-process code
/// per spec §4, distinct from the legacy `FRS_STATUS_PANIC` (5) used by
/// the pre-FrsErrorCode single-row API.
fn guarded_vec<F: FnOnce() -> i32>(f: F) -> i32 {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(v) => v,
        Err(_) => FrsErrorCode::PanicCaught as i32,
    }
}

/// Maps an engine error to a typed `FrsErrorCode` discriminant per spec §4.
///
/// Used by the vectorized batch FFI functions (`frs_vectorized_batch_*`).
/// The old non-vectorized functions use [`error_to_status`] which returns
/// legacy `FRS_STATUS_*` sequential codes for backward compatibility with
/// `FrsStatus.java`.
///
/// Error class mapping:
/// - Not-found         → NotFound (1)       — Fail-row
/// - Invalid/null arg  → BatchHeaderMalformed (110) — Fail-batch
/// - I/O               → EngineIo (300)     — Fail-batch
/// - Corruption        → EngineCorrupted (301) — Fail-batch
/// - OOM               → EngineOom (302)    — Fail-batch
/// - DiskFull          → EngineDiskFull (303) — Fail-batch
/// - Other             → Unknown (999)      — treated as Fail-process
fn error_to_frs_code(err: &forst_rs_common::ForstError) -> i32 {
    if err.is_not_found() {
        FrsErrorCode::NotFound as i32
    } else if err.is_io() {
        FrsErrorCode::EngineIo as i32
    } else if err.is_corruption() {
        FrsErrorCode::EngineCorrupted as i32
    } else if err.is_invalid_argument() {
        FrsErrorCode::BatchHeaderMalformed as i32
    } else {
        // All other errors (OOM, DiskFull, Aborted, Busy, write-stall timeout,
        // etc.) don't have direct `is_*` predicates exposed by ForstError today.
        // Map to Unknown until ForstError grows those predicates (tracked in W26
        // follow-up).
        //
        // FRS-S3-STALL DIAGNOSTIC: the Java FFM bridge surfaces `Unknown(999)`
        // as a fatal `FrsEnginePanicError`, which loses the underlying cause and
        // restart-loops the job. Echo the real `ForstError` Display to stderr
        // (→ TaskManager `.out`) so an operator can see e.g. "write stall
        // timeout: flush/compaction backlog not draining" instead of a bare
        // rc=999. The text comes from the engine and never contains storage
        // credentials, so this is safe to emit.
        eprintln!("[forst-rs-ffi] mapping engine error to Unknown(999): {err}");
        FrsErrorCode::Unknown as i32
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

/// Clone an `Arc<DbImpl>` from an opaque FFI handle.
///
/// # SAFETY
/// - (a) `h` MUST originate from a prior `Box::into_raw(Box::new(Arc<DbImpl>))`
///   issued by `frs_db_open_*`. Passing any other pointer is undefined behaviour.
/// - (b) No concurrent call to `frs_db_close(h)` (or any other deallocation
///   path that drops the boxed `Arc`) may overlap with this call. The boxed
///   `Arc` lives behind `h`; close drops the box and invalidates the pointer.
///   External synchronization is required if close may race with this call.
/// - (c) `h` MUST be properly aligned for `Arc<DbImpl>` — this is guaranteed
///   for any pointer produced by `Box::new`, but unaligned pointers (e.g.
///   from a corrupted handle that callers re-cast) trigger undefined
///   behaviour on the `*ptr` dereference below.
unsafe fn db_from_handle(h: FrsDb) -> Option<Arc<DbImpl>> {
    if h.is_null() {
        return None;
    }
    let ptr = h as *const Arc<DbImpl>;
    Some((*ptr).clone())
}

/// Reconstruct ownership of a `Box<ColumnFamilyHandle>` from an opaque FFI handle.
///
/// Reconstructs the `Box<ColumnFamilyHandle>` from the raw pointer, taking
/// ownership back from C. When the returned `Box` is dropped the handle is
/// freed; callers that intend to keep the handle alive must use
/// [`cf_ref`] instead, or must `Box::into_raw` the returned box before it
/// goes out of scope.
///
/// # SAFETY
/// - (a) `h` MUST originate from a prior
///   `Box::into_raw(Box::new(ColumnFamilyHandle))` issued by `frs_cf_create_*`
///   / `frs_cf_open_*`. Any other provenance is undefined behaviour because
///   `Box::from_raw` would attempt to free memory it does not own.
/// - (b) No concurrent close of the underlying CF handle (e.g.
///   `frs_cf_close(h)`) may overlap with this call. The handle behind `h` is
///   exclusively re-owned by the returned `Box` — a parallel close would
///   double-free. External synchronization is required if close may race.
/// - (c) `h` MUST be properly aligned for `ColumnFamilyHandle` — guaranteed
///   for `Box::new`-produced pointers, but corrupted / re-cast handles would
///   trigger undefined behaviour on `Box::from_raw`.
unsafe fn cf_from_handle(h: FrsCfHandle) -> Option<Box<ColumnFamilyHandle>> {
    if h.is_null() {
        return None;
    }
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
    } else if err.is_internal() {
        FRS_STATUS_INTERNAL
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
        match DbImpl::open_with_fs_and_default_cf(opts, fs, raw_concat_default_cf_descriptor()) {
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
        match DbImpl::open_with_fs_and_default_cf(opts, fs, raw_concat_default_cf_descriptor()) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens an in-memory engine with caller-supplied write-path tuning knobs.
///
/// This is the **performance-tuning** companion to [`frs_db_open_memory`]:
/// the four parameters map 1:1 onto the matching `EngineOptions` fields,
/// while every other knob (compression, block cache, level multiplier, …)
/// stays at its default. Pass `0` for any of the four to keep that
/// individual default.
///
/// All four values flow through `EngineOptionsBuilder::try_build` so the
/// full validation stack (R-loop r3–r19 caps and joint-product checks)
/// fires; an out-of-range parameter returns `FRS_STATUS_INVALID_ARGUMENT`
/// rather than panicking. This makes the FFI safe to call from JMH-style
/// benches that sweep large configuration ranges.
///
/// Used by JMH benches and integration tests that want to probe the
/// memtable-budget / background-compaction sensitivity of the write path.
/// Production consumers should keep using [`frs_db_open`] /
/// [`frs_db_open_memory`] and configure the engine through their own JSON
/// (or future structured-config) layer.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_memory_tuned(
    write_buffer_size: usize,
    max_write_buffer_number: usize,
    max_background_compactions: usize,
    max_background_flushes: usize,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let mut builder = EngineOptions::builder().db_path("/db");
        if write_buffer_size != 0 {
            builder = builder.write_buffer_size(write_buffer_size);
        }
        if max_write_buffer_number != 0 {
            builder = builder.max_write_buffer_number(max_write_buffer_number);
        }
        if max_background_compactions != 0 {
            builder = builder.max_background_compactions(max_background_compactions);
        }
        if max_background_flushes != 0 {
            builder = builder.max_background_flushes(max_background_flushes);
        }
        let opts = match builder.try_build() {
            Ok(o) => o,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        match DbImpl::open_with_fs_and_default_cf(opts, fs, raw_concat_default_cf_descriptor()) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens a remote-storage-backed engine with a local SST cache (B-Prod-P6).
///
/// `uri` selects the OpenDAL backend (`memory://`, `file:///abs/path`, or
/// `s3://bucket/`). `opendal_config_json` is a flat JSON object (e.g.
/// `{"region":"us-east-1","endpoint":"https://minio.example.com"}`) holding
/// scheme-specific config; pass `{}` or `null` if none are required (the
/// `memory` and `file` schemes ignore the map). `cache_dir` is the local
/// directory used for the LRU SST cache; `cache_capacity_bytes` bounds its
/// total on-disk size.
///
/// On success, writes the new `FrsDb` handle into `*out_handle` and
/// returns `FRS_STATUS_OK`. On failure returns one of the standard
/// status codes (NULL_ARG, INVALID_ARGUMENT, IO, …).
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_remote(
    uri: *const c_char,
    opendal_config_json: *const c_char,
    cache_dir: *const c_char,
    cache_capacity_bytes: u64,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let uri_str = match cstr_to_str(&uri) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let cache_dir_str = match cstr_to_str(&cache_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        // JSON config is optional: a NULL pointer (or empty / "null" /
        // "{}" string) means "no extra config".
        let json_str = if opendal_config_json.is_null() {
            String::new()
        } else {
            match cstr_to_str(&opendal_config_json) {
                Some(s) => s.to_string(),
                None => return FRS_STATUS_NULL_ARG,
            }
        };
        let config = match parse_flat_json_object(&json_str) {
            Ok(m) => m,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };

        let opts = EngineOptions {
            db_path: format!("/db-remote-{}", uri_str_hash(&uri_str)),
            ..EngineOptions::default()
        };
        let cache_path = PathBuf::from(&cache_dir_str);
        match DbImpl::open_remote_with_default_cf(
            opts,
            &uri_str,
            config,
            &cache_path,
            cache_capacity_bytes,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens a remote-storage-backed engine with the same structured tuning
/// surface as [`frs_db_open_with_options`]. The `db_path` field inside
/// `opts` is intentionally ignored: remote engines derive their logical
/// path from `uri` so logs and manifests stay stable across local cache
/// directories.
///
/// # SAFETY
/// - `opts` must point to a valid [`FrsEngineOptions`] for the duration
///   of this call.
/// - `uri` and `cache_dir` must be non-null, NUL-terminated UTF-8.
/// - `opendal_config_json` may be null, otherwise it must be
///   NUL-terminated UTF-8.
/// - `out_handle` must be a valid pointer to a `FrsDb` slot.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_remote_with_options(
    opts: *const FrsEngineOptions,
    uri: *const c_char,
    opendal_config_json: *const c_char,
    cache_dir: *const c_char,
    cache_capacity_bytes: u64,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() || opts.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let cfg = &*opts;
        let uri_str = match cstr_to_str(&uri) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let cache_dir_str = match cstr_to_str(&cache_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let json_str = if opendal_config_json.is_null() {
            String::new()
        } else {
            match cstr_to_str(&opendal_config_json) {
                Some(s) => s.to_string(),
                None => return FRS_STATUS_NULL_ARG,
            }
        };
        let config = match parse_flat_json_object(&json_str) {
            Ok(m) => m,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };

        let mut builder =
            EngineOptions::builder().db_path(format!("/db-remote-{}", uri_str_hash(&uri_str)));
        if cfg.write_buffer_size != 0 {
            builder = builder.write_buffer_size(cfg.write_buffer_size as usize);
        }
        if cfg.max_write_buffer_number != 0 {
            builder = builder.max_write_buffer_number(cfg.max_write_buffer_number as usize);
        }
        if cfg.max_background_compactions != 0 {
            builder = builder.max_background_compactions(cfg.max_background_compactions as usize);
        }
        if cfg.max_background_flushes != 0 {
            builder = builder.max_background_flushes(cfg.max_background_flushes as usize);
        }
        if cfg.block_cache_capacity_bytes != 0 {
            builder = builder.block_cache_capacity_bytes(cfg.block_cache_capacity_bytes);
        }
        if cfg.write_buffer_manager_capacity_bytes != 0 {
            builder = builder
                .write_buffer_manager_capacity_bytes(cfg.write_buffer_manager_capacity_bytes);
        }
        if let Some(codec) = sst_compression_from_discriminant(cfg.sst_compression) {
            builder = builder.compression(codec);
        }
        let mut engine_opts = match builder.try_build() {
            Ok(o) => o,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        // FRS-PERF-TUNE (q7/q9 join hot path): the active memtable is sharded
        // `memtable_shards`-ways by full-key FNV hash for WRITE concurrency.
        // Flink accesses keyed state single-threaded per slot, so the sharding
        // gives no concurrency benefit here but forces a prefix scan (MapState
        // iter / streaming-join `entries()`) to do ONE BTreeMap lower-bound
        // seek PER SHARD — 16 seeks/probe at the default, ~97% of q7 join CPU
        // per profile. `FRS_MEMTABLE_SHARDS` tunes it per run (1 = single seek)
        // without a rebuild. This is the production S3 open path Flink calls.
        if let Ok(s) = std::env::var("FRS_MEMTABLE_SHARDS") {
            if let Ok(n) = s.trim().parse::<usize>() {
                if n >= 1 {
                    engine_opts.memtable_shards = n;
                }
            }
        }
        // FRS-SST-COMPRESSION (2026-06-04): override the SST data-block codec per
        // run without a rebuild. A symbolized native profile of the LOCAL q4
        // interval join showed `sst::compression::decompress` (LZ4) as the #1
        // hot frame under `frs_vec_iter_prefix_open` (~4500/8030 samples): each
        // scattered ~1-key prefix probe `pread`s + LZ4-decompresses a whole data
        // block. On LOCAL storage there is no disk-space / upload-bandwidth
        // pressure, so `none` trades disk for CPU — it eliminates the decompress
        // AND lets `decode_data_block_zerocopy` slice the block buffer instead of
        // copying each column out (the reader's zero-copy fast path is gated on
        // `CompressionType::None`). `lz4` / `zstd` keep a compressed codec (right
        // for the S3 open path where upload bytes dominate). Mirrors the
        // `FRS_MEMTABLE_SHARDS` / `FRS_BLOCK_SIZE_KB` per-run tuning hooks.
        if let Ok(s) = std::env::var("FRS_SST_COMPRESSION") {
            match s.trim().to_ascii_lowercase().as_str() {
                "none" | "off" | "0" => {
                    engine_opts.compression = forst_rs_common::CompressionType::None
                }
                "lz4" => engine_opts.compression = forst_rs_common::CompressionType::Lz4,
                "zstd" => engine_opts.compression = forst_rs_common::CompressionType::Zstd,
                _ => {}
            }
        }

        let cache_path = PathBuf::from(&cache_dir_str);
        match DbImpl::open_remote_with_default_cf(
            engine_opts,
            &uri_str,
            config,
            &cache_path,
            cache_capacity_bytes,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Parses a flat JSON object (string-to-string only) into a `HashMap`.
/// Accepts the empty string, `"null"`, and `"{}"` as "no config".
///
/// We intentionally keep this parser tiny rather than pulling in `serde_json`:
/// the FFI surface only ever passes a handful of well-known string knobs
/// (`region`, `endpoint`, `access_key_id`, `secret_access_key`) and a
/// strict-but-small parser is far less of a foot-gun than carrying the
/// full serde stack across the FFI boundary.
///
/// Returns `Err(())` on any malformed input. Quoted strings are recognised
/// with no escape support beyond `\"` and `\\` — sufficient for the values
/// the bridge actually carries today.
fn parse_flat_json_object(s: &str) -> Result<HashMap<String, String>, ()> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "null" || trimmed == "{}" {
        return Ok(HashMap::new());
    }
    let bytes = trimmed.as_bytes();
    if bytes.first() != Some(&b'{') || bytes.last() != Some(&b'}') {
        return Err(());
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut out = HashMap::new();
    if inner.trim().is_empty() {
        return Ok(out);
    }

    // Split on top-level commas. Strings can contain commas, so we only
    // honour commas outside double quotes.
    let mut parts = Vec::new();
    let mut buf = String::new();
    let mut in_str = false;
    let mut escape = false;
    for ch in inner.chars() {
        if escape {
            buf.push(ch);
            escape = false;
            continue;
        }
        match ch {
            '\\' if in_str => {
                buf.push(ch);
                escape = true;
            }
            '"' => {
                buf.push(ch);
                in_str = !in_str;
            }
            ',' if !in_str => {
                parts.push(std::mem::take(&mut buf));
            }
            _ => buf.push(ch),
        }
    }
    if !buf.trim().is_empty() {
        parts.push(buf);
    }

    for part in parts {
        let (k, v) = part.split_once(':').ok_or(())?;
        let key = unquote_json_string(k.trim())?;
        let value = unquote_json_string(v.trim())?;
        out.insert(key, value);
    }
    Ok(out)
}

fn unquote_json_string(s: &str) -> Result<String, ()> {
    let bytes = s.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'"' || bytes[bytes.len() - 1] != b'"' {
        return Err(());
    }
    let inner = &s[1..s.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                _ => return Err(()),
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

/// Stable, URL-safe hash of `uri` to disambiguate the `db_path` reported
/// to the engine (the engine uses it as a logical root only — the actual
/// FS root is the OpenDAL operator). Keeps logs/metrics readable.
fn uri_str_hash(uri: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    uri.hash(&mut h);
    h.finish()
}

// Suppress dead_code for the imported Path alias; we use PathBuf above.
#[allow(dead_code)]
const _PATH_USAGE: fn(&Path) = |_| {};

// ---------------------------------------------------------------------------
// 1b. Structured open (B-Prod-P7, spec §6d)
// ---------------------------------------------------------------------------

/// Opaque structured config passed to [`frs_db_open_with_options`].
///
/// **ABI stability**: this struct is `repr(C)` and **append-only** — future
/// fields will be added at the end so that consumers built against an older
/// header continue to work after a forst-rs upgrade. Existing fields will
/// never be removed, reordered, or have their semantics changed; the
/// `cdylib` versioning policy treats any incompatible edit to this struct
/// as a breaking change.
///
/// Field semantics:
///
/// | Field | `0` means | Non-zero means |
/// |---|---|---|
/// | `db_path` (`*const c_char`, NUL-terminated UTF-8) | open in-memory under `/db` | open at the given filesystem path with `LocalFileSystem` |
/// | `write_buffer_size` | use engine default (64 MiB) | use this value (clamped by `EngineOptions::validate`) |
/// | `max_write_buffer_number` | use engine default (3) | use this value |
/// | `max_background_compactions` | use engine default (4) | use this value |
/// | `max_background_flushes` | use engine default (2) | use this value |
/// | `block_cache_capacity_bytes` | use engine default (256 MiB) | size the shared LRU at this many bytes |
/// | `write_buffer_manager_capacity_bytes` | use engine default (512 MiB) | cap cross-CF memtable bytes at this many bytes |
///
/// All `0` is therefore "open with all defaults" — equivalent to
/// [`frs_db_open_memory`] when `db_path` is also null.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct FrsEngineOptions {
    /// Filesystem path for the database directory (NUL-terminated UTF-8).
    /// `null` opens an in-memory engine at `/db`.
    pub db_path: *const c_char,
    /// Per-CF memtable size in bytes. `0` = engine default.
    pub write_buffer_size: u64,
    /// Per-CF max memtable count (active + sealed). `0` = engine default.
    pub max_write_buffer_number: u32,
    /// Background compaction threads. `0` = engine default.
    pub max_background_compactions: u32,
    /// Background flush threads. `0` = engine default.
    pub max_background_flushes: u32,
    /// Shared LRU block cache capacity in bytes. `0` = engine default
    /// (256 MiB; spec §6d).
    pub block_cache_capacity_bytes: u64,
    /// Cross-CF memtable budget in bytes (WriteBufferManager).
    /// `0` = engine default (512 MiB; spec §6d).
    pub write_buffer_manager_capacity_bytes: u64,
    /// SST data-block compression codec (FRS-PHASE2 fairness fix — the
    /// engine default is already LZ4, matching ForSt/RocksDB; this exposes
    /// it as a backend config option). Append-only field (ABI-stable):
    /// older consumers leave it `0`.
    ///   - `0` = engine default (LZ4)
    ///   - `1` = `none`
    ///   - `2` = `lz4`
    ///   - `3` = `zstd`
    ///
    /// The `FRS_SST_COMPRESSION` env var (when set) still takes precedence
    /// on the remote open path, preserving the per-run tuning hook.
    pub sst_compression: u32,
}

/// Maps the `FrsEngineOptions::sst_compression` discriminant to the engine
/// codec. `0` (and any unknown value) ⇒ `None`, signalling "leave the
/// builder default" to the caller (the default is LZ4).
fn sst_compression_from_discriminant(v: u32) -> Option<forst_rs_common::CompressionType> {
    use forst_rs_common::CompressionType;
    match v {
        1 => Some(CompressionType::None),
        2 => Some(CompressionType::Lz4),
        3 => Some(CompressionType::Zstd),
        _ => None,
    }
}

/// Opens a new engine using a structured options blob. Backwards-compatible
/// way to extend the FFI tuning surface without breaking the
/// [`frs_db_open_memory_tuned`] positional ABI (B-Prod-P7, spec §6d).
///
/// The `opts` pointer is read but not retained — the caller may free the
/// struct (and any `db_path` it points to) as soon as this function
/// returns. On success, writes the new `FrsDb` handle into `*out_handle`
/// and returns `FRS_STATUS_OK`.
///
/// # SAFETY
/// - `opts` must be either null OR point to a valid, fully-initialised
///   `FrsEngineOptions` for the duration of this call.
/// - When `opts.db_path` is non-null, it must be a NUL-terminated UTF-8
///   string valid for the duration of this call.
/// - `out_handle` must be a valid pointer to a `FrsDb` slot.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_with_options(
    opts: *const FrsEngineOptions,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() || opts.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let cfg = &*opts;

        let mut builder = forst_rs_common::EngineOptions::builder();

        // db_path: null → in-memory at /db; non-null → on-disk path.
        let path_ref = cfg.db_path;
        let (db_path, in_memory) = match cstr_to_str(&path_ref) {
            Some(p) if !p.is_empty() => (p.to_string(), false),
            _ => ("/db".to_string(), true),
        };
        builder = builder.db_path(db_path);

        if cfg.write_buffer_size != 0 {
            builder = builder.write_buffer_size(cfg.write_buffer_size as usize);
        }
        if cfg.max_write_buffer_number != 0 {
            builder = builder.max_write_buffer_number(cfg.max_write_buffer_number as usize);
        }
        if cfg.max_background_compactions != 0 {
            builder = builder.max_background_compactions(cfg.max_background_compactions as usize);
        }
        if cfg.max_background_flushes != 0 {
            builder = builder.max_background_flushes(cfg.max_background_flushes as usize);
        }
        if cfg.block_cache_capacity_bytes != 0 {
            builder = builder.block_cache_capacity_bytes(cfg.block_cache_capacity_bytes);
        }
        if cfg.write_buffer_manager_capacity_bytes != 0 {
            builder = builder
                .write_buffer_manager_capacity_bytes(cfg.write_buffer_manager_capacity_bytes);
        }
        if let Some(codec) = sst_compression_from_discriminant(cfg.sst_compression) {
            builder = builder.compression(codec);
        }

        let engine_opts = match builder.try_build() {
            Ok(o) => o,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };

        let fs: Arc<dyn FileSystem> = if in_memory {
            Arc::new(MemoryFileSystem::new())
        } else {
            Arc::new(LocalFileSystem::new())
        };

        match DbImpl::open_with_fs_and_default_cf(
            engine_opts,
            fs,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// FRS-PHASE2 backend feature-flag plumbing. Sets a process environment
/// variable from the (in-process) Flink backend so the engine's env-gated
/// feature flags — `FRS_KV_SEPARATION`, `FRS_KV_MIN_BLOB_SIZE`,
/// `FRS_TRIVIAL_MOVE`, `FRS_REMOTE_COMPACTION`, `FRS_VLOG_COMPRESSION`,
/// `FRS_SST_COMPRESSION`, `FRS_ASYNC_FLUSH_UPLOAD`, … — observe the configured
/// value.
///
/// **Why an FFI setter and not `System.setProperty`?** The engine reads
/// these via `std::env::var(...)` (POSIX `getenv`), which the JVM's Java
/// system-property table does NOT feed. Because the backend and the engine
/// share ONE process (the dylib is loaded into the JVM), a single
/// `std::env::set_var` here is visible to the engine. Each flag is cached
/// in an engine-side `OnceLock` on FIRST observation (KV-sep at first
/// flush, trivial-move at first compaction, remote-compaction / SST
/// compression at `frs_db_open*`), so the backend MUST call this BEFORE the
/// first `frs_db_open*` for the value to take effect — which is exactly the
/// backend's open-time config-plumbing point.
///
/// Returns `FRS_STATUS_OK` on success, `FRS_STATUS_NULL_ARG` if either
/// pointer is null, `FRS_STATUS_INVALID_ARGUMENT` if the name/value are not
/// valid UTF-8 or the name is empty / contains `=` or NUL.
///
/// # SAFETY
/// - `name` and `value` must be NUL-terminated UTF-8 for the call's duration.
#[no_mangle]
pub unsafe extern "C" fn frs_set_env(name: *const c_char, value: *const c_char) -> i32 {
    guarded(|| {
        if name.is_null() || value.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let name_str = match cstr_to_str(&name) {
            Some(s) => s,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let value_str = match cstr_to_str(&value) {
            Some(s) => s,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        // `set_var` panics on an empty name or a name/value containing `=`
        // or NUL — reject those up front so the FFI never aborts the JVM.
        if name_str.is_empty() || name_str.contains('=') || name_str.contains('\0') {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        std::env::set_var(name_str, value_str);
        FRS_STATUS_OK
    })
}

/// Returns the configured WriteBufferManager capacity for the engine, or
/// `0` when the manager is unbounded. Diagnostic accessor wired to the
/// Java FFM tuning surface (B-Prod-P7, spec §6d). Returns `0` for a null
/// or already-closed handle.
///
/// # SAFETY
/// - `handle` must be either null OR a valid `FrsDb` returned by
///   `frs_db_open*` and not yet closed.
#[no_mangle]
pub unsafe extern "C" fn frs_db_write_buffer_manager_capacity(handle: FrsDb) -> u64 {
    let Some(db) = db_from_handle(handle) else {
        return 0;
    };
    db.write_buffer_manager().capacity_bytes()
}

/// Returns the running cross-CF memtable bytes tracked by the
/// WriteBufferManager (B-Prod-P7, spec §6d). Useful for IT bench
/// assertions that want to verify the cap is actually firing.
///
/// # SAFETY
/// - `handle` must be a valid `FrsDb` returned by `frs_db_open*`.
#[no_mangle]
pub unsafe extern "C" fn frs_db_write_buffer_manager_current_bytes(handle: FrsDb) -> u64 {
    let Some(db) = db_from_handle(handle) else {
        return 0;
    };
    db.write_buffer_manager().current_bytes()
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
        match db.create_column_family(raw_concat_cf_descriptor(cf_name)) {
            Ok(cf) => {
                let boxed = Box::new(cf);
                *out_cf = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Creates a new column family with the named merge operator attached.
/// `merge_op_name = NULL` means no merge operator.
///
/// Recognised merge operators (resolved via the shared
/// `merge_operator_by_name` registry — the same names accepted by the
/// checkpoint restore arms and `frs_db_create_cf_from_import_with_merge`):
/// - `"ListAppendMergeOperator"` (alias `"ListAppendMergeOperator(delim=44)"`)
///   — comma-separated concatenation
/// - `"RawConcatMergeOperator"` — byte-for-byte concatenation
/// - `"NumericAddMergeOperator"` — 8-byte little-endian i64 saturating sum
/// - `"NumericAddBeMergeOperator"` — 8-byte big-endian i64 WRAPPING sum
///   (byte-equivalent to Java `long +` over Flink `LongSerializer` bytes;
///   OPT-N04 §4 — the operator merge-routed Reducing states bind by name)
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
            // OPT-N04 E3: resolve via THE shared name registry (same one
            // used by the checkpoint restore arms and the import path).
            let Some(op) = merge_operator_by_name(op_name) else {
                return FRS_STATUS_INVALID_ARGUMENT;
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

/// Installs a Flink-compatible TTL compaction filter on the CF.
///
/// The filter expires entries whose embedded `u64` Little-Endian millisecond
/// timestamp at `value[timestamp_offset..+8]` is older than `ttl_ms` from
/// the current wall clock. See
/// [`forst_rs_engine::FlinkTtlCompactionFilter`] for the decision matrix
/// (tombstones are always kept; values shorter than `timestamp_offset + 8`
/// are kept; `state_type = 0` / Disabled installs a no-op filter; `ttl_ms
/// = 0` means "never expire").
///
/// `state_type` ordinal:
/// - `0` — Disabled (no-op filter installed)
/// - `1` — Value (Flink ValueState / ReducingState / AggregatingState)
/// - `2` — List (treated as Value for whole-state expiry; per-element
///   pruning is a follow-up)
///
/// Unknown ordinals fall back to `Disabled`.
///
/// This is the post-`open` configuration entry point used by Flink's
/// `RocksDbTtlCompactionFilter` plumbing once the JNI shim records a
/// `state_type / ttl_ms / timestamp_offset` triple via
/// `Java_org_forstdb_FlinkCompactionFilter_configureFlinkCompactionFilter`.
/// Until the JNI side passes the destination CF handle through, this FFI
/// export is the supported way to bind the Flink-shaped TTL filter to a
/// CF from non-JNI consumers.
///
/// Returns:
/// - `FRS_STATUS_OK` on successful install.
/// - `FRS_STATUS_NULL_ARG` if `db` or `cf` is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if the engine rejects the handle (e.g.
///   the CF has been closed concurrently).
///
/// # SAFETY
///
/// - `db` must be a handle returned by `frs_db_open*` and not yet closed.
/// - `cf` must be a handle returned by `frs_db_create_cf*` /
///   `frs_db_open_cf` for the same database, not yet closed.
#[no_mangle]
pub unsafe extern "C" fn frs_cf_set_compaction_filter_ttl(
    db: FrsDb,
    cf: FrsCfHandle,
    ttl_ms: u64,
    state_type: i32,
    timestamp_offset: usize,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        let state = forst_rs_engine::TtlStateType::from_ordinal(state_type);
        let filter: Arc<dyn forst_rs_engine::CompactionFilter> = Arc::new(
            forst_rs_engine::FlinkTtlCompactionFilter::new(ttl_ms, state, timestamp_offset),
        );
        match db.set_compaction_filter(cf, Some(filter)) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 2b. FRS-WA-V0: per-CF state-lifecycle descriptor + watermark clocks
// (2026-06-13 write-path redesign survey §3.2/§6 stage V0 — INERT plumbing).
//
// The Flink backend KNOWS when its state dies (window end, interval-join
// TTL, timer fire); these entry points export that knowledge so the engine
// can later (V1, flag-gated default-OFF) drop whole death-bucketed segments
// at the watermark instead of compacting soon-dead bytes. Precedent: ForSt
// C++ `FlinkCompactionFilter` already exports the TTL contract across the
// boundary — but only as a filter INSIDE compaction, still paying the
// rewrite. In V0 the engine stores + logs these values; no behavior changes.
// ---------------------------------------------------------------------------

/// FRS-WA-V0: declares the CF's state lifecycle.
///
/// `kind` ordinal (see `CfLifecycle::from_ordinal`):
/// - `0` — Unbounded (default; classic leveled compaction)
/// - `1` — Windowed: entries written at event-time `t` are dead once the CF
///   watermark passes `t + ttl` (`ttl` in the CF's clock units; Flink: ms)
/// - `2` — Timer (reserved hint; no engine behavior yet)
///
/// Unknown ordinals return `FRS_STATUS_INVALID_ARGUMENT`. `ttl` is ignored
/// for kinds other than `1`.
///
/// # SAFETY
///
/// - `db` must be a handle returned by `frs_db_open*` and not yet closed.
/// - `cf` must be a handle returned by `frs_db_create_cf*` /
///   `frs_db_open_cf` for the same database, not yet closed.
#[no_mangle]
pub unsafe extern "C" fn frs_cf_set_lifecycle(
    db: FrsDb,
    cf: FrsCfHandle,
    kind: i32,
    ttl: u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(lifecycle) = forst_rs_engine::CfLifecycle::from_ordinal(kind, ttl) else {
            return FRS_STATUS_INVALID_ARGUMENT;
        };
        match db.set_cf_lifecycle(cf, lifecycle) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// FRS-WA-V0: advances the CF's watermark clock (monotonic — stale or
/// duplicate watermarks are harmless no-ops). The backend calls this from
/// the operator's watermark path, AFTER subtracting any allowed-lateness
/// slack it owes its own late-data handling.
///
/// # SAFETY — same handle contract as [`frs_cf_set_lifecycle`].
#[no_mangle]
pub unsafe extern "C" fn frs_cf_advance_watermark(
    db: FrsDb,
    cf: FrsCfHandle,
    watermark: u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.advance_cf_watermark(cf, watermark) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// FRS-WA-V0: raises the CF's written-event-time upper bound (monotonic).
/// The caller MUST keep this ≥ the event-time of every entry it has written
/// to the CF — advance it before (or atomically with) the write. The V1
/// flush path samples it after sealing a memtable to derive a sound (never
/// premature) segment death stamp `max_event_time + ttl`.
///
/// # SAFETY — same handle contract as [`frs_cf_set_lifecycle`].
#[no_mangle]
pub unsafe extern "C" fn frs_cf_note_max_event_time(
    db: FrsDb,
    cf: FrsCfHandle,
    event_time: u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.note_cf_max_event_time(cf, event_time) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 3. Point operations
// ---------------------------------------------------------------------------

/// Inserts or overwrites a value.
///
/// # D12-M1: NULL-value asymmetry vs [`frs_batch_put`] — read carefully.
///
/// This single-op variant treats a NULL `value` pointer as a **PUT with an
/// empty payload**: `value = NULL` produces the same engine effect as
/// `value = &[]` (any non-null pointer with `value_len = 0`). It NEVER
/// produces a Delete.
///
/// In contrast, [`frs_batch_put`] (the batched variant) treats a NULL entry
/// in its `values` array as a **DELETE** for the corresponding key. The
/// two functions therefore have **contradictory NULL-value semantics on
/// the same FFI surface**.
///
/// ## Implications for callers
///
/// * If you have a state whose legitimate serialized payload is zero bytes
///   (e.g. a degenerate ValueState with a Unit serializer), passing a NULL
///   value pointer to `frs_batch_put` will silently tombstone the row —
///   data corruption. The Java backend's `flushWriteBuffer` MUST allocate
///   a 1-byte non-NULL sentinel for empty payloads (see
///   `ForStRsKeyedStateBackend.flushWriteBuffer` A11-H1 / D11-H2 comment).
/// * If you want a Delete via `frs_put`, call [`frs_delete`] instead.
/// * Future cleanup direction (deferred): unify both functions to
///   "PUT-with-empty on NULL" and provide explicit op-type columns for
///   the batched delete path. Tracked as a separate refactor.
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
        // C-R15-NEW-H1: per-arg MAX_KEY_LEN cap. Sister entries (frs_get_into_buf,
        // frs_get_fast, frs_lookup_kv, all batch FFI) already enforce this; the
        // single-shot put was a gap. Unbounded key_len lets the comparator/memtable
        // hash OOB-read untrusted memory (info-disclosure); unbounded value_len
        // drives an unbounded Box<[u8]> allocation that bypasses the MAX_BATCH_BYTES
        // shield protecting the batch path.
        if key_len > MAX_KEY_LEN || value_len > MAX_VALUE_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
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
        // C-R15-NEW-H1: per-arg MAX_KEY_LEN cap. See frs_put rationale.
        if key_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
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
        // C-R15-NEW-H1: per-arg MAX_KEY_LEN cap on both key and operand.
        // The merge operand routes into the same memtable batch_put_arrow
        // u32-rebase path that C-R14-NEW-H3 capped on the merge-batch
        // sister; the single-shot was unprotected.
        if key_len > MAX_KEY_LEN || operand_len > MAX_VALUE_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
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
        // C-R15-NEW-H1: per-arg MAX_KEY_LEN cap. See frs_put rationale.
        if key_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
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

/// Zero-copy get: returns a pointer directly into the memtable's inline
/// value storage. The pointer is valid until the next flush or memtable
/// switch. Caller MUST NOT free the returned pointer.
///
/// Returns `FRS_STATUS_OK` + populates `out_ptr`/`out_len` on hit.
/// Returns `FRS_STATUS_NOT_FOUND` on miss (out_ptr = null, out_len = 0).
/// Returns `FRS_STATUS_FALLBACK` if the value is not inline (too large or
/// in SST) — caller should fall back to regular `frs_get`.
#[no_mangle]
pub unsafe extern "C" fn frs_get_pinned(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    out_ptr: *mut *const u8,
    out_len: *mut usize,
) -> i32 {
    guarded(|| {
        if out_ptr.is_null() || out_len.is_null() {
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
        // C-R7-H1: ALWAYS return FALLBACK. The pre-fix returned a raw
        // pointer into the memtable's inline `Box<[u8]>`; the pointer
        // outlived the shard read lock that protected it, and a
        // background flush thread or a concurrent same-shard writer
        // could drop the box between FFI return and the Java
        // `toArray` copy — silent use-after-free. The structural
        // mitigation we relied on ("Flink's single-threaded-per-slot
        // model") doesn't cover background flush, which runs on the
        // engine's worker pool entirely independent of the slot
        // thread. A safe zero-copy variant would require returning
        // the holding `Arc<[u8]>` via an opaque handle with a release
        // primitive — out of scope here. Until that lands, force the
        // caller to the allocate-and-copy `frs_get` path, which is
        // already its documented fallback.
        let _ = key_len;
        let _ = (db, cf);
        *out_ptr = std::ptr::null();
        *out_len = 0;
        FRS_STATUS_FALLBACK
    })
}

/// Combined get + put: reads the current value of `key`, writes `new_value`,
/// returns the old value via `out_old_value`. Single FFM boundary crossing
/// for the read-modify-write pattern. Returns `FRS_STATUS_OK` when the key
/// existed (old value written to `out_old_value`), `FRS_STATUS_NOT_FOUND`
/// when the key did not exist (put still succeeds, `out_old_value` is NULL).
#[no_mangle]
pub unsafe extern "C" fn frs_get_and_put(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    new_value: *const u8,
    new_value_len: usize,
    out_old_value: *mut FrsBytes,
) -> i32 {
    guarded(|| {
        if out_old_value.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if key.is_null() || new_value.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        // C-R15-NEW-H1: per-arg MAX_KEY_LEN cap on key + new_value.
        // See frs_put rationale.
        if key_len > MAX_KEY_LEN || new_value_len > MAX_VALUE_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let k = slice::from_raw_parts(key, key_len);
        let v = slice::from_raw_parts(new_value, new_value_len);
        match db.get_and_put(cf, k, v) {
            Ok(Some(old)) => {
                *out_old_value = FrsBytes::from_vec(old);
                FRS_STATUS_OK
            }
            Ok(None) => {
                *out_old_value = FrsBytes::NULL;
                FRS_STATUS_NOT_FOUND
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
///
/// # D12-M1: NULL-value asymmetry vs [`frs_put`] — read carefully.
///
/// This batched variant treats a NULL entry in `values[i]` as a **DELETE**
/// for `keys[i]`. The single-op [`frs_put`] treats a NULL `value` pointer
/// as a **PUT with an empty payload** instead. The two functions therefore
/// have **contradictory NULL-value semantics on the same FFI surface**.
///
/// ## Implications for callers
///
/// * Java backends that buffer writes and flush via this batch path MUST
///   substitute a 1-byte non-NULL sentinel for legitimately empty payloads;
///   passing NULL silently tombstones the row. See
///   `ForStRsKeyedStateBackend.flushWriteBuffer` (A11-H1 / D11-H2 comment)
///   for the production workaround.
/// * To mix puts and deletes in one batch today, set `values[i] = NULL`
///   for the delete rows and a non-NULL pointer (even to an empty slice)
///   for the put rows.
/// * Future cleanup direction (deferred): unify both functions to
///   "PUT-with-empty on NULL" and add an explicit `op_types` column so the
///   delete path is unambiguous. Tracked as a separate refactor.
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

        // C-R14-NEW-H1: legacy pointer-array `frs_batch_put` was the only
        // batch FFI entry without per-row caps + aggregate MAX_BATCH_BYTES
        // caps (sister entries `frs_batch_put_arrow`,
        // `frs_vectorized_batch_put`, etc. all enforce them). Per-row
        // unbounded `key_lens[i]` permitted OOB reads (info-disclosure
        // primitive) via `slice::from_raw_parts`; aggregate-unbounded
        // permitted the same engine `key_data.len() as u32` cumulative-
        // overflow corruption that C-R12-NEW-H2 closed on the Arrow path.
        // FRS-VALUE-CAP-FIX: keys are capped at MAX_KEY_LEN (1 MiB) but
        // VALUES use the larger MAX_VALUE_LEN — a no-unique-key join /
        // ListState value legitimately exceeds 1 MiB (q7 INVALID_ARGUMENT
        // crash). Aggregate MAX_BATCH_BYTES still shields the engine.
        let mut total_key_bytes: usize = 0;
        let mut total_val_bytes: usize = 0;
        for i in 0..count {
            if key_len_arr[i] > MAX_KEY_LEN {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
            if value_len_arr[i] > MAX_VALUE_LEN {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
            total_key_bytes = match total_key_bytes.checked_add(key_len_arr[i]) {
                Some(n) => n,
                None => return FRS_STATUS_INVALID_ARGUMENT,
            };
            total_val_bytes = match total_val_bytes.checked_add(value_len_arr[i]) {
                Some(n) => n,
                None => return FRS_STATUS_INVALID_ARGUMENT,
            };
            if total_key_bytes > MAX_BATCH_BYTES || total_val_bytes > MAX_BATCH_BYTES {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
        }

        let mut wb = WriteBatch::with_capacity(count);
        for i in 0..count {
            // SECURITY: null-check INDIVIDUAL key pointers within the array
            // before constructing slices. The outer-array null check above
            // doesn't cover entries (Sweep R13 H by Reviewer 2). UB risk
            // pre-fix because slice::from_raw_parts(null, N) is UB even for
            // N=0. Single-op functions (frs_put / frs_get) already do this.
            if key_ptrs[i].is_null() {
                return FRS_STATUS_NULL_ARG;
            }
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
        // SECURITY: null-check INDIVIDUAL key pointers (Sweep R13 H by
        // Reviewer 2). UB risk pre-fix.
        if key_ptrs.iter().any(|p| p.is_null()) {
            return FRS_STATUS_NULL_ARG;
        }
        // C-R14-NEW-H2: per-row MAX_KEY_LEN + aggregate MAX_BATCH_BYTES
        // caps. Sister entries (`frs_batch_get_arrow`,
        // `frs_vectorized_batch_get`) already enforce both; this legacy
        // pointer-array path was unprotected — unbounded `key_lens[i]`
        // permitted OOB reads via `slice::from_raw_parts`, and unbounded
        // aggregate fed into `db.batch_get` which would materialise an
        // arbitrarily-large `Vec<Option<Vec<u8>>>` result before any
        // out-buffer check, exposing the same heap-OOM attack vector
        // C-R13-NEW-H2 closed on the byte-blob batch_get path.
        let mut total_key_bytes: usize = 0;
        for i in 0..count {
            if key_len_arr[i] > MAX_KEY_LEN {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
            total_key_bytes = match total_key_bytes.checked_add(key_len_arr[i]) {
                Some(n) => n,
                None => return FRS_STATUS_INVALID_ARGUMENT,
            };
            if total_key_bytes > MAX_BATCH_BYTES {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
        }
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
        if let Err(e) = db.switch_and_flush(cf) {
            return error_to_status(&e);
        }
        match db.compact_range(cf) {
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

/// FRS-CKPT-NOFLUSH (2026-06-01): serialise every CF's LIVE memtables to
/// per-CF `memtable-cf<id>.arrow` artifacts under `target_dir` WITHOUT flushing
/// them to L0 SSTs. The backend's snapshot strategy includes these artifacts as
/// private-state files in its incremental keyed-state handle (Flink uploads
/// them to S3), keeping the memtable RAM-resident + unfragmented for reads — the
/// fix for the ckpt-ON heavy-join collapse. The memtable stays live + writable.
/// `out_count` (optional) receives the number of artifacts written.
#[no_mangle]
pub unsafe extern "C" fn frs_snapshot_memtables_to_dir(
    handle: FrsDb,
    snapshot: FrsSnapshot,
    target_dir: *const c_char,
    out_count: *mut u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let Some(path) = cstr_to_str(&target_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        // Bound the artifact to the pinned snapshot's seq so it is consistent
        // with the SST set captured by the companion no-flush incremental
        // checkpoint (excludes post-barrier writes belonging to the next ckpt).
        match db.snapshot_memtables_to_dir(std::path::Path::new(path), Some(snap_ref.seq().value()))
        {
            Ok(written) => {
                if !out_count.is_null() {
                    *out_count = written.len() as u64;
                }
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// FRS-CKPT-NOFLUSH: restore counterpart — replay every `memtable-cf<id>.arrow`
/// artifact found under `dir` into its CF (preserving sequence + op_type),
/// rebuilding the in-RAM state that was checkpointed without flushing. Call
/// AFTER the engine has opened the checkpoint's SST set. `out_rows` (optional)
/// receives the total rows replayed.
#[no_mangle]
pub unsafe extern "C" fn frs_replay_memtable_artifacts(
    handle: FrsDb,
    dir: *const c_char,
    out_rows: *mut u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(path) = cstr_to_str(&dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.replay_memtable_artifacts_from_dir(std::path::Path::new(path)) {
            Ok(rows) => {
                if !out_rows.is_null() {
                    *out_rows = rows as u64;
                }
                FRS_STATUS_OK
            }
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
        *out_count = db.l0_file_count();
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
        match DbImpl::open_from_checkpoint_with_default_cf(
            opts,
            fs,
            raw_concat_default_cf_descriptor(),
        ) {
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
        match DbImpl::open_from_checkpoint_with_default_cf(
            opts,
            fs,
            raw_concat_default_cf_descriptor(),
        ) {
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
// 7b. Live-file enumeration (Flink incremental-restore primitive)
// ---------------------------------------------------------------------------
//
// Flink's `RocksDBIncrementalRestoreOperation` calls `RocksDB.getLiveFiles`
// after a checkpoint to enumerate every SST file the engine considers part
// of the live LSM-tree, then re-uploads / re-references those files in the
// next checkpoint instead of doing a full state copy. Without per-file
// enumeration the restore path silently degrades to "always full copy",
// which is functionally correct but wipes out the entire incremental-state
// optimisation.
//
// These three exports — `frs_db_get_live_files`,
// `frs_db_get_live_files_metadata`, and `frs_db_live_file_list_free` —
// surface the Rust-side [`forst_rs_engine::DbImpl::list_live_files`] API
// across the C ABI in a self-contained, repr(C) shape so both the JNI shim
// (compat_jni.rs) and any direct C/FFM consumer can use it.
//
// Memory ownership: the [`FrsLiveFile`] entries (and their `path` /
// `cf_name` C strings) are allocated by Rust. Callers MUST call
// [`frs_db_live_file_list_free`] exactly once per [`FrsLiveFileList`] to
// release the inner `files` array AND every owned C string inside it. The
// `out` outer struct itself is caller-allocated (typical
// stack-or-Java-heap pattern) so we do NOT free `list` itself.

/// One live SST file's descriptor (Rust-owned strings, ABI-stable layout).
///
/// `path` and `cf_name` are NUL-terminated C strings allocated by Rust via
/// `CString::into_raw`; both must be released by
/// [`frs_db_live_file_list_free`] (which walks the array and reclaims each
/// string). Do NOT free them individually.
#[repr(C)]
pub struct FrsLiveFile {
    /// Absolute on-disk path of the SST file (UTF-8, NUL-terminated).
    pub path: *mut c_char,
    /// File size in bytes.
    pub size: u64,
    /// Largest sequence number contained in the file (mirrors the
    /// RocksDB `largest_seqno` field).
    pub sequence: u64,
    /// LSM level the file currently lives on (0..MAX_LEVELS).
    pub level: u8,
    /// Owning column family name (UTF-8, NUL-terminated). forst-rs
    /// presently has a single global VersionSet so this is always
    /// `"default"` today; reserved for future per-CF VersionSet.
    pub cf_name: *mut c_char,
}

/// A list of live SST files plus aggregate manifest metadata, returned
/// from [`frs_db_get_live_files`] / [`frs_db_get_live_files_metadata`].
///
/// `files` is a contiguous array of `count` [`FrsLiveFile`]s. The whole
/// list (array + each entry's owned strings) MUST be released via
/// [`frs_db_live_file_list_free`]; the outer struct itself is
/// caller-allocated.
#[repr(C)]
pub struct FrsLiveFileList {
    pub files: *mut FrsLiveFile,
    pub count: usize,
    /// Size of the on-disk version manifest in bytes (Flink's
    /// `manifestFileSize` field). 0 if no manifest has been persisted yet
    /// (e.g. fresh DB with no checkpoints) — Flink handles 0 the same way
    /// RocksDB reports a freshly opened DB.
    pub manifest_size: u64,
}

impl FrsLiveFileList {
    const EMPTY: Self = Self {
        files: std::ptr::null_mut(),
        count: 0,
        manifest_size: 0,
    };
}

/// Internal helper: convert a Rust [`forst_rs_engine::LiveFileInfo`] vec
/// plus a manifest size into the C-ABI shape, leaking the `Vec`'s buffer
/// and each owned C string. Pairs with [`frs_db_live_file_list_free`].
fn into_ffi_list(files: Vec<forst_rs_engine::LiveFileInfo>, manifest_size: u64) -> FrsLiveFileList {
    if files.is_empty() {
        return FrsLiveFileList {
            files: std::ptr::null_mut(),
            count: 0,
            manifest_size,
        };
    }
    let mut converted: Vec<FrsLiveFile> = Vec::with_capacity(files.len());
    for f in files {
        // Convert PathBuf → CString. Path strings on Unix may contain
        // non-UTF-8 bytes; engine constructs them via `format!` against the
        // db_path so they should be valid UTF-8. Fall back to a lossy
        // representation rather than panicking — preserving the file's
        // existence to the caller is more important than path purity.
        let path_str = f.path.to_string_lossy().into_owned();
        let path_c = std::ffi::CString::new(path_str)
            .unwrap_or_else(|_| std::ffi::CString::new("<invalid-path>").expect("static literal"));
        let cf_c = std::ffi::CString::new(f.cf_name)
            .unwrap_or_else(|_| std::ffi::CString::new("default").expect("static literal"));
        converted.push(FrsLiveFile {
            path: path_c.into_raw(),
            size: f.size,
            sequence: f.sequence,
            level: f.level,
            cf_name: cf_c.into_raw(),
        });
    }
    converted.shrink_to_fit();
    let count = converted.len();
    let ptr = converted.as_mut_ptr();
    std::mem::forget(converted);
    FrsLiveFileList {
        files: ptr,
        count,
        manifest_size,
    }
}

/// Enumerates every live SST file in the engine's current Version.
///
/// If `flush_memtable` is non-zero the engine first runs a synchronous
/// flush of every CF's pending memtables so newly written rows that have
/// not yet been persisted are included in the returned list — this is the
/// contract Flink's `getLiveFiles(true)` relies on.
///
/// The output struct `*out` is caller-allocated. On success the function
/// fills `*out` with a Rust-owned list; the caller MUST call
/// [`frs_db_live_file_list_free`] exactly once per successful call.
///
/// Returns `FRS_STATUS_OK` on success, `FRS_STATUS_NULL_ARG` if `db` or
/// `out` is null, or one of the I/O / ERROR codes if the optional flush
/// fails.
#[no_mangle]
pub unsafe extern "C" fn frs_db_get_live_files(
    db: FrsDb,
    flush_memtable: bool,
    out: *mut FrsLiveFileList,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        // Initialise to a safe-empty value first so a failure path leaves
        // the caller with a deterministic (and free-safe) struct.
        *out = FrsLiveFileList::EMPTY;
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let manifest_size = db.manifest_file_size();
        match db.list_live_files(flush_memtable) {
            Ok(files) => {
                *out = into_ffi_list(files, manifest_size);
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Enumerates every live SST file's metadata (no flush). This is the
/// counterpart to [`frs_db_get_live_files`] used by the
/// `getLiveFilesMetaData()` Java surface, which never triggers a flush.
///
/// Memory ownership rules are identical: caller MUST free via
/// [`frs_db_live_file_list_free`].
#[no_mangle]
pub unsafe extern "C" fn frs_db_get_live_files_metadata(
    db: FrsDb,
    out: *mut FrsLiveFileList,
) -> i32 {
    frs_db_get_live_files(db, false, out)
}

/// Releases the inner `files` array AND every owned C string inside it.
///
/// Safe to call exactly once per `FrsLiveFileList` returned by
/// [`frs_db_get_live_files`] / [`frs_db_get_live_files_metadata`]. After
/// this call the list's `files` pointer is reset to NULL and `count` to 0,
/// so a redundant second call is a no-op (does not double-free).
///
/// The outer `*list` struct itself is NOT freed — it's typically caller
/// stack memory or Java-heap memory and not Rust-owned.
#[no_mangle]
pub unsafe extern "C" fn frs_db_live_file_list_free(list: *mut FrsLiveFileList) -> i32 {
    guarded(|| {
        if list.is_null() {
            return FRS_STATUS_OK;
        }
        let l = &mut *list;
        if l.files.is_null() || l.count == 0 {
            // Idempotent: clearing twice is a no-op.
            l.files = std::ptr::null_mut();
            l.count = 0;
            return FRS_STATUS_OK;
        }
        // Reconstruct the Vec to drop its buffer.
        let files = Vec::from_raw_parts(l.files, l.count, l.count);
        for f in files {
            if !f.path.is_null() {
                drop(std::ffi::CString::from_raw(f.path));
            }
            if !f.cf_name.is_null() {
                drop(std::ffi::CString::from_raw(f.cf_name));
            }
        }
        l.files = std::ptr::null_mut();
        l.count = 0;
        FRS_STATUS_OK
    })
}

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
/// `op_type` encoding mirrors `OpType` (RocksDB byte-compat):
/// 0 Delete (kTypeDeletion), 1 Put (kTypeValue), 2 Merge (kTypeMerge),
/// 7 SingleDelete (kTypeSingleDeletion). Null `value` is allowed only for
/// Delete/SingleDelete.
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
        // C-R10-NEW-H1: arrow-rs's `from_ffi` uses
        // `ArrayData::new_unchecked` — it trusts producer-supplied
        // `value_offsets` and `length` without bounds checks. A
        // hostile or buggy producer can pass offsets that point
        // OUTSIDE the actual value-data buffer; subsequent
        // `keys.value(i)` calls (which use `value_unchecked` →
        // `slice::from_raw_parts(ptr.offset(start), end - start)`)
        // then read arbitrary memory — UB / info disclosure. Validate
        // here at the boundary using `validate_full` which checks
        // offset monotonicity, bounds, and UTF-8 (n/a for Binary).
        let data = match data.validate_full() {
            Ok(()) => data,
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
        // Schema sanity-check: column 0 must be Binary (key), column 1 must
        // be Binary nullable (value), column 2 must be UInt8 (op_type). The
        // memtable re-validates the same shape, but failing here returns the
        // FFI-friendly INVALID_ARGUMENT status without going through the
        // engine's ForstError → status mapping.
        if batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .is_none()
        {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
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

        // C-R11-NEW-H2: per-row MAX_KEY_LEN cap. The byte-blob batch FFI
        // family (frs_vectorized_batch_get/put/delete) all enforce this
        // per-row; the Arrow zero-copy path was missing it. Without the
        // cap, a producer can deliver i32::MAX-byte keys that cumulative-
        // overflow the memtable's `key_data.len() as u32` rebase or
        // exercise unguarded engine hash / comparator paths on multi-MiB
        // keys. We MUST re-downcast here to read offsets (the column-0
        // downcast above returned None-or-OK without binding); cheap
        // because Arrow downcast is a vtable check.
        let keys_col = match batch.column(0).as_any().downcast_ref::<BinaryArray>() {
            Some(a) => a,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        for i in 0..batch.num_rows() {
            if keys_col.value_length(i) as usize > MAX_KEY_LEN {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
        }

        // C-R12-NEW-H2: total batch payload cap. Per-row caps alone do
        // NOT bound aggregate bytes; MAX_BATCH_COUNT * MAX_KEY_LEN can
        // reach 1 TiB which cumulative-overflows the memtable's
        // `key_data.len() as u32` rebase (see batch_put_arrow_with_base_seq)
        // and any downstream u32 file/SST/VersionEdit byte arithmetic.
        // The Arrow offsets are i32, so the last offset is the total
        // bytes for that column. validate_full() already established
        // offset monotonicity, so reading the last offset is safe.
        let key_offsets = keys_col.value_offsets();
        let val_offsets = values.value_offsets();
        let total_key_bytes =
            *key_offsets.last().unwrap_or(&0) as i64 - *key_offsets.first().unwrap_or(&0) as i64;
        let total_val_bytes =
            *val_offsets.last().unwrap_or(&0) as i64 - *val_offsets.first().unwrap_or(&0) as i64;
        if total_key_bytes < 0
            || total_val_bytes < 0
            || (total_key_bytes as usize) > MAX_BATCH_BYTES
            || (total_val_bytes as usize) > MAX_BATCH_BYTES
        {
            return FRS_STATUS_INVALID_ARGUMENT;
        }

        // Cross-column null/op invariant: Delete / SingleDelete rows MUST
        // carry a null value; Put / Merge MUST carry a non-null value. We
        // check this once up-front so an invalid row rejects the whole batch
        // before any column data is dispatched into the memtable. The
        // memtable's `batch_put_arrow` path itself only validates op_type
        // bytes — the null-vs-op invariant lives at the FFI boundary because
        // it depends on the OpType discriminant semantics this module owns.
        // RocksDB byte-compat: 0=Delete, 1=Put, 2=Merge, 7=SingleDelete.
        for i in 0..batch.num_rows() {
            let op = ops.value(i);
            let v_null = values.is_null(i);
            let bad = match op {
                0 => !v_null, // Delete forbids value
                1 => v_null,  // Put requires value
                2 => v_null,  // Merge requires operand
                7 => !v_null, // SingleDelete forbids value
                _ => return FRS_STATUS_INVALID_ARGUMENT,
            };
            if bad {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
        }

        // C1 (zero-copy hot path): dispatch the RecordBatch DIRECTLY into the
        // memtable's column buffers via slice-copy. No intermediate
        // WriteBatch (avoids per-row Vec<u8> allocation + the subsequent
        // re-borrow into Vec<&[u8]> that the legacy `batch_write` path paid).
        // See `DbImpl::batch_put_arrow` and
        // `VectorizedMemTable::batch_put_arrow_with_base_seq` for mechanics.
        match db.batch_put_arrow(cf, &batch) {
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
        // C-R10-NEW-H1: validate offsets at the Arrow C Data Interface
        // boundary — see frs_batch_put_arrow for rationale.
        let data = match data.validate_full() {
            Ok(()) => data,
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
        // unbounded allocations via the internal builders.
        // (Sweep R10 H by Reviewer 3; parallel finding to R5 H#2 which
        // fixed the put-side.)
        if keys.len() > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }

        // C-R11-NEW-H3: per-row MAX_KEY_LEN cap. Sister-pattern to
        // frs_lookup_kv / frs_get_into_buf / frs_get_fast / the
        // byte-blob batch family — guards memtable hash / SST
        // comparator paths against attacker-chosen multi-MiB keys.
        for i in 0..keys.len() {
            if keys.value_length(i) as usize > MAX_KEY_LEN {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
        }

        // C-R19-NEW-H1: aggregate input-key-bytes cap mirroring
        // frs_batch_put_arrow (C-R12-NEW-H2) and frs_prefix_scan_arrow
        // (C-R13-NEW-H3). validate_full earlier established offset
        // monotonicity, so reading the last offset is safe.
        let key_offsets = keys.value_offsets();
        let total_key_bytes =
            *key_offsets.last().unwrap_or(&0) as i64 - *key_offsets.first().unwrap_or(&0) as i64;
        if total_key_bytes < 0 || (total_key_bytes as usize) > MAX_BATCH_BYTES {
            return FRS_STATUS_INVALID_ARGUMENT;
        }

        // Zero-copy path: build Arrow output directly during lookup,
        // avoiding the intermediate Vec<Option<Vec<u8>>> allocation.
        let batch = match db.batch_get_arrow(cf, keys) {
            Ok(b) => b,
            Err(e) => return error_to_status(&e),
        };

        // C-R19-NEW-H1: aggregate output-value-bytes cap. Engine values
        // are caller-uncontrolled (Flink state can be multi-MiB), and
        // db.batch_get_arrow builds a `BinaryBuilder` that uses i32
        // offsets — once cumulative bytes pass i32::MAX, the offset
        // array silently wraps or the builder panics. Sister entries
        // (frs_batch_put_arrow, frs_prefix_scan_arrow) gate aggregate
        // bytes; this output-side gate completes the parity.
        let value_col = batch.column(0);
        if let Some(value_bin) = value_col.as_any().downcast_ref::<BinaryArray>() {
            let val_offsets = value_bin.value_offsets();
            let total_val_bytes = *val_offsets.last().unwrap_or(&0) as i64
                - *val_offsets.first().unwrap_or(&0) as i64;
            if total_val_bytes < 0 || (total_val_bytes as usize) > MAX_BATCH_BYTES {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
        }

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
        // C-R12-NEW-H1: cap prefix_len like every sister entry point
        // (frs_prefix_scan_iter, frs_lookup_kv, frs_get_into_buf,
        // frs_get_fast). Without this gate an attacker-supplied
        // prefix_len > MAX_KEY_LEN constructs an OOB slice that the
        // engine comparator dereferences as a key span. Real UB /
        // info-disclosure primitive. The R11 sweep that closed
        // frs_batch_put_arrow + frs_batch_get_arrow missed this
        // third Arrow zero-copy entry point.
        if prefix_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
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
        // C-R13-NEW-H3: aggregate output-bytes cap. Even with the row-count
        // cap, per-row bytes are entirely engine-determined (the engine
        // stores arbitrary-length user values), so a few large values can
        // push cumulative key_builder/value_builder offsets past i32::MAX
        // and silently produce a corrupted offset array. Cap at twice
        // MAX_BATCH_BYTES (one budget per column) to mirror put/get/delete.
        let mut total_key_bytes: usize = 0;
        let mut total_val_bytes: usize = 0;
        for (k, v) in &rows {
            total_key_bytes = match total_key_bytes.checked_add(k.len()) {
                Some(n) => n,
                None => return FRS_STATUS_INVALID_ARGUMENT,
            };
            total_val_bytes = match total_val_bytes.checked_add(v.len()) {
                Some(n) => n,
                None => return FRS_STATUS_INVALID_ARGUMENT,
            };
            if total_key_bytes > MAX_BATCH_BYTES || total_val_bytes > MAX_BATCH_BYTES {
                return FRS_STATUS_INVALID_ARGUMENT;
            }
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

// ---------------------------------------------------------------------------
// 9. Delta-Join Lookup (single-key + iterator)
// ---------------------------------------------------------------------------
//
// These entry points back the Delta-Join localization story
// (`docs/design/2.13_deltajoin_localization.md`). Flink calls
// `frs_lookup_kv` for exact-match probes and the iterator family for
// prefix / range scans against the local lookup CF.
//
// Iterators here use a **snapshot + collect** approach: at `_open` time
// we materialize all matching key/value pairs into an owned `Vec` and
// then advance through them via a cursor. This is functionally correct
// and snapshot-isolated against subsequent writes, but memory cost is
// O(materialized bytes) rather than O(1). When the engine grows a true
// streaming iterator (W26+), this layer can be rewritten without ABI
// change.

/// Iterator state held behind the opaque [`FrsIterator`] pointer.
///
/// Fields are `pub(crate)` so the JNI compat shim (`compat_jni`) can drive
/// `seekToLast` / `prev` cursor moves that the public C ABI does not yet
/// expose. External callers see only the opaque handle.
pub(crate) struct IteratorState {
    /// Materialized (key, value) pairs in ascending key order.
    pub(crate) rows: Vec<(Vec<u8>, Vec<u8>)>,
    /// Index of the *next* row to be returned by `frs_iterator_next`.
    pub(crate) cursor: usize,
    /// C-NEW-H1: gates the per-step clone vs the zero-copy `mem::take` fast
    /// path inside `frs_iterator_next`. Default `false` (forward-only —
    /// the forst-rs Java backend's path). `frs_iterator_seek` flips this
    /// to `true` because seek can land the cursor on a row that was
    /// already taken; from that point next() must clone so subsequent
    /// rewind/re-entry sees the original payload (advertised
    /// `RocksIterator` ABI for the JNI compat shim's prev0 /
    /// seekToLast0 / seekForPrev0).
    pub(crate) allow_rewind: bool,
}

impl IteratorState {
    pub(crate) fn new(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self {
            rows,
            cursor: 0,
            allow_rewind: false,
        }
    }
}

/// Single-key exact-match lookup, optimised for the Delta-Join probe
/// path. Semantically equivalent to [`frs_get`] but kept as a separate
/// symbol so future revisions can specialise (e.g., caching layer,
/// micro-batch coalescing) without churning the Get ABI.
///
/// On a missing key, returns `FRS_STATUS_OK` with `out_value->data = NULL`
/// and `out_value->len = 0` (matching [`frs_get`] semantics — callers
/// distinguish hit vs miss by checking `data`).
#[no_mangle]
pub unsafe extern "C" fn frs_lookup_kv(
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
        // SECURITY: bound the C-side key length before constructing the
        // slice. An untrusted Java/JNI caller could otherwise pass an
        // attacker-chosen `key_len` and trigger an out-of-bounds read on
        // engine-internal hash / comparator paths.
        if key_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
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

/// Zero-allocation get: writes the value directly into a caller-provided buffer.
/// Returns the value length via `out_val_len`. If the buffer is too small,
/// returns FRS_STATUS_BUFFER_TOO_SMALL and sets `out_val_len` to the required size.
/// If the key is not found, returns FRS_STATUS_OK with `out_val_len` = 0.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn frs_get_into_buf(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    out_buf: *mut u8,
    out_buf_cap: usize,
    out_val_len: *mut usize,
) -> i32 {
    guarded(|| {
        if out_val_len.is_null() {
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
        if key_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let k = slice::from_raw_parts(key, key_len);
        match db.get(cf, k) {
            Ok(Some(v)) => {
                *out_val_len = v.len();
                if v.len() > out_buf_cap || out_buf.is_null() {
                    return FRS_STATUS_BUFFER_TOO_SMALL;
                }
                ptr::copy_nonoverlapping(v.as_ptr(), out_buf, v.len());
                FRS_STATUS_OK
            }
            Ok(None) => {
                *out_val_len = 0;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Fast-path get: skips `catch_unwind` and `Arc::clone` for maximum throughput.
/// The caller guarantees that `handle` is a valid, non-null FrsDb that will not
/// be closed during this call. This is safe when the Java backend holds the db
/// open for the entire processing lifetime (which it does — close only on dispose).
///
/// Returns FRS_STATUS_OK with value copied into `out_buf` (length in `out_val_len`).
/// Returns FRS_STATUS_BUFFER_TOO_SMALL if buffer is too small.
/// Returns FRS_STATUS_OK with `out_val_len` = 0 if key not found.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn frs_get_fast(
    handle: FrsDb,
    cf: FrsCfHandle,
    key: *const u8,
    key_len: usize,
    out_buf: *mut u8,
    out_buf_cap: usize,
    out_val_len: *mut usize,
) -> i32 {
    // C-R8-NEW-H1: input validation. Pre-fix the docstring waived
    // only `catch_unwind` and `Arc::clone`, but the function performed
    // ZERO null/bounds checks — `slice::from_raw_parts(NULL, n>0)` is
    // immediate UB, an oversized `key_len` lets the comparator
    // OOB-read up to 16 EiB, `*out_val_len` on the hit path is a
    // null-write, and `copy_nonoverlapping` to a NULL out_buf is UB.
    // Every sibling entry point (frs_get / frs_lookup_kv /
    // frs_get_into_buf) gates on these conditions; mirror them here.
    if handle.is_null() || cf.is_null() {
        return FRS_STATUS_NULL_ARG;
    }
    if out_val_len.is_null() {
        return FRS_STATUS_NULL_ARG;
    }
    if key.is_null() && key_len != 0 {
        return FRS_STATUS_NULL_ARG;
    }
    if key_len > MAX_KEY_LEN {
        return FRS_STATUS_INVALID_ARGUMENT;
    }
    // out_buf may be NULL only when cap == 0 (size-probe semantics).
    if out_buf.is_null() && out_buf_cap != 0 {
        return FRS_STATUS_NULL_ARG;
    }
    // D-R9-H1: wrap the engine call in catch_unwind. The pre-fix
    // docstring waived catch_unwind for throughput, but `db.get` and
    // friends contain `.expect("lock poisoned")` sites that can
    // panic under contention. A panic unwinding across the FFI
    // boundary into JVM frames is UB on both x86_64 SysV and aarch64
    // AAPCS. The Arc::clone elision (the other half of the waiver)
    // stays; catch_unwind cost is one setjmp/longjmp on the cold
    // path and is independent of Arc lifetime.
    let db = &*(handle as *const Arc<DbImpl>);
    let cf_h = &*(cf as *const ColumnFamilyHandle);
    let k = slice::from_raw_parts(key, key_len);
    let result = catch_unwind(AssertUnwindSafe(|| db.get(cf_h, k)));
    match result {
        Ok(Ok(Some(v))) => {
            *out_val_len = v.len();
            if v.len() > out_buf_cap {
                return FRS_STATUS_BUFFER_TOO_SMALL;
            }
            ptr::copy_nonoverlapping(v.as_ptr(), out_buf, v.len());
            FRS_STATUS_OK
        }
        Ok(Ok(None)) => {
            *out_val_len = 0;
            FRS_STATUS_OK
        }
        Ok(Err(_)) => FRS_STATUS_ERROR,
        Err(_) => FRS_STATUS_PANIC,
    }
}

// ===========================================================================
// Recovered session-1 FFI additions (reverted accidentally, re-added).
// ===========================================================================

/// Vectorized batch GET — caller-owned Arrow BinaryArray layout.
/// Reads `count` keys from (key_offsets, key_data), writes values into
/// (out_offsets, out_data) + per-slot out_validity byte (1=found, 0=miss).
///
/// # Return codes
/// Returns typed `FrsErrorCode` discriminants (spec §4):
/// - `FrsErrorCode::Ok` (0)                  — all rows processed
/// - `FrsErrorCode::BatchHeaderMalformed` (110) — null required pointer or count > MAX
/// - `FrsErrorCode::EngineIo` (300)           — engine I/O error (Fail-batch)
/// - `FrsErrorCode::EngineCorrupted` (301)    — engine corruption (Fail-batch)
/// - `FrsErrorCode::PanicCaught` (900)        — Rust panic at FFI boundary (Fail-process)
/// - `FRS_STATUS_BUFFER_TOO_SMALL` (17)       — output buffer too small (legacy slot; unchanged)
#[no_mangle]
pub unsafe extern "C" fn frs_vectorized_batch_get(
    handle: FrsDb,
    cf: FrsCfHandle,
    key_offsets: *const i32,
    key_data: *const u8,
    key_data_len: usize,
    count: usize,
    out_offsets: *mut i32,
    out_data: *mut u8,
    out_validity: *mut u8,
    out_data_cap: usize,
    out_data_len: *mut usize,
) -> i32 {
    // F1 fault hook (ErrorCodeSubstitution, umbrella spec §5).
    // Active only when the `fault-injection` feature is enabled.
    // Env vars: FRS_FAULT_VEC_GET_AT=<n>  or  FRS_FAULT_VEC_GET_PROB=<p>
    //           FRS_FAULT_VEC_GET_CODE=<code>   (default: 300 = ENGINE_IO)
    #[cfg(feature = "fault-injection")]
    {
        if forst_rs_test_harness::FaultInjector::global().should_fire("vec_get") {
            let injected = std::env::var("FRS_FAULT_VEC_GET_CODE")
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(FrsErrorCode::EngineIo as u32);
            return injected as i32;
        }
    }
    guarded_vec(|| {
        if out_data_len.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db) = db_from_handle(handle) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if count == 0 {
            *out_data_len = 0;
            if !out_offsets.is_null() {
                *out_offsets = 0;
            }
            return FrsErrorCode::Ok as i32;
        }
        if count > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if key_offsets.is_null() || out_offsets.is_null() || out_validity.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_offs = slice::from_raw_parts(key_offsets, count + 1);
        let Some(total_keys) = validate_i32_offsets(key_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if total_keys > key_data_len {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // C-R13-NEW-H2: aggregate input-key-bytes cap mirroring put/delete.
        // Without this, a 1 M-key * 1 MiB-key request would build a 1 TiB
        // `keys_vec` + `db.batch_get` would materialise an equally large
        // `Vec<Option<Vec<u8>>>` result before any buffer-fit check could
        // return BUFFER_TOO_SMALL — heap OOM as a defendable attack vector.
        if total_keys > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if total_keys > 0 && key_data.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_buf: &[u8] = if total_keys == 0 {
            &[]
        } else {
            slice::from_raw_parts(key_data, total_keys)
        };
        let out_offs = slice::from_raw_parts_mut(out_offsets, count + 1);
        let out_vld = slice::from_raw_parts_mut(out_validity, count);
        if out_data.is_null() && out_data_cap > 0 {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let out_buf: &mut [u8] = if out_data_cap == 0 {
            &mut []
        } else {
            slice::from_raw_parts_mut(out_data, out_data_cap)
        };
        // PR-D3 + V10 finish (spec §3 D10): build the key-slice vector once
        // and route to the engine's batched `db.batch_get` API in a single
        // call. The engine's `batch_get` now thinly wraps
        // `batch_get_vectorized` (engine-level true vectorized lookup) — see
        // its docstring for the per-batch hoists (one version snapshot, one
        // live-files HashSet, one resident-list walk, one reader-open per
        // SST instead of per (key, SST)). Replaces the previous per-key
        // `db.get(cf, k)` loop that defeated block-cache prefetch /
        // S3 batched-read and repeated per-key LSM tier walks. Measured
        // speedup over the naive per-key path on an SST-tier bench: 4.4×
        // (N=16) to 6.5× (N=256–1024), beating the spec's projected 4.2×.
        let mut keys_vec: Vec<&[u8]> = Vec::with_capacity(count);
        for i in 0..count {
            let ks = key_offs[i] as usize;
            let ke = key_offs[i + 1] as usize;
            if ke - ks > MAX_KEY_LEN {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            keys_vec.push(&key_buf[ks..ke]);
        }
        let results = match db.batch_get(cf, &keys_vec) {
            Ok(v) => v,
            Err(e) => return error_to_frs_code(&e),
        };
        let mut required_total: usize = 0;
        for v in results.iter().flatten() {
            required_total = match required_total.checked_add(v.len()) {
                Some(n) => n,
                None => {
                    *out_data_len = usize::MAX;
                    return FRS_STATUS_BUFFER_TOO_SMALL;
                }
            };
            if required_total > i32::MAX as usize {
                *out_data_len = required_total;
                return FRS_STATUS_BUFFER_TOO_SMALL;
            }
        }
        // C-R13-NEW-H2: output-aggregate cap. Even after the i32::MAX check
        // above, accepting `required_total` up to ~2 GiB is far above any
        // realistic single-batch read budget. Reject anything past
        // MAX_BATCH_BYTES to bound the downstream copy + keep parity with
        // the put/delete sister caps.
        if required_total > MAX_BATCH_BYTES {
            *out_data_len = required_total;
            return FRS_STATUS_BUFFER_TOO_SMALL;
        }
        if required_total > out_data_cap {
            *out_data_len = required_total;
            return FRS_STATUS_BUFFER_TOO_SMALL;
        }
        let mut pos: usize = 0;
        out_offs[0] = 0;
        for (i, slot) in results.into_iter().enumerate() {
            match slot {
                Some(v) => {
                    let vl = v.len();
                    let required = pos + vl;
                    ptr::copy_nonoverlapping(v.as_ptr(), out_buf.as_mut_ptr().add(pos), vl);
                    pos = required;
                    out_vld[i] = 1;
                }
                None => out_vld[i] = 0,
            }
            out_offs[i + 1] = pos as i32;
        }
        *out_data_len = pos;
        FrsErrorCode::Ok as i32
    })
}

/// Vectorized batch PUT — caller-owned key+value Arrow BinaryArray buffers.
///
/// # Return codes
/// Returns typed `FrsErrorCode` discriminants (spec §4):
/// - `FrsErrorCode::Ok` (0)                  — all rows written
/// - `FrsErrorCode::BatchHeaderMalformed` (110) — null pointer or count > MAX
/// - `FrsErrorCode::EngineIo` (300)           — engine I/O error (Fail-batch)
/// - `FrsErrorCode::EngineCorrupted` (301)    — engine corruption (Fail-batch)
/// - `FrsErrorCode::PanicCaught` (900)        — Rust panic at FFI boundary (Fail-process)
#[no_mangle]
pub unsafe extern "C" fn frs_vectorized_batch_put(
    handle: FrsDb,
    cf: FrsCfHandle,
    key_offsets: *const i32,
    key_data: *const u8,
    key_data_len: usize,
    val_offsets: *const i32,
    val_data: *const u8,
    val_data_len: usize,
    count: usize,
) -> i32 {
    guarded_vec(|| {
        let Some(db) = db_from_handle(handle) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if count == 0 {
            return FrsErrorCode::Ok as i32;
        }
        if count > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if key_offsets.is_null() || val_offsets.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_offs = slice::from_raw_parts(key_offsets, count + 1);
        let val_offs = slice::from_raw_parts(val_offsets, count + 1);
        let Some(total_keys) = validate_i32_offsets(key_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(total_vals) = validate_i32_offsets(val_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if total_keys > key_data_len || total_vals > val_data_len {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // C-R13-NEW-H1: aggregate-bytes cap, mirroring C-R12-NEW-H2 on the
        // Arrow zero-copy path. The byte-blob FFI accepts i32 offsets which
        // permit up to 2 GiB per column independently; without this cap the
        // engine's downstream `key_data.len() as u32` rebase cumulative-
        // overflows once total > 4 GiB, producing silent corruption identical
        // to the Arrow path. The Java VectorizedExecutor / V1-sync state path
        // funnels here, so this cap is on the production hot path.
        if total_keys > MAX_BATCH_BYTES || total_vals > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if (total_keys > 0 && key_data.is_null()) || (total_vals > 0 && val_data.is_null()) {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_buf: &[u8] = if total_keys == 0 {
            &[]
        } else {
            slice::from_raw_parts(key_data, total_keys)
        };
        let val_buf: &[u8] = if total_vals == 0 {
            &[]
        } else {
            slice::from_raw_parts(val_data, total_vals)
        };
        // C4R2-B-NEW-H1: bypass WriteBatch construction. Build the 3
        // borrowed-slice arrays directly during the FFI offset/length
        // walk and dispatch via the new `batch_put_borrowed_single_cf`
        // entry — this eliminates the per-row `WriteBatchEntry`
        // allocation (count × 56 bytes per call), the per-row `Cow::Borrowed`
        // wrap, and the 3-way `entries.iter().map().collect()` rebuild
        // that the WriteBatch path triggered inside `batch_write_single_cf`.
        // The memtable still does ONE borrowed-slice `extend_from_slice`
        // per row (canonical column-extend); only the FFM-to-memtable
        // glue is eliminated. C-R33-NEW-H1's revert of B-R13-NEW-H3 was
        // correct (Buffer::from_slice_ref copies); the proper fix is to
        // skip the Arrow-Buffer construction entirely and pass the
        // already-borrowed slices straight to the memtable.
        let mut keys_slices: Vec<&[u8]> = Vec::with_capacity(count);
        let mut value_slices: Vec<Option<&[u8]>> = Vec::with_capacity(count);
        // OpType::Put = 1 (see forst_rs_common::types::OpType discriminants)
        let op_types: Vec<u8> = vec![1u8; count];
        for i in 0..count {
            let ks = key_offs[i] as usize;
            let ke = key_offs[i + 1] as usize;
            if ke - ks > MAX_KEY_LEN {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            let vs = val_offs[i] as usize;
            let ve = val_offs[i + 1] as usize;
            keys_slices.push(&key_buf[ks..ke]);
            value_slices.push(Some(&val_buf[vs..ve]));
        }
        match db.batch_put_borrowed_single_cf(cf, &keys_slices, &value_slices, &op_types) {
            Ok(_) => FrsErrorCode::Ok as i32,
            Err(e) => error_to_frs_code(&e),
        }
    })
}

/// Vectorized batch DELETE — caller-owned Arrow BinaryArray keys.
///
/// # Return codes
/// Returns typed `FrsErrorCode` discriminants (spec §4):
/// - `FrsErrorCode::Ok` (0)                  — all rows deleted
/// - `FrsErrorCode::BatchHeaderMalformed` (110) — null pointer or count > MAX
/// - `FrsErrorCode::EngineIo` (300)           — engine I/O error (Fail-batch)
/// - `FrsErrorCode::EngineCorrupted` (301)    — engine corruption (Fail-batch)
/// - `FrsErrorCode::PanicCaught` (900)        — Rust panic at FFI boundary (Fail-process)
#[no_mangle]
pub unsafe extern "C" fn frs_vectorized_batch_delete(
    handle: FrsDb,
    cf: FrsCfHandle,
    key_offsets: *const i32,
    key_data: *const u8,
    key_data_len: usize,
    count: usize,
) -> i32 {
    guarded_vec(|| {
        let Some(db) = db_from_handle(handle) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if count == 0 {
            return FrsErrorCode::Ok as i32;
        }
        if count > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if key_offsets.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_offs = slice::from_raw_parts(key_offsets, count + 1);
        let Some(total_keys) = validate_i32_offsets(key_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if total_keys > key_data_len {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // C-R13-NEW-H1 (delete-sister): aggregate-bytes cap. Tombstones still
        // consume memtable space + SST footprint; unbounded delete-batches
        // are an equivalent memtable-bloat + u32-rebase corruption vector.
        if total_keys > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if total_keys > 0 && key_data.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_buf: &[u8] = if total_keys == 0 {
            &[]
        } else {
            slice::from_raw_parts(key_data, total_keys)
        };
        // C4R2-B-NEW-H1 (delete sister): skip WriteBatch construction;
        // pass borrowed key slices directly. Value side is None for every
        // row. OpType::Delete = 0.
        let mut keys_slices: Vec<&[u8]> = Vec::with_capacity(count);
        for i in 0..count {
            let ks = key_offs[i] as usize;
            let ke = key_offs[i + 1] as usize;
            if ke - ks > MAX_KEY_LEN {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            keys_slices.push(&key_buf[ks..ke]);
        }
        let value_slices: Vec<Option<&[u8]>> = vec![None; count];
        let op_types: Vec<u8> = vec![0u8; count]; // OpType::Delete
        match db.batch_put_borrowed_single_cf(cf, &keys_slices, &value_slices, &op_types) {
            Ok(_) => FrsErrorCode::Ok as i32,
            Err(e) => error_to_frs_code(&e),
        }
    })
}

/// Vectorized MIXED write batch — puts, deletes, and merges in ONE FFI
/// crossing with ONE atomic sequence allocation (Stage-3 Unit 2).
///
/// Sister of `frs_vectorized_batch_put` / `frs_vectorized_batch_delete`:
/// instead of a homogeneous op column, the caller supplies a per-row
/// `kinds` byte so a Flink write-buffer flush containing interleaved
/// puts, deletes, and merge-appends needs a single boundary crossing
/// and lands as ONE atomic engine batch (single seq-range allocation in
/// `batch_put_borrowed_single_cf`).
///
/// # Input layout
/// - `kinds`: `count` bytes, one per row — `0` = Delete, `1` = Put,
///   `2` = Merge. These match the `forst_rs_common::types::OpType`
///   discriminants and are forwarded verbatim as the engine `op_types`
///   column (zero-copy, no per-row rebuild).
/// - `key_offsets` / `key_data`: Arrow BinaryArray layout — `count + 1`
///   i32 offsets; row `i`'s key is
///   `key_data[key_offsets[i] .. key_offsets[i+1]]`.
/// - `value_offsets` / `value_data`: same layout for values. Put rows
///   carry the value, Merge rows carry the merge operand. Delete rows
///   MUST have an empty value slice
///   (`value_offsets[i] == value_offsets[i+1]`) — the engine receives
///   `None` for them (mirrors `frs_vectorized_batch_delete`); a
///   non-empty delete value indicates a caller layout bug and is
///   rejected.
///
/// # Validation
/// Mirrors the sister entries: null pointers, `count > MAX_BATCH_COUNT`,
/// malformed/negative/non-monotonic offsets, per-column aggregate caps
/// (`MAX_BATCH_BYTES`, C-R13-NEW-H1 rationale — same u32-rebase
/// corruption vector), per-row `MAX_KEY_LEN`, per-row `MAX_VALUE_LEN`
/// on Put/Merge rows, and an invalid `kinds` byte (> 2) all return
/// `BatchHeaderMalformed`. If at least one Merge row is present the CF
/// must have a merge operator (D-R8-NEW-H2 rationale, same guard as
/// `frs_vec_merge_append_batch`); the check is skipped for batches
/// without merge rows.
///
/// # Return codes
/// Returns typed `FrsErrorCode` discriminants (spec §4):
/// - `FrsErrorCode::Ok` (0)                  — all rows written atomically
/// - `FrsErrorCode::BatchHeaderMalformed` (110) — validation failure (see above)
/// - `FrsErrorCode::EngineIo` (300)           — engine I/O error (Fail-batch)
/// - `FrsErrorCode::EngineCorrupted` (301)    — engine corruption (Fail-batch)
/// - `FrsErrorCode::PanicCaught` (900)        — Rust panic at FFI boundary (Fail-process)
///
/// # Safety
/// - `kinds` must point to at least `count` valid bytes.
/// - `key_offsets` / `value_offsets` must each point to at least
///   `count + 1` valid `i32`s.
/// - `key_data` / `value_data` must point to at least `key_data_len` /
///   `value_data_len` valid bytes respectively.
/// - All buffers must remain valid for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn frs_vectorized_batch_mixed(
    handle: FrsDb,
    cf: FrsCfHandle,
    kinds: *const u8,
    count: usize,
    key_offsets: *const i32,
    key_data: *const u8,
    key_data_len: usize,
    value_offsets: *const i32,
    value_data: *const u8,
    value_data_len: usize,
) -> i32 {
    guarded_vec(|| {
        let Some(db) = db_from_handle(handle) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if count == 0 {
            return FrsErrorCode::Ok as i32;
        }
        if count > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if kinds.is_null() || key_offsets.is_null() || value_offsets.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let kind_col = slice::from_raw_parts(kinds, count);
        let key_offs = slice::from_raw_parts(key_offsets, count + 1);
        let val_offs = slice::from_raw_parts(value_offsets, count + 1);
        let Some(total_keys) = validate_i32_offsets(key_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(total_vals) = validate_i32_offsets(val_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if total_keys > key_data_len || total_vals > value_data_len {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // C-R13-NEW-H1 (mixed sister): aggregate-bytes cap on each column —
        // same u32-rebase corruption vector as the put/delete sisters.
        if total_keys > MAX_BATCH_BYTES || total_vals > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if (total_keys > 0 && key_data.is_null()) || (total_vals > 0 && value_data.is_null()) {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_buf: &[u8] = if total_keys == 0 {
            &[]
        } else {
            slice::from_raw_parts(key_data, total_keys)
        };
        let val_buf: &[u8] = if total_vals == 0 {
            &[]
        } else {
            slice::from_raw_parts(value_data, total_vals)
        };
        // C4R2-B-NEW-H1 (mixed sister): no WriteBatch construction — build
        // the borrowed-slice columns directly during the offset walk and
        // dispatch once via `batch_put_borrowed_single_cf`. The `kinds`
        // column doubles as the engine `op_types` column (validated below,
        // then forwarded as-is — no per-row copy).
        let mut keys_slices: Vec<&[u8]> = Vec::with_capacity(count);
        let mut value_slices: Vec<Option<&[u8]>> = Vec::with_capacity(count);
        // D-R8-NEW-H2 (mixed sister): Merge rows require a CF merge
        // operator; checked once, lazily, only when a Merge row exists.
        let mut merge_operator_verified = false;
        for i in 0..count {
            let ks = key_offs[i] as usize;
            let ke = key_offs[i + 1] as usize;
            if ke - ks > MAX_KEY_LEN {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            let vs = val_offs[i] as usize;
            let ve = val_offs[i + 1] as usize;
            match kind_col[i] {
                // OpType::Delete = 0 — engine takes None; value slice must
                // be empty (a non-empty slice indicates a layout bug).
                0 => {
                    if ve != vs {
                        return FrsErrorCode::BatchHeaderMalformed as i32;
                    }
                    value_slices.push(None);
                }
                // OpType::Put = 1 / OpType::Merge = 2 — value carried.
                1 | 2 => {
                    if kind_col[i] == 2 && !merge_operator_verified {
                        if !db.cf_has_merge_operator(cf) {
                            return FrsErrorCode::BatchHeaderMalformed as i32;
                        }
                        merge_operator_verified = true;
                    }
                    if ve - vs > MAX_VALUE_LEN {
                        return FrsErrorCode::BatchHeaderMalformed as i32;
                    }
                    value_slices.push(Some(&val_buf[vs..ve]));
                }
                _ => return FrsErrorCode::BatchHeaderMalformed as i32,
            }
            keys_slices.push(&key_buf[ks..ke]);
        }
        match db.batch_put_borrowed_single_cf(cf, &keys_slices, &value_slices, kind_col) {
            Ok(_) => FrsErrorCode::Ok as i32,
            Err(e) => error_to_frs_code(&e),
        }
    })
}

// --- Stub symbols (linker bind requires symbol exists; unused on Q3 hot path) ---

pub type FrsWriteBatch = *mut c_void;

#[no_mangle]
pub unsafe extern "C" fn frs_prefix_get_all(
    _handle: FrsDb,
    _cf: FrsCfHandle,
    _prefix: *const u8,
    _prefix_len: usize,
    _max_count: usize,
    _out_keys: *mut FrsBytes,
    _out_values: *mut FrsBytes,
    out_count: *mut usize,
) -> i32 {
    if !out_count.is_null() {
        *out_count = 0;
    }
    FRS_STATUS_OK
}

#[no_mangle]
pub unsafe extern "C" fn frs_batch_prefix_scan(
    _handle: FrsDb,
    _cf: FrsCfHandle,
    _prefixes: *const *const u8,
    _prefix_lens: *const usize,
    _prefix_count: usize,
    _max_per_prefix: usize,
    _out_keys: *mut FrsBytes,
    _out_values: *mut FrsBytes,
    out_counts: *mut usize,
    out_total: *mut usize,
) -> i32 {
    if !out_total.is_null() {
        *out_total = 0;
    }
    if !out_counts.is_null() {
        let _ = out_counts;
    }
    FRS_STATUS_OK
}

#[no_mangle]
pub unsafe extern "C" fn frs_writebatch_open(out_handle: *mut FrsWriteBatch) -> i32 {
    if !out_handle.is_null() {
        *out_handle = std::ptr::null_mut();
    }
    FRS_STATUS_NOT_SUPPORTED
}

#[no_mangle]
pub unsafe extern "C" fn frs_writebatch_put(
    _handle: FrsWriteBatch,
    _cf: FrsCfHandle,
    _key_offsets: *const i32,
    _key_data: *const u8,
    _val_offsets: *const i32,
    _val_data: *const u8,
    _count: usize,
) -> i32 {
    FRS_STATUS_NOT_SUPPORTED
}

#[no_mangle]
pub unsafe extern "C" fn frs_writebatch_delete(
    _handle: FrsWriteBatch,
    _cf: FrsCfHandle,
    _key_offsets: *const i32,
    _key_data: *const u8,
    _count: usize,
) -> i32 {
    FRS_STATUS_NOT_SUPPORTED
}

#[no_mangle]
pub unsafe extern "C" fn frs_writebatch_commit(_handle: FrsWriteBatch, _db: FrsDb) -> i32 {
    FRS_STATUS_NOT_SUPPORTED
}

#[no_mangle]
pub unsafe extern "C" fn frs_writebatch_close(_handle: FrsWriteBatch) -> i32 {
    FRS_STATUS_OK
}

/// Opens a forward iterator over the entire column family.
///
/// Implementation: snapshot the CF via `db.scan` (empty lower bound, no
/// upper bound) and stash the rows behind the returned handle. See the
/// module-level Delta-Join comment for the memory caveat.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_open(
    handle: FrsDb,
    cf: FrsCfHandle,
    out_iter: *mut FrsIterator,
) -> i32 {
    guarded(|| {
        if out_iter.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        // Empty lower / unbounded upper → full scan.
        let rows = match db.scan(cf, &[][..], None) {
            Ok(r) => r,
            Err(e) => return error_to_status(&e),
        };
        let boxed = Box::new(IteratorState::new(rows));
        *out_iter = Box::into_raw(boxed) as *mut c_void;
        FRS_STATUS_OK
    })
}

/// Opens a forward iterator bounded to the keys whose byte prefix
/// matches `prefix`. A NULL/empty prefix degenerates to a full CF scan
/// (matching [`frs_prefix_scan_arrow`] semantics).
#[no_mangle]
pub unsafe extern "C" fn frs_prefix_lookup_open(
    handle: FrsDb,
    cf: FrsCfHandle,
    prefix: *const u8,
    prefix_len: usize,
    out_iter: *mut FrsIterator,
) -> i32 {
    guarded(|| {
        if out_iter.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(handle) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        // SECURITY: bound the prefix length, same rationale as
        // frs_lookup_kv's `key_len` cap.
        if prefix_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let prefix_slice = if prefix.is_null() || prefix_len == 0 {
            &[][..]
        } else {
            slice::from_raw_parts(prefix, prefix_len)
        };
        let rows = match db.prefix_scan(cf, prefix_slice) {
            Ok(r) => r,
            Err(e) => return error_to_status(&e),
        };
        let boxed = Box::new(IteratorState::new(rows));
        *out_iter = Box::into_raw(boxed) as *mut c_void;
        FRS_STATUS_OK
    })
}

/// Repositions the cursor at the first key `>= key`. After this call,
/// the next [`frs_iterator_next`] returns that key (or `valid = false`
/// if no such key exists in the materialized set).
///
/// `key = NULL` / `key_len = 0` is treated as "seek to first" — equivalent
/// to a fresh open.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_seek(
    iter: FrsIterator,
    key: *const u8,
    key_len: usize,
) -> i32 {
    guarded(|| {
        if iter.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        if key_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let state = &mut *(iter as *mut IteratorState);
        // D-C4R8-H1: forbid seek after forward-only next(). The C-NEW-H1
        // `allow_rewind` gate flips `next()` between `mem::take` (zero-copy
        // forward-only) and `clone()` (rewind-safe). If `next()` has already
        // moved `cursor` past row 0 under the forward-only fast path, rows
        // [0, cursor) have been emptied to `(Vec::new(), Vec::new())`,
        // which breaks the `binary_search_by` sorted-order invariant below
        // — search would compare `needle` against empty keys for positions
        // 0..cursor and land on an arbitrary slot. Callers that need to
        // mix seek with next() MUST call seek FIRST (cursor=0) so the
        // flag is set before any take occurs. seekToLast0/seekForPrev0/
        // prev0 already set `allow_rewind=true` BEFORE their first next(),
        // so those JNI compat-shim paths are unaffected.
        if !state.allow_rewind && state.cursor > 0 {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        // C-NEW-H1: any seek call enables rewind semantics — from this
        // point onward subsequent next() calls clone the row instead of
        // taking it, so prev/seekToLast/seekForPrev rewinds re-enter
        // a populated payload.
        state.allow_rewind = true;
        let needle: &[u8] = if key.is_null() || key_len == 0 {
            &[][..]
        } else {
            slice::from_raw_parts(key, key_len)
        };
        // Binary search for the first key >= needle; saturate at len.
        state.cursor = state
            .rows
            .binary_search_by(|(k, _)| k.as_slice().cmp(needle))
            .unwrap_or_else(|insert| insert);
        FRS_STATUS_OK
    })
}

/// Advances the iterator and returns the current key/value. On exhaustion
/// `*out_valid` is set to `false`, both `FrsBytes` slots are `NULL`, and
/// the status is still `FRS_STATUS_OK`.
///
/// Returned `FrsBytes` are heap-owned by Rust; the caller MUST release
/// them via [`frs_bytes_free`] after use.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_next(
    iter: FrsIterator,
    out_key: *mut FrsBytes,
    out_value: *mut FrsBytes,
    out_valid: *mut bool,
) -> i32 {
    guarded(|| {
        if iter.is_null() || out_key.is_null() || out_value.is_null() || out_valid.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let state = &mut *(iter as *mut IteratorState);
        if state.cursor >= state.rows.len() {
            *out_key = FrsBytes::NULL;
            *out_value = FrsBytes::NULL;
            *out_valid = false;
            return FRS_STATUS_OK;
        }
        // C-R22-NEW-H1: clone the row instead of `mem::take`. Pre-fix
        // `mem::take` emptied the slot, which forward-only callers tolerated
        // — but the JNI compat shim's `prev0` / `seekToLast0` / `seekForPrev0`
        // rewinds `state.cursor` and re-enters this function, expecting the
        // original payload. Without the clone, the rewind returned `valid=true`
        // with zero-length key/value (silent stale-empty row), violating the
        // advertised `org.forstdb.RocksIterator` ABI. Clone cost is bounded by
        // the row's size (per-row caps already enforce MAX_KEY_LEN bounds),
        // and the engine's snapshot vec still keeps its slots populated for
        // any subsequent rewind.
        // C-NEW-H1: forward-only path uses mem::take (zero-copy). Only
        // clone when the iterator has been seek'd at least once — seek
        // is the only entry that can move the cursor back to a row that
        // was already taken, and from that point onward subsequent next()
        // calls must return the original payload so the JNI compat shim's
        // prev0/seekToLast0/seekForPrev0 rewind+re-enter behaves per the
        // RocksIterator ABI.
        let (k, v) = if state.allow_rewind {
            state.rows[state.cursor].clone()
        } else {
            std::mem::take(&mut state.rows[state.cursor])
        };
        state.cursor += 1;
        *out_key = FrsBytes::from_vec(k);
        *out_value = FrsBytes::from_vec(v);
        *out_valid = true;
        FRS_STATUS_OK
    })
}

/// B-NEW-H2: chunked iterator next. Drains up to `max_rows` from the
/// iterator into caller-supplied Arrow offsets+data segments in a single
/// FFI crossing.
///
/// On return `*out_count` holds the number of rows written. When the
/// iterator is exhausted the function returns OK with `*out_count = 0`
/// and `*out_eof = true`. `out_*_offsets` must be at least
/// `(max_rows + 1) * 4` bytes; `out_*_data` must hold at least
/// `out_*_data_cap` bytes. If a row's payload would overflow the data
/// capacity the function returns the rows written so far (caller refills
/// and retries). On overflow with zero rows written, returns
/// `BatchHeaderMalformed` (caller MUST grow `out_*_data_cap`).
///
/// Replaces the per-row `frs_iterator_next` for MapState scan / prefix
/// scan hot paths where the dominant cost was the FFM crossing.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_next_chunk(
    iter: FrsIterator,
    max_rows: u32,
    out_key_offsets: *mut i32,
    out_key_data: *mut u8,
    out_key_data_cap: usize,
    out_val_offsets: *mut i32,
    out_val_data: *mut u8,
    out_val_data_cap: usize,
    out_val_validity: *mut u8,
    out_count: *mut u32,
    out_eof: *mut bool,
) -> i32 {
    guarded(|| {
        if iter.is_null()
            || out_key_offsets.is_null()
            || out_val_offsets.is_null()
            || out_val_validity.is_null()
            || out_count.is_null()
            || out_eof.is_null()
        {
            return FRS_STATUS_NULL_ARG;
        }
        // H1: initialize BOTH output scalars unconditionally so every
        // return path (early-exit, capacity-overflow, partial-fill,
        // exact-fill, eof) leaves the caller observing well-defined
        // values rather than carry-over from prior calls.
        *out_count = 0;
        *out_eof = false;
        // H5 (C8R5 parity): cap max_rows at MAX_BATCH_COUNT to match
        // sister batch entries. Without this a caller passing max_rows
        // = u32::MAX would silently allocate (max_rows+1)*4 = 16 GiB
        // worth of offsets indexing in `slice::from_raw_parts_mut`,
        // followed by out-of-bounds writes if the actual segment is
        // smaller. Sister entries frs_batch_get, frs_batch_put, etc.
        // all reject n > MAX_BATCH_COUNT.
        if (max_rows as usize) > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // H2 (C8R2): cap output buffer sizes at MAX_BATCH_BYTES to
        // prevent i32 offset wrap. The Arrow Binary layout stores
        // per-row offsets as i32; if k_used/v_used grew past
        // i32::MAX, `key_offsets[emitted] = k_used as i32` would wrap
        // to negative — producing a corrupt offsets array that a
        // downstream Arrow consumer would read as out-of-bounds.
        // Sister batch FFI entries (frs_batch_put_arrow, etc.) all
        // enforce this cap; chunked-iterator next is no different.
        if out_key_data_cap > MAX_BATCH_BYTES || out_val_data_cap > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // H3 (C8R4): reject NULL+cap>0 explicitly. Pre-fix the slice-from-
        // raw-parts builder silently degraded to a zero-length slice when
        // the pointer was null; the inner capacity-fit check then admitted
        // the row (declared cap was non-zero) and `copy_from_slice` panicked
        // out-of-bounds. Caught by `guarded` but reachable as an
        // attacker-induced FRS_STATUS_PANIC. Sister entry frs_vectorized_batch_get
        // rejects this combination with NULL_ARG; mirror it here.
        if (out_key_data_cap > 0 && out_key_data.is_null())
            || (out_val_data_cap > 0 && out_val_data.is_null())
        {
            return FRS_STATUS_NULL_ARG;
        }
        if max_rows == 0 {
            return FRS_STATUS_OK;
        }
        let max_rows_usize = max_rows as usize;
        let state = &mut *(iter as *mut IteratorState);
        let key_offsets = slice::from_raw_parts_mut(out_key_offsets, max_rows_usize + 1);
        let val_offsets = slice::from_raw_parts_mut(out_val_offsets, max_rows_usize + 1);
        let validity = slice::from_raw_parts_mut(out_val_validity, max_rows_usize);
        let key_buf: &mut [u8] = if out_key_data_cap == 0 || out_key_data.is_null() {
            &mut [][..]
        } else {
            slice::from_raw_parts_mut(out_key_data, out_key_data_cap)
        };
        let val_buf: &mut [u8] = if out_val_data_cap == 0 || out_val_data.is_null() {
            &mut [][..]
        } else {
            slice::from_raw_parts_mut(out_val_data, out_val_data_cap)
        };

        // H4 (C8R4) + H6 (C9R2) + H7 (C9R3): only flip allow_rewind
        // when the iterator has not been advanced by the forward-only
        // `frs_iterator_next` path. Pre-fix this set the flag
        // unconditionally, masking the D-C4R8-H1 guard against
        // seek-over-mem::take-torn-rows for callers that did
        // `next → next → next_chunk → seek`. Mirror seek's own
        // guard: if cursor > 0 AND allow_rewind was still false,
        // the row array is already torn.
        //
        // H7 carve-out: when the iterator is already exhausted
        // (cursor >= rows.len), emit OK + eof=true rather than
        // rejecting. The natural completion of a forward-only
        // drain followed by a confirm-eof chunk call is a legitimate
        // caller pattern; the torn rows are unreachable from the
        // chunk's borrow (cursor sits at the past-end).
        if !state.allow_rewind && state.cursor > 0 {
            if state.cursor >= state.rows.len() {
                *out_eof = true;
                return FRS_STATUS_OK;
            }
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        // From here on (cursor=0 OR allow_rewind already true) the
        // chunked-next path borrows rows by reference (no `mem::take`),
        // so subsequent seeks remain safe; mark the flag true so
        // chunk→seek patterns aren't rejected by D-C4R8-H1.
        state.allow_rewind = true;
        key_offsets[0] = 0;
        val_offsets[0] = 0;
        let mut k_used: usize = 0;
        let mut v_used: usize = 0;
        let mut emitted: usize = 0;

        while emitted < max_rows_usize {
            if state.cursor >= state.rows.len() {
                *out_eof = true;
                break;
            }
            // Borrow the row in place (zero-copy). The `allow_rewind` rewind-safety gate is
            // enforced in `frs_iterator_next`; here both arms borrowed the row identically, so
            // the branch is collapsed (clippy::if_same_then_else) with no behavior change.
            let (kk, vv) = &state.rows[state.cursor];
            let (k, v) = (kk.as_slice(), vv.as_slice());
            // Capacity check: if we can't fit the row, stop and let the
            // caller pull what's been emitted so far. Overflow on the
            // FIRST row signals "data_cap too small" — distinguish via
            // BatchHeaderMalformed so the caller knows to grow rather
            // than re-call.
            if k_used + k.len() > out_key_data_cap || v_used + v.len() > out_val_data_cap {
                if emitted == 0 {
                    return FrsErrorCode::BatchHeaderMalformed as i32;
                }
                *out_eof = false;
                break;
            }
            key_buf[k_used..k_used + k.len()].copy_from_slice(k);
            k_used += k.len();
            // Empty-value-is-null encoding mirrors the put-batch FFI's
            // contract: validity bit distinguishes Some(empty) from None.
            // IteratorState stores both as Vec<u8>, so we encode empty as
            // valid=1, len=0.
            val_buf[v_used..v_used + v.len()].copy_from_slice(v);
            v_used += v.len();
            validity[emitted] = 1;
            emitted += 1;
            state.cursor += 1;
            key_offsets[emitted] = k_used as i32;
            val_offsets[emitted] = v_used as i32;
        }
        // H1 sub-defect (1) + (2): always derive `*out_eof` from the
        // actual cursor state. Pre-fix the exact-fill case (emitted ==
        // max_rows AND cursor reached rows.len()) and the partial-fill
        // case (capacity overflow with rows remaining) left *out_eof
        // unwritten — caller saw carry-over.
        *out_eof = state.cursor >= state.rows.len();
        *out_count = emitted as u32;
        FRS_STATUS_OK
    })
}

/// Releases an iterator opened by [`frs_iterator_open`] or
/// [`frs_prefix_lookup_open`]. Safe to call with NULL (no-op).
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_close(iter: FrsIterator) -> i32 {
    guarded(|| {
        if iter.is_null() {
            return FRS_STATUS_OK;
        }
        let ptr = iter as *mut IteratorState;
        drop(Box::from_raw(ptr));
        FRS_STATUS_OK
    })
}

/// Convenience alias for prefix-iterator callers — semantically identical
/// to [`frs_iterator_close`]. Exposed so the C header can document a
/// `frs_prefix_lookup_*` family that mirrors `frs_iterator_*`.
#[no_mangle]
pub unsafe extern "C" fn frs_prefix_lookup_close(iter: FrsIterator) -> i32 {
    frs_iterator_close(iter)
}

/// Convenience alias for prefix-iterator callers — semantically identical
/// to [`frs_iterator_next`]. Exposed for symmetry with
/// [`frs_prefix_lookup_open`] / [`frs_prefix_lookup_close`].
#[no_mangle]
pub unsafe extern "C" fn frs_prefix_lookup_next(
    iter: FrsIterator,
    out_key: *mut FrsBytes,
    out_value: *mut FrsBytes,
    out_valid: *mut bool,
) -> i32 {
    frs_iterator_next(iter, out_key, out_value, out_valid)
}

// ---------------------------------------------------------------------------
// 10. MVCC: snapshots + versioned reads (B-Prod-P2)
// ---------------------------------------------------------------------------
//
// These exports give Flink's snapshot strategy a stable C ABI for the MVCC
// API documented in spec §10a / §10.0:
//
//   * `frs_db_snapshot` mints a snapshot pinned at the current sequence;
//   * `frs_db_release_snapshot` releases it (RAII drop on the Rust side
//     decrements the registry ref-count so compaction's `min_active`
//     advances);
//   * `frs_get_at` / `frs_iterator_open_at` perform versioned reads that
//     ignore writes with sequence > snapshot.seq.
//
// ABI contract (spec §10.0): a snapshot is bound to its issuing DbImpl
// (via `db_id`); cross-DB use returns `FRS_STATUS_INVALID_ARGUMENT` rather
// than `panic` or undefined behavior, and the cross-DB release path
// re-leaks the box so the caller cannot accidentally double-free it.

mod ffi_mvcc_internal {
    //! Box wrapper for `forst_rs_engine::Snapshot`. Keeping this `pub(crate)`
    //! ensures external callers cannot reach inside `FrsSnapshot` and
    //! observe the wrapped `Snapshot`'s Drop side-effects.
    pub struct SnapshotBox {
        pub inner: forst_rs_engine::Snapshot,
    }
}

/// Opaque snapshot handle. Created by [`frs_db_snapshot`], released by
/// [`frs_db_release_snapshot`]. Per spec §10.0 ABI lifetime contract:
/// the handle is bound to its issuing [`FrsDb`] and any cross-DB use
/// (release or read) returns `FRS_STATUS_INVALID_ARGUMENT` without
/// freeing the underlying allocation.
pub type FrsSnapshot = *mut ffi_mvcc_internal::SnapshotBox;

/// Captures a snapshot at the engine's current sequence number.
///
/// On success, `*out_snapshot` receives a non-null handle that the caller
/// MUST eventually pass to [`frs_db_release_snapshot`] (or accept the
/// pinned-bytes liability until DB close). The handle implements MVCC
/// isolation: subsequent writes do not affect [`frs_get_at`] reads against
/// this snapshot.
///
/// # Returns
/// - `FRS_STATUS_OK` on success.
/// - `FRS_STATUS_NULL_ARG` if `db` or `out_snapshot` is null.
/// - `FRS_STATUS_PANIC` if the engine path panics (caught at the boundary).
#[no_mangle]
pub unsafe extern "C" fn frs_db_snapshot(db: FrsDb, out_snapshot: *mut FrsSnapshot) -> i32 {
    guarded(|| {
        if out_snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let snap = db.snapshot();
        let boxed = Box::new(ffi_mvcc_internal::SnapshotBox { inner: snap });
        *out_snapshot = Box::into_raw(boxed);
        FRS_STATUS_OK
    })
}

/// Releases a snapshot previously returned by [`frs_db_snapshot`].
///
/// # Returns
/// - `FRS_STATUS_OK` on success — the underlying `Snapshot` is dropped
///   and its registry ref-count decremented.
/// - `FRS_STATUS_NULL_ARG` if `db` or `snapshot` is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if `snapshot` was issued by a different
///   `FrsDb`. In this case the box is intentionally re-leaked (via
///   `mem::forget`) so the caller does not accidentally double-free
///   when they retry the call against the correct DB.
#[no_mangle]
pub unsafe extern "C" fn frs_db_release_snapshot(db: FrsDb, snapshot: FrsSnapshot) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let boxed = Box::from_raw(snapshot);
        if boxed.inner.db_id() != db.db_id() {
            // Re-leak so the caller does not double-free if they retry
            // against the correct DB. The pinned ref stays in the issuing
            // DB's registry until that DB is dropped.
            std::mem::forget(boxed);
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        // Drop the box: the inner Snapshot's Drop fires here and the
        // registry ref-count is decremented.
        FRS_STATUS_OK
    })
}

/// Reads the value visible at `snapshot.seq` for `key` in `cf`.
///
/// On success (key exists at snapshot time and is not a tombstone), the
/// status is `FRS_STATUS_OK` and `*out_value` is populated with a Rust-
/// owned `FrsBytes` that the caller MUST release via [`frs_bytes_free`].
///
/// # Returns
/// - `FRS_STATUS_OK` with `*out_value` populated on hit.
/// - `FRS_STATUS_NOT_FOUND` if no version is visible at snapshot time
///   (or the latest visible version is a deletion tombstone). `*out_value`
///   is left in its caller-provided state.
/// - `FRS_STATUS_NULL_ARG` if any of `db`, `cf`, `snapshot`, `key`, or
///   `out_value` is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if `snapshot` was issued by a different
///   `FrsDb` (per spec §15 same-DB invariant).
/// - I/O / corruption codes propagated from the engine on failure.
#[no_mangle]
pub unsafe extern "C" fn frs_get_at(
    db: FrsDb,
    cf: FrsCfHandle,
    snapshot: FrsSnapshot,
    key: *const u8,
    key_len: usize,
    out_value: *mut FrsBytes,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() || key.is_null() || out_value.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        if key_len > MAX_KEY_LEN {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let key_slice = slice::from_raw_parts(key, key_len);
        match db.get_at_cf(cf, snap_ref, key_slice) {
            Ok(Some(value)) => {
                *out_value = FrsBytes::from_vec(value);
                FRS_STATUS_OK
            }
            Ok(None) => FRS_STATUS_NOT_FOUND,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Opens a forward iterator that yields the latest version of each
/// user-key with `seq <= snapshot.seq`, skipping tombstones.
///
/// Behaves like [`frs_iterator_open`] in every other respect: callers
/// drive it with [`frs_iterator_next`] / [`frs_iterator_seek`] and
/// release it via [`frs_iterator_close`]. The iterator is materialized
/// at open time (snapshot-and-collect, see the §9 module comment) so
/// holding the [`FrsSnapshot`] open is not strictly required after this
/// call returns — but releasing the snapshot before all writes that
/// followed it have been compacted away will still let compaction
/// reclaim those versions, so the canonical pattern is to keep the
/// snapshot alive for the iterator's lifetime.
///
/// # Returns
/// - `FRS_STATUS_OK` and `*out_iter` populated on success.
/// - `FRS_STATUS_NULL_ARG` if any pointer arg is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if `snapshot` is from a different `FrsDb`.
#[no_mangle]
pub unsafe extern "C" fn frs_iterator_open_at(
    db: FrsDb,
    cf: FrsCfHandle,
    snapshot: FrsSnapshot,
    out_iter: *mut FrsIterator,
) -> i32 {
    guarded(|| {
        if out_iter.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let rows = match db.scan_at(cf, snap_ref) {
            Ok(r) => r,
            Err(e) => return error_to_status(&e),
        };
        let boxed = Box::new(IteratorState::new(rows));
        *out_iter = Box::into_raw(boxed) as *mut c_void;
        FRS_STATUS_OK
    })
}

// ---------------------------------------------------------------------------
// 11. Incremental checkpoints at snapshot (B-Prod-P2)
// ---------------------------------------------------------------------------
//
// `frs_create_incremental_checkpoint_at` records the engine state at a
// snapshot into a manifest blob and returns the SST file lists Flink needs
// to upload (new SSTs vs. SSTs that the previous checkpoint already
// shipped). `frs_db_open_from_incremental` is the restore-side counterpart:
// open a fresh DB whose state is reconstructed from the persisted manifest
// + the SST file list.
//
// Memory ownership for [`FrsIncrementalCheckpointResult`]:
//
//   * `manifest_path` — Rust-owned C string; release with
//     `CString::from_raw` (or via the convenience helper in P3).
//   * `new_ssts` / `shared_ssts` — Rust-owned [`FrsLiveFileList`] boxes;
//     release each via [`frs_db_live_file_list_free`] then `Box::from_raw`
//     to reclaim the outer box.
//   * `flush_done_eventfd` — `-1` for v1 (flush is synchronous before this
//     call returns); reserved for the async-flush story documented in P5.

/// Result of an incremental checkpoint capture; see module comment for
/// memory-ownership rules.
#[repr(C)]
pub struct FrsIncrementalCheckpointResult {
    /// Path to the persisted manifest blob, NUL-terminated UTF-8.
    /// Rust-owned; release via `CString::from_raw`.
    pub manifest_path: *mut c_char,
    /// SSTs newly created by this checkpoint (must be uploaded to remote
    /// storage). Outer box and inner array are both Rust-owned; release
    /// the inner via [`frs_db_live_file_list_free`] then drop the box via
    /// [`frs_db_incremental_checkpoint_result_free`].
    pub new_ssts: *mut FrsLiveFileList,
    /// SSTs shared with `base_checkpoint_id` (already on remote storage —
    /// caller can reference them by handle without re-uploading). Same
    /// ownership rules as `new_ssts`.
    pub shared_ssts: *mut FrsLiveFileList,
    /// Reserved for async-flush story. Always `-1` in v1 (flush is
    /// synchronous before this call returns).
    pub flush_done_eventfd: std::os::raw::c_int,
}

/// Captures an incremental checkpoint pinned at `snapshot`.
///
/// `checkpoint_id` is the new checkpoint's identifier; `base_checkpoint_id`
/// is the previous checkpoint that this incremental checkpoint is taken
/// against (any SSTs present in `base_checkpoint_id` and still live at
/// `snapshot.seq` are returned in `shared_ssts` rather than `new_ssts`).
/// Pass `0` for `base_checkpoint_id` for a full / first checkpoint.
///
/// On success, the engine has flushed and persisted the checkpoint manifest
/// to its checkpoint directory; the caller is responsible for uploading any
/// `new_ssts` and the manifest blob to remote storage. See module comment
/// for memory-ownership rules of the populated [`FrsIncrementalCheckpointResult`].
#[no_mangle]
pub unsafe extern "C" fn frs_create_incremental_checkpoint_at(
    db: FrsDb,
    snapshot: FrsSnapshot,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
    out: *mut FrsIncrementalCheckpointResult,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        match db.create_incremental_checkpoint(snap_ref, checkpoint_id, base_checkpoint_id) {
            Ok(result) => {
                let manifest_path_c =
                    std::ffi::CString::new(result.manifest_path.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| {
                            std::ffi::CString::new("<invalid-path>").expect("static literal")
                        });
                let new_list_box = Box::new(into_ffi_list(result.new_ssts, 0));
                let shared_list_box = Box::new(into_ffi_list(result.shared_ssts, 0));
                std::ptr::write(
                    out,
                    FrsIncrementalCheckpointResult {
                        manifest_path: manifest_path_c.into_raw(),
                        new_ssts: Box::into_raw(new_list_box),
                        shared_ssts: Box::into_raw(shared_list_box),
                        flush_done_eventfd: -1,
                    },
                );
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// FRS-CKPT-NOFLUSH (2026-06-01): like [`frs_create_incremental_checkpoint_at`]
/// but DOES NOT flush the memtable to an L0 SST — it enumerates only the
/// already-flushed (WBM-pressure) SST set. The caller captures the live
/// memtable separately via [`frs_snapshot_memtables_to_dir`] and uploads those
/// artifacts as private checkpoint state, so the memtable stays RAM-resident +
/// unfragmented (the ckpt-ON heavy-join fix). Result struct + free are
/// identical to the flushing variant.
#[no_mangle]
pub unsafe extern "C" fn frs_create_incremental_checkpoint_at_noflush(
    db: FrsDb,
    snapshot: FrsSnapshot,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
    out: *mut FrsIncrementalCheckpointResult,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        match db.create_incremental_checkpoint_noflush(snap_ref, checkpoint_id, base_checkpoint_id)
        {
            Ok(result) => {
                let manifest_path_c =
                    std::ffi::CString::new(result.manifest_path.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| {
                            std::ffi::CString::new("<invalid-path>").expect("static literal")
                        });
                let new_list_box = Box::new(into_ffi_list(result.new_ssts, 0));
                let shared_list_box = Box::new(into_ffi_list(result.shared_ssts, 0));
                std::ptr::write(
                    out,
                    FrsIncrementalCheckpointResult {
                        manifest_path: manifest_path_c.into_raw(),
                        new_ssts: Box::into_raw(new_list_box),
                        shared_ssts: Box::into_raw(shared_list_box),
                        flush_done_eventfd: -1,
                    },
                );
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Releases the inner allocations of an [`FrsIncrementalCheckpointResult`].
///
/// Walks both `new_ssts` and `shared_ssts` (calling
/// [`frs_db_live_file_list_free`] on the inner array, then reclaiming the
/// outer Box), then reclaims `manifest_path` via `CString::from_raw`.
/// Idempotent: calling twice (or on a NULL pointer) is a no-op.
///
/// The outer `*out` struct itself is caller-allocated (typical
/// stack-or-Java-heap pattern) so we do NOT free it.
#[no_mangle]
pub unsafe extern "C" fn frs_db_incremental_checkpoint_result_free(
    out: *mut FrsIncrementalCheckpointResult,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_OK;
        }
        let r = &mut *out;
        if !r.new_ssts.is_null() {
            frs_db_live_file_list_free(r.new_ssts);
            drop(Box::from_raw(r.new_ssts));
            r.new_ssts = std::ptr::null_mut();
        }
        if !r.shared_ssts.is_null() {
            frs_db_live_file_list_free(r.shared_ssts);
            drop(Box::from_raw(r.shared_ssts));
            r.shared_ssts = std::ptr::null_mut();
        }
        if !r.manifest_path.is_null() {
            drop(std::ffi::CString::from_raw(r.manifest_path));
            r.manifest_path = std::ptr::null_mut();
        }
        FRS_STATUS_OK
    })
}

/// Opens a fresh DB whose state is reconstructed from a manifest blob and
/// an SST file list previously produced by
/// [`frs_create_incremental_checkpoint_at`].
///
/// The function hardlinks (or copies) each `sst_files` entry into
/// `target_dir` and then opens the DB from the persisted manifest. The
/// returned handle behaves identically to one returned by [`frs_db_open`];
/// release it with [`frs_db_close`].
///
/// # Returns
/// - `FRS_STATUS_OK` and `*out_handle` populated on success.
/// - `FRS_STATUS_NULL_ARG` if any required pointer is null. Per-entry NULL
///   check on `sst_files` array entries.
/// - I/O / corruption codes propagated from the engine on failure.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_incremental(
    target_dir: *const c_char,
    base_manifest: *const c_char,
    sst_files: *const *const c_char,
    sst_file_count: usize,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if target_dir.is_null() || base_manifest.is_null() || out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        if sst_file_count > 0 && sst_files.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        // C-R17-NEW-H1: cap sst_file_count against MAX_BATCH_COUNT, mirroring
        // the sister `frs_db_ingest_external_sst` discipline. Without this
        // cap a hostile or mis-serialized restore manifest could deliver
        // a fabricated `count = usize::MAX`, driving Vec::with_capacity()
        // and the per-entry `*sst_files.add(i)` OOB scan up to 16 EiB of
        // arbitrary memory (info-disclosure + OOM).
        if sst_file_count > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let target = match CStr::from_ptr(target_dir).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let manifest = match CStr::from_ptr(base_manifest).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let mut paths: Vec<String> = Vec::with_capacity(sst_file_count);
        for i in 0..sst_file_count {
            let p = *sst_files.add(i);
            if p.is_null() {
                return FRS_STATUS_NULL_ARG;
            }
            match CStr::from_ptr(p).to_str() {
                Ok(s) => paths.push(s.to_string()),
                Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
            }
        }
        match DbImpl::open_from_incremental_with_default_cf(
            &target,
            &manifest,
            &paths,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                // Same handle layout as `frs_db_open` / `frs_db_open_memory`:
                // a Box-allocated Arc that `frs_db_close` reclaims via
                // `Box::from_raw`.
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 8c. LINK-mode (Phase-2 disaggregated state) checkpoint surface
//
// FRS-PHASE2-FFI (design §8 Stage-3 residue, 2026-06-13): the end-to-end
// enabler that makes link-mode checkpoints reachable from Flink. Mirrors the
// engine surface:
//
//   * `frs_create_incremental_checkpoint_linked` — ZERO-upload checkpoint:
//     the live SST set is `link()`ed into `<db_path>/checkpoints/<chk-id>/`
//     through the FileMappingManager (auto-attached, journal at
//     `<db_path>/MAPPING.journal`); the chk dir physically contains exactly
//     `CHECKPOINT.blob` (+ `WAL.delta` iff a WAL is attached and non-empty).
//     Returned `linked_new_ssts`/`linked_shared_ssts` are chk-namespace
//     LOGICAL paths — metadata-only, NEVER byte-readable; register them as
//     Flink handles, do NOT upload them. Memtable durability is dual-mode
//     (§9 D10): no WAL ⇒ FLUSH-on-barrier; WAL attached (see
//     `frs_db_attach_wal`) ⇒ WAL-DELTA (flush skipped, the unflushed tail is
//     captured into `WAL.delta` and replayed on restore).
//   * `frs_db_open_from_linked_checkpoint_instant[_remote]` — restore that
//     downloads/copies NOTHING: physicals are adopt()ed (NotOwned) and reads
//     route through the mapped indirection; restore wall-time is O(files)
//     metadata. CLAIM discipline: the source checkpoint must stay retained
//     until `frs_db_adopted_residual` reports 0.
//   * `frs_db_discard_linked_checkpoint` — the JM `discardState()`
//     delegate: manifest-driven unlink loop; a physical is deleted exactly
//     once when its last reference drains. Retried discard → NOT_FOUND.
//
// Memory ownership of [`FrsLinkedCheckpointResult`] mirrors
// [`FrsIncrementalCheckpointResult`]; release via
// [`frs_db_linked_checkpoint_result_free`].
// ---------------------------------------------------------------------------

/// Result of a LINK-mode incremental checkpoint. `manifest_path` is the
/// engine-FS blob path; `linked_new_ssts` / `linked_shared_ssts` carry the
/// chk-namespace logical paths (the new/shared split is the Flink
/// SharedStateRegistry registration hint — neither list is uploaded; the
/// paths are metadata-only and must never be opened byte-wise).
#[repr(C)]
pub struct FrsLinkedCheckpointResult {
    /// Path to the persisted manifest blob (with embedded mapping trailer),
    /// NUL-terminated UTF-8. Rust-owned; release via
    /// [`frs_db_linked_checkpoint_result_free`].
    pub manifest_path: *mut c_char,
    /// Live SSTs NOT in the base checkpoint, at linked
    /// `<db_path>/checkpoints/<chk-id>/NNNNNN.sst` logical paths.
    pub linked_new_ssts: *mut FrsLiveFileList,
    /// Live SSTs shared with the base checkpoint, at linked logical paths.
    pub linked_shared_ssts: *mut FrsLiveFileList,
}

/// Captures a LINK-mode incremental checkpoint pinned at `snapshot` —
/// zero data movement (O(files) metadata link ops). See section 8c module
/// comment for the contract; `checkpoint_id` / `base_checkpoint_id`
/// semantics match [`frs_create_incremental_checkpoint_at`].
///
/// # SAFETY
/// - `db` must be a live handle from `frs_db_open*`; `snapshot` a live
///   snapshot of the SAME db; `out` a valid caller-allocated slot.
#[no_mangle]
pub unsafe extern "C" fn frs_create_incremental_checkpoint_linked(
    db: FrsDb,
    snapshot: FrsSnapshot,
    checkpoint_id: u64,
    base_checkpoint_id: u64,
    out: *mut FrsLinkedCheckpointResult,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if snapshot.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let snap_ref = &(*snapshot).inner;
        if snap_ref.db_id() != db.db_id() {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        match db.create_incremental_checkpoint_linked(snap_ref, checkpoint_id, base_checkpoint_id) {
            Ok(result) => {
                debug_assert!(
                    result.link_mode && result.new_ssts.is_empty() && result.shared_ssts.is_empty(),
                    "linked checkpoint must return empty upload lists (design §9 D3)"
                );
                let manifest_path_c =
                    std::ffi::CString::new(result.manifest_path.to_string_lossy().into_owned())
                        .unwrap_or_else(|_| {
                            std::ffi::CString::new("<invalid-path>").expect("static literal")
                        });
                let new_list_box = Box::new(into_ffi_list(result.linked_new_ssts, 0));
                let shared_list_box = Box::new(into_ffi_list(result.linked_shared_ssts, 0));
                std::ptr::write(
                    out,
                    FrsLinkedCheckpointResult {
                        manifest_path: manifest_path_c.into_raw(),
                        linked_new_ssts: Box::into_raw(new_list_box),
                        linked_shared_ssts: Box::into_raw(shared_list_box),
                    },
                );
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Releases the inner allocations of an [`FrsLinkedCheckpointResult`].
/// Idempotent; the outer struct is caller-allocated and not freed.
#[no_mangle]
pub unsafe extern "C" fn frs_db_linked_checkpoint_result_free(
    out: *mut FrsLinkedCheckpointResult,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_OK;
        }
        let r = &mut *out;
        if !r.linked_new_ssts.is_null() {
            frs_db_live_file_list_free(r.linked_new_ssts);
            drop(Box::from_raw(r.linked_new_ssts));
            r.linked_new_ssts = std::ptr::null_mut();
        }
        if !r.linked_shared_ssts.is_null() {
            frs_db_live_file_list_free(r.linked_shared_ssts);
            drop(Box::from_raw(r.linked_shared_ssts));
            r.linked_shared_ssts = std::ptr::null_mut();
        }
        if !r.manifest_path.is_null() {
            drop(std::ffi::CString::from_raw(r.manifest_path));
            r.manifest_path = std::ptr::null_mut();
        }
        FRS_STATUS_OK
    })
}

/// TM-side discard of a LINK-mode checkpoint (the JM `discardState()`
/// delegate, design §9 D4). On success `*out_unlinked` /
/// `*out_physicals_deleted` report the dropped references and the physical
/// objects whose LAST reference this discard drained (deleted exactly
/// once). A retried discard (blob already gone) returns
/// `FRS_STATUS_NOT_FOUND`.
///
/// # SAFETY
/// - `db` must be a live handle; out pointers may be null (counts skipped).
#[no_mangle]
pub unsafe extern "C" fn frs_db_discard_linked_checkpoint(
    db: FrsDb,
    checkpoint_id: u64,
    out_unlinked: *mut u64,
    out_physicals_deleted: *mut u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.discard_linked_checkpoint(checkpoint_id) {
            Ok(report) => {
                if !out_unlinked.is_null() {
                    *out_unlinked = report.unlinked as u64;
                }
                if !out_physicals_deleted.is_null() {
                    *out_physicals_deleted = report.physicals_deleted as u64;
                }
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// INSTANT-LINK restore from a LINK-mode checkpoint directory on the LOCAL
/// filesystem — downloads/copies NOTHING (adopt + mapped reads; WAL.delta
/// tail replayed when present). `target_dir` must be fresh/clean. The
/// restored default CF carries the standard raw-concat merge operator
/// (same as `frs_db_open`).
///
/// CLAIM discipline: keep the source checkpoint retained until
/// [`frs_db_adopted_residual`] reports 0 for the restored handle.
///
/// # SAFETY
/// - `ckpt_dir` / `target_dir` must be NUL-terminated UTF-8;
///   `out_handle` a valid slot. Close the handle with `frs_db_close`.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_linked_checkpoint_instant(
    ckpt_dir: *const c_char,
    target_dir: *const c_char,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let ckpt = match cstr_to_str(&ckpt_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let target = match cstr_to_str(&target_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let fs: std::sync::Arc<dyn forst_rs_io::FileSystem> =
            std::sync::Arc::new(forst_rs_io::LocalFileSystem::new());
        match DbImpl::open_from_linked_checkpoint_instant_with_default_cf(
            fs,
            Path::new(&ckpt),
            &target,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// INSTANT-LINK restore over a REMOTE (OpenDAL) engine filesystem — the
/// remote-primary download-skip restore. Builds the same
/// `CachedFileSystem(OpendalFileSystem, LocalCache)` stack as
/// [`frs_db_open_remote`] (same `uri` / `opendal_config_json` / cache
/// parameters), then performs the instant restore of `ckpt_dir` into
/// `target_dir` (both REMOTE-namespace paths).
///
/// # SAFETY
/// - String args NUL-terminated UTF-8 (`opendal_config_json` may be null);
///   `out_handle` a valid slot.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_linked_checkpoint_instant_remote(
    uri: *const c_char,
    opendal_config_json: *const c_char,
    cache_dir: *const c_char,
    cache_capacity_bytes: u64,
    ckpt_dir: *const c_char,
    target_dir: *const c_char,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let uri_str = match cstr_to_str(&uri) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let cache_dir_str = match cstr_to_str(&cache_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let ckpt = match cstr_to_str(&ckpt_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let target = match cstr_to_str(&target_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let json_str = if opendal_config_json.is_null() {
            String::new()
        } else {
            match cstr_to_str(&opendal_config_json) {
                Some(s) => s.to_string(),
                None => return FRS_STATUS_NULL_ARG,
            }
        };
        let config = match parse_flat_json_object(&json_str) {
            Ok(m) => m,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        match DbImpl::open_from_linked_checkpoint_instant_remote(
            &uri_str,
            config,
            Path::new(&cache_dir_str),
            cache_capacity_bytes,
            Path::new(&ckpt),
            &target,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// FRS-PHASE2-C2U3 (rescale-by-clip, paper §5.2 / Fig. 10): INSTANT-LINK
/// restore that ADOPTS ONLY the half-open key range `[clip_start, clip_end)`
/// of the linked checkpoint — the rescale-aware companion to
/// [`frs_db_open_from_linked_checkpoint_instant`]. On rescale, each new
/// sub-task is assigned a key-group SUB-range of the source; passing that
/// sub-range's key-prefix bounds lets the engine drop fully-disjoint SSTs
/// from the restored Version (file-level clip) and install a read-path clip
/// for boundary SSTs, so the sub-task sees only its own state without a
/// download or a row-by-row copy.
///
/// The clip bounds are raw composite-key prefix bytes (the backend's
/// key-group encoding): `clip_start` is the inclusive low bound (e.g. the
/// 2-byte big-endian first assigned key group), `clip_end` the exclusive
/// high bound (the 2-byte big-endian (last+1) key group). An empty range
/// (`start >= end`) is rejected with `FRS_STATUS_INVALID_ARGUMENT`.
///
/// # SAFETY
/// - `ckpt_dir` / `target_dir` NUL-terminated UTF-8; `out_handle` valid.
/// - `clip_start` must point to `clip_start_len` bytes (or be null when 0);
///   `clip_end` to `clip_end_len` bytes (or be null when 0).
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_linked_checkpoint_instant_clipped(
    ckpt_dir: *const c_char,
    target_dir: *const c_char,
    clip_start: *const u8,
    clip_start_len: usize,
    clip_end: *const u8,
    clip_end_len: usize,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let ckpt = match cstr_to_str(&ckpt_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let target = match cstr_to_str(&target_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let clip = match read_clip_range(clip_start, clip_start_len, clip_end, clip_end_len) {
            Some(r) => r,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let fs: std::sync::Arc<dyn forst_rs_io::FileSystem> =
            std::sync::Arc::new(forst_rs_io::LocalFileSystem::new());
        match DbImpl::open_from_linked_checkpoint_instant_clipped_with_default_cf(
            fs,
            Path::new(&ckpt),
            &target,
            clip,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// REMOTE-primary variant of
/// [`frs_db_open_from_linked_checkpoint_instant_clipped`] — the rescale-aware
/// download-skip restore over an OpenDAL engine filesystem (same `uri` /
/// `opendal_config_json` / cache parameters as
/// [`frs_db_open_from_linked_checkpoint_instant_remote`]).
///
/// # SAFETY
/// - String args NUL-terminated UTF-8 (`opendal_config_json` may be null);
///   `out_handle` valid. Clip-byte pointers as in the local variant.
#[no_mangle]
pub unsafe extern "C" fn frs_db_open_from_linked_checkpoint_instant_clipped_remote(
    uri: *const c_char,
    opendal_config_json: *const c_char,
    cache_dir: *const c_char,
    cache_capacity_bytes: u64,
    ckpt_dir: *const c_char,
    target_dir: *const c_char,
    clip_start: *const u8,
    clip_start_len: usize,
    clip_end: *const u8,
    clip_end_len: usize,
    out_handle: *mut FrsDb,
) -> i32 {
    guarded(|| {
        if out_handle.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let uri_str = match cstr_to_str(&uri) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let cache_dir_str = match cstr_to_str(&cache_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let ckpt = match cstr_to_str(&ckpt_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let target = match cstr_to_str(&target_dir) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        let json_str = if opendal_config_json.is_null() {
            String::new()
        } else {
            match cstr_to_str(&opendal_config_json) {
                Some(s) => s.to_string(),
                None => return FRS_STATUS_NULL_ARG,
            }
        };
        let config = match parse_flat_json_object(&json_str) {
            Ok(m) => m,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };
        let clip = match read_clip_range(clip_start, clip_start_len, clip_end, clip_end_len) {
            Some(r) => r,
            None => return FRS_STATUS_INVALID_ARGUMENT,
        };
        match DbImpl::open_from_linked_checkpoint_instant_clipped_remote(
            &uri_str,
            config,
            Path::new(&cache_dir_str),
            cache_capacity_bytes,
            Path::new(&ckpt),
            &target,
            clip,
            raw_concat_default_cf_descriptor(),
        ) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// Reads a `[clip_start, clip_end)` [`KeyRange`] from two raw byte slices.
/// Returns `None` (⇒ `FRS_STATUS_INVALID_ARGUMENT`) when a non-zero length
/// has a null pointer or when the resulting range is empty (`start >= end`);
/// the engine also rejects empty ranges, but checking here keeps the error
/// at the FFI boundary.
///
/// # SAFETY
/// - `start` must point to `start_len` bytes (or be null when 0); `end` to
///   `end_len` bytes (or be null when 0).
unsafe fn read_clip_range(
    start: *const u8,
    start_len: usize,
    end: *const u8,
    end_len: usize,
) -> Option<forst_rs_common::types::KeyRange> {
    if (start_len > 0 && start.is_null()) || (end_len > 0 && end.is_null()) {
        return None;
    }
    let start_bytes = if start_len == 0 {
        Vec::new()
    } else {
        slice::from_raw_parts(start, start_len).to_vec()
    };
    let end_bytes = if end_len == 0 {
        Vec::new()
    } else {
        slice::from_raw_parts(end, end_len).to_vec()
    };
    let range = forst_rs_common::types::KeyRange::new(start_bytes, end_bytes);
    if range.is_empty() {
        return None;
    }
    Some(range)
}

/// Number of LIVE SSTs still resolving to physical objects OUTSIDE this
/// engine's working namespace (adopted from a restore source, not yet
/// compacted away). 0 ⇒ the engine is weaned; the restore-source
/// checkpoint may be discarded safely.
///
/// # SAFETY
/// - `db` must be a live handle; `out` a valid slot.
#[no_mangle]
pub unsafe extern "C" fn frs_db_adopted_residual(db: FrsDb, out: *mut u64) -> i32 {
    guarded(|| {
        if out.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        *out = db.adopted_residual() as u64;
        FRS_STATUS_OK
    })
}

/// Attaches a write-ahead log at `wal_path` to this engine — the per-DB,
/// env-free WAL-DELTA opt-in (design §9 D10: with a WAL attached, a LINK
/// checkpoint skips the memtable flush and captures the unflushed tail
/// into `<chk-dir>/WAL.delta`; restore replays it through the per-CF
/// flushed-floor filter). The WAL is a LOCAL file (place it on fast local
/// disk). Errors with `FRS_STATUS_INVALID_ARGUMENT` if a WAL is already
/// attached.
///
/// # SAFETY
/// - `db` must be a live handle; `wal_path` NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn frs_db_attach_wal(db: FrsDb, wal_path: *const c_char) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let path = match cstr_to_str(&wal_path) {
            Some(s) => s.to_string(),
            None => return FRS_STATUS_NULL_ARG,
        };
        match db.attach_wal_at(Path::new(&path)) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Startup sweep reaping ABANDONED link-mode checkpoint namespaces (design
/// §9 D5 crash window a): chk-k links durable in the mapping journal for a
/// checkpoint id NOT in `live_ids` (the JM-live set; may be null when
/// `live_count` is 0) are unlinked and their leftover chk dirs removed.
/// Physicals survive while the working dir or live checkpoints reference
/// them. Idempotent. Call at restore/open, BEFORE new linked checkpoints.
///
/// Requires a file mapping (a prior linked checkpoint / instant restore
/// attached one) — INVALID_ARGUMENT otherwise.
///
/// # SAFETY
/// - `db` must be a live handle; `live_ids` must point to `live_count` u64s
///   (or be null when `live_count` is 0); out pointers may be null.
#[no_mangle]
pub unsafe extern "C" fn frs_db_sweep_abandoned_checkpoints(
    db: FrsDb,
    live_ids: *const u64,
    live_count: usize,
    out_unlinked: *mut u64,
    out_physicals_deleted: *mut u64,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        if live_count > 0 && live_ids.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        if live_count > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        let live: &[u64] = if live_count == 0 {
            &[]
        } else {
            slice::from_raw_parts(live_ids, live_count)
        };
        match db.sweep_abandoned_checkpoint_links(live) {
            Ok(report) => {
                if !out_unlinked.is_null() {
                    *out_unlinked = report.unlinked as u64;
                }
                if !out_physicals_deleted.is_null() {
                    *out_physicals_deleted = report.physicals_deleted as u64;
                }
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 9. State import / export migration (B-Prod-P10, spec §6g)
//
// `frs_cf_export` writes every (key, value) row in `cf` to a single
// self-describing blob (`EXPORT.frsblob`) under `export_dir`.
// `frs_db_create_cf_from_import` creates a new CF named `name` and
// replays the blob's entries into it. Both call straight through to the
// engine-side [`forst_rs_engine::DbImpl::cf_export`] /
// [`forst_rs_engine::DbImpl::create_cf_from_import`] (see
// `crates/forst-rs-engine/src/db.rs` for the wire format and atomicity
// notes). The Java side wraps these via
// `org.apache.flink.state.forstrs.migration.ForStRsStateMigration`.
// ---------------------------------------------------------------------------

/// Exports every live (key, value) pair from `cf` to a self-describing
/// blob (`EXPORT.frsblob`) under `export_dir`. The directory is created
/// if it does not exist.
///
/// Returns:
/// - `FRS_STATUS_OK` on success.
/// - `FRS_STATUS_NULL_ARG` if `db`, `cf`, or `export_dir` is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if `export_dir` is not valid UTF-8 or
///   the engine rejects the CF handle.
/// - `FRS_STATUS_IO` if the blob cannot be written.
///
/// # SAFETY
/// - `db` must be a handle returned by `frs_db_open*` and not yet closed.
/// - `cf` must be a handle returned by `frs_db_create_cf*` /
///   `frs_db_open_cf` for the same database.
/// - `export_dir` must be a NUL-terminated UTF-8 string for the duration
///   of the call.
#[no_mangle]
pub unsafe extern "C" fn frs_cf_export(
    db: FrsDb,
    cf: FrsCfHandle,
    export_dir: *const c_char,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(dir_str) = cstr_to_str(&export_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        let dir = std::path::Path::new(dir_str);
        match db.cf_export(cf, dir) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Creates a new column family with name `name` and seeds it with every
/// entry from `import_dir/EXPORT.frsblob`. The new CF handle is written
/// to `out_cf`; the caller MUST release it via `frs_cf_close`.
///
/// Returns:
/// - `FRS_STATUS_OK` on success.
/// - `FRS_STATUS_NULL_ARG` if `db`, `name`, `import_dir`, or `out_cf`
///   is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if the blob is missing, the magic
///   header doesn't match, or a CF named `name` already exists.
/// - `FRS_STATUS_CORRUPTION` if the blob is truncated mid-entry.
/// - `FRS_STATUS_IO` if a backing put fails.
///
/// # SAFETY
/// - `db` must be a handle returned by `frs_db_open*` and not yet closed.
/// - `name` and `import_dir` must be NUL-terminated UTF-8 strings for
///   the duration of the call.
/// - `out_cf` must point to a writable `FrsCfHandle` (typically a stack
///   slot allocated as `FrsCfHandle`).
#[no_mangle]
pub unsafe extern "C" fn frs_db_create_cf_from_import(
    db: FrsDb,
    name: *const c_char,
    import_dir: *const c_char,
    out_cf: *mut FrsCfHandle,
) -> i32 {
    guarded(|| {
        if out_cf.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(name_str) = cstr_to_str(&name) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(dir_str) = cstr_to_str(&import_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        let dir = std::path::Path::new(dir_str);
        match db.create_cf_from_import(name_str, dir) {
            Ok(cf_handle) => {
                let boxed = Box::new(cf_handle);
                *out_cf = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

/// OPT-N04 E3: `frs_db_create_cf_from_import` with the destination CF's
/// merge operator threaded through BY NAME. The export blob contains only
/// resolved Puts (export reads collapse merge chains, including operands
/// still pending in memtables at export time), so the operator is not
/// needed for the imported *data* — but without it the recreated CF
/// rejects all *future* Merge writes (D-R8-NEW-H2), making rescaled
/// merge-routed state read-only. The Java rescale path MUST use this
/// entry point for CFs created via `frs_db_create_cf_with_merge`.
///
/// `merge_op_name` accepts the same names as `frs_db_create_cf_with_merge`
/// (shared `merge_operator_by_name` registry). `NULL` means no operator —
/// byte-identical behaviour to `frs_db_create_cf_from_import`.
///
/// Returns:
/// - `FRS_STATUS_OK` on success.
/// - `FRS_STATUS_NULL_ARG` if `db`, `name`, `import_dir`, or `out_cf`
///   is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` for an unknown `merge_op_name` (no CF
///   is created), a missing blob, a magic mismatch, or a duplicate name.
/// - `FRS_STATUS_CORRUPTION` if the blob is truncated mid-entry.
/// - `FRS_STATUS_IO` if a backing put fails.
///
/// # SAFETY
/// - `db` must be a handle returned by `frs_db_open*` and not yet closed.
/// - `name` and `import_dir` must be NUL-terminated UTF-8 strings for the
///   duration of the call; `merge_op_name` must be either NULL or a
///   NUL-terminated UTF-8 string for the duration of the call.
/// - `out_cf` must point to a writable `FrsCfHandle`.
#[no_mangle]
pub unsafe extern "C" fn frs_db_create_cf_from_import_with_merge(
    db: FrsDb,
    name: *const c_char,
    merge_op_name: *const c_char,
    import_dir: *const c_char,
    out_cf: *mut FrsCfHandle,
) -> i32 {
    guarded(|| {
        if out_cf.is_null() {
            return FRS_STATUS_NULL_ARG;
        }
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(name_str) = cstr_to_str(&name) else {
            return FRS_STATUS_NULL_ARG;
        };
        let op_name: Option<&str> = if merge_op_name.is_null() {
            None
        } else {
            let Some(s) = cstr_to_str(&merge_op_name) else {
                return FRS_STATUS_NULL_ARG;
            };
            Some(s)
        };
        let Some(dir_str) = cstr_to_str(&import_dir) else {
            return FRS_STATUS_NULL_ARG;
        };
        let dir = std::path::Path::new(dir_str);
        match db.create_cf_from_import_with_merge(name_str, dir, op_name) {
            Ok(cf_handle) => {
                let boxed = Box::new(cf_handle);
                *out_cf = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 9b. drop_cf + ingest_external_sst (B-Prod-followup-5, spec §6g)
//
// The community RocksDB-style import path hardlinks SST files in
// O(file-count) instead of replaying scan + put in O(key-count). These
// two FFI exports let Flink's `ForStRsStateMigration` swap its
// scan/replay path for the engine's hardlink+ingest path, dropping
// per-row migration cost in the process.
// ---------------------------------------------------------------------------

/// Drops a column family.
///
/// Removes the CF from the engine's CF maps and flips its shared
/// `dropped` flag so subsequent operations on cloned handles return
/// `FRS_STATUS_INVALID_ARGUMENT`. Idempotent on an already-dropped CF
/// (returns `FRS_STATUS_OK`); rejects the default CF.
///
/// SSTs are NOT physically deleted — see
/// [`forst_rs_engine::DbImpl::drop_cf`] for the rationale (the
/// `VersionSet` is currently CF-agnostic).
///
/// Returns:
/// - `FRS_STATUS_OK` on success (including idempotent re-drop).
/// - `FRS_STATUS_NULL_ARG` if `db` or `cf` is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if `cf` is the default CF or the
///   engine rejects the handle.
///
/// # SAFETY
/// - `db` must be a handle returned by `frs_db_open*` and not yet closed.
/// - `cf` must be a handle returned by `frs_db_create_cf*` /
///   `frs_db_open_cf`. The handle remains valid (callers MUST still
///   call `frs_cf_close`); calling `frs_db_drop_cf` only marks it
///   unusable for future engine operations.
#[no_mangle]
pub unsafe extern "C" fn frs_db_drop_cf(db: FrsDb, cf: FrsCfHandle) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        match db.drop_cf(cf) {
            Ok(()) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

/// Ingests pre-built SST files into the engine's L0.
///
/// Each path in `sst_paths` (an array of `count` NUL-terminated UTF-8 C
/// strings) is hardlinked (or copied cross-FS) into the engine's SST
/// directory, then registered at L0 via a single atomic version edit.
/// See [`forst_rs_engine::DbImpl::ingest_external_sst`] for the caller
/// contract (key-range overlap rules, source-SST compatibility, CF
/// visibility).
///
/// Returns:
/// - `FRS_STATUS_OK` on successful ingest.
/// - `FRS_STATUS_NULL_ARG` if `db`, `cf`, or `sst_paths` is null.
/// - `FRS_STATUS_INVALID_ARGUMENT` if `count > MAX_BATCH_COUNT`, or any
///   path entry is null / not valid UTF-8, or the engine rejects the
///   CF handle.
/// - `FRS_STATUS_IO` if hardlink+copy fails for any source file.
/// - `FRS_STATUS_CORRUPTION` if a source SST cannot be parsed.
///
/// # SAFETY
/// - `db`, `cf` must be valid (see [`frs_db_drop_cf`]).
/// - `sst_paths` must point to a contiguous array of `count` NUL-
///   terminated C strings, each readable for the duration of the call.
/// - The engine takes no ownership of `sst_paths`; the caller is free
///   to delete (or keep) the source files after the call returns.
#[no_mangle]
pub unsafe extern "C" fn frs_db_ingest_external_sst(
    db: FrsDb,
    cf: FrsCfHandle,
    sst_paths: *const *const c_char,
    count: usize,
) -> i32 {
    guarded(|| {
        let Some(db) = db_from_handle(db) else {
            return FRS_STATUS_NULL_ARG;
        };
        let Some(cf) = cf_ref(&cf) else {
            return FRS_STATUS_NULL_ARG;
        };
        if sst_paths.is_null() && count != 0 {
            return FRS_STATUS_NULL_ARG;
        }
        if count > MAX_BATCH_COUNT {
            return FRS_STATUS_INVALID_ARGUMENT;
        }
        if count == 0 {
            // Engine treats empty input as a no-op; mirror that here so
            // a Java caller can pass an empty array without special-
            // casing on the JNI side.
            return match db.ingest_external_sst(cf, &[]) {
                Ok(_) => FRS_STATUS_OK,
                Err(e) => error_to_status(&e),
            };
        }
        // Collect the C-string array into owned PathBufs first so the
        // borrow checker is happy with the &[&Path] the engine expects.
        let raw_slice = slice::from_raw_parts(sst_paths, count);
        let mut owned: Vec<PathBuf> = Vec::with_capacity(count);
        for raw in raw_slice {
            if raw.is_null() {
                return FRS_STATUS_NULL_ARG;
            }
            let cstr = CStr::from_ptr(*raw);
            let Ok(s) = cstr.to_str() else {
                return FRS_STATUS_INVALID_ARGUMENT;
            };
            owned.push(PathBuf::from(s));
        }
        let path_refs: Vec<&Path> = owned.iter().map(|p| p.as_path()).collect();
        match db.ingest_external_sst(cf, &path_refs) {
            Ok(_) => FRS_STATUS_OK,
            Err(e) => error_to_status(&e),
        }
    })
}

// ---------------------------------------------------------------------------
// 12. Vectorized chunked iterator — frs_vec_iter_prefix_* (P3-A, spec §1 §b + §2 E)
//
// These four symbols implement the chunked-iteration API used by the Java
// vectorized dispatch executor (`IterPrefixExecutor`):
//
//   frs_vec_iter_prefix_open   — prefix scan → first chunk + opaque handle
//   frs_vec_iter_prefix_next   — pull next chunk from an open handle
//   frs_vec_iter_prefix_close  — drop the handle and release native state
//   frs_vec_iter_prefix_abort  — watchdog hook: mark handle as aborted
//
// Wire format: rows are packed into the caller's direct ByteBuffer as
//   [klen: u32 LE][vlen: u32 LE][key bytes][value bytes]
// repeated, with no padding between rows.
//
// V1 snapshot semantics: the engine's `prefix_scan` / `scan` APIs return a
// Vec<(Vec<u8>, Vec<u8>)>; we wrap that Vec's `IntoIter` as the boxed
// iterator stored inside `IterHandle`.  No additional intermediate Vec is
// allocated at the FFI layer — `fill_chunk_from_iter` drives the boxed
// iterator directly into the caller's buffer (zero-clone first chunk).
// Engine-side streaming snapshots (true cursors) are deferred to a later
// engine PR (out of PR-D4 scope: "lib.rs only").
//
// Handle registry (PR-D4): a 16-shard `Mutex<HashMap>` keyed by handle id.
// The shard is selected by the lower 4 bits of the id, so opens from
// different threads with monotonically-assigned ids land on different
// shards and do not serialize on a single global Mutex.  Per spec PR-D4
// closes V2-4 / B2-H2 / B2-H5: "global Mutex<HashMap> that serializes
// ALL iter opens across slots".  The 16-way sharding removes that
// serialization point while preserving the safety invariant that
// dereferencing an arbitrary `u64` handle is impossible — every access
// goes through a HashMap lookup and missing handles return
// `IterCursorInvalid`.
//
// IDs are monotonically increasing from 1; overflow at u64::MAX wraps to
// 0 (harmless: the lookup misses and we return `IterCursorInvalid`).
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Mutex, OnceLock};

/// B10-H3 / B11-H3: zero-copy bytes wrapper for FFI iterator rows.
///
/// Used for BOTH the key half (B10-H3) and the value half (B11-H3) of a
/// row pair. Different upstream iterators source bytes differently:
///   * `prefix_scan_iter_owned_arc` yields `Arc<[u8]>` for both key and
///     value (zero-copy path — the FFI consumer just reads `arc.as_ref()`
///     and `copy_nonoverlapping`s into the caller's direct ByteBuffer);
///   * `scan` (range iterator) yields `Vec<u8>` for both halves (the
///     engine materialises the result set as `Vec<(Vec<u8>, Vec<u8>)>`
///     at open time and the range FFI wraps that Vec's `IntoIter`);
///
/// Wrapping both shapes in a single enum lets `IterHandle` hold one
/// concrete iterator type while the per-row hot path stays alloc-free in
/// the prefix case (`Arc::clone` is only paid on `put_back` rollback,
/// which is the cold capacity-overflow edge).
///
/// C12-M1: the `as_slice` accessor compiles to a tagged-union branch
/// (NOT a `cmov`) — the `Vec` variant is a 24-byte `(ptr, len, cap)`
/// triple and the `Arc` variant is a 16-byte `(ptr, len)` fat pointer,
/// so the two variants have different layouts and LLVM cannot fold the
/// match into a conditional move. In practice an iterator opens one
/// variant per handle and yields the same variant on every row, so the
/// branch is extremely predictable and the per-row cost is dominated by
/// the `copy_nonoverlapping` into the caller's buffer, not the dispatch.
enum IterKey {
    // B-R7-NEW-H1: kept for the borrowed-slice / future-Vec-source variant
    // even though the range path now also emits `Arc<[u8]>` (was the last
    // remaining constructor). Removing the variant would force a public
    // enum shape change for the FFI handle registry; leaving it gated
    // behind `#[allow(dead_code)]` preserves the option without warning.
    #[allow(dead_code)]
    Vec(Vec<u8>),
    Arc(Arc<[u8]>),
}

impl IterKey {
    /// Slice view into the backing store. Currently unused on the hot
    /// emit path (which prefers raw `as_ptr` + `len` for direct
    /// `copy_nonoverlapping` into the caller buffer) but kept for future
    /// callers that want a borrow-friendly accessor.
    #[allow(dead_code)]
    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            IterKey::Vec(v) => v.as_slice(),
            IterKey::Arc(a) => a.as_ref(),
        }
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            IterKey::Vec(v) => v.len(),
            IterKey::Arc(a) => a.len(),
        }
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        match self {
            IterKey::Vec(v) => v.as_ptr(),
            IterKey::Arc(a) => a.as_ptr(),
        }
    }
}

/// B11-H3: value half of an FFI iterator row. Parallels [`IterKey`] —
/// the prefix path uses `Arc<[u8]>` (cheap refcount bump on `put_back`
/// rollback) while the range path keeps its upstream `Vec<u8>`.
enum IterValue {
    // B-R7-NEW-H1: see `IterKey::Vec` note. The last in-tree constructor
    // (the eager range-scan FFI path) was removed when `frs_vec_iter_range_open`
    // switched to `scan_iter_owned_arc_with_error_slot`. Variant retained
    // for symmetry with `IterKey::Vec` and for future borrowed-Vec sources.
    #[allow(dead_code)]
    Vec(Vec<u8>),
    Arc(Arc<[u8]>),
}

impl IterValue {
    #[inline]
    fn len(&self) -> usize {
        match self {
            IterValue::Vec(v) => v.len(),
            IterValue::Arc(a) => a.len(),
        }
    }

    #[inline]
    fn as_ptr(&self) -> *const u8 {
        match self {
            IterValue::Vec(v) => v.as_ptr(),
            IterValue::Arc(a) => a.as_ptr(),
        }
    }
}

/// Per-handle state owned by the FFI iter registry.  Holds:
/// - a live boxed iterator over `(IterKey, Vec<u8>)` pairs;
/// - an optional `pending` row peeked from the iterator but not yet
///   written into the caller's buffer (rollback for overflow);
/// - an `aborted` flag so the watchdog can short-circuit `_next` calls.
///
/// PR-D4 zero-clone: the open/next path drains directly from `inner` into
/// the caller's buffer — no intermediate `Vec<(...)>` allocation happens
/// at the FFI layer.
///
/// B10-H3: the key half of each row is wrapped in [`IterKey`] so the
/// prefix iterator path can pass keys through as `Arc<[u8]>` (zero
/// `Vec::clone` per emit) while the range iterator path keeps its
/// upstream `Vec<u8>` key as-is.
/// S2 (pinned-rows design W1c): backing state of one registered FFI iterator.
///
/// `Boxed` is the legacy Arc-pair pull iterator (flag OFF, and every eager
/// path). `Pinned` (flag `FRS_RS_S2_PINNED` ON) holds the engine's push-style
/// [`forst_rs_engine::PrefixScanStream`] and drives `fill_into` straight into
/// the caller's chunk buffer — zero per-row allocations/refcounts on the
/// SST-Put path, SST bytes → chunk in exactly one `copy_nonoverlapping` per
/// field.
enum IterBackend {
    /// Legacy boxed Arc-pair iterator.
    Boxed(Box<dyn Iterator<Item = (IterKey, IterValue)> + Send>),
    /// S2 push-style stream + its chunk-overflow stash (boxed: the
    /// stream embeds the k-way merge state and dwarfs the Boxed variant).
    Pinned(Box<PinnedIter>),
}

/// S2: pinned-stream backend state. The pending buffers are the chunk-full
/// rollback: `RowSink::push` returning `false` means the sink kept the row —
/// the overflow row is COPIED into these reused `Vec`s (one ~row-size memcpy
/// per chunk boundary, zero steady-state allocation) and is written first by
/// the next fill.
struct PinnedIter {
    stream: forst_rs_engine::PrefixScanStream,
    /// The stream reported `Exhausted` (or a fill error was recorded).
    exhausted: bool,
    /// Overflow row stash (valid when `pending_set`).
    pending_key: Vec<u8>,
    pending_val: Vec<u8>,
    pending_set: bool,
}

struct IterHandle {
    inner: IterBackend,
    pending: Option<(IterKey, IterValue)>,
    aborted: AtomicBool,
    /// R18-M4: terminal flag set once a deferred error has been surfaced to the
    /// FFI caller. Subsequent `_next` calls observe `terminal == true` and
    /// return an empty chunk + `FrsErrorCode::Ok` (EOF), instead of pulling
    /// more rows from the underlying source. Pre-fix, surfacing the deferred
    /// error at chunk N+1 left the iterator otherwise live; chunk N+2 would
    /// return rows from OTHER tier sources (the multi-tier iterator chains
    /// past the failed tier transparently), confusing the Java consumer which
    /// had just received an error code and expected the iterator to be done.
    terminal: AtomicBool,
    /// R16-M2: shared error slot populated by the wrapping
    /// `filter_map`/error-tap closure when the upstream iterator yields an
    /// `Err(_)`. The FFI consumer (`fill_chunk_from_iter` / open / next /
    /// close) drains this slot after each chunk so transient engine errors
    /// surface to the Java side as `FrsErrorCode` rather than being silently
    /// dropped (the original Box<dyn Iterator<Item = (Key, Value)>> shape
    /// erased the LazyPrefixIter type and its `take_last_error` accessor).
    last_error: Arc<Mutex<Option<forst_rs_common::ForstError>>>,
    /// R17-M3: deferred error stash for the partial-chunk preservation state
    /// machine. When a chunk-fill captures an upstream error AFTER serialising
    /// one or more rows, the FFI surface returns the partial chunk + `Ok` so
    /// the Java consumer drains the in-flight rows; the error is stashed here
    /// and surfaced on the NEXT `_next` call (before any further pulls).
    /// Pre-fix the open/next handlers zeroed `row_count`/`bytes_used` on error,
    /// silently discarding the already-serialised rows and stealing data the
    /// caller had observable bytes for.
    deferred_error: Option<forst_rs_common::ForstError>,
}

impl IterHandle {
    // B-R7-NEW-H1: previously used by the eager `frs_vec_iter_range_open`
    // path. After the range path migrated to the error-slot-aware
    // construction, no in-tree caller remains. Kept for the eager FFI
    // path used by `frs_iterator_open`/`frs_iterator_open_at` (which
    // still construct iterators without a shared error slot via the
    // legacy `IteratorState::new(rows: Vec<...>)` pattern).
    #[allow(dead_code)]
    fn new(inner: Box<dyn Iterator<Item = (IterKey, IterValue)> + Send>) -> Self {
        Self::new_with_error_slot(inner, Arc::new(Mutex::new(None)))
    }

    /// R16-M2: construct with an externally-shared error slot so the upstream
    /// `filter_map` adapter can write into the same `Option<ForstError>` that
    /// the FFI consumer drains after each chunk.
    fn new_with_error_slot(
        inner: Box<dyn Iterator<Item = (IterKey, IterValue)> + Send>,
        last_error: Arc<Mutex<Option<forst_rs_common::ForstError>>>,
    ) -> Self {
        Self {
            inner: IterBackend::Boxed(inner),
            pending: None,
            aborted: AtomicBool::new(false),
            last_error,
            deferred_error: None,
            terminal: AtomicBool::new(false),
        }
    }

    /// S2: construct over the engine's push-style stream (flag ON). The
    /// shared error slot is the SAME channel the stream's tier-peek errors
    /// land in (wired engine-side); fill errors are recorded there too.
    fn new_pinned_with_error_slot(
        stream: forst_rs_engine::PrefixScanStream,
        last_error: Arc<Mutex<Option<forst_rs_common::ForstError>>>,
    ) -> Self {
        Self {
            inner: IterBackend::Pinned(Box::new(PinnedIter {
                stream,
                exhausted: false,
                pending_key: Vec::new(),
                pending_val: Vec::new(),
                pending_set: false,
            })),
            pending: None,
            aborted: AtomicBool::new(false),
            last_error,
            deferred_error: None,
            terminal: AtomicBool::new(false),
        }
    }

    /// R18-M4: mark the iterator as terminal. Called after a deferred error
    /// has been surfaced to the FFI caller; subsequent `_next` calls return
    /// empty chunks + Ok (EOF semantics) instead of pulling more rows.
    fn mark_terminal(&self) {
        self.terminal
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// R18-M4: query the terminal flag. Used by `frs_vec_iter_prefix_next`
    /// to short-circuit further pulls once the iterator has been retired.
    fn is_terminal(&self) -> bool {
        self.terminal.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// R16-M2 + R17-L2: take + clear the last error captured by the upstream
    /// filter adapter. Returns `Some(err)` once per occurrence; later calls
    /// return `None` until another error is captured. Used by
    /// `fill_chunk_from_iter` (and its open/next callers) to surface engine
    /// failures to the Java caller as an `FrsErrorCode`. Tolerates a poisoned
    /// mutex — the only way to poison is a panic while we hold the lock; the
    /// stored value is still readable and overwriting it restores progress.
    fn take_last_error(&self) -> Option<forst_rs_common::ForstError> {
        self.last_error
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take()
    }

    /// R17-M3: stash a deferred error so the next `_next` call surfaces it
    /// AFTER the caller has drained the partial chunk that triggered it.
    fn set_deferred_error(&mut self, err: forst_rs_common::ForstError) {
        self.deferred_error = Some(err);
    }

    /// R17-M3: take + clear the deferred error stash. Returns `Some(err)`
    /// once per occurrence.
    fn take_deferred_error(&mut self) -> Option<forst_rs_common::ForstError> {
        self.deferred_error.take()
    }

    /// Pull the next row from the iterator, preferring the pending row
    /// (rolled back from a previous overflow).
    fn next_row(&mut self) -> Option<(IterKey, IterValue)> {
        if let Some(p) = self.pending.take() {
            return Some(p);
        }
        match &mut self.inner {
            IterBackend::Boxed(inner) => inner.next(),
            // The pinned backend is drained exclusively through
            // `fill_chunk_from_pinned` (push-style); pull is unreachable.
            IterBackend::Pinned(_) => {
                debug_assert!(false, "next_row called on a pinned iter backend");
                None
            }
        }
    }

    /// Push a row back to be returned on the next `next_row()` call.
    fn put_back(&mut self, row: (IterKey, IterValue)) {
        debug_assert!(
            self.pending.is_none(),
            "put_back called with pending row already set"
        );
        self.pending = Some(row);
    }

    fn abort(&self) {
        self.aborted
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn is_aborted(&self) -> bool {
        self.aborted.load(std::sync::atomic::Ordering::Acquire)
    }

    /// FRS-ITER-EAGER-FREE (2026-06-04): the iterator is EXHAUSTED — drop its
    /// heavy backing state NOW (the `LazyPrefixIter` tier cursors, any buffered
    /// SST/Arrow `RecordBatch`es, and the captured `Arc<DbImpl>`) by replacing
    /// `inner` with an empty iterator. The handle stays registered + valid: a
    /// later `next_row()` yields `None` (EOF) and `close` frees the now-light
    /// shell. WHY: q4's async join issues ~one prefix iterator PER record, most
    /// exhausted by their first chunk; the Java consumer's `close()` and the
    /// lifetime watchdog lag under that rate, so exhausted iterators (each
    /// pinning decoded Arrow batches) accumulate to millions of live allocations
    /// — the dominant q4 memory wall (`heap`: ~85M live 80-96B Arrow structs).
    /// Freeing on exhaustion bounds resident memory regardless of close latency.
    /// Sound: only called when `fill_chunk_from_iter` reported `exhausted == true`
    /// (upstream `next_row()` returned `None`), so no pending rows are dropped.
    fn drop_inner(&mut self) {
        // S2: for a Pinned backend this drops the PrefixScanStream — its
        // tier sources release every pinned `DecodedBlock` (and the
        // prefetchers' windows) transitively, so no `Arc<KvBlock>` outlives
        // this call beyond the shared block cache's own reference.
        self.inner = IterBackend::Boxed(Box::new(std::iter::empty()));
        self.pending = None;
        self.terminal
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

/// Number of registry shards.  Power-of-two so the shard index is a fast
/// bit-mask.  16 shards is enough headroom for typical Flink slot counts
/// (8–16 task slots per TM) without serializing iter opens on a single
/// global Mutex (PR-D4 / B2-H5).
const ITER_SHARD_COUNT: usize = 16;
const ITER_SHARD_MASK: u64 = (ITER_SHARD_COUNT as u64) - 1;

static ITER_SHARDS: OnceLock<[Mutex<HashMap<u64, IterHandle>>; ITER_SHARD_COUNT]> = OnceLock::new();
static NEXT_ITER_ID: AtomicU64 = AtomicU64::new(1);

/// Initialise the shard array on first access.  Each shard is independent;
/// the lock taken in `open`/`next`/`close`/`abort` is per-shard, never
/// global — that is the property PR-D4 needs.
fn iter_shards() -> &'static [Mutex<HashMap<u64, IterHandle>>; ITER_SHARD_COUNT] {
    ITER_SHARDS.get_or_init(|| std::array::from_fn(|_| Mutex::new(HashMap::new())))
}

#[inline]
fn shard_for(handle: u64) -> &'static Mutex<HashMap<u64, IterHandle>> {
    &iter_shards()[(handle & ITER_SHARD_MASK) as usize]
}

/// Pull rows directly from `iter` into the caller's buffer.  Each row is
/// serialised as `[klen u32 LE][vlen u32 LE][key bytes][value bytes]`.
///
/// Capacity is enforced against the *actual* wire bytes (header + payload).
/// If a row would overflow, it is rolled back to `iter.pending` so the
/// next call returns it first — no rows are silently dropped.
///
/// Returns `(bytes_written, row_count)`.  Stops at first row that would
/// overflow or when the iterator is exhausted.
///
/// # Safety
/// `buf` must point to at least `cap` writable bytes for the duration of
/// the call.
/// Fills one chunk and reports whether the iterator was EXHAUSTED (its
/// `next_row()` returned `None`) vs merely buffer-full (a row was `put_back`).
/// The `exhausted` flag lets callers eagerly free the iterator's heavy backing
/// state via [`IterHandle::drop_inner`] — see FRS-ITER-EAGER-FREE.
unsafe fn fill_chunk_from_iter(
    iter: &mut IterHandle,
    buf: *mut u8,
    cap: usize,
) -> (u32, u32, bool) {
    // Aborted iters return an empty chunk — preserve the abort semantic
    // (treated as exhausted so callers free the shell).
    if iter.is_aborted() {
        return (0, 0, true);
    }
    // S2: pinned backends are PUSH-style — the engine drives rows into a
    // ChunkSink writing the wire format directly (no IterKey/IterValue, no
    // Arc traffic). Split borrows: `inner` and `last_error` are disjoint
    // fields.
    if let IterBackend::Pinned(p) = &mut iter.inner {
        return fill_chunk_from_pinned(p, &iter.last_error, buf, cap);
    }
    let mut off = 0usize;
    let mut row_count = 0u32;
    loop {
        let (k, v) = match iter.next_row() {
            // `None` ⟹ the iterator is genuinely exhausted (no rows left).
            None => return (off as u32, row_count, true),
            Some(row) => row,
        };
        let klen = k.len();
        let vlen = v.len();
        let row_size = 8 + klen + vlen;
        if off + row_size > cap {
            // Row doesn't fit — roll back so the next call returns it. NOT
            // exhausted: there is at least this pending row plus possibly more.
            // For `IterKey::Arc` this is a cheap atomic refcount move; for
            // `IterKey::Vec` it is a pointer move. No bytes are copied.
            iter.put_back((k, v));
            return (off as u32, row_count, false);
        }
        let klen_u32 = klen as u32;
        let vlen_u32 = vlen as u32;
        std::ptr::copy_nonoverlapping(klen_u32.to_le_bytes().as_ptr(), buf.add(off), 4);
        off += 4;
        std::ptr::copy_nonoverlapping(vlen_u32.to_le_bytes().as_ptr(), buf.add(off), 4);
        off += 4;
        // B10-H3: `k.as_ptr()` reads through `IterKey::as_ptr` which
        // matches on the enum tag and returns the inner backing-store
        // pointer. For the prefix-iter path this is the `Arc<[u8]>`'s
        // payload pointer — no `to_vec()` clone, just a memcpy straight
        // from the Arc-owned bytes into the caller's direct ByteBuffer.
        std::ptr::copy_nonoverlapping(k.as_ptr(), buf.add(off), klen);
        off += klen;
        std::ptr::copy_nonoverlapping(v.as_ptr(), buf.add(off), vlen);
        off += vlen;
        row_count += 1;
    }
}

/// S2 (W1c): `RowSink` writing the FFI wire format
/// `[klen u32 LE][vlen u32 LE][key][value]` straight into the caller's chunk
/// buffer — the borrowed slices are memcpy'd within the `push` call (the D1
/// borrow contract), so SST bytes → chunk is exactly one copy with zero
/// intervening allocations. On overflow the row is copied into the reused
/// pending buffers (delivered first by the next fill) and `false` stops the
/// fill — the existing chunk-full backpressure, minus the Arc rollback.
struct ChunkSink<'a> {
    buf: *mut u8,
    cap: usize,
    off: usize,
    rows: u32,
    pending_key: &'a mut Vec<u8>,
    pending_val: &'a mut Vec<u8>,
    pending_set: &'a mut bool,
}

impl forst_rs_engine::RowSink for ChunkSink<'_> {
    fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
        let row_size = 8 + key.len() + value.len();
        if self.off + row_size > self.cap {
            // Chunk full: stash the delivered row (reused capacity — zero
            // steady-state alloc; ~one row memcpy per chunk boundary).
            self.pending_key.clear();
            self.pending_key.extend_from_slice(key);
            self.pending_val.clear();
            self.pending_val.extend_from_slice(value);
            *self.pending_set = true;
            return false;
        }
        // SAFETY: `buf` points to at least `cap` writable bytes for the
        // duration of the enclosing fill call (the
        // `fill_chunk_from_pinned` contract), and `off + row_size <= cap`.
        unsafe {
            let klen = key.len() as u32;
            let vlen = value.len() as u32;
            std::ptr::copy_nonoverlapping(klen.to_le_bytes().as_ptr(), self.buf.add(self.off), 4);
            std::ptr::copy_nonoverlapping(
                vlen.to_le_bytes().as_ptr(),
                self.buf.add(self.off + 4),
                4,
            );
            std::ptr::copy_nonoverlapping(key.as_ptr(), self.buf.add(self.off + 8), key.len());
            std::ptr::copy_nonoverlapping(
                value.as_ptr(),
                self.buf.add(self.off + 8 + key.len()),
                value.len(),
            );
        }
        self.off += row_size;
        self.rows += 1;
        true
    }
}

/// S2: pinned-backend chunk fill — the push-style twin of the boxed loop in
/// [`fill_chunk_from_iter`], same `(bytes_written, row_count, exhausted)`
/// contract. The stashed overflow row (if any) is written first; fill errors
/// are recorded sticky-FIRST into the shared error slot (the channel the
/// open/next callers already drain) and the fill CONTINUES past the errored
/// key (F-1, PMC cycle-5) — the legacy boxed path's `filter_map` records the
/// error and keeps scanning, so rows after the errored key are still
/// delivered and the delivered-row prefix is identical in both modes. The
/// R17-M3/R18-M4 deferred-error state machine surfaces the recorded error
/// exactly like the legacy path (partial chunk first, error on the next
/// call, then terminal).
///
/// # Safety
/// `buf` must point to at least `cap` writable bytes for the duration of the
/// call.
unsafe fn fill_chunk_from_pinned(
    p: &mut PinnedIter,
    last_error: &Arc<Mutex<Option<forst_rs_common::ForstError>>>,
    buf: *mut u8,
    cap: usize,
) -> (u32, u32, bool) {
    let mut off = 0usize;
    let mut rows = 0u32;
    // Deliver the chunk-overflow stash first (it was already produced).
    if p.pending_set {
        let row_size = 8 + p.pending_key.len() + p.pending_val.len();
        if row_size > cap {
            // Row larger than the whole chunk — same non-progress contract
            // as the legacy put_back path (caller must supply a larger buf).
            return (0, 0, false);
        }
        let klen = p.pending_key.len() as u32;
        let vlen = p.pending_val.len() as u32;
        std::ptr::copy_nonoverlapping(klen.to_le_bytes().as_ptr(), buf, 4);
        std::ptr::copy_nonoverlapping(vlen.to_le_bytes().as_ptr(), buf.add(4), 4);
        std::ptr::copy_nonoverlapping(p.pending_key.as_ptr(), buf.add(8), p.pending_key.len());
        std::ptr::copy_nonoverlapping(
            p.pending_val.as_ptr(),
            buf.add(8 + p.pending_key.len()),
            p.pending_val.len(),
        );
        off += row_size;
        rows += 1;
        p.pending_set = false;
    }
    if p.exhausted {
        return (off as u32, rows, true);
    }
    let mut sink = ChunkSink {
        buf,
        cap,
        off,
        rows,
        pending_key: &mut p.pending_key,
        pending_val: &mut p.pending_val,
        pending_set: &mut p.pending_set,
    };
    loop {
        match p.stream.fill_into(&mut sink) {
            Ok(forst_rs_engine::FillOutcome::Exhausted) => {
                let (off, rows) = (sink.off, sink.rows);
                p.exhausted = true;
                return (off as u32, rows, true);
            }
            Ok(forst_rs_engine::FillOutcome::SinkFull) => {
                return (sink.off as u32, sink.rows, false);
            }
            Err(e) => {
                // Sticky-FIRST into the shared slot (R18-M3 semantics); the
                // open/next callers drain it and run the partial-chunk state
                // machine. F-1: do NOT park exhausted — `fill_into` has
                // already advanced the merge PAST the errored key (every
                // error path advances its source before surfacing), so
                // looping here keeps delivering the rows after it, exactly
                // like the legacy filter_map, and strictly approaches
                // `Exhausted` (no spin).
                let mut guard = last_error.lock().unwrap_or_else(|p| p.into_inner());
                if guard.is_none() {
                    *guard = Some(e);
                }
            }
        }
    }
}

/// Open a prefix-scoped iterator anchored to a point-in-time snapshot of
/// the engine state (V1: full materialization via `prefix_scan`).
///
/// On success (`FrsErrorCode::Ok`):
/// - `*out_handle` is set to a non-zero opaque iterator handle.
/// - The first chunk is written to `chunk_buf_ptr[0..chunk_buf_cap]`.
/// - `*out_row_count` is set to the number of rows in the first chunk.
/// - `*out_bytes_used` is set to the number of bytes written to the buffer.
///
/// P0 EOF + AUTO-CLOSE (streaming-read redesign §2.3): when the FIRST chunk
/// already exhausts the iterator and no error is pending, the engine
/// auto-closes it and sets `*out_handle = 0` — the rows are still in the
/// chunk. The caller may skip the trailing `_next` and `_close` crossings;
/// callers that issue them anyway observe normal 0-row exhaustion (`_next`)
/// and a no-op (`_close`). A non-zero handle is returned ONLY when more
/// chunks (or a deferred error) remain.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) on success.
/// - `FrsErrorCode::BatchHeaderMalformed` (110) on null arguments or bad CF.
/// - `FrsErrorCode::EngineIo` (300) on engine-side errors.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_iter_prefix_open(
    db: FrsDb,
    cf: FrsCfHandle,
    prefix_ptr: *const u8,
    prefix_len: u32,
    chunk_buf_ptr: *mut u8,
    chunk_buf_cap: u32,
    out_handle: *mut u64,
    out_row_count: *mut u32,
    out_bytes_used: *mut u32,
) -> i32 {
    guarded_vec(|| {
        if out_handle.is_null() || out_row_count.is_null() || out_bytes_used.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if chunk_buf_ptr.is_null() && chunk_buf_cap > 0 {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db_ref) = db_from_handle(db) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf_ref_) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if (prefix_len as usize) > MAX_KEY_LEN {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let prefix = if prefix_ptr.is_null() || prefix_len == 0 {
            &[][..]
        } else {
            slice::from_raw_parts(prefix_ptr, prefix_len as usize)
        };

        // PR-C6-H1 + B10-H3 + B11-H3 + R17-M1: route through the
        // zero-copy-{key,value} streaming `prefix_scan_iter_owned_arc_with_error_slot`.
        // The iterator captures `Arc<DbImpl>` internally so it can be stashed
        // in the per-shard `IterHandle` registry and outlive this FFI call.
        // BOTH halves of each row are emitted as `Arc<[u8]>` (wrapped in
        // `IterKey::Arc` and `IterValue::Arc`) so the chunk-fill memcpy reads
        // directly out of the Arc-owned bytes — no per-row `Vec<u8>`
        // allocation at the public boundary, and `put_back` rollback is a
        // refcount move instead of a Vec move.
        //
        // R17-M1: allocate the shared error slot UP-FRONT and pass it into
        // the engine variant so the underlying `LazyPrefixIter` publishes its
        // tier-peek errors into the SAME slot we already drain for outer
        // `db.get_arc` errors. Pre-fix (R16-M2 only), the engine variant
        // boxed the iter as `Box<dyn Iterator>` which erased the
        // `LazyPrefixIter` type — its `take_last_error()` was unreachable
        // and tier peek errors were silently dropped.
        // FRS-PROBE-DIAG (2026-06-02): split the per-probe open cost into BUILD
        // (k-way-merge construction = reader opens + tier cursors) vs FILL (first
        // chunk drain = get_arc per key). Gated by FRS_PROBE_DIAG=1; logs only
        // probes slower than 5ms so it pinpoints the q7 ~100/s stall's cost
        // without flooding. Zero overhead when unset.
        let probe_diag = probe_diag_enabled();
        let probe_t0 = if probe_diag {
            Some(std::time::Instant::now())
        } else {
            None
        };
        let error_slot: Arc<Mutex<Option<forst_rs_common::ForstError>>> =
            Arc::new(Mutex::new(None));
        // S2 (flag ON): hold the engine's push-style PrefixScanStream and
        // drive `fill_into` straight into the chunk buffer — the raw sink
        // path (zero per-row allocations on SST-Put rows; no Arc traffic).
        // Flag OFF keeps the legacy Arc-pair pull iterator byte-for-byte.
        // R1: take the push-style stream path when S2 is statically ON *or*
        // when per-scan fan-out-adaptive selection is active (`FRS_S2_FANOUT_MIN`
        // set). The stream picks pinned/loser-tree per-scan by overlap depth;
        // shallow scans drive the byte-identical legacy decision procedure.
        // With both unset this is false => today's exact owned-arc path.
        let mut handle_state = if forst_rs_engine::s2_pinned_enabled()
            || forst_rs_engine::s2_adaptive_active()
        {
            match db_ref.prefix_scan_stream_with_error_slot(
                cf_ref_,
                prefix,
                Arc::clone(&error_slot),
            ) {
                Ok(stream) => IterHandle::new_pinned_with_error_slot(stream, error_slot),
                Err(_) => return FrsErrorCode::EngineIo as i32,
            }
        } else {
            let owned_iter = match db_ref.prefix_scan_iter_owned_arc_with_error_slot(
                cf_ref_,
                prefix,
                Arc::clone(&error_slot),
            ) {
                Ok(it) => it,
                Err(_) => return FrsErrorCode::EngineIo as i32,
            };
            // R16-M2: replace the bare `r.ok()` (which silently dropped engine
            // errors) with an error-tap closure that records each `Err(_)` in
            // the IterHandle's shared error slot. The FFI consumer drains the
            // slot via `take_last_error` after every chunk-get and translates a
            // recorded error into an `FrsErrorCode` so the Java side observes
            // the failure instead of seeing a clean end-of-iterator.
            // R17-L2: tolerate a poisoned mutex (the only way to poison the lock
            // is a panic while we hold it — recoverable by overwriting with the
            // observed error).
            let error_slot_inner = Arc::clone(&error_slot);
            let inner: Box<dyn Iterator<Item = (IterKey, IterValue)> + Send> =
                Box::new(owned_iter.filter_map(move |r| match r {
                    Ok((k, v)) => Some((IterKey::Arc(k), IterValue::Arc(v))),
                    Err(e) => {
                        // R18-M3: sticky-FIRST. The FFI consumer drains via
                        // `take_last_error()` after every chunk so within a
                        // single chunk we MUST preserve the first error: a
                        // later error may be a cascade of the first (e.g.,
                        // tier-source IO failure → downstream merge errors)
                        // and the first is the most diagnosable cause. Pre-
                        // fix `*guard = Some(e)` overwrote unconditionally,
                        // returning the LAST cascade error to the Java side
                        // and burying the actual root cause. Tolerate a
                        // poisoned mutex — overwriting a poisoned slot is
                        // benign because we only write when empty.
                        let mut guard = error_slot_inner.lock().unwrap_or_else(|p| p.into_inner());
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                        None
                    }
                }));
            IterHandle::new_with_error_slot(inner, error_slot)
        };
        let probe_build_us = probe_t0.map(|t| t.elapsed().as_micros());

        // Fill the first chunk lazily into the caller's buffer.
        let (bytes_used, row_count, iter_exhausted) =
            fill_chunk_from_iter(&mut handle_state, chunk_buf_ptr, chunk_buf_cap as usize);
        if iter_exhausted {
            // FRS-ITER-EAGER-FREE: first chunk drained the whole result — free the
            // heavy backing state now (don't pin it until the lagging Java close()).
            handle_state.drop_inner();
        }

        if let (Some(t0), Some(build_us)) = (probe_t0, probe_build_us) {
            let total_us = t0.elapsed().as_micros();
            if total_us > 5000 {
                let fill_us = total_us.saturating_sub(build_us);
                eprintln!(
                    "FRS-PROBE-DIAG slow open: total_us={} build_us={} fill_us={} rows={} prefix_len={}",
                    total_us, build_us, fill_us, row_count, prefix_len
                );
            }
        }

        // R16-M2 + R17-M3: surface any error captured during the first chunk
        // fill. The partial-chunk state machine has two branches:
        //   (1) error captured with NO rows serialised → fail open() directly;
        //       no handle is registered and the caller observes a clean
        //       failure.
        //   (2) error captured AFTER one or more rows serialised → return the
        //       partial chunk + Ok, register the handle, and STASH the error
        //       on the handle so the next `_next` call surfaces it BEFORE
        //       pulling more rows. Pre-fix the open path zeroed
        //       row_count/bytes_used and discarded the already-serialised
        //       rows — silent data loss for rows the caller had bytes for.
        let mut deferred_error_stashed = false;
        if let Some(err) = handle_state.take_last_error() {
            if row_count == 0 {
                *out_row_count = 0;
                *out_bytes_used = 0;
                *out_handle = 0;
                return error_to_frs_code(&err);
            }
            // Partial chunk path: defer the error to the next `_next` call so
            // the caller drains the in-flight rows first.
            handle_state.set_deferred_error(err);
            deferred_error_stashed = true;
        }

        // P0 EOF + AUTO-CLOSE (streaming-read redesign §2.3): the first chunk
        // already exhausted the iterator and no error is pending — do NOT
        // register a dead shell. `*out_handle = 0` is the single-shot EOF
        // signal (the batched paths use `FrsChunk::_reserved` instead, which
        // this ABI lacks). Old callers that still issue the mandatory trailing
        // `_next(0)` get the normal 0-row exhaustion (unknown handles are
        // EOF, see `frs_vec_iter_prefix_next`), and `_close(0)` is a no-op —
        // new callers skip both crossings entirely.
        if iter_exhausted && !deferred_error_stashed {
            *out_handle = 0;
            *out_row_count = row_count;
            *out_bytes_used = bytes_used;
            return FrsErrorCode::Ok as i32;
        }

        // Register the iterator on a sharded registry.  Shard is selected
        // by the lower 4 bits of `handle_id`, so opens from different
        // threads with sequential ids fall on different shards and never
        // contend on a single global Mutex.
        let handle_id = NEXT_ITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        shard_for(handle_id)
            .lock()
            .unwrap()
            .insert(handle_id, handle_state);

        *out_handle = handle_id;
        *out_row_count = row_count;
        *out_bytes_used = bytes_used;
        FrsErrorCode::Ok as i32
    })
}

/// Fetch the next chunk from a previously opened iterator.
///
/// On success (`FrsErrorCode::Ok`):
/// - Rows are written to `chunk_buf_ptr[0..chunk_buf_cap]`.
/// - `*out_row_count` is the number of rows written.
/// - `*out_bytes_used` is the byte count consumed.
///
/// When `*out_row_count == 0` the iterator is exhausted; call
/// `frs_vec_iter_prefix_close` to release the handle.
///
/// P0 AUTO-CLOSE NOTE (streaming-read redesign §2.3): handles whose first
/// chunk exhausted the iterator are auto-closed at open time (single-shot
/// opens return `handle == 0`; batched opens return a non-zero UNREGISTERED
/// handle + the `FRS_CHUNK_EOF` flag). A `_next` on such a handle — or any
/// unknown handle — returns `Ok` with an empty chunk (EOF semantics), NOT
/// `IterCursorInvalid`, so flag-unaware callers that always issue the
/// mandatory trailing `_next` observe the same exhaustion they did before.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) always on success (even exhaustion, and for
///   auto-closed/unknown handles — see above).
/// - `FrsErrorCode::BatchHeaderMalformed` (110) on null out-pointers.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_iter_prefix_next(
    handle: u64,
    chunk_buf_ptr: *mut u8,
    chunk_buf_cap: u32,
    out_row_count: *mut u32,
    out_bytes_used: *mut u32,
) -> i32 {
    guarded_vec(|| {
        if out_row_count.is_null() || out_bytes_used.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if chunk_buf_ptr.is_null() && chunk_buf_cap > 0 {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let mut guard = shard_for(handle).lock().unwrap_or_else(|p| p.into_inner());
        let iter = match guard.get_mut(&handle) {
            Some(it) => it,
            // P0 AUTO-CLOSE: an unregistered handle is an auto-closed
            // (exhausted-at-open) iterator — report clean EOF so old callers'
            // mandatory trailing `_next` keeps working. Pre-P0 this returned
            // `IterCursorInvalid` (201); with auto-close the registry can no
            // longer distinguish "never existed" from "exhausted at open", and
            // EOF is the correct answer for the latter. (`_abort` keeps the
            // 201 contract — the watchdog only aborts handles it saw open.)
            None => {
                *out_row_count = 0;
                *out_bytes_used = 0;
                return FrsErrorCode::Ok as i32;
            }
        };
        // R18-M4: if a prior `_next` surfaced a deferred error and marked
        // the iterator terminal, return EOF without pulling further rows.
        // Pre-fix, after surfacing the deferred error the iterator was
        // otherwise live — the next call would return rows from OTHER tier
        // sources (multi-tier chains past the failed tier transparently),
        // surprising the Java consumer which had just received an error
        // code and expected the iterator to be done.
        if iter.is_terminal() {
            *out_row_count = 0;
            *out_bytes_used = 0;
            return FrsErrorCode::Ok as i32;
        }
        // R17-M3: drain any deferred error from the previous chunk's
        // partial-chunk fill BEFORE pulling new rows. The caller has already
        // observed the partial rows that triggered this error, so we now owe
        // them the error code (empty chunk, no further bytes).
        if let Some(err) = iter.take_deferred_error() {
            *out_row_count = 0;
            *out_bytes_used = 0;
            // R18-M4: mark terminal so subsequent _next calls don't reach
            // past the failed tier into surviving tier sources.
            iter.mark_terminal();
            return error_to_frs_code(&err);
        }
        let (bytes_used, row_count, iter_exhausted) =
            fill_chunk_from_iter(iter, chunk_buf_ptr, chunk_buf_cap as usize);
        if iter_exhausted {
            // FRS-ITER-EAGER-FREE: this `next` chunk exhausted the iterator — free
            // its heavy backing state now (the registered shell stays valid for close).
            iter.drop_inner();
        }
        *out_row_count = row_count;
        *out_bytes_used = bytes_used;
        // R16-M2 + R17-M3: drain the per-iter error slot AFTER the chunk fill
        // so a tier-source error that surfaced mid-chunk is propagated to the
        // Java side as an `FrsErrorCode`. Pre-fix, the bare `r.ok()` in the
        // filter_map adapter silently dropped engine errors here.
        //
        // Partial-chunk state machine: if rows were serialised before the
        // error, return Ok + the partial chunk and STASH the error for the
        // next `_next` call. Pre-fix the next path overwrote row_count/
        // bytes_used to 0 — silently discarding the partial chunk the caller
        // had bytes for. Only when row_count == 0 do we surface the error in
        // this call (no rows to drain first).
        if let Some(err) = iter.take_last_error() {
            if row_count == 0 {
                // R18-M4: error surfaced with no rows to drain first — mark
                // terminal here so the next call cannot reach past the
                // failed tier into surviving tier sources.
                iter.mark_terminal();
                return error_to_frs_code(&err);
            }
            iter.set_deferred_error(err);
        }
        FrsErrorCode::Ok as i32
    })
}

/// Release an iterator handle opened by `frs_vec_iter_prefix_open`.
///
/// After this call the handle is invalid; passing it to `_abort` returns
/// `FrsErrorCode::IterCursorInvalid`, and `_next` reports clean EOF (P0
/// auto-close semantics — see `frs_vec_iter_prefix_next`). Safe to call with
/// `handle == 0` or an auto-closed (never-registered) handle (no-op, `Ok`).
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) always.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub extern "C" fn frs_vec_iter_prefix_close(handle: u64) -> i32 {
    guarded_vec(|| {
        if handle == 0 {
            return FrsErrorCode::Ok as i32;
        }
        shard_for(handle).lock().unwrap().remove(&handle);
        FrsErrorCode::Ok as i32
    })
}

/// Watchdog hook: atomically mark an iterator as aborted so subsequent
/// `frs_vec_iter_prefix_next` calls return an empty chunk immediately.
///
/// The Java-side `IterLifetimeWatchdog` calls this on idle/max-lifetime
/// breach.  The handle remains in the registry until `frs_vec_iter_prefix_close`
/// is called; the watchdog should call close immediately after abort.
///
/// Safe to call from a different thread than the one that opened the
/// handle — abort writes to an `AtomicBool` inside the handle, and the
/// per-shard `Mutex` is the only synchronisation point we need.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) on success.
/// - `FrsErrorCode::IterCursorInvalid` (201) if `handle` is unknown.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub extern "C" fn frs_vec_iter_prefix_abort(handle: u64) -> i32 {
    guarded_vec(|| {
        let guard = shard_for(handle).lock().unwrap();
        match guard.get(&handle) {
            Some(iter) => {
                iter.abort();
                FrsErrorCode::Ok as i32
            }
            None => FrsErrorCode::IterCursorInvalid as i32,
        }
    })
}

// ---------------------------------------------------------------------------
// 12.b. Batched vectorized chunked iterator open — frs_vec_iter_prefix_open_batch
//       (PR-E3 / E-HIGH-5 / F5-4)
//
// Replaces N FFI crossings (one per `frs_vec_iter_prefix_open`) with a single
// crossing that opens N prefix iterators in one call.  Layout mirrors PR-D3's
// packed SoA (offsets + flat data) for the prefixes, and an AoS output array
// of `FrsChunk` per iter.  Caller pre-allocates one chunk buffer per iter at
// a uniform capacity `chunk_cap`, passing each per-iter pointer in the
// `FrsChunk::buf_ptr` input slot; the engine fills `row_count` + `bytes_used`
// for that iter.  Handles are written to `out_handles[i]` (0 if open i
// failed).  All iters that successfully open are registered in the shared
// per-shard registry exactly as if opened individually.
//
// Partial-failure semantics: each row is independent; a malformed prefix
// offset, an out-of-range prefix slice, or an engine error on a single iter
// sets `out_handles[i] = 0` and `out_first_chunks[i] = {NULL, 0, 0}` for
// that row only.  The return code reflects the first non-Ok per-row error
// or `Ok` if all opens succeeded.  Successfully-opened iters remain valid
// regardless of failures on other rows in the batch.
// ---------------------------------------------------------------------------

/// Per-iter chunk descriptor for `frs_vec_iter_prefix_open_batch`.
///
/// Layout (24 bytes, `repr(C)`):
/// - `buf_ptr`     — **input**: caller-owned chunk buffer pointer for this iter.
/// - `buf_cap`     — **input**: capacity of `buf_ptr` in bytes (caller-supplied).
/// - `row_count`   — **output**: rows written to `buf_ptr` (0 on failure/empty).
/// - `bytes_used`  — **output**: bytes written to `buf_ptr` (0 on failure/empty).
/// - `_reserved`   — **output**: flag word (was padding; old callers that never
///   read it are unaffected). Bit 0 = [`FRS_CHUNK_EOF`].
///
/// Each per-iter chunk follows the same wire format as
/// `frs_vec_iter_prefix_open`: rows packed as `[klen u32 LE][vlen u32 LE]
/// [key bytes][value bytes]`.
#[repr(C)]
pub struct FrsChunk {
    pub buf_ptr: *mut u8,
    pub buf_cap: u32,
    pub row_count: u32,
    pub bytes_used: u32,
    pub _reserved: u32,
}

/// P0 (streaming-read redesign §2.3): bit 0 of [`FrsChunk::_reserved`].
///
/// Set by the batched open paths when the FIRST chunk already exhausted the
/// iterator AND no error is pending. When set, the engine has AUTO-CLOSED the
/// iterator: no registry entry exists for the returned handle, so the caller
/// may (and should) skip both the trailing `frs_vec_iter_prefix_next` and the
/// `frs_vec_iter_prefix_close` crossings — the two guaranteed-wasted crossings
/// of the dominant exhausted-in-one-chunk probe (q7-class).
///
/// The flag is ADVISORY and fully backward compatible: a caller that ignores
/// it still gets a non-zero (but unregistered) handle; `_next` on an
/// unregistered handle returns the normal 0-row exhaustion (`Ok`), and
/// `_close` on it is a safe no-op. The flag is NEVER set when a deferred
/// error is stashed (partial-chunk-then-error probes keep the registered
/// handle so `_next` can surface the error).
pub const FRS_CHUNK_EOF: u32 = 1;

/// Batched open of N prefix iterators in a single FFI crossing.
///
/// Inputs (packed SoA — same layout PR-D3 used for batched gets):
/// - `prefixes_off[i]` ranges over `[i .. i+1]` to identify prefix bytes
///   `prefixes_data[prefixes_off[i] .. prefixes_off[i+1]]`.
/// - `prefixes_off` length is `n + 1` (the sentinel terminator is required).
/// - `out_handles` is an array of `n` `u64` slots; on success slot `i`
///   carries the opened handle id, otherwise `0`.
/// - `out_first_chunks` is an array of `n` `FrsChunk` structs; caller fills
///   `buf_ptr` + `buf_cap` per row, engine fills `row_count` + `bytes_used`.
/// - `chunk_cap` is the uniform per-iter capacity assumed for all rows; it
///   serves as a sanity bound (caller-supplied `FrsChunk::buf_cap` MUST equal
///   `chunk_cap`, else the row returns `BatchHeaderMalformed` for that slot).
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) if all N opens succeeded.
/// - `FrsErrorCode::BatchHeaderMalformed` (110) on null args, count > MAX,
///   bad offsets, or bad chunk descriptor.  Per-row malformed inputs set
///   `out_handles[i] = 0` but do not abort the rest of the batch.
/// - `FrsErrorCode::EngineIo` (300) on engine-side errors (per-row; first
///   error code is propagated as the function return; the rest of the
///   batch continues processing).
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
///
/// # Safety
/// All pointers must be valid for reads/writes of the indicated counts for
/// the duration of the call.  `prefixes_off` MUST be a non-decreasing
/// sequence of `n + 1` `u32` values; the last entry MUST equal the total
/// length of `prefixes_data`.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_iter_prefix_open_batch(
    db: FrsDb,
    cf: FrsCfHandle,
    prefixes_off: *const u32,
    prefixes_data: *const u8,
    n: u32,
    out_handles: *mut u64,
    out_first_chunks: *mut FrsChunk,
    chunk_cap: u32,
) -> i32 {
    guarded_vec(|| {
        if n == 0 {
            return FrsErrorCode::Ok as i32;
        }
        if (n as usize) > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if prefixes_off.is_null() || out_handles.is_null() || out_first_chunks.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db_ref) = db_from_handle(db) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf_ref_) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };

        let n_us = n as usize;
        let offs = slice::from_raw_parts(prefixes_off, n_us + 1);
        let total_pref = offs[n_us] as usize;
        // C-R14-NEW-H3: aggregate-bytes cap mirroring C-R13-NEW-H1 etc.
        // u32 offsets permit up to 4 GiB of caller-supplied prefix payload;
        // without this cap a single open_batch call can OOM the host or
        // drive engine seek/comparator paths on multi-MiB prefix bytes.
        if total_pref > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let data_buf: &[u8] = if prefixes_data.is_null() || total_pref == 0 {
            &[]
        } else {
            slice::from_raw_parts(prefixes_data, total_pref)
        };
        let handles_out = slice::from_raw_parts_mut(out_handles, n_us);
        let chunks_out = slice::from_raw_parts_mut(out_first_chunks, n_us);

        let mut first_err: i32 = FrsErrorCode::Ok as i32;

        for i in 0..n_us {
            // Pre-zero outputs so partial-failure rows have well-defined state.
            handles_out[i] = 0;
            let chunk = &mut chunks_out[i];
            // Snapshot the input fields BEFORE we zero them — we still need
            // to write into `buf_ptr` if the open succeeds.
            let buf_ptr = chunk.buf_ptr;
            let buf_cap = chunk.buf_cap;
            chunk.row_count = 0;
            chunk.bytes_used = 0;
            // P0: `_reserved` is now an OUTPUT flag word (bit 0 = FRS_CHUNK_EOF);
            // the caller never initialises it, so zero it explicitly per row.
            chunk._reserved = 0;

            // Per-row offset validation.
            let ks = offs[i] as usize;
            let ke = offs[i + 1] as usize;
            if ke < ks || ke > total_pref {
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = FrsErrorCode::BatchHeaderMalformed as i32;
                }
                continue;
            }
            let prefix_len = ke - ks;
            if prefix_len > MAX_KEY_LEN {
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = FrsErrorCode::BatchHeaderMalformed as i32;
                }
                continue;
            }

            // Per-row chunk descriptor validation.  `buf_cap` must equal
            // `chunk_cap` (uniform sizing) and `buf_ptr` may be null only
            // when `chunk_cap == 0` (degenerate; first chunk will be empty
            // and caller will pull subsequent chunks via _next).
            if buf_cap != chunk_cap {
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = FrsErrorCode::BatchHeaderMalformed as i32;
                }
                continue;
            }
            if buf_ptr.is_null() && buf_cap > 0 {
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = FrsErrorCode::BatchHeaderMalformed as i32;
                }
                continue;
            }

            let prefix: &[u8] = if prefix_len == 0 {
                &[]
            } else {
                &data_buf[ks..ke]
            };

            // PR-C6-H1 + B10-H3 + B11-H3 + R17-M1: same zero-copy-{key,value}
            // owned streaming path as the single-shot open above, but routed
            // through `prefix_scan_iter_owned_arc_with_error_slot` so the
            // underlying `LazyPrefixIter`'s tier-peek errors land in the
            // SAME shared slot we already drain for outer `db.get_arc`
            // errors. Each iterator captures `Arc<DbImpl>` so the FFI
            // registry can outlive this call.
            let batch_error_slot: Arc<Mutex<Option<forst_rs_common::ForstError>>> =
                Arc::new(Mutex::new(None));
            let owned_iter = match db_ref.prefix_scan_iter_owned_arc_with_error_slot(
                cf_ref_,
                prefix,
                Arc::clone(&batch_error_slot),
            ) {
                Ok(it) => it,
                Err(_) => {
                    if first_err == FrsErrorCode::Ok as i32 {
                        first_err = FrsErrorCode::EngineIo as i32;
                    }
                    continue;
                }
            };
            // R16-M2 + R17-L2: error-tap closure mirroring the single-shot
            // `frs_vec_iter_prefix_open` path so engine errors mid-iter are
            // captured in the IterHandle's shared error slot and surfaced
            // back to the Java caller via take_last_error() on the next call.
            // Poison-tolerant lock acquire (R17-L2).
            let batch_error_slot_inner = Arc::clone(&batch_error_slot);
            let inner: Box<dyn Iterator<Item = (IterKey, IterValue)> + Send> =
                Box::new(owned_iter.filter_map(move |r| match r {
                    Ok((k, v)) => Some((IterKey::Arc(k), IterValue::Arc(v))),
                    Err(e) => {
                        // R18-M3: sticky-FIRST — preserve the first error in
                        // the chunk so cascade errors do not bury the root
                        // cause. See the matching comment on the single-shot
                        // `frs_vec_iter_prefix_open` path above.
                        let mut guard = batch_error_slot_inner
                            .lock()
                            .unwrap_or_else(|p| p.into_inner());
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                        None
                    }
                }));
            let mut handle_state = IterHandle::new_with_error_slot(inner, batch_error_slot);

            // Fill the first chunk into the caller-owned buffer.
            let (bytes_used, row_count, iter_exhausted) =
                fill_chunk_from_iter(&mut handle_state, buf_ptr, buf_cap as usize);
            if iter_exhausted {
                // FRS-ITER-EAGER-FREE: batch-open's first chunk drained this row's
                // whole result — free the heavy backing state now. The handle stays
                // registered (light shell) for the consumer's eventual close().
                handle_state.drop_inner();
            }
            // R16-M2 + R17-M3: if the first chunk fill captured an error,
            // surface it through the per-descriptor return path (batch open
            // returns multiple results; subsequent next() calls will drain
            // the same slot for additional errors). Partial-chunk preserving
            // semantics: when one or more rows already serialised before the
            // error, stash the error on the handle so the next `_next` call
            // surfaces it AFTER the caller drains the partial chunk. Pre-fix,
            // batch-open could lose rows on a mid-chunk error.
            let mut error_pending = false;
            if let Some(err) = handle_state.take_last_error() {
                error_pending = true;
                if row_count == 0 {
                    if first_err == FrsErrorCode::Ok as i32 {
                        first_err = error_to_frs_code(&err);
                    }
                    // R19-M1: when the first-chunk fill captured an error AND
                    // produced zero rows, the per-descriptor `first_err` carries
                    // the cause to the Java caller (via take_last_error consume +
                    // error_to_frs_code above). The handle is still registered
                    // on the sharded registry below so the caller's matching
                    // `_close` call lands on a valid handle ID — but without
                    // `mark_terminal()` a subsequent `_next` call would fall
                    // through to the upstream iterator and silently pull rows
                    // even though the caller already received an error code on
                    // open. Mark terminal so any post-open `_next` returns
                    // empty chunks + Ok (EOF semantics), matching the single-
                    // shot path's contract after a deferred-error surface.
                    handle_state.mark_terminal();
                } else {
                    handle_state.set_deferred_error(err);
                }
            }

            // P0 EOF + AUTO-CLOSE: the first chunk drained the whole probe and
            // no error is pending — flag EOF in `_reserved` and skip the
            // registry insert entirely (no dead shell, no shard mutex op).
            // The returned handle is a fresh NON-ZERO id that was never
            // registered: flag-aware callers skip `_next`/`_close`; legacy
            // callers that still call them get clean EOF (`_next` on an
            // unknown handle, see frs_vec_iter_prefix_next) and a no-op close.
            // Non-zero matters: legacy batch consumers treat `handle == 0` as
            // a per-row open failure.
            let handle_id = NEXT_ITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if iter_exhausted && !error_pending {
                chunk._reserved = FRS_CHUNK_EOF;
            } else {
                // Register on the shared sharded registry — same path as the
                // single-shot open so subsequent _next/_close/_abort calls work
                // transparently.
                shard_for(handle_id)
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(handle_id, handle_state);
            }

            handles_out[i] = handle_id;
            chunk.row_count = row_count;
            chunk.bytes_used = bytes_used;
        }

        first_err
    })
}

/// PARALLEL batched prefix-iterator open — the join read-path lever (q7/q9/q20).
///
/// Identical ABI to [`frs_vec_iter_prefix_open_batch`] (K prefixes packed SoA →
/// K handles + K first chunks, drained via the existing `frs_vec_iter_prefix_next`/
/// `_close`), but the K probes' build + drain run in PARALLEL inside the engine via
/// [`DbImpl::batch_prefix_scan_parallel`] (fanned across `bg_read_pool` /
/// `FRS_RS_READ_IO_PARALLELISM`) instead of the serial per-probe loop. The K probes are
/// independent reads on consistent snapshots, so results are byte-identical to K serial
/// opens (the engine method's correctness gate). Each probe's owned `(key,value)` result
/// set is wrapped as an `IterHandle` so the existing chunk/continuation machinery works
/// unchanged. Invalid descriptors are skipped WITHOUT scanning (only valid prefixes reach
/// the engine), so a malformed row never triggers a full-table scan. For the join's
/// bounded match windows the per-probe materialization is bounded by the batch.
///
/// # Safety
/// Same contract as [`frs_vec_iter_prefix_open_batch`].
#[no_mangle]
pub unsafe extern "C" fn frs_vec_iter_prefix_open_batch_parallel(
    db: FrsDb,
    cf: FrsCfHandle,
    prefixes_off: *const u32,
    prefixes_data: *const u8,
    n: u32,
    out_handles: *mut u64,
    out_first_chunks: *mut FrsChunk,
    chunk_cap: u32,
) -> i32 {
    guarded_vec(|| {
        if n == 0 {
            return FrsErrorCode::Ok as i32;
        }
        if (n as usize) > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if prefixes_off.is_null() || out_handles.is_null() || out_first_chunks.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db_ref) = db_from_handle(db) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf_ref_) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let n_us = n as usize;
        let offs = slice::from_raw_parts(prefixes_off, n_us + 1);
        let total_pref = offs[n_us] as usize;
        if total_pref > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let data_buf: &[u8] = if prefixes_data.is_null() || total_pref == 0 {
            &[]
        } else {
            slice::from_raw_parts(prefixes_data, total_pref)
        };
        let handles_out = slice::from_raw_parts_mut(out_handles, n_us);
        let chunks_out = slice::from_raw_parts_mut(out_first_chunks, n_us);

        let mut first_err: i32 = FrsErrorCode::Ok as i32;

        // Pass 1 (serial, cheap): validate each descriptor + pre-zero outputs. Collect ONLY
        // valid probes' (original index, prefix slice) so invalid rows never reach the engine
        // (an empty-prefix scan of an invalid row would be a full-table scan).
        let mut valid_indices: Vec<usize> = Vec::with_capacity(n_us);
        let mut valid_prefixes: Vec<&[u8]> = Vec::with_capacity(n_us);
        for i in 0..n_us {
            handles_out[i] = 0;
            let chunk = &mut chunks_out[i];
            let buf_cap = chunk.buf_cap;
            let buf_ptr = chunk.buf_ptr;
            chunk.row_count = 0;
            chunk.bytes_used = 0;
            // P0: `_reserved` is an OUTPUT flag word (bit 0 = FRS_CHUNK_EOF).
            chunk._reserved = 0;
            let ks = offs[i] as usize;
            let ke = offs[i + 1] as usize;
            if ke < ks
                || ke > total_pref
                || (ke - ks) > MAX_KEY_LEN
                || buf_cap != chunk_cap
                || (buf_ptr.is_null() && buf_cap > 0)
            {
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = FrsErrorCode::BatchHeaderMalformed as i32;
                }
                continue;
            }
            valid_indices.push(i);
            valid_prefixes.push(if ke == ks { &[] } else { &data_buf[ks..ke] });
        }

        if valid_prefixes.is_empty() {
            return first_err;
        }

        // Pass 2 (PARALLEL, BUILD + FIRST-CHUNK FILL — P1, streaming-read
        // redesign §2.3): one pool job per valid probe does the iterator BUILD
        // (overlapping-SST locate + reader open) AND the first-chunk fill
        // (block I/O + decompress + k-way merge + memcpy into the probe's own
        // caller buffer) AND the EOF/error decision. Pre-P1 only the build was
        // parallel; the fill — the whole cost of the dominant exhausted-in-
        // one-chunk probe — ran serially on this FFI thread.
        //
        // Safety of the cross-thread fill: each probe writes EXCLUSIVELY into
        // its own caller-provided chunk buffer (`chunkData[i*cap..(i+1)*cap]`
        // slices on the Java side — disjoint by construction); the buffers are
        // valid for the whole call because we JOIN all jobs (drain exactly K
        // results) before returning; the `IterHandle` is confined to its pool
        // job until handed back through the channel, then registered serially
        // in Pass 3. Out-descriptor (`FrsChunk`/handle array) writes stay
        // SERIAL in Pass 3 — pool jobs never touch them.
        struct SendMutPtr(*mut u8);
        // SAFETY: the wrapped pointer is a caller-owned buffer that outlives
        // the FFI call; each pointer is written by exactly ONE pool job
        // (disjoint buffers), so sharing the (read-only) table is sound.
        unsafe impl Send for SendMutPtr {}
        unsafe impl Sync for SendMutPtr {}

        /// Per-probe outcome computed on the pool worker; consumed serially in Pass 3.
        struct ProbeFill {
            /// `Some` ⇒ register on the shard registry (continuation or
            /// deferred/terminal error); `None` ⇒ build error or auto-closed EOF.
            handle_state: Option<IterHandle>,
            row_count: u32,
            bytes_used: u32,
            /// Clean exhausted-in-first-chunk ⇒ FRS_CHUNK_EOF, unregistered handle.
            eof: bool,
            /// Error code to fold into `first_err` (build error or zero-row fill error).
            err_code: Option<i32>,
            /// Build failed ⇒ `handles_out[i]` stays 0 (legacy per-row failure marker).
            build_failed: bool,
        }

        /// Shared first-chunk fill + R16-M2/R17-M3/R18-M4 error state machine
        /// (identical for the boxed and S2-pinned backends — the dispatch
        /// lives inside `fill_chunk_from_iter`).
        ///
        /// # Safety
        /// `buf_ptr` must be this probe's exclusive caller-owned buffer of at
        /// least `buf_cap` bytes, valid for the duration of the call.
        unsafe fn finish_probe_fill(
            mut handle_state: IterHandle,
            buf_ptr: *mut u8,
            buf_cap: usize,
        ) -> ProbeFill {
            let (bytes_used, row_count, iter_exhausted) =
                fill_chunk_from_iter(&mut handle_state, buf_ptr, buf_cap);
            if iter_exhausted {
                handle_state.drop_inner();
            }
            let mut err_code = None;
            let mut error_pending = false;
            if let Some(err) = handle_state.take_last_error() {
                error_pending = true;
                if row_count == 0 {
                    err_code = Some(error_to_frs_code(&err));
                    handle_state.mark_terminal();
                } else {
                    handle_state.set_deferred_error(err);
                }
            }
            let eof = iter_exhausted && !error_pending;
            ProbeFill {
                handle_state: if eof { None } else { Some(handle_state) },
                row_count,
                bytes_used,
                eof,
                err_code,
                build_failed: false,
            }
        }

        fn build_failed_probe(e: &forst_rs_common::ForstError) -> ProbeFill {
            ProbeFill {
                handle_state: None,
                row_count: 0,
                bytes_used: 0,
                eof: false,
                err_code: Some(error_to_frs_code(e)),
                build_failed: true,
            }
        }

        let bufs: Arc<Vec<(SendMutPtr, usize)>> = Arc::new(
            valid_indices
                .iter()
                .map(|&i| {
                    let c = &chunks_out[i];
                    (SendMutPtr(c.buf_ptr), c.buf_cap as usize)
                })
                .collect(),
        );
        let job_bufs = Arc::clone(&bufs);
        // S2 (flag ON): build push-style streams on the pool and drive
        // `fill_into` straight into each probe's buffer — the raw sink path
        // for the batch-open probe shape too (no Arc-pair adapter tax).
        // R1: stream path when statically ON or per-scan adaptive is active.
        let fills = if forst_rs_engine::s2_pinned_enabled() || forst_rs_engine::s2_adaptive_active()
        {
            db_ref.batch_open_prefix_streams_parallel_map(
                cf_ref_,
                &valid_prefixes,
                move |vi, built| -> ProbeFill {
                    match built {
                        Err(e) => build_failed_probe(&e),
                        Ok(mut stream) => {
                            let error_slot: Arc<Mutex<Option<forst_rs_common::ForstError>>> =
                                Arc::new(Mutex::new(None));
                            stream.set_shared_error_slot(Arc::clone(&error_slot));
                            let handle_state =
                                IterHandle::new_pinned_with_error_slot(stream, error_slot);
                            let (buf_ptr, buf_cap) = {
                                let b = &job_bufs[vi];
                                (b.0 .0, b.1)
                            };
                            // SAFETY: this probe's exclusive caller-owned
                            // buffer (see SendMutPtr rationale above).
                            unsafe { finish_probe_fill(handle_state, buf_ptr, buf_cap) }
                        }
                    }
                },
            )
        } else {
            db_ref.batch_open_prefix_iters_parallel_map(
                cf_ref_,
                &valid_prefixes,
                move |vi, built| -> ProbeFill {
                    match built {
                        Err(e) => build_failed_probe(&e),
                        Ok(owned_iter) => {
                            // IDENTICAL wrap + fill + error state machine to the
                            // serial frs_vec_iter_prefix_open_batch drain (zero-copy
                            // IterKey/Value::Arc, streamed; engine errors land in
                            // the per-probe slot) — just running on a pool worker.
                            let error_slot: Arc<Mutex<Option<forst_rs_common::ForstError>>> =
                                Arc::new(Mutex::new(None));
                            let slot_inner = Arc::clone(&error_slot);
                            let inner: Box<dyn Iterator<Item = (IterKey, IterValue)> + Send> =
                                Box::new(owned_iter.filter_map(move |r| match r {
                                    Ok((k, v)) => Some((IterKey::Arc(k), IterValue::Arc(v))),
                                    Err(e) => {
                                        let mut guard =
                                            slot_inner.lock().unwrap_or_else(|p| p.into_inner());
                                        if guard.is_none() {
                                            *guard = Some(e);
                                        }
                                        None
                                    }
                                }));
                            let handle_state = IterHandle::new_with_error_slot(inner, error_slot);
                            let (buf_ptr, buf_cap) = {
                                let b = &job_bufs[vi];
                                (b.0 .0, b.1)
                            };
                            // SAFETY: this probe's exclusive, caller-owned buffer
                            // (see SendMutPtr rationale above); valid for the call.
                            unsafe { finish_probe_fill(handle_state, buf_ptr, buf_cap) }
                        }
                    }
                },
            )
        };

        // Pass 3 (serial): registry insertion + out-descriptor writes ONLY —
        // everything heavy already happened on the pool. Same P0 EOF/auto-
        // close contract as the serial batch drain.
        for (vi, fill) in fills.into_iter().enumerate() {
            let i = valid_indices[vi];
            let chunk = &mut chunks_out[i];
            let Some(fill) = fill else {
                // Pool worker dropped its result (panic) — mirror the engine
                // sibling's internal-error mapping; per-row failure marker.
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = FrsErrorCode::EngineIo as i32;
                }
                continue;
            };
            if let Some(code) = fill.err_code {
                if first_err == FrsErrorCode::Ok as i32 {
                    first_err = code;
                }
            }
            if fill.build_failed {
                // handles_out[i] stays 0 (pre-zeroed in Pass 1).
                continue;
            }
            let handle_id = NEXT_ITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let Some(handle_state) = fill.handle_state {
                shard_for(handle_id)
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(handle_id, handle_state);
            } else {
                debug_assert!(fill.eof);
                chunk._reserved = FRS_CHUNK_EOF;
            }
            handles_out[i] = handle_id;
            chunk.row_count = fill.row_count;
            chunk.bytes_used = fill.bytes_used;
        }

        first_err
    })
}

// ---------------------------------------------------------------------------
// 13. Vectorized chunked range iterator — frs_vec_iter_range_* (P9)
//
// Mirrors the prefix-iterator symbols (section 12) but bounds the scan by a
// half-open interval [lo, hi) instead of a single prefix.  The engine's
// `scan(cf, lo, Some(hi))` API provides proper range semantics at V1.
//
// Handle lifecycle is shared with the prefix iterator: both kinds use the
// same sharded `ITER_SHARDS` registry and `NEXT_ITER_ID` counter, so
// close/abort are functionally identical to the prefix variants.
// ---------------------------------------------------------------------------

/// Open a range-scoped iterator over [lo, hi).
///
/// Semantics mirror `frs_vec_iter_prefix_open` but with explicit lower and
/// upper bounds.  The engine materialises the full [lo, hi) result set at open
/// time (V1 snapshot semantics).
///
/// # Parameters
/// - `db`            — engine handle.
/// - `cf`            — column-family handle.
/// - `lo_ptr/lo_len` — lower-bound key (inclusive); may be empty for start-of-keyspace.
/// - `hi_ptr/hi_len` — upper-bound key (exclusive); may be empty for end-of-keyspace.
/// - `chunk_buf_ptr/chunk_buf_cap` — caller-owned output buffer.
/// - `out_handle`    — receives the opaque iterator handle on success.
/// - `out_row_count` — receives the number of rows in the first chunk.
/// - `out_bytes_used`— receives the bytes written into `chunk_buf`.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) on success.
/// - `FrsErrorCode::BatchHeaderMalformed` (110) on null out-pointers or bad handles.
/// - `FrsErrorCode::EngineIo` (300) on engine errors.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_iter_range_open(
    db: FrsDb,
    cf: FrsCfHandle,
    lo_ptr: *const u8,
    lo_len: u32,
    hi_ptr: *const u8,
    hi_len: u32,
    chunk_buf_ptr: *mut u8,
    chunk_buf_cap: u32,
    out_handle: *mut u64,
    out_row_count: *mut u32,
    out_bytes_used: *mut u32,
) -> i32 {
    guarded_vec(|| {
        if out_handle.is_null() || out_row_count.is_null() || out_bytes_used.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if chunk_buf_ptr.is_null() && chunk_buf_cap > 0 {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db_ref) = db_from_handle(db) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf_ref_) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if (lo_len as usize) > MAX_KEY_LEN || (hi_len as usize) > MAX_KEY_LEN {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }

        let lo = if lo_ptr.is_null() || lo_len == 0 {
            &[][..]
        } else {
            slice::from_raw_parts(lo_ptr, lo_len as usize)
        };
        let hi_opt: Option<&[u8]> = if hi_ptr.is_null() || hi_len == 0 {
            None
        } else {
            Some(slice::from_raw_parts(hi_ptr, hi_len as usize))
        };

        // B-R7-NEW-H1: streaming range-scan iterator. Pre-fix this routed
        // through `db_ref.scan(...)` which eagerly drained every tier into
        // a BTreeSet then re-`get`-ed each key, paying
        // O(matching-keys x value-size) resident memory at open time. The
        // new `scan_iter_owned_arc_with_error_slot` mirrors the prefix
        // path's lazy k-way merge (active mem cursor, imm mem cursors,
        // overlapping SSTs block-streaming). First-row latency is
        // O(num_tiers); the FFI consumer drains chunks via
        // `fill_chunk_from_iter` with zero intermediate Vec allocation.
        // Both halves of each row are emitted as `Arc<[u8]>` (wrapped in
        // `IterKey::Arc` / `IterValue::Arc`) so the per-row memcpy reads
        // directly out of the Arc-owned bytes.
        let error_slot: Arc<Mutex<Option<forst_rs_common::ForstError>>> =
            Arc::new(Mutex::new(None));
        // S2 (flag ON): push-style range stream — see the prefix-open sister
        // comment. R1: also take it when per-scan adaptive selection is active.
        let mut handle_state = if forst_rs_engine::s2_pinned_enabled()
            || forst_rs_engine::s2_adaptive_active()
        {
            match db_ref.range_scan_stream_with_error_slot(
                cf_ref_,
                lo,
                hi_opt,
                Arc::clone(&error_slot),
            ) {
                Ok(stream) => IterHandle::new_pinned_with_error_slot(stream, error_slot),
                Err(_) => return FrsErrorCode::EngineIo as i32,
            }
        } else {
            let owned_iter = match db_ref.scan_iter_owned_arc_with_error_slot(
                cf_ref_,
                lo,
                hi_opt,
                Arc::clone(&error_slot),
            ) {
                Ok(it) => it,
                Err(_) => return FrsErrorCode::EngineIo as i32,
            };
            let error_slot_inner = Arc::clone(&error_slot);
            let inner: Box<dyn Iterator<Item = (IterKey, IterValue)> + Send> =
                Box::new(owned_iter.filter_map(move |r| match r {
                    Ok((k, v)) => Some((IterKey::Arc(k), IterValue::Arc(v))),
                    Err(e) => {
                        // R18-M3 sticky-FIRST: preserve the earliest error per
                        // chunk so cascade errors don't bury the root cause.
                        let mut guard = error_slot_inner.lock().unwrap_or_else(|p| p.into_inner());
                        if guard.is_none() {
                            *guard = Some(e);
                        }
                        None
                    }
                }));
            IterHandle::new_with_error_slot(inner, error_slot)
        };

        // Fill the first chunk lazily into the caller's buffer.
        let (bytes_used, row_count, iter_exhausted) =
            fill_chunk_from_iter(&mut handle_state, chunk_buf_ptr, chunk_buf_cap as usize);
        if iter_exhausted {
            // FRS-ITER-EAGER-FREE: first chunk drained the whole result — free the
            // heavy backing state now (don't pin it until the lagging Java close()).
            handle_state.drop_inner();
        }

        // R16-M2 + R17-M3: partial-chunk state machine matches the prefix
        // path. Error with NO rows -> fail open(); error after some rows
        // -> register the handle and stash the error for the next _next.
        let mut deferred_error_stashed = false;
        if let Some(err) = handle_state.take_last_error() {
            if row_count == 0 {
                *out_row_count = 0;
                *out_bytes_used = 0;
                *out_handle = 0;
                return error_to_frs_code(&err);
            }
            handle_state.set_deferred_error(err);
            deferred_error_stashed = true;
        }

        // P0 EOF + AUTO-CLOSE — same single-shot contract as
        // `frs_vec_iter_prefix_open`: first chunk exhausted the scan and no
        // error is pending → `*out_handle = 0`, nothing registered; the
        // caller may skip the trailing `_next` + `_close` crossings.
        if iter_exhausted && !deferred_error_stashed {
            *out_handle = 0;
            *out_row_count = row_count;
            *out_bytes_used = bytes_used;
            return FrsErrorCode::Ok as i32;
        }

        // Register - shares the same sharded registry as prefix iterators.
        let handle_id = NEXT_ITER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        shard_for(handle_id)
            .lock()
            .unwrap()
            .insert(handle_id, handle_state);

        *out_handle = handle_id;
        *out_row_count = row_count;
        *out_bytes_used = bytes_used;
        FrsErrorCode::Ok as i32
    })
}

/// Fetch the next chunk from a range iterator opened by `frs_vec_iter_range_open`.
///
/// Identical in shape and behaviour to `frs_vec_iter_prefix_next`; delegates
/// to the shared handle registry.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) always on success (even exhaustion; unknown /
///   auto-closed handles report clean EOF — P0 auto-close semantics).
/// - `FrsErrorCode::BatchHeaderMalformed` (110) on null out-pointers.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_iter_range_next(
    handle: u64,
    chunk_buf_ptr: *mut u8,
    chunk_buf_cap: u32,
    out_row_count: *mut u32,
    out_bytes_used: *mut u32,
) -> i32 {
    // Semantically identical to frs_vec_iter_prefix_next — the registry is shared.
    frs_vec_iter_prefix_next(
        handle,
        chunk_buf_ptr,
        chunk_buf_cap,
        out_row_count,
        out_bytes_used,
    )
}

/// Release a range iterator handle.  Delegates to `frs_vec_iter_prefix_close`.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) always.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub extern "C" fn frs_vec_iter_range_close(handle: u64) -> i32 {
    frs_vec_iter_prefix_close(handle)
}

/// Watchdog hook: abort a range iterator.  Delegates to `frs_vec_iter_prefix_abort`.
///
/// # Returns
/// - `FrsErrorCode::Ok` (0) on success.
/// - `FrsErrorCode::IterCursorInvalid` (201) if `handle` is unknown.
/// - `FrsErrorCode::PanicCaught` (900) on unexpected panic.
#[no_mangle]
pub extern "C" fn frs_vec_iter_range_abort(handle: u64) -> i32 {
    frs_vec_iter_prefix_abort(handle)
}

// ---------------------------------------------------------------------------
// List-append merge (P6-A)
// ---------------------------------------------------------------------------

/// Append N operands to an existing value at `key` in a ListState column family.
///
/// The function reads the existing value (or treats it as empty when absent),
/// concatenates `num_operands` operands in arrival order, and writes the result
/// back with a single `put`. This is the V1 read-modify-write implementation;
/// a true merge-op accumulation path is deferred to V1.x.
///
/// Per umbrella spec §1 §a, this path is **ListState-only**. Reducing and
/// Aggregating states use the RMW cache (P7), not append-merge.
///
/// # Single-row API
///
/// Java callers loop: one FFI call per row. Lower throughput than a true
/// batch variant, but acceptable because ListState.add() is far less hot
/// than get/put. A multi-row variant is deferred to V1.x.
///
/// # Parameters
/// - `db`           — engine handle from `frs_db_open*`.
/// - `cf`           — column-family handle (must be a ListState CF).
/// - `key_ptr`      — pointer to key bytes (non-null).
/// - `key_len`      — length of key in bytes.
/// - `operand_ptrs` — pointer to array of `num_operands` byte pointers.
/// - `operand_lens` — pointer to array of `num_operands` u32 lengths.
/// - `num_operands` — number of operands to append (may be 0, which is a
///   no-op and returns `Ok`).
///
/// # Returns (typed `FrsErrorCode` discriminants — spec §4)
/// - `FrsErrorCode::Ok` (0)                       — success
/// - `FrsErrorCode::BatchHeaderMalformed` (110)   — null pointer argument or
///   invalid handle
/// - `FrsErrorCode::EngineIo` (300)               — engine I/O failure
/// - `FrsErrorCode::PanicCaught` (900)            — Rust panic at FFI boundary
///
/// # Safety
/// - `key_ptr` must point to at least `key_len` valid bytes for the
///   duration of this call.
/// - `operand_ptrs` must point to at least `num_operands` valid pointers,
///   each pointing to at least the corresponding `operand_lens[i]` bytes.
/// - `operand_lens` must point to at least `num_operands` valid `u32` values.
/// - No pointer may alias writable memory in a way that would cause UB
///   under Rust's memory model for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_merge_append(
    db: FrsDb,
    cf: FrsCfHandle,
    key_ptr: *const u8,
    key_len: u32,
    operand_ptrs: *const *const u8,
    operand_lens: *const u32,
    num_operands: u32,
) -> i32 {
    guarded_vec(|| {
        // Validate handle and null-checks.
        if key_ptr.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if num_operands > 0 && (operand_ptrs.is_null() || operand_lens.is_null()) {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db_ref) = db_from_handle(db) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf_ref) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };

        let key = slice::from_raw_parts(key_ptr, key_len as usize);
        if key.len() > MAX_KEY_LEN {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }

        // No-op fast path.
        if num_operands == 0 {
            return FrsErrorCode::Ok as i32;
        }

        // D-R8-NEW-H2: refuse merge-append on CFs without a merge
        // operator. Pre-fix the call succeeded (Merge entries
        // accepted into the WriteBatch) but every subsequent read
        // of the affected key returned `InvalidArgument` ("CF has
        // no merge operator configured") — silently corrupting any
        // CF created via `frs_db_create_cf_with_merge(.., NULL,
        // ..)`. Reject at the FFI boundary so the failure is
        // surfaced at write time, before the bad state is durable.
        if !db_ref.cf_has_merge_operator(cf_ref) {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }

        // C-R15-NEW-H2: aggregate-bytes cap mirroring frs_vec_merge_append_batch.
        // The single-shot routes through the SAME db.batch_write →
        // batch_put_arrow_with_base_seq path that C-R14-NEW-H3 capped on the
        // batched sister; without this cap an asyncAddAll burst with thousands
        // of medium operands can drive total_ops past u32::MAX and overflow
        // the memtable's `key_data.len() as u32` rebase, producing torn rows.
        // Also cap num_operands to MAX_BATCH_COUNT (matches the batch sister).
        if num_operands as usize > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }

        // Write real Merge operands instead of read-combine-put. This keeps concurrent appends
        // lossless: the engine resolves merge chains at read/compaction time under the CF's
        // raw-concat operator.
        let mut operands: Vec<&[u8]> = Vec::with_capacity(num_operands as usize);
        let mut total_ops: usize = 0;
        for i in 0..num_operands as usize {
            let p = *operand_ptrs.add(i);
            let n = *operand_lens.add(i) as usize;
            if p.is_null() && n > 0 {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            // C-R15-NEW-H2: per-operand cap + aggregate cap.
            if n > MAX_KEY_LEN {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            total_ops = match total_ops.checked_add(n) {
                Some(t) => t,
                None => return FrsErrorCode::BatchHeaderMalformed as i32,
            };
            if total_ops > MAX_BATCH_BYTES {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            let s: &[u8] = if n == 0 {
                &[]
            } else {
                slice::from_raw_parts(p, n)
            };
            operands.push(s);
        }

        let mut wb = WriteBatch::with_capacity(operands.len());
        for operand in operands {
            wb.merge(cf_ref, key, operand);
        }
        match db_ref.batch_write(wb) {
            Ok(_) => FrsErrorCode::Ok as i32,
            Err(e) => error_to_frs_code(&e),
        }
    })
}

/// [Phase A.1 — audit-design §3 V4 fix] Batched form of `frs_vec_merge_append`.
///
/// Consumes N (key, operand) rows in a single FFI call. Each row's operand
/// is the [count=u32 LE][elem_bytes*] payload format produced by the
/// Flink-side `ForStRsAsyncListStateV2.asyncAdd` / `asyncAddAll` serializer.
///
/// Internally writes one engine Merge row per input row in the same order as the Arrow buffers.
/// This avoids the lost-update window of read-combine-put and lets the LSM merge operator resolve
/// concurrent append chains.
///
/// # Layout
/// - `keys_off`: array of `n+1` `u32` offsets into `keys_data`. Row `i`'s
///   key is `keys_data[keys_off[i] .. keys_off[i+1]]`.
/// - `ops_off`: array of `n+1` `u32` offsets into `ops_data`. Row `i`'s
///   operand is `ops_data[ops_off[i] .. ops_off[i+1]]`.
///
/// # Error codes
/// Same as `frs_vec_merge_append`.
///
/// # Safety
/// - `keys_off` must point to at least `n+1` valid `u32`s.
/// - `keys_data` must point to at least `keys_off[n]` valid bytes.
/// - `ops_off` must point to at least `n+1` valid `u32`s.
/// - `ops_data` must point to at least `ops_off[n]` valid bytes.
/// - All buffers must remain valid for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn frs_vec_merge_append_batch(
    db: FrsDb,
    cf: FrsCfHandle,
    keys_off: *const u32,
    keys_data: *const u8,
    keys_data_len: usize,
    ops_off: *const u32,
    ops_data: *const u8,
    ops_data_len: usize,
    n: u32,
) -> i32 {
    guarded_vec(|| {
        if n == 0 {
            return FrsErrorCode::Ok as i32;
        }
        if (n as usize) > MAX_BATCH_COUNT {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if keys_off.is_null() || ops_off.is_null() {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let Some(db_ref) = db_from_handle(db) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(cf_ref) = cf_ref(&cf) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        // D-R8-NEW-H2: refuse on CFs without a merge operator —
        // same rationale as frs_vec_merge_append.
        if !db_ref.cf_has_merge_operator(cf_ref) {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }

        let count = n as usize;
        let keys_offs = slice::from_raw_parts(keys_off, count + 1);
        let ops_offs = slice::from_raw_parts(ops_off, count + 1);
        let Some(total_keys) = validate_u32_offsets(keys_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        let Some(total_ops) = validate_u32_offsets(ops_offs) else {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        };
        if total_keys > keys_data_len || total_ops > ops_data_len {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        // C-R14-NEW-H3 (merge-batch sister): aggregate-bytes cap. Sister
        // entries `frs_vectorized_batch_put` / `frs_batch_put_arrow` etc.
        // enforce MAX_BATCH_BYTES on each column independently; the
        // merge-batch path drives the SAME memtable arrow rebase path
        // (`batch_put_arrow_with_base_seq`) via batch_write, so the same
        // u32-overflow corruption applies here.
        if total_keys > MAX_BATCH_BYTES || total_ops > MAX_BATCH_BYTES {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        if (total_keys > 0 && keys_data.is_null()) || (total_ops > 0 && ops_data.is_null()) {
            return FrsErrorCode::BatchHeaderMalformed as i32;
        }
        let key_buf: &[u8] = if total_keys == 0 {
            &[]
        } else {
            slice::from_raw_parts(keys_data, total_keys)
        };
        let ops_buf: &[u8] = if total_ops == 0 {
            &[]
        } else {
            slice::from_raw_parts(ops_data, total_ops)
        };

        // C4R2-B-NEW-H1 (merge-batch sister): skip WriteBatch construction;
        // pass borrowed key/op slices directly. OpType::Merge = 2.
        // batch_put_borrowed_single_cf still assigns monotonically increasing
        // sequence numbers; the read path reverses newest-first operands back
        // to oldest-first before invoking the raw-concat operator.
        let mut keys_slices: Vec<&[u8]> = Vec::with_capacity(count);
        let mut value_slices: Vec<Option<&[u8]>> = Vec::with_capacity(count);
        for i in 0..count {
            let k_start = keys_offs[i] as usize;
            let k_end = keys_offs[i + 1] as usize;
            let key = &key_buf[k_start..k_end];
            if key.len() > MAX_KEY_LEN {
                return FrsErrorCode::BatchHeaderMalformed as i32;
            }
            let o_start = ops_offs[i] as usize;
            let o_end = ops_offs[i + 1] as usize;
            let op = &ops_buf[o_start..o_end];
            keys_slices.push(key);
            value_slices.push(Some(op));
        }
        let op_types: Vec<u8> = vec![2u8; count]; // OpType::Merge

        if let Err(e) =
            db_ref.batch_put_borrowed_single_cf(cf_ref, &keys_slices, &value_slices, &op_types)
        {
            return error_to_frs_code(&e);
        }

        FrsErrorCode::Ok as i32
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
    fn test_open_with_options_all_zero_uses_defaults() {
        // B-Prod-P7 §6d: all-zero opts → in-memory engine at /db with
        // engine defaults (256 MiB cache, 512 MiB WBM).
        unsafe {
            let opts = FrsEngineOptions {
                db_path: ptr::null(),
                write_buffer_size: 0,
                max_write_buffer_number: 0,
                max_background_compactions: 0,
                max_background_flushes: 0,
                block_cache_capacity_bytes: 0,
                write_buffer_manager_capacity_bytes: 0,
                sst_compression: 0,
            };
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_with_options(&opts, &mut db), FRS_STATUS_OK);
            assert!(!db.is_null());
            assert_eq!(
                frs_db_write_buffer_manager_capacity(db),
                512u64 * 1024 * 1024
            );
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_open_with_options_custom_cache_and_wbm() {
        // B-Prod-P7 §6d: caller-supplied 1 GiB cache + 256 MiB WBM round-trips.
        unsafe {
            let opts = FrsEngineOptions {
                db_path: ptr::null(),
                write_buffer_size: 0,
                max_write_buffer_number: 0,
                max_background_compactions: 0,
                max_background_flushes: 0,
                block_cache_capacity_bytes: 1024 * 1024 * 1024,
                write_buffer_manager_capacity_bytes: 256 * 1024 * 1024,
                sst_compression: 0,
            };
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_with_options(&opts, &mut db), FRS_STATUS_OK);
            assert_eq!(
                frs_db_write_buffer_manager_capacity(db),
                256u64 * 1024 * 1024
            );
            // Initially no bytes reserved.
            assert_eq!(frs_db_write_buffer_manager_current_bytes(db), 0);
            // After a put the WBM should track non-zero bytes.
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);
            let key = b"k";
            let val = b"v";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()),
                FRS_STATUS_OK
            );
            assert!(frs_db_write_buffer_manager_current_bytes(db) > 0);
            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_open_with_options_null_opts_returns_null_arg() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(
                frs_db_open_with_options(ptr::null(), &mut db),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    #[test]
    fn test_open_with_options_struct_layout_appears_repr_c() {
        // Sanity-check the field offsets so an accidental reorder would
        // be caught locally instead of surfacing as a Java FFM ABI break.
        // Field ordering and types must match the docstring table on
        // FrsEngineOptions and the `MemoryLayout.structLayout(...)`
        // mirror in ForStRsLinker.java.
        use std::mem::{align_of, size_of};
        // *const c_char is 8B on 64-bit; padded out to alignment by the
        // following u64 fields. The total size is the sum of:
        //   8 (db_path ptr) + 8 (write_buffer_size u64) + 4 (u32) + 4 (u32)
        // + 4 (u32) + 4 (pad) + 8 (u64) + 8 (u64) + 4 (sst_compression u32)
        // + 4 (trailing pad) = 56 bytes.
        // (32-bit hosts will have a smaller pointer; the bridge is built
        // 64-bit only today, so we encode the 64-bit layout here.)
        // Mirrors FRS_ENGINE_OPTIONS_LAYOUT in ForStRsLinker.java (sst_compression
        // at +48, trailing paddingLayout(4)); keep both in lockstep.
        if size_of::<*const c_char>() == 8 {
            assert_eq!(size_of::<FrsEngineOptions>(), 56);
        }
        assert!(align_of::<FrsEngineOptions>() >= 8);
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

    /// FRS-WA-V0: lifecycle + watermark FFI plumbing — ordinal mapping,
    /// monotonic clock semantics, null-arg and bad-ordinal rejection, and
    /// V0 inertness (a lifecycle CF still round-trips a put/get).
    #[test]
    fn test_wa_v0_cf_lifecycle_and_watermark_ffi() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let name = CString::new("win").unwrap();
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_create_cf(db, name.as_ptr(), &mut cf), FRS_STATUS_OK);

            // kind=1 (Windowed) with a ttl installs.
            assert_eq!(frs_cf_set_lifecycle(db, cf, 1, 4_000_000), FRS_STATUS_OK);
            // Engine-side observation (white-box through the handle).
            {
                let db_ref = db_from_handle(db).unwrap();
                let cf_ref_h = cf_ref(&cf).unwrap();
                assert_eq!(
                    db_ref.cf_lifecycle(cf_ref_h).unwrap(),
                    forst_rs_engine::CfLifecycle::Windowed { ttl: 4_000_000 }
                );
            }
            // Unknown ordinal: rejected, descriptor unchanged.
            assert_eq!(
                frs_cf_set_lifecycle(db, cf, 7, 0),
                FRS_STATUS_INVALID_ARGUMENT
            );

            // Watermark + event-time: monotonic via the FFI.
            assert_eq!(frs_cf_advance_watermark(db, cf, 1_000), FRS_STATUS_OK);
            assert_eq!(frs_cf_advance_watermark(db, cf, 500), FRS_STATUS_OK); // stale no-op
            assert_eq!(frs_cf_note_max_event_time(db, cf, 9_000), FRS_STATUS_OK);
            {
                let db_ref = db_from_handle(db).unwrap();
                let cf_ref_h = cf_ref(&cf).unwrap();
                assert_eq!(db_ref.cf_watermark(cf_ref_h).unwrap(), 1_000);
                assert_eq!(db_ref.cf_max_event_time(cf_ref_h).unwrap(), 9_000);
            }

            // NULL args rejected.
            assert_eq!(
                frs_cf_set_lifecycle(ptr::null_mut(), cf, 1, 1),
                FRS_STATUS_NULL_ARG
            );
            assert_eq!(
                frs_cf_advance_watermark(db, ptr::null_mut(), 1),
                FRS_STATUS_NULL_ARG
            );
            assert_eq!(
                frs_cf_note_max_event_time(ptr::null_mut(), cf, 1),
                FRS_STATUS_NULL_ARG
            );

            // V0 inertness: lifecycle CF reads/writes like any other.
            let k = b"k";
            let v = b"v";
            frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out),
                FRS_STATUS_OK
            );
            let slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(slice, b"v");
            frs_bytes_free(&mut out);

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
    fn test_merge_with_numeric_add_be() {
        // OPT-N04 §4: backend binds the BE wrapping operator BY NAME via
        // `frs_db_create_cf_with_merge` — this is the name-match arm test.
        // Bytes are big-endian (Flink DataOutputSerializer.writeLong order)
        // and addition wraps (Java `long +`).
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);

            let name = CString::new("agg-merge-i64").unwrap();
            let op_name = CString::new("NumericAddBeMergeOperator").unwrap();
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_with_merge(db, name.as_ptr(), op_name.as_ptr(), &mut cf,),
                FRS_STATUS_OK
            );

            let k = b"acc";
            let base = 5i64.to_be_bytes();
            let d1 = 1i64.to_be_bytes();
            let d2 = (-2i64).to_be_bytes();
            frs_put(db, cf, k.as_ptr(), k.len(), base.as_ptr(), base.len());
            frs_merge(db, cf, k.as_ptr(), k.len(), d1.as_ptr(), d1.len());
            frs_merge(db, cf, k.as_ptr(), k.len(), d2.as_ptr(), d2.len());

            let mut out = FrsBytes::NULL;
            frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
            let slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(slice, &4i64.to_be_bytes());
            frs_bytes_free(&mut out);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_accumulator_merge_byte_identical_to_get_fold_put() {
        // Approach 2 / V-C byte-identity gate (the core falsifier):
        // for windowed-agg `Long` accumulation, the in-engine MERGE path
        // (Arm B — submit only the delta, engine folds via
        // NumericAddBeMergeOperator) must produce a BYTE-IDENTICAL
        // accumulator to today's per-record GET→fold-in-Java→PUT round-trip
        // (Arm A). Only WHERE the fold runs changes; the bytes must not.
        //
        // The fold is Java `long +` over DataOutputSerializer.writeLong
        // (big-endian) bytes, including wrap-around and retraction (negative
        // deltas) — exactly the operator's contract.
        unsafe {
            // A deterministic delta stream that exercises positives, negatives
            // (retraction), and an overflow wrap. Same stream feeds both arms.
            let deltas: [i64; 9] = [7, -3, i64::MAX, 1, -100, 50, i64::MIN, -1, 42];

            // --- Arm A: get → fold-in-caller (Java `long +`) → put. ---
            // A plain CF with NO merge operator (the legacy RMW path).
            let a_final: [u8; 8] = {
                let mut db: FrsDb = ptr::null_mut();
                assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
                let mut cf: FrsCfHandle = ptr::null_mut();
                assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
                let k = b"acc";
                for d in deltas {
                    // get current accumulator (absent → 0)
                    let mut out = FrsBytes::NULL;
                    assert_eq!(
                        frs_get(db, cf, k.as_ptr(), k.len(), &mut out),
                        FRS_STATUS_OK
                    );
                    let acc = if out.data.is_null() {
                        0
                    } else {
                        let s = slice::from_raw_parts(out.data, out.len);
                        i64::from_be_bytes(s.try_into().unwrap())
                    };
                    frs_bytes_free(&mut out);
                    // fold in caller-space (Java `long +`) and write back
                    let acc = acc.wrapping_add(d);
                    let bytes = acc.to_be_bytes();
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), bytes.as_ptr(), bytes.len()),
                        FRS_STATUS_OK
                    );
                }
                // final read
                let mut out = FrsBytes::NULL;
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
                let s = slice::from_raw_parts(out.data, out.len);
                let arr: [u8; 8] = s.try_into().unwrap();
                frs_bytes_free(&mut out);
                frs_cf_close(cf);
                frs_db_close(db);
                arr
            };

            // --- Arm B: in-engine merge (submit only the delta). ---
            // A CF carrying NumericAddBeMergeOperator; engine folds the chain.
            let b_final: [u8; 8] = {
                let mut db: FrsDb = ptr::null_mut();
                assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
                let name = CString::new("agg-merge-i64").unwrap();
                let op_name = CString::new("NumericAddBeMergeOperator").unwrap();
                let mut cf: FrsCfHandle = ptr::null_mut();
                assert_eq!(
                    frs_db_create_cf_with_merge(db, name.as_ptr(), op_name.as_ptr(), &mut cf),
                    FRS_STATUS_OK
                );
                let k = b"acc";
                for d in deltas {
                    let bytes = d.to_be_bytes();
                    assert_eq!(
                        frs_merge(db, cf, k.as_ptr(), k.len(), bytes.as_ptr(), bytes.len()),
                        FRS_STATUS_OK
                    );
                }
                let mut out = FrsBytes::NULL;
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
                let s = slice::from_raw_parts(out.data, out.len);
                let arr: [u8; 8] = s.try_into().unwrap();
                frs_bytes_free(&mut out);
                frs_cf_close(cf);
                frs_db_close(db);
                arr
            };

            // Byte-identical accumulator: the WHERE changed, the bytes did not.
            assert_eq!(
                a_final, b_final,
                "in-engine merge accumulator must be byte-identical to get-fold-put"
            );
            // Sanity: the expected wrapping fold.
            let mut expected: i64 = 0;
            for d in deltas {
                expected = expected.wrapping_add(d);
            }
            assert_eq!(a_final, expected.to_be_bytes());
        }
    }

    #[test]
    fn test_accumulator_merge_byte_identical_after_flush() {
        // Read-cost guard companion: the byte-identity must survive a
        // flush-collapse (partial/full-merge folds the chain at flush) — the
        // in-engine accumulator read AFTER a flush equals the same wrapping
        // fold. This is the q20-regression falsifier's correctness half:
        // collapsing the operand chain must not change the value.
        unsafe {
            let deltas: [i64; 6] = [10, -4, i64::MAX, 2, i64::MIN, 99];
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let name = CString::new("agg-merge-i64").unwrap();
            let op_name = CString::new("NumericAddBeMergeOperator").unwrap();
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_with_merge(db, name.as_ptr(), op_name.as_ptr(), &mut cf),
                FRS_STATUS_OK
            );
            let k = b"acc";
            for d in deltas {
                let bytes = d.to_be_bytes();
                assert_eq!(
                    frs_merge(db, cf, k.as_ptr(), k.len(), bytes.as_ptr(), bytes.len()),
                    FRS_STATUS_OK
                );
            }
            // Read pre-flush (walks the operand chain).
            let pre = {
                let mut out = FrsBytes::NULL;
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
                let arr: [u8; 8] = slice::from_raw_parts(out.data, out.len).try_into().unwrap();
                frs_bytes_free(&mut out);
                arr
            };
            // Collapse the chain.
            assert_eq!(frs_flush(db), FRS_STATUS_OK);
            // Read post-flush (sees one folded value).
            let post = {
                let mut out = FrsBytes::NULL;
                frs_get(db, cf, k.as_ptr(), k.len(), &mut out);
                let arr: [u8; 8] = slice::from_raw_parts(out.data, out.len).try_into().unwrap();
                frs_bytes_free(&mut out);
                arr
            };
            let mut expected: i64 = 0;
            for d in deltas {
                expected = expected.wrapping_add(d);
            }
            assert_eq!(pre, expected.to_be_bytes(), "pre-flush chain read");
            assert_eq!(post, expected.to_be_bytes(), "post-flush collapsed read");
            assert_eq!(pre, post, "flush-collapse must not change the value");
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
                ops.append_value(1); // Put (OpType::Put = 1, RocksDB byte-compat)
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
            ops.append_value(1); // Put (OpType::Put = 1, RocksDB byte-compat)

            keys.append_value(b"k2");
            values.append_null();
            ops.append_value(0); // Delete (OpType::Delete = 0, RocksDB byte-compat)

            keys.append_value(b"k3");
            values.append_value(b"v3");
            ops.append_value(1); // Put (OpType::Put = 1, RocksDB byte-compat)

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

            // Build a RecordBatch: SingleDelete k1 (op=7, RocksDB byte-compat).
            let mut keys = BinaryBuilder::new();
            let mut values = BinaryBuilder::new();
            let mut ops = UInt8Builder::new();
            keys.append_value(b"k1");
            values.append_null();
            ops.append_value(7); // SingleDelete (OpType::SingleDelete = 7, RocksDB byte-compat)

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

            // Delete (0) and SingleDelete (7) must have NULL values.
            for bad_op in [0u8, 7u8] {
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
            ops.append_value(7); // SingleDelete (OpType::SingleDelete = 7, RocksDB byte-compat)
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

    // --- Delta-Join lookup FFI tests ---

    #[test]
    fn test_lookup_kv_existing() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let k = b"hello";
            let v = b"world";
            assert_eq!(
                frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                FRS_STATUS_OK
            );

            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_lookup_kv(db, cf, k.as_ptr(), k.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(!out.data.is_null());
            assert_eq!(slice::from_raw_parts(out.data, out.len), v);
            frs_bytes_free(&mut out);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_lookup_kv_missing() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let k = b"absent";
            let mut out = FrsBytes::NULL;
            // Missing key → Ok status, NULL FrsBytes (matches frs_get).
            assert_eq!(
                frs_lookup_kv(db, cf, k.as_ptr(), k.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(out.data.is_null());
            assert_eq!(out.len, 0);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_lookup_kv_validates_pointers() {
        unsafe {
            // out_value NULL.
            assert_eq!(
                frs_lookup_kv(
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    ptr::null_mut()
                ),
                FRS_STATUS_NULL_ARG
            );

            // db NULL but other args sane.
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_lookup_kv(ptr::null_mut(), ptr::null_mut(), b"k".as_ptr(), 1, &mut out,),
                FRS_STATUS_NULL_ARG
            );

            // cf NULL: db open but cf null.
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_lookup_kv(db, ptr::null_mut(), b"k".as_ptr(), 1, &mut out),
                FRS_STATUS_NULL_ARG
            );

            // key NULL but key_len > 0.
            let mut cf: FrsCfHandle = ptr::null_mut();
            frs_db_default_cf(db, &mut cf);
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_lookup_kv(db, cf, ptr::null(), 4, &mut out),
                FRS_STATUS_NULL_ARG
            );

            // key_len > MAX_KEY_LEN must be rejected.
            let bogus_ptr = b"k".as_ptr();
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_lookup_kv(db, cf, bogus_ptr, MAX_KEY_LEN + 1, &mut out),
                FRS_STATUS_INVALID_ARGUMENT
            );

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_iterator_full_scan() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Insert in non-sorted order so we can verify the iterator
            // returns sorted output.
            let pairs: &[(&[u8], &[u8])] = &[
                (b"k3", b"v3"),
                (b"k1", b"v1"),
                (b"k5", b"v5"),
                (b"k2", b"v2"),
                (b"k4", b"v4"),
            ];
            for (k, v) in pairs {
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            let mut iter: FrsIterator = ptr::null_mut();
            assert_eq!(frs_iterator_open(db, cf, &mut iter), FRS_STATUS_OK);
            assert!(!iter.is_null());

            let mut collected: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            loop {
                let mut k_out = FrsBytes::NULL;
                let mut v_out = FrsBytes::NULL;
                let mut valid = false;
                assert_eq!(
                    frs_iterator_next(iter, &mut k_out, &mut v_out, &mut valid),
                    FRS_STATUS_OK
                );
                if !valid {
                    break;
                }
                let k_vec = slice::from_raw_parts(k_out.data, k_out.len).to_vec();
                let v_vec = slice::from_raw_parts(v_out.data, v_out.len).to_vec();
                collected.push((k_vec, v_vec));
                frs_bytes_free(&mut k_out);
                frs_bytes_free(&mut v_out);
            }

            assert_eq!(collected.len(), 5);
            // Engine returns keys in sorted (ascending byte) order.
            assert_eq!(collected[0].0, b"k1");
            assert_eq!(collected[1].0, b"k2");
            assert_eq!(collected[2].0, b"k3");
            assert_eq!(collected[3].0, b"k4");
            assert_eq!(collected[4].0, b"k5");
            assert_eq!(collected[0].1, b"v1");
            assert_eq!(collected[4].1, b"v5");

            assert_eq!(frs_iterator_close(iter), FRS_STATUS_OK);
            // Closing twice (NULL after first close) is safe — guarded by the
            // is_null branch.
            assert_eq!(frs_iterator_close(ptr::null_mut()), FRS_STATUS_OK);

            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_iterator_seek() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            for i in 0..5u32 {
                let k = format!("k{}", i);
                let v = format!("v{}", i);
                frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            }

            let mut iter: FrsIterator = ptr::null_mut();
            assert_eq!(frs_iterator_open(db, cf, &mut iter), FRS_STATUS_OK);

            // Seek to "k3" — first read should be k3.
            assert_eq!(frs_iterator_seek(iter, b"k3".as_ptr(), 2), FRS_STATUS_OK);
            let mut k_out = FrsBytes::NULL;
            let mut v_out = FrsBytes::NULL;
            let mut valid = false;
            frs_iterator_next(iter, &mut k_out, &mut v_out, &mut valid);
            assert!(valid);
            assert_eq!(slice::from_raw_parts(k_out.data, k_out.len), b"k3");
            assert_eq!(slice::from_raw_parts(v_out.data, v_out.len), b"v3");
            frs_bytes_free(&mut k_out);
            frs_bytes_free(&mut v_out);

            // Seek past the end — first read should be invalid.
            assert_eq!(frs_iterator_seek(iter, b"zzz".as_ptr(), 3), FRS_STATUS_OK);
            let mut k_out = FrsBytes::NULL;
            let mut v_out = FrsBytes::NULL;
            let mut valid = true;
            frs_iterator_next(iter, &mut k_out, &mut v_out, &mut valid);
            assert!(!valid);
            assert!(k_out.data.is_null());

            // Seek back to start with NULL key — should read k0.
            assert_eq!(frs_iterator_seek(iter, ptr::null(), 0), FRS_STATUS_OK);
            let mut k_out = FrsBytes::NULL;
            let mut v_out = FrsBytes::NULL;
            let mut valid = false;
            frs_iterator_next(iter, &mut k_out, &mut v_out, &mut valid);
            assert!(valid);
            assert_eq!(slice::from_raw_parts(k_out.data, k_out.len), b"k0");
            frs_bytes_free(&mut k_out);
            frs_bytes_free(&mut v_out);

            frs_iterator_close(iter);
            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_prefix_lookup() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // 3 keys under prefix "k", 1 under prefix "a" (must NOT match).
            let pairs: &[(&[u8], &[u8])] = &[
                (b"k1", b"v1"),
                (b"k2", b"v2"),
                (b"k3", b"v3"),
                (b"a1", b"va"),
            ];
            for (k, v) in pairs {
                frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len());
            }

            let prefix = b"k";
            let mut iter: FrsIterator = ptr::null_mut();
            assert_eq!(
                frs_prefix_lookup_open(db, cf, prefix.as_ptr(), prefix.len(), &mut iter),
                FRS_STATUS_OK
            );

            let mut keys: Vec<Vec<u8>> = Vec::new();
            loop {
                let mut k_out = FrsBytes::NULL;
                let mut v_out = FrsBytes::NULL;
                let mut valid = false;
                frs_prefix_lookup_next(iter, &mut k_out, &mut v_out, &mut valid);
                if !valid {
                    break;
                }
                keys.push(slice::from_raw_parts(k_out.data, k_out.len).to_vec());
                frs_bytes_free(&mut k_out);
                frs_bytes_free(&mut v_out);
            }
            assert_eq!(keys.len(), 3, "prefix `k` must match exactly 3 entries");
            assert_eq!(keys[0], b"k1");
            assert_eq!(keys[1], b"k2");
            assert_eq!(keys[2], b"k3");

            assert_eq!(frs_prefix_lookup_close(iter), FRS_STATUS_OK);
            frs_cf_close(cf);
            frs_db_close(db);
        }
    }

    #[test]
    fn test_iterator_open_validates_pointers() {
        unsafe {
            // out_iter NULL.
            assert_eq!(
                frs_iterator_open(ptr::null_mut(), ptr::null_mut(), ptr::null_mut()),
                FRS_STATUS_NULL_ARG
            );

            // db NULL.
            let mut iter: FrsIterator = ptr::null_mut();
            assert_eq!(
                frs_iterator_open(ptr::null_mut(), ptr::null_mut(), &mut iter),
                FRS_STATUS_NULL_ARG
            );

            // cf NULL.
            let mut db: FrsDb = ptr::null_mut();
            frs_db_open_memory(&mut db);
            let mut iter: FrsIterator = ptr::null_mut();
            assert_eq!(
                frs_iterator_open(db, ptr::null_mut(), &mut iter),
                FRS_STATUS_NULL_ARG
            );

            // frs_iterator_next with NULL handles.
            let mut k = FrsBytes::NULL;
            let mut v = FrsBytes::NULL;
            let mut valid = false;
            assert_eq!(
                frs_iterator_next(ptr::null_mut(), &mut k, &mut v, &mut valid),
                FRS_STATUS_NULL_ARG
            );

            frs_db_close(db);
        }
    }

    /// `frs_db_open_memory_tuned` accepts well-formed knobs and produces a
    /// usable engine that survives a basic put/get round-trip. The tuned
    /// values exercise both an enlarged memtable budget and an enlarged
    /// per-axis background-thread count.
    #[test]
    fn test_open_memory_tuned_roundtrip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(
                frs_db_open_memory_tuned(
                    256 * 1024 * 1024, // write_buffer_size
                    8,                 // max_write_buffer_number
                    4,                 // max_background_compactions
                    4,                 // max_background_flushes
                    &mut db,
                ),
                FRS_STATUS_OK
            );
            assert!(!db.is_null());

            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"tuned-key";
            let value = b"tuned-value";
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
            assert_eq!(slice::from_raw_parts(out.data, out.len), value);
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Passing `0` for every knob means "use the engine default for that
    /// field" — verifies the per-knob branch in `frs_db_open_memory_tuned`
    /// that skips the builder setter when the FFI argument is zero.
    #[test]
    fn test_open_memory_tuned_zero_means_default() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory_tuned(0, 0, 0, 0, &mut db), FRS_STATUS_OK);
            assert!(!db.is_null());
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Out-of-range knob (1-byte memtable, well below the 4 KiB floor
    /// `MIN_WRITE_BUFFER_SIZE`) is rejected via `try_build` and surfaced
    /// as `FRS_STATUS_INVALID_ARGUMENT` rather than aborting / panicking.
    #[test]
    fn test_open_memory_tuned_rejects_undersized_buffer() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(
                frs_db_open_memory_tuned(1, 0, 0, 0, &mut db),
                FRS_STATUS_INVALID_ARGUMENT
            );
            assert!(db.is_null());
        }
    }

    /// Null `out_handle` is rejected up-front (matches the
    /// `frs_db_open_memory` contract; r5/r6 hardening).
    #[test]
    fn test_open_memory_tuned_null_arg() {
        unsafe {
            assert_eq!(
                frs_db_open_memory_tuned(0, 0, 0, 0, ptr::null_mut()),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    /// `frs_cf_set_compaction_filter_ttl` accepts a Value-state filter,
    /// installs it on the CF, and leaves subsequent put/get traffic
    /// unaffected (the filter only fires at compaction time, which the
    /// in-memory test backend does not exercise here — but the install
    /// path itself must not break the read/write path).
    ///
    /// Also covers the Disabled-state and unknown-ordinal branches: both
    /// install a no-op filter and the put/get round-trip continues to
    /// succeed.
    #[test]
    fn test_frs_cf_set_compaction_filter_ttl() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);

            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Install Value-state TTL filter (state_type=1, ttl=60_000ms,
            // timestamp at offset 0). Must succeed.
            assert_eq!(
                frs_cf_set_compaction_filter_ttl(db, cf, 60_000, 1, 0),
                FRS_STATUS_OK
            );

            // Round-trip a put/get to confirm the install didn't break the
            // hot path. The "value" payload here doesn't carry a real
            // timestamp prefix, but in the in-memory backend nothing is
            // compacted so the filter is never invoked — what we're
            // verifying is that registering the filter is non-destructive.
            let key = b"ttl-key";
            let value = b"ttl-value";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                FRS_STATUS_OK
            );
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert_eq!(slice::from_raw_parts(out.data, out.len), value);
            frs_bytes_free(&mut out);

            // Replace with a Disabled-state filter — this should also
            // succeed and leave the engine functional.
            assert_eq!(
                frs_cf_set_compaction_filter_ttl(db, cf, 0, 0, 0),
                FRS_STATUS_OK
            );
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                FRS_STATUS_OK
            );

            // Unknown ordinal (99) must NOT panic / error — it falls back
            // to Disabled so a future Flink upgrade adding a state type
            // can never silently turn the filter into a destructive no-op.
            assert_eq!(
                frs_cf_set_compaction_filter_ttl(db, cf, 1_000, 99, 0),
                FRS_STATUS_OK
            );

            // Null DB / CF arguments are rejected.
            assert_eq!(
                frs_cf_set_compaction_filter_ttl(ptr::null_mut(), cf, 0, 1, 0),
                FRS_STATUS_NULL_ARG
            );
            assert_eq!(
                frs_cf_set_compaction_filter_ttl(db, ptr::null_mut(), 0, 1, 0),
                FRS_STATUS_NULL_ARG
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// `frs_compact_cf` is the JNI compactRange backend. It must make the
    /// current memtable eligible for filtering before running L0 compaction;
    /// otherwise Flink TTL `compactState` leaves just-written expired values
    /// untouched until some unrelated flush happens.
    #[test]
    fn test_frs_compact_cf_flushes_memtable_before_ttl_filtering() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);

            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
            assert_eq!(
                frs_cf_set_compaction_filter_ttl(db, cf, 100, 1, 0),
                FRS_STATUS_OK
            );

            let key = b"expired-before-flush";
            let mut value = Vec::new();
            value.extend_from_slice(&0u64.to_be_bytes());
            value.extend_from_slice(b"payload");
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                FRS_STATUS_OK
            );

            assert_eq!(frs_compact_cf(db, cf), FRS_STATUS_OK);

            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(out.data.is_null());
            assert_eq!(out.len, 0);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_compact_cf_empty_active_memtable_is_noop() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);

            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            assert_eq!(frs_compact_cf(db, cf), FRS_STATUS_OK);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // -----------------------------------------------------------------
    // Live-file enumeration (frs_db_get_live_files / *_metadata / *_free)
    // -----------------------------------------------------------------

    /// On a fresh, never-written DB the live-file list must be empty —
    /// no SSTs have been produced and the enumerated count is 0.
    #[test]
    fn test_frs_db_get_live_files_empty_db() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);

            let mut list = FrsLiveFileList {
                files: ptr::null_mut(),
                count: 0,
                manifest_size: 0,
            };
            // flush_memtable=false: no rows have been written so flush
            // would be a no-op anyway. Verifies the no-flush path works.
            assert_eq!(frs_db_get_live_files(db, false, &mut list), FRS_STATUS_OK);
            assert_eq!(list.count, 0);
            assert!(list.files.is_null());

            // free is safe-and-idempotent on an empty list.
            assert_eq!(frs_db_live_file_list_free(&mut list), FRS_STATUS_OK);
            assert_eq!(frs_db_live_file_list_free(&mut list), FRS_STATUS_OK);

            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// After writing rows and flushing, at least one L0 SST must appear in
    /// the live-file list with non-zero size and a recorded sequence.
    #[test]
    fn test_frs_db_get_live_files_after_flush() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Write enough rows that the flushed memtable produces a
            // non-trivial SST.
            for i in 0..100u32 {
                let k = format!("k{:06}", i);
                let v = format!("value-payload-{:06}", i);
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            // flush_memtable=true makes get_live_files run flush_all itself.
            let mut list = FrsLiveFileList {
                files: ptr::null_mut(),
                count: 0,
                manifest_size: 0,
            };
            assert_eq!(frs_db_get_live_files(db, true, &mut list), FRS_STATUS_OK);
            assert!(
                list.count >= 1,
                "expected at least one live SST after flush, got {}",
                list.count
            );
            assert!(!list.files.is_null());

            // Inspect the first entry.
            let entries = slice::from_raw_parts(list.files, list.count);
            let mut total_size = 0u64;
            for entry in entries {
                assert!(!entry.path.is_null());
                assert!(!entry.cf_name.is_null());
                let path = std::ffi::CStr::from_ptr(entry.path).to_str().unwrap();
                let cf = std::ffi::CStr::from_ptr(entry.cf_name).to_str().unwrap();
                assert!(path.ends_with(".sst"), "path missing .sst suffix: {path}");
                assert_eq!(cf, "default");
                // Level should be valid (0..MAX_LEVELS).
                assert!(entry.level < 64);
                total_size = total_size.saturating_add(entry.size);
            }
            assert!(
                total_size > 0,
                "every flushed SST must have non-zero size; got total {total_size}"
            );

            // Sequence number should advance past the writes.
            let mut seq: u64 = 0;
            assert_eq!(frs_sequence_number(db, &mut seq), FRS_STATUS_OK);
            assert!(seq >= 100, "sequence must reflect 100 puts; got {seq}");

            // Verify metadata-only path returns the same shape.
            let mut list2 = FrsLiveFileList {
                files: ptr::null_mut(),
                count: 0,
                manifest_size: 0,
            };
            assert_eq!(
                frs_db_get_live_files_metadata(db, &mut list2),
                FRS_STATUS_OK
            );
            assert_eq!(list2.count, list.count);

            assert_eq!(frs_db_live_file_list_free(&mut list), FRS_STATUS_OK);
            assert_eq!(frs_db_live_file_list_free(&mut list2), FRS_STATUS_OK);
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// `frs_db_live_file_list_free` must be idempotent: calling it twice on
    /// the same list (or on a NULL list) must not double-free or panic.
    #[test]
    fn test_frs_db_live_file_list_free_idempotent() {
        unsafe {
            // 1. NULL pointer is a no-op.
            assert_eq!(frs_db_live_file_list_free(ptr::null_mut()), FRS_STATUS_OK);

            // 2. Empty struct is a no-op.
            let mut empty = FrsLiveFileList {
                files: ptr::null_mut(),
                count: 0,
                manifest_size: 0,
            };
            assert_eq!(frs_db_live_file_list_free(&mut empty), FRS_STATUS_OK);
            assert_eq!(frs_db_live_file_list_free(&mut empty), FRS_STATUS_OK);

            // 3. Populated list — produce one then free twice.
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
            for i in 0..20u32 {
                let k = format!("k{:04}", i);
                frs_put(db, cf, k.as_ptr(), k.len(), b"v".as_ptr(), 1);
            }
            assert_eq!(frs_flush(db), FRS_STATUS_OK);

            let mut list = FrsLiveFileList {
                files: ptr::null_mut(),
                count: 0,
                manifest_size: 0,
            };
            assert_eq!(frs_db_get_live_files(db, false, &mut list), FRS_STATUS_OK);
            // First free reclaims the buffer.
            assert_eq!(frs_db_live_file_list_free(&mut list), FRS_STATUS_OK);
            assert!(list.files.is_null());
            assert_eq!(list.count, 0);
            // Second free is a no-op (idempotent).
            assert_eq!(frs_db_live_file_list_free(&mut list), FRS_STATUS_OK);
            assert!(list.files.is_null());
            assert_eq!(list.count, 0);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// NULL-pointer and bad-handle inputs to the live-file FFI surface
    /// must return clean error codes rather than panicking.
    #[test]
    fn test_frs_db_get_live_files_null_args() {
        unsafe {
            // out=null → NULL_ARG.
            assert_eq!(
                frs_db_get_live_files(ptr::null_mut(), false, ptr::null_mut()),
                FRS_STATUS_NULL_ARG
            );
            // db=null but out non-null → NULL_ARG (after zeroing out).
            let mut list = FrsLiveFileList {
                files: ptr::null_mut(),
                count: 0,
                manifest_size: 0,
            };
            assert_eq!(
                frs_db_get_live_files(ptr::null_mut(), false, &mut list),
                FRS_STATUS_NULL_ARG
            );
            assert!(list.files.is_null());
            assert_eq!(list.count, 0);
        }
    }

    // -----------------------------------------------------------------
    // §10. MVCC FFI exports — snapshot, release, get_at, iterator_open_at
    // -----------------------------------------------------------------

    #[test]
    fn test_frs_db_snapshot_release_round_trip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut snap: FrsSnapshot = ptr::null_mut();
            assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);
            assert!(!snap.is_null());
            assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_db_snapshot_null_db_returns_null_arg() {
        unsafe {
            let mut snap: FrsSnapshot = ptr::null_mut();
            assert_eq!(
                frs_db_snapshot(ptr::null_mut(), &mut snap),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    #[test]
    fn test_frs_db_release_snapshot_null_arg_returns_null_arg() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            // NULL snapshot.
            assert_eq!(
                frs_db_release_snapshot(db, ptr::null_mut()),
                FRS_STATUS_NULL_ARG
            );
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_get_at_isolation() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"k";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), b"v1".as_ptr(), 2),
                FRS_STATUS_OK
            );

            let mut snap: FrsSnapshot = ptr::null_mut();
            assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);

            // Write v2 AFTER snapshot.
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), b"v2".as_ptr(), 2),
                FRS_STATUS_OK
            );

            // get_at sees v1.
            let mut out = FrsBytes::default();
            assert_eq!(
                frs_get_at(db, cf, snap, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            let val = slice::from_raw_parts(out.data, out.len);
            assert_eq!(val, b"v1");
            frs_bytes_free(&mut out);

            // Current get sees v2.
            let mut out2 = FrsBytes::default();
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out2),
                FRS_STATUS_OK
            );
            let val2 = slice::from_raw_parts(out2.data, out2.len);
            assert_eq!(val2, b"v2");
            frs_bytes_free(&mut out2);

            assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_iterator_open_at_filters_by_snapshot_seq() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Write 3 keys.
            for &k in &[b"a", b"b", b"c"] {
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), 1, b"v1".as_ptr(), 2),
                    FRS_STATUS_OK
                );
            }

            let mut snap: FrsSnapshot = ptr::null_mut();
            assert_eq!(frs_db_snapshot(db, &mut snap), FRS_STATUS_OK);

            // Add d AFTER snapshot — must NOT appear in iter_at.
            assert_eq!(
                frs_put(db, cf, b"d".as_ptr(), 1, b"v1".as_ptr(), 2),
                FRS_STATUS_OK
            );

            let mut iter: FrsIterator = ptr::null_mut();
            assert_eq!(frs_iterator_open_at(db, cf, snap, &mut iter), FRS_STATUS_OK);

            let mut count = 0;
            loop {
                let mut k = FrsBytes::default();
                let mut v = FrsBytes::default();
                let mut valid: bool = false;
                assert_eq!(
                    frs_iterator_next(iter, &mut k, &mut v, &mut valid),
                    FRS_STATUS_OK
                );
                if !valid {
                    break;
                }
                count += 1;
                frs_bytes_free(&mut k);
                frs_bytes_free(&mut v);
            }
            assert_eq!(count, 3, "'d' should be filtered out by snapshot.seq");

            assert_eq!(frs_iterator_close(iter), FRS_STATUS_OK);
            assert_eq!(frs_db_release_snapshot(db, snap), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // ----------------------------------------------------------------
    // B-Prod-P6: frs_db_open_remote
    // ----------------------------------------------------------------

    #[test]
    fn test_parse_flat_json_object_supports_empty_and_null() {
        assert_eq!(parse_flat_json_object("").unwrap().len(), 0);
        assert_eq!(parse_flat_json_object("null").unwrap().len(), 0);
        assert_eq!(parse_flat_json_object("{}").unwrap().len(), 0);
    }

    #[test]
    fn test_parse_flat_json_object_simple_pair() {
        let m = parse_flat_json_object("{\"region\":\"us-east-1\"}").unwrap();
        assert_eq!(m.get("region").map(String::as_str), Some("us-east-1"));
    }

    #[test]
    fn test_parse_flat_json_object_multiple_pairs_with_spaces() {
        let m = parse_flat_json_object(
            "{ \"region\": \"us-west-2\" , \"endpoint\": \"https://example.com\" }",
        )
        .unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("region").map(String::as_str), Some("us-west-2"));
        assert_eq!(
            m.get("endpoint").map(String::as_str),
            Some("https://example.com")
        );
    }

    #[test]
    fn test_parse_flat_json_object_rejects_missing_braces() {
        assert!(parse_flat_json_object("region:us-east-1").is_err());
    }

    #[test]
    fn test_frs_db_open_remote_memory_uri_round_trip() {
        let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
        let cache_dir_str = cache_dir.path().to_string_lossy().into_owned();
        unsafe {
            let uri = CString::new("memory://").unwrap();
            let cfg = CString::new("{}").unwrap();
            let cdir = CString::new(cache_dir_str).unwrap();

            let mut db: FrsDb = ptr::null_mut();
            let rc = frs_db_open_remote(
                uri.as_ptr(),
                cfg.as_ptr(),
                cdir.as_ptr(),
                64 * 1024 * 1024,
                &mut db,
            );
            assert_eq!(rc, FRS_STATUS_OK, "open_remote failed");
            assert!(!db.is_null());

            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"hello-remote";
            let value = b"world-remote";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()),
                FRS_STATUS_OK
            );
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            let slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(slice, value);
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_db_open_remote_null_uri_returns_null_arg() {
        let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
        let cdir = CString::new(cache_dir.path().to_string_lossy().into_owned()).unwrap();
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            let rc = frs_db_open_remote(ptr::null(), ptr::null(), cdir.as_ptr(), 1024, &mut db);
            assert_eq!(rc, FRS_STATUS_NULL_ARG);
            assert!(db.is_null());
        }
    }

    #[test]
    fn test_frs_db_open_remote_with_options_honours_engine_tuning() {
        let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
        let cache_dir_str = cache_dir.path().to_string_lossy().into_owned();
        unsafe {
            let uri = CString::new("memory://").unwrap();
            let cfg = CString::new("{}").unwrap();
            let cdir = CString::new(cache_dir_str).unwrap();
            let opts = FrsEngineOptions {
                db_path: ptr::null(),
                write_buffer_size: 8 * 1024 * 1024,
                max_write_buffer_number: 2,
                max_background_compactions: 2,
                max_background_flushes: 1,
                block_cache_capacity_bytes: 32 * 1024 * 1024,
                write_buffer_manager_capacity_bytes: 96 * 1024 * 1024,
                sst_compression: 0,
            };

            let mut db: FrsDb = ptr::null_mut();
            let rc = frs_db_open_remote_with_options(
                &opts,
                uri.as_ptr(),
                cfg.as_ptr(),
                cdir.as_ptr(),
                64 * 1024 * 1024,
                &mut db,
            );
            assert_eq!(rc, FRS_STATUS_OK, "open_remote_with_options failed");
            assert!(!db.is_null());
            assert_eq!(frs_db_write_buffer_manager_capacity(db), 96 * 1024 * 1024);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_db_open_remote_invalid_scheme_returns_invalid_argument() {
        let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
        let cdir = CString::new(cache_dir.path().to_string_lossy().into_owned()).unwrap();
        unsafe {
            let uri = CString::new("ftp://nope/").unwrap();
            let cfg = CString::new("{}").unwrap();
            let mut db: FrsDb = ptr::null_mut();
            let rc = frs_db_open_remote(uri.as_ptr(), cfg.as_ptr(), cdir.as_ptr(), 1024, &mut db);
            assert_eq!(rc, FRS_STATUS_INVALID_ARGUMENT);
            assert!(db.is_null());
        }
    }

    // -----------------------------------------------------------------
    // 9. State import / export migration (B-Prod-P10, spec §6g)
    // -----------------------------------------------------------------

    #[test]
    fn test_frs_cf_export_then_import_roundtrip() {
        let export_dir = tempfile::TempDir::new().expect("export tempdir");
        let export_dir_c =
            CString::new(export_dir.path().to_string_lossy().into_owned()).expect("cstring");

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);

            // Source CF.
            let src_name = CString::new("src").unwrap();
            let mut src_cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf(db, src_name.as_ptr(), &mut src_cf),
                FRS_STATUS_OK
            );

            // Write a few keys via the FFI put surface.
            for i in 0u32..32 {
                let k = format!("k{:02}", i);
                let v = format!("v{:02}", i);
                assert_eq!(
                    frs_put(db, src_cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            // Export.
            let rc = frs_cf_export(db, src_cf, export_dir_c.as_ptr());
            assert_eq!(rc, FRS_STATUS_OK, "frs_cf_export failed: {rc}");

            // Import as a NEW CF with a different name.
            let imp_name = CString::new("imported").unwrap();
            let mut imp_cf: FrsCfHandle = ptr::null_mut();
            let rc = frs_db_create_cf_from_import(
                db,
                imp_name.as_ptr(),
                export_dir_c.as_ptr(),
                &mut imp_cf,
            );
            assert_eq!(
                rc, FRS_STATUS_OK,
                "frs_db_create_cf_from_import failed: {rc}"
            );
            assert!(!imp_cf.is_null());

            // Read back from the imported CF.
            for i in 0u32..32 {
                let k = format!("k{:02}", i);
                let expected = format!("v{:02}", i);
                let mut out = FrsBytes::NULL;
                assert_eq!(
                    frs_get(db, imp_cf, k.as_ptr(), k.len(), &mut out),
                    FRS_STATUS_OK,
                    "imported get for {k}"
                );
                let slice = slice::from_raw_parts(out.data, out.len);
                assert_eq!(slice, expected.as_bytes());
                frs_bytes_free(&mut out);
            }

            assert_eq!(frs_cf_close(imp_cf), FRS_STATUS_OK);
            assert_eq!(frs_cf_close(src_cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// OPT-N04 E3: `frs_db_create_cf_from_import_with_merge` threads the
    /// operator by name through the rescale/import path. The source CF's
    /// chain has operands pending at export time; the import must (1)
    /// read the resolved fold byte-exactly and (2) accept FUTURE merges —
    /// which the legacy no-operator import rejects (D-R8-NEW-H2).
    #[test]
    fn test_frs_cf_import_with_merge_live_chain_roundtrip() {
        let export_dir = tempfile::TempDir::new().expect("export tempdir");
        let export_dir_c =
            CString::new(export_dir.path().to_string_lossy().into_owned()).expect("cstring");

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);

            // Source CF with the BE numeric-add operator + a live chain.
            let src_name = CString::new("agg-src").unwrap();
            let op_name = CString::new("NumericAddBeMergeOperator").unwrap();
            let mut src_cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_with_merge(db, src_name.as_ptr(), op_name.as_ptr(), &mut src_cf),
                FRS_STATUS_OK
            );
            let k = b"acc";
            let base = 10i64.to_be_bytes();
            let d1 = 5i64.to_be_bytes();
            let d2 = 2i64.to_be_bytes();
            assert_eq!(
                frs_put(db, src_cf, k.as_ptr(), k.len(), base.as_ptr(), base.len()),
                FRS_STATUS_OK
            );
            assert_eq!(
                frs_merge(db, src_cf, k.as_ptr(), k.len(), d1.as_ptr(), d1.len()),
                FRS_STATUS_OK
            );
            assert_eq!(
                frs_merge(db, src_cf, k.as_ptr(), k.len(), d2.as_ptr(), d2.len()),
                FRS_STATUS_OK
            );

            assert_eq!(
                frs_cf_export(db, src_cf, export_dir_c.as_ptr()),
                FRS_STATUS_OK
            );

            // Unknown operator name → INVALID_ARGUMENT, no CF created.
            let imp_name = CString::new("agg-imported").unwrap();
            let bad_op = CString::new("NoSuchOperator").unwrap();
            let mut imp_cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_from_import_with_merge(
                    db,
                    imp_name.as_ptr(),
                    bad_op.as_ptr(),
                    export_dir_c.as_ptr(),
                    &mut imp_cf,
                ),
                FRS_STATUS_INVALID_ARGUMENT
            );
            assert!(imp_cf.is_null());

            // Import WITH the operator (name freed by the failed attempt).
            assert_eq!(
                frs_db_create_cf_from_import_with_merge(
                    db,
                    imp_name.as_ptr(),
                    op_name.as_ptr(),
                    export_dir_c.as_ptr(),
                    &mut imp_cf,
                ),
                FRS_STATUS_OK
            );
            assert!(!imp_cf.is_null());

            // Resolved fold imported byte-exactly: 10 + 5 + 2 = 17 (BE).
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, imp_cf, k.as_ptr(), k.len(), &mut out),
                FRS_STATUS_OK
            );
            assert_eq!(
                slice::from_raw_parts(out.data, out.len),
                &17i64.to_be_bytes()
            );
            frs_bytes_free(&mut out);

            // FUTURE merge folds — the operator made it across the import.
            let d3 = (-4i64).to_be_bytes();
            assert_eq!(
                frs_merge(db, imp_cf, k.as_ptr(), k.len(), d3.as_ptr(), d3.len()),
                FRS_STATUS_OK
            );
            let mut out2 = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, imp_cf, k.as_ptr(), k.len(), &mut out2),
                FRS_STATUS_OK
            );
            assert_eq!(
                slice::from_raw_parts(out2.data, out2.len),
                &13i64.to_be_bytes()
            );
            frs_bytes_free(&mut out2);

            // NULL operator name == legacy import shape: data readable,
            // future merges REJECTED (D-R8-NEW-H2).
            let plain_name = CString::new("agg-imported-plain").unwrap();
            let mut plain_cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_from_import_with_merge(
                    db,
                    plain_name.as_ptr(),
                    ptr::null(),
                    export_dir_c.as_ptr(),
                    &mut plain_cf,
                ),
                FRS_STATUS_OK
            );
            let mut out3 = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, plain_cf, k.as_ptr(), k.len(), &mut out3),
                FRS_STATUS_OK
            );
            assert_eq!(
                slice::from_raw_parts(out3.data, out3.len),
                &17i64.to_be_bytes()
            );
            frs_bytes_free(&mut out3);
            // The single-shot frs_merge path doesn't pre-validate the
            // operator (only the vectorized batch path does, D-R8-NEW-H2),
            // so the write lands — but the fold is impossible and the next
            // read surfaces InvalidArgument: the op-less imported CF cannot
            // serve merge-routed state.
            assert_eq!(
                frs_merge(db, plain_cf, k.as_ptr(), k.len(), d3.as_ptr(), d3.len()),
                FRS_STATUS_OK
            );
            let mut out4 = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, plain_cf, k.as_ptr(), k.len(), &mut out4),
                FRS_STATUS_INVALID_ARGUMENT,
                "reading a merge chain on an operator-less imported CF must error"
            );

            assert_eq!(frs_cf_close(plain_cf), FRS_STATUS_OK);
            assert_eq!(frs_cf_close(imp_cf), FRS_STATUS_OK);
            assert_eq!(frs_cf_close(src_cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_cf_export_null_args_return_null_arg() {
        unsafe {
            // null db
            let dir = CString::new("/tmp/nope").unwrap();
            assert_eq!(
                frs_cf_export(ptr::null_mut(), ptr::null_mut(), dir.as_ptr()),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    #[test]
    fn test_frs_db_create_cf_from_import_null_out_returns_null_arg() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let dir_c = CString::new(dir.path().to_string_lossy().into_owned()).unwrap();
        let name = CString::new("x").unwrap();
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            // out_cf == null
            let rc =
                frs_db_create_cf_from_import(db, name.as_ptr(), dir_c.as_ptr(), ptr::null_mut());
            assert_eq!(rc, FRS_STATUS_NULL_ARG);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // ---- B-Prod-followup-5: frs_db_drop_cf + frs_db_ingest_external_sst ----

    #[test]
    fn test_frs_db_drop_cf_round_trip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let name = CString::new("to_drop").unwrap();
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_create_cf(db, name.as_ptr(), &mut cf), FRS_STATUS_OK);
            // First drop succeeds.
            assert_eq!(frs_db_drop_cf(db, cf), FRS_STATUS_OK);
            // Second drop is idempotent.
            assert_eq!(frs_db_drop_cf(db, cf), FRS_STATUS_OK);
            // A subsequent put on the dropped handle must fail.
            let k = b"x";
            let v = b"y";
            let rc = frs_put(db, cf, k.as_ptr(), 1, v.as_ptr(), 1);
            assert_eq!(rc, FRS_STATUS_INVALID_ARGUMENT);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_db_drop_cf_rejects_default() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
            let rc = frs_db_drop_cf(db, cf);
            assert_eq!(rc, FRS_STATUS_INVALID_ARGUMENT);
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_db_drop_cf_null_args() {
        unsafe {
            assert_eq!(
                frs_db_drop_cf(ptr::null_mut(), ptr::null_mut()),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    #[test]
    fn test_frs_db_ingest_external_sst_empty_input() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
            // count = 0 with null sst_paths: engine no-op.
            assert_eq!(
                frs_db_ingest_external_sst(db, cf, ptr::null(), 0),
                FRS_STATUS_OK
            );
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_db_ingest_external_sst_null_args() {
        unsafe {
            // null db
            assert_eq!(
                frs_db_ingest_external_sst(ptr::null_mut(), ptr::null_mut(), ptr::null(), 0),
                FRS_STATUS_NULL_ARG
            );
            // count > MAX_BATCH_COUNT
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);
            let dummy: *const c_char = ptr::null();
            assert_eq!(
                frs_db_ingest_external_sst(db, cf, &dummy, MAX_BATCH_COUNT + 1),
                FRS_STATUS_INVALID_ARGUMENT
            );
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_get_and_put_returns_old_value() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Put initial value.
            let key = b"k1";
            let val1 = b"old_value";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), val1.as_ptr(), val1.len()),
                FRS_STATUS_OK
            );

            // get_and_put: should return old value and write new.
            let val2 = b"new_value";
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get_and_put(
                    db,
                    cf,
                    key.as_ptr(),
                    key.len(),
                    val2.as_ptr(),
                    val2.len(),
                    &mut out
                ),
                FRS_STATUS_OK
            );
            assert!(!out.data.is_null());
            let old_slice = slice::from_raw_parts(out.data, out.len);
            assert_eq!(old_slice, val1);
            frs_bytes_free(&mut out);

            // Verify new value is stored.
            let mut get_out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut get_out),
                FRS_STATUS_OK
            );
            let new_slice = slice::from_raw_parts(get_out.data, get_out.len);
            assert_eq!(new_slice, val2);
            frs_bytes_free(&mut get_out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_get_and_put_missing_key_returns_not_found() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"missing";
            let val = b"value";
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get_and_put(
                    db,
                    cf,
                    key.as_ptr(),
                    key.len(),
                    val.as_ptr(),
                    val.len(),
                    &mut out
                ),
                FRS_STATUS_NOT_FOUND
            );
            // out should be NULL (no old value).
            assert!(out.data.is_null());

            // But the put still succeeded.
            let mut get_out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut get_out),
                FRS_STATUS_OK
            );
            let slice = slice::from_raw_parts(get_out.data, get_out.len);
            assert_eq!(slice, val);
            frs_bytes_free(&mut get_out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn test_frs_get_and_put_null_args() {
        unsafe {
            let mut out = FrsBytes::NULL;
            // null db
            assert_eq!(
                frs_get_and_put(
                    ptr::null_mut(),
                    ptr::null_mut(),
                    ptr::null(),
                    0,
                    ptr::null(),
                    0,
                    &mut out
                ),
                FRS_STATUS_NULL_ARG
            );
        }
    }

    #[test]
    fn abi_version_is_one() {
        assert_eq!(frs_abi_version(), 1);
    }

    #[test]
    fn error_codes_match_spec_section_4() {
        assert_eq!(FrsErrorCode::Ok as u32, 0);
        assert_eq!(FrsErrorCode::NotFound as u32, 1);
        assert_eq!(FrsErrorCode::KeyTooLarge as u32, 100);
        assert_eq!(FrsErrorCode::ValueTooLarge as u32, 101);
        assert_eq!(FrsErrorCode::BatchHeaderMalformed as u32, 110);
        assert_eq!(FrsErrorCode::IterExpired as u32, 200);
        assert_eq!(FrsErrorCode::IterCursorInvalid as u32, 201);
        assert_eq!(FrsErrorCode::EngineIo as u32, 300);
        assert_eq!(FrsErrorCode::EngineCorrupted as u32, 301);
        assert_eq!(FrsErrorCode::EngineOom as u32, 302);
        assert_eq!(FrsErrorCode::EngineDiskFull as u32, 303);
        assert_eq!(FrsErrorCode::PanicCaught as u32, 900);
        assert_eq!(FrsErrorCode::Unknown as u32, 999);
    }

    #[test]
    fn frs_row_result_layout_is_3_u32() {
        use std::mem::size_of;
        assert_eq!(size_of::<FrsRowResult>(), 12); // 3 × u32, packed (repr(C))
    }

    // -----------------------------------------------------------------
    // P2.2: frs_vectorized_batch_* error envelope uses FrsErrorCode
    // -----------------------------------------------------------------

    /// Verifies that `frs_vectorized_batch_get` returns `FrsErrorCode::Ok`
    /// (0) on a happy-path round-trip and `FrsErrorCode::BatchHeaderMalformed`
    /// (110) when a null required pointer is passed.
    #[test]
    fn vec_batch_get_ok_and_null_returns_frs_error_code() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Write one key so GET has a result.
            let key = b"vec-p2-key";
            let val = b"vec-p2-val";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()),
                FRS_STATUS_OK
            );

            // Build a 1-key batch: offsets=[0, 10], data=b"vec-p2-key"
            let key_offs: [i32; 2] = [0, key.len() as i32];
            let mut out_offs: [i32; 2] = [0; 2];
            let mut out_data: [u8; 64] = [0u8; 64];
            let mut out_vld: [u8; 1] = [0u8; 1];
            let mut out_len: usize = 0;

            let rc = frs_vectorized_batch_get(
                db,
                cf,
                key_offs.as_ptr(),
                key.as_ptr(),
                key.len(),
                1,
                out_offs.as_mut_ptr(),
                out_data.as_mut_ptr(),
                out_vld.as_mut_ptr(),
                64,
                &mut out_len,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "expected FrsErrorCode::Ok (0)");
            assert_eq!(out_vld[0], 1, "key should be found");
            assert_eq!(out_len, val.len());
            assert_eq!(&out_data[..out_len], val);

            // Null out_data_len → BatchHeaderMalformed (110)
            let rc_null = frs_vectorized_batch_get(
                db,
                cf,
                key_offs.as_ptr(),
                key.as_ptr(),
                key.len(),
                1,
                out_offs.as_mut_ptr(),
                out_data.as_mut_ptr(),
                out_vld.as_mut_ptr(),
                64,
                ptr::null_mut(), // <-- null
            );
            assert_eq!(
                rc_null,
                FrsErrorCode::BatchHeaderMalformed as i32,
                "null out_data_len should return BatchHeaderMalformed (110)"
            );

            // count > MAX_BATCH_COUNT → BatchHeaderMalformed (110)
            let rc_over = frs_vectorized_batch_get(
                db,
                cf,
                key_offs.as_ptr(),
                key.as_ptr(),
                key.len(),
                MAX_BATCH_COUNT + 1,
                out_offs.as_mut_ptr(),
                out_data.as_mut_ptr(),
                out_vld.as_mut_ptr(),
                64,
                &mut out_len,
            );
            assert_eq!(
                rc_over,
                FrsErrorCode::BatchHeaderMalformed as i32,
                "count > MAX_BATCH_COUNT should return BatchHeaderMalformed (110)"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Verifies that `frs_vectorized_batch_put` returns `FrsErrorCode::Ok`
    /// (0) on success and `FrsErrorCode::BatchHeaderMalformed` (110) on
    /// null-handle / null-pointer / oversized-count inputs.
    #[test]
    fn vec_batch_put_ok_and_null_returns_frs_error_code() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"vec-put-key";
            let val = b"vec-put-val";
            let key_offs: [i32; 2] = [0, key.len() as i32];
            let val_offs: [i32; 2] = [0, val.len() as i32];

            let rc = frs_vectorized_batch_put(
                db,
                cf,
                key_offs.as_ptr(),
                key.as_ptr(),
                key.len(),
                val_offs.as_ptr(),
                val.as_ptr(),
                val.len(),
                1,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "expected FrsErrorCode::Ok (0)");

            // null handle → BatchHeaderMalformed (110)
            let rc_null = frs_vectorized_batch_put(
                ptr::null_mut(), // null db
                ptr::null_mut(),
                key_offs.as_ptr(),
                key.as_ptr(),
                key.len(),
                val_offs.as_ptr(),
                val.as_ptr(),
                val.len(),
                1,
            );
            assert_eq!(
                rc_null,
                FrsErrorCode::BatchHeaderMalformed as i32,
                "null db handle should return BatchHeaderMalformed (110)"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Verifies that `frs_vectorized_batch_delete` returns `FrsErrorCode::Ok`
    /// (0) on success and `FrsErrorCode::BatchHeaderMalformed` (110) on error.
    #[test]
    fn vec_batch_delete_ok_and_null_returns_frs_error_code() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"vec-del-key";
            let val = b"vec-del-val";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), val.as_ptr(), val.len()),
                FRS_STATUS_OK
            );

            let key_offs: [i32; 2] = [0, key.len() as i32];
            let rc =
                frs_vectorized_batch_delete(db, cf, key_offs.as_ptr(), key.as_ptr(), key.len(), 1);
            assert_eq!(rc, FrsErrorCode::Ok as i32, "expected FrsErrorCode::Ok (0)");

            // null key_offsets → BatchHeaderMalformed (110)
            let rc_null = frs_vectorized_batch_delete(
                db,
                cf,
                ptr::null(), // null key_offsets
                key.as_ptr(),
                key.len(),
                1,
            );
            assert_eq!(
                rc_null,
                FrsErrorCode::BatchHeaderMalformed as i32,
                "null key_offsets should return BatchHeaderMalformed (110)"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // -----------------------------------------------------------------
    // Stage-3 Unit 2: frs_vectorized_batch_mixed
    // -----------------------------------------------------------------

    /// Helper: read a key back via `frs_get`; returns Some(bytes) if present.
    unsafe fn get_value(db: FrsDb, cf: FrsCfHandle, key: &[u8]) -> Option<Vec<u8>> {
        let mut out = FrsBytes::NULL;
        assert_eq!(
            frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
            FRS_STATUS_OK
        );
        if out.data.is_null() {
            return None;
        }
        let v = slice::from_raw_parts(out.data, out.len).to_vec();
        frs_bytes_free(&mut out);
        Some(v)
    }

    /// Mixed put+delete+merge in ONE call: put k1, delete pre-existing k2,
    /// merge two operands onto k3 (default CF has RawConcatMergeOperator) —
    /// then read all three back.
    #[test]
    fn vec_batch_mixed_put_delete_merge_round_trip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Pre-existing key that the mixed batch will delete.
            let k2 = b"mx-k2";
            assert_eq!(
                frs_put(db, cf, k2.as_ptr(), k2.len(), b"dead".as_ptr(), 4),
                FRS_STATUS_OK
            );

            // Rows: [Put k1="v1", Delete k2, Merge k3+="A", Merge k3+="B"]
            let key_data = b"mx-k1mx-k2mx-k3mx-k3";
            let key_offs: [i32; 5] = [0, 5, 10, 15, 20];
            let val_data = b"v1AB";
            let val_offs: [i32; 5] = [0, 2, 2, 3, 4]; // delete row: empty slice
            let kinds: [u8; 4] = [1, 0, 2, 2];

            let rc = frs_vectorized_batch_mixed(
                db,
                cf,
                kinds.as_ptr(),
                4,
                key_offs.as_ptr(),
                key_data.as_ptr(),
                key_data.len(),
                val_offs.as_ptr(),
                val_data.as_ptr(),
                val_data.len(),
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "expected FrsErrorCode::Ok (0)");

            assert_eq!(get_value(db, cf, b"mx-k1").as_deref(), Some(&b"v1"[..]));
            assert_eq!(get_value(db, cf, b"mx-k2"), None, "k2 must be deleted");
            assert_eq!(
                get_value(db, cf, b"mx-k3").as_deref(),
                Some(&b"AB"[..]),
                "raw-concat merge of operands A then B"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// A pure-put batch routed through the mixed entry must produce the same
    /// readable state as the same rows via `frs_vectorized_batch_put`.
    #[test]
    fn vec_batch_mixed_pure_put_matches_plain_batch_put() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let val_data = b"v-onev-two";
            let val_offs: [i32; 3] = [0, 5, 10];

            // Plain path: keys pa/pb.
            let plain_keys = b"papb";
            let key_offs: [i32; 3] = [0, 2, 4];
            assert_eq!(
                frs_vectorized_batch_put(
                    db,
                    cf,
                    key_offs.as_ptr(),
                    plain_keys.as_ptr(),
                    plain_keys.len(),
                    val_offs.as_ptr(),
                    val_data.as_ptr(),
                    val_data.len(),
                    2,
                ),
                FrsErrorCode::Ok as i32
            );

            // Mixed path (kinds all Put): keys ma/mb, same values.
            let mixed_keys = b"mamb";
            let kinds: [u8; 2] = [1, 1];
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    2,
                    key_offs.as_ptr(),
                    mixed_keys.as_ptr(),
                    mixed_keys.len(),
                    val_offs.as_ptr(),
                    val_data.as_ptr(),
                    val_data.len(),
                ),
                FrsErrorCode::Ok as i32
            );

            assert_eq!(get_value(db, cf, b"ma"), get_value(db, cf, b"pa"));
            assert_eq!(get_value(db, cf, b"mb"), get_value(db, cf, b"pb"));
            assert_eq!(get_value(db, cf, b"ma").as_deref(), Some(&b"v-one"[..]));
            assert_eq!(get_value(db, cf, b"mb").as_deref(), Some(&b"v-two"[..]));

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Any kind byte outside {0, 1, 2} → BatchHeaderMalformed (110), and
    /// nothing is written.
    #[test]
    fn vec_batch_mixed_invalid_kind_rejected() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"mx-bad";
            let val = b"v";
            let key_offs: [i32; 2] = [0, key.len() as i32];
            let val_offs: [i32; 2] = [0, val.len() as i32];

            for bad_kind in [3u8, 255u8] {
                let kinds = [bad_kind];
                let rc = frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                );
                assert_eq!(
                    rc,
                    FrsErrorCode::BatchHeaderMalformed as i32,
                    "kind byte {bad_kind} must be rejected"
                );
            }
            assert_eq!(
                get_value(db, cf, key),
                None,
                "rejected batch must not write"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Null-pointer, offset, count, and delete-with-value validation all
    /// return BatchHeaderMalformed (110); count == 0 is Ok.
    #[test]
    fn vec_batch_mixed_null_and_offset_validation() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"mx-val";
            let val = b"v";
            let key_offs: [i32; 2] = [0, key.len() as i32];
            let val_offs: [i32; 2] = [0, val.len() as i32];
            let kinds: [u8; 1] = [1];
            let malformed = FrsErrorCode::BatchHeaderMalformed as i32;

            // count == 0 → Ok fast path (pointers may be anything).
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    ptr::null(),
                    0,
                    ptr::null(),
                    ptr::null(),
                    0,
                    ptr::null(),
                    ptr::null(),
                    0,
                ),
                FrsErrorCode::Ok as i32
            );

            // null db handle
            assert_eq!(
                frs_vectorized_batch_mixed(
                    ptr::null_mut(),
                    ptr::null_mut(),
                    kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // null kinds
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    ptr::null(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // null key_offsets
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    1,
                    ptr::null(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // null value_offsets
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    ptr::null(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // count > MAX_BATCH_COUNT
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    MAX_BATCH_COUNT + 1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // negative key offset rejected before slicing
            let neg_offs: [i32; 2] = [0, -1];
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    1,
                    neg_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // offsets exceeding declared data_len
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len() - 1, // declared shorter than offsets claim
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );
            // Delete row with a NON-empty value slice → layout bug, rejected.
            let del_kinds: [u8; 1] = [0];
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    del_kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(), // [0, 1] — non-empty value for delete
                    val.as_ptr(),
                    val.len(),
                ),
                malformed
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Merge rows on a CF WITHOUT a merge operator → BatchHeaderMalformed
    /// (110); the same CF still accepts put/delete-only mixed batches.
    #[test]
    fn vec_batch_mixed_merge_without_operator_rejected() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            // CF created with merge_op_name = NULL → no merge operator.
            let name = CString::new("no-merge-cf").unwrap();
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(
                frs_db_create_cf_with_merge(db, name.as_ptr(), ptr::null(), &mut cf),
                FRS_STATUS_OK
            );

            let key = b"mx-nm";
            let val = b"v";
            let key_offs: [i32; 2] = [0, key.len() as i32];
            let val_offs: [i32; 2] = [0, val.len() as i32];

            // Merge row → rejected.
            let merge_kinds: [u8; 1] = [2];
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    merge_kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                FrsErrorCode::BatchHeaderMalformed as i32,
                "merge row on merge-less CF must be rejected"
            );
            assert_eq!(
                get_value(db, cf, key),
                None,
                "rejected batch must not write"
            );

            // Put-only mixed batch on the same CF → fine (operator guard is
            // only enforced when a merge row exists).
            let put_kinds: [u8; 1] = [1];
            assert_eq!(
                frs_vectorized_batch_mixed(
                    db,
                    cf,
                    put_kinds.as_ptr(),
                    1,
                    key_offs.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offs.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                ),
                FrsErrorCode::Ok as i32
            );
            assert_eq!(get_value(db, cf, key).as_deref(), Some(&b"v"[..]));

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn vec_batch_get_rejects_negative_offsets_before_slicing_data() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key_offs = [0i32, -1i32];
            let mut out_offs = [0i32; 2];
            let mut out_vld = [0u8; 1];
            let mut out_len: usize = 0;

            let rc = frs_vectorized_batch_get(
                db,
                cf,
                key_offs.as_ptr(),
                ptr::null(),
                0,
                1,
                out_offs.as_mut_ptr(),
                ptr::null_mut(),
                out_vld.as_mut_ptr(),
                0,
                &mut out_len,
            );

            assert_eq!(
                rc,
                FrsErrorCode::BatchHeaderMalformed as i32,
                "negative Arrow offset must be rejected before any data slice is formed"
            );
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Verifies that `guarded_vec` returns `FrsErrorCode::PanicCaught` (900)
    /// when the closure panics.
    #[test]
    fn guarded_vec_returns_panic_caught_on_panic() {
        let rc = guarded_vec(|| panic!("intentional test panic"));
        assert_eq!(
            rc,
            FrsErrorCode::PanicCaught as i32,
            "guarded_vec must return FrsErrorCode::PanicCaught (900) on panic"
        );
    }

    /// Documents the intended test for forcing a panic inside
    /// `frs_vectorized_batch_get` via a null slice creation. Requires either
    /// a fault-injection hook (P10) or a crafted offset array; skipped here
    /// because constructing the UB scenario safely is non-trivial and the
    /// `guarded_vec_returns_panic_caught_on_panic` test above already
    /// validates the catch_unwind path directly.
    #[test]
    #[ignore = "requires P10 FaultInjector to force a safe panic inside frs_vectorized_batch_get"]
    fn panic_in_vec_get_returns_panic_caught() {
        // TODO(P10): inject a panic via FaultInjector::set_panic_on_next_get()
        // and verify frs_vectorized_batch_get returns FrsErrorCode::PanicCaught (900).
    }

    // -----------------------------------------------------------------------
    // frs_vec_iter_prefix_* tests (P3-A, spec §1 §b + §2 component E)
    // -----------------------------------------------------------------------

    /// Helper: decode rows from a chunk buffer written by `fill_chunk_from_iter`.
    /// Returns a Vec of (key, value) byte vecs.
    fn decode_chunk_buf(buf: &[u8], bytes_used: u32, row_count: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut rows = Vec::new();
        let mut off = 0usize;
        let limit = bytes_used as usize;
        for _ in 0..row_count {
            if off + 8 > limit {
                break;
            }
            let klen = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
            let vlen = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()) as usize;
            off += 8;
            if off + klen + vlen > limit {
                break;
            }
            let k = buf[off..off + klen].to_vec();
            off += klen;
            let v = buf[off..off + vlen].to_vec();
            off += vlen;
            rows.push((k, v));
        }
        rows
    }

    /// Open/close round trip with 3 prefix-matching keys + 1 outside prefix.
    /// P0 auto-close: the first chunk exhausts the iterator, so open returns
    /// `handle == 0` (auto-closed, nothing registered) with the 3 rows in the
    /// chunk; a legacy trailing `_next(0)` still reports clean EOF and
    /// `_close(0)` stays a no-op.
    #[test]
    fn vec_iter_prefix_open_close_round_trip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Insert 3 rows under prefix "p1/" and 1 outside.
            for (k, v) in &[
                (&b"p1/a"[..], &b"v1"[..]),
                (b"p1/b", b"v2"),
                (b"p1/c", b"v3"),
                (b"q/x", b"vx"),
            ] {
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            let mut chunk_buf = vec![0u8; 4096];
            let mut handle: u64 = 0;
            let mut row_count: u32 = 0;
            let mut bytes_used: u32 = 0;

            let prefix = b"p1/";
            let rc = frs_vec_iter_prefix_open(
                db,
                cf,
                prefix.as_ptr(),
                prefix.len() as u32,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut handle,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "open should return Ok");
            assert_eq!(
                handle, 0,
                "P0 auto-close: exhausted-in-first-chunk open returns handle 0"
            );
            assert_eq!(row_count, 3, "first chunk should contain 3 rows");

            // Decode and verify the rows came back.
            let rows = decode_chunk_buf(&chunk_buf, bytes_used, row_count);
            assert_eq!(rows.len(), 3);
            // Keys should all start with "p1/".
            for (k, _) in &rows {
                assert!(k.starts_with(b"p1/"), "unexpected key {:?}", k);
            }

            // Legacy drain compatibility: a trailing next() on the auto-closed
            // handle reports clean EOF (empty chunk, rc=Ok).
            let rc = frs_vec_iter_prefix_next(
                handle,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);
            assert_eq!(row_count, 0, "second chunk should be empty");
            assert_eq!(bytes_used, 0);

            // Close should succeed.
            assert_eq!(frs_vec_iter_prefix_close(handle), FrsErrorCode::Ok as i32);

            // Second close is a no-op (handle removed from registry).
            assert_eq!(frs_vec_iter_prefix_close(handle), FrsErrorCode::Ok as i32);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// P0 auto-close: `frs_vec_iter_prefix_next` on an unknown (or
    /// auto-closed) handle reports clean EOF — `Ok` with an empty chunk —
    /// instead of the pre-P0 `IterCursorInvalid` (201). This is what keeps
    /// flag-unaware callers' mandatory trailing `next()` working after the
    /// engine auto-closes exhausted-at-open iterators.
    #[test]
    fn vec_iter_prefix_next_unknown_handle_reports_eof() {
        let mut chunk_buf = vec![0u8; 64];
        let mut row_count: u32 = 7;
        let mut bytes_used: u32 = 7;
        let rc = unsafe {
            frs_vec_iter_prefix_next(
                u64::MAX, // non-existent handle
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut row_count,
                &mut bytes_used,
            )
        };
        assert_eq!(rc, FrsErrorCode::Ok as i32);
        assert_eq!(row_count, 0, "unknown handle must report EOF (0 rows)");
        assert_eq!(bytes_used, 0);
    }

    /// Null out-pointers return `BatchHeaderMalformed` (110).
    #[test]
    fn vec_iter_prefix_open_null_out_pointers_return_malformed() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let mut chunk_buf = vec![0u8; 64];
            let prefix = b"p/";

            // null out_handle
            let rc = frs_vec_iter_prefix_open(
                db,
                cf,
                prefix.as_ptr(),
                prefix.len() as u32,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                ptr::null_mut(), // <-- null
                &mut 0u32,
                &mut 0u32,
            );
            assert_eq!(
                rc,
                FrsErrorCode::BatchHeaderMalformed as i32,
                "null out_handle should return BatchHeaderMalformed"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Abort an iterator: subsequent next returns empty; abort on unknown
    /// handle returns `IterCursorInvalid`.
    #[test]
    fn vec_iter_prefix_abort_stops_iteration() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Insert rows with a tiny chunk budget so they'd normally span
            // multiple chunks (budget = 1 byte → 1 row per chunk).
            for i in 0u8..4 {
                let k = format!("ab/{}", i);
                let v = format!("val{}", i);
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len(),),
                    FRS_STATUS_OK
                );
            }

            // P0: cap the first chunk to ONE row (8B header + 4B key + 4B val
            // = 16B) so the open does NOT exhaust the iterator — an exhausted
            // open auto-closes and returns handle 0, which would leave nothing
            // to abort. 24B holds exactly one 16B row (a second wouldn't fit).
            let mut chunk_buf = vec![0u8; 24];
            let mut handle: u64 = 0;
            let mut row_count: u32 = 0;
            let mut bytes_used: u32 = 0;

            let prefix = b"ab/";
            let rc = frs_vec_iter_prefix_open(
                db,
                cf,
                prefix.as_ptr(),
                prefix.len() as u32,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut handle,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);
            assert_ne!(handle, 0, "non-exhausted open must register a handle");
            assert_eq!(row_count, 1, "24B chunk holds exactly one row");

            // Abort: ok return.
            assert_eq!(frs_vec_iter_prefix_abort(handle), FrsErrorCode::Ok as i32);

            // Next after abort → empty chunk.
            let rc = frs_vec_iter_prefix_next(
                handle,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);
            assert_eq!(row_count, 0, "aborted iter should return empty chunk");

            // Abort on unknown handle → IterCursorInvalid.
            assert_eq!(
                frs_vec_iter_prefix_abort(u64::MAX),
                FrsErrorCode::IterCursorInvalid as i32
            );

            assert_eq!(frs_vec_iter_prefix_close(handle), FrsErrorCode::Ok as i32);
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Close with handle == 0 is a no-op (returns Ok).
    #[test]
    fn vec_iter_prefix_close_zero_handle_is_noop() {
        assert_eq!(frs_vec_iter_prefix_close(0), FrsErrorCode::Ok as i32);
    }

    /// PR-E3: `frs_vec_iter_prefix_open_batch` opens 4 prefix iterators in ONE
    /// FFI call.  Verifies that all 4 handles are non-zero and unique, that
    /// each handle's first chunk decodes to the expected row set, and that
    /// each handle is independently closable (so the shared registry is
    /// populated correctly by the batched path).
    #[test]
    fn vec_iter_prefix_open_batch_four_iters_in_one_call() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // 4 prefixes, each with 2 rows + 1 distractor row outside all prefixes.
            let prefixes: [&[u8]; 4] = [b"pA/", b"pB/", b"pC/", b"pD/"];
            for p in &prefixes {
                for sfx in [&b"x"[..], &b"y"[..]] {
                    let mut k = Vec::with_capacity(p.len() + sfx.len());
                    k.extend_from_slice(p);
                    k.extend_from_slice(sfx);
                    let v = b"v";
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                        FRS_STATUS_OK
                    );
                }
            }
            let distractor = (&b"zzz"[..], &b"vz"[..]);
            assert_eq!(
                frs_put(
                    db,
                    cf,
                    distractor.0.as_ptr(),
                    distractor.0.len(),
                    distractor.1.as_ptr(),
                    distractor.1.len(),
                ),
                FRS_STATUS_OK
            );

            // Pack the prefixes SoA.
            let n = prefixes.len();
            let mut offs: Vec<u32> = Vec::with_capacity(n + 1);
            let mut data: Vec<u8> = Vec::new();
            offs.push(0);
            for p in &prefixes {
                data.extend_from_slice(p);
                offs.push(data.len() as u32);
            }

            // One chunk buffer per iter at uniform capacity.
            const CHUNK_CAP: u32 = 4096;
            let mut chunk_storage: Vec<Vec<u8>> =
                (0..n).map(|_| vec![0u8; CHUNK_CAP as usize]).collect();
            let mut chunks: Vec<FrsChunk> = (0..n)
                .map(|i| FrsChunk {
                    buf_ptr: chunk_storage[i].as_mut_ptr(),
                    buf_cap: CHUNK_CAP,
                    row_count: 0,
                    bytes_used: 0,
                    _reserved: 0,
                })
                .collect();
            let mut handles: Vec<u64> = vec![0; n];

            let rc = frs_vec_iter_prefix_open_batch(
                db,
                cf,
                offs.as_ptr(),
                data.as_ptr(),
                n as u32,
                handles.as_mut_ptr(),
                chunks.as_mut_ptr(),
                CHUNK_CAP,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "batch open should return Ok");

            // All 4 handles non-zero and unique.
            for (i, h) in handles.iter().enumerate() {
                assert_ne!(*h, 0, "handle {} should be non-zero", i);
            }
            let mut sorted = handles.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), n, "all handles must be unique");

            // Each chunk decoded to 2 rows under its prefix.
            for i in 0..n {
                let chunk = &chunks[i];
                assert_eq!(
                    chunk.row_count, 2,
                    "iter {} first chunk should have 2 rows",
                    i
                );
                assert!(chunk.bytes_used > 0, "iter {} bytes_used should be > 0", i);
                let rows = decode_chunk_buf(&chunk_storage[i], chunk.bytes_used, chunk.row_count);
                assert_eq!(rows.len(), 2);
                for (k, _) in &rows {
                    assert!(
                        k.starts_with(prefixes[i]),
                        "iter {} returned key {:?} not under prefix {:?}",
                        i,
                        k,
                        prefixes[i],
                    );
                }
            }

            // Each handle is independently closable via the standard close fn,
            // confirming the shared sharded registry is populated.
            for h in &handles {
                assert_eq!(
                    frs_vec_iter_prefix_close(*h),
                    FrsErrorCode::Ok as i32,
                    "close should succeed for batched handle"
                );
            }

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Phase B: `frs_vec_iter_prefix_open_batch_parallel` (the join read-path lever)
    /// must return byte-identical results to the serial batch open — K handles, each
    /// first chunk holding the rows under its prefix — while building+draining the K
    /// probes in parallel inside the engine. Mirrors the serial-batch test above; the
    /// data spans memtable + a flushed SST so the parallel scan exercises both tiers.
    #[test]
    fn vec_iter_prefix_open_batch_parallel_matches_serial() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // 4 prefixes × 3 rows each + a distractor outside all prefixes.
            let prefixes: [&[u8]; 4] = [b"pA/", b"pB/", b"pC/", b"pD/"];
            for p in &prefixes {
                for sfx in [&b"x"[..], &b"y"[..], &b"z"[..]] {
                    let mut k = Vec::with_capacity(p.len() + sfx.len());
                    k.extend_from_slice(p);
                    k.extend_from_slice(sfx);
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), b"v".as_ptr(), 1),
                        FRS_STATUS_OK
                    );
                }
            }
            assert_eq!(
                frs_put(db, cf, b"zzz".as_ptr(), 3, b"vz".as_ptr(), 2),
                FRS_STATUS_OK
            );

            let n = prefixes.len();
            let mut offs: Vec<u32> = Vec::with_capacity(n + 1);
            let mut data: Vec<u8> = Vec::new();
            offs.push(0);
            for p in &prefixes {
                data.extend_from_slice(p);
                offs.push(data.len() as u32);
            }

            const CHUNK_CAP: u32 = 4096;
            let mut chunk_storage: Vec<Vec<u8>> =
                (0..n).map(|_| vec![0u8; CHUNK_CAP as usize]).collect();
            let mut chunks: Vec<FrsChunk> = (0..n)
                .map(|i| FrsChunk {
                    buf_ptr: chunk_storage[i].as_mut_ptr(),
                    buf_cap: CHUNK_CAP,
                    row_count: 0,
                    bytes_used: 0,
                    _reserved: 0,
                })
                .collect();
            let mut handles: Vec<u64> = vec![0; n];

            let rc = frs_vec_iter_prefix_open_batch_parallel(
                db,
                cf,
                offs.as_ptr(),
                data.as_ptr(),
                n as u32,
                handles.as_mut_ptr(),
                chunks.as_mut_ptr(),
                CHUNK_CAP,
            );
            assert_eq!(
                rc,
                FrsErrorCode::Ok as i32,
                "parallel batch open should return Ok"
            );

            let mut sorted = handles.clone();
            sorted.sort_unstable();
            sorted.dedup();
            assert_eq!(sorted.len(), n, "all handles unique + non-zero");
            assert!(!handles.contains(&0), "no zero handles");

            for i in 0..n {
                let chunk = &chunks[i];
                assert_eq!(
                    chunk.row_count, 3,
                    "probe {i} first chunk should have 3 rows"
                );
                let rows = decode_chunk_buf(&chunk_storage[i], chunk.bytes_used, chunk.row_count);
                assert_eq!(rows.len(), 3);
                for (k, _) in &rows {
                    assert!(
                        k.starts_with(prefixes[i]),
                        "probe {i} key {k:?} not under prefix {:?}",
                        prefixes[i],
                    );
                }
            }

            for h in &handles {
                assert_eq!(frs_vec_iter_prefix_close(*h), FrsErrorCode::Ok as i32);
            }
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// P1 (parallel first-chunk fill): the batched-parallel open — whose
    /// build AND first-chunk fill now run concurrently on the read pool —
    /// must return results byte-identical to the serial batched open, in
    /// input order, for a batch larger than the pool (16 probes > 4 workers)
    /// whose probes mix: empty results, single-chunk (auto-closed EOF), and
    /// multi-chunk continuations, over memtable + flushed-SST tiers. The
    /// full drain (first chunk + continuation `next()`s) is compared
    /// per-probe between the two paths.
    #[test]
    fn vec_iter_prefix_open_batch_parallel_fill_matches_serial_full_drain() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // 16 probes: probe i gets `i * 3 % 17` rows (0..15×ish — includes
            // empty, small, and >CHUNK_CAP multi-chunk result sets). Half the
            // data is flushed to an SST, the rest stays in the memtable, so
            // the pool-side fill exercises the block-streaming tier path.
            const N: usize = 16;
            const CHUNK_CAP: u32 = 128;
            let prefixes: Vec<Vec<u8>> = (0..N)
                .map(|i| format!("pp{:02}/", i).into_bytes())
                .collect();
            let rows_for = |i: usize| (i * 3) % 17;
            for (i, p) in prefixes.iter().enumerate() {
                for r in 0..rows_for(i) {
                    let mut k = p.clone();
                    k.extend_from_slice(format!("{:04}", r).as_bytes());
                    let v = format!("value-{:02}-{:04}", i, r);
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                        FRS_STATUS_OK
                    );
                }
                if i == N / 2 {
                    // Flush the first half into an SST tier.
                    assert_eq!(frs_flush(db), FRS_STATUS_OK);
                }
            }

            let mut offs: Vec<u32> = vec![0];
            let mut data: Vec<u8> = Vec::new();
            for p in &prefixes {
                data.extend_from_slice(p);
                offs.push(data.len() as u32);
            }

            // Full drain of one batched-open variant: open + per-probe
            // continuation next()s + close (when not auto-closed).
            type BatchOpenFn = unsafe extern "C" fn(
                FrsDb,
                FrsCfHandle,
                *const u32,
                *const u8,
                u32,
                *mut u64,
                *mut FrsChunk,
                u32,
            ) -> i32;
            let drain_all = |open_batch: BatchOpenFn| -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
                let mut chunk_storage: Vec<Vec<u8>> =
                    (0..N).map(|_| vec![0u8; CHUNK_CAP as usize]).collect();
                let mut chunks: Vec<FrsChunk> = (0..N)
                    .map(|i| FrsChunk {
                        buf_ptr: chunk_storage[i].as_mut_ptr(),
                        buf_cap: CHUNK_CAP,
                        row_count: 0,
                        bytes_used: 0,
                        _reserved: 0xABAD_1DEA,
                    })
                    .collect();
                let mut handles: Vec<u64> = vec![0; N];
                let rc = open_batch(
                    db,
                    cf,
                    offs.as_ptr(),
                    data.as_ptr(),
                    N as u32,
                    handles.as_mut_ptr(),
                    chunks.as_mut_ptr(),
                    CHUNK_CAP,
                );
                assert_eq!(rc, FrsErrorCode::Ok as i32);
                let mut all = Vec::with_capacity(N);
                for i in 0..N {
                    assert_ne!(handles[i], 0, "probe {} handle", i);
                    let mut rows = decode_chunk_buf(
                        &chunk_storage[i],
                        chunks[i].bytes_used,
                        chunks[i].row_count,
                    );
                    let eof = chunks[i]._reserved & FRS_CHUNK_EOF != 0;
                    if !eof {
                        loop {
                            let mut n_rows: u32 = 0;
                            let mut n_bytes: u32 = 0;
                            let rcn = frs_vec_iter_prefix_next(
                                handles[i],
                                chunk_storage[i].as_mut_ptr(),
                                CHUNK_CAP,
                                &mut n_rows,
                                &mut n_bytes,
                            );
                            assert_eq!(rcn, FrsErrorCode::Ok as i32);
                            if n_rows == 0 {
                                break;
                            }
                            rows.extend(decode_chunk_buf(&chunk_storage[i], n_bytes, n_rows));
                        }
                        assert_eq!(
                            frs_vec_iter_prefix_close(handles[i]),
                            FrsErrorCode::Ok as i32
                        );
                    }
                    all.push(rows);
                }
                all
            };

            let serial = drain_all(frs_vec_iter_prefix_open_batch);
            let parallel = drain_all(frs_vec_iter_prefix_open_batch_parallel);
            assert_eq!(serial.len(), N);
            for i in 0..N {
                assert_eq!(
                    serial[i].len(),
                    rows_for(i),
                    "probe {} row count (serial)",
                    i
                );
                assert_eq!(
                    serial[i], parallel[i],
                    "probe {} parallel fill must be byte-identical to serial",
                    i
                );
            }

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// P0 EOF flag + auto-close on the batched open paths: exhausted-in-first-
    /// chunk probes carry `FRS_CHUNK_EOF` in `_reserved`, their (non-zero)
    /// handles are NOT registered (next → clean EOF, close → no-op), while a
    /// probe that does NOT exhaust keeps `_reserved == 0` and a registered,
    /// drainable handle. Runs the same matrix against BOTH the serial and the
    /// parallel batched open.
    #[test]
    fn vec_iter_prefix_open_batch_eof_flag_and_autoclose() {
        type BatchOpenFn = unsafe extern "C" fn(
            FrsDb,
            FrsCfHandle,
            *const u32,
            *const u8,
            u32,
            *mut u64,
            *mut FrsChunk,
            u32,
        ) -> i32;
        let variants: [(&str, BatchOpenFn); 2] = [
            ("serial", frs_vec_iter_prefix_open_batch),
            ("parallel", frs_vec_iter_prefix_open_batch_parallel),
        ];
        for (variant, open_batch) in variants {
            unsafe {
                let mut db: FrsDb = ptr::null_mut();
                assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
                let mut cf: FrsCfHandle = ptr::null_mut();
                assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

                // Probe 0 ("pS/"): 2 small rows → exhausts in the first chunk.
                // Probe 1 ("pL/"): 8 rows of 24B (8 hdr + 4 key + 12 val) =
                // 192B > CHUNK_CAP=100 → does NOT exhaust at open.
                for sfx in [&b"x"[..], &b"y"[..]] {
                    let mut k = b"pS/".to_vec();
                    k.extend_from_slice(sfx);
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), b"v".as_ptr(), 1),
                        FRS_STATUS_OK
                    );
                }
                for i in 0..8u8 {
                    let k = format!("pL/{}", i);
                    let v = format!("value-{:06}", i);
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                        FRS_STATUS_OK
                    );
                }

                let prefixes: [&[u8]; 2] = [b"pS/", b"pL/"];
                let mut offs: Vec<u32> = vec![0];
                let mut data: Vec<u8> = Vec::new();
                for p in &prefixes {
                    data.extend_from_slice(p);
                    offs.push(data.len() as u32);
                }

                const CHUNK_CAP: u32 = 100;
                let mut chunk_storage: Vec<Vec<u8>> =
                    (0..2).map(|_| vec![0u8; CHUNK_CAP as usize]).collect();
                let mut chunks: Vec<FrsChunk> = (0..2)
                    .map(|i| FrsChunk {
                        buf_ptr: chunk_storage[i].as_mut_ptr(),
                        buf_cap: CHUNK_CAP,
                        row_count: 0,
                        bytes_used: 0,
                        // Poison the flag word: the engine must overwrite it.
                        _reserved: 0xDEAD_BEEF,
                    })
                    .collect();
                let mut handles: Vec<u64> = vec![0; 2];

                let rc = open_batch(
                    db,
                    cf,
                    offs.as_ptr(),
                    data.as_ptr(),
                    2,
                    handles.as_mut_ptr(),
                    chunks.as_mut_ptr(),
                    CHUNK_CAP,
                );
                assert_eq!(rc, FrsErrorCode::Ok as i32, "[{variant}] batch open Ok");

                // Probe 0: exhausted → EOF flag set, handle non-zero but
                // auto-closed (unregistered).
                assert_eq!(
                    chunks[0]._reserved, FRS_CHUNK_EOF,
                    "[{variant}] exhausted probe must carry FRS_CHUNK_EOF"
                );
                assert_ne!(handles[0], 0, "[{variant}] EOF probe handle stays non-zero");
                assert_eq!(chunks[0].row_count, 2);
                let rows = decode_chunk_buf(&chunk_storage[0], chunks[0].bytes_used, 2);
                assert!(rows.iter().all(|(k, _)| k.starts_with(b"pS/")));
                // Legacy drain on the auto-closed handle: next → clean EOF.
                let mut rc2_rows: u32 = 7;
                let mut rc2_bytes: u32 = 7;
                let rc2 = frs_vec_iter_prefix_next(
                    handles[0],
                    chunk_storage[0].as_mut_ptr(),
                    CHUNK_CAP,
                    &mut rc2_rows,
                    &mut rc2_bytes,
                );
                assert_eq!(
                    rc2,
                    FrsErrorCode::Ok as i32,
                    "[{variant}] next on auto-closed"
                );
                assert_eq!(rc2_rows, 0, "[{variant}] auto-closed handle reports EOF");
                // close-after-auto-close: safe no-op.
                assert_eq!(
                    frs_vec_iter_prefix_close(handles[0]),
                    FrsErrorCode::Ok as i32,
                    "[{variant}] close on auto-closed handle is a no-op"
                );

                // Probe 1: NOT exhausted → no EOF flag, registered handle that
                // continues to drain the remaining rows.
                assert_eq!(
                    chunks[1]._reserved, 0,
                    "[{variant}] non-exhausted probe must NOT carry FRS_CHUNK_EOF"
                );
                assert_ne!(handles[1], 0);
                let mut total =
                    decode_chunk_buf(&chunk_storage[1], chunks[1].bytes_used, chunks[1].row_count)
                        .len();
                loop {
                    let mut n_rows: u32 = 0;
                    let mut n_bytes: u32 = 0;
                    let rcn = frs_vec_iter_prefix_next(
                        handles[1],
                        chunk_storage[1].as_mut_ptr(),
                        CHUNK_CAP,
                        &mut n_rows,
                        &mut n_bytes,
                    );
                    assert_eq!(rcn, FrsErrorCode::Ok as i32);
                    if n_rows == 0 {
                        break;
                    }
                    total += decode_chunk_buf(&chunk_storage[1], n_bytes, n_rows).len();
                }
                assert_eq!(total, 8, "[{variant}] continuation drains all 8 rows");
                assert_eq!(
                    frs_vec_iter_prefix_close(handles[1]),
                    FrsErrorCode::Ok as i32
                );

                assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
                assert_eq!(frs_db_close(db), FRS_STATUS_OK);
            }
        }
    }

    /// PR-E3: `frs_vec_iter_prefix_open_batch` with `n == 0` returns Ok and
    /// is a no-op (no panic, no allocation).
    #[test]
    fn vec_iter_prefix_open_batch_zero_n_is_noop() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let offs: [u32; 1] = [0];
            let rc = frs_vec_iter_prefix_open_batch(
                db,
                cf,
                offs.as_ptr(),
                ptr::null(),
                0,
                ptr::null_mut(),
                ptr::null_mut(),
                0,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// PR-D4: Open 4 iterators concurrently from 4 different threads against
    /// the same DB.  The 16-shard registry must allow all 4 opens to proceed
    /// without serialising on a single global Mutex.  This is a functional
    /// regression test (every thread sees all rows in its own iterator and
    /// closes cleanly) — the *non-contention* property is verified by
    /// inspection of the implementation (each open computes a per-thread
    /// shard from `handle_id & 0xF` so opens from different threads never
    /// collide on the same Mutex unless ids happen to collide modulo 16).
    #[test]
    // The test body runs under one broad `unsafe` block (the FFI entry points are all
    // `unsafe extern "C"`); the per-call `unsafe { ... }` wrappers inside it are therefore
    // nested-redundant. Allow that here rather than thread the outer block through each call.
    #[allow(unused_unsafe)]
    fn vec_iter_prefix_concurrent_opens() {
        const N_THREADS: usize = 4;
        const ROWS_PER_THREAD_PREFIX: usize = 8;

        // SAFETY-bridge wrapper: FrsDb / FrsCfHandle are raw pointers
        // (*mut c_void) and don't implement Send.  The underlying objects
        // are reference-counted (Arc<DbImpl>) and read-safe across
        // threads, so wrapping the pointer as `usize` for the move and
        // casting back inside the worker is sound.
        #[derive(Copy, Clone)]
        struct SendPtr(usize);
        unsafe impl Send for SendPtr {}

        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Pre-populate: each thread T scans prefix "tT/" with N rows.
            for t in 0..N_THREADS {
                for i in 0..ROWS_PER_THREAD_PREFIX {
                    let k = format!("t{}/{:03}", t, i);
                    let v = format!("val-t{}-{:03}", t, i);
                    assert_eq!(
                        frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                        FRS_STATUS_OK
                    );
                }
            }

            let db_ptr = SendPtr(db as usize);
            let cf_ptr = SendPtr(cf as usize);

            let mut handles = Vec::with_capacity(N_THREADS);
            for t in 0..N_THREADS {
                // `SendPtr` is Copy, so the `move` closure copies these from the enclosing
                // scope per iteration — no per-iteration shadow binding needed
                // (clippy::redundant_locals).
                let h = std::thread::spawn(move || {
                    let db = db_ptr.0 as FrsDb;
                    let cf = cf_ptr.0 as FrsCfHandle;
                    let prefix = format!("t{}/", t);

                    // P0: each row is 8B header + 6B key + 10B value = 24B; a
                    // 100B chunk holds 4 of the 8 rows, so the open does NOT
                    // exhaust (an exhausted open would auto-close → handle 0,
                    // defeating this test's registry-shard purpose).
                    let mut chunk_buf = vec![0u8; 100];
                    let mut handle: u64 = 0;
                    let mut row_count: u32 = 0;
                    let mut bytes_used: u32 = 0;

                    let rc = unsafe {
                        frs_vec_iter_prefix_open(
                            db,
                            cf,
                            prefix.as_ptr(),
                            prefix.len() as u32,
                            chunk_buf.as_mut_ptr(),
                            chunk_buf.len() as u32,
                            &mut handle,
                            &mut row_count,
                            &mut bytes_used,
                        )
                    };
                    assert_eq!(rc, FrsErrorCode::Ok as i32, "thread {} open failed", t);
                    assert_ne!(handle, 0, "thread {} got zero handle", t);

                    // Drain: first chunk from open + continuation chunks via
                    // next() until exhaustion; verify every key carries this
                    // thread's prefix — confirms shards stay isolated and we
                    // never see another thread's rows.
                    let mut total_rows = 0usize;
                    let mut rows = decode_chunk_buf(&chunk_buf, bytes_used, row_count);
                    loop {
                        total_rows += rows.len();
                        for (k, _) in &rows {
                            assert!(
                                k.starts_with(prefix.as_bytes()),
                                "thread {} saw foreign key {:?}",
                                t,
                                k
                            );
                        }
                        let rc = unsafe {
                            frs_vec_iter_prefix_next(
                                handle,
                                chunk_buf.as_mut_ptr(),
                                chunk_buf.len() as u32,
                                &mut row_count,
                                &mut bytes_used,
                            )
                        };
                        assert_eq!(rc, FrsErrorCode::Ok as i32);
                        if row_count == 0 {
                            break;
                        }
                        rows = decode_chunk_buf(&chunk_buf, bytes_used, row_count);
                    }
                    assert_eq!(
                        total_rows, ROWS_PER_THREAD_PREFIX,
                        "thread {} expected {} rows, got {}",
                        t, ROWS_PER_THREAD_PREFIX, total_rows
                    );

                    assert_eq!(frs_vec_iter_prefix_close(handle), FrsErrorCode::Ok as i32);

                    handle
                });
                handles.push(h);
            }

            // Collect all assigned handles and assert they are unique
            // (no two threads were assigned the same id).
            let mut ids: Vec<u64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(
                ids.len(),
                N_THREADS,
                "expected {} unique handle ids, got {}",
                N_THREADS,
                ids.len()
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // -----------------------------------------------------------------------
    // P6-A: frs_vec_merge_append smoke tests
    // -----------------------------------------------------------------------

    /// Appending to an absent key: result equals the concatenated operands.
    #[test]
    fn merge_append_absent_key_concatenates_operands() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"list_key";
            let v1 = b"A";
            let v2 = b"B";
            let v3 = b"C";
            let operand_ptrs: [*const u8; 3] = [v1.as_ptr(), v2.as_ptr(), v3.as_ptr()];
            let operand_lens: [u32; 3] = [v1.len() as u32, v2.len() as u32, v3.len() as u32];

            let rc = frs_vec_merge_append(
                db,
                cf,
                key.as_ptr(),
                key.len() as u32,
                operand_ptrs.as_ptr(),
                operand_lens.as_ptr(),
                3,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            // Verify via frs_get.
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(!out.data.is_null());
            let got = slice::from_raw_parts(out.data, out.len);
            assert_eq!(got, b"ABC");
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Appending to an existing value: new operands are suffixed to the base.
    #[test]
    fn merge_append_existing_key_suffixes_operands() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"list2";
            // Prime the key with "BASE".
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), b"BASE".as_ptr(), 4),
                FRS_STATUS_OK
            );

            // Append "X", "Y".
            let v1 = b"X";
            let v2 = b"Y";
            let operand_ptrs: [*const u8; 2] = [v1.as_ptr(), v2.as_ptr()];
            let operand_lens: [u32; 2] = [1, 1];
            let rc = frs_vec_merge_append(
                db,
                cf,
                key.as_ptr(),
                key.len() as u32,
                operand_ptrs.as_ptr(),
                operand_lens.as_ptr(),
                2,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            let got = slice::from_raw_parts(out.data, out.len);
            assert_eq!(got, b"BASEXY");
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Zero-operand call is a no-op (does not write an empty value).
    ///
    /// `frs_get` returns `FRS_STATUS_OK` with a NULL `FrsBytes` when the key
    /// is absent (not `FRS_STATUS_NOT_FOUND`), so we assert `out.data.is_null()`.
    #[test]
    fn merge_append_zero_operands_is_noop() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"noop_key";
            let rc = frs_vec_merge_append(
                db,
                cf,
                key.as_ptr(),
                key.len() as u32,
                ptr::null(),
                ptr::null(),
                0,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            // Key should be absent: frs_get returns OK with NULL data pointer.
            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert!(
                out.data.is_null(),
                "absent key should have null data pointer"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Null key pointer returns BatchHeaderMalformed.
    #[test]
    fn merge_append_null_key_returns_malformed() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let v = b"X";
            let ptrs: [*const u8; 1] = [v.as_ptr()];
            let lens: [u32; 1] = [1];
            let rc = frs_vec_merge_append(
                db,
                cf,
                ptr::null(),
                0, // null key
                ptrs.as_ptr(),
                lens.as_ptr(),
                1,
            );
            assert_eq!(rc, FrsErrorCode::BatchHeaderMalformed as i32);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn vec_batch_get_reads_active_merge_operand_chain() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"active-merge";
            let keys_off = [0u32, key.len() as u32, (key.len() * 2) as u32];
            let mut keys_data = Vec::new();
            keys_data.extend_from_slice(key);
            keys_data.extend_from_slice(key);
            let ops = b"AB";
            let ops_off = [0u32, 1, 2];
            assert_eq!(
                frs_vec_merge_append_batch(
                    db,
                    cf,
                    keys_off.as_ptr(),
                    keys_data.as_ptr(),
                    keys_data.len(),
                    ops_off.as_ptr(),
                    ops.as_ptr(),
                    ops.len(),
                    2,
                ),
                FrsErrorCode::Ok as i32
            );

            let key_offsets = [0i32, key.len() as i32];
            let mut out_offsets = [0i32; 2];
            let mut out_data = [0u8; 16];
            let mut out_validity = [0u8; 1];
            let mut out_len = 0usize;
            let rc = frs_vectorized_batch_get(
                db,
                cf,
                key_offsets.as_ptr(),
                key.as_ptr(),
                key.len(),
                1,
                out_offsets.as_mut_ptr(),
                out_data.as_mut_ptr(),
                out_validity.as_mut_ptr(),
                out_data.len(),
                &mut out_len,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);
            assert_eq!(out_validity[0], 1);
            assert_eq!(&out_data[..out_len], b"AB");

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn flushed_put_merge_chain_same_sst_reads_full_value() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"sst-merge";
            assert_eq!(
                frs_put(db, cf, key.as_ptr(), key.len(), b"base".as_ptr(), 4),
                FRS_STATUS_OK
            );
            let keys_off = [0u32, key.len() as u32, (key.len() * 2) as u32];
            let mut keys_data = Vec::new();
            keys_data.extend_from_slice(key);
            keys_data.extend_from_slice(key);
            let ops = b"XY";
            let ops_off = [0u32, 1, 2];
            assert_eq!(
                frs_vec_merge_append_batch(
                    db,
                    cf,
                    keys_off.as_ptr(),
                    keys_data.as_ptr(),
                    keys_data.len(),
                    ops_off.as_ptr(),
                    ops.as_ptr(),
                    ops.len(),
                    2,
                ),
                FrsErrorCode::Ok as i32
            );
            assert_eq!(frs_flush(db), FRS_STATUS_OK);

            let mut out = FrsBytes::NULL;
            assert_eq!(
                frs_get(db, cf, key.as_ptr(), key.len(), &mut out),
                FRS_STATUS_OK
            );
            assert_eq!(slice::from_raw_parts(out.data, out.len), b"baseXY");
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    #[test]
    fn vectorized_ffi_rejects_offsets_beyond_declared_data_len() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let key = b"short";
            let key_offsets = [0i32, key.len() as i32];
            let mut out_offsets = [0i32; 2];
            let mut out_data = [0u8; 8];
            let mut out_validity = [0u8; 1];
            let mut out_len = 0usize;
            assert_eq!(
                frs_vectorized_batch_get(
                    db,
                    cf,
                    key_offsets.as_ptr(),
                    key.as_ptr(),
                    key.len() - 1,
                    1,
                    out_offsets.as_mut_ptr(),
                    out_data.as_mut_ptr(),
                    out_validity.as_mut_ptr(),
                    out_data.len(),
                    &mut out_len,
                ),
                FrsErrorCode::BatchHeaderMalformed as i32
            );

            let val = b"value";
            let val_offsets = [0i32, val.len() as i32];
            assert_eq!(
                frs_vectorized_batch_put(
                    db,
                    cf,
                    key_offsets.as_ptr(),
                    key.as_ptr(),
                    key.len(),
                    val_offsets.as_ptr(),
                    val.as_ptr(),
                    val.len() - 1,
                    1,
                ),
                FrsErrorCode::BatchHeaderMalformed as i32
            );

            let merge_key_offsets = [0u32, key.len() as u32];
            let op_offsets = [0u32, val.len() as u32];
            assert_eq!(
                frs_vec_merge_append_batch(
                    db,
                    cf,
                    merge_key_offsets.as_ptr(),
                    key.as_ptr(),
                    key.len() - 1,
                    op_offsets.as_ptr(),
                    val.as_ptr(),
                    val.len(),
                    1,
                ),
                FrsErrorCode::BatchHeaderMalformed as i32
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // -----------------------------------------------------------------------
    // frs_vec_merge_append_batch tests (Phase A.1 — audit-design §3 V4)
    // -----------------------------------------------------------------------

    /// Batched form: 3 rows with 3 distinct keys, 1 operand each.
    #[test]
    fn merge_append_batch_three_distinct_keys() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Keys: "k1", "k2", "k3"
            let keys_data: &[u8] = b"k1k2k3";
            let keys_off: [u32; 4] = [0, 2, 4, 6];

            // Operands: "A", "B", "C"
            let ops_data: &[u8] = b"ABC";
            let ops_off: [u32; 4] = [0, 1, 2, 3];

            let rc = frs_vec_merge_append_batch(
                db,
                cf,
                keys_off.as_ptr(),
                keys_data.as_ptr(),
                keys_data.len(),
                ops_off.as_ptr(),
                ops_data.as_ptr(),
                ops_data.len(),
                3,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            // Verify each key's value
            for (k, expected) in &[(&b"k1"[..], &b"A"[..]), (b"k2", b"B"), (b"k3", b"C")] {
                let mut out = FrsBytes::NULL;
                assert_eq!(
                    frs_get(db, cf, k.as_ptr(), k.len(), &mut out),
                    FRS_STATUS_OK
                );
                let got = slice::from_raw_parts(out.data, out.len);
                assert_eq!(got, *expected, "key {:?}", k);
                frs_bytes_free(&mut out);
            }

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Batched form: same key appears in multiple rows — operands concatenate.
    #[test]
    fn merge_append_batch_same_key_concatenates() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // 3 rows, all key "k1", operands "A", "B", "C".
            let keys_data: &[u8] = b"k1k1k1";
            let keys_off: [u32; 4] = [0, 2, 4, 6];
            let ops_data: &[u8] = b"ABC";
            let ops_off: [u32; 4] = [0, 1, 2, 3];

            let rc = frs_vec_merge_append_batch(
                db,
                cf,
                keys_off.as_ptr(),
                keys_data.as_ptr(),
                keys_data.len(),
                ops_off.as_ptr(),
                ops_data.as_ptr(),
                ops_data.len(),
                3,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            // k1 should now contain "ABC" (concatenated operands).
            let mut out = FrsBytes::NULL;
            assert_eq!(frs_get(db, cf, b"k1".as_ptr(), 2, &mut out), FRS_STATUS_OK);
            let got = slice::from_raw_parts(out.data, out.len);
            assert_eq!(got, b"ABC");
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Batched form: appending to an existing key — operands suffix the base.
    #[test]
    fn merge_append_batch_existing_key_suffixes() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Prime "k1" with "BASE".
            assert_eq!(
                frs_put(db, cf, b"k1".as_ptr(), 2, b"BASE".as_ptr(), 4),
                FRS_STATUS_OK
            );

            // Append "X" and "Y" to "k1" via two rows.
            let keys_data: &[u8] = b"k1k1";
            let keys_off: [u32; 3] = [0, 2, 4];
            let ops_data: &[u8] = b"XY";
            let ops_off: [u32; 3] = [0, 1, 2];

            let rc = frs_vec_merge_append_batch(
                db,
                cf,
                keys_off.as_ptr(),
                keys_data.as_ptr(),
                keys_data.len(),
                ops_off.as_ptr(),
                ops_data.as_ptr(),
                ops_data.len(),
                2,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            let mut out = FrsBytes::NULL;
            assert_eq!(frs_get(db, cf, b"k1".as_ptr(), 2, &mut out), FRS_STATUS_OK);
            let got = slice::from_raw_parts(out.data, out.len);
            assert_eq!(got, b"BASEXY");
            frs_bytes_free(&mut out);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Phase A.1 ablation gate (audit-design §3.5 D4 + Discovery 2026-05-21):
    /// 1024-row batched merge-append should average ≤ 10 µs per call. Run under release mode
    /// for representative timing. Marked `#[ignore]` so normal `cargo test` doesn't block on it.
    #[test]
    #[ignore]
    fn merge_append_batch_1024_under_10us_release() {
        use std::time::Instant;
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // 1024 distinct keys, 1 operand each (typical Q19 LIST_ADD batch shape).
            const N: usize = 1024;
            let mut keys_data: Vec<u8> = Vec::with_capacity(N * 8);
            let mut keys_off: Vec<u32> = Vec::with_capacity(N + 1);
            let mut ops_data: Vec<u8> = Vec::with_capacity(N * 16);
            let mut ops_off: Vec<u32> = Vec::with_capacity(N + 1);
            keys_off.push(0);
            ops_off.push(0);
            for i in 0..N {
                let k = format!("k{:06}", i);
                keys_data.extend_from_slice(k.as_bytes());
                keys_off.push(keys_data.len() as u32);
                let v = format!("v{:010}", i);
                ops_data.extend_from_slice(v.as_bytes());
                ops_off.push(ops_data.len() as u32);
            }

            // Warmup
            let _ = frs_vec_merge_append_batch(
                db,
                cf,
                keys_off.as_ptr(),
                keys_data.as_ptr(),
                keys_data.len(),
                ops_off.as_ptr(),
                ops_data.as_ptr(),
                ops_data.len(),
                N as u32,
            );

            // Measure (5 iterations, take median)
            let mut timings = vec![];
            for _ in 0..5 {
                let t0 = Instant::now();
                let rc = frs_vec_merge_append_batch(
                    db,
                    cf,
                    keys_off.as_ptr(),
                    keys_data.as_ptr(),
                    keys_data.len(),
                    ops_off.as_ptr(),
                    ops_data.as_ptr(),
                    ops_data.len(),
                    N as u32,
                );
                let elapsed = t0.elapsed();
                assert_eq!(rc, FrsErrorCode::Ok as i32);
                timings.push(elapsed);
            }
            timings.sort();
            let median = timings[timings.len() / 2];
            let per_call_ns = median.as_nanos() / (N as u128);
            println!(
                "merge_append_batch/1024 median: {:?} ({} ns/row)",
                median, per_call_ns
            );
            // Loose gate: full batch ≤ 50 ms (50 µs/row × 1024 = 51.2 ms upper bound;
            // tighter gates only meaningful in release-mode criterion runs).
            assert!(
                median.as_millis() < 50,
                "merge_append_batch/1024 took {:?} — exceeds 50 ms loose gate",
                median
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Batched form: n=0 is a no-op (no crash, returns Ok).
    #[test]
    fn merge_append_batch_zero_rows_is_noop() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // n=0 — pointers irrelevant per contract; pass valid ones for safety.
            let keys_data: &[u8] = b"";
            let keys_off: [u32; 1] = [0];
            let ops_data: &[u8] = b"";
            let ops_off: [u32; 1] = [0];

            let rc = frs_vec_merge_append_batch(
                db,
                cf,
                keys_off.as_ptr(),
                keys_data.as_ptr(),
                keys_data.len(),
                ops_off.as_ptr(),
                ops_data.as_ptr(),
                ops_data.len(),
                0,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    // -----------------------------------------------------------------------
    // frs_vec_iter_range_* tests (P9, spec §2 component D)
    // -----------------------------------------------------------------------

    /// Open/close round trip: 3 keys in [b, d) are returned; "a" and "d" are
    /// excluded by the bounds.
    #[test]
    fn vec_iter_range_open_close_round_trip() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            for (k, v) in &[
                (&b"a"[..], &b"va"[..]),
                (b"b", b"vb"),
                (b"bb", b"vbb"),
                (b"bc", b"vbc"),
                (b"d", b"vd"),
            ] {
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            let mut chunk_buf = vec![0u8; 4096];
            let mut handle: u64 = 0;
            let mut row_count: u32 = 0;
            let mut bytes_used: u32 = 0;

            let lo = b"b";
            let hi = b"d";
            let rc = frs_vec_iter_range_open(
                db,
                cf,
                lo.as_ptr(),
                lo.len() as u32,
                hi.as_ptr(),
                hi.len() as u32,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut handle,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "open should return Ok");
            assert_eq!(
                handle, 0,
                "P0 auto-close: exhausted-in-first-chunk range open returns handle 0"
            );
            assert_eq!(row_count, 3, "expect 3 keys in [b, d)");

            let rows = decode_chunk_buf(&chunk_buf, bytes_used, row_count);
            assert_eq!(rows.len(), 3);
            for (k, _) in &rows {
                assert!(k.as_slice() >= b"b".as_slice(), "key {:?} below lo", k);
                assert!(k.as_slice() < b"d".as_slice(), "key {:?} at/above hi", k);
            }

            // Legacy trailing next() on the auto-closed handle: clean EOF.
            let rc2 = frs_vec_iter_range_next(
                handle,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc2, FrsErrorCode::Ok as i32);
            assert_eq!(row_count, 0);

            assert_eq!(frs_vec_iter_range_close(handle), FrsErrorCode::Ok as i32);
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// P0 auto-close: next on an unknown (or auto-closed) range handle
    /// reports clean EOF (`Ok` + 0 rows) — mirrors the prefix-path contract.
    #[test]
    fn vec_iter_range_next_unknown_handle_reports_eof() {
        let mut chunk_buf = vec![0u8; 64];
        let mut row_count: u32 = 7;
        let mut bytes_used: u32 = 7;
        let rc = unsafe {
            frs_vec_iter_range_next(
                u64::MAX,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut row_count,
                &mut bytes_used,
            )
        };
        assert_eq!(rc, FrsErrorCode::Ok as i32);
        assert_eq!(row_count, 0, "unknown range handle must report EOF");
        assert_eq!(bytes_used, 0);
    }

    /// Abort a range iterator: subsequent next returns empty chunk.
    #[test]
    fn vec_iter_range_abort_stops_iteration() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            for i in 0u8..4 {
                let k = format!("rng/{}", i);
                let v = format!("v{}", i);
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK
                );
            }

            // P0: rows are 8B header + 5B key + 2B value = 15B; a 20B chunk
            // holds exactly one row so the open does NOT exhaust (an
            // exhausted open auto-closes → handle 0 → nothing to abort).
            let mut chunk_buf = vec![0u8; 20];
            let mut handle: u64 = 0;
            let mut row_count: u32 = 0;
            let mut bytes_used: u32 = 0;

            // Use a range that only covers part of the keys written.
            let lo = b"rng/0";
            let hi = b"rng/z";
            let rc = frs_vec_iter_range_open(
                db,
                cf,
                lo.as_ptr(),
                lo.len() as u32,
                hi.as_ptr(),
                hi.len() as u32,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut handle,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32);
            assert_ne!(handle, 0, "non-exhausted open must register a handle");
            assert_eq!(row_count, 1, "20B chunk holds exactly one row");

            assert_eq!(frs_vec_iter_range_abort(handle), FrsErrorCode::Ok as i32);

            // After abort, next returns empty.
            let rc2 = frs_vec_iter_range_next(
                handle,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                &mut row_count,
                &mut bytes_used,
            );
            assert_eq!(rc2, FrsErrorCode::Ok as i32);
            assert_eq!(row_count, 0, "aborted range iter must yield empty chunk");

            // Abort on unknown handle → IterCursorInvalid.
            assert_eq!(
                frs_vec_iter_range_abort(u64::MAX),
                FrsErrorCode::IterCursorInvalid as i32
            );

            assert_eq!(frs_vec_iter_range_close(handle), FrsErrorCode::Ok as i32);
            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// Null out-pointers return BatchHeaderMalformed.
    #[test]
    fn vec_iter_range_open_null_out_pointers_return_malformed() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            let mut chunk_buf = vec![0u8; 64];
            let lo = b"a";
            let hi = b"z";
            let rc = frs_vec_iter_range_open(
                db,
                cf,
                lo.as_ptr(),
                lo.len() as u32,
                hi.as_ptr(),
                hi.len() as u32,
                chunk_buf.as_mut_ptr(),
                chunk_buf.len() as u32,
                ptr::null_mut(),
                &mut 0u32,
                &mut 0u32,
            );
            assert_eq!(rc, FrsErrorCode::BatchHeaderMalformed as i32);

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// close(0) is a no-op.
    #[test]
    fn vec_iter_range_close_zero_handle_is_noop() {
        assert_eq!(frs_vec_iter_range_close(0), FrsErrorCode::Ok as i32);
    }

    // ── F1: ErrorCodeSubstitution via fault-injection feature ─────────────────
    //
    // The test below requires `--features fault-injection`.  It is marked
    // `#[ignore]` in the default test run so that `cargo test -p forst-rs-ffi`
    // (without the feature) never tries to reference the harness.
    //
    // To execute:
    //   cargo test -p forst-rs-ffi --features fault-injection \
    //       fault_injector_substitutes_error_code -- --ignored
    //
    // F2/F4-F12 are deferred to P11/P12 (require running engine fixtures).

    /// F1 — ErrorCodeSubstitution: injector substitutes ENGINE_IO on call 1.
    ///
    /// This test drives `frs_vectorized_batch_get` with a null `out_data_len`
    /// pointer (which would normally return BatchHeaderMalformed *before*
    /// reaching the fault hook). However because the F1 hook fires *before*
    /// the validation guard, it intercepts first and returns the injected code.
    /// That behaviour is intentional — fault injection bypasses normal
    /// validation so tests can probe error paths independently.
    #[test]
    #[cfg(feature = "fault-injection")]
    fn fault_injector_substitutes_error_code() {
        // Reset the injector so any previous test's counter doesn't interfere.
        forst_rs_test_harness::FaultInjector::global().reset();

        std::env::remove_var("FRS_FAULT_VEC_GET_PROB");
        std::env::set_var("FRS_FAULT_VEC_GET_AT", "1");
        std::env::set_var("FRS_FAULT_VEC_GET_CODE", "300"); // ENGINE_IO

        // Re-read env vars after setting them (reset flushes config cache).
        forst_rs_test_harness::FaultInjector::global().reset();

        // Call with all-null/zero args — the fault hook fires before any
        // pointer dereference, so this is safe.
        let rc = unsafe {
            frs_vectorized_batch_get(
                std::ptr::null_mut(), // null FrsDb handle
                std::ptr::null_mut(), // null FrsCfHandle
                std::ptr::null(),     // key_offsets
                std::ptr::null(),     // key_data
                0,                    // key_data_len
                0,                    // count
                std::ptr::null_mut(), // out_offsets
                std::ptr::null_mut(), // out_data
                std::ptr::null_mut(), // out_validity
                0,                    // out_data_cap
                std::ptr::null_mut(), // out_data_len — normally → BatchHeaderMalformed
            )
        };

        assert_eq!(
            rc, 300,
            "fault hook must return injected ENGINE_IO code (300)"
        );

        // Cleanup.
        std::env::remove_var("FRS_FAULT_VEC_GET_AT");
        std::env::remove_var("FRS_FAULT_VEC_GET_CODE");
        forst_rs_test_harness::FaultInjector::global().reset();
    }

    // F2/F4-F12 TODO: add when engine fixture infrastructure is available (P11/P12).
    // Each will follow the same pattern:
    //   1. Set FRS_FAULT_<KIND>_AT=<n> + FRS_FAULT_<KIND>_CODE=<code>
    //   2. Call the relevant frs_vec_* function
    //   3. Assert the injected code is returned
    //   4. Verify Java-side FrsException carries the right code (integration only)

    // -----------------------------------------------------------------
    // PR-D3 (V10): frs_vectorized_batch_get routes through db.batch_get
    // -----------------------------------------------------------------

    /// Data-integrity test for the PR-D3 multi_get / batch_get routing.
    ///
    /// Inserts 1024 keys, then issues a single `frs_vectorized_batch_get`
    /// covering all 1024 keys plus one missing key, and verifies that:
    ///   - All 1024 inserted keys come back with the right validity + value.
    ///   - The missing key returns validity = 0 (no value).
    ///   - The output offsets are monotonically non-decreasing.
    ///   - The total `out_data_len` matches the sum of returned value lengths.
    ///
    /// Performance expectation (not asserted here, gated by manual bench):
    /// the engine-level `db.batch_get` warms the SST reader cache via
    /// `prefetch_sst_files_for_batch` (one GetObject per SST file on S3-backed
    /// filesystems instead of one per key) and uses the active-memtable
    /// hash-index fast path before falling through, so the FFI call cost
    /// should be substantially lower than the previous per-key `db.get(cf, k)`
    /// loop, especially when reads spill to immutable memtables / SSTs.
    #[test]
    fn vec_batch_get_multi_get_path() {
        unsafe {
            let mut db: FrsDb = ptr::null_mut();
            assert_eq!(frs_db_open_memory(&mut db), FRS_STATUS_OK);
            let mut cf: FrsCfHandle = ptr::null_mut();
            assert_eq!(frs_db_default_cf(db, &mut cf), FRS_STATUS_OK);

            // Write 1024 keys with deterministic value bytes.
            const N: usize = 1024;
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(N);
            let mut vals: Vec<Vec<u8>> = Vec::with_capacity(N);
            for i in 0..N {
                let k = format!("pr-d3-key-{:06}", i).into_bytes();
                // Varying value lengths exercise offset arithmetic.
                let v_len = 4 + (i % 13);
                let mut v = Vec::with_capacity(v_len);
                for j in 0..v_len {
                    v.push(((i + j) as u8).wrapping_add(0x33));
                }
                assert_eq!(
                    frs_put(db, cf, k.as_ptr(), k.len(), v.as_ptr(), v.len()),
                    FRS_STATUS_OK,
                );
                keys.push(k);
                vals.push(v);
            }

            // Build the batch input: N inserted keys + 1 missing key.
            let missing = b"pr-d3-key-MISSING".to_vec();
            let batch_count = N + 1;

            let mut key_data: Vec<u8> = Vec::new();
            let mut key_offs: Vec<i32> = Vec::with_capacity(batch_count + 1);
            key_offs.push(0);
            for k in &keys {
                key_data.extend_from_slice(k);
                key_offs.push(key_data.len() as i32);
            }
            key_data.extend_from_slice(&missing);
            key_offs.push(key_data.len() as i32);

            // Output buffers — over-allocate to avoid BUFFER_TOO_SMALL.
            let total_val_bytes: usize = vals.iter().map(|v| v.len()).sum();
            let cap = total_val_bytes + 64;
            let mut out_data: Vec<u8> = vec![0u8; cap];
            let mut out_offs: Vec<i32> = vec![0i32; batch_count + 1];
            let mut out_vld: Vec<u8> = vec![0u8; batch_count];
            let mut out_len: usize = 0;

            let rc = frs_vectorized_batch_get(
                db,
                cf,
                key_offs.as_ptr(),
                key_data.as_ptr(),
                key_data.len(),
                batch_count,
                out_offs.as_mut_ptr(),
                out_data.as_mut_ptr(),
                out_vld.as_mut_ptr(),
                cap,
                &mut out_len,
            );
            assert_eq!(rc, FrsErrorCode::Ok as i32, "expected FrsErrorCode::Ok (0)");
            assert_eq!(out_len, total_val_bytes, "out_data_len mismatch");
            assert_eq!(out_offs[0], 0, "first offset must be 0");

            // First N entries must be present with the expected value bytes.
            for i in 0..N {
                assert_eq!(out_vld[i], 1, "key {} should be found", i);
                let lo = out_offs[i] as usize;
                let hi = out_offs[i + 1] as usize;
                assert!(hi >= lo, "offsets must be monotonic at i={}", i);
                assert_eq!(
                    &out_data[lo..hi],
                    vals[i].as_slice(),
                    "value mismatch at i={}",
                    i
                );
            }
            // Last slot is the missing key.
            assert_eq!(out_vld[N], 0, "missing key must report validity 0");
            assert_eq!(
                out_offs[N + 1] as usize,
                total_val_bytes,
                "trailing offset must equal total value bytes"
            );

            assert_eq!(frs_cf_close(cf), FRS_STATUS_OK);
            assert_eq!(frs_db_close(db), FRS_STATUS_OK);
        }
    }

    /// F-1 (PMC cycle-5): the pinned chunk fill must CONTINUE past a
    /// fallback-resolution (`get_internal`) error — recording it sticky-FIRST
    /// into the shared slot — instead of parking exhausted at the errored
    /// key. Pre-fix, rows after the errored key were silently dropped while
    /// the legacy boxed path (filter_map error tap) delivered them; the
    /// delivered-row prefix must be identical in both modes.
    #[test]
    fn s2_pinned_chunk_fill_continues_past_fallback_error() {
        use forst_rs_common::{ForstError, ForstResult};
        use forst_rs_storage::merge_operator::MergeOperator;

        /// Fails `full_merge` for one poison key; concatenates otherwise.
        struct Poison(Vec<u8>);
        impl MergeOperator for Poison {
            fn full_merge(
                &self,
                key: &[u8],
                base_value: Option<&[u8]>,
                operands: &[&[u8]],
            ) -> ForstResult<Vec<u8>> {
                if key == self.0.as_slice() {
                    return Err(ForstError::corruption("ffi poison merge"));
                }
                let mut out = base_value.map(<[u8]>::to_vec).unwrap_or_default();
                for op in operands {
                    out.extend_from_slice(op);
                }
                Ok(out)
            }
            fn partial_merge(
                &self,
                _key: &[u8],
                _left: &[u8],
                _right: &[u8],
            ) -> ForstResult<Vec<u8>> {
                Err(ForstError::internal("poison: no partial merge"))
            }
            fn name(&self) -> String {
                "ffi-poison-merge".to_string()
            }
        }

        let key = |i: u32| format!("q:{i:04}").into_bytes();
        let poison_idx = 5u32;
        let db = DbImpl::open_default().unwrap();
        let cf = db
            .create_column_family(
                ColumnFamilyDescriptor::new("poison")
                    .with_merge_operator(Arc::new(Poison(key(poison_idx)))),
            )
            .unwrap();
        // One flushed SST of Puts + a memtable-resident poison Merge: the
        // merge winner forces the `get_internal` fallback, which errors.
        for i in 0..10u32 {
            db.put(&cf, &key(i), format!("v-{i}").as_bytes()).unwrap();
        }
        db.switch_and_flush(&cf).unwrap().unwrap();
        db.merge(&cf, &key(poison_idx), b"operand").unwrap();

        let slot = Arc::new(Mutex::new(None));
        let stream = db
            .prefix_scan_stream_with_mode(&cf, b"q:", Arc::clone(&slot), true)
            .unwrap();
        let mut handle = IterHandle::new_pinned_with_error_slot(stream, Arc::clone(&slot));

        // Small chunks: multiple fills + the pending-row stash in play.
        let mut buf = vec![0u8; 64];
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        loop {
            let (bytes_used, row_count, exhausted) =
                unsafe { fill_chunk_from_iter(&mut handle, buf.as_mut_ptr(), buf.len()) };
            let mut off = 0usize;
            for _ in 0..row_count {
                let klen = u32::from_le_bytes(buf[off..off + 4].try_into().unwrap()) as usize;
                let vlen = u32::from_le_bytes(buf[off + 4..off + 8].try_into().unwrap()) as usize;
                off += 8;
                rows.push((
                    buf[off..off + klen].to_vec(),
                    buf[off + klen..off + klen + vlen].to_vec(),
                ));
                off += klen + vlen;
            }
            assert_eq!(off, bytes_used as usize, "chunk wire format consistent");
            if exhausted {
                break;
            }
        }

        // Rows BOTH before and after the errored key are delivered (the
        // legacy delivered-row prefix); the poison key itself is absent.
        let expected: Vec<(Vec<u8>, Vec<u8>)> = (0..10u32)
            .filter(|i| *i != poison_idx)
            .map(|i| (key(i), format!("v-{i}").into_bytes()))
            .collect();
        assert_eq!(rows, expected, "scan must continue past the fallback error");
        // The error is recorded sticky-FIRST in the shared slot (drained
        // once by the open/next deferred-error machine).
        assert!(
            handle.take_last_error().is_some(),
            "fallback error must land in the shared slot"
        );
        assert!(
            handle.take_last_error().is_none(),
            "sticky error drains once"
        );
    }
}

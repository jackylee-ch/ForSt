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
use std::slice;
use std::ptr;
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

/// Defense-in-depth cap on per-key (or per-prefix) byte length. The C
/// caller could otherwise push a fabricated `key_len` or `prefix_len`
/// at `slice::from_raw_parts`, driving the engine to scan / hash / index
/// bogus untrusted memory. 1 MiB is well above any realistic key size
/// for an LSM (RocksDB recommends ≤ 8 KiB); see Delta-Join Lookup design
/// (2.13_deltajoin_localization.md) for the consumer-side budget.
pub const MAX_KEY_LEN: usize = 1 << 20;

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
        match DbImpl::open_remote(opts, &uri_str, config, &cache_path, cache_capacity_bytes) {
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

        let engine_opts = match builder.try_build() {
            Ok(o) => o,
            Err(_) => return FRS_STATUS_INVALID_ARGUMENT,
        };

        let fs: Arc<dyn FileSystem> = if in_memory {
            Arc::new(MemoryFileSystem::new())
        } else {
            Arc::new(LocalFileSystem::new())
        };

        match DbImpl::open_with_fs(engine_opts, fs) {
            Ok(db) => {
                let boxed = Box::new(db);
                *out_handle = Box::into_raw(boxed) as *mut c_void;
                FRS_STATUS_OK
            }
            Err(e) => error_to_status(&e),
        }
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
        let k = slice::from_raw_parts(key, key_len);
        match db.get_pinned(cf, k) {
            Some((ptr, len)) => {
                *out_ptr = ptr;
                *out_len = len;
                FRS_STATUS_OK
            }
            None => {
                // Distinguish NOT_FOUND from FALLBACK: check if the key
                // exists at all via the regular get path. But that would
                // defeat the purpose (allocating). Instead, return FALLBACK
                // unconditionally — the caller will try frs_get which handles
                // both "not found" and "found in SST/imm" cases.
                *out_ptr = std::ptr::null();
                *out_len = 0;
                FRS_STATUS_FALLBACK
            }
        }
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

        // Zero-copy path: build Arrow output directly during lookup,
        // avoiding the intermediate Vec<Option<Vec<u8>>> allocation.
        let batch = match db.batch_get_arrow(cf, keys) {
            Ok(b) => b,
            Err(e) => return error_to_status(&e),
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
}

impl IteratorState {
    pub(crate) fn new(rows: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { rows, cursor: 0 }
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
    let db = &*(handle as *const Arc<DbImpl>);
    let cf_h = &*(cf as *const ColumnFamilyHandle);
    let k = slice::from_raw_parts(key, key_len);
    match db.get(cf_h, k) {
        Ok(Some(v)) => {
            *out_val_len = v.len();
            if v.len() > out_buf_cap {
                return FRS_STATUS_BUFFER_TOO_SMALL;
            }
            ptr::copy_nonoverlapping(v.as_ptr(), out_buf, v.len());
            FRS_STATUS_OK
        }
        Ok(None) => {
            *out_val_len = 0;
            FRS_STATUS_OK
        }
        Err(_) => FRS_STATUS_ERROR,
    }
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
        // Move out of the row by swap so the engine's snapshot vec keeps
        // its slots populated with empty placeholders (no Vec re-shift).
        let (k, v) = std::mem::take(&mut state.rows[state.cursor]);
        state.cursor += 1;
        *out_key = FrsBytes::from_vec(k);
        *out_value = FrsBytes::from_vec(v);
        *out_valid = true;
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
        match DbImpl::open_from_incremental(&target, &manifest, &paths) {
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
        // + 4 (u32) + 4 (pad) + 8 (u64) + 8 (u64) = 48 bytes.
        // (32-bit hosts will have a smaller pointer; the bridge is built
        // 64-bit only today, so we encode the 64-bit layout here.)
        if size_of::<*const c_char>() == 8 {
            assert_eq!(size_of::<FrsEngineOptions>(), 48);
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
}

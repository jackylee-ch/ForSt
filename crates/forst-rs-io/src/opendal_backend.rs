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

//! OpenDAL-backed [`FileSystem`] implementation.
//!
//! This backend bridges ForSt-RS's synchronous [`FileSystem`] trait onto
//! [`opendal::Operator`], giving the engine a single interface for local
//! filesystems, in-memory storage, S3, GCS, Azure Blob, etc. Concrete
//! services are gated by Cargo features in `opendal` (`services-fs`,
//! `services-memory`, `services-s3` are enabled by default in this crate).
//!
//! # Async ↔ sync bridge
//!
//! [`opendal::Operator`] is async by design. The [`FileSystem`] trait is
//! synchronous, so [`OpendalFileSystem`] internally drives an async
//! [`tokio::runtime::Runtime`] via `block_on`. The runtime selection
//! protocol is:
//!
//! 1. If a Tokio runtime is already running on the current thread (i.e.
//!    [`tokio::runtime::Handle::try_current`] succeeds), use that handle —
//!    no new runtime is created. The caller must NOT call into this
//!    backend from inside an async task on a single-threaded runtime
//!    (deadlock); a multi-threaded runtime is safe.
//! 2. Otherwise, [`OpendalFileSystem`] owns a private multi-threaded
//!    runtime created on construction, and all operations execute on it.
//!
//! # File model
//!
//! - [`OpendalSequentialFile`]: streams the object through OpenDAL's blocking
//!   reader, keeping memory bounded by OpenDAL's internal chunks.
//! - [`OpendalRandomAccessFile`]: serves [`RandomAccessFile::read_at`] via
//!   `op.read_with(path).range(off..off+len)`, performing one ranged GET
//!   per call.
//! - [`OpendalWritableFile`]: streams writes through OpenDAL's blocking
//!   writer, enabling multipart/object-store uploads without retaining the
//!   full SST in Rust heap memory.
//!
//! # Path handling
//!
//! All paths are converted to `&str` via `Path::to_str()`. Non-UTF-8
//! paths are rejected with [`ForstError::invalid_argument`]. Directory
//! operations append a trailing `/` to satisfy OpenDAL's convention.

use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use std::time::Duration;

use bytes::Buf;
use forst_rs_common::error::{ForstError, ForstResult};
use opendal::layers::{BlockingLayer, RetryLayer};
use opendal::{ErrorKind as OdErrorKind, Executor, Metakey, Operator};
use tokio::runtime::{Handle, Runtime};
use tokio::sync::Semaphore;

use crate::filesystem::{
    FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode,
};

// ---------------------------------------------------------------------------
// Runtime bridge
// ---------------------------------------------------------------------------

/// A handle to whichever Tokio runtime should drive the OpenDAL operator.
///
/// We deliberately avoid storing a runtime *and* a handle simultaneously:
/// either we attach to a borrowed handle (no `Runtime` field) or we own a
/// runtime that we created ourselves. The `Owned` variant keeps the
/// runtime alive for the lifetime of the filesystem.
#[derive(Debug)]
enum RuntimeHandle {
    /// Use the caller's pre-existing runtime.
    Borrowed(Handle),
    /// We created and own this runtime.
    Owned(Arc<Runtime>),
}

impl RuntimeHandle {
    /// Returns the [`Handle`] for whichever runtime is in use.
    fn handle(&self) -> Handle {
        match self {
            RuntimeHandle::Borrowed(h) => h.clone(),
            RuntimeHandle::Owned(rt) => rt.handle().clone(),
        }
    }

    /// Acquires a runtime: prefer the current one if any, else build a fresh
    /// multi-threaded runtime.
    fn acquire() -> ForstResult<Self> {
        if let Ok(h) = Handle::try_current() {
            Ok(RuntimeHandle::Borrowed(h))
        } else {
            // Fall back to a dedicated multi-threaded runtime. We use a small
            // worker pool because most ForSt-RS callers will already have
            // their own runtime; the owned one is just for tests and
            // single-shot CLIs.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .thread_name("forst-rs-opendal")
                .build()
                .map_err(|e| {
                    ForstError::Io(std::io::Error::other(format!(
                        "OpendalFileSystem: failed to build dedicated tokio runtime: {e}"
                    )))
                })?;
            Ok(RuntimeHandle::Owned(Arc::new(rt)))
        }
    }
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

/// Converts an [`opendal::Error`] into a [`ForstError`], preserving the
/// `ErrorKind::NotFound` variant so that callers can use the engine's
/// usual `is_not_found()` predicate.
fn map_opendal_err(err: opendal::Error, context: &str) -> ForstError {
    match err.kind() {
        OdErrorKind::NotFound => ForstError::not_found(format!("{context}: {err}")),
        OdErrorKind::AlreadyExists => ForstError::invalid_argument(format!("{context}: {err}")),
        OdErrorKind::Unsupported => ForstError::not_supported(format!("{context}: {err}")),
        OdErrorKind::PermissionDenied => ForstError::Io(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!("{context}: {err}"),
        )),
        _ => ForstError::Io(std::io::Error::other(format!("{context}: {err}"))),
    }
}

/// Converts a [`Path`] to `&str`, returning [`ForstError::invalid_argument`]
/// if it is not valid UTF-8. ForSt-RS requires UTF-8 paths everywhere.
fn path_str<'a>(path: &'a Path, context: &str) -> ForstResult<&'a str> {
    path.to_str().ok_or_else(|| {
        ForstError::invalid_argument(format!(
            "{context}: path is not valid UTF-8: {}",
            path.display()
        ))
    })
}

// ---------------------------------------------------------------------------
// OpendalFileSystem
// ---------------------------------------------------------------------------

/// A [`FileSystem`] backed by [`opendal::Operator`].
///
/// Use [`OpendalFileSystem::with_operator`] to wrap an existing operator,
/// or one of the convenience constructors ([`OpendalFileSystem::memory`],
/// [`OpendalFileSystem::local`], [`OpendalFileSystem::s3`]) to build one
/// from common settings.
/// 2026-05-29 WRITE-BACK FLUSH: outcome of one async upload, broadcast to ALL
/// awaiters. `Result<(), String>` (not `ForstResult`) because the value must be
/// `Clone` to fan out through a `watch` channel and `ForstError` is not `Clone`.
/// Upload failures are only ever Io/corruption (never NotFound), so collapsing
/// the error to a string and re-wrapping as `ForstError::Io` in `await_upload`
/// loses no semantically-meaningful classification.
type UploadOutcome = Result<(), String>;

/// 2026-05-29 WRITE-BACK FLUSH: registry of in-flight asynchronous SST/MANIFEST
/// uploads, keyed by the object path.
///
/// 2026-05-29 RACE FIX: previously stored a single-consume `JoinHandle`, which
/// `await_upload` would `remove()` then join OUTSIDE the lock. Under concurrent
/// readers (parallelism ≥ 2) a second `await_upload(P)` arriving after the
/// `remove` but before the join completed found NO handle, returned `Ok(())`
/// early, and read the S3 object that the first awaiter's upload had NOT yet
/// finished writing → `NotFound` → fatal `frs_vectorized_batch_get rc=1`. Now a
/// `watch::Receiver` whose value transitions `None → Some(outcome)` when the
/// spawned upload task completes; EVERY awaiter clones the receiver and blocks
/// until the outcome is published, so no awaiter can race ahead of the upload.
type PendingUploads =
    Arc<Mutex<HashMap<String, tokio::sync::watch::Receiver<Option<UploadOutcome>>>>>;

/// 2026-05-29 WRITE-BACK FLUSH: cap on concurrent in-flight buffered uploads.
/// Each spawned upload acquires one permit before touching S3 and releases it
/// on completion, providing backpressure so a slow S3 endpoint cannot let an
/// unbounded number of serialized SSTs accumulate in memory (each in-flight
/// upload holds its whole buffered SST plus ~`S3_WRITE_CONCURRENCY *
/// S3_WRITE_CHUNK_BYTES` of multipart parts). 8 concurrent SST uploads bounds
/// resident upload memory at roughly `8 * (SST_size + 128 MiB)`.
const MAX_INFLIGHT_UPLOADS: usize = 8;

pub struct OpendalFileSystem {
    op: Operator,
    rt: RuntimeHandle,
    name: String,
    /// 2026-05-29 WRITE-BACK FLUSH: in-flight async upload registry. Shared
    /// (cloned) into every [`OpendalWritableFile`] so the writer can register
    /// its spawned upload, and consulted by `await_upload`/`await_all_uploads`.
    pending: PendingUploads,
    /// 2026-05-29 WRITE-BACK FLUSH: backpressure semaphore limiting concurrent
    /// in-flight buffered uploads to [`MAX_INFLIGHT_UPLOADS`].
    upload_sem: Arc<Semaphore>,
}

impl std::fmt::Debug for OpendalFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalFileSystem")
            .field("scheme", &self.op.info().scheme().into_static())
            .field("name", &self.name)
            .finish()
    }
}

/// Default retry policy applied to every [`OpendalFileSystem`] (PR-A12).
///
/// Transient S3 / GCS / Azure failures (`5xx`, throttling, RST connections)
/// are very common at scale: a checkpoint that writes N SSTs in parallel
/// will see at least one transient fault per ckpt once N grows past a
/// dozen. Fail-fast on the first retry-able error multiplies failure
/// probability across SSTs and produces a high per-ckpt failure rate.
///
/// We attach an [`opendal::layers::RetryLayer`] to every constructed
/// operator with parameters chosen to match Flink's
/// `ExponentialBackoffDelayRetryStrategyBuilder` defaults on the Java
/// side:
///
/// - `max_times = 5` — five retries → six total attempts.
/// - `min_delay = 100ms`, `factor = 2.0` — 100 / 200 / 400 / 800 / 1600 ms.
/// - `max_delay = 30s` — clamps the exponential schedule.
/// - `jitter` — adds ±50% noise to each delay to prevent thundering
///   herds when many SSTs retry simultaneously.
///
/// OpenDAL classifies errors as retryable internally
/// ([`opendal::Error::is_temporary`]) so non-transient failures
/// (NotFound, PermissionDenied, …) still fail fast.
fn default_retry_layer() -> RetryLayer {
    RetryLayer::new()
        .with_max_times(5)
        .with_factor(2.0)
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_jitter()
}

/// R45-M1: extract host string from an HTTP authority component (after
/// `http://` and before any path). Handles both `host[:port]` and the
/// IPv6 bracketed form `[v6addr][:port]`.
///
/// Examples:
/// - `"127.0.0.1:9000"` → `"127.0.0.1"`
/// - `"localhost"`      → `"localhost"`
/// - `"[::1]:9000"`     → `"::1"` (brackets stripped)
/// - `"[::1]"`          → `"::1"`
/// - `"[2001:db8::1]:9000"` → `"2001:db8::1"`
///
/// Falls back to returning the input slice unchanged for malformed
/// inputs (e.g. a `[` with no closing `]`) — the caller treats it as a
/// non-loopback host and emits the warning anyway, which is the correct
/// conservative behavior.
fn extract_host_from_authority(authority: &str) -> &str {
    if let Some(rest) = authority.strip_prefix('[') {
        // IPv6 literal: the host runs to the closing `]`.
        if let Some(end) = rest.find(']') {
            return &rest[..end];
        }
        // Malformed (no closing bracket): return as-is.
        return authority;
    }
    // R46-L1: an UNBRACKETED IPv6 literal like `::1` or
    // `2001:db8::1` (no `[]`, no port) carries more than one `:`. RFC
    // 3986 §3.2.2 requires IPv6 hosts to be bracketed when a port is
    // present — so authority strings with multiple colons but no
    // brackets are either malformed (port-implying) or unbracketed
    // literals. Either way, naively trimming at the rightmost colon
    // would butcher the address. RFC 6874 / 3986 say: when in doubt,
    // do not strip. We mirror that conservative behaviour and return
    // the input unchanged (loopback detection for `::1` then works
    // correctly because `extract_host_from_authority("::1") == "::1"`).
    //
    // Single-colon strings remain `host:port`-shaped (IPv4 or DNS
    // name + port) and are stripped as before.
    if authority.bytes().filter(|&b| b == b':').count() > 1 {
        return authority;
    }
    // Plain host[:port]: trim any port suffix.
    match authority.rfind(':') {
        Some(idx) => &authority[..idx],
        None => authority,
    }
}

impl OpendalFileSystem {
    /// Wraps an existing [`opendal::Operator`].
    ///
    /// The operator is wrapped in a [`RetryLayer`] (see
    /// `default_retry_layer`) so transient S3/GCS/Azure failures
    /// retry with exponential backoff before propagating to the
    /// caller. This does NOT layer [`BlockingLayer`] onto it; callers
    /// who already have a configured operator are presumed to have
    /// done that themselves if needed.
    pub fn with_operator(op: Operator) -> ForstResult<Self> {
        let rt = RuntimeHandle::acquire()?;
        let name = format!("OpendalFileSystem({})", op.info().scheme().into_static());
        // R18-L1: RetryLayer is NOT idempotent w.r.t. layering — applying
        // it twice MULTIPLIES retries (each layer wraps the previous, so
        // the effective retry count becomes outer * inner), which is not
        // what we want. Callers that already have a retry policy on their
        // operator should use `with_operator_no_retry` to avoid the
        // double-layer. The pre-fix comment claimed idempotency in the
        // first sentence but contradicted itself in the next — the
        // multiplicative behaviour is the actual semantics, and the
        // wording here now matches.
        let op = op.layer(default_retry_layer());
        Ok(Self {
            op,
            rt,
            name,
            pending: Arc::new(Mutex::new(HashMap::new())),
            upload_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_UPLOADS)),
        })
    }

    /// Wraps an existing [`opendal::Operator`] WITHOUT attaching the
    /// default [`RetryLayer`].
    ///
    /// Used by tests that need to observe the underlying error
    /// behaviour, or by callers that have already attached a custom
    /// retry policy to the operator. Production code should prefer
    /// [`OpendalFileSystem::with_operator`].
    pub fn with_operator_no_retry(op: Operator) -> ForstResult<Self> {
        let rt = RuntimeHandle::acquire()?;
        let name = format!("OpendalFileSystem({})", op.info().scheme().into_static());
        Ok(Self {
            op,
            rt,
            name,
            pending: Arc::new(Mutex::new(HashMap::new())),
            upload_sem: Arc::new(Semaphore::new(MAX_INFLIGHT_UPLOADS)),
        })
    }

    /// Constructs an OpenDAL filesystem backed by the in-memory service.
    ///
    /// Useful for tests and for staging buffers in unit harnesses.
    pub fn memory() -> ForstResult<Self> {
        let op = Operator::new(opendal::services::Memory::default())
            .map_err(|e| map_opendal_err(e, "OpendalFileSystem::memory: builder"))?
            .finish();
        Self::with_operator(op)
    }

    /// Constructs an OpenDAL filesystem rooted at `root` on the local FS.
    ///
    /// `root` must be valid UTF-8. The directory does not need to exist
    /// yet — OpenDAL creates it on first write.
    pub fn local(root: &Path) -> ForstResult<Self> {
        let root_str = path_str(root, "OpendalFileSystem::local")?;
        let builder = opendal::services::Fs::default().root(root_str);
        let op = Operator::new(builder)
            .map_err(|e| map_opendal_err(e, "OpendalFileSystem::local: builder"))?
            .finish();
        Self::with_operator(op)
    }

    /// Constructs an OpenDAL filesystem backed by an S3 bucket.
    ///
    /// Optional `endpoint` lets the caller point at S3-compatible services
    /// (MinIO, Ceph RGW, R2, etc.). Credentials are picked up from the
    /// environment if `access_key_id`/`secret_access_key` are `None`,
    /// matching the AWS SDK default chain.
    pub fn s3(
        bucket: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
    ) -> ForstResult<Self> {
        Self::s3_with_root(
            bucket,
            "",
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        )
    }

    /// Constructs an S3-backed filesystem rooted under `root` inside the bucket.
    pub fn s3_with_root(
        bucket: &str,
        root: &str,
        region: &str,
        endpoint: Option<&str>,
        access_key_id: Option<&str>,
        secret_access_key: Option<&str>,
    ) -> ForstResult<Self> {
        let mut builder = opendal::services::S3::default()
            .bucket(bucket)
            .region(region);
        if !root.is_empty() {
            builder = builder.root(root);
        }
        if let Some(ep) = endpoint {
            // R44-L1: warn when the operator points at a non-localhost
            // endpoint over plaintext HTTP. S3 credentials traversing such
            // a link are observable on the wire; production deployments
            // should use HTTPS. We do NOT refuse the configuration —
            // tests and dev MinIO setups still need cleartext — but we
            // make the risk visible in logs.
            if ep.starts_with("http://") {
                let host_part = ep.trim_start_matches("http://");
                // R47-L1: strip every authority-trailing component before
                // parsing the host. URLs may carry a path (`/foo`), query
                // (`?bar=1`), and/or fragment (`#frag`); a naïve split on
                // `/` alone leaves `?...`/`#...` attached to the host and
                // breaks the loopback comparison (e.g. `localhost?x=1`
                // would NOT match `localhost`). Order: `/` → `?` → `#`
                // (RFC 3986 section 3 — path > query > fragment).
                let authority = host_part
                    .split('/')
                    .next()
                    .unwrap_or(host_part)
                    .split('?')
                    .next()
                    .unwrap_or(host_part)
                    .split('#')
                    .next()
                    .unwrap_or(host_part);
                // R45-M1: properly extract host from an authority that may
                // include an IPv6 literal (`[::1]:9000`) or a regular
                // `host:port`. Naïvely splitting on `:` would butcher the
                // IPv6 literal into `[` and never match the loopback list.
                let host_no_port = extract_host_from_authority(authority);
                let is_loopback = host_no_port == "localhost"
                    || host_no_port == "127.0.0.1"
                    || host_no_port == "::1";
                if !is_loopback {
                    tracing::warn!(
                        target: "forst_rs_io::opendal_backend",
                        endpoint = %ep,
                        bucket = %bucket,
                        "S3 endpoint uses plaintext http:// to a non-loopback host — \
                         credentials and object data will traverse the network unencrypted. \
                         Use https:// for production deployments."
                    );
                }
            }
            builder = builder.endpoint(ep);
        }
        if let Some(ak) = access_key_id {
            builder = builder.access_key_id(ak);
        }
        if let Some(sk) = secret_access_key {
            builder = builder.secret_access_key(sk);
        }
        let op = Operator::new(builder)
            .map_err(|e| map_opendal_err(e, "OpendalFileSystem::s3: builder"))?
            .finish();
        Self::with_operator(op)
    }

    /// Returns a clone of the inner [`opendal::Operator`].
    ///
    /// Useful when callers need OpenDAL's async API directly (streaming
    /// writers, layered tracing, etc.) while still routing other ops
    /// through ForSt-RS's [`FileSystem`].
    pub fn operator(&self) -> Operator {
        self.op.clone()
    }

    /// Drives an async closure to completion on the bridged runtime.
    fn block_on<F, T>(&self, fut: F) -> T
    where
        F: std::future::Future<Output = T>,
    {
        let handle = self.rt.handle();
        // `block_on` panics if called from inside an async task on the same
        // runtime. We document this constraint at the type level.
        handle.block_on(fut)
    }

    /// Builds a [`BlockingLayer`]-equipped clone of the inner operator.
    ///
    /// Used by [`OpendalRandomAccessFile`] to support `read_at` from any
    /// thread, including threads outside of the runtime. Wrapped in a
    /// `OnceLock` so we only pay the layer cost once.
    fn blocking_op(&self) -> ForstResult<opendal::BlockingOperator> {
        let _guard = self.rt.handle().enter();
        let op = self.op.clone().layer(
            BlockingLayer::create()
                .map_err(|e| map_opendal_err(e, "OpendalFileSystem: BlockingLayer::create"))?,
        );
        Ok(op.blocking())
    }
}

// ---------------------------------------------------------------------------
// SequentialFile — eagerly download then read from cursor
// ---------------------------------------------------------------------------

/// A sequential reader that streams the object through OpenDAL's blocking reader.
pub struct OpendalSequentialFile {
    reader: opendal::StdReader,
}

impl SequentialFile for OpendalSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        self.reader
            .read(buf)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("OpenDAL stream read: {e}"))))
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        let delta = i64::try_from(n).map_err(|_| {
            ForstError::invalid_argument(format!("skip offset {n} exceeds i64::MAX"))
        })?;
        self.reader.seek(SeekFrom::Current(delta)).map_err(|e| {
            ForstError::Io(std::io::Error::other(format!("OpenDAL stream skip: {e}")))
        })?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// RandomAccessFile — ranged reads via OpenDAL
// ---------------------------------------------------------------------------

/// A random-access reader that issues a ranged read per [`Self::read_at`].
///
/// The `BlockingOperator` is constructed once at open time and reused for
/// every positioned read. `size` is cached from a single `stat` at open.
pub struct OpendalRandomAccessFile {
    op: opendal::BlockingOperator,
    /// Async operator clone used by [`read_ranges`] to issue concurrent
    /// ranged GETs on the bridged runtime. Cheap to clone (an `Arc`
    /// internally).
    ///
    /// [`read_ranges`]: RandomAccessFile::read_ranges
    op_async: opendal::Operator,
    /// Runtime handle to drive concurrent reads to completion. Cloned from
    /// the backend's `RuntimeHandle`; `Send`/`Sync`.
    handle: Handle,
    path: String,
    size: u64,
}

/// Per-range short-read window for the async ranged-GET helper. Mirrors the
/// constant in `read_at`: large single ranged GETs are exactly where
/// OpenDAL 0.50.2 / non-AWS S3 (BOS) truncate the tail, so we stitch 4 MiB
/// windows back together.
const READ_WINDOW: u64 = 4 * 1024 * 1024;
/// Bounded empty-reply retries before declaring corruption (mirrors `read_at`).
const MAX_EMPTY_RETRIES: u32 = 16;
/// Max number of ranged GETs in flight at once. Capped to avoid exhausting
/// the S3 endpoint / connection pool — the Nexmark harness already competes
/// for that pool.
const READ_RANGES_CONCURRENCY: usize = 8;

/// Reads `[offset, offset+len)` fully from `op_async` at `path`, applying the
/// same windowed short-read loop + bounded empty-retry guard as the
/// synchronous `read_at`. Returns the bytes actually read (EOF-shortened if
/// the range runs past `size`). `size` is the cached object length.
async fn read_range_async(
    op_async: &opendal::Operator,
    path: &str,
    size: u64,
    offset: u64,
    len: usize,
) -> ForstResult<Vec<u8>> {
    if offset >= size || len == 0 {
        return Ok(Vec::new());
    }
    let want = u64::try_from(len).unwrap_or(u64::MAX);
    let end = offset.saturating_add(want).min(size);
    let mut out: Vec<u8> = Vec::with_capacity(len.min((end - offset) as usize));
    let mut cur = offset;
    let mut empty_retries: u32 = 0;
    while cur < end && (out.len() as u64) < (end - offset) {
        let win_end = cur.saturating_add(READ_WINDOW).min(end);
        let mut buffer = op_async
            .read_with(path)
            .range(cur..win_end)
            .await
            .map_err(|e| map_opendal_err(e, &format!("OpenDAL ranged read: {path}")))?;
        let got = buffer.len();
        if got == 0 {
            empty_retries += 1;
            if empty_retries > MAX_EMPTY_RETRIES {
                return Err(ForstError::corruption(format!(
                    "OpenDAL ranged read returned 0 bytes for {cur}..{end} of {path} \
                     (size {size}) after {MAX_EMPTY_RETRIES} retries"
                )));
            }
            continue;
        }
        empty_retries = 0;
        // Clamp to the requested window so we never overshoot `end`.
        let remaining = (end - cur) as usize;
        let take = got.min(remaining);
        let start = out.len();
        out.resize(start + take, 0u8);
        buffer.copy_to_slice(&mut out[start..start + take]);
        cur = cur.saturating_add(take as u64);
    }
    Ok(out)
}

impl RandomAccessFile for OpendalRandomAccessFile {
    /// §2.1: remote object-store reads — the prefetcher uses the deep
    /// (4 MiB) readahead regime and ramps after the first block.
    fn is_local(&self) -> bool {
        false
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        if offset >= self.size {
            return Ok(0);
        }
        let want = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        let end = offset.saturating_add(want).min(self.size);
        // FRS-S3-SHORTREAD-FIX: LOOP until the requested [offset, end) range is
        // fully read. A single `read_with().range().call()` on OpenDAL 0.50.2
        // can return a SHORT (or transiently empty) `Buffer` for the tail of a
        // large multipart object even though the object's `content_length`
        // covers the whole range — the bytes ARE on S3, OpenDAL just hands them
        // back in incomplete pieces. The pre-fix single-shot return truncated
        // SSTs by tens of KB (dropping the trailing footer magic), surfacing as
        // `Corruption("...short read")` / "missing trailing magic" and crashing
        // q4/q7-style large-join-state reads. Each ranged GET that makes
        // progress advances `cur`; a genuinely empty reply is retried a bounded
        // number of times before we give up (so a real backend failure errors
        // rather than silently truncating, and we never spin forever).
        let mut filled: usize = 0;
        let mut cur = offset;
        let mut empty_retries: u32 = 0;
        // `READ_WINDOW` / `MAX_EMPTY_RETRIES`: file-level consts shared with the
        // async `read_range_async` helper so the serial and concurrent paths
        // apply byte-identical windowing + empty-retry semantics.
        while cur < end && filled < buf.len() {
            let win_end = cur.saturating_add(READ_WINDOW).min(end);
            let mut buffer = self
                .op
                .read_with(&self.path)
                .range(cur..win_end)
                .call()
                .map_err(|e| map_opendal_err(e, &format!("OpenDAL ranged read: {}", self.path)))?;
            let got = buffer.len();
            if got == 0 {
                empty_retries += 1;
                if empty_retries > MAX_EMPTY_RETRIES {
                    return Err(ForstError::corruption(format!(
                        "OpenDAL ranged read returned 0 bytes for {cur}..{end} of {} \
                         (size {}) after {MAX_EMPTY_RETRIES} retries",
                        self.path, self.size
                    )));
                }
                continue;
            }
            empty_retries = 0;
            // `opendal::Buffer` may be non-contiguous; `copy_to_slice` streams
            // chunk-by-chunk directly into `buf` with no intermediate Vec.
            let n = got.min(buf.len() - filled);
            buffer.copy_to_slice(&mut buf[filled..filled + n]);
            filled += n;
            cur = cur.saturating_add(n as u64);
        }
        Ok(filled)
    }

    /// Issues every range as a concurrent ranged GET on the bridged runtime,
    /// capped at `READ_RANGES_CONCURRENCY` in flight. Returns one `Vec` per
    /// range in input order, byte-identical to looping `read_at` serially.
    fn read_ranges(&self, ranges: &[(u64, usize)]) -> ForstResult<Vec<Vec<u8>>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        if ranges.len() == 1 {
            // Single range: no concurrency to win; reuse the helper directly.
            let (off, len) = ranges[0];
            let op_async = self.op_async.clone();
            let path = self.path.clone();
            let size = self.size;
            let v = self.handle.block_on(async move {
                read_range_async(&op_async, &path, size, off, len).await
            })?;
            return Ok(vec![v]);
        }

        // Spawn one task per range, tagged with its index so we can restore
        // input order after `buffer_unordered`-style completion. We bound the
        // number in flight via `buffered`-style chunking using a JoinSet plus
        // an index→slot map; capping at READ_RANGES_CONCURRENCY avoids
        // overwhelming the S3 connection pool.
        let op_async = self.op_async.clone();
        let path = self.path.clone();
        let size = self.size;
        let ranges = ranges.to_vec();

        self.handle.block_on(async move {
            let mut results: Vec<Option<Vec<u8>>> = vec![None; ranges.len()];
            let mut join: tokio::task::JoinSet<(usize, ForstResult<Vec<u8>>)> =
                tokio::task::JoinSet::new();
            let mut next = 0usize;
            // Prime the pipeline up to the concurrency cap.
            while next < ranges.len() && join.len() < READ_RANGES_CONCURRENCY {
                let (off, len) = ranges[next];
                let idx = next;
                let op_async = op_async.clone();
                let path = path.clone();
                join.spawn(async move {
                    let r = read_range_async(&op_async, &path, size, off, len).await;
                    (idx, r)
                });
                next += 1;
            }
            while let Some(joined) = join.join_next().await {
                let (idx, r) = joined.map_err(|e| {
                    ForstError::Io(std::io::Error::other(format!(
                        "OpenDAL read_ranges task join: {e}"
                    )))
                })?;
                results[idx] = Some(r?);
                // Refill the pipeline to keep up to the cap in flight.
                if next < ranges.len() {
                    let (off, len) = ranges[next];
                    let idx = next;
                    let op_async = op_async.clone();
                    let path = path.clone();
                    join.spawn(async move {
                        let r = read_range_async(&op_async, &path, size, off, len).await;
                        (idx, r)
                    });
                    next += 1;
                }
            }
            // Every slot must be filled (one task per index, all joined).
            Ok(results.into_iter().map(|o| o.unwrap_or_default()).collect())
        })
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.size)
    }
}

// ---------------------------------------------------------------------------
// WritableFile — streaming OpenDAL writer
// ---------------------------------------------------------------------------

// FRS (dead_code): the earlier 8 MiB / 4-way multipart constants were superseded by the
// 16 MiB / 8-way BUFFERED object-store path below (S3_WRITE_CHUNK_BYTES / S3_WRITE_CONCURRENCY);
// removed as unused.

/// 2026-05-29 FRS-S3-FLUSH-CONCURRENT: multipart params for the BUFFERED
/// object-store write path (the active SST flush/compaction-output path).
/// 16 MiB parts (vs OpenDAL's 5 MiB default) cut per-request overhead; 8-way
/// concurrency (vs default 1 = sequential) is the throughput lever. Research
/// (Velox 10 MiB sequential; AWS guidance 16-64 MiB / 8-16 concurrent;
/// OpenDAL #5929) lands on 16 MiB × 8 as the throughput/memory sweet spot.
/// Peak in-flight part memory ≈ 8 × 16 MiB = 128 MiB per concurrent SST write.
const S3_WRITE_CHUNK_BYTES: usize = 16 * 1024 * 1024;
const S3_WRITE_CONCURRENCY: usize = 8;

/// The underlying OpenDAL writer driving an [`OpendalWritableFile`].
///
/// FRS-S3-MULTIPART: object-store CreateNew/CreateOrTruncate writes use the
/// **async** [`opendal::Writer`] driven via `block_on`, because only the async
/// builder exposes `.concurrent()` for parallel multipart part-uploads (the
/// blocking builder in opendal 0.50 has no concurrency knob, and the
/// `BlockingLayer` would `block_on` each `write()` individually, serializing
/// parts). Local-FS / append-mode writes keep the proven blocking writer.
enum WriterKind {
    /// Async writer (object-store multipart); driven via the runtime handle.
    ///
    /// FRS (dead_code): currently never constructed — the object-store CreateNew/
    /// CreateOrTruncate path uses the BUFFERED variant below. Retained for the
    /// streaming async-multipart writer path (kept off the unused-variant gate).
    #[allow(dead_code)]
    Async(opendal::Writer),
    /// Blocking writer (local FS, memory, or append mode) — self-driving.
    Blocking(opendal::BlockingWriter),
    /// FRS-S3-MULTIPART-TRUNC-FIX: object-store CreateNew/CreateOrTruncate
    /// path. Buffers the whole object in memory, then on close does a SINGLE
    /// `op.write(path, buf)` and VERIFIES the stored `content_length` equals
    /// what we wrote. The previous streaming `.chunk()` multipart `Writer`
    /// dropped the final part on `close()` against non-AWS S3 (BOS),
    /// truncating SSTs (`"missing trailing magic"` → engine CORRUPTION/PANIC
    /// on read-back) on the q4/q7/q9 heavy-join large-SST path. A single
    /// buffered write is far more robust, and the post-write stat-verify turns
    /// any residual truncation into a hard error (the SST is never published
    /// with a bad footer) instead of silent data corruption. Memory is bounded
    /// by the SST size (`target_file_size_base`, default 64 MiB).
    Buffered { op: opendal::Operator, buf: Vec<u8> },
}

/// A writable file that streams bytes to OpenDAL.
pub struct OpendalWritableFile {
    path: String,
    writer: Option<WriterKind>,
    /// Runtime handle used to drive the async writer's `write`/`close` to
    /// completion. Cloned from the backend's [`RuntimeHandle`]. Cheap to hold
    /// (a tokio `Handle` is an `Arc` internally) and `Send`/`Sync` so the
    /// writable file can move across flush/compaction worker threads.
    handle: Handle,
    bytes_written: u64,
    closed: bool,
    /// 2026-05-29 WRITE-BACK FLUSH: shared registry of in-flight async uploads.
    /// `Some` only for the buffered object-store path; the spawned upload+verify
    /// JoinHandle is registered here on close so `await_upload`/`await_all_uploads`
    /// can later block on it. `None` for blocking/append paths (synchronous).
    pending: Option<PendingUploads>,
    /// 2026-05-29 WRITE-BACK FLUSH: backpressure semaphore (clone of the
    /// backend's). The spawned upload acquires a permit before touching S3.
    upload_sem: Option<Arc<Semaphore>>,
    /// FRS-FADVISE (2026-06-08): absolute on-disk path, set ONLY when this file is
    /// backed by the local `fs` opendal service. On close we `fsync` + `posix_fadvise
    /// (POSIX_FADV_DONTNEED)` it so its pages leave the OS page cache — bounding the
    /// container cgroup page cache (anon + cache) that OOM-kills the 8c/32g join
    /// queries (q9/q20/q4) once written SSTs accumulate. `None` for S3/memory (the
    /// pages there are not local; nothing to advise away).
    fadvise_path: Option<std::path::PathBuf>,
    /// FRS-SST-WRITE-COALESCE (2026-06-08): streaming SST writes arrive one data
    /// block at a time (~8 KiB), and the old `append` did a tokio `block_on(opendal
    /// write)` PER block → ~27K block_on round-trips per 219 MB SST. Profiled as the
    /// DOMINANT flush cost on q17 (sink-write 428s vs encode 17s + buffer 8s). This
    /// buffer coalesces block appends and flushes to opendal in large chunks
    /// (`SST_WRITE_COALESCE_BYTES`), cutting the block_on count ~500×. Drained on
    /// `close_writer` (sync) so the SST is never truncated.
    coalesce: Vec<u8>,
}

/// FRS-SST-WRITE-COALESCE: flush the streaming-write accumulator at this size.
const SST_WRITE_COALESCE_BYTES: usize = 4 * 1024 * 1024;

impl WritableFile for OpendalWritableFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        if self.closed {
            return Err(ForstError::invalid_argument(format!(
                "OpenDAL append after close: {}",
                self.path
            )));
        }
        // FRS-S3-MULTIPART-TRUNC-FIX: the buffered object-store path just
        // accumulates bytes in memory; the single write + verify happens on
        // close. Handle it before the streaming coalesce path.
        if let Some(WriterKind::Buffered { buf: acc, .. }) = self.writer.as_mut() {
            acc.extend_from_slice(data);
            self.bytes_written = self.bytes_written.saturating_add(data.len() as u64);
            return Ok(());
        }
        if self.writer.is_none() {
            return Err(ForstError::invalid_argument(format!(
                "OpenDAL writer missing: {}",
                self.path
            )));
        }
        // FRS-SST-WRITE-COALESCE: streaming SST data blocks arrive ~8 KiB at a time;
        // doing a block_on(opendal write) PER block was the dominant flush cost
        // (profiled: sink-write ≫ encode). Accumulate and flush in large chunks.
        self.coalesce.extend_from_slice(data);
        self.bytes_written = self.bytes_written.saturating_add(data.len() as u64);
        if self.coalesce.len() >= SST_WRITE_COALESCE_BYTES {
            self.flush_coalesce()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        Ok(())
    }

    fn sync(&mut self) -> ForstResult<()> {
        self.close_writer()
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.bytes_written)
    }
}

impl OpendalWritableFile {
    /// FRS-SST-WRITE-COALESCE: flush the accumulated streaming buffer to opendal in
    /// ONE write. No-op when empty or for the Buffered/absent writer kinds. Must run
    /// before `close_writer` takes the writer, or the SST tail (bloom/index/footer)
    /// is lost → "missing trailing magic" corruption.
    fn flush_coalesce(&mut self) -> ForstResult<()> {
        if self.coalesce.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.coalesce);
        let buf = opendal::Buffer::from(bytes::Bytes::from(bytes));
        match self.writer.as_mut() {
            Some(WriterKind::Async(w)) => self
                .handle
                .block_on(w.write(buf))
                .map_err(|e| map_opendal_err(e, &format!("OpenDAL async write: {}", self.path)))?,
            Some(WriterKind::Blocking(w)) => w.write(buf).map_err(|e| {
                map_opendal_err(e, &format!("OpenDAL streaming write: {}", self.path))
            })?,
            Some(WriterKind::Buffered { .. }) | None => {}
        }
        Ok(())
    }

    fn close_writer(&mut self) -> ForstResult<()> {
        if self.closed {
            return Ok(());
        }
        // Drain any coalesced streaming bytes BEFORE taking/closing the writer.
        self.flush_coalesce()?;
        if let Some(writer) = self.writer.take() {
            // FRS-S3-MULTIPART: `close()` is what publishes the object — for the
            // async multipart writer it issues CompleteMultipartUpload, so the
            // object only becomes visible on success (preserving the
            // crash-atomic-on-close contract the SST-rename fix relies on).
            match writer {
                WriterKind::Async(mut w) => self.handle.block_on(w.close()).map_err(|e| {
                    map_opendal_err(e, &format!("OpenDAL async close writer: {}", self.path))
                })?,
                WriterKind::Blocking(mut w) => w.close().map_err(|e| {
                    map_opendal_err(e, &format!("OpenDAL close writer: {}", self.path))
                })?,
                // FRS-S3-MULTIPART-TRUNC-FIX + 2026-05-29 WRITE-BACK FLUSH:
                // buffered write + verify, SPAWNED so the flush worker returns on
                // the LOCAL serialization rather than blocking on S3
                // CompleteMultipartUpload. The immutable memtable + WriteBufferManager
                // budget free as soon as `close_writer` returns; the actual S3
                // upload runs on the bridged runtime off the critical path.
                //
                // Safety: Design A "resident-flushed memtables" keep the just-flushed
                // memtable in RAM and the read path shadows the byte-identical L0 SST
                // from RAM, so reads served while this upload is in flight never touch
                // the not-yet-uploaded S3 object. The only paths that read an SST
                // directly from S3 (`get_or_open_sst_reader`, compaction inputs) call
                // `await_upload` first, and the checkpoint barrier / shutdown call
                // `await_all_uploads`, so a checkpoint can never reference a
                // non-uploaded SST.
                WriterKind::Buffered { op, buf } => {
                    let expected = buf.len() as u64;
                    let p = self.path.clone();
                    match (self.pending.as_ref(), self.upload_sem.as_ref()) {
                        (Some(pending), Some(sem)) => {
                            let op = op.clone();
                            let sem = sem.clone();
                            let path_for_task = p.clone();
                            // 2026-05-29 FRS-S3-FLUSH-CONCURRENT: `write_with` provides
                            // the WHOLE buffer up front (no streaming `.chunk()` "final
                            // part dropped on close" truncation bug), with
                            // `.concurrent(8).chunk(16 MiB)` fanning parts out 8-way.
                            // The verify-stat catches any truncation as a hard error so
                            // a truncated SST is NEVER published + read back.
                            // 2026-05-29 RACE FIX: broadcast the upload outcome over a
                            // `watch` channel so EVERY `await_upload`/`await_all_uploads`
                            // caller blocks until the upload truly completes (see
                            // `PendingUploads`). The spawned task drives the upload on the
                            // bridged runtime and publishes `Some(outcome)` on finish; the
                            // receiver is registered for awaiters.
                            let (tx, rx) =
                                tokio::sync::watch::channel::<Option<UploadOutcome>>(None);
                            self.handle.spawn(async move {
                                let outcome: UploadOutcome = async {
                                    // Backpressure: hold a permit for the whole upload so
                                    // at most MAX_INFLIGHT_UPLOADS SSTs are resident at
                                    // once under a slow S3 endpoint.
                                    let _permit = sem.acquire().await.map_err(|e| {
                                        format!(
                                            "OpenDAL upload semaphore closed: {path_for_task}: {e}"
                                        )
                                    })?;
                                    op.write_with(&path_for_task, buf)
                                        // `executors-tokio`-backed Executor: required for
                                        // `.concurrent()` or opendal's default `()` executor
                                        // panics. We are inside `handle.spawn`, so
                                        // TokioExecutor's `tokio::task::spawn` finds this
                                        // runtime.
                                        .executor(Executor::new())
                                        .concurrent(S3_WRITE_CONCURRENCY)
                                        .chunk(S3_WRITE_CHUNK_BYTES)
                                        .await
                                        .map_err(|e| {
                                            map_opendal_err(
                                                e,
                                                &format!("OpenDAL buffered write: {path_for_task}"),
                                            )
                                            .to_string()
                                        })?;
                                    let meta = op.stat(&path_for_task).await.map_err(|e| {
                                        map_opendal_err(
                                            e,
                                            &format!(
                                                "OpenDAL buffered write verify-stat: {path_for_task}"
                                            ),
                                        )
                                        .to_string()
                                    })?;
                                    let stored = meta.content_length();
                                    if stored != expected {
                                        return Err(format!(
                                            "OpenDAL buffered write truncated: wrote {expected} \
                                             bytes but stored object is {stored} bytes for \
                                             {path_for_task}"
                                        ));
                                    }
                                    Ok(())
                                }
                                .await;
                                // Publish to all awaiters. Ignore send error (no receivers
                                // left — the FS was dropped — which is benign on shutdown).
                                let _ = tx.send(Some(outcome));
                            });
                            // Register the receiver so a later await can block on it. If a
                            // prior upload to the SAME path is still pending (path reuse),
                            // await it first to preserve last-writer-wins ordering.
                            let prior = pending
                                .lock()
                                .expect("upload registry poisoned")
                                .insert(p.clone(), rx);
                            if let Some(mut prior_rx) = prior {
                                // Drain the superseded upload so its outcome is observed
                                // before we return (the new write supersedes it on S3).
                                let prior_outcome = self.handle.block_on(async move {
                                    match prior_rx.wait_for(|v| v.is_some()).await {
                                        Ok(g) => g.clone().unwrap_or(Ok(())),
                                        // Sender dropped without publishing (task aborted on
                                        // shutdown) — treat as benign for a superseded write.
                                        Err(_) => Ok(()),
                                    }
                                });
                                if let Err(msg) = prior_outcome {
                                    return Err(ForstError::Io(std::io::Error::other(format!(
                                        "OpenDAL prior upload {}: {msg}",
                                        self.path
                                    ))));
                                }
                            }
                        }
                        // No registry (defensive): fall back to a synchronous
                        // buffered write + verify so durability is never lost.
                        _ => {
                            self.handle
                                .block_on(async {
                                    op.write_with(&p, buf)
                                        // See above: Executor required for `.concurrent()`.
                                        // Inside `handle.block_on`, so the tokio runtime
                                        // context is present for TokioExecutor.
                                        .executor(Executor::new())
                                        .concurrent(S3_WRITE_CONCURRENCY)
                                        .chunk(S3_WRITE_CHUNK_BYTES)
                                        .await
                                })
                                .map_err(|e| {
                                    map_opendal_err(
                                        e,
                                        &format!("OpenDAL buffered write: {}", self.path),
                                    )
                                })?;
                            let meta =
                                self.handle
                                    .block_on(async { op.stat(&p).await })
                                    .map_err(|e| {
                                        map_opendal_err(
                                            e,
                                            &format!(
                                                "OpenDAL buffered write verify-stat: {}",
                                                self.path
                                            ),
                                        )
                                    })?;
                            let stored = meta.content_length();
                            if stored != expected {
                                return Err(ForstError::corruption(format!(
                                    "OpenDAL buffered write truncated: wrote {expected} bytes but \
                                     stored object is {stored} bytes for {}",
                                    self.path
                                )));
                            }
                        }
                    }
                }
            };
        }
        // FRS-FADVISE: the writer is now closed and the bytes are on the local fs.
        // Drop this file's pages from the OS page cache so the container cgroup
        // (anon + page cache) does not OOM-kill heavy-write join queries on 8c/32g.
        // No-op for S3/memory (fadvise_path is None) and on non-Linux.
        if let Some(path) = self.fadvise_path.take() {
            fadvise_dontneed(&path);
        }
        self.closed = true;
        Ok(())
    }
}

/// FRS-FADVISE (2026-06-08): `fsync` + `posix_fadvise(POSIX_FADV_DONTNEED)` a
/// just-written local file so its pages leave the OS page cache. The fsync first
/// makes the pages clean (DONTNEED is a no-op on dirty pages); both are cheap if
/// the engine already synced. Best-effort: any error is logged and ignored — page
/// cache is a soft RAM concern, never a correctness invariant. Linux-only.
#[cfg(target_os = "linux")]
fn fadvise_dontneed(path: &std::path::Path) {
    use std::os::unix::io::AsRawFd;
    use std::sync::OnceLock;
    // FRS-FADVISE default OFF (2026-06-08): the per-SST `sync_all` + posix_fadvise is
    // EXPENSIVE on a host-bind-mounted /tmp (virtiofs/9p — slow fsync), which taxed
    // write-heavy queries (q11/q16/q19) on 8c/32g. The page-cache benefit it was added
    // for is marginal (the join OOM was anon + container-disk, not page cache). So it is
    // opt-in via FRS_FADVISE=1 for environments where dropping written-SST pages helps.
    static ON: OnceLock<bool> = OnceLock::new();
    let on = *ON.get_or_init(|| {
        matches!(
            std::env::var("FRS_FADVISE").ok().as_deref(),
            Some("1") | Some("true")
        )
    });
    if !on {
        return;
    }
    let f = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return, // file may have been compacted away already; benign
    };
    // Ensure pages are clean so they are actually droppable.
    let _ = f.sync_all();
    // offset=0, len=0 → the whole file. nix gives a safe wrapper (this crate is
    // `#![forbid(unsafe_code)]`). Best-effort: ignore errors (soft RAM concern).
    let _ = nix::fcntl::posix_fadvise(
        f.as_raw_fd(),
        0,
        0,
        nix::fcntl::PosixFadviseAdvice::POSIX_FADV_DONTNEED,
    );
}

#[cfg(not(target_os = "linux"))]
fn fadvise_dontneed(_path: &std::path::Path) {}

impl Drop for OpendalWritableFile {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(e) = self.close_writer() {
                tracing::warn!(
                    target: "forst_rs_io::opendal_backend",
                    path = %self.path,
                    error = %e,
                    "OpendalWritableFile dropped before sync; best-effort close failed",
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// FileSystem impl
// ---------------------------------------------------------------------------

impl FileSystem for OpendalFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let p = path_str(path, "open_sequential_file")?;
        let blocking = self.blocking_op()?;
        let reader = blocking
            .reader(p)
            .and_then(|r| r.into_std_read(..))
            .map_err(|e| map_opendal_err(e, &format!("open_sequential_file: {p}")))?;
        Ok(Box::new(OpendalSequentialFile { reader }))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let p = path_str(path, "open_random_access_file")?;
        // A previous buffered write to this object may have returned from
        // sync/close after spawning the async upload but before the object is
        // visible to stat/read. Wait here so callers that observe an SST in the
        // version never race a not-yet-published object and get a transient
        // NotFound.
        self.await_upload(path)?;
        let meta = self
            .block_on(self.op.stat(p))
            .map_err(|e| map_opendal_err(e, &format!("open_random_access_file stat: {p}")))?;
        let size = meta.content_length();
        let blocking = self.blocking_op()?;
        Ok(Box::new(OpendalRandomAccessFile {
            op: blocking,
            op_async: self.op.clone(),
            handle: self.rt.handle(),
            path: p.to_string(),
            size,
        }))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let p = path_str(path, "open_writable_file")?;
        let blocking = self.blocking_op()?;

        // 2026-05-30 WRITE-BACK VISIBILITY RACE FIX: a prior write to THIS path may
        // still have an async upload (buffered write-back close) in flight — its
        // `sync()`/`close()` returns before the spawned upload lands on the backend.
        // Re-opening the path now (Append must read the full existing object;
        // CreateNew must see an accurate `exists`; CreateOrTruncate must not race a
        // late upload overwriting our truncate) requires that upload to have
        // completed first. `await_upload` is a cheap no-op when nothing is pending.
        self.await_upload(path)?;

        let exists = match self.block_on(self.op.exists(p)) {
            Ok(v) => v,
            Err(e) => {
                return Err(map_opendal_err(
                    e,
                    &format!("open_writable_file exists: {p}"),
                ))
            }
        };

        let append = match mode {
            WriteMode::CreateNew => {
                if exists {
                    return Err(ForstError::invalid_argument(format!(
                        "open_writable_file: file already exists: {p}"
                    )));
                }
                false
            }
            WriteMode::CreateOrTruncate => false,
            WriteMode::Append => true,
        };
        let initial_size = if append && exists {
            self.block_on(self.op.stat(p))
                .map_err(|e| map_opendal_err(e, &format!("open_writable_file stat: {p}")))?
                .content_length()
        } else {
            0
        };
        // FRS-S3-MULTIPART: object-store CreateNew/CreateOrTruncate writes use
        // the async writer with concurrent multipart part-uploads. Append mode
        // (WAL) and local-FS/memory backends keep the proven blocking writer
        // (multipart concurrency is meaningless for append, and local FS gains
        // nothing from it — this also keeps the local test suite on the
        // unchanged code path).
        let use_buffered_object_store = !append && self.op.info().scheme() != opendal::Scheme::Fs;
        // 2026-05-29 WRITE-BACK FLUSH: only the buffered object-store path is
        // eligible for asynchronous upload; carry the registry + semaphore into
        // the writer for that case. Blocking/append (WAL) paths stay synchronous
        // (None), so the WAL is durable on `sync()`.
        let (file_pending, file_sem) = if use_buffered_object_store {
            (Some(self.pending.clone()), Some(self.upload_sem.clone()))
        } else {
            (None, None)
        };
        let writer_kind = if use_buffered_object_store {
            // FRS-S3-MULTIPART-TRUNC-FIX: object-store CreateNew/CreateOrTruncate
            // writes BUFFER the whole object then do a single `op.write` + a
            // post-write size-verify on close (see `WriterKind::Buffered` +
            // `close_writer`). The previous streaming `.chunk(MULTIPART_CHUNK_BYTES)`
            // async `Writer` dropped the final multipart part on `close()`
            // against non-AWS S3 (BOS) for large (>8 MiB) SSTs, truncating the
            // SST footer ("missing trailing magic") → engine CORRUPTION/PANIC on
            // read-back of q4/q7/q9 large join-state. A single buffered write is
            // robust and the verify makes any residual truncation a hard error
            // rather than silent corruption. Memory is bounded by the SST size
            // (`target_file_size_base`, default 64 MiB).
            WriterKind::Buffered {
                op: self.op.clone(),
                buf: Vec::new(),
            }
        } else if append && self.op.info().scheme() != opendal::Scheme::Fs {
            // FRS-APPEND-EMULATION: only the local FS service supports native append;
            // object stores (S3) and the in-memory service do not, and OpenDAL returns
            // `Unsupported` for `.append(true)` there. Emulate append via the buffered
            // writer: load the existing object into the buffer up front, accumulate
            // subsequent appends in memory, and write the whole object once on
            // close()/sync() (a synchronous, durable write — `pending` is None on this
            // path). This makes WriteMode::Append work uniformly across backends instead
            // of failing on memory/object stores. Memory cost is bounded by the object
            // size (append targets — e.g. small WAL/manifest blobs — stay small).
            let mut buf = Vec::with_capacity(initial_size as usize);
            if exists {
                let existing = self.block_on(self.op.read(p)).map_err(|e| {
                    map_opendal_err(e, &format!("open_writable_file append-read: {p}"))
                })?;
                buf.extend_from_slice(&existing.to_vec());
            }
            WriterKind::Buffered {
                op: self.op.clone(),
                buf,
            }
        } else {
            let w = blocking
                .writer_with(p)
                .append(append)
                .call()
                .map_err(|e| map_opendal_err(e, &format!("open_writable_file writer: {p}")))?;
            WriterKind::Blocking(w)
        };

        // FRS-FADVISE: resolve the absolute on-disk path for the local `fs` service
        // so close() can drop this file's pages from the page cache. opendal's
        // `info().root()` is the absolute mount root; `p` is the path relative to it.
        let fadvise_path = if self.op.info().scheme() == opendal::Scheme::Fs {
            let root: String = self.op.info().root().to_string();
            Some(std::path::Path::new(&root).join(p.trim_start_matches('/')))
        } else {
            None
        };

        Ok(Box::new(OpendalWritableFile {
            path: p.to_string(),
            writer: Some(writer_kind),
            handle: self.rt.handle(),
            bytes_written: initial_size,
            closed: false,
            pending: file_pending,
            upload_sem: file_sem,
            fadvise_path,
            coalesce: Vec::new(),
        }))
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        let p = path_str(path, "file_exists")?;
        match self.block_on(self.op.exists(p)) {
            Ok(true) => Ok(true),
            // 2026-05-30 WRITE-BACK VISIBILITY RACE FIX: a `false`/NotFound here may
            // mean the object's async upload (write-back close) has not yet landed
            // on the backend — `sync()`/`close()` returns BEFORE the spawned upload
            // completes. Await any in-flight upload for this exact path (a cheap
            // no-op when none is registered) and retry once, so a stat that races a
            // just-written object sees it. Closes the same NotFound surface as the
            // cached_fs `remote_size_awaiting_upload` retry, at the raw backend.
            Ok(false) | Err(_) => {
                self.await_upload(path)?;
                match self.block_on(self.op.exists(p)) {
                    Ok(b) => Ok(b),
                    Err(e) if e.kind() == OdErrorKind::NotFound => Ok(false),
                    Err(e) => Err(map_opendal_err(e, &format!("file_exists: {p}"))),
                }
            }
        }
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        let p = path_str(path, "get_file_metadata")?;
        let meta = match self.block_on(self.op.stat(p)) {
            Ok(m) => m,
            // WRITE-BACK VISIBILITY RACE FIX (see `file_exists`): await any in-flight
            // async upload of this path then retry once before surfacing the error.
            Err(_) => {
                self.await_upload(path)?;
                self.block_on(self.op.stat(p))
                    .map_err(|e| map_opendal_err(e, &format!("get_file_metadata: {p}")))?
            }
        };
        Ok(FileMetadata {
            path: PathBuf::from(p),
            size: meta.content_length(),
            is_dir: meta.is_dir(),
        })
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        let raw = path_str(dir, "list_dir")?;
        // OpenDAL list requires a trailing `/` for directory listing. We
        // append it on the caller's behalf if missing — this matches what
        // local POSIX filesystems do implicitly.
        let dir_str: String = if raw.ends_with('/') || raw.is_empty() {
            raw.to_string()
        } else {
            format!("{raw}/")
        };

        // Default `list` only fetches Mode; we need ContentLength too so
        // FileMetadata.size is meaningful. Request both via `list_with`.
        // `list_with` returns an `OperatorFuture` (only `IntoFuture`), so we
        // explicitly drive it via `IntoFuture::into_future()` before
        // handing to `block_on`.
        use std::future::IntoFuture;
        let entries = self
            .block_on(
                self.op
                    .list_with(&dir_str)
                    .metakey(Metakey::Mode | Metakey::ContentLength)
                    .into_future(),
            )
            .map_err(|e| map_opendal_err(e, &format!("list_dir: {dir_str}")))?;

        let mut out = Vec::with_capacity(entries.len());
        for entry in entries {
            // Skip the directory marker itself (some services include it).
            if entry.path() == dir_str {
                continue;
            }
            let meta = entry.metadata();
            out.push(FileMetadata {
                path: PathBuf::from(entry.path()),
                size: meta.content_length(),
                is_dir: meta.is_dir(),
            });
        }
        Ok(out)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        // FRS-S3-DIRMARKER: Object stores (S3/GCS/Azure/OSS/Memory) have no real
        // directories — the path hierarchy is implicit and auto-created on the first
        // object write. OpenDAL emulates a directory via `create_dir`, which PUTs a
        // zero-byte trailing-slash "marker" object. Some S3-compatible services
        // (notably Baidu BOS) REJECT that marker PUT with HTTP 400 InvalidArgument,
        // which surfaced as `frs_db_open_remote_with_options (status=IO)` on the first
        // keyed-state DB open (the engine calls create_dir_all on the db path before
        // any real write). Only the local `Fs` backend needs a real mkdir; for every
        // other scheme this is a correct no-op — the subsequent object writes create
        // the hierarchy. (Confirmed: a plain object PUT to the same prefix succeeds
        // while the directory-marker PUT 400s.)
        if self.op.info().scheme() != opendal::Scheme::Fs {
            return Ok(());
        }
        let raw = path_str(dir, "create_dir_all")?;
        let dir_str: String = if raw.ends_with('/') {
            raw.to_string()
        } else {
            format!("{raw}/")
        };
        self.block_on(self.op.create_dir(&dir_str))
            .map_err(|e| map_opendal_err(e, &format!("create_dir_all: {dir_str}")))
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        let p = path_str(path, "delete_file")?;
        // OpenDAL's `delete` is idempotent — it returns Ok even when the
        // object is missing. ForSt-RS expects NotFound, so we explicitly
        // probe first.
        let exists = self
            .block_on(self.op.exists(p))
            .map_err(|e| map_opendal_err(e, &format!("delete_file exists: {p}")))?;
        if !exists {
            return Err(ForstError::not_found(format!("delete_file: {p}")));
        }
        self.block_on(self.op.delete(p))
            .map_err(|e| map_opendal_err(e, &format!("delete_file: {p}")))
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        let raw = path_str(path, "delete_dir")?;
        let dir_str: String = if raw.ends_with('/') {
            raw.to_string()
        } else {
            format!("{raw}/")
        };
        if recursive {
            self.block_on(self.op.remove_all(&dir_str))
                .map_err(|e| map_opendal_err(e, &format!("delete_dir recursive: {dir_str}")))
        } else {
            // For non-recursive delete we first check that the directory is
            // empty; OpenDAL's `delete` on a directory marker only removes
            // that marker, leaving children orphaned. Reject if children
            // exist to match POSIX semantics.
            let entries = self
                .block_on(self.op.list(&dir_str))
                .map_err(|e| map_opendal_err(e, &format!("delete_dir list: {dir_str}")))?;
            let non_self: Vec<_> = entries
                .into_iter()
                .filter(|e| e.path() != dir_str)
                .collect();
            if !non_self.is_empty() {
                return Err(ForstError::Io(std::io::Error::other(format!(
                    "delete_dir: directory not empty: {dir_str}"
                ))));
            }
            self.block_on(self.op.delete(&dir_str))
                .map_err(|e| map_opendal_err(e, &format!("delete_dir: {dir_str}")))
        }
    }

    /// FRS-S3-SSTRENAME: object stores have no atomic server-side rename, so
    /// the SST/MANIFEST write paths must stream straight to the final key
    /// rather than staging to `.tmp` + rename. Only the local `Fs` scheme
    /// keeps the temp→rename convention. See `FileSystem::supports_atomic_rename`.
    fn supports_atomic_rename(&self) -> bool {
        self.op.info().scheme() == opendal::Scheme::Fs
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        let s = path_str(src, "rename src")?;
        let d = path_str(dst, "rename dst")?;
        // The engine relies on rename for atomic temp→final publication. A copy+delete fallback
        // can expose two objects or publish the destination while returning failure if deleting
        // the source fails, so object stores without native rename must fail fast.
        match self.block_on(self.op.rename(s, d)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == OdErrorKind::Unsupported => Err(ForstError::not_supported(
                format!("rename requires native atomic move support: {s} -> {d}: {e}"),
            )),
            Err(e) => Err(map_opendal_err(e, &format!("rename: {s} -> {d}"))),
        }
    }

    /// 2026-05-29 WRITE-BACK FLUSH: block until the in-flight async upload of
    /// `path` (if any) has completed, propagating its error. CLONES the broadcast
    /// `watch` receiver and waits for the published outcome, so any number of
    /// concurrent readers awaiting the SAME path all block until the single
    /// upload truly finishes (the consume-once `JoinHandle` it replaced let a
    /// second awaiter race ahead and read a not-yet-uploaded object → NotFound).
    /// No pending entry → `Ok(())` (written synchronously, already completed, or
    /// never async).
    fn await_upload(&self, path: &Path) -> ForstResult<()> {
        let p = path_str(path, "await_upload")?;
        // 2026-05-29 RACE FIX: CLONE the receiver (do not remove) so concurrent
        // awaiters all block on the SAME upload completion instead of one of them
        // racing ahead with no handle. The receiver stays registered until the
        // outcome is observed below, then is removed to bound the map.
        let rx = {
            let pending = self.pending.lock().expect("upload registry poisoned");
            pending.get(p).cloned()
        };
        let Some(mut rx) = rx else {
            // No pending entry: written synchronously, already completed + removed,
            // or never async. The object is durable.
            return Ok(());
        };
        let outcome = self.block_on(async move {
            match rx.wait_for(|v| v.is_some()).await {
                Ok(g) => g.clone().unwrap_or(Ok(())),
                // Sender dropped without publishing — only happens if the upload
                // task was aborted (FS drop / shutdown). Surface as an error so a
                // read never proceeds against a possibly-absent object.
                Err(_) => Err(format!(
                    "await_upload {p}: upload task dropped before completion"
                )),
            }
        });
        // Completed: drop the entry so the map does not grow unbounded. A late
        // awaiter that missed it returns Ok (object is durable by now).
        self.pending
            .lock()
            .expect("upload registry poisoned")
            .remove(p);
        outcome.map_err(|msg| ForstError::Io(std::io::Error::other(msg)))
    }

    /// 2026-05-29 WRITE-BACK FLUSH: durability barrier. Drains EVERY in-flight
    /// upload handle and blocks on all of them, then returns the FIRST error
    /// observed. We await all handles before returning so no task is left
    /// dangling even on the error path (a checkpoint that aborts on one bad
    /// upload must still not leave others racing in the background). This is the
    /// guarantee that a checkpoint never references an SST that is only in a
    /// local/in-flight buffer.
    fn await_all_uploads(&self) -> ForstResult<()> {
        // 2026-05-29 RACE FIX: drain all receivers and wait for each outcome via
        // the broadcast `watch` channel (see `await_upload`). Draining is safe
        // here because this is the barrier — no concurrent reader should be
        // mid-`await_upload` for the same path at a checkpoint/shutdown boundary,
        // and even if one is, it holds its own cloned receiver.
        let receivers: Vec<tokio::sync::watch::Receiver<Option<UploadOutcome>>> = {
            let mut pending = self.pending.lock().expect("upload registry poisoned");
            pending.drain().map(|(_, rx)| rx).collect()
        };
        let mut first_err: Option<ForstError> = None;
        for mut rx in receivers {
            let outcome = self.block_on(async move {
                match rx.wait_for(|v| v.is_some()).await {
                    Ok(g) => g.clone().unwrap_or(Ok(())),
                    Err(_) => Ok(()),
                }
            });
            if let Err(msg) = outcome {
                if first_err.is_none() {
                    first_err = Some(ForstError::Io(std::io::Error::other(msg)));
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn name(&self) -> &str {
        // The cached name was materialized at construction from
        // `op.info().scheme()` (which returns a `Scheme` enum, not `&str`).
        &self.name
    }

    /// FRS-SCAN-OPEN-FANOUT: OpenDAL random-access files always take the remote
    /// regime (`OpendalRandomAccessFile::is_local()==false`), so opens can pay a
    /// round-trip — report NOT-local at the FS level so the scan open-fanout
    /// engages.
    fn is_local(&self) -> bool {
        false
    }
}

impl Drop for OpendalFileSystem {
    /// 2026-05-29 WRITE-BACK FLUSH: best-effort drain of any in-flight uploads on
    /// clean shutdown so no SST upload is lost. We never panic in `Drop`; a
    /// failed upload is logged. The engine's `DbImpl` close path calls
    /// `await_all_uploads` explicitly BEFORE the filesystem is dropped (the
    /// surfacing point for errors); this is the backstop for any handle that
    /// was registered after that barrier.
    fn drop(&mut self) {
        if let Err(e) = self.await_all_uploads() {
            tracing::warn!(
                target: "forst_rs_io::opendal_backend",
                error = %e,
                "OpendalFileSystem dropped with a failing in-flight upload",
            );
        }
    }
}

// `OpendalFileSystem` is `Send + Sync`: `Operator` is `Send + Sync`, the
// runtime handle is `Send + Sync`, and the cached `name` is immutable
// after construction. The trait `FileSystem: Send + Sync` is therefore
// satisfied automatically.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OpendalFileSystem>();
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // --- R45-M1 IPv6-aware authority parsing --------------------------------

    /// R45-M1: `extract_host_from_authority` must not butcher IPv6
    /// literals. Pre-fix, splitting on the first `:` would turn
    /// `[::1]:9000` into `[`, which never matches the loopback list and
    /// silently spammed the warning on every loopback IPv6 endpoint.
    #[test]
    fn test_r45_m1_extract_host_ipv4_strips_port() {
        assert_eq!(extract_host_from_authority("127.0.0.1:9000"), "127.0.0.1");
        assert_eq!(extract_host_from_authority("127.0.0.1"), "127.0.0.1");
        assert_eq!(extract_host_from_authority("localhost:9000"), "localhost");
        assert_eq!(extract_host_from_authority("localhost"), "localhost");
    }

    #[test]
    fn test_r45_m1_extract_host_ipv6_unwraps_brackets() {
        assert_eq!(extract_host_from_authority("[::1]:9000"), "::1");
        assert_eq!(extract_host_from_authority("[::1]"), "::1");
        assert_eq!(
            extract_host_from_authority("[2001:db8::1]:9000"),
            "2001:db8::1"
        );
        assert_eq!(
            extract_host_from_authority("[fe80::1%eth0]:80"),
            "fe80::1%eth0"
        );
    }

    #[test]
    fn test_r45_m1_extract_host_malformed_returns_input() {
        // Unclosed bracket — preserve the input so the caller treats it
        // as a non-loopback host and emits the warning (conservative).
        assert_eq!(extract_host_from_authority("[::1"), "[::1");
    }

    /// R46-L1: unbracketed IPv6 literals (multi-colon, no `[`) must not
    /// have their rightmost-`:` segment stripped — that would butcher
    /// `::1` into `:` and `2001:db8::1` into `2001:db8::`. RFC 3986
    /// §3.2.2 requires brackets when a port is present; multi-colon
    /// authorities without brackets are either literals or malformed,
    /// and stripping is wrong in both cases. The conservative choice
    /// is to return the input unchanged.
    #[test]
    fn test_r46_l1_extract_host_unbracketed_ipv6_preserved() {
        // Loopback IPv6 literal, no brackets, no port.
        assert_eq!(extract_host_from_authority("::1"), "::1");
        // Full IPv6 literal, no brackets, no port.
        assert_eq!(extract_host_from_authority("2001:db8::1"), "2001:db8::1");
        // Compressed link-local, no brackets, no port.
        assert_eq!(extract_host_from_authority("fe80::1"), "fe80::1");
        // Single-colon strings remain `host:port`-shaped — unchanged
        // from the R45-M1 behaviour.
        assert_eq!(extract_host_from_authority("127.0.0.1:9000"), "127.0.0.1");
    }

    // --- Memory backend round-trips -----------------------------------------

    #[test]
    fn test_opendal_memory_roundtrip() {
        let fs = OpendalFileSystem::memory().expect("build memory fs");

        let path = Path::new("hello.bin");
        let payload = b"opendal-memory-roundtrip-payload";

        // Write
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(payload).expect("append");
        w.sync().expect("sync");
        assert_eq!(w.file_size().unwrap(), payload.len() as u64);
        drop(w);

        // 2026-05-29 WRITE-BACK FLUSH: the memory backend is a non-Fs scheme, so
        // the buffered write is uploaded asynchronously; await it before
        // reading back (mirrors the engine's await-before-read guards).
        fs.await_upload(path).expect("await_upload");

        // Sequential read
        let mut r = fs.open_sequential_file(path).expect("open sequential");
        let mut buf = vec![0u8; payload.len() + 16];
        let n = r.read(&mut buf).expect("read");
        assert_eq!(n, payload.len());
        assert_eq!(&buf[..n], payload);

        // Random-access read across a slice
        let rar = fs.open_random_access_file(path).expect("open random");
        assert_eq!(rar.file_size().unwrap(), payload.len() as u64);
        let mut chunk = [0u8; 7];
        let n = rar.read_at(8, &mut chunk).expect("read_at");
        assert_eq!(n, 7);
        assert_eq!(&chunk[..n], &payload[8..15]);
    }

    // --- Local FS backend round-trips ---------------------------------------

    #[test]
    fn test_opendal_local_fs_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let fs = OpendalFileSystem::local(tmp.path()).expect("build local fs");

        let path = Path::new("nested/data.bin");
        // Need to create the parent dir first; OpenDAL FS auto-creates
        // intermediate dirs only on write, so we test without create_dir.
        let payload = b"local-fs-via-opendal";

        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(payload).expect("append");
        w.sync().expect("sync");
        drop(w);

        // Re-read via sequential
        let mut r = fs.open_sequential_file(path).expect("open sequential");
        let mut buf = vec![0u8; payload.len()];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(n, payload.len());
        assert_eq!(&buf, payload);

        // Verify on disk under the temp root
        let on_disk = tmp.path().join("nested/data.bin");
        assert!(on_disk.exists(), "expected file on disk at {on_disk:?}");
        assert_eq!(std::fs::read(&on_disk).unwrap(), payload);
    }

    // --- list_dir returns all written entries -------------------------------

    #[test]
    fn test_opendal_list() {
        let fs = OpendalFileSystem::memory().expect("build memory fs");

        for i in 0..3 {
            let path = PathBuf::from(format!("listing/file_{i}.dat"));
            let mut w = fs
                .open_writable_file(&path, WriteMode::CreateOrTruncate)
                .expect("open writable");
            w.append(format!("payload-{i}").as_bytes()).expect("append");
            w.sync().expect("sync");
        }
        // WRITE-BACK FLUSH: await all async uploads before listing.
        fs.await_all_uploads().expect("await_all_uploads");

        let entries = fs
            .list_dir(Path::new("listing"))
            .expect("list_dir succeeds");
        // Filter out any spurious self-references just in case.
        let files: Vec<_> = entries.into_iter().filter(|e| !e.is_dir).collect();
        assert_eq!(files.len(), 3, "expected 3 file entries, got {files:?}");

        let mut names: Vec<String> = files
            .iter()
            .map(|e| {
                e.path
                    .file_name()
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default()
            })
            .collect();
        names.sort();
        assert_eq!(names, vec!["file_0.dat", "file_1.dat", "file_2.dat"]);
    }

    // --- delete removes the file --------------------------------------------

    #[test]
    fn test_opendal_delete() {
        let fs = OpendalFileSystem::memory().expect("build memory fs");

        let path = Path::new("ephemeral.dat");
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(b"transient").expect("append");
        w.sync().expect("sync");
        drop(w);
        // WRITE-BACK FLUSH: await the async upload so the object is published.
        fs.await_upload(path).expect("await_upload");

        assert!(
            fs.file_exists(path).unwrap(),
            "file should exist after sync"
        );

        fs.delete_file(path).expect("delete");
        assert!(
            !fs.file_exists(path).unwrap(),
            "file should be gone after delete"
        );

        // Deleting twice surfaces NotFound.
        let err = fs.delete_file(path).expect_err("second delete must fail");
        assert!(err.is_not_found(), "expected NotFound, got {err}");
    }

    // --- WriteMode::CreateNew rejects existing ------------------------------

    #[test]
    fn test_opendal_create_new_rejects_existing() {
        let fs = OpendalFileSystem::memory().unwrap();
        let path = Path::new("dup.dat");

        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"first").unwrap();
        w.sync().unwrap();
        drop(w);
        // WRITE-BACK FLUSH: await the async upload so the object is published.
        fs.await_upload(path).expect("await_upload");

        let err = match fs.open_writable_file(path, WriteMode::CreateNew) {
            Err(e) => e,
            Ok(_) => panic!("CreateNew on existing must fail"),
        };
        assert!(err.is_invalid_argument(), "got {err}");
    }

    #[test]
    fn test_s3_with_root_preserves_bucket_prefix() {
        let fs = OpendalFileSystem::s3_with_root(
            "forst-checkpoints",
            "jobs/app-1/chk",
            "us-east-1",
            Some("http://127.0.0.1:9000"),
            Some("access-key"),
            Some("secret-key"),
        )
        .expect("build s3 fs");

        assert_eq!(fs.op.info().name(), "forst-checkpoints");
        assert_eq!(fs.op.info().root(), "/jobs/app-1/chk/");
    }

    // --- WriteMode::Append preserves existing bytes -------------------------

    #[test]
    fn test_opendal_append_mode() {
        let fs = OpendalFileSystem::memory().unwrap();
        let path = Path::new("appended.dat");

        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"hello ").unwrap();
        w.sync().unwrap();
        drop(w);

        let mut w = fs.open_writable_file(path, WriteMode::Append).unwrap();
        w.append(b"world").unwrap();
        w.sync().unwrap();
        drop(w);

        let mut r = fs.open_sequential_file(path).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello world");
    }

    // --- Non-UTF-8 paths rejected -------------------------------------------

    #[cfg(unix)]
    #[test]
    fn test_opendal_non_utf8_path_rejected() {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let fs = OpendalFileSystem::memory().unwrap();
        let bad = OsStr::from_bytes(b"bad\xFFname.dat");
        let p = Path::new(bad);

        let err = match fs.open_writable_file(p, WriteMode::CreateOrTruncate) {
            Err(e) => e,
            Ok(_) => panic!("non-UTF-8 path must be rejected"),
        };
        assert!(err.is_invalid_argument(), "got {err}");
    }

    // --- PR-A12: default RetryLayer is non-destructive on happy-path ------
    //
    // The retry layer ships on every constructed `OpendalFileSystem`. We
    // can't easily fault-inject from a unit test (the `ChaosLayer` is
    // feature-gated and we don't enable it here), so this test pins two
    // looser but still-meaningful contracts:
    //
    //   1. The default constructor builds with the retry layer attached
    //      and a happy-path round-trip still succeeds (no regression vs
    //      the no-retry variant).
    //   2. The `with_operator_no_retry` escape hatch exists for callers
    //      that need to observe raw errors (used by tests / custom
    //      retry policies). Both paths produce equal results on a
    //      successful write+read.
    //
    // Real fault-injection lives in the Java `S3Retry*Test` suite where
    // we mock the storage layer to surface transient errors.
    #[test]
    fn test_pr_a12_default_retry_layer_happy_path() {
        // Direct constructor (retry attached).
        let with_retry = OpendalFileSystem::memory().expect("build memory fs with retry");

        // Mirror the same operator but bypass the retry layer.
        let raw_op = Operator::new(opendal::services::Memory::default())
            .expect("memory builder")
            .finish();
        let no_retry =
            OpendalFileSystem::with_operator_no_retry(raw_op).expect("build memory fs no retry");

        let path = Path::new("retry/probe.bin");
        let payload = b"pr-a12-retry-layer-roundtrip";

        for fs in [&with_retry, &no_retry] {
            let mut w = fs
                .open_writable_file(path, WriteMode::CreateOrTruncate)
                .expect("open writable");
            w.append(payload).expect("append");
            w.sync().expect("sync");
            drop(w);
            // WRITE-BACK FLUSH: await the async upload before read-back.
            fs.await_upload(path).expect("await_upload");

            let mut r = fs.open_sequential_file(path).expect("open sequential");
            let mut buf = vec![0u8; payload.len()];
            let n = r.read(&mut buf).expect("read");
            assert_eq!(n, payload.len());
            assert_eq!(
                &buf, payload,
                "retry layer must be transparent on happy path"
            );
        }
    }

    // --- PR-A12: retry policy builder pins the documented parameters ------
    //
    // The default policy is referenced by the Java-side SstRetryStrategy
    // (max=5, base=100ms, factor=2.0, cap=30s, jitter). Any future tuning
    // must update both sides in lock-step. This test pins the builder
    // exists and produces a usable layer; we can't introspect the
    // configured parameters because RetryLayer is opaque, so this is
    // really a "did someone delete the function" guard.
    #[test]
    fn test_pr_a12_default_retry_layer_constructible() {
        let _layer = default_retry_layer();
        // The layer is opaque; just confirm we can apply it to a fresh
        // operator without panicking. The happy-path test above
        // exercises that operations still complete.
        let op = Operator::new(opendal::services::Memory::default())
            .expect("memory builder")
            .finish();
        let _wrapped = op.layer(default_retry_layer());
    }

    // --- name() and Debug ---------------------------------------------------

    #[test]
    fn test_opendal_name_and_debug() {
        let fs = OpendalFileSystem::memory().unwrap();
        assert!(fs.name().contains("Opendal"), "got {}", fs.name());
        let dbg = format!("{fs:?}");
        assert!(dbg.contains("OpendalFileSystem"), "got {dbg}");
    }

    // --- rename requires native atomic move support -------------------------
    //
    // The in-memory service rejects `rename` with `Unsupported`. The engine
    // relies on rename for atomic temp→final publication, so copy+delete must
    // not be used as a transparent fallback.
    #[test]
    fn test_opendal_rename_unsupported_fails_fast() {
        let fs = OpendalFileSystem::memory().unwrap();
        let src = Path::new("rename/src.dat");
        let dst = Path::new("rename/dst.dat");

        let payload = b"rename-fallback-payload";
        {
            let mut w = fs
                .open_writable_file(src, WriteMode::CreateOrTruncate)
                .unwrap();
            w.append(payload).unwrap();
            w.sync().unwrap();
        }
        // WRITE-BACK FLUSH: await the async upload so the source is published.
        fs.await_upload(src).expect("await_upload");
        assert!(fs.file_exists(src).unwrap());
        assert!(!fs.file_exists(dst).unwrap());

        let err = fs
            .rename(src, dst)
            .expect_err("unsupported rename must fail");
        assert!(err.is_not_supported(), "got {err}");

        assert!(
            fs.file_exists(src).unwrap(),
            "source must remain after failed rename"
        );
        assert!(
            !fs.file_exists(dst).unwrap(),
            "destination must not be published by failed rename"
        );
    }

    // --- PR-D1: streaming round-trip on the storage layer -------------------
    //
    // Writes a 1 MiB payload, then reads it back via both the sequential
    // and random-access readers. Sequential reads stream through OpenDAL's
    // blocking reader, while random-access reads copy directly from OpenDAL
    // buffers into the caller slice. The test asserts data equality across multiple read modes
    // and offsets, which exercises:
    //   - `OpendalSequentialFile` streaming `StdReader`,
    //   - `OpendalRandomAccessFile::read_at` using `Buf::copy_to_slice`
    //     (streaming copy, no `Vec` intermediate),
    //   - `OpendalWritableFile` streaming writer close.
    #[test]
    fn bytes_zero_copy_round_trip() {
        use std::path::Path;

        let fs = OpendalFileSystem::memory().expect("build memory fs");
        let path = Path::new("zero_copy/payload.bin");

        // 1 MiB pseudo-random payload (xorshift32 so the test is
        // deterministic and doesn't pull in `rand`).
        const SIZE: usize = 1 << 20;
        let mut payload = Vec::with_capacity(SIZE);
        let mut state: u32 = 0xDEAD_BEEF;
        while payload.len() < SIZE {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            payload.extend_from_slice(&state.to_le_bytes());
        }
        payload.truncate(SIZE);

        // Write via OpendalWritableFile's streaming writer.
        {
            let mut w = fs
                .open_writable_file(path, WriteMode::CreateOrTruncate)
                .expect("open writable");
            w.append(&payload).expect("append");
            w.sync().expect("sync");
        }
        // WRITE-BACK FLUSH: await the async upload before read-back.
        fs.await_upload(path).expect("await_upload");

        // Sequential read covers the full object through the streaming reader.
        {
            let mut r = fs.open_sequential_file(path).expect("open sequential");
            let mut buf = vec![0u8; SIZE];
            let mut total = 0;
            while total < SIZE {
                let n = r.read(&mut buf[total..]).expect("read");
                if n == 0 {
                    break;
                }
                total += n;
            }
            assert_eq!(total, SIZE, "expected full payload");
            assert_eq!(&buf[..], &payload[..], "sequential payload mismatch");
        }

        // Random-access reads at three offsets — exercises Buf::copy_to_slice
        // streaming path. We deliberately pick a non-power-of-2 offset so
        // any latent off-by-one in range arithmetic surfaces.
        let rar = fs.open_random_access_file(path).expect("open random");
        assert_eq!(rar.file_size().unwrap(), SIZE as u64);

        for &(offset, len) in &[(0usize, 4096), (12_345usize, 7_891), (SIZE - 1024, 1024)] {
            let mut chunk = vec![0u8; len];
            let n = rar.read_at(offset as u64, &mut chunk).expect("read_at");
            assert_eq!(n, len, "short read at offset {offset}");
            assert_eq!(
                &chunk[..],
                &payload[offset..offset + len],
                "random-read payload mismatch at offset {offset}"
            );
        }

        // Read past EOF returns 0 (no Vec allocated, no copy).
        let mut chunk = [0u8; 32];
        let n = rar.read_at(SIZE as u64, &mut chunk).expect("read_at eof");
        assert_eq!(n, 0);
    }

    /// `read_ranges` (concurrent OpenDAL override) must return byte-identical
    /// results to N serial `read_at` calls — including an EOF-shortened tail
    /// range and a fully-past-EOF range. Uses > READ_RANGES_CONCURRENCY (8)
    /// ranges so the pipeline-refill path is exercised.
    #[test]
    fn read_ranges_matches_serial_read_at() {
        use std::path::Path;

        let fs = OpendalFileSystem::memory().expect("build memory fs");
        let path = Path::new("ranges/payload.bin");

        const SIZE: usize = 200_000;
        let mut payload = Vec::with_capacity(SIZE);
        let mut state: u32 = 0x1234_5678;
        while payload.len() < SIZE {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            payload.extend_from_slice(&state.to_le_bytes());
        }
        payload.truncate(SIZE);

        {
            let mut w = fs
                .open_writable_file(path, WriteMode::CreateOrTruncate)
                .expect("open writable");
            w.append(&payload).expect("append");
            w.sync().expect("sync");
        }
        // WRITE-BACK FLUSH: await the async upload before read-back.
        fs.await_upload(path).expect("await_upload");

        let rar = fs.open_random_access_file(path).expect("open random");

        // 11 ranges: contiguous chunks, an unaligned offset, a tail that runs
        // past EOF (shortened), and a fully-past-EOF range (empty).
        let ranges: Vec<(u64, usize)> = vec![
            (0, 4096),
            (4096, 4096),
            (8192, 4096),
            (12_288, 4096),
            (16_384, 4096),
            (20_480, 4096),
            (24_576, 4096),
            (28_672, 4096),
            (32_768, 4096),
            (12_345, 7_891),
            ((SIZE - 100) as u64, 4096), // shortened to 100 bytes
            (SIZE as u64, 512),          // fully past EOF -> empty
        ];

        let concurrent = rar.read_ranges(&ranges).expect("read_ranges");
        assert_eq!(concurrent.len(), ranges.len());

        for (i, &(off, len)) in ranges.iter().enumerate() {
            // Serial reference via repeated read_at with the short-read loop.
            let mut serial = vec![0u8; len];
            let mut filled = 0;
            while filled < len {
                let n = rar
                    .read_at(off + filled as u64, &mut serial[filled..])
                    .expect("read_at");
                if n == 0 {
                    break;
                }
                filled += n;
            }
            serial.truncate(filled);

            assert_eq!(
                concurrent[i], serial,
                "concurrent range {i} (off={off} len={len}) differs from serial"
            );

            // And both must match the source payload slice.
            let end = (off as usize + len).min(SIZE);
            let expected = if off as usize >= SIZE {
                &[][..]
            } else {
                &payload[off as usize..end]
            };
            assert_eq!(concurrent[i], expected, "range {i} differs from source");
        }
    }

    // --- 2026-05-29 WRITE-BACK FLUSH async upload --------------------------

    /// The memory backend's scheme is NOT `Fs`, so a CreateOrTruncate write
    /// goes through the buffered async upload path. After `await_upload`, the
    /// object must exist on the remote with byte-identical contents.
    #[test]
    fn test_async_upload_await_then_read_back() {
        let fs = OpendalFileSystem::memory().expect("build memory fs");
        let path = Path::new("sst/000007.sst");
        let payload: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();

        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(&payload).expect("append");
        // close (sync) SPAWNS the upload and returns immediately.
        w.sync().expect("sync");
        drop(w);

        // The durability barrier: block until the spawned upload completes.
        fs.await_upload(path).expect("await_upload");

        // Object must now exist and be byte-identical.
        assert!(fs.file_exists(path).expect("file_exists"));
        let rar = fs.open_random_access_file(path).expect("open random");
        assert_eq!(rar.file_size().unwrap(), payload.len() as u64);
        let mut got = vec![0u8; payload.len()];
        let mut filled = 0;
        while filled < got.len() {
            let n = rar
                .read_at(filled as u64, &mut got[filled..])
                .expect("read_at");
            if n == 0 {
                break;
            }
            filled += n;
        }
        got.truncate(filled);
        assert_eq!(got, payload, "read-back bytes differ from written payload");

        // await_upload is idempotent: a second call (no pending entry) is Ok.
        fs.await_upload(path).expect("await_upload idempotent");
    }

    /// Opening a just-closed buffered object must wait for its pending async
    /// upload. Engine compaction/read paths do not call `await_upload`
    /// separately before opening newly version-visible SSTs.
    #[test]
    fn test_random_access_open_waits_for_pending_upload() {
        let fs = OpendalFileSystem::memory().expect("build memory fs");
        let path = Path::new("sst/000008.sst");
        let payload: Vec<u8> = (0..2_000_000u32).map(|i| (i % 239) as u8).collect();

        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(&payload).expect("append");
        w.sync().expect("sync");
        drop(w);

        let rar = fs.open_random_access_file(path).expect("open random");
        assert_eq!(rar.file_size().unwrap(), payload.len() as u64);
        let mut got = vec![0u8; payload.len()];
        let n = rar.read_at(0, &mut got).expect("read_at");
        got.truncate(n);
        assert_eq!(got, payload);
    }

    /// `await_all_uploads` must drain every in-flight upload across multiple
    /// objects and make them all readable.
    #[test]
    fn test_async_upload_await_all() {
        let fs = OpendalFileSystem::memory().expect("build memory fs");
        let mut payloads = Vec::new();
        for n in 0..5u32 {
            let path = PathBuf::from(format!("sst/{n:06}.sst"));
            let payload: Vec<u8> = (0..50_000u32).map(|i| ((i + n) % 211) as u8).collect();
            let mut w = fs
                .open_writable_file(&path, WriteMode::CreateOrTruncate)
                .expect("open writable");
            w.append(&payload).expect("append");
            w.sync().expect("sync");
            payloads.push((path, payload));
        }

        // Durability barrier for ALL uploads.
        fs.await_all_uploads().expect("await_all_uploads");

        for (path, payload) in &payloads {
            assert!(fs.file_exists(path).expect("file_exists"));
            let rar = fs.open_random_access_file(path).expect("open random");
            assert_eq!(rar.file_size().unwrap(), payload.len() as u64);
            let mut got = vec![0u8; payload.len()];
            let n = rar.read_at(0, &mut got).expect("read_at");
            got.truncate(n);
            assert_eq!(&got, payload, "object {} differs", path.display());
        }

        // Idempotent: nothing left pending.
        fs.await_all_uploads()
            .expect("await_all_uploads idempotent");
    }
}

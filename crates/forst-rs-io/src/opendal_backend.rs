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
//! - [`OpendalSequentialFile`]: eagerly downloads the entire object on
//!   open, then serves reads from an in-memory cursor. SSTs and WAL
//!   segments fit comfortably here.
//! - [`OpendalRandomAccessFile`]: serves [`RandomAccessFile::read_at`] via
//!   `op.read_with(path).range(off..off+len)`, performing one ranged GET
//!   per call.
//! - [`OpendalWritableFile`]: buffers the entire write in memory and
//!   flushes the object on [`WritableFile::sync`] (or on `Drop` as a
//!   best-effort fallback). This matches the typical SST/WAL write
//!   pattern (build → flush → never re-open) and avoids OpenDAL's more
//!   complex multipart writer state machine.
//!
//! # Path handling
//!
//! All paths are converted to `&str` via `Path::to_str()`. Non-UTF-8
//! paths are rejected with [`ForstError::invalid_argument`]. Directory
//! operations append a trailing `/` to satisfy OpenDAL's convention.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::time::Duration;

use bytes::{Buf, Bytes};
use forst_rs_common::error::{ForstError, ForstResult};
use opendal::layers::{BlockingLayer, RetryLayer};
use opendal::{ErrorKind as OdErrorKind, Metakey, Operator};
use tokio::runtime::{Handle, Runtime};

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
pub struct OpendalFileSystem {
    op: Operator,
    rt: RuntimeHandle,
    name: String,
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

impl OpendalFileSystem {
    /// Wraps an existing [`opendal::Operator`].
    ///
    /// The operator is wrapped in a [`RetryLayer`] (see
    /// [`default_retry_layer`]) so transient S3/GCS/Azure failures
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
        Ok(Self { op, rt, name })
    }

    /// Wraps an existing [`opendal::Operator`] WITHOUT attaching the
    /// default [`RetryLayer`].
    ///
    /// Used by tests that need to observe the underlying error
    /// behaviour, or by callers that have already attached a custom
    /// retry policy to the operator. Production code should prefer
    /// [`with_operator`].
    pub fn with_operator_no_retry(op: Operator) -> ForstResult<Self> {
        let rt = RuntimeHandle::acquire()?;
        let name = format!("OpendalFileSystem({})", op.info().scheme().into_static());
        Ok(Self { op, rt, name })
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
        let mut builder = opendal::services::S3::default()
            .bucket(bucket)
            .region(region);
        if let Some(ep) = endpoint {
            // R44-L1: warn when the operator points at a non-localhost
            // endpoint over plaintext HTTP. S3 credentials traversing such
            // a link are observable on the wire; production deployments
            // should use HTTPS. We do NOT refuse the configuration —
            // tests and dev MinIO setups still need cleartext — but we
            // make the risk visible in logs.
            if ep.starts_with("http://") {
                let host_part = ep.trim_start_matches("http://");
                // Trim any path component before checking the host.
                let host_only = host_part.split('/').next().unwrap_or(host_part);
                // Trim any port component.
                let host_no_port = host_only.split(':').next().unwrap_or(host_only);
                let is_loopback = host_no_port == "localhost"
                    || host_no_port == "127.0.0.1"
                    || host_no_port == "::1"
                    || host_no_port == "[::1]";
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

/// A sequential reader that holds the full object in memory.
///
/// The payload is stored as a [`bytes::Bytes`] handle — ref-counted and
/// slice-able without memcpy. Construction from `opendal::Buffer::to_bytes()`
/// is zero-copy when the underlying buffer is contiguous (the common case for
/// services that return a single chunk per GET).
pub struct OpendalSequentialFile {
    /// The full object bytes. Held as `Bytes` so cheap slicing and sharing
    /// is available to callers (e.g. the storage layer's in-memory adapters).
    bytes: Bytes,
    pos: usize,
}

impl SequentialFile for OpendalSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        let remaining = self.bytes.len().saturating_sub(self.pos);
        let n = remaining.min(buf.len());
        // `Bytes` deref-coerces to `&[u8]`; this is a single memcpy from the
        // ref-counted buffer into the caller's slice (no intermediate `Vec`).
        buf[..n].copy_from_slice(&self.bytes[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        let n_usize = usize::try_from(n).map_err(|_| {
            ForstError::invalid_argument(format!("skip offset {n} exceeds usize::MAX"))
        })?;
        self.pos = self.pos.saturating_add(n_usize).min(self.bytes.len());
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
    path: String,
    size: u64,
}

impl RandomAccessFile for OpendalRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        if offset >= self.size {
            return Ok(0);
        }
        let want = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        let end = offset.saturating_add(want).min(self.size);
        let mut buffer = self
            .op
            .read_with(&self.path)
            .range(offset..end)
            .call()
            .map_err(|e| map_opendal_err(e, &format!("OpenDAL ranged read: {}", self.path)))?;
        // `opendal::Buffer` may be non-contiguous (a sequence of `Bytes`
        // chunks). Going through `to_vec()` would force a contiguous copy
        // into a fresh `Vec`, then a second copy into the caller's slice.
        // `Buf::copy_to_slice` streams chunk-by-chunk directly into `buf` —
        // one memcpy per chunk, no intermediate `Vec` allocation.
        let n = buffer.len().min(buf.len());
        buffer.copy_to_slice(&mut buf[..n]);
        Ok(n)
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.size)
    }
}

// ---------------------------------------------------------------------------
// WritableFile — buffered, flush on sync/drop
// ---------------------------------------------------------------------------

/// A writable file that buffers all writes in memory and flushes on
/// [`Self::sync`] (or on drop as a best-effort fallback).
///
/// This matches the typical ForSt-RS write pattern (one writer per SST or
/// WAL segment, written sequentially, then sealed). Streaming uploads are
/// available via the OpenDAL writer API but are not exposed here to keep
/// the trait surface minimal.
///
/// # Buffer ownership model
///
/// The buffer lives in one of two states:
///
/// - **Mutable**: `buffer` holds a `Vec<u8>` accumulator; appends are
///   amortized-O(1) growth. `frozen` is `None`.
/// - **Frozen**: after [`Self::persist`], the accumulator has been moved
///   into a ref-counted [`bytes::Bytes`] snapshot stored in `frozen`. The
///   `Vec` is now empty. Subsequent persists (e.g. `flush(); sync();`)
///   re-PUT this snapshot via cheap ref-count clones — no memcpy.
///
/// A subsequent [`Self::append`] re-materializes the frozen snapshot back
/// into the mutable accumulator before appending. This costs one memcpy
/// per "frozen → append" transition, but matches the pre-existing cost on
/// the same path and is rare in the typical "build → sync → drop" workflow.
pub struct OpendalWritableFile {
    op: opendal::BlockingOperator,
    path: String,
    /// Mutable accumulator for in-progress appends. After a successful
    /// [`Self::persist`], this is empty and `frozen` holds the snapshot.
    buffer: Vec<u8>,
    /// Ref-counted snapshot of the most recently persisted content.
    /// Allows repeat persists (`flush(); sync();`) to re-PUT without
    /// recopying the full buffer.
    frozen: Option<Bytes>,
    /// True after sync() succeeds; suppresses the drop-time flush.
    flushed: bool,
}

impl WritableFile for OpendalWritableFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        // If the buffer was previously frozen (last call was a persist),
        // re-materialize it before appending. This is the only memcpy in
        // the writer's hot path; it only happens when callers interleave
        // appends with persists, which is uncommon for the SST flush path.
        if let Some(snapshot) = self.frozen.take() {
            // `Bytes::to_vec()` performs the copy; `into_iter().collect()`
            // would be equivalent. Keep semantics explicit.
            self.buffer = snapshot.to_vec();
        }
        self.buffer.extend_from_slice(data);
        self.flushed = false;
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        // Buffered writes in memory; nothing to push to the OS yet.
        // The semantic contract is "data is visible to readers"; for object
        // stores that means PUT. We honor it via the same path as sync() so
        // callers that flush-without-sync still observe consistent reads.
        self.persist()
    }

    fn sync(&mut self) -> ForstResult<()> {
        self.persist()
    }

    fn file_size(&self) -> ForstResult<u64> {
        // Size reflects the current logical content: the live accumulator
        // when not frozen, otherwise the frozen snapshot.
        if let Some(snapshot) = &self.frozen {
            Ok(snapshot.len() as u64)
        } else {
            Ok(self.buffer.len() as u64)
        }
    }
}

impl OpendalWritableFile {
    fn persist(&mut self) -> ForstResult<()> {
        // Writing the same content twice is idempotent for OpenDAL services
        // we target (PUT semantics). We always send the full buffer because
        // OpenDAL's simple `write` API replaces objects atomically.
        let bytes = match &self.frozen {
            // Repeat-persist (flush() then sync(), or sync() then sync()):
            // ref-count clone of the existing snapshot. Zero-copy.
            Some(snapshot) => snapshot.clone(),
            // First persist since last append: take the accumulator zero-copy
            // (Vec<u8> → Bytes via From<Vec<u8>> is zero-copy in bytes 1.x).
            // The ref-counted Bytes is shared between opendal (for the PUT)
            // and our own `frozen` slot (for future re-PUTs). No memcpy.
            None => {
                let taken: Bytes = std::mem::take(&mut self.buffer).into();
                self.frozen = Some(taken.clone());
                taken
            }
        };
        self.op
            .write(&self.path, bytes)
            .map_err(|e| map_opendal_err(e, &format!("OpenDAL write: {}", self.path)))?;
        self.flushed = true;
        Ok(())
    }
}

impl Drop for OpendalWritableFile {
    fn drop(&mut self) {
        let has_content = !self.buffer.is_empty() || self.frozen.is_some();
        if !self.flushed && has_content {
            // Best-effort flush. Errors here cannot propagate; we log via
            // tracing so operators can correlate. Callers MUST call
            // `sync()` for durability guarantees.
            if let Err(e) = self.persist() {
                tracing::warn!(
                    target: "forst_rs_io::opendal_backend",
                    path = %self.path,
                    error = %e,
                    "OpendalWritableFile dropped without sync; best-effort flush failed",
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
        let buffer = self
            .block_on(self.op.read(p))
            .map_err(|e| map_opendal_err(e, &format!("open_sequential_file: {p}")))?;
        // `Buffer::to_bytes()` is zero-copy when the underlying chunks are
        // contiguous (the common case for a single GetObject), and a single
        // concat otherwise — strictly better than `to_vec()` which forces a
        // separate `Vec` allocation regardless.
        Ok(Box::new(OpendalSequentialFile {
            bytes: buffer.to_bytes(),
            pos: 0,
        }))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let p = path_str(path, "open_random_access_file")?;
        let meta = self
            .block_on(self.op.stat(p))
            .map_err(|e| map_opendal_err(e, &format!("open_random_access_file stat: {p}")))?;
        let size = meta.content_length();
        let blocking = self.blocking_op()?;
        Ok(Box::new(OpendalRandomAccessFile {
            op: blocking,
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

        // Seed the buffer based on the requested mode. OpenDAL has no native
        // append semantics for most services, so Append is implemented as
        // "read current contents, buffer them, append new writes, PUT".
        let (buffer, exists) = match self.block_on(self.op.exists(p)) {
            Ok(true) => {
                let buf = self
                    .block_on(self.op.read(p))
                    .map_err(|e| map_opendal_err(e, &format!("open_writable_file read: {p}")))?;
                (buf.to_vec(), true)
            }
            Ok(false) => (Vec::new(), false),
            Err(e) => {
                return Err(map_opendal_err(
                    e,
                    &format!("open_writable_file exists: {p}"),
                ))
            }
        };

        let initial = match mode {
            WriteMode::CreateNew => {
                if exists {
                    return Err(ForstError::invalid_argument(format!(
                        "open_writable_file: file already exists: {p}"
                    )));
                }
                Vec::new()
            }
            WriteMode::CreateOrTruncate => Vec::new(),
            WriteMode::Append => buffer,
        };

        Ok(Box::new(OpendalWritableFile {
            op: blocking,
            path: p.to_string(),
            buffer: initial,
            frozen: None,
            flushed: false,
        }))
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        let p = path_str(path, "file_exists")?;
        match self.block_on(self.op.exists(p)) {
            Ok(b) => Ok(b),
            Err(e) if e.kind() == OdErrorKind::NotFound => Ok(false),
            Err(e) => Err(map_opendal_err(e, &format!("file_exists: {p}"))),
        }
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        let p = path_str(path, "get_file_metadata")?;
        let meta = self
            .block_on(self.op.stat(p))
            .map_err(|e| map_opendal_err(e, &format!("get_file_metadata: {p}")))?;
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

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        let s = path_str(src, "rename src")?;
        let d = path_str(dst, "rename dst")?;
        // OpenDAL's `rename` is supported on services that have native
        // move (FS, GCS, …); others return `Unsupported`. The engine
        // flush path relies on rename for atomic temp→final SST moves,
        // so we transparently fall back to copy+delete on services that
        // lack native rename. This is non-atomic but matches what
        // OpenDAL itself does internally for S3 today, and matches the
        // user's expectation that any FileSystem-backed engine works
        // regardless of substrate.
        match self.block_on(self.op.rename(s, d)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == OdErrorKind::Unsupported => {
                // copy() is also Unsupported on some services (notably
                // services-memory). Emulate via read + write + delete
                // as a last resort. PUT is atomic per object, so the
                // destination either appears whole or not at all; the
                // source delete that follows might leave behind a stale
                // copy if it fails, but that is the same risk profile
                // as OpenDAL's own copy+delete fallback.
                let copy_res = self.block_on(self.op.copy(s, d));
                match copy_res {
                    Ok(()) => {}
                    Err(ce) if ce.kind() == OdErrorKind::Unsupported => {
                        let buf = self.block_on(self.op.read(s)).map_err(|re| {
                            map_opendal_err(re, &format!("rename fallback read: {s}"))
                        })?;
                        self.block_on(self.op.write(d, buf)).map_err(|we| {
                            map_opendal_err(we, &format!("rename fallback write: {d}"))
                        })?;
                    }
                    Err(ce) => {
                        return Err(map_opendal_err(
                            ce,
                            &format!("rename copy fallback: {s} -> {d}"),
                        ))
                    }
                }
                self.block_on(self.op.delete(s))
                    .map_err(|de| map_opendal_err(de, &format!("rename delete src: {s}")))?;
                Ok(())
            }
            Err(e) => Err(map_opendal_err(e, &format!("rename: {s} -> {d}"))),
        }
    }

    fn name(&self) -> &str {
        // The cached name was materialized at construction from
        // `op.info().scheme()` (which returns a `Scheme` enum, not `&str`).
        &self.name
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

        let err = match fs.open_writable_file(path, WriteMode::CreateNew) {
            Err(e) => e,
            Ok(_) => panic!("CreateNew on existing must fail"),
        };
        assert!(err.is_invalid_argument(), "got {err}");
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

            let mut r = fs.open_sequential_file(path).expect("open sequential");
            let mut buf = vec![0u8; payload.len()];
            let n = r.read(&mut buf).expect("read");
            assert_eq!(n, payload.len());
            assert_eq!(&buf, payload, "retry layer must be transparent on happy path");
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

    // --- rename fallback on services without native rename ------------------
    //
    // The in-memory service rejects `rename` with `Unsupported`. We added a
    // copy+delete fallback so the engine's atomic-temp-file flush path keeps
    // working on these substrates. This test pins that contract: rename must
    // succeed end-to-end on memory backend, and the destination must contain
    // the source bytes while the source disappears.
    #[test]
    fn test_opendal_rename_fallback_on_unsupported() {
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
        assert!(fs.file_exists(src).unwrap());
        assert!(!fs.file_exists(dst).unwrap());

        fs.rename(src, dst)
            .expect("rename via copy+delete fallback");

        assert!(
            !fs.file_exists(src).unwrap(),
            "source must be gone after rename"
        );
        assert!(
            fs.file_exists(dst).unwrap(),
            "destination must exist after rename"
        );

        let mut r = fs.open_sequential_file(dst).unwrap();
        let mut buf = vec![0u8; payload.len() + 8];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], payload, "rename must preserve content");
    }

    // --- PR-D1: bytes::Bytes round-trip on the storage layer ---------------
    //
    // Writes a 1 MiB payload, then reads it back via both the sequential
    // and random-access readers. Both now serve from a `bytes::Bytes`
    // refcounted buffer instead of allocating an intermediate `Vec<u8>`
    // per read. The test asserts data equality across multiple read modes
    // and offsets, which exercises:
    //   - `OpendalSequentialFile::bytes: Bytes` (zero-copy from
    //     `Buffer::to_bytes()`),
    //   - `OpendalRandomAccessFile::read_at` using `Buf::copy_to_slice`
    //     (streaming copy, no `Vec` intermediate),
    //   - `OpendalWritableFile::persist` using
    //     `mem::take(&mut self.buffer).into()` for zero-copy `Vec → Bytes`.
    //
    // The intent is to pin the no-Vec-intermediate invariant; a regression
    // that re-introduces `to_vec()` on the read path would not fail this
    // test directly but would show up in the `s3_read_64MB` criterion
    // benchmark referenced by the PR-D1 acceptance criterion.
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

        // Write via OpendalWritableFile (exercises persist's mem::take →
        // Bytes path).
        {
            let mut w = fs
                .open_writable_file(path, WriteMode::CreateOrTruncate)
                .expect("open writable");
            w.append(&payload).expect("append");
            w.sync().expect("sync");
        }

        // Sequential read covers the full object; serves from
        // OpendalSequentialFile's `Bytes` field.
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
}

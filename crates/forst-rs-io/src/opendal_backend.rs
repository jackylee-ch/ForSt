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

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use std::time::Duration;

use bytes::Buf;
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
// WritableFile — streaming OpenDAL writer
// ---------------------------------------------------------------------------

/// A writable file that streams bytes to OpenDAL's blocking writer.
pub struct OpendalWritableFile {
    path: String,
    writer: Option<opendal::BlockingWriter>,
    bytes_written: u64,
    closed: bool,
}

impl WritableFile for OpendalWritableFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        if self.closed {
            return Err(ForstError::invalid_argument(format!(
                "OpenDAL append after close: {}",
                self.path
            )));
        }
        let writer = self.writer.as_mut().ok_or_else(|| {
            ForstError::invalid_argument(format!("OpenDAL writer missing: {}", self.path))
        })?;
        writer
            .write(data.to_vec())
            .map_err(|e| map_opendal_err(e, &format!("OpenDAL streaming write: {}", self.path)))?;
        self.bytes_written = self.bytes_written.saturating_add(data.len() as u64);
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
    fn close_writer(&mut self) -> ForstResult<()> {
        if self.closed {
            return Ok(());
        }
        if let Some(mut writer) = self.writer.take() {
            writer
                .close()
                .map_err(|e| map_opendal_err(e, &format!("OpenDAL close writer: {}", self.path)))?;
        }
        self.closed = true;
        Ok(())
    }
}

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
        let writer = blocking
            .writer_with(p)
            .append(append)
            .call()
            .map_err(|e| map_opendal_err(e, &format!("open_writable_file writer: {p}")))?;

        Ok(Box::new(OpendalWritableFile {
            path: p.to_string(),
            writer: Some(writer),
            bytes_written: initial_size,
            closed: false,
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
}

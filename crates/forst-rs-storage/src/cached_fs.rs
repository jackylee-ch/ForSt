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

//! `CachedFileSystem` — a [`FileSystem`] decorator that fronts a remote
//! backend with a local-disk LRU cache ([`crate::local_cache::LocalCache`]).
//!
//! # When to use
//!
//! Build a [`CachedFileSystem`] when state is hosted on object storage
//! (S3, GCS, Azure) and you want to avoid one round trip per SST read.
//! The cache lives under `cache_dir` on the local filesystem with a
//! caller-supplied byte budget; SSTs that fit the budget are served
//! locally, the rest fall through to the remote backend on every read.
//!
//! # Cached vs. pass-through operations
//!
//! - **Cached**: `open_sequential_file`, `open_random_access_file`. These
//!   are the hot read paths the engine drives during compaction and
//!   point lookups. On a cache miss we fetch the entire file from the
//!   remote backend, persist it under `cache_dir`, and serve from the
//!   local copy. On a hit we never touch the remote backend.
//! - **Pass-through**: every write operation (`open_writable_file`,
//!   `rename`, `delete_file`, `delete_dir`, `create_dir_all`). Writes
//!   land directly on the remote backend; durability semantics are
//!   identical to running on the raw remote FS. We could write-through
//!   the cache too, but doing so introduces a consistency window with
//!   no upside on the SST flush path (an SST is only ever read after
//!   it has been atomically renamed in place, by which point the cache
//!   layer will fetch it on the first reader).
//! - **Invalidation**: `delete_file` and `rename` invalidate the cache
//!   entry for the affected path so stale bytes never resurface.
//!
//! # Cache key
//!
//! The logical path string (e.g. `/db/00000123.sst`) is used as the
//! cache key. Two distinct OpenDAL roots that map the same logical
//! path MUST NOT share a `cache_dir` — there is no per-FS namespace
//! prefix today. (Operators who multi-tenant a single cache should
//! provide one `cache_dir` per database.)

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use forst_rs_common::error::{ForstError, ForstResult};
use forst_rs_io::{
    FileMetadata, FileSystem, LocalFileSystem, RandomAccessFile, SequentialFile, WritableFile,
    WriteMode,
};

use crate::local_cache::LocalCache;

/// A [`FileSystem`] decorator that interposes a local LRU file cache on
/// top of a remote backend.
pub struct CachedFileSystem {
    remote: Arc<dyn FileSystem>,
    cache: Arc<LocalCache>,
    name: String,
}

impl std::fmt::Debug for CachedFileSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedFileSystem")
            .field("remote", &self.remote.name())
            .field("cache_dir", &self.cache.cache_dir())
            .field("capacity_bytes", &self.cache.capacity_bytes())
            .finish()
    }
}

impl CachedFileSystem {
    /// Constructs a new `CachedFileSystem` from a remote backend and a
    /// pre-opened local cache.
    pub fn new(remote: Arc<dyn FileSystem>, cache: Arc<LocalCache>) -> Self {
        let name = format!("Cached({})", remote.name());
        Self {
            remote,
            cache,
            name,
        }
    }

    /// Returns the local cache.
    /// 2026-05-30 WRITE-BACK READ RACE: an SST written via the write-back path
    /// is uploaded to the remote (S3) asynchronously. A read that reaches the
    /// remote before the upload finishes (or while it is registering) sees a
    /// 404. Stat the remote; on miss for an SST, await the in-flight upload (a
    /// cheap no-op when nothing is pending) and retry once so the read observes
    /// the now-durable object instead of failing with NotFound.
    fn remote_size_awaiting_upload(&self, path: &Path) -> Option<u64> {
        match self.remote.get_file_metadata(path) {
            Ok(m) => Some(m.size),
            Err(_) => {
                let _ = self.remote.await_upload(path);
                self.remote.get_file_metadata(path).ok().map(|m| m.size)
            }
        }
    }

    pub fn cache(&self) -> &Arc<LocalCache> {
        &self.cache
    }

    /// FRS-LOCAL-FIRST-SST (2026-06-01): open an SST reader that serves from the
    /// local write-through copy (populated synchronously on flush/compaction
    /// `sync()` by [`CachePopulatingWritableFile`]) WITHOUT blocking on the S3
    /// upload. Returns `None` when no local copy exists (restore on a fresh
    /// instance, or post-eviction) so the caller falls back to the remote path.
    ///
    /// This is the structural fix for the heavy-join FREEZE: a 2 GiB flushed SST
    /// takes ~114 s to upload at the ~18 MB/s object-store uplink, and
    /// `get_or_open_sst_reader → await_upload` blocked every probe of that SST's
    /// key range for the whole upload. The bytes are already on local NVMe the
    /// instant the SST is visible, so reads should never wait on the upload.
    fn open_local_first_sst(&self, path: &Path) -> Option<Box<dyn RandomAccessFile>> {
        let key = self.cache_key(path).ok()?.to_string();
        let size = self.cache.entry_size(&key)?;
        Some(Box::new(LocalFirstSstFile {
            cache: Arc::clone(&self.cache),
            key,
            file_size: size,
            remote: Arc::clone(&self.remote),
            path: path.to_path_buf(),
            remote_fallback: std::sync::Mutex::new(None),
        }))
    }

    /// Returns the remote backend.
    pub fn remote(&self) -> &Arc<dyn FileSystem> {
        &self.remote
    }

    /// Ensures the file at `path` is present in the local cache.
    ///
    /// On a cache hit this is a no-op (just a hash-map lookup). On a miss
    /// the entire file is fetched from the remote backend in one GetObject
    /// call and persisted to the local cache directory.
    ///
    /// # Use case — S3 vector I/O prefetch
    ///
    /// Call `ensure_cached` for each SST file that a batch of lookups will
    /// touch *before* opening the readers. This amortizes S3 RTT: one
    /// large GetObject per SST instead of many small range reads, and
    /// allows the caller to issue multiple prefetches in parallel (e.g.
    /// via `rayon` or `tokio::spawn`).
    ///
    /// ```text
    /// // Prefetch all SSTs a batch_get will touch:
    /// for path in sst_paths {
    ///     cached_fs.ensure_cached(&path)?;
    /// }
    /// // Now open_random_access_file is a local-only operation.
    /// ```
    pub fn ensure_cached(&self, path: &Path) -> ForstResult<()> {
        let key = self.cache_key(path)?;
        if self.cache.contains(key) {
            return Ok(());
        }
        // Miss: fetch the entire file and populate the cache.
        self.fetch_through_cache(path)?;
        Ok(())
    }

    /// Batch-prefetch multiple files into the local cache.
    ///
    /// Iterates `paths` and calls [`ensure_cached`](Self::ensure_cached) for
    /// each. Files already in the cache are skipped (cheap hash-map check).
    /// Returns the number of cache misses that triggered a remote fetch.
    ///
    /// For maximum throughput on S3, callers should parallelize across
    /// paths at a higher layer (e.g. `rayon::par_iter` or async tasks).
    /// This sequential helper is provided for convenience when the caller
    /// does not have a parallel runtime available.
    pub fn prefetch_files(&self, paths: &[&Path]) -> ForstResult<usize> {
        let mut misses = 0usize;
        for path in paths {
            let key = self.cache_key(path)?;
            if !self.cache.contains(key) {
                self.fetch_through_cache(path)?;
                misses += 1;
            }
        }
        Ok(misses)
    }

    /// FRS-S3-READ-CONCURRENCY (2026-05-31): warm the cache for `paths`,
    /// fetching cache MISSES CONCURRENTLY instead of serially.
    ///
    /// A symbolized `sample` of q9's join stall showed the operator thread
    /// spending ~58% of `batch_get` inside
    /// `prefetch_sst_files_for_batch → fetch_through_cache → opendal read_at →
    /// tokio park → pthread_cond_wait` — i.e. BLOCKED on serial S3 round-trips,
    /// one whole-SST download at a time. Each miss is an independent S3 GET, so
    /// K misses cost K×RTT serially. Fanning the blocking `fetch_through_cache`
    /// calls out across scoped threads collapses that to ≈1×RTT per wave
    /// (opendal's blocking reader drives the shared tokio runtime, so N
    /// concurrent blocking reads become N concurrent S3 requests).
    ///
    /// Best-effort: per-file errors are swallowed (the real read path retries
    /// and surfaces them). `fetch_through_cache` re-checks the cache on entry,
    /// so this is idempotent and races harmlessly with the on-demand read path.
    /// Reuses `fetch_through_cache` UNCHANGED — the short-read/size-verify and
    /// `await_upload` correctness logic is preserved verbatim.
    pub fn prefetch_files_concurrent(&self, paths: &[&Path]) {
        // Pre-filter to misses so the warm-cache common case spawns no threads.
        let misses: Vec<&Path> = paths
            .iter()
            .copied()
            .filter(|p| match self.cache_key(p) {
                Ok(k) => !self.cache.contains(k),
                Err(_) => false,
            })
            .collect();
        if misses.is_empty() {
            return;
        }
        if misses.len() == 1 {
            let _ = self.fetch_through_cache(misses[0]);
            return;
        }
        // Bound in-flight S3 requests (and spawned threads) per wave. Mirrors
        // the write-back upload concurrency cap.
        const MAX_CONCURRENT_FETCH: usize = 8;
        for wave in misses.chunks(MAX_CONCURRENT_FETCH) {
            std::thread::scope(|s| {
                for &path in wave {
                    s.spawn(move || {
                        let _ = self.fetch_through_cache(path);
                    });
                }
            });
        }
    }

    /// Resolves a `&Path` to its cache-key string (UTF-8). The same key
    /// shape is used on remote reads, local writes, and invalidations.
    fn cache_key<'a>(&self, path: &'a Path) -> ForstResult<&'a str> {
        path.to_str().ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "CachedFileSystem: path is not valid UTF-8: {}",
                path.display()
            ))
        })
    }

    /// Fetches the full object bytes, populating the cache on success.
    /// Returns the bytes either from the cache (on hit) or the remote
    /// backend (on miss).
    ///
    /// The returned [`Bytes`] is ref-counted and slice-able — callers can
    /// hand it to multiple readers without recopying. The conversion from
    /// `Vec<u8>` (returned by [`LocalCache::get`]) to `Bytes` is zero-copy.
    fn fetch_through_cache(&self, path: &Path) -> ForstResult<Bytes> {
        let key = self.cache_key(path)?.to_string();
        if let Some(bytes) = self
            .cache
            .get(&key)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("cache get: {e}"))))?
        {
            // `Bytes::from(Vec<u8>)` is zero-copy in `bytes` 1.x — the Vec's
            // allocation is transferred into a refcounted Bytes handle.
            return Ok(Bytes::from(bytes));
        }
        // Miss: pull the whole file from the remote backend.
        //
        // FRS-S3-SHORTREAD-FIX: read via size-bounded RANGED reads, not the
        // streaming sequential reader. OpenDAL's blocking `StdReader`
        // (`into_std_read`) can signal a PREMATURE EOF on large multipart
        // objects — the SST's S3 `content_length` is correct but the
        // streaming read returns `Ok(0)` a few KB before the real end,
        // silently truncating the SST. This corrupted q4/q7-style large
        // join-state values (captured on S3:
        //   "short read: expected 13858858 bytes, got 13847535").
        // q3 never hit it because its SSTs are below the 8 MiB multipart
        // chunk size. Individual ranged GETs are bounded and complete, so
        // looping to the metadata-reported size reads the whole object.
        // Await any in-flight write-back upload before sizing/reading (see
        // `remote_size_awaiting_upload`): the sequential / whole-file path is
        // used by checkpoint staging, which must not race the async upload.
        let cap_hint = self
            .remote_size_awaiting_upload(path)
            .map(|s| s as usize)
            .unwrap_or(0);
        let mut bytes;
        if cap_hint > 0 {
            let raf = self.remote.open_random_access_file(path)?;
            bytes = vec![0u8; cap_hint];
            let mut off = 0usize;
            while off < cap_hint {
                let n = raf.read_at(off as u64, &mut bytes[off..])?;
                if n == 0 {
                    // Genuine EOF before the reported size — caught by the
                    // length check below and surfaced as corruption rather
                    // than admitting a truncated entry into the cache.
                    break;
                }
                off += n;
            }
            bytes.truncate(off);
        } else {
            // Metadata unavailable — cannot size-bound; fall back to the
            // streaming sequential reader (best-effort, small-object path).
            let mut reader = self.remote.open_sequential_file(path)?;
            bytes = Vec::new();
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                let n = reader.read(&mut chunk)?;
                if n == 0 {
                    break;
                }
                bytes.extend_from_slice(&chunk[..n]);
            }
        }
        // R75-M2: a short stream (truncation mid-transfer, OpenDAL ranged-
        // sequential bug) would otherwise admit a partial file into the
        // on-disk cache, and the poisoned cache entry would persist across
        // process restarts — infecting every reader until eviction. Compare
        // against the metadata-reported `cap_hint` and refuse to admit any
        // entry whose length disagrees. `cap_hint == 0` means metadata was
        // unavailable; we cannot validate and admit the streamed bytes as-is
        // (consistent with the prior best-effort behavior on that branch).
        if cap_hint > 0 && bytes.len() != cap_hint {
            return Err(ForstError::corruption(format!(
                "CachedFileSystem fetch_through_cache short read: expected {} bytes, got {} for {}",
                cap_hint,
                bytes.len(),
                path.display()
            )));
        }
        // Best-effort cache write; failures here just mean the next read
        // pays the same miss, so we swallow the error (the streamed
        // bytes are still good) rather than failing an otherwise-good
        // SST read. Pre-R78-M1 the `?` at the end of the chain
        // propagated the cache-put error, contradicting the comment
        // and turning a transient cache-disk hiccup (ENOSPC, EACCES on
        // cache_dir) into a hard read failure. `forst-rs-storage` does
        // not pull in `tracing` so the diagnostic is via `eprintln!`
        // to stderr — the engine layer above relays anything important.
        if let Err(e) = self.cache.put(&key, &bytes) {
            eprintln!(
                "CachedFileSystem cache put for {} failed: {} \
                 (continuing with the streamed bytes; next read will retry)",
                path.display(),
                e
            );
        }
        // Zero-copy hand-off into the refcounted Bytes container. Callers
        // can slice / share without recopying the full payload.
        Ok(Bytes::from(bytes))
    }
}

impl FileSystem for CachedFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let bytes = self.fetch_through_cache(path)?;
        Ok(Box::new(InMemorySequential::new(bytes)))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        // FRS-BLOCKCACHE (2026-05-27, Option ②): do NOT download the whole file.
        //
        // The prior path `fetch_through_cache` pulled the ENTIRE 64 MiB SST from
        // S3 on first touch, even when a reader only needs the ~KB footer+index
        // plus a few data blocks. Under ckpt-ON, every 30 s checkpoint force-
        // flushes the memtable → a new L0 SST; the heavy-join probe (q7/q9/q16/q20
        // `MapState.asyncEntries()`) then opens O(num_L0) overlapping SSTs and paid
        // a full 64 MiB download per SST → q7 collapsed to ~410 rec/s (vs 208 K
        // rec/s ckpt-OFF). Confirmed by the 300 s-interval experiment (410 → 54 K
        // rec/s) — see memory project_q7_ckpton_rootcause_2026-05-27.
        //
        // RocksDB-parity fix: serve random reads from 1 MiB chunks fetched lazily
        // via S3 RANGE reads and cached per-chunk in the SAME on-disk LRU
        // (cache key `{path}#c{idx}`). A join probe that touches a few blocks now
        // transfers a few MiB, not 64. SSTs are write-once and file-numbers are
        // unique per instance (the collision bug is already fixed), so a
        // `(path, chunk_idx)` key maps to immutable bytes for the life of the file
        // → NO chunk invalidation is required on delete/rename (stale chunks are
        // impossible; deleted-SST chunks simply age out of the LRU). `delete_file`
        // still invalidates the legacy whole-file key for the sequential path.
        //
        // CORRECTNESS GUARD: the chunk cache is sound ONLY for write-once files.
        // SSTs qualify; MANIFEST/CURRENT can be rewritten in place, where a cached
        // chunk at a fixed offset could go stale. Restrict the chunk path to
        // `*.sst` and fall back to the whole-file fetch for everything else (those
        // files are small, so the whole-file path is cheap anyway).
        let is_sst = path
            .extension()
            .map(|e| e.eq_ignore_ascii_case("sst"))
            .unwrap_or(false);
        // FRS-LOCAL-FIRST-SST (2026-06-01): if a local write-through copy of this
        // SST exists, serve reads from it directly — NO `await_upload`, NO S3 GET.
        // This decouples the read/checkpoint hot path from the slow object-store
        // upload (the proven cause of the heavy-join 114 s freeze). Falls through
        // to the remote range path when there is no local copy (restore / evicted).
        if is_sst {
            if let Some(local) = self.open_local_first_sst(path) {
                return Ok(local);
            }
        }
        // The chunk reader needs an authoritative file size (to clamp reads and
        // size the final chunk). If metadata is unavailable (e.g. an in-memory
        // backend in tests, or a backend that does not report size), fall back to
        // the whole-file path — same tolerant behavior `fetch_through_cache` uses.
        let size = if is_sst {
            self.remote_size_awaiting_upload(path)
        } else {
            None
        };
        let Some(file_size) = size else {
            let bytes = self.fetch_through_cache(path)?;
            return Ok(Box::new(InMemoryRandom::new(bytes)));
        };
        let key = self.cache_key(path)?.to_string();
        let remote = self.remote.open_random_access_file(path)?;
        Ok(Box::new(RangeCachedRandomAccessFile {
            remote,
            cache: Arc::clone(&self.cache),
            path_key: key,
            file_size,
        }))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        // FRS-S3-WRITETHROUGH REVERTED (2026-05-27): write-through SST caching
        // populated the on-disk LRU with EVERY freshly-flushed SST on the hot
        // flush path. For heavy joins (q7/q9) the flush write-volume is many GB,
        // so write-through churned the 64 GiB LRU with write-side SSTs and EVICTED
        // the join's hot READ working set → join-state reads missed → S3 round-trip
        // (~20 ms) → throughput collapsed to ~210-500 ev/s (q7 stalled at ~2M/100M
        // records). The +36% it gave q8 (a light query with a small working set) is
        // dwarfed by the catastrophic heavy-join regression, and heavy joins
        // dominate the total. The READ path (fetch_through_cache, ~line 130) already
        // populates the cache on miss — exactly the fast pre-write-through (v6f)
        // behavior — so reverting starves nothing. Mutable files still pre-
        // invalidate so the next reader never sees ghost bytes.
        if let Ok(key) = self.cache_key(path) {
            // SSTs are write-once/new (no stale entry to invalidate) but calling
            // invalidate is harmless and keeps a single uniform path.
            let _ = self.cache.invalidate(key);
        }
        let inner = self.remote.open_writable_file(path, mode)?;
        // FRS-S3-WRITETHROUGH-V2 (2026-05-31): write-through ONLY SST files into
        // the on-disk cache so heavy-join reads of just-flushed state hit local
        // disk (~100 µs) instead of S3 (~20 ms). This targets the q9/q7 ckpt-ON
        // cold-read collapse: state spilled past the 1 GiB RAM resident shadow
        // lives only on S3, so every probe of it pays an S3 round-trip.
        //
        // The 2026-05-27 write-through revert churned a 64 GiB LRU (flush volume
        // evicted the hot READ working set). V2 differs: (a) gated to `.sst`
        // files (WAL/MANIFEST never pollute the read cache), and (b) paired with
        // a much larger on-disk cache (config `cache-capacity-mb`, raised to fit
        // the full flush volume + hot read set) so there is no eviction of hot
        // reads. Validated by A/B on q9; revert both if it regresses.
        let is_sst = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.eq_ignore_ascii_case("sst"))
            .unwrap_or(false);
        if is_sst {
            if let Ok(key) = self.cache_key(path) {
                return Ok(Box::new(CachePopulatingWritableFile {
                    inner,
                    buf: Vec::new(),
                    key: key.to_string(),
                    cache: self.cache.clone(),
                    populated: false,
                }));
            }
        }
        Ok(inner)
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        // Existence is a cheap remote operation and the cache may have a
        // file for a path the remote no longer has (after a parallel
        // delete). Consult the remote backend directly so existence
        // checks are never stale.
        self.remote.file_exists(path)
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        self.remote.get_file_metadata(path)
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        self.remote.list_dir(dir)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        self.remote.create_dir_all(dir)
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        if let Ok(key) = self.cache_key(path) {
            let _ = self.cache.invalidate(key);
        }
        self.remote.delete_file(path)
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        // Conservative: drop any cached entry inside `path` by full-scan
        // on delete. Recursive deletes of large prefixes are rare in the
        // engine; iterating the in-memory map is acceptable cost.
        // Note: for the non-recursive path the remote backend rejects
        // delete on a non-empty dir anyway, so a no-op invalidation is
        // fine. We do not currently track per-prefix cache contents, so
        // recursive deletes leave the cache slightly stale; the on-disk
        // cache files remain (taking space) until evicted by LRU. This
        // is safe (reads will see NotFound on the remote and the cache
        // entries will be naturally evicted) and matches the contract
        // documented in this module's top-level doc.
        let _ = recursive;
        self.remote.delete_dir(path, recursive)
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        if let Ok(key) = self.cache_key(src) {
            let _ = self.cache.invalidate(key);
        }
        if let Ok(key) = self.cache_key(dst) {
            let _ = self.cache.invalidate(key);
        }
        self.remote.rename(src, dst)
    }

    /// FRS-S3-SSTRENAME: forward the rename-capability query to the backing
    /// remote FS. Without this override the wrapper inherited the trait
    /// default `true`, so the SST flush/compaction paths kept staging to
    /// `.tmp` + `rename()` even when `self.remote` was an object store with
    /// no atomic rename — delegating straight into the `Unsupported` failure.
    /// Forwarding lets those paths see `false` and stream the SST to its
    /// final key instead.
    fn supports_atomic_rename(&self) -> bool {
        self.remote.supports_atomic_rename()
    }

    /// 2026-05-29 WRITE-BACK FLUSH: delegate the upload barriers to the backing
    /// remote FS. When the remote uploads SST/MANIFEST writes asynchronously
    /// (object-store write-back), these block until the named / all in-flight
    /// uploads have completed, so a direct SST read (or the checkpoint barrier)
    /// sees a fully-uploaded object.
    fn await_upload(&self, path: &Path) -> ForstResult<()> {
        self.remote.await_upload(path)
    }

    fn await_all_uploads(&self) -> ForstResult<()> {
        self.remote.await_all_uploads()
    }

    /// R50-H1: fsync both the remote-side directory entry AND the local
    /// cache directory. The remote leg is delegated to the backing FS
    /// (object stores no-op, a wrapped POSIX backend honours it). The
    /// cache leg fsyncs the on-disk cache dir so any cache-file create /
    /// rename / unlink performed by [`crate::local_cache::LocalCache`]
    /// (e.g. miss-fetch landing a new SST blob) is durable on a
    /// power-loss event.
    ///
    /// Falling back to the trait default no-op (pre-fix behaviour) made
    /// the engine's R49-H3 `sync_dir(parent)` silently a no-op when a
    /// `CachedFileSystem` sat in front of `LocalFileSystem` — a regression
    /// of the rename-durability contract that motivated R49-H3.
    ///
    /// We instantiate a fresh [`LocalFileSystem`] for the cache-side
    /// fsync. `LocalFileSystem` is stateless (a unit struct) so
    /// construction is free.
    fn sync_dir(&self, dir: &Path) -> ForstResult<()> {
        self.remote.sync_dir(dir)?;
        let cache_dir = self.cache.cache_dir();
        let local = LocalFileSystem::new();
        local.sync_dir(cache_dir)?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn ensure_cached(&self, path: &Path) -> ForstResult<()> {
        let key = self.cache_key(path)?;
        if self.cache.contains(key) {
            return Ok(());
        }
        self.fetch_through_cache(path)?;
        Ok(())
    }

    fn prefetch_concurrent(&self, paths: &[&Path]) {
        // Concurrent fan-out of the per-file fetch — see the inherent method.
        self.prefetch_files_concurrent(paths);
    }
}

// ---------------------------------------------------------------------------
// Write-through cache population for immutable SST files
//
// FRS-S3-WRITETHROUGH: wraps the remote backend's `WritableFile` for an SST
// (write-once, immutable). It accumulates every appended byte in `buf` and, on
// the first successful `sync()` (the point at which the buffered S3 writer
// commits the object via CompleteMultipartUpload — the SST is now durable and
// its bytes are final), it write-throughs the full payload into the
// `LocalCache`. The next reader of this SST hits the local disk (~100 µs)
// instead of paying an S3 GetObject (~20 ms).
//
// `buf` transiently holds a full SST copy (≤ ~64 MiB) during the write — an
// acceptable, short-lived cost paid once per flushed SST. Cache-put failures
// are best-effort (swallowed) and never fail the write: the SST is already
// durable on S3, and a missing cache entry only costs the next reader one S3
// fetch (which re-populates the cache via `fetch_through_cache`).
// ---------------------------------------------------------------------------

struct CachePopulatingWritableFile {
    inner: Box<dyn WritableFile>,
    /// Accumulated copy of every byte appended, used to populate the cache.
    buf: Vec<u8>,
    /// Owned cache key (logical path string) for the populated entry.
    key: String,
    /// Shared handle to the on-disk LRU cache (`put` takes `&self`).
    cache: Arc<LocalCache>,
    /// Guard so we populate the cache exactly once.
    populated: bool,
}

/// FRS-LOCAL-FIRST-SST (2026-06-01): a `RandomAccessFile` that serves an SST's
/// bytes from the local write-through cache copy via positional `get_range`
/// reads, falling back to the remote backend only if the local copy is evicted.
///
/// Sound because SSTs are write-once: a `(key, offset)` maps to immutable bytes.
/// During the SST's S3-upload window the local copy is guaranteed present (the
/// flush `sync()` populates it synchronously before the SST is version-visible),
/// so reads in that window never touch S3 — eliminating the upload-await freeze.
/// After the upload completes the entry may eventually be LRU-evicted; a read
/// then lazily opens the remote (whose object is long-since durable, so
/// `await_upload` returns immediately) and delegates.
struct LocalFirstSstFile {
    cache: Arc<LocalCache>,
    key: String,
    file_size: u64,
    remote: Arc<dyn FileSystem>,
    path: PathBuf,
    /// Lazily-opened remote reader, used only on a local cache miss (eviction).
    remote_fallback: std::sync::Mutex<Option<Box<dyn RandomAccessFile>>>,
}

impl RandomAccessFile for LocalFirstSstFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Fast path: positional read from the local write-through copy.
        match self.cache.get_range(&self.key, offset, buf.len()) {
            Ok(Some(bytes)) => {
                // `bytes` may be shorter than `buf` only at EOF (write-once file)
                // — mirror the remote reader's short-read-at-EOF semantics.
                let n = bytes.len().min(buf.len());
                buf[..n].copy_from_slice(&bytes[..n]);
                return Ok(n);
            }
            // `None` = entry evicted; fall back to the remote backend below.
            Ok(None) => {}
            Err(e) => {
                return Err(ForstError::internal(format!(
                    "local-first SST read {}: {e}",
                    self.path.display()
                )))
            }
        }
        // Slow path (post-eviction): lazily open the remote reader once. The
        // upload is long complete by the time an entry ages out of the LRU, so
        // `await_upload` here is a cheap no-op; we call it for correctness on the
        // off chance the entry was never uploaded yet (defensive).
        let mut guard = self
            .remote_fallback
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        if guard.is_none() {
            let _ = self.remote.await_upload(&self.path);
            *guard = Some(self.remote.open_random_access_file(&self.path)?);
        }
        guard
            .as_ref()
            .expect("remote fallback initialized above")
            .read_at(offset, buf)
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.file_size)
    }
}

impl CachePopulatingWritableFile {
    /// Best-effort write-through of the accumulated bytes into the cache.
    /// Runs at most once (guarded by `populated`); cache-put failures are
    /// swallowed so they can never fail an otherwise-durable SST write.
    ///
    /// FRS-S3-WRITETHROUGH-V2 (2026-05-31): SYNCHRONOUS on purpose. An async
    /// (detached-thread) variant was tried and REGRESSED q9 (6.7M vs 13M @583s,
    /// with 60-120 s zero-rate stalls): making the populate async lets an SST
    /// become version-visible / readable BEFORE its bytes land in the local
    /// cache, so the first probes of just-flushed state miss the cache and pay
    /// the S3 round-trip anyway — defeating the whole point — plus the detached
    /// writes pile up under heavy flush volume. Synchronous put guarantees the
    /// local copy exists the instant the SST is durable+visible, so every
    /// subsequent read hits local disk. The added local-NVMe write (~tens of ms)
    /// on the flush worker is far cheaper than the S3 reads it eliminates.
    fn populate_cache(&mut self) {
        if self.populated {
            return;
        }
        // Cache-put failure is non-fatal: the SST is already durable on the
        // remote backend, and the next reader simply re-fetches it.
        let _ = self.cache.put(&self.key, &self.buf);
        self.populated = true;
    }
}

impl WritableFile for CachePopulatingWritableFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        self.inner.append(data)?;
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        self.inner.flush()
    }

    fn sync(&mut self) -> ForstResult<()> {
        // Durably commit to the remote backend FIRST. Only after the object is
        // committed (and its bytes final) do we admit them to the cache, so a
        // failed sync never leaves a cache entry for a non-durable SST.
        self.inner.sync()?;
        self.populate_cache();
        Ok(())
    }

    fn file_size(&self) -> ForstResult<u64> {
        self.inner.file_size()
    }
}

// ---------------------------------------------------------------------------
// In-memory file adapters
//
// Both adapters serve cached bytes from a `bytes::Bytes` handle. `Bytes`
// is ref-counted and slice-able without memcpy — the engine's SST reader
// can hold any number of overlapping views without recopying the payload.
// These adapters keep the refcounted buffer alive for the lifetime of the
// reader handle.
// ---------------------------------------------------------------------------

struct InMemorySequential {
    bytes: Bytes,
    pos: usize,
}

impl InMemorySequential {
    fn new(bytes: Bytes) -> Self {
        Self { bytes, pos: 0 }
    }
}

impl SequentialFile for InMemorySequential {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        let remaining = self.bytes.len().saturating_sub(self.pos);
        let n = remaining.min(buf.len());
        // `Bytes` deref-coerces to `&[u8]`; a single memcpy from the
        // ref-counted buffer to the caller's slice.
        buf[..n].copy_from_slice(&self.bytes[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        let n_usize = usize::try_from(n)
            .map_err(|_| ForstError::invalid_argument(format!("skip {n} exceeds usize::MAX")))?;
        self.pos = self.pos.saturating_add(n_usize).min(self.bytes.len());
        Ok(())
    }
}

struct InMemoryRandom {
    bytes: Bytes,
}

impl InMemoryRandom {
    fn new(bytes: Bytes) -> Self {
        Self { bytes }
    }
}

impl RandomAccessFile for InMemoryRandom {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let offset = usize::try_from(offset).map_err(|_| {
            ForstError::invalid_argument(format!("offset {offset} exceeds usize::MAX"))
        })?;
        if offset >= self.bytes.len() {
            return Ok(0);
        }
        let end = (offset + buf.len()).min(self.bytes.len());
        let n = end - offset;
        buf[..n].copy_from_slice(&self.bytes[offset..end]);
        Ok(n)
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.bytes.len() as u64)
    }
}

/// FRS-BLOCKCACHE: a [`RandomAccessFile`] that serves reads from fixed-size
/// chunks fetched lazily via the remote backend's RANGE reads and cached
/// per-chunk in the on-disk LRU. Avoids the whole-file download that the
/// legacy `fetch_through_cache` path paid on first touch.
///
/// Correctness: SST files are write-once and their paths (file numbers) are
/// unique per instance, so `{path}#c{idx}` → immutable bytes. The chunk cache
/// therefore needs no invalidation. The last chunk is shorter than
/// `CHUNK_SIZE` (clamped to `file_size`); all other chunks are exactly
/// `CHUNK_SIZE` and a short remote read for them is surfaced as corruption
/// (mirrors `fetch_through_cache`'s short-read guard).
struct RangeCachedRandomAccessFile {
    remote: Box<dyn RandomAccessFile>,
    cache: Arc<LocalCache>,
    path_key: String,
    file_size: u64,
}

/// 1 MiB chunk granularity: large enough to amortize the S3 per-request
/// latency and give scan locality (a sorted prefix-scan's adjacent blocks
/// share a chunk), small enough that a few-block read does not drag in the
/// whole 64 MiB SST.
const BLOCK_CACHE_CHUNK_SIZE: u64 = 1 << 20;

impl RangeCachedRandomAccessFile {
    /// Returns the bytes of chunk `chunk_idx` (`[chunk_start, chunk_end)`),
    /// from the on-disk cache on a hit or via a bounded ranged remote read on
    /// a miss (populating the cache). `chunk_end` is clamped to `file_size`.
    fn chunk_bytes(&self, chunk_idx: u64) -> ForstResult<Vec<u8>> {
        let chunk_start = chunk_idx * BLOCK_CACHE_CHUNK_SIZE;
        let chunk_end = (chunk_start + BLOCK_CACHE_CHUNK_SIZE).min(self.file_size);
        let chunk_len = (chunk_end - chunk_start) as usize;
        let key = format!("{}#c{}", self.path_key, chunk_idx);
        // Hit: serve from the local LRU (NVMe read).
        if let Some(bytes) = self
            .cache
            .get(&key)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("block cache get: {e}"))))?
        {
            if bytes.len() == chunk_len {
                return Ok(bytes);
            }
            // A cached chunk of the wrong length means a poisoned entry
            // (e.g. a truncated admit from a prior bug). Drop it and re-fetch
            // rather than serving wrong bytes.
            let _ = self.cache.invalidate(&key);
        }
        // Miss: ranged read of exactly this chunk from the remote, looping to
        // tolerate OpenDAL short reads (same guard as `fetch_through_cache`).
        let mut buf = vec![0u8; chunk_len];
        let mut off = 0usize;
        while off < chunk_len {
            let n = self.remote.read_at(chunk_start + off as u64, &mut buf[off..])?;
            if n == 0 {
                break;
            }
            off += n;
        }
        if off != chunk_len {
            return Err(ForstError::corruption(format!(
                "RangeCachedRandomAccessFile short read: expected {} bytes for chunk {} of {} \
                 (file_size {}), got {}",
                chunk_len, chunk_idx, self.path_key, self.file_size, off
            )));
        }
        // Best-effort cache populate; a failure just means the next read re-fetches.
        if let Err(e) = self.cache.put(&key, &buf) {
            eprintln!(
                "RangeCachedRandomAccessFile chunk cache put for {} failed: {} \
                 (continuing with the fetched bytes; next read will retry)",
                key, e
            );
        }
        Ok(buf)
    }

    /// Returns the on-disk byte length of chunk `chunk_idx` (clamped to
    /// `file_size` for the final chunk).
    fn chunk_len(&self, chunk_idx: u64) -> usize {
        let chunk_start = chunk_idx * BLOCK_CACHE_CHUNK_SIZE;
        let chunk_end = (chunk_start + BLOCK_CACHE_CHUNK_SIZE).min(self.file_size);
        (chunk_end - chunk_start) as usize
    }

    /// Tries to serve chunk `chunk_idx` from the local LRU. Returns
    /// `Ok(Some(bytes))` on a length-validated hit, `Ok(None)` on miss (also
    /// invalidating a poisoned wrong-length entry so the concurrent fetch
    /// re-reads it). Mirrors the hit-path validation in [`chunk_bytes`].
    fn cache_hit(&self, chunk_idx: u64) -> ForstResult<Option<Vec<u8>>> {
        let key = format!("{}#c{}", self.path_key, chunk_idx);
        if let Some(bytes) = self
            .cache
            .get(&key)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("block cache get: {e}"))))?
        {
            if bytes.len() == self.chunk_len(chunk_idx) {
                return Ok(Some(bytes));
            }
            let _ = self.cache.invalidate(&key);
        }
        Ok(None)
    }

    /// Fetches several missing chunks CONCURRENTLY via the remote backend's
    /// [`read_ranges`] primitive (true parallelism on OpenDAL/S3; serial on
    /// local/memory). Each fetched chunk is length-validated (short read →
    /// corruption, same guard as [`chunk_bytes`]) and best-effort cached.
    /// Returns the chunks in the SAME order as `missing`.
    ///
    /// [`read_ranges`]: forst_rs_io::RandomAccessFile::read_ranges
    fn fetch_missing_chunks(&self, missing: &[u64]) -> ForstResult<Vec<Vec<u8>>> {
        let ranges: Vec<(u64, usize)> = missing
            .iter()
            .map(|&idx| (idx * BLOCK_CACHE_CHUNK_SIZE, self.chunk_len(idx)))
            .collect();
        let fetched = self.remote.read_ranges(&ranges)?;
        debug_assert_eq!(fetched.len(), missing.len());
        for (i, &chunk_idx) in missing.iter().enumerate() {
            let want = self.chunk_len(chunk_idx);
            let bytes = &fetched[i];
            if bytes.len() != want {
                return Err(ForstError::corruption(format!(
                    "RangeCachedRandomAccessFile short read: expected {} bytes for chunk {} of {} \
                     (file_size {}), got {}",
                    want, chunk_idx, self.path_key, self.file_size, bytes.len()
                )));
            }
            let key = format!("{}#c{}", self.path_key, chunk_idx);
            if let Err(e) = self.cache.put(&key, bytes) {
                eprintln!(
                    "RangeCachedRandomAccessFile chunk cache put for {} failed: {} \
                     (continuing with the fetched bytes; next read will retry)",
                    key, e
                );
            }
        }
        Ok(fetched)
    }

    /// The original serial chunk-at-a-time read of `[offset, end)` into `buf`,
    /// fetching each chunk via [`chunk_bytes`] (cache hit or single ranged
    /// remote read). Used for single-chunk reads and the 0-1-missing fast path.
    fn serial_read_at(&self, offset: u64, end: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let total = (end - offset) as usize;
        let mut filled = 0usize;
        let mut pos = offset;
        while filled < total {
            let chunk_idx = pos / BLOCK_CACHE_CHUNK_SIZE;
            let chunk_start = chunk_idx * BLOCK_CACHE_CHUNK_SIZE;
            let chunk = self.chunk_bytes(chunk_idx)?;
            let in_chunk = (pos - chunk_start) as usize;
            let take = (chunk.len() - in_chunk).min(total - filled);
            buf[filled..filled + take].copy_from_slice(&chunk[in_chunk..in_chunk + take]);
            filled += take;
            pos += take as u64;
        }
        Ok(filled)
    }
}

impl RandomAccessFile for RangeCachedRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        if offset >= self.file_size || buf.is_empty() {
            return Ok(0);
        }
        let end = offset
            .saturating_add(buf.len() as u64)
            .min(self.file_size);
        let total = (end - offset) as usize;

        // FRS-WHOLE-SST-PREAD (2026-05-31): if the whole SST is resident in the
        // local cache, serve this block by preading EXACTLY the requested range
        // from it. Write-through V2 admits every flushed/compacted SST under the
        // whole-file key (`self.path_key`, no `#c` suffix) — the SAME key, but
        // the per-chunk read path below queries `{key}#c{idx}` and so NEVER read
        // those write-through bytes. Worse, to return one ~16 KiB data block the
        // chunk path `fs::read`s an entire 1 MiB chunk file (a 64× read
        // amplification: 1 MiB in-kernel memcpy + heap alloc per block, per
        // overlapping SST, per join probe — the profiled q4/q7/q9 wall). A
        // positional pread of the whole-SST file reads only the bytes asked for,
        // mirroring RocksDB's local-SST + block-pread model. Falls through to the
        // ranged chunk path on a miss (SST not write-through resident, e.g. a
        // cold restore read) or a short read (truncated/poisoned entry). The SST
        // is write-once, so a fixed (key, offset) is immutable — always sound.
        match self.cache.get_range(&self.path_key, offset, total) {
            Ok(Some(bytes)) if bytes.len() == total => {
                buf[..total].copy_from_slice(&bytes);
                return Ok(total);
            }
            // Miss or short read: fall through to the per-chunk ranged path.
            Ok(_) => {}
            Err(e) => {
                return Err(ForstError::Io(std::io::Error::other(format!(
                    "whole-SST pread for {}: {e}",
                    self.path_key
                ))))
            }
        }

        // Range of chunk indices this read spans (inclusive).
        let first_chunk = offset / BLOCK_CACHE_CHUNK_SIZE;
        let last_chunk = (end - 1) / BLOCK_CACHE_CHUNK_SIZE;
        let span = (last_chunk - first_chunk + 1) as usize;

        // Fast path: a single chunk — keep the existing serial fetch (no
        // concurrency to win, no first-pass overhead).
        if span <= 1 {
            return self.serial_read_at(offset, end, buf);
        }

        // First pass: probe the cache for every chunk in the span, holding the
        // hits and recording the misses. A length-validated hit is reused
        // directly; a miss (or poisoned entry, already invalidated by
        // `cache_hit`) is queued for a concurrent remote fetch.
        let mut cached: Vec<Option<Vec<u8>>> = Vec::with_capacity(span);
        let mut missing: Vec<u64> = Vec::new();
        for chunk_idx in first_chunk..=last_chunk {
            match self.cache_hit(chunk_idx)? {
                Some(bytes) => cached.push(Some(bytes)),
                None => {
                    cached.push(None);
                    missing.push(chunk_idx);
                }
            }
        }

        // 0-1 missing: not worth a concurrent round-trip; fall back to the
        // serial path (which re-probes the cache — cheap NVMe — for the hits).
        if missing.len() <= 1 {
            return self.serial_read_at(offset, end, buf);
        }

        // >1 missing: fetch them ALL concurrently in one `read_ranges` call,
        // then assemble the output from cache hits + freshly fetched chunks.
        let fetched = self.fetch_missing_chunks(&missing)?;
        let mut fetched_iter = fetched.into_iter();
        for slot in cached.iter_mut() {
            if slot.is_none() {
                *slot = fetched_iter.next();
            }
        }

        let mut filled = 0usize;
        let mut pos = offset;
        while filled < total {
            let chunk_idx = pos / BLOCK_CACHE_CHUNK_SIZE;
            let chunk_start = chunk_idx * BLOCK_CACHE_CHUNK_SIZE;
            let chunk = cached[(chunk_idx - first_chunk) as usize]
                .as_ref()
                .expect("every chunk in span was either cached or fetched");
            let in_chunk = (pos - chunk_start) as usize;
            let take = (chunk.len() - in_chunk).min(total - filled);
            buf[filled..filled + take].copy_from_slice(&chunk[in_chunk..in_chunk + take]);
            filled += take;
            pos += take as u64;
        }
        Ok(filled)
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.file_size)
    }
}

// Confirm `Send + Sync` so the engine can hold `Arc<dyn FileSystem>`.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CachedFileSystem>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_io::MemoryFileSystem;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn read_populates_cache_and_subsequent_reads_hit() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        // Seed the remote with a file.
        let path = PathBuf::from("/db/file.sst");
        let mut w = remote
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"hello-from-remote").unwrap();
        w.sync().unwrap();
        drop(w);

        // First read: miss → populate cache.
        let mut r = fs.open_sequential_file(&path).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello-from-remote");
        assert!(cache.contains("/db/file.sst"));

        // Second read: hit (delete the remote to prove it).
        remote.delete_file(&path).unwrap();
        let r2 = fs.open_random_access_file(&path).unwrap();
        let mut chunk = [0u8; 5];
        let n = r2.read_at(6, &mut chunk).unwrap();
        assert_eq!(&chunk[..n], b"from-");
    }

    #[test]
    fn ensure_cached_fetches_entire_file_on_first_access() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        // Seed the remote with a file.
        let path = PathBuf::from("/db/test.sst");
        let mut w = remote
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"hello world data").unwrap();
        w.sync().unwrap();
        drop(w);

        // First access: cache miss, fetches entire file.
        assert!(!cache.contains("/db/test.sst"));
        fs.ensure_cached(&path).unwrap();
        assert!(cache.contains("/db/test.sst"));

        // Subsequent reads are cache hits (delete remote to prove it).
        remote.delete_file(&path).unwrap();
        let r = fs.open_random_access_file(&path).unwrap();
        let mut buf = [0u8; 5];
        let n = r.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..n], b"hello");
    }

    #[test]
    fn ensure_cached_is_noop_on_hit() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        let path = PathBuf::from("/db/hit.sst");
        let mut w = remote
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"cached-data").unwrap();
        w.sync().unwrap();
        drop(w);

        // Populate cache.
        fs.ensure_cached(&path).unwrap();
        assert!(cache.contains("/db/hit.sst"));

        // Delete remote — ensure_cached should not fail because it's a hit.
        remote.delete_file(&path).unwrap();
        fs.ensure_cached(&path).unwrap(); // no-op, no remote access
    }

    #[test]
    fn prefetch_files_returns_miss_count() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        let path_a = PathBuf::from("/db/a.sst");
        let path_b = PathBuf::from("/db/b.sst");
        for (p, data) in [(&path_a, b"aaa" as &[u8]), (&path_b, b"bbb" as &[u8])] {
            let mut w = remote
                .open_writable_file(p, WriteMode::CreateOrTruncate)
                .unwrap();
            w.append(data).unwrap();
            w.sync().unwrap();
        }

        // Both are misses on first prefetch.
        let misses = fs
            .prefetch_files(&[path_a.as_path(), path_b.as_path()])
            .unwrap();
        assert_eq!(misses, 2);

        // Second call: both are hits.
        let misses = fs
            .prefetch_files(&[path_a.as_path(), path_b.as_path()])
            .unwrap();
        assert_eq!(misses, 0);
    }

    #[test]
    fn open_writable_sst_is_write_through_cached() {
        // FRS-S3-WRITETHROUGH-V2 (2026-05-31): SST write-through is RE-ENABLED
        // (gated to `.sst` files, paired with a large on-disk cache) so reads of
        // just-flushed state hit local disk instead of S3 — the q9/q7 ckpt-ON
        // cold-read collapse. sync() must populate the cache with the SST bytes.
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        // Flush an SST through the caching FS (the engine's write path).
        let path = PathBuf::from("/db/00000042.sst");
        let mut w = fs
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"sst-block-").unwrap();
        w.append(b"contents").unwrap();
        w.sync().unwrap();
        drop(w);

        // Write-through V2 (synchronous): the SST bytes are in the local cache
        // the instant sync() returns (no S3 read needed to serve the next
        // reader, and no race between visibility and population).
        assert!(
            cache.contains("/db/00000042.sst"),
            "write-through V2: sync() must populate the cache for .sst files"
        );
        let cached = cache.get("/db/00000042.sst").unwrap().unwrap();
        assert_eq!(&cached, b"sst-block-contents");

        // The read path serves the correct bytes (from cache now).
        let mut r = fs.open_sequential_file(&path).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"sst-block-contents");

        // Non-SST files (e.g. WAL/MANIFEST) are NOT write-through cached — they
        // must not pollute the read cache.
        let wal = PathBuf::from("/db/00000044.log");
        let mut wl = fs
            .open_writable_file(&wal, WriteMode::CreateOrTruncate)
            .unwrap();
        wl.append(b"wal-bytes").unwrap();
        wl.sync().unwrap();
        drop(wl);
        assert!(
            !cache.contains("/db/00000044.log"),
            "write-through V2 is gated to .sst — non-SST files must not be cached"
        );

        // file_size delegates to the inner writer (sanity check of the
        // wrapper's delegation while the writer is still open).
        let mut w2 = fs
            .open_writable_file(&PathBuf::from("/db/00000043.sst"), WriteMode::CreateOrTruncate)
            .unwrap();
        w2.append(b"abcde").unwrap();
        assert_eq!(w2.file_size().unwrap(), 5);
    }

    #[test]
    fn range_cached_raf_reads_correctly_across_chunk_boundaries() {
        // FRS-BLOCKCACHE: validate the chunked range reader against a known
        // multi-chunk payload — boundary-spanning reads, the short final chunk,
        // reads past EOF, and a cache-hit second pass must all be byte-exact.
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 64 << 20).unwrap());

        // 2.5 MiB → chunks [0..1Mi), [1Mi..2Mi), [2Mi..2.5Mi) (last is short).
        let size = (BLOCK_CACHE_CHUNK_SIZE * 2 + BLOCK_CACHE_CHUNK_SIZE / 2) as usize;
        let mut data = vec![0u8; size];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8; // deterministic, non-trivial pattern
        }
        let path = PathBuf::from("/db/00000077.sst");
        {
            let mut w = remote
                .open_writable_file(&path, WriteMode::CreateOrTruncate)
                .unwrap();
            w.append(&data).unwrap();
            w.sync().unwrap();
        }

        let raf = RangeCachedRandomAccessFile {
            remote: remote.open_random_access_file(&path).unwrap(),
            cache: Arc::clone(&cache),
            path_key: "/db/00000077.sst".to_string(),
            file_size: size as u64,
        };
        assert_eq!(raf.file_size().unwrap(), size as u64);

        let read_check = |raf: &RangeCachedRandomAccessFile, off: u64, len: usize| {
            let mut buf = vec![0u8; len];
            let n = raf.read_at(off, &mut buf).unwrap();
            let off = off as usize;
            let expect = (size.saturating_sub(off)).min(len);
            assert_eq!(n, expect, "read_at({off},{len}) returned wrong count");
            assert_eq!(&buf[..n], &data[off..off + n], "bytes mismatch at off {off}");
        };

        // Within one chunk, spanning two chunks, spanning all three, the short
        // tail, exactly at EOF, and entirely past EOF.
        read_check(&raf, 100, 200);
        read_check(&raf, BLOCK_CACHE_CHUNK_SIZE - 50, 100); // straddles chunk 0/1
        read_check(&raf, 10, size - 10); // whole file minus a bit (all chunks)
        read_check(&raf, BLOCK_CACHE_CHUNK_SIZE * 2 + 1000, 100_000); // into short tail
        read_check(&raf, size as u64 - 5, 50); // clamps at EOF
        assert_eq!(raf.read_at(size as u64, &mut [0u8; 16]).unwrap(), 0); // past EOF

        // Second pass must be served from the chunk cache (delete the remote so
        // any remote read would fail) and still be byte-exact.
        remote.delete_file(&path).unwrap();
        read_check(&raf, 0, size);
        read_check(&raf, BLOCK_CACHE_CHUNK_SIZE + 7, 4096);
    }

    #[test]
    fn read_at_serves_from_whole_sst_writethrough_entry() {
        // FRS-WHOLE-SST-PREAD: when write-through V2 has admitted the whole SST
        // under the whole-file key, read_at must serve blocks by preading that
        // entry directly — byte-exact, and WITHOUT any remote read or any
        // per-chunk (`#c`) cache entry being created (the chunk path is the
        // fallback only).
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 64 << 20).unwrap());

        let size = (BLOCK_CACHE_CHUNK_SIZE * 2 + 4096) as usize; // spans 3 chunks
        let mut data = vec![0u8; size];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        let path = PathBuf::from("/db/00000088.sst");
        {
            let mut w = remote
                .open_writable_file(&path, WriteMode::CreateOrTruncate)
                .unwrap();
            w.append(&data).unwrap();
            w.sync().unwrap();
        }
        let key = "/db/00000088.sst".to_string();
        // Simulate write-through V2: the whole SST is resident under the
        // whole-file key.
        assert!(cache.put(&key, &data).unwrap());

        let raf = RangeCachedRandomAccessFile {
            remote: remote.open_random_access_file(&path).unwrap(),
            cache: Arc::clone(&cache),
            path_key: key.clone(),
            file_size: size as u64,
        };

        // Delete the remote so ANY fallback to the ranged chunk path would fail:
        // every read below MUST be served from the whole-SST pread fast path.
        remote.delete_file(&path).unwrap();

        let read_check = |off: u64, len: usize| {
            let mut buf = vec![0u8; len];
            let n = raf.read_at(off, &mut buf).unwrap();
            let off = off as usize;
            let expect = (size.saturating_sub(off)).min(len);
            assert_eq!(n, expect, "read_at({off},{len}) wrong count");
            assert_eq!(&buf[..n], &data[off..off + n], "bytes mismatch at {off}");
        };
        read_check(100, 200); // small block within chunk 0
        read_check(BLOCK_CACHE_CHUNK_SIZE - 50, 100); // straddles a chunk boundary
        read_check(BLOCK_CACHE_CHUNK_SIZE * 2 + 1000, 4096); // into the short tail
        read_check(size as u64 - 5, 50); // clamps at EOF
        assert_eq!(raf.read_at(size as u64, &mut [0u8; 16]).unwrap(), 0); // past EOF

        // The fast path must NOT have created any per-chunk entries.
        assert!(!cache.contains(&format!("{key}#c0")));
        assert!(!cache.contains(&format!("{key}#c1")));
    }

    #[test]
    fn open_writable_non_sst_is_not_cached_on_write() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        // The checkpoint manifest is MUTABLE — it must NOT be write-cached,
        // or a rewrite would leave stale bytes in the cache.
        let manifest = PathBuf::from("/db/CHECKPOINT.blob");
        let mut w = fs
            .open_writable_file(&manifest, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"manifest-v1").unwrap();
        w.sync().unwrap();
        drop(w);
        assert!(
            !cache.contains("/db/CHECKPOINT.blob"),
            "mutable manifest must never be populated into the cache on write"
        );

        // Likewise MANIFEST and the WAL (no .sst extension).
        let wal = PathBuf::from("/db/000001.log");
        let mut w = fs
            .open_writable_file(&wal, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"wal-record").unwrap();
        w.sync().unwrap();
        drop(w);
        assert!(!cache.contains("/db/000001.log"));
    }

    #[test]
    fn open_writable_non_sst_pre_invalidates_stale_entry() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        // Seed a cache entry for a mutable path via a read.
        let manifest = PathBuf::from("/db/MANIFEST-000001");
        let mut w = remote
            .open_writable_file(&manifest, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"old-manifest").unwrap();
        w.sync().unwrap();
        drop(w);
        let _ = fs.open_sequential_file(&manifest).unwrap();
        assert!(cache.contains("/db/MANIFEST-000001"));

        // Reopening for write must pre-invalidate the stale cache entry.
        let _w = fs
            .open_writable_file(&manifest, WriteMode::CreateOrTruncate)
            .unwrap();
        assert!(
            !cache.contains("/db/MANIFEST-000001"),
            "non-SST open_writable_file must pre-invalidate the stale cache entry"
        );
    }

    #[test]
    fn delete_invalidates_cache_entry() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        let cache = Arc::new(LocalCache::open(tmp.path(), 1 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        let path = PathBuf::from("/db/ephemeral.sst");
        let mut w = remote
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"transient").unwrap();
        w.sync().unwrap();
        drop(w);
        let _ = fs.open_sequential_file(&path).unwrap();
        assert!(cache.contains("/db/ephemeral.sst"));

        fs.delete_file(&path).unwrap();
        assert!(!cache.contains("/db/ephemeral.sst"));
    }

    /// FRS-CONCURRENT-READ: a multi-chunk read that misses on >1 chunk takes
    /// the concurrent `read_ranges` assembly path. Its bytes MUST be identical
    /// to a serial chunk-by-chunk read of the same offsets, and identical to
    /// the source payload. We use a > 3 MiB file (4 chunks) and a cache large
    /// enough to hold them all so the concurrent fetch + assembly is exercised
    /// without eviction noise.
    #[test]
    fn multi_chunk_concurrent_read_matches_serial_and_source() {
        let tmp = TempDir::new().unwrap();
        let remote: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        remote.create_dir_all(Path::new("/db")).unwrap();
        // 16 MiB cache: plenty for the whole file (no eviction).
        let cache = Arc::new(LocalCache::open(tmp.path(), 16 << 20).unwrap());
        let fs = CachedFileSystem::new(remote.clone(), cache.clone());

        // 3.5 MiB payload spanning 4 chunks (1 MiB each, last is partial).
        const SIZE: usize = 3 * (1 << 20) + (1 << 19);
        let mut payload = Vec::with_capacity(SIZE);
        let mut state: u32 = 0x9E37_79B9;
        while payload.len() < SIZE {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            payload.extend_from_slice(&state.to_le_bytes());
        }
        payload.truncate(SIZE);

        let path = PathBuf::from("/db/multi.sst");
        let mut w = remote
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(&payload).unwrap();
        w.sync().unwrap();
        drop(w);

        // COLD read spanning all 4 chunks -> >1 miss -> concurrent path.
        // Use an unaligned offset/len so chunk-boundary arithmetic is tested.
        let offset = 12_345u64;
        let len = SIZE - 20_000;
        let r = fs.open_random_access_file(&path).unwrap();
        let mut concurrent = vec![0u8; len];
        let n = r.read_at(offset, &mut concurrent).unwrap();
        concurrent.truncate(n);

        // Reference: exact source slice.
        let expected = &payload[offset as usize..offset as usize + len];
        assert_eq!(n, len, "cold concurrent read returned short");
        assert_eq!(concurrent, expected, "concurrent bytes differ from source");

        // WARM read (all chunks now cached) must yield identical bytes — this
        // exercises the all-hits assembly branch (0 missing).
        let r2 = fs.open_random_access_file(&path).unwrap();
        let mut warm = vec![0u8; len];
        let n2 = r2.read_at(offset, &mut warm).unwrap();
        warm.truncate(n2);
        assert_eq!(warm, expected, "warm read bytes differ from source");

        // Read whole file in one shot through a fresh (cold) cache and compare
        // to the source — exercises first_chunk=0..last_chunk path end-to-end.
        let tmp2 = TempDir::new().unwrap();
        let cache2 = Arc::new(LocalCache::open(tmp2.path(), 16 << 20).unwrap());
        let fs2 = CachedFileSystem::new(remote.clone(), cache2.clone());
        let r3 = fs2.open_random_access_file(&path).unwrap();
        let mut whole = vec![0u8; SIZE];
        let n3 = r3.read_at(0, &mut whole).unwrap();
        assert_eq!(n3, SIZE);
        assert_eq!(whole, payload, "whole-file concurrent read differs from source");
    }
}

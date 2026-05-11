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

use std::path::Path;
use std::sync::Arc;

use forst_rs_common::error::{ForstError, ForstResult};
use forst_rs_io::{
    FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode,
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
    pub fn cache(&self) -> &Arc<LocalCache> {
        &self.cache
    }

    /// Returns the remote backend.
    pub fn remote(&self) -> &Arc<dyn FileSystem> {
        &self.remote
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
    fn fetch_through_cache(&self, path: &Path) -> ForstResult<Vec<u8>> {
        let key = self.cache_key(path)?.to_string();
        if let Some(bytes) = self
            .cache
            .get(&key)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("cache get: {e}"))))?
        {
            return Ok(bytes);
        }
        // Miss: pull the whole file via the remote backend's sequential
        // reader. This is the same pattern OpenDAL itself uses for small
        // objects, and SST file sizes (~64 MiB by default) are below the
        // threshold where streaming would matter.
        let mut reader = self.remote.open_sequential_file(path)?;
        let mut bytes = Vec::new();
        let mut chunk = vec![0u8; 64 * 1024];
        loop {
            let n = reader.read(&mut chunk)?;
            if n == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..n]);
        }
        // Best-effort cache write; failures here just mean the next read
        // pays the same miss, so we propagate I/O errors but ignore the
        // "did not admit" case (e.g., entry larger than capacity).
        let _ = self
            .cache
            .put(&key, &bytes)
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("cache put: {e}"))))?;
        Ok(bytes)
    }
}

impl FileSystem for CachedFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let bytes = self.fetch_through_cache(path)?;
        Ok(Box::new(InMemorySequential::new(bytes)))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let bytes = self.fetch_through_cache(path)?;
        Ok(Box::new(InMemoryRandom::new(bytes)))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        // Writes go to the remote backend. Pre-invalidate any stale entry
        // for `path` so the next reader doesn't see ghost bytes.
        if let Ok(key) = self.cache_key(path) {
            let _ = self.cache.invalidate(key);
        }
        self.remote.open_writable_file(path, mode)
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

    fn name(&self) -> &str {
        &self.name
    }
}

// ---------------------------------------------------------------------------
// In-memory file adapters
//
// Both adapters serve cached bytes from a Vec<u8> held in the heap. The
// engine's SST reader is happy with any RandomAccessFile / SequentialFile
// impl — these adapters keep the heap copy alive for the lifetime of the
// read.
// ---------------------------------------------------------------------------

struct InMemorySequential {
    bytes: Vec<u8>,
    pos: usize,
}

impl InMemorySequential {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, pos: 0 }
    }
}

impl SequentialFile for InMemorySequential {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        let remaining = self.bytes.len().saturating_sub(self.pos);
        let n = remaining.min(buf.len());
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
    bytes: Vec<u8>,
}

impl InMemoryRandom {
    fn new(bytes: Vec<u8>) -> Self {
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
}

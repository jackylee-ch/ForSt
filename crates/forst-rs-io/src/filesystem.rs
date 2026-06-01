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

//! Filesystem abstraction for ForSt-RS.
//!
//! This module defines the core traits that all filesystem backends must
//! implement. The design mirrors RocksDB's `Env`/`FileSystem` split:
//!
//! - [`FileSystem`]: Directory and file lifecycle operations.
//! - [`SequentialFile`]: Forward-only reading (WAL replay, SST scanning).
//! - [`RandomAccessFile`]: Positioned reads (SST block lookups).
//! - [`WritableFile`]: Append-only writing (WAL, SST, MANIFEST).
//!
//! All I/O operations are synchronous in this trait layer. Async wrappers
//! (e.g., for S3/remote storage) are built on top using Tokio.

use std::path::{Path, PathBuf};

use forst_rs_common::error::{ForstError, ForstResult};

// ---------------------------------------------------------------------------
// File metadata
// ---------------------------------------------------------------------------

/// Metadata about a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    /// Full path to the file.
    pub path: PathBuf,
    /// File size in bytes.
    pub size: u64,
    /// Whether this entry is a directory.
    pub is_dir: bool,
}

/// Options controlling how a file is opened for writing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Create a new file (error if exists).
    CreateNew,
    /// Create or truncate.
    CreateOrTruncate,
    /// Open for append (create if not exists).
    Append,
}

// ---------------------------------------------------------------------------
// Core file traits
// ---------------------------------------------------------------------------

/// A file opened for sequential (forward-only) reading.
///
/// Used for WAL replay and full SST scans.
pub trait SequentialFile: Send {
    /// Reads up to `buf.len()` bytes into `buf`.
    ///
    /// Returns the number of bytes actually read. A return value of 0
    /// indicates end-of-file.
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize>;

    /// Skips `n` bytes ahead. Equivalent to reading and discarding.
    fn skip(&mut self, n: u64) -> ForstResult<()>;
}

/// A file opened for random (positioned) reads.
///
/// Used for reading SST blocks and index data. Implementations must
/// be safe to call from multiple threads concurrently (the trait
/// requires `Send + Sync`).
pub trait RandomAccessFile: Send + Sync {
    /// Reads up to `buf.len()` bytes starting at `offset`.
    ///
    /// Returns the number of bytes actually read. If fewer bytes are
    /// returned than requested and no error occurs, the caller has
    /// reached end-of-file.
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize>;

    /// Reads multiple `(offset, len)` ranges and returns one `Vec<u8>` per
    /// range, in the same order as `ranges`.
    ///
    /// The default implementation loops serially over [`read_at`], applying
    /// the short-read loop per range so a truncated/empty backend reply does
    /// not silently shorten a range. Backends that own an async runtime
    /// (e.g. OpenDAL/S3) override this to issue the ranges CONCURRENTLY,
    /// which collapses a cold N-chunk scan from N sequential round-trips into
    /// ~ceil(N/concurrency) batches. Each returned `Vec` is truncated to the
    /// number of bytes actually read (EOF-shortened ranges are shorter than
    /// requested), exactly matching what repeated `read_at` calls would yield.
    ///
    /// [`read_at`]: RandomAccessFile::read_at
    fn read_ranges(&self, ranges: &[(u64, usize)]) -> ForstResult<Vec<Vec<u8>>> {
        ranges
            .iter()
            .map(|&(off, len)| {
                let mut b = vec![0u8; len];
                let mut filled = 0;
                while filled < len {
                    let n = self.read_at(off + filled as u64, &mut b[filled..])?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                b.truncate(filled);
                Ok(b)
            })
            .collect()
    }

    /// Returns the total file size in bytes.
    fn file_size(&self) -> ForstResult<u64>;
}

/// A file opened for sequential (append-only) writes.
///
/// Used for writing WAL records, SST files, and MANIFEST.
pub trait WritableFile: Send {
    /// Appends `data` to the file.
    fn append(&mut self, data: &[u8]) -> ForstResult<()>;

    /// Flushes buffered data to the OS page cache.
    ///
    /// This does NOT guarantee durability — use [`sync`](WritableFile::sync)
    /// for that.
    fn flush(&mut self) -> ForstResult<()>;

    /// Ensures all written data is durable on persistent storage.
    fn sync(&mut self) -> ForstResult<()>;

    /// Returns the current file size (bytes written so far).
    fn file_size(&self) -> ForstResult<u64>;
}

// ---------------------------------------------------------------------------
// FileSystem trait
// ---------------------------------------------------------------------------

/// The filesystem abstraction.
///
/// Implementations provide directory management and file open/create
/// operations. The engine accesses all persistent storage through this
/// trait, enabling pluggable backends (local POSIX, in-memory, S3, etc.).
pub trait FileSystem: Send + Sync {
    /// Opens a file for sequential reading.
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>>;

    /// Opens a file for random-access reading.
    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>>;

    /// Opens (or creates) a file for writing.
    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>>;

    /// Returns `true` if the path exists and is a file.
    fn file_exists(&self, path: &Path) -> ForstResult<bool>;

    /// Returns metadata for a file or directory.
    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata>;

    /// Lists all children (files and directories) under `dir`.
    ///
    /// The returned paths are the full paths, not just basenames.
    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>>;

    /// Creates a directory (and parents if necessary).
    fn create_dir_all(&self, dir: &Path) -> ForstResult<()>;

    /// Deletes a file. Returns `NotFound` if the file doesn't exist.
    fn delete_file(&self, path: &Path) -> ForstResult<()>;

    /// Deletes a directory. Returns error if the directory is not empty
    /// (unless `recursive` is true).
    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()>;

    /// Renames a file from `src` to `dst`.
    ///
    /// This must be atomic on the same filesystem (required for MANIFEST
    /// and SST file rotation).
    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()>;

    /// Whether this filesystem provides an atomic [`Self::rename`].
    ///
    /// Local POSIX filesystems do (`rename(2)` is atomic within a mount), so
    /// the SST/MANIFEST write paths stage to a `.tmp` file and rename into
    /// place for crash-atomic publication. **Object stores (S3/GCS/OSS/Azure)
    /// have no server-side rename** — OpenDAL surfaces it as `Unsupported`, and
    /// a copy+delete emulation is *not* atomic (it can expose two objects or
    /// orphan the source). For those backends this returns `false`, and the
    /// write paths instead stream the SST straight to its final key: an
    /// object-store multipart upload only publishes the object on a successful
    /// `close()` (CompleteMultipartUpload), so a mid-write crash leaves an
    /// incomplete upload that never becomes visible — crash-atomic without a
    /// rename. Mirrors how ForSt's native engine writes to disaggregated
    /// storage.
    ///
    /// Default `true` so POSIX-style backends need not override.
    fn supports_atomic_rename(&self) -> bool {
        true
    }

    /// R49-H3: fsync the directory at `dir` so any prior `rename()` /
    /// create / unlink of a file inside it survives a power-loss event.
    /// POSIX requires this for the directory entry change to be durable
    /// even after the renamed file's own contents are fsynced.
    ///
    /// Implementations:
    /// * `LocalFileSystem` opens the directory with `O_RDONLY` and fsyncs
    ///   the descriptor.
    /// * Memory / object-store / OpenDAL backends are no-ops — they have
    ///   no notion of a kernel-cached directory entry.
    ///
    /// Default implementation is a no-op so non-POSIX backends don't need
    /// to override.
    fn sync_dir(&self, _dir: &Path) -> ForstResult<()> {
        Ok(())
    }

    /// Returns a human-readable name for this filesystem implementation.
    fn name(&self) -> &str;

    /// Hint: ensure the file at `path` is locally available for fast reads.
    ///
    /// For caching filesystem implementations (e.g. `CachedFileSystem`),
    /// this fetches the entire file from the remote backend into the local
    /// cache on a miss. For local or in-memory filesystems this is a no-op.
    ///
    /// Callers use this to prefetch SST files before opening readers,
    /// amortizing S3 round-trip latency across multiple files (vector I/O
    /// prefetch pattern).
    ///
    /// The default implementation is a no-op that always succeeds.
    fn ensure_cached(&self, _path: &Path) -> ForstResult<()> {
        Ok(())
    }

    /// FRS-S3-READ-CONCURRENCY (2026-05-31): best-effort warm the cache for
    /// `paths`, fetching cache MISSES CONCURRENTLY when the backend supports it.
    ///
    /// Caching/object-store backends override this to fan the per-file fetches
    /// out across threads so K misses cost ≈1 round-trip instead of K serial
    /// ones (the q9 join-stall hot path — see `CachedFileSystem`). The default
    /// is a serial loop over [`ensure_cached`](Self::ensure_cached), which is a
    /// no-op for local/in-memory filesystems.
    fn prefetch_concurrent(&self, paths: &[&Path]) {
        for p in paths {
            let _ = self.ensure_cached(p);
        }
    }

    /// 2026-05-29 WRITE-BACK FLUSH: block until any in-flight asynchronous
    /// upload of `path` has completed (and propagate its error). Backends that
    /// upload writes asynchronously (object-store / S3 write-back) override
    /// this so a reader that needs `path` from the remote first waits for the
    /// upload to finish — the correctness guard that lets the flush hot path
    /// return on the local write and upload to S3 off the critical path.
    ///
    /// Default no-op: local/memory backends write synchronously, nothing to
    /// await.
    fn await_upload(&self, _path: &Path) -> ForstResult<()> {
        Ok(())
    }

    /// 2026-05-29 WRITE-BACK FLUSH: block until ALL in-flight asynchronous
    /// uploads have completed (and propagate the first error). Used at the
    /// checkpoint barrier and on shutdown to establish remote durability of
    /// every flushed/compacted SST. Default no-op.
    fn await_all_uploads(&self) -> ForstResult<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Convenience: map std::io::Error to ForstError
// ---------------------------------------------------------------------------

/// Converts an `io::Error` that represents "not found" into
/// `ForstError::NotFound`, and all others into `ForstError::Io`.
pub fn map_io_error(err: std::io::Error, context: &str) -> ForstError {
    match err.kind() {
        std::io::ErrorKind::NotFound => ForstError::not_found(format!("{}: {}", context, err)),
        std::io::ErrorKind::AlreadyExists => {
            ForstError::invalid_argument(format!("{}: {}", context, err))
        }
        _ => ForstError::Io(err),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_metadata_eq() {
        let m1 = FileMetadata {
            path: PathBuf::from("/tmp/test.sst"),
            size: 1024,
            is_dir: false,
        };
        let m2 = m1.clone();
        assert_eq!(m1, m2);
    }

    #[test]
    fn test_write_mode_copy() {
        let mode = WriteMode::CreateNew;
        let mode2 = mode;
        assert_eq!(mode, mode2);
    }

    #[test]
    fn test_map_io_error_not_found() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "gone");
        let forst_err = map_io_error(io_err, "reading SST");
        assert!(forst_err.is_not_found());
        assert!(forst_err.to_string().contains("reading SST"));
    }

    #[test]
    fn test_map_io_error_already_exists() {
        let io_err = std::io::Error::new(std::io::ErrorKind::AlreadyExists, "exists");
        let forst_err = map_io_error(io_err, "creating file");
        assert!(forst_err.is_invalid_argument());
    }

    #[test]
    fn test_map_io_error_other() {
        let io_err = std::io::Error::other("disk failure");
        let forst_err = map_io_error(io_err, "writing WAL");
        assert!(forst_err.is_io());
    }
}

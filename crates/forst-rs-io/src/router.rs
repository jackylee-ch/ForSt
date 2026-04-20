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

//! Filesystem router for ForSt-RS tiered storage.
//!
//! [`FileSystemRouter`] routes file operations to the appropriate filesystem
//! backend based on file type:
//!
//! - **SST files** (`.sst`) are routed to a remote filesystem (e.g., S3)
//!   for tiered storage in DeltaJoin localization scenarios.
//! - **All other files** (WAL, MANIFEST, CURRENT, OPTIONS, etc.) remain on
//!   the local filesystem for low-latency access.
//!
//! When no remote filesystem is configured, all operations fall back to the
//! local filesystem transparently.
//!
//! This design follows the RocksDB/ForSt `Env` routing pattern and aligns
//! with the DeltaJoin localization design (Section 5.2).

use std::path::Path;
use std::sync::Arc;

use forst_rs_common::error::ForstResult;

use crate::filesystem::{FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode};

// ---------------------------------------------------------------------------
// FileSystemRouter
// ---------------------------------------------------------------------------

/// Routes file operations to the appropriate filesystem based on file extension.
///
/// SST files (`.sst`) are routed to a configurable remote filesystem (e.g., S3),
/// while all other files (WAL, MANIFEST, CURRENT, etc.) are handled by the
/// local filesystem. When no remote filesystem is configured, all operations
/// fall back to the local filesystem.
///
/// # Examples
///
/// ```
/// use forst_rs_io::router::FileSystemRouter;
/// use forst_rs_io::memory_fs::MemoryFileSystem;
/// use forst_rs_io::filesystem::FileSystem;
/// use std::sync::Arc;
///
/// // Local-only mode (no remote filesystem).
/// let local = Arc::new(MemoryFileSystem::new());
/// let router = FileSystemRouter::new(local);
/// assert_eq!(router.name(), "FileSystemRouter");
/// assert_eq!(
///     router.display_name(),
///     "FileSystemRouter(local=MemoryFileSystem)"
/// );
/// ```
///
/// ```
/// use forst_rs_io::router::FileSystemRouter;
/// use forst_rs_io::memory_fs::MemoryFileSystem;
/// use forst_rs_io::filesystem::FileSystem;
/// use std::sync::Arc;
///
/// // Tiered mode: SSTs go to remote, everything else stays local.
/// let local = Arc::new(MemoryFileSystem::new());
/// let remote = Arc::new(MemoryFileSystem::new());
/// let router = FileSystemRouter::with_remote(local, remote);
/// assert_eq!(router.name(), "FileSystemRouter");
/// assert_eq!(
///     router.display_name(),
///     "FileSystemRouter(local=MemoryFileSystem, remote=MemoryFileSystem)"
/// );
/// ```
pub struct FileSystemRouter {
    local_fs: Arc<dyn FileSystem>,
    remote_fs: Option<Arc<dyn FileSystem>>,
}

impl FileSystemRouter {
    /// Creates a router that sends all operations to the local filesystem.
    ///
    /// This is equivalent to running without tiered storage; SST files will
    /// also be stored locally.
    pub fn new(local_fs: Arc<dyn FileSystem>) -> Self {
        Self {
            local_fs,
            remote_fs: None,
        }
    }

    /// Creates a router with both local and remote filesystems.
    ///
    /// SST files will be routed to `remote_fs`, and all other files to
    /// `local_fs`.
    pub fn with_remote(
        local_fs: Arc<dyn FileSystem>,
        remote_fs: Arc<dyn FileSystem>,
    ) -> Self {
        Self {
            local_fs,
            remote_fs: Some(remote_fs),
        }
    }

    /// Returns `true` if the file at `path` should be stored remotely.
    ///
    /// Currently, only `.sst` files are considered remote. This matches
    /// the RocksDB/ForSt convention where SST files are the bulk of
    /// data and are candidates for offloading to object storage.
    fn is_remote_file(path: &Path) -> bool {
        path.extension().is_some_and(|ext| ext == "sst")
    }

    /// Returns the filesystem that should handle operations for `path`.
    ///
    /// If no remote filesystem is configured, always returns the local
    /// filesystem regardless of file type.
    fn route(&self, path: &Path) -> &dyn FileSystem {
        if Self::is_remote_file(path) {
            self.remote_fs
                .as_deref()
                .unwrap_or(self.local_fs.as_ref())
        } else {
            self.local_fs.as_ref()
        }
    }

    /// Returns a reference to the local filesystem.
    pub fn local_fs(&self) -> &dyn FileSystem {
        self.local_fs.as_ref()
    }

    /// Returns a reference to the remote filesystem, if configured.
    pub fn remote_fs(&self) -> Option<&dyn FileSystem> {
        self.remote_fs.as_deref()
    }

    /// Returns `true` if a remote filesystem has been configured.
    pub fn has_remote(&self) -> bool {
        self.remote_fs.is_some()
    }
}

// ---------------------------------------------------------------------------
// FileSystem implementation — delegates every method via route()
// ---------------------------------------------------------------------------

impl FileSystem for FileSystemRouter {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        self.route(path).open_sequential_file(path)
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        self.route(path).open_random_access_file(path)
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        self.route(path).open_writable_file(path, mode)
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        self.route(path).file_exists(path)
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        self.route(path).get_file_metadata(path)
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        // Directory listing is always handled by the local filesystem,
        // because directories (WAL dir, db dir, etc.) live locally.
        self.local_fs.list_dir(dir)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        // Directories are always local; remote object stores typically
        // do not have real directories.
        self.local_fs.create_dir_all(dir)
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        self.route(path).delete_file(path)
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        // Directories are always local.
        self.local_fs.delete_dir(path, recursive)
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        // Both src and dst should be on the same filesystem.
        // Route based on the destination path (the file's final location).
        self.route(dst).rename(src, dst)
    }

    fn name(&self) -> &str {
        // We return a static string; for display purposes the user can
        // inspect local_fs().name() and remote_fs().map(|f| f.name()).
        "FileSystemRouter"
    }
}

// Override the default `name()` to include child filesystem names for
// debugging. We implement a separate method since the trait returns &str.
impl std::fmt::Display for FileSystemRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.remote_fs {
            Some(remote) => write!(
                f,
                "FileSystemRouter(local={}, remote={})",
                self.local_fs.name(),
                remote.name()
            ),
            None => write!(
                f,
                "FileSystemRouter(local={})",
                self.local_fs.name()
            ),
        }
    }
}

impl std::fmt::Debug for FileSystemRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

// ---------------------------------------------------------------------------
// FileSystemRouter — extended name() via a separate helper
// ---------------------------------------------------------------------------

impl FileSystemRouter {
    /// Returns a descriptive name including the child filesystem names.
    ///
    /// Unlike `FileSystem::name()` which returns `&str`, this method
    /// allocates a `String` with full detail for logging and diagnostics.
    pub fn display_name(&self) -> String {
        self.to_string()
    }
}

// Override `FileSystem::name()` to return a richer description.
// Since the trait requires `&str`, we provide descriptive output via
// the `Display` impl above and keep `name()` returning a static str.

// ---------------------------------------------------------------------------
// Convenience: convert typed paths to route decisions
// ---------------------------------------------------------------------------

/// Known ForSt-RS file extensions and their storage locality.
///
/// This is informational only; the routing decision itself is made by
/// [`FileSystemRouter::is_remote_file`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileLocality {
    /// File should be stored on local filesystem (WAL, MANIFEST, etc.).
    Local,
    /// File should be stored on remote filesystem (SST data files).
    Remote,
}

/// Determines the intended storage locality for a given file path.
///
/// This is a pure function with no side effects; it only inspects the
/// file extension.
pub fn file_locality(path: &Path) -> FileLocality {
    if FileSystemRouter::is_remote_file(path) {
        FileLocality::Remote
    } else {
        FileLocality::Local
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_fs::MemoryFileSystem;

    // -- Routing logic -------------------------------------------------------

    #[test]
    fn test_is_remote_file_sst() {
        assert!(FileSystemRouter::is_remote_file(Path::new("/data/000042.sst")));
        assert!(FileSystemRouter::is_remote_file(Path::new("table.sst")));
    }

    #[test]
    fn test_is_remote_file_non_sst() {
        assert!(!FileSystemRouter::is_remote_file(Path::new("/wal/000001.log")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("MANIFEST-000001")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("CURRENT")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("OPTIONS-000001")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("LOCK")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("/db/IDENTITY")));
    }

    #[test]
    fn test_is_remote_file_no_extension() {
        assert!(!FileSystemRouter::is_remote_file(Path::new("CURRENT")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("/path/to/MANIFEST")));
    }

    #[test]
    fn test_file_locality_function() {
        assert_eq!(file_locality(Path::new("000001.sst")), FileLocality::Remote);
        assert_eq!(file_locality(Path::new("000001.log")), FileLocality::Local);
        assert_eq!(file_locality(Path::new("MANIFEST-000001")), FileLocality::Local);
        assert_eq!(file_locality(Path::new("CURRENT")), FileLocality::Local);
    }

    // -- Local-only mode (no remote) -----------------------------------------

    #[test]
    fn test_local_only_sst_goes_to_local() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(Arc::clone(&local) as Arc<dyn FileSystem>);

        assert!(!router.has_remote());

        // Create directory and write an SST file — should go to local.
        local.create_dir_all(Path::new("/db")).unwrap();
        let mut w = router
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst-data").unwrap();
        drop(w);

        // Verify it's on the local filesystem.
        assert!(local.file_exists(Path::new("/db/000001.sst")).unwrap());
    }

    #[test]
    fn test_local_only_wal_goes_to_local() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(Arc::clone(&local) as Arc<dyn FileSystem>);

        local.create_dir_all(Path::new("/db")).unwrap();
        let mut w = router
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"wal-data").unwrap();
        drop(w);

        assert!(local.file_exists(Path::new("/db/000001.log")).unwrap());
    }

    // -- Tiered mode (local + remote) ----------------------------------------

    fn create_tiered_router() -> (
        Arc<MemoryFileSystem>,
        Arc<MemoryFileSystem>,
        FileSystemRouter,
    ) {
        let local = Arc::new(MemoryFileSystem::new());
        let remote = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::with_remote(
            Arc::clone(&local) as Arc<dyn FileSystem>,
            Arc::clone(&remote) as Arc<dyn FileSystem>,
        );
        (local, remote, router)
    }

    #[test]
    fn test_tiered_sst_goes_to_remote() {
        let (local, remote, router) = create_tiered_router();

        assert!(router.has_remote());

        // Create directories on both filesystems.
        local.create_dir_all(Path::new("/db")).unwrap();
        remote.create_dir_all(Path::new("/db")).unwrap();

        // Write an SST file via router.
        let mut w = router
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst-data").unwrap();
        drop(w);

        // SST should be on remote, NOT on local.
        assert!(remote.file_exists(Path::new("/db/000001.sst")).unwrap());
        assert!(!local.file_exists(Path::new("/db/000001.sst")).unwrap());
    }

    #[test]
    fn test_tiered_wal_stays_local() {
        let (local, remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db")).unwrap();
        remote.create_dir_all(Path::new("/db")).unwrap();

        // Write a WAL file via router.
        let mut w = router
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"wal-data").unwrap();
        drop(w);

        // WAL should be on local, NOT on remote.
        assert!(local.file_exists(Path::new("/db/000001.log")).unwrap());
        assert!(!remote.file_exists(Path::new("/db/000001.log")).unwrap());
    }

    #[test]
    fn test_tiered_manifest_stays_local() {
        let (local, _remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db")).unwrap();

        // Write a MANIFEST file via router.
        let mut w = router
            .open_writable_file(Path::new("/db/MANIFEST-000001"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"manifest-data").unwrap();
        drop(w);

        assert!(local.file_exists(Path::new("/db/MANIFEST-000001")).unwrap());
    }

    // -- Read operations via router ------------------------------------------

    #[test]
    fn test_tiered_sequential_read_sst_from_remote() {
        let (_local, remote, router) = create_tiered_router();

        remote.create_dir_all(Path::new("/db")).unwrap();

        // Write SST to remote directly.
        let mut w = remote
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst-content").unwrap();
        drop(w);

        // Read via router — should go to remote.
        let mut r = router
            .open_sequential_file(Path::new("/db/000001.sst"))
            .unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"sst-content");
    }

    #[test]
    fn test_tiered_random_access_read_sst_from_remote() {
        let (_local, remote, router) = create_tiered_router();

        remote.create_dir_all(Path::new("/db")).unwrap();

        let mut w = remote
            .open_writable_file(Path::new("/db/000002.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"ABCDEFGHIJ").unwrap();
        drop(w);

        let r = router
            .open_random_access_file(Path::new("/db/000002.sst"))
            .unwrap();
        assert_eq!(r.file_size().unwrap(), 10);

        let mut buf = [0u8; 3];
        let n = r.read_at(4, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"EFG");
    }

    #[test]
    fn test_tiered_sequential_read_wal_from_local() {
        let (local, _remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db")).unwrap();

        let mut w = local
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"wal-content").unwrap();
        drop(w);

        let mut r = router
            .open_sequential_file(Path::new("/db/000001.log"))
            .unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"wal-content");
    }

    // -- File metadata and existence -----------------------------------------

    #[test]
    fn test_tiered_file_exists_routes_correctly() {
        let (local, remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db")).unwrap();
        remote.create_dir_all(Path::new("/db")).unwrap();

        // Create WAL on local.
        let mut w = local
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"wal").unwrap();
        drop(w);

        // Create SST on remote.
        let mut w = remote
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst").unwrap();
        drop(w);

        // Router should find each on its respective filesystem.
        assert!(router.file_exists(Path::new("/db/000001.log")).unwrap());
        assert!(router.file_exists(Path::new("/db/000001.sst")).unwrap());

        // But not the other way around.
        assert!(!router.file_exists(Path::new("/db/000002.log")).unwrap());
        assert!(!router.file_exists(Path::new("/db/000002.sst")).unwrap());
    }

    #[test]
    fn test_tiered_get_file_metadata_routes_correctly() {
        let (_local, remote, router) = create_tiered_router();

        remote.create_dir_all(Path::new("/db")).unwrap();

        let mut w = remote
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"12345").unwrap();
        drop(w);

        let meta = router
            .get_file_metadata(Path::new("/db/000001.sst"))
            .unwrap();
        assert_eq!(meta.size, 5);
        assert!(!meta.is_dir);
    }

    // -- Delete and rename ---------------------------------------------------

    #[test]
    fn test_tiered_delete_sst_from_remote() {
        let (_local, remote, router) = create_tiered_router();

        remote.create_dir_all(Path::new("/db")).unwrap();

        let mut w = remote
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();
        drop(w);

        assert!(remote.file_exists(Path::new("/db/000001.sst")).unwrap());
        router.delete_file(Path::new("/db/000001.sst")).unwrap();
        assert!(!remote.file_exists(Path::new("/db/000001.sst")).unwrap());
    }

    #[test]
    fn test_tiered_delete_wal_from_local() {
        let (local, _remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db")).unwrap();

        let mut w = local
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();
        drop(w);

        assert!(local.file_exists(Path::new("/db/000001.log")).unwrap());
        router.delete_file(Path::new("/db/000001.log")).unwrap();
        assert!(!local.file_exists(Path::new("/db/000001.log")).unwrap());
    }

    #[test]
    fn test_tiered_rename_sst_goes_to_remote() {
        let (_local, remote, router) = create_tiered_router();

        remote.create_dir_all(Path::new("/db")).unwrap();

        let mut w = remote
            .open_writable_file(Path::new("/db/tmp.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst-data").unwrap();
        drop(w);

        // Rename within remote (dst is .sst so route goes to remote).
        router
            .rename(Path::new("/db/tmp.sst"), Path::new("/db/000001.sst"))
            .unwrap();

        assert!(!remote.file_exists(Path::new("/db/tmp.sst")).unwrap());
        assert!(remote.file_exists(Path::new("/db/000001.sst")).unwrap());
    }

    // -- Directory operations always go to local -----------------------------

    #[test]
    fn test_tiered_create_dir_always_local() {
        let (local, remote, router) = create_tiered_router();

        router.create_dir_all(Path::new("/db/subdir")).unwrap();

        // Should be on local.
        let meta = local.get_file_metadata(Path::new("/db/subdir")).unwrap();
        assert!(meta.is_dir);

        // Should NOT be on remote.
        assert!(remote.get_file_metadata(Path::new("/db/subdir")).is_err());
    }

    #[test]
    fn test_tiered_list_dir_from_local() {
        let (local, _remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db")).unwrap();

        let mut w = local
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();
        drop(w);

        let entries = router.list_dir(Path::new("/db")).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].path.file_name().unwrap().to_string_lossy(),
            "000001.log"
        );
    }

    #[test]
    fn test_tiered_delete_dir_from_local() {
        let (local, _remote, router) = create_tiered_router();

        local.create_dir_all(Path::new("/db/subdir")).unwrap();
        router.delete_dir(Path::new("/db/subdir"), false).unwrap();
        assert!(local.get_file_metadata(Path::new("/db/subdir")).is_err());
    }

    // -- Display and Debug ---------------------------------------------------

    #[test]
    fn test_display_local_only() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(local);
        assert_eq!(
            router.to_string(),
            "FileSystemRouter(local=MemoryFileSystem)"
        );
    }

    #[test]
    fn test_display_tiered() {
        let local = Arc::new(MemoryFileSystem::new());
        let remote = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::with_remote(
            local as Arc<dyn FileSystem>,
            remote as Arc<dyn FileSystem>,
        );
        assert_eq!(
            router.to_string(),
            "FileSystemRouter(local=MemoryFileSystem, remote=MemoryFileSystem)"
        );
    }

    #[test]
    fn test_debug_output() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(local);
        let debug = format!("{:?}", router);
        assert!(debug.contains("FileSystemRouter"));
    }

    #[test]
    fn test_name_returns_static_str() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(local);
        assert_eq!(router.name(), "FileSystemRouter");
    }

    #[test]
    fn test_display_name_method() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(local);
        assert_eq!(
            router.display_name(),
            "FileSystemRouter(local=MemoryFileSystem)"
        );
    }

    // -- Accessor methods ----------------------------------------------------

    #[test]
    fn test_local_fs_accessor() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(Arc::clone(&local) as Arc<dyn FileSystem>);
        assert_eq!(router.local_fs().name(), "MemoryFileSystem");
    }

    #[test]
    fn test_remote_fs_accessor_none() {
        let local = Arc::new(MemoryFileSystem::new());
        let router = FileSystemRouter::new(local);
        assert!(router.remote_fs().is_none());
    }

    #[test]
    fn test_remote_fs_accessor_some() {
        let (_, _, router) = create_tiered_router();
        assert!(router.remote_fs().is_some());
        assert_eq!(router.remote_fs().unwrap().name(), "MemoryFileSystem");
    }
}

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

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use forst_rs_common::error::{ForstError, ForstResult};

use crate::filesystem::{
    FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode,
};

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
    /// Optional scheme-prefix → filesystem registry.
    ///
    /// When a path starts with one of these prefixes (e.g., `s3://`,
    /// `opendal://`), the router strips the prefix and dispatches to the
    /// registered filesystem. This is independent of the legacy SST→remote
    /// extension routing (which still applies to non-prefixed paths) and
    /// is the recommended way to wire OpenDAL-backed services.
    scheme_fs: HashMap<String, Arc<dyn FileSystem>>,
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
            scheme_fs: HashMap::new(),
        }
    }

    /// Creates a router with both local and remote filesystems.
    ///
    /// SST files will be routed to `remote_fs`, and all other files to
    /// `local_fs`.
    pub fn with_remote(local_fs: Arc<dyn FileSystem>, remote_fs: Arc<dyn FileSystem>) -> Self {
        Self {
            local_fs,
            remote_fs: Some(remote_fs),
            scheme_fs: HashMap::new(),
        }
    }

    /// Registers a filesystem under a scheme prefix (e.g., `"s3://"`,
    /// `"opendal://"`).
    ///
    /// After registration, any path that starts with `prefix` is routed
    /// to `fs`, with the prefix stripped before being passed downstream.
    /// This is the recommended way to wire OpenDAL-backed remote stores
    /// alongside the local filesystem.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    /// use forst_rs_io::filesystem::FileSystem;
    /// use forst_rs_io::memory_fs::MemoryFileSystem;
    /// use forst_rs_io::opendal_backend::OpendalFileSystem;
    /// use forst_rs_io::router::FileSystemRouter;
    ///
    /// let local = Arc::new(MemoryFileSystem::new());
    /// let mut router = FileSystemRouter::new(local);
    /// let remote = Arc::new(OpendalFileSystem::memory().unwrap());
    /// router.register_scheme("opendal://", remote);
    /// ```
    pub fn register_scheme(&mut self, prefix: &str, fs: Arc<dyn FileSystem>) {
        self.scheme_fs.insert(prefix.to_string(), fs);
    }

    /// Returns the registered filesystem for `prefix`, if any.
    pub fn scheme_fs(&self, prefix: &str) -> Option<&Arc<dyn FileSystem>> {
        self.scheme_fs.get(prefix)
    }

    /// If `path` starts with any registered scheme prefix, returns
    /// `(filesystem, stripped_path)`. Otherwise returns `None`.
    fn match_scheme<'a>(&'a self, path: &Path) -> Option<(&'a Arc<dyn FileSystem>, PathBuf)> {
        let s = path.to_str()?;
        for (prefix, fs) in &self.scheme_fs {
            if let Some(rest) = s.strip_prefix(prefix.as_str()) {
                return Some((fs, PathBuf::from(rest)));
            }
        }
        None
    }

    /// Returns `true` if the file at `path` should be stored remotely.
    ///
    /// Recognises both the final `<N>.sst` form and every mid-write /
    /// post-mortem tmp variant we ship today:
    ///
    /// * `<N>.sst`                       — final SST (extension `.sst`)
    /// * `.<N>.sst.tmp`                  — flush + compaction tmp
    ///   (`flush::sst_temp_path`)
    /// * `<N>.sst.cf-rewrite.tmp`        — `rewrite_sst_footer` tmp
    /// * `<N>.sst.orphan-<ts>`           — restore-rename of an orphan
    /// * `.<N>.sst.tmp.orphan-<ts>`      — restore-rename of a tmp orphan
    ///
    /// Implementation: a path routes remote iff its final extension is
    /// `.sst` OR `.sst` appears as a non-final extension (i.e. the path
    /// string contains `.sst.`). This is the **R52-H2/H3** fix — without
    /// it, `Router::rename` rejects the `<tmp>.tmp → <final>.sst`
    /// rename as a cross-fs move in tiered mode because the source
    /// extension is `.tmp` (→ local) while the destination is `.sst`
    /// (→ remote), even though both files belong on the remote FS.
    fn is_remote_file(path: &Path) -> bool {
        if path.extension().is_some_and(|ext| ext == "sst") {
            return true;
        }
        // Tmp / orphan shapes carry `.sst.` as a non-terminal component.
        // Use raw string match so we catch every suffix the writer side
        // may invent (`.tmp`, `.cf-rewrite.tmp`, `.orphan-<ts>`, …)
        // without enumerating each one here.
        path.to_string_lossy().contains(".sst.")
    }

    /// Returns the filesystem that should handle operations for `path`.
    ///
    /// If no remote filesystem is configured, always returns the local
    /// filesystem regardless of file type.
    fn route(&self, path: &Path) -> &dyn FileSystem {
        if Self::is_remote_file(path) {
            self.remote_fs.as_deref().unwrap_or(self.local_fs.as_ref())
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
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.open_sequential_file(&stripped);
        }
        self.route(path).open_sequential_file(path)
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.open_random_access_file(&stripped);
        }
        self.route(path).open_random_access_file(path)
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.open_writable_file(&stripped, mode);
        }
        self.route(path).open_writable_file(path, mode)
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.file_exists(&stripped);
        }
        self.route(path).file_exists(path)
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.get_file_metadata(&stripped);
        }
        self.route(path).get_file_metadata(path)
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        if let Some((fs, stripped)) = self.match_scheme(dir) {
            return fs.list_dir(&stripped);
        }
        // R52-M2: directories live on the local FS (WAL dir, db dir,
        // checkpoint dir, …) so we always list the local leg. In TIERED
        // mode, SST writes (and their tmp / orphan variants) land on the
        // REMOTE leg per `is_remote_file`, and the engine's
        // `open_from_checkpoint` orphan-scan calls `fs.list_dir(&db_path)`
        // to find stranded files left behind by a crashed flush /
        // compaction / rewrite. Without merging the remote leg in, the
        // orphan-scan never sees remote-resident orphans and the next
        // restore's `next_file_number` is computed off the local leg only
        // — a brand-new flush can then collide with an existing remote
        // SST number.
        //
        // We merge both listings, dedup by path (exact match), and prefer
        // the remote-side `FileMetadata` when both backends report the
        // same entry (in normal operation the same path only appears on
        // one leg, but the dedup keeps the union well-defined for the
        // shared-Arc and the dev-mode single-FS configurations).
        //
        // A remote-leg `list_dir` failure is best-effort: object-store
        // backends sometimes return `NotFound` for an empty / yet-to-be-
        // created prefix. We log and continue with just the local leg
        // rather than failing the open.
        let local_entries = self.local_fs.list_dir(dir)?;
        let Some(remote) = self.remote_fs.as_ref() else {
            return Ok(local_entries);
        };
        // Shared-Arc case: avoid double-listing the same backend.
        if Arc::ptr_eq(&self.local_fs, remote) {
            return Ok(local_entries);
        }
        let remote_entries = match remote.list_dir(dir) {
            Ok(v) => v,
            Err(e) => {
                // Object stores routinely surface "directory does not
                // exist" as an error; treat as empty rather than fatal.
                tracing::debug!(
                    "Router::list_dir remote leg {} failed: {} \
                     (continuing with local-only listing)",
                    dir.display(),
                    e
                );
                return Ok(local_entries);
            }
        };
        let mut merged: Vec<FileMetadata> =
            Vec::with_capacity(local_entries.len() + remote_entries.len());
        let mut seen: std::collections::HashSet<PathBuf> =
            std::collections::HashSet::with_capacity(local_entries.len() + remote_entries.len());
        for e in local_entries {
            if seen.insert(e.path.clone()) {
                merged.push(e);
            }
        }
        for e in remote_entries {
            if seen.insert(e.path.clone()) {
                merged.push(e);
            }
        }
        Ok(merged)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        if let Some((fs, stripped)) = self.match_scheme(dir) {
            return fs.create_dir_all(&stripped);
        }
        // Directories are always local; remote object stores typically
        // do not have real directories.
        self.local_fs.create_dir_all(dir)
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.delete_file(&stripped);
        }
        self.route(path).delete_file(path)
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.delete_dir(&stripped, recursive);
        }
        // Directories are always local.
        self.local_fs.delete_dir(path, recursive)
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        // Scheme-prefixed renames must stay within the same registered
        // filesystem (we cannot move bytes across backends in one op).
        let src_match = self.match_scheme(src);
        let dst_match = self.match_scheme(dst);
        match (src_match, dst_match) {
            (Some((sfs, ssrc)), Some((dfs, sdst))) => {
                if !Arc::ptr_eq(sfs, dfs) {
                    return Err(ForstError::invalid_argument(format!(
                        "cannot rename across registered schemes: src={} dst={}",
                        src.display(),
                        dst.display(),
                    )));
                }
                return sfs.rename(&ssrc, &sdst);
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ForstError::invalid_argument(format!(
                    "cannot rename across filesystems (scheme vs unscheme): src={} dst={}",
                    src.display(),
                    dst.display(),
                )));
            }
            (None, None) => {}
        }

        // Both src and dst must be on the same filesystem.
        let src_remote = Self::is_remote_file(src);
        let dst_remote = Self::is_remote_file(dst);
        if src_remote != dst_remote {
            return Err(ForstError::invalid_argument(format!(
                "cannot rename across filesystems: src={} ({}), dst={} ({})",
                src.display(),
                if src_remote { "remote" } else { "local" },
                dst.display(),
                if dst_remote { "remote" } else { "local" },
            )));
        }
        self.route(dst).rename(src, dst)
    }

    /// R50-H1: route `sync_dir` to the underlying filesystem instead of
    /// falling to the trait's default no-op. Without this override, the
    /// engine's R49-H3 `sync_dir(parent)` after a rename silently no-ops
    /// even when the underlying backend is `LocalFileSystem` — the rename
    /// would survive but its directory-entry update would not, defeating
    /// the entire crash-safety contract.
    ///
    /// R51-M1: in tiered mode, the engine calls `sync_dir(parent_of_sst)`
    /// after a remote SST rename. Parent directories have no `.sst`
    /// extension, so `is_remote_file` returns `false` and `route()`
    /// dispatches to `local_fs` — fsyncing the WRONG filesystem and
    /// leaving the remote rename's directory entry un-synced. Fix: when
    /// a remote filesystem is configured, fan the call out to BOTH the
    /// local and the remote filesystems. Object-store backends implement
    /// `sync_dir` as a no-op (no kernel-cached directory entry), so the
    /// extra leg is essentially free; a wrapped POSIX remote (e.g. NFS
    /// behind OpenDAL) honours it correctly. Scheme-prefixed paths still
    /// dispatch through the registry first.
    fn sync_dir(&self, dir: &Path) -> ForstResult<()> {
        if let Some((fs, stripped)) = self.match_scheme(dir) {
            return fs.sync_dir(&stripped);
        }
        // Fan-out: always fsync local (directories live there) and, if
        // configured, also fsync remote so SST renames on the remote leg
        // become durable. Order matters only for error propagation —
        // local first, since that's where the directory entries the
        // engine actually relies on for crash recovery live.
        self.local_fs.sync_dir(dir)?;
        // R52-M1: only fan out to the remote leg when it is a DIFFERENT
        // Arc from the local leg. When the engine is configured with a
        // single shared backend (e.g. a single `LocalFileSystem` for both
        // local and remote slots in dev / single-tier mode), the second
        // `sync_dir` would fsync the exact same path again — wasted
        // syscall and, more importantly, doubles the error surface
        // (a transient EIO on the second call masquerades as a
        // remote-leg failure when the directory is local).
        if let Some(remote) = self.remote_fs.as_ref() {
            if !Arc::ptr_eq(&self.local_fs, remote) {
                remote.sync_dir(dir)?;
            }
        }
        Ok(())
    }

    /// R51-M2: dispatch `ensure_cached` to the underlying filesystem
    /// that owns `path` instead of falling through to the trait default
    /// no-op. The engine calls this to prefetch SST files into the local
    /// cache before opening readers (amortising S3 round-trip latency).
    /// Without an override, the call silently no-ops in tiered mode and
    /// every reader open still pays the full remote-fetch cost.
    ///
    /// Routing mirrors the read path: scheme prefix wins, then the
    /// extension-based local/remote split. Local files are no-ops via
    /// the trait default; remote files (in particular `CachedFileSystem`-
    /// wrapped remotes) actually populate the local cache.
    fn ensure_cached(&self, path: &Path) -> ForstResult<()> {
        if let Some((fs, stripped)) = self.match_scheme(path) {
            return fs.ensure_cached(&stripped);
        }
        self.route(path).ensure_cached(path)
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
            None => write!(f, "FileSystemRouter(local={})", self.local_fs.name()),
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
/// `FileSystemRouter::is_remote_file` (private internal helper).
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
        assert!(FileSystemRouter::is_remote_file(Path::new(
            "/data/000042.sst"
        )));
        assert!(FileSystemRouter::is_remote_file(Path::new("table.sst")));
    }

    // R52-H2/H3: every tmp / orphan variant routes remote in tiered mode.
    #[test]
    fn test_is_remote_file_sst_tmp_variants() {
        // .<N>.sst.tmp — flush + compaction tmp (sst_temp_path)
        assert!(FileSystemRouter::is_remote_file(Path::new(
            "/db/.000042.sst.tmp"
        )));
        // <N>.sst.cf-rewrite.tmp — rewrite_sst_footer tmp
        assert!(FileSystemRouter::is_remote_file(Path::new(
            "/db/000042.sst.cf-rewrite.tmp"
        )));
        // <N>.sst.orphan-<ts> — restore-rename of a stranded SST
        assert!(FileSystemRouter::is_remote_file(Path::new(
            "/db/000042.sst.orphan-1234567890"
        )));
        // .<N>.sst.tmp.orphan-<ts> — restore-rename of a stranded tmp
        assert!(FileSystemRouter::is_remote_file(Path::new(
            "/db/.000042.sst.tmp.orphan-1234567890"
        )));
    }

    // R52-H2/H3: rename a tmp file (`.<N>.sst.tmp`) → final (`<N>.sst`)
    // must succeed in tiered mode because both forms route remote.
    #[test]
    fn test_tiered_rename_sst_tmp_to_final_succeeds() {
        let (_local, remote, router) = create_tiered_router();
        remote.create_dir_all(Path::new("/db")).unwrap();

        // Write the tmp file via the router — must land on remote.
        let mut w = router
            .open_writable_file(Path::new("/db/.000001.sst.tmp"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst-bytes").unwrap();
        drop(w);
        assert!(remote.file_exists(Path::new("/db/.000001.sst.tmp")).unwrap());

        // Rename tmp → final (the path the flush job actually drives).
        router
            .rename(
                Path::new("/db/.000001.sst.tmp"),
                Path::new("/db/000001.sst"),
            )
            .unwrap();
        assert!(!remote
            .file_exists(Path::new("/db/.000001.sst.tmp"))
            .unwrap());
        assert!(remote.file_exists(Path::new("/db/000001.sst")).unwrap());
    }

    #[test]
    fn test_is_remote_file_non_sst() {
        assert!(!FileSystemRouter::is_remote_file(Path::new(
            "/wal/000001.log"
        )));
        assert!(!FileSystemRouter::is_remote_file(Path::new(
            "MANIFEST-000001"
        )));
        assert!(!FileSystemRouter::is_remote_file(Path::new("CURRENT")));
        assert!(!FileSystemRouter::is_remote_file(Path::new(
            "OPTIONS-000001"
        )));
        assert!(!FileSystemRouter::is_remote_file(Path::new("LOCK")));
        assert!(!FileSystemRouter::is_remote_file(Path::new("/db/IDENTITY")));
    }

    #[test]
    fn test_is_remote_file_no_extension() {
        assert!(!FileSystemRouter::is_remote_file(Path::new("CURRENT")));
        assert!(!FileSystemRouter::is_remote_file(Path::new(
            "/path/to/MANIFEST"
        )));
    }

    #[test]
    fn test_file_locality_function() {
        assert_eq!(file_locality(Path::new("000001.sst")), FileLocality::Remote);
        assert_eq!(file_locality(Path::new("000001.log")), FileLocality::Local);
        assert_eq!(
            file_locality(Path::new("MANIFEST-000001")),
            FileLocality::Local
        );
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

    // R52-M2: tiered `list_dir` must union local + remote so the engine's
    // open_from_checkpoint orphan-scan sees stranded SSTs on the remote leg.
    #[test]
    fn test_tiered_list_dir_unions_local_and_remote() {
        let (local, remote, router) = create_tiered_router();
        local.create_dir_all(Path::new("/db")).unwrap();
        remote.create_dir_all(Path::new("/db")).unwrap();

        // WAL on local.
        let mut w = local
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"wal").unwrap();
        drop(w);
        // SST + tmp on remote.
        let mut w = remote
            .open_writable_file(Path::new("/db/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"sst").unwrap();
        drop(w);
        let mut w = remote
            .open_writable_file(Path::new("/db/.000002.sst.tmp"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"tmp").unwrap();
        drop(w);

        let entries = router.list_dir(Path::new("/db")).unwrap();
        let names: std::collections::HashSet<String> = entries
            .iter()
            .map(|e| e.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains("000001.log"), "got {:?}", names);
        assert!(names.contains("000001.sst"), "got {:?}", names);
        assert!(names.contains(".000002.sst.tmp"), "got {:?}", names);
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

    // -- Scheme registry -----------------------------------------------------

    #[test]
    fn test_register_scheme_routes_writes_and_reads() {
        let local = Arc::new(MemoryFileSystem::new());
        let remote = Arc::new(MemoryFileSystem::new());
        let mut router = FileSystemRouter::new(Arc::clone(&local) as Arc<dyn FileSystem>);
        router.register_scheme("opendal://", Arc::clone(&remote) as Arc<dyn FileSystem>);

        // Pre-create the target directory on the remote so
        // MemoryFileSystem accepts the write (mirrors POSIX).
        remote.create_dir_all(Path::new("data")).unwrap();

        // Write through the scheme prefix.
        let mut w = router
            .open_writable_file(Path::new("opendal://data/000001.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"remote-via-scheme").unwrap();
        drop(w);

        // The bytes live on the registered remote (without the prefix).
        assert!(remote.file_exists(Path::new("data/000001.sst")).unwrap());
        // And NOT on the local filesystem under either name.
        assert!(!local
            .file_exists(Path::new("opendal://data/000001.sst"))
            .unwrap());
        assert!(!local.file_exists(Path::new("data/000001.sst")).unwrap());

        // Read back through the scheme prefix.
        let mut r = router
            .open_sequential_file(Path::new("opendal://data/000001.sst"))
            .unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"remote-via-scheme");
    }

    #[test]
    fn test_register_scheme_accessor() {
        let local = Arc::new(MemoryFileSystem::new());
        let remote = Arc::new(MemoryFileSystem::new());
        let mut router = FileSystemRouter::new(local);
        router.register_scheme("s3://", Arc::clone(&remote) as Arc<dyn FileSystem>);
        assert!(router.scheme_fs("s3://").is_some());
        assert!(router.scheme_fs("gs://").is_none());
    }

    #[test]
    fn test_cross_filesystem_rename_rejected() {
        let (local, _remote, router) = create_tiered_router();
        local.create_dir_all(Path::new("/db")).unwrap();

        // Create a local .log file
        let mut w = local
            .open_writable_file(Path::new("/db/000001.log"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"wal-data").unwrap();
        drop(w);

        // Try to rename .log (local) → .sst (remote) — should fail
        let result = router.rename(Path::new("/db/000001.log"), Path::new("/db/000001.sst"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            format!("{}", err).contains("cannot rename across filesystems"),
            "unexpected error: {}",
            err
        );
    }
}

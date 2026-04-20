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

//! In-memory filesystem backend for ForSt-RS.
//!
//! Provides [`MemoryFileSystem`], a fully in-memory filesystem implementation
//! intended for use in tests. All data is stored in a shared
//! `Arc<Mutex<HashMap<PathBuf, FileEntry>>>`.
//!
//! File types:
//! - [`MemorySequentialFile`]: Cursor-based sequential reads from `Vec<u8>`.
//! - [`MemoryRandomAccessFile`]: Random reads from a shared `Arc<Vec<u8>>`.
//! - [`MemoryWritableFile`]: Appends to a shared `Arc<Mutex<Vec<u8>>>`.

use std::collections::HashMap;
use std::io::Cursor;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use forst_rs_common::error::{ForstError, ForstResult};

use crate::filesystem::{
    FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode,
};

// ---------------------------------------------------------------------------
// FileEntry — the in-memory storage element
// ---------------------------------------------------------------------------

/// An entry in the in-memory filesystem: either a file or a directory.
#[derive(Clone, Debug)]
enum FileEntry {
    /// A file whose contents are stored behind a shared, lockable buffer.
    File(Arc<Mutex<Vec<u8>>>),
    /// A directory (has no data of its own; existence is tracked by the map key).
    Directory,
}

// ---------------------------------------------------------------------------
// Helper: canonical path
// ---------------------------------------------------------------------------

/// Normalise a path by resolving `.` and `..` components without touching the
/// real filesystem (we cannot use `std::fs::canonicalize` because the paths
/// are virtual).
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => {
                components.pop();
            }
            std::path::Component::CurDir => {}
            other => components.push(other),
        }
    }
    components.iter().collect()
}

// ---------------------------------------------------------------------------
// MemorySequentialFile
// ---------------------------------------------------------------------------

/// A sequential (forward-only) file backed by a `Cursor<Vec<u8>>`.
pub struct MemorySequentialFile {
    cursor: Cursor<Vec<u8>>,
}

impl SequentialFile for MemorySequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        Read::read(&mut self.cursor, buf).map_err(ForstError::Io)
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        let new_pos = self.cursor.position().saturating_add(n);
        self.cursor.set_position(new_pos);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MemoryRandomAccessFile
// ---------------------------------------------------------------------------

/// A random-access file backed by a shared, immutable snapshot (`Arc<Vec<u8>>`).
///
/// Because the underlying data is an immutable `Vec<u8>` behind an `Arc`,
/// concurrent `read_at` calls are lock-free.
pub struct MemoryRandomAccessFile {
    data: Arc<Vec<u8>>,
}

impl RandomAccessFile for MemoryRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let offset = offset as usize;
        if offset >= self.data.len() {
            return Ok(0);
        }
        let available = &self.data[offset..];
        let to_copy = std::cmp::min(available.len(), buf.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        Ok(to_copy)
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.data.len() as u64)
    }
}

// ---------------------------------------------------------------------------
// MemoryWritableFile
// ---------------------------------------------------------------------------

/// A writable file that appends to a shared `Arc<Mutex<Vec<u8>>>` so that
/// the written bytes are visible to other handles opened after the write.
pub struct MemoryWritableFile {
    data: Arc<Mutex<Vec<u8>>>,
    bytes_written: u64,
}

impl WritableFile for MemoryWritableFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        let mut buf = self
            .data
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        buf.extend_from_slice(data);
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        // No-op for in-memory storage.
        Ok(())
    }

    fn sync(&mut self) -> ForstResult<()> {
        // No-op for in-memory storage.
        Ok(())
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.bytes_written)
    }
}

// ---------------------------------------------------------------------------
// MemoryFileSystem
// ---------------------------------------------------------------------------

/// A fully in-memory [`FileSystem`] implementation for testing.
///
/// Thread-safe: the inner state is protected by a `Mutex` and the struct is
/// `Send + Sync` via `Arc`.
///
/// # Example
///
/// ```
/// use forst_rs_io::memory_fs::MemoryFileSystem;
/// use forst_rs_io::filesystem::{FileSystem, WriteMode};
/// use std::path::Path;
///
/// let fs = MemoryFileSystem::new();
/// fs.create_dir_all(Path::new("/data")).unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct MemoryFileSystem {
    entries: Arc<Mutex<HashMap<PathBuf, FileEntry>>>,
}

impl MemoryFileSystem {
    /// Creates a new, empty `MemoryFileSystem` with a root directory.
    pub fn new() -> Self {
        let mut map = HashMap::new();
        // Seed with root directory.
        map.insert(PathBuf::from("/"), FileEntry::Directory);
        Self {
            entries: Arc::new(Mutex::new(map)),
        }
    }
}

impl Default for MemoryFileSystem {
    fn default() -> Self {
        Self::new()
    }
}

/// Acquires the inner lock, mapping a poisoned-mutex error to `ForstError`.
fn lock_entries(
    entries: &Mutex<HashMap<PathBuf, FileEntry>>,
) -> ForstResult<std::sync::MutexGuard<'_, HashMap<PathBuf, FileEntry>>> {
    entries
        .lock()
        .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))
}

impl FileSystem for MemoryFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let path = normalize_path(path);
        let map = lock_entries(&self.entries)?;
        match map.get(&path) {
            Some(FileEntry::File(data)) => {
                let buf = data
                    .lock()
                    .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
                Ok(Box::new(MemorySequentialFile {
                    cursor: Cursor::new(buf.clone()),
                }))
            }
            Some(FileEntry::Directory) => Err(ForstError::invalid_argument(format!(
                "open_sequential_file: {} is a directory",
                path.display()
            ))),
            None => Err(ForstError::not_found(format!(
                "open_sequential_file: {}",
                path.display()
            ))),
        }
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let path = normalize_path(path);
        let map = lock_entries(&self.entries)?;
        match map.get(&path) {
            Some(FileEntry::File(data)) => {
                let buf = data
                    .lock()
                    .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
                Ok(Box::new(MemoryRandomAccessFile {
                    data: Arc::new(buf.clone()),
                }))
            }
            Some(FileEntry::Directory) => Err(ForstError::invalid_argument(format!(
                "open_random_access_file: {} is a directory",
                path.display()
            ))),
            None => Err(ForstError::not_found(format!(
                "open_random_access_file: {}",
                path.display()
            ))),
        }
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let path = normalize_path(path);
        let mut map = lock_entries(&self.entries)?;

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            if !map.contains_key(&normalize_path(parent)) {
                return Err(ForstError::not_found(format!(
                    "open_writable_file: parent directory does not exist: {}",
                    parent.display()
                )));
            }
        }

        match mode {
            WriteMode::CreateNew => {
                if map.contains_key(&path) {
                    return Err(ForstError::invalid_argument(format!(
                        "open_writable_file: file already exists: {}",
                        path.display()
                    )));
                }
                let data = Arc::new(Mutex::new(Vec::new()));
                map.insert(path, FileEntry::File(Arc::clone(&data)));
                Ok(Box::new(MemoryWritableFile {
                    data,
                    bytes_written: 0,
                }))
            }
            WriteMode::CreateOrTruncate => {
                let data = Arc::new(Mutex::new(Vec::new()));
                map.insert(path, FileEntry::File(Arc::clone(&data)));
                Ok(Box::new(MemoryWritableFile {
                    data,
                    bytes_written: 0,
                }))
            }
            WriteMode::Append => {
                let (data, initial_size) = match map.get(&path) {
                    Some(FileEntry::File(existing)) => {
                        let size = existing
                            .lock()
                            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?
                            .len() as u64;
                        (Arc::clone(existing), size)
                    }
                    Some(FileEntry::Directory) => {
                        return Err(ForstError::invalid_argument(format!(
                            "open_writable_file: {} is a directory",
                            path.display()
                        )));
                    }
                    None => {
                        let data = Arc::new(Mutex::new(Vec::new()));
                        map.insert(path, FileEntry::File(Arc::clone(&data)));
                        (data, 0)
                    }
                };
                Ok(Box::new(MemoryWritableFile {
                    data,
                    bytes_written: initial_size,
                }))
            }
        }
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        let path = normalize_path(path);
        let map = lock_entries(&self.entries)?;
        Ok(matches!(map.get(&path), Some(FileEntry::File(_))))
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        let path = normalize_path(path);
        let map = lock_entries(&self.entries)?;
        match map.get(&path) {
            Some(FileEntry::File(data)) => {
                let size = data
                    .lock()
                    .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?
                    .len() as u64;
                Ok(FileMetadata {
                    path: path.clone(),
                    size,
                    is_dir: false,
                })
            }
            Some(FileEntry::Directory) => Ok(FileMetadata {
                path: path.clone(),
                size: 0,
                is_dir: true,
            }),
            None => Err(ForstError::not_found(format!(
                "get_file_metadata: {}",
                path.display()
            ))),
        }
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        let dir = normalize_path(dir);
        let map = lock_entries(&self.entries)?;

        // Verify the directory exists.
        match map.get(&dir) {
            Some(FileEntry::Directory) => {}
            Some(FileEntry::File(_)) => {
                return Err(ForstError::invalid_argument(format!(
                    "list_dir: {} is not a directory",
                    dir.display()
                )));
            }
            None => {
                return Err(ForstError::not_found(format!(
                    "list_dir: {}",
                    dir.display()
                )));
            }
        }

        let mut result = Vec::new();
        for (entry_path, entry) in map.iter() {
            // Direct children: parent matches and it's not the directory itself.
            if entry_path != &dir && entry_path.parent() == Some(&dir) {
                match entry {
                    FileEntry::File(data) => {
                        let size = data
                            .lock()
                            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?
                            .len() as u64;
                        result.push(FileMetadata {
                            path: entry_path.clone(),
                            size,
                            is_dir: false,
                        });
                    }
                    FileEntry::Directory => {
                        result.push(FileMetadata {
                            path: entry_path.clone(),
                            size: 0,
                            is_dir: true,
                        });
                    }
                }
            }
        }
        // Sort for deterministic ordering.
        result.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(result)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        let dir = normalize_path(dir);
        let mut map = lock_entries(&self.entries)?;

        // Build the list of ancestor directories that need to be created.
        let mut to_create = Vec::new();
        let mut current = dir.as_path();
        loop {
            let current_norm = normalize_path(current);
            match map.get(&current_norm) {
                Some(FileEntry::Directory) => break,
                Some(FileEntry::File(_)) => {
                    return Err(ForstError::invalid_argument(format!(
                        "create_dir_all: {} exists and is a file",
                        current_norm.display()
                    )));
                }
                None => {
                    to_create.push(current_norm);
                }
            }
            match current.parent() {
                Some(parent) if parent != current => current = parent,
                _ => break,
            }
        }

        for p in to_create.into_iter().rev() {
            map.insert(p, FileEntry::Directory);
        }
        Ok(())
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        let path = normalize_path(path);
        let mut map = lock_entries(&self.entries)?;
        match map.get(&path) {
            Some(FileEntry::File(_)) => {
                map.remove(&path);
                Ok(())
            }
            Some(FileEntry::Directory) => Err(ForstError::invalid_argument(format!(
                "delete_file: {} is a directory",
                path.display()
            ))),
            None => Err(ForstError::not_found(format!(
                "delete_file: {}",
                path.display()
            ))),
        }
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        let path = normalize_path(path);
        let mut map = lock_entries(&self.entries)?;

        match map.get(&path) {
            Some(FileEntry::Directory) => {}
            Some(FileEntry::File(_)) => {
                return Err(ForstError::invalid_argument(format!(
                    "delete_dir: {} is not a directory",
                    path.display()
                )));
            }
            None => {
                return Err(ForstError::not_found(format!(
                    "delete_dir: {}",
                    path.display()
                )));
            }
        }

        // Collect children.
        let children: Vec<PathBuf> = map
            .keys()
            .filter(|k| *k != &path && k.starts_with(&path))
            .cloned()
            .collect();

        if !recursive && !children.is_empty() {
            return Err(ForstError::Io(std::io::Error::other(format!(
                "delete_dir: {} is not empty",
                path.display()
            ))));
        }

        for child in children {
            map.remove(&child);
        }
        map.remove(&path);
        Ok(())
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        let src = normalize_path(src);
        let dst = normalize_path(dst);
        let mut map = lock_entries(&self.entries)?;

        // Ensure destination parent exists.
        if let Some(parent) = dst.parent() {
            let parent_norm = normalize_path(parent);
            if !matches!(map.get(&parent_norm), Some(FileEntry::Directory)) {
                return Err(ForstError::not_found(format!(
                    "rename: destination parent does not exist: {}",
                    parent_norm.display()
                )));
            }
        }

        match map.remove(&src) {
            Some(entry) => {
                map.insert(dst, entry);
                Ok(())
            }
            None => Err(ForstError::not_found(format!(
                "rename: source not found: {}",
                src.display()
            ))),
        }
    }

    fn name(&self) -> &str {
        "MemoryFileSystem"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn create_fs() -> MemoryFileSystem {
        MemoryFileSystem::new()
    }

    // -- Directory operations ------------------------------------------------

    #[test]
    fn test_create_dir_all_and_list_dir() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/a/b/c")).unwrap();

        let entries = fs.list_dir(Path::new("/a/b")).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, PathBuf::from("/a/b/c"));
        assert!(entries[0].is_dir);
    }

    #[test]
    fn test_create_dir_all_idempotent() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/x/y")).unwrap();
        // Calling again should succeed without error.
        fs.create_dir_all(Path::new("/x/y")).unwrap();
    }

    #[test]
    fn test_create_dir_all_over_existing_file_fails() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/d")).unwrap();
        // Create a file at /d/f
        let mut w = fs
            .open_writable_file(Path::new("/d/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();

        // Trying to create a directory at /d/f/sub should fail.
        let err = fs.create_dir_all(Path::new("/d/f/sub"));
        assert!(err.is_err());
        assert!(err.unwrap_err().is_invalid_argument());
    }

    #[test]
    fn test_list_dir_not_found() {
        let fs = create_fs();
        let result = fs.list_dir(Path::new("/nonexistent"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_delete_dir_empty() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/rmdir")).unwrap();
        fs.delete_dir(Path::new("/rmdir"), false).unwrap();

        let result = fs.get_file_metadata(Path::new("/rmdir"));
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_dir_non_empty_fails() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/parent/child")).unwrap();

        let result = fs.delete_dir(Path::new("/parent"), false);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_dir_recursive() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/r/s/t")).unwrap();
        let mut w = fs
            .open_writable_file(Path::new("/r/s/file.txt"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"hello").unwrap();

        fs.delete_dir(Path::new("/r"), true).unwrap();

        assert!(fs.get_file_metadata(Path::new("/r")).is_err());
        assert!(fs.get_file_metadata(Path::new("/r/s")).is_err());
        assert!(fs.get_file_metadata(Path::new("/r/s/t")).is_err());
        assert!(!fs.file_exists(Path::new("/r/s/file.txt")).unwrap());
    }

    // -- File operations -----------------------------------------------------

    #[test]
    fn test_write_and_read_sequential() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/data")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/data/test.sst"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"hello world").unwrap();
        w.flush().unwrap();
        w.sync().unwrap();
        assert_eq!(w.file_size().unwrap(), 11);
        drop(w);

        let mut r = fs
            .open_sequential_file(Path::new("/data/test.sst"))
            .unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf[..n], b"hello world");

        // EOF
        let n2 = r.read(&mut buf).unwrap();
        assert_eq!(n2, 0);
    }

    #[test]
    fn test_sequential_skip() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/sk")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/sk/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"0123456789").unwrap();
        drop(w);

        let mut r = fs.open_sequential_file(Path::new("/sk/f")).unwrap();
        r.skip(5).unwrap();
        let mut buf = [0u8; 5];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"56789");
    }

    #[test]
    fn test_random_access_read() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/ra")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/ra/data.dat"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"ABCDEFGHIJ").unwrap();
        drop(w);

        let r = fs
            .open_random_access_file(Path::new("/ra/data.dat"))
            .unwrap();
        assert_eq!(r.file_size().unwrap(), 10);

        let mut buf = [0u8; 3];
        let n = r.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"ABC");

        let n = r.read_at(7, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"HIJ");

        // Beyond EOF
        let n = r.read_at(100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_create_new_already_exists() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/dup")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/dup/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"x").unwrap();
        drop(w);

        let result = fs.open_writable_file(Path::new("/dup/f"), WriteMode::CreateNew);
        match result {
            Err(e) => assert!(e.is_invalid_argument()),
            Ok(_) => panic!("expected InvalidArgument error"),
        }
    }

    #[test]
    fn test_create_or_truncate() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/trunc")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/trunc/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"old data that is long").unwrap();
        drop(w);

        let mut w = fs
            .open_writable_file(Path::new("/trunc/f"), WriteMode::CreateOrTruncate)
            .unwrap();
        w.append(b"new").unwrap();
        drop(w);

        let mut r = fs.open_sequential_file(Path::new("/trunc/f")).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"new");
    }

    #[test]
    fn test_append_mode() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/app")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/app/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"first").unwrap();
        drop(w);

        let mut w = fs
            .open_writable_file(Path::new("/app/f"), WriteMode::Append)
            .unwrap();
        assert_eq!(w.file_size().unwrap(), 5);
        w.append(b"second").unwrap();
        assert_eq!(w.file_size().unwrap(), 11);
        drop(w);

        let mut r = fs.open_sequential_file(Path::new("/app/f")).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"firstsecond");
    }

    #[test]
    fn test_file_exists() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/ex")).unwrap();

        assert!(!fs.file_exists(Path::new("/ex/nope")).unwrap());

        let mut w = fs
            .open_writable_file(Path::new("/ex/yes"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();
        drop(w);

        assert!(fs.file_exists(Path::new("/ex/yes")).unwrap());
        // Directories are not files.
        assert!(!fs.file_exists(Path::new("/ex")).unwrap());
    }

    #[test]
    fn test_get_file_metadata() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/meta")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/meta/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"12345").unwrap();
        drop(w);

        let meta = fs.get_file_metadata(Path::new("/meta/f")).unwrap();
        assert_eq!(meta.size, 5);
        assert!(!meta.is_dir);

        let dir_meta = fs.get_file_metadata(Path::new("/meta")).unwrap();
        assert!(dir_meta.is_dir);
        assert_eq!(dir_meta.size, 0);

        // Not found
        let result = fs.get_file_metadata(Path::new("/meta/nope"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_delete_file() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/del")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/del/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();
        drop(w);

        assert!(fs.file_exists(Path::new("/del/f")).unwrap());
        fs.delete_file(Path::new("/del/f")).unwrap();
        assert!(!fs.file_exists(Path::new("/del/f")).unwrap());
    }

    #[test]
    fn test_delete_file_not_found() {
        let fs = create_fs();
        let result = fs.delete_file(Path::new("/nope"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_rename() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/ren")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/ren/src"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"payload").unwrap();
        drop(w);

        fs.rename(Path::new("/ren/src"), Path::new("/ren/dst"))
            .unwrap();

        assert!(!fs.file_exists(Path::new("/ren/src")).unwrap());
        assert!(fs.file_exists(Path::new("/ren/dst")).unwrap());

        let mut r = fs.open_sequential_file(Path::new("/ren/dst")).unwrap();
        let mut buf = vec![0u8; 64];
        let n = r.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"payload");
    }

    #[test]
    fn test_rename_not_found() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/rn")).unwrap();
        let result = fs.rename(Path::new("/rn/nofile"), Path::new("/rn/dst"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_name() {
        let fs = create_fs();
        assert_eq!(fs.name(), "MemoryFileSystem");
    }

    #[test]
    fn test_open_sequential_nonexistent() {
        let fs = create_fs();
        let result = fs.open_sequential_file(Path::new("/no/such/file"));
        match result {
            Err(e) => assert!(e.is_not_found()),
            Ok(_) => panic!("expected NotFound error"),
        }
    }

    #[test]
    fn test_open_random_access_nonexistent() {
        let fs = create_fs();
        let result = fs.open_random_access_file(Path::new("/no/such/file"));
        match result {
            Err(e) => assert!(e.is_not_found()),
            Ok(_) => panic!("expected NotFound error"),
        }
    }

    #[test]
    fn test_list_dir_with_files_and_dirs() {
        let fs = create_fs();
        fs.create_dir_all(Path::new("/mix/sub")).unwrap();

        let mut w = fs
            .open_writable_file(Path::new("/mix/a.txt"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"aaa").unwrap();
        drop(w);

        let mut w = fs
            .open_writable_file(Path::new("/mix/b.txt"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"bb").unwrap();
        drop(w);

        let entries = fs.list_dir(Path::new("/mix")).unwrap();
        assert_eq!(entries.len(), 3); // a.txt, b.txt, sub/

        let names: Vec<String> = entries
            .iter()
            .map(|e| e.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains(&"a.txt".to_string()));
        assert!(names.contains(&"b.txt".to_string()));
        assert!(names.contains(&"sub".to_string()));

        // Verify sizes
        let a_entry = entries.iter().find(|e| e.path.ends_with("a.txt")).unwrap();
        assert_eq!(a_entry.size, 3);
        assert!(!a_entry.is_dir);

        let sub_entry = entries.iter().find(|e| e.path.ends_with("sub")).unwrap();
        assert!(sub_entry.is_dir);
    }

    #[test]
    fn test_clone_shares_state() {
        let fs1 = create_fs();
        let fs2 = fs1.clone();

        fs1.create_dir_all(Path::new("/shared")).unwrap();
        let mut w = fs1
            .open_writable_file(Path::new("/shared/f"), WriteMode::CreateNew)
            .unwrap();
        w.append(b"data").unwrap();
        drop(w);

        // fs2 should see the file created via fs1.
        assert!(fs2.file_exists(Path::new("/shared/f")).unwrap());
    }

    #[test]
    fn test_thread_safety() {
        use std::thread;

        let fs = create_fs();
        fs.create_dir_all(Path::new("/mt")).unwrap();

        let handles: Vec<_> = (0..8)
            .map(|i| {
                let fs_clone = fs.clone();
                thread::spawn(move || {
                    let path_str = format!("/mt/file_{}", i);
                    let path = Path::new(&path_str);
                    let mut w = fs_clone
                        .open_writable_file(path, WriteMode::CreateNew)
                        .unwrap();
                    w.append(format!("thread {}", i).as_bytes()).unwrap();
                    drop(w);
                    assert!(fs_clone.file_exists(path).unwrap());
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let entries = fs.list_dir(Path::new("/mt")).unwrap();
        assert_eq!(entries.len(), 8);
    }
}

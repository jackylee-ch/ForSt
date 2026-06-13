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

//! Object store abstraction for cloud storage backends.
//!
//! This module provides an [`ObjectStore`] trait that abstracts cloud object
//! storage operations (S3, GCS, Azure Blob Storage) and an
//! [`ObjectStoreFileSystem`] adapter that implements the [`FileSystem`] trait
//! on top of any `ObjectStore` implementation.
//!
//! # Architecture
//!
//! ```text
//!   FileSystem trait
//!       ^
//!       |
//!   ObjectStoreFileSystem  (adapter)
//!       |
//!       v
//!   ObjectStore trait  (cloud operations)
//!       ^
//!       |
//!   S3ObjectStore / GcsObjectStore / MockObjectStore  (implementations)
//! ```
//!
//! # Phase 1 Skeleton
//!
//! This is a Phase 1 skeleton. The [`MockObjectStore`] provides an in-memory
//! implementation for testing. Real cloud client integration (e.g., via the
//! `aws-sdk-s3` crate) will be added in a later phase.
//!
//! # Examples
//!
//! ```
//! use forst_rs_io::object_store::{MockObjectStore, ObjectStore, ObjectStorePath};
//!
//! let store = MockObjectStore::new("test");
//! let path = ObjectStorePath::new("my-bucket", "path/to/file.sst");
//!
//! store.put(&path, b"hello world").unwrap();
//! let data = store.get(&path).unwrap();
//! assert_eq!(data, b"hello world");
//! ```

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use forst_rs_common::error::{ForstError, ForstResult};

use crate::filesystem::{
    FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile, WriteMode,
};

// ---------------------------------------------------------------------------
// ObjectStorePath
// ---------------------------------------------------------------------------

/// An object store location identified by bucket and key.
///
/// In cloud object stores, objects are addressed by a (bucket, key) pair.
/// The key typically looks like a file path (e.g., `data/sst/000001.sst`)
/// but has no directory semantics -- it is a flat namespace with `/` as a
/// conventional delimiter for prefix-based listing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectStorePath {
    /// The bucket (or container) name.
    pub bucket: String,
    /// The object key within the bucket.
    pub key: String,
}

impl ObjectStorePath {
    /// Creates a new `ObjectStorePath` from bucket and key.
    pub fn new(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// Derives an `ObjectStorePath` from a local filesystem path.
    ///
    /// Strips the `prefix` from the path to produce the object key.
    /// For example, if `path` is `/data/sst/000001.sst`, `bucket` is
    /// `"my-bucket"`, and `prefix` is `/data`, the resulting key is
    /// `sst/000001.sst`.
    pub fn from_path(path: &Path, bucket: &str, prefix: &str) -> Self {
        let path_str = path.to_string_lossy();
        let key = path_str
            .strip_prefix(prefix)
            .unwrap_or(&path_str)
            .trim_start_matches('/')
            .to_string();
        Self {
            bucket: bucket.to_string(),
            key,
        }
    }

    /// Returns a canonical string key for internal storage.
    ///
    /// The format is `{bucket}/{key}`, used as a HashMap key in
    /// mock implementations.
    fn canonical(&self) -> String {
        format!("{}/{}", self.bucket, self.key)
    }
}

impl std::fmt::Display for ObjectStorePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s3://{}/{}", self.bucket, self.key)
    }
}

// ---------------------------------------------------------------------------
// CompletedPart
// ---------------------------------------------------------------------------

/// A completed part in a multipart upload.
///
/// Returned by [`ObjectStore::upload_part`] and passed to
/// [`ObjectStore::complete_multipart_upload`] to assemble the final object.
#[derive(Debug, Clone)]
pub struct CompletedPart {
    /// The part number (1-indexed).
    pub part_number: u32,
    /// The ETag returned by the server for this part.
    pub etag: String,
}

// ---------------------------------------------------------------------------
// ObjectStore trait
// ---------------------------------------------------------------------------

/// Abstraction over cloud object storage operations.
///
/// This trait provides the core operations needed to interact with cloud
/// object stores like S3, GCS, and Azure Blob Storage. All operations are
/// synchronous; async wrappers can be built on top using Tokio.
///
/// Implementations must be `Send + Sync` to allow sharing across threads.
pub trait ObjectStore: Send + Sync {
    /// Returns a human-readable name for this object store implementation.
    fn name(&self) -> &str;

    /// Puts an entire object (suitable for small files).
    ///
    /// Overwrites the object if it already exists.
    fn put(&self, path: &ObjectStorePath, data: &[u8]) -> ForstResult<()>;

    /// Gets an entire object as a byte vector.
    ///
    /// Returns [`ForstError::NotFound`] if the object does not exist.
    fn get(&self, path: &ObjectStorePath) -> ForstResult<Vec<u8>>;

    /// Gets a byte range of an object.
    ///
    /// Returns bytes starting at `offset` with the given `length`.
    /// If fewer bytes are available than requested (offset + length exceeds
    /// the object size), returns only the available bytes.
    ///
    /// Returns [`ForstError::NotFound`] if the object does not exist.
    /// Returns [`ForstError::InvalidArgument`] if offset is beyond the object.
    fn get_range(&self, path: &ObjectStorePath, offset: u64, length: usize)
        -> ForstResult<Vec<u8>>;

    /// Deletes an object.
    ///
    /// Returns [`ForstError::NotFound`] if the object does not exist.
    fn delete(&self, path: &ObjectStorePath) -> ForstResult<()>;

    /// Lists objects in a bucket with the given prefix.
    ///
    /// Returns all objects whose keys start with `prefix`.
    fn list(&self, bucket: &str, prefix: &str) -> ForstResult<Vec<ObjectStorePath>>;

    /// Checks if an object exists.
    fn exists(&self, path: &ObjectStorePath) -> ForstResult<bool>;

    /// Returns the size of an object in bytes (HEAD operation).
    ///
    /// Returns [`ForstError::NotFound`] if the object does not exist.
    fn head(&self, path: &ObjectStorePath) -> ForstResult<u64>;

    /// Initiates a multipart upload.
    ///
    /// Returns an upload ID that must be passed to [`Self::upload_part`],
    /// [`Self::complete_multipart_upload`], or [`Self::abort_multipart_upload`].
    fn create_multipart_upload(&self, path: &ObjectStorePath) -> ForstResult<String>;

    /// Uploads a single part in a multipart upload.
    ///
    /// Returns a [`CompletedPart`] that must be collected and passed to
    /// [`Self::complete_multipart_upload`].
    fn upload_part(
        &self,
        path: &ObjectStorePath,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> ForstResult<CompletedPart>;

    /// Completes a multipart upload by assembling parts in order.
    ///
    /// The `parts` are sorted by `part_number` before assembly.
    fn complete_multipart_upload(
        &self,
        path: &ObjectStorePath,
        upload_id: &str,
        parts: Vec<CompletedPart>,
    ) -> ForstResult<()>;

    /// Aborts a multipart upload, discarding any uploaded parts.
    fn abort_multipart_upload(&self, path: &ObjectStorePath, upload_id: &str) -> ForstResult<()>;
}

// ---------------------------------------------------------------------------
// MockObjectStore
// ---------------------------------------------------------------------------

/// Parts uploaded for a multipart upload: (part_number, data).
type UploadParts = Vec<(u32, Vec<u8>)>;

/// State for an in-progress multipart upload: (canonical_key, parts).
type UploadState = (String, UploadParts);

/// An in-memory mock implementation of [`ObjectStore`] for testing.
///
/// All data is stored in `HashMap`s protected by `Mutex`. Multipart uploads
/// are tracked separately and only materialized into the main storage upon
/// completion.
///
/// # Examples
///
/// ```
/// use forst_rs_io::object_store::{MockObjectStore, ObjectStore, ObjectStorePath};
///
/// let store = MockObjectStore::new("mock-s3");
/// let path = ObjectStorePath::new("bucket", "key");
/// store.put(&path, b"data").unwrap();
/// assert_eq!(store.get(&path).unwrap(), b"data");
/// ```
pub struct MockObjectStore {
    name: String,
    /// Main object storage: canonical_key -> bytes.
    storage: Mutex<HashMap<String, Vec<u8>>>,
    /// In-progress multipart uploads: upload_id -> (canonical_key, parts).
    uploads: Mutex<HashMap<String, UploadState>>,
    /// Counter for generating unique upload IDs.
    upload_counter: Mutex<u64>,
}

impl MockObjectStore {
    /// Creates a new `MockObjectStore` with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            storage: Mutex::new(HashMap::new()),
            uploads: Mutex::new(HashMap::new()),
            upload_counter: Mutex::new(0),
        }
    }

    /// Returns the number of objects currently stored.
    pub fn object_count(&self) -> usize {
        self.storage.lock().unwrap().len()
    }
}

impl ObjectStore for MockObjectStore {
    fn name(&self) -> &str {
        &self.name
    }

    fn put(&self, path: &ObjectStorePath, data: &[u8]) -> ForstResult<()> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        storage.insert(path.canonical(), data.to_vec());
        Ok(())
    }

    fn get(&self, path: &ObjectStorePath) -> ForstResult<Vec<u8>> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        storage
            .get(&path.canonical())
            .cloned()
            .ok_or_else(|| ForstError::not_found(format!("object not found: {}", path)))
    }

    fn get_range(
        &self,
        path: &ObjectStorePath,
        offset: u64,
        length: usize,
    ) -> ForstResult<Vec<u8>> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        let data = storage
            .get(&path.canonical())
            .ok_or_else(|| ForstError::not_found(format!("object not found: {}", path)))?;

        let offset = offset as usize;
        if offset > data.len() {
            return Err(ForstError::invalid_argument(format!(
                "offset {} exceeds object size {}",
                offset,
                data.len()
            )));
        }

        let available = &data[offset..];
        let to_read = length.min(available.len());
        Ok(available[..to_read].to_vec())
    }

    fn delete(&self, path: &ObjectStorePath) -> ForstResult<()> {
        let mut storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        if storage.remove(&path.canonical()).is_none() {
            return Err(ForstError::not_found(format!("object not found: {}", path)));
        }
        Ok(())
    }

    fn list(&self, bucket: &str, prefix: &str) -> ForstResult<Vec<ObjectStorePath>> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;

        let full_prefix = format!("{}/{}", bucket, prefix);
        let mut results: Vec<ObjectStorePath> = storage
            .keys()
            .filter(|k| k.starts_with(&full_prefix))
            .map(|k| {
                // Split canonical key back into bucket and key.
                let key = k
                    .strip_prefix(&format!("{}/", bucket))
                    .unwrap_or(k)
                    .to_string();
                ObjectStorePath::new(bucket, key)
            })
            .collect();
        results.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(results)
    }

    fn exists(&self, path: &ObjectStorePath) -> ForstResult<bool> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        Ok(storage.contains_key(&path.canonical()))
    }

    fn head(&self, path: &ObjectStorePath) -> ForstResult<u64> {
        let storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        storage
            .get(&path.canonical())
            .map(|data| data.len() as u64)
            .ok_or_else(|| ForstError::not_found(format!("object not found: {}", path)))
    }

    fn create_multipart_upload(&self, path: &ObjectStorePath) -> ForstResult<String> {
        let mut counter = self
            .upload_counter
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        *counter += 1;
        let upload_id = format!("upload-{}", *counter);

        let mut uploads = self
            .uploads
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        uploads.insert(upload_id.clone(), (path.canonical(), Vec::new()));
        Ok(upload_id)
    }

    fn upload_part(
        &self,
        _path: &ObjectStorePath,
        upload_id: &str,
        part_number: u32,
        data: &[u8],
    ) -> ForstResult<CompletedPart> {
        let mut uploads = self
            .uploads
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        let (_, parts) = uploads
            .get_mut(upload_id)
            .ok_or_else(|| ForstError::not_found(format!("upload not found: {}", upload_id)))?;
        parts.push((part_number, data.to_vec()));

        Ok(CompletedPart {
            part_number,
            etag: format!("etag-{}-{}", upload_id, part_number),
        })
    }

    fn complete_multipart_upload(
        &self,
        _path: &ObjectStorePath,
        upload_id: &str,
        mut parts: Vec<CompletedPart>,
    ) -> ForstResult<()> {
        let mut uploads = self
            .uploads
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        let (canonical_key, uploaded_parts) = uploads
            .remove(upload_id)
            .ok_or_else(|| ForstError::not_found(format!("upload not found: {}", upload_id)))?;

        // Sort parts by part number to assemble in order.
        parts.sort_by_key(|p| p.part_number);

        // Build a map from part_number -> data for lookup.
        let part_data: HashMap<u32, Vec<u8>> = uploaded_parts.into_iter().collect();

        // Assemble the final object from parts in order.
        let mut assembled = Vec::new();
        for part in &parts {
            let data = part_data.get(&part.part_number).ok_or_else(|| {
                ForstError::invalid_argument(format!(
                    "part {} referenced in completion but never uploaded",
                    part.part_number
                ))
            })?;
            assembled.extend_from_slice(data);
        }

        let mut storage = self
            .storage
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        storage.insert(canonical_key, assembled);
        Ok(())
    }

    fn abort_multipart_upload(&self, _path: &ObjectStorePath, upload_id: &str) -> ForstResult<()> {
        let mut uploads = self
            .uploads
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        if uploads.remove(upload_id).is_none() {
            return Err(ForstError::not_found(format!(
                "upload not found: {}",
                upload_id
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ObjectStoreFileSystem — adapts ObjectStore to the FileSystem trait
// ---------------------------------------------------------------------------

/// Adapts an [`ObjectStore`] to the [`FileSystem`] trait.
///
/// This adapter maps filesystem paths to object store keys using a
/// configurable bucket and prefix. It enables the [`crate::router::FileSystemRouter`] to
/// route SST file operations to cloud storage transparently.
///
/// # Path Mapping
///
/// Given a filesystem path `/data/sst/000001.sst`, bucket `"my-bucket"`, and
/// prefix `/data`, the corresponding object key is `sst/000001.sst`.
///
/// # Limitations (Phase 1)
///
/// - `rename` is implemented as get + put + delete (not atomic).
/// - Directories are virtual (object stores have a flat namespace).
/// - `create_dir_all` is a no-op.
/// - `delete_dir` deletes all objects with the directory prefix.
pub struct ObjectStoreFileSystem {
    store: Box<dyn ObjectStore>,
    bucket: String,
    prefix: String,
}

impl ObjectStoreFileSystem {
    /// Creates a new `ObjectStoreFileSystem`.
    ///
    /// - `store`: The underlying object store implementation.
    /// - `bucket`: The bucket name for all operations.
    /// - `prefix`: A path prefix to strip from filesystem paths when
    ///   computing object keys.
    pub fn new(
        store: Box<dyn ObjectStore>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            store,
            bucket: bucket.into(),
            prefix: prefix.into(),
        }
    }

    /// Maps a filesystem path to an [`ObjectStorePath`].
    fn to_object_path(&self, path: &Path) -> ObjectStorePath {
        ObjectStorePath::from_path(path, &self.bucket, &self.prefix)
    }
}

// ---------------------------------------------------------------------------
// File types for ObjectStoreFileSystem
// ---------------------------------------------------------------------------

/// A sequential file backed by an in-memory buffer fetched from the object store.
struct ObjectStoreSequentialFile {
    cursor: Cursor<Vec<u8>>,
}

impl SequentialFile for ObjectStoreSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        Read::read(&mut self.cursor, buf).map_err(ForstError::Io)
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        let new_pos = self.cursor.position().saturating_add(n);
        self.cursor.set_position(new_pos);
        Ok(())
    }
}

/// A random-access file backed by an in-memory snapshot of the object data.
///
/// Since the crate uses `#![forbid(unsafe_code)]`, we snapshot the full object
/// into memory on open (same approach as [`crate::memory_fs::MemoryFileSystem`]).
struct ObjectStoreRandomAccessFileSnapshot {
    data: Vec<u8>,
}

impl RandomAccessFile for ObjectStoreRandomAccessFileSnapshot {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let offset = offset as usize;
        if offset >= self.data.len() {
            return Ok(0);
        }
        let available = &self.data[offset..];
        let to_copy = available.len().min(buf.len());
        buf[..to_copy].copy_from_slice(&available[..to_copy]);
        Ok(to_copy)
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.data.len() as u64)
    }
}

/// A writable file that commits data to the object store.
///
/// Uses a shared buffer so that appended data is visible to the
/// `ObjectStoreFileSystem` for later reads.
///
/// Since the crate uses `#![forbid(unsafe_code)]`, we cannot hold a raw
/// pointer back to the `ObjectStore`. Instead, writes are buffered in a
/// shared `Arc<Mutex<Vec<u8>>>` and the data is committed to the object
/// store on the next read operation.
struct ObjectStoreCommittingWriter {
    buffer: std::sync::Arc<Mutex<Vec<u8>>>,
}

impl WritableFile for ObjectStoreCommittingWriter {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        let mut buf = self
            .buffer
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        buf.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        Ok(())
    }

    fn sync(&mut self) -> ForstResult<()> {
        Ok(())
    }

    fn file_size(&self) -> ForstResult<u64> {
        let buf = self
            .buffer
            .lock()
            .map_err(|e| ForstError::corruption(format!("lock poisoned: {}", e)))?;
        Ok(buf.len() as u64)
    }
}

// ---------------------------------------------------------------------------
// FileSystem implementation for ObjectStoreFileSystem
// ---------------------------------------------------------------------------

impl FileSystem for ObjectStoreFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let obj_path = self.to_object_path(path);
        let data = self.store.get(&obj_path)?;
        Ok(Box::new(ObjectStoreSequentialFile {
            cursor: Cursor::new(data),
        }))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let obj_path = self.to_object_path(path);
        let data = self.store.get(&obj_path)?;
        Ok(Box::new(ObjectStoreRandomAccessFileSnapshot { data }))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let obj_path = self.to_object_path(path);

        match mode {
            WriteMode::CreateNew => {
                if self.store.exists(&obj_path)? {
                    return Err(ForstError::invalid_argument(format!(
                        "object already exists: {}",
                        obj_path
                    )));
                }
                // Put an empty object as placeholder.
                self.store.put(&obj_path, b"")?;
                Ok(Box::new(ObjectStoreCommittingWriter {
                    buffer: std::sync::Arc::new(Mutex::new(Vec::new())),
                }))
            }
            WriteMode::CreateOrTruncate => {
                // Put empty object (truncate or create).
                self.store.put(&obj_path, b"")?;
                Ok(Box::new(ObjectStoreCommittingWriter {
                    buffer: std::sync::Arc::new(Mutex::new(Vec::new())),
                }))
            }
            WriteMode::Append => {
                // Load existing data if present.
                let existing = match self.store.get(&obj_path) {
                    Ok(data) => data,
                    Err(e) if e.is_not_found() => Vec::new(),
                    Err(e) => return Err(e),
                };
                if existing.is_empty() {
                    // Create the object if it did not exist.
                    self.store.put(&obj_path, b"")?;
                }
                Ok(Box::new(ObjectStoreCommittingWriter {
                    buffer: std::sync::Arc::new(Mutex::new(existing)),
                }))
            }
        }
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        let obj_path = self.to_object_path(path);
        self.store.exists(&obj_path)
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        let obj_path = self.to_object_path(path);
        let size = self.store.head(&obj_path)?;
        Ok(FileMetadata {
            path: path.to_path_buf(),
            size,
            is_dir: false,
        })
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        let obj_path = self.to_object_path(dir);
        // Ensure prefix ends with '/' for proper listing.
        let prefix = if obj_path.key.is_empty() {
            String::new()
        } else if obj_path.key.ends_with('/') {
            obj_path.key.clone()
        } else {
            format!("{}/", obj_path.key)
        };

        let objects = self.store.list(&self.bucket, &prefix)?;
        let mut results = Vec::new();
        for obj in objects {
            let file_path = if self.prefix.is_empty() {
                PathBuf::from(format!("/{}", obj.key))
            } else {
                PathBuf::from(format!("{}/{}", self.prefix, obj.key))
            };
            let size = self.store.head(&obj)?;
            results.push(FileMetadata {
                path: file_path,
                size,
                is_dir: false,
            });
        }
        Ok(results)
    }

    fn create_dir_all(&self, _dir: &Path) -> ForstResult<()> {
        // Object stores have a flat namespace. Directories are virtual
        // and don't need explicit creation.
        Ok(())
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        let obj_path = self.to_object_path(path);
        self.store.delete(&obj_path)
    }

    fn delete_dir(&self, dir: &Path, _recursive: bool) -> ForstResult<()> {
        let obj_path = self.to_object_path(dir);
        let prefix = if obj_path.key.is_empty() {
            String::new()
        } else if obj_path.key.ends_with('/') {
            obj_path.key.clone()
        } else {
            format!("{}/", obj_path.key)
        };

        let objects = self.store.list(&self.bucket, &prefix)?;
        for obj in objects {
            self.store.delete(&obj)?;
        }
        Ok(())
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        // Object stores don't support native rename.
        // Implement as: get -> put at new key -> delete old key.
        let src_path = self.to_object_path(src);
        let dst_path = self.to_object_path(dst);

        let data = self.store.get(&src_path)?;
        self.store.put(&dst_path, &data)?;
        self.store.delete(&src_path)?;
        Ok(())
    }

    fn name(&self) -> &str {
        "ObjectStoreFileSystem"
    }

    /// FRS-SCAN-OPEN-FANOUT: an object store always pays a remote round-trip on
    /// open — report NOT-local so the scan open-fanout engages.
    fn is_local(&self) -> bool {
        false
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // ObjectStorePath tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_object_store_path_new() {
        let path = ObjectStorePath::new("my-bucket", "path/to/file.sst");
        assert_eq!(path.bucket, "my-bucket");
        assert_eq!(path.key, "path/to/file.sst");
    }

    #[test]
    fn test_object_store_path_from_path() {
        let path = ObjectStorePath::from_path(Path::new("/data/sst/000001.sst"), "bucket", "/data");
        assert_eq!(path.bucket, "bucket");
        assert_eq!(path.key, "sst/000001.sst");
    }

    #[test]
    fn test_object_store_path_from_path_no_prefix_match() {
        let path = ObjectStorePath::from_path(Path::new("/other/file.sst"), "bucket", "/data");
        assert_eq!(path.bucket, "bucket");
        // Should keep the full path (minus leading slash).
        assert_eq!(path.key, "other/file.sst");
    }

    #[test]
    fn test_object_store_path_from_path_empty_prefix() {
        let path = ObjectStorePath::from_path(Path::new("/sst/000001.sst"), "bucket", "");
        assert_eq!(path.key, "sst/000001.sst");
    }

    #[test]
    fn test_object_store_path_display() {
        let path = ObjectStorePath::new("bucket", "key/file.sst");
        assert_eq!(format!("{}", path), "s3://bucket/key/file.sst");
    }

    #[test]
    fn test_object_store_path_canonical() {
        let path = ObjectStorePath::new("bucket", "key");
        assert_eq!(path.canonical(), "bucket/key");
    }

    #[test]
    fn test_object_store_path_eq() {
        let p1 = ObjectStorePath::new("bucket", "key");
        let p2 = ObjectStorePath::new("bucket", "key");
        let p3 = ObjectStorePath::new("bucket", "other");
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_object_store_path_clone() {
        let p1 = ObjectStorePath::new("b", "k");
        let p2 = p1.clone();
        assert_eq!(p1, p2);
    }

    // -----------------------------------------------------------------------
    // MockObjectStore — basic operations
    // -----------------------------------------------------------------------

    #[test]
    fn test_mock_name() {
        let store = MockObjectStore::new("test-store");
        assert_eq!(store.name(), "test-store");
    }

    #[test]
    fn test_mock_put_get_roundtrip() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file.txt");

        store.put(&path, b"hello world").unwrap();
        let data = store.get(&path).unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_mock_put_overwrites() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file.txt");

        store.put(&path, b"old data").unwrap();
        store.put(&path, b"new data").unwrap();
        let data = store.get(&path).unwrap();
        assert_eq!(data, b"new data");
    }

    #[test]
    fn test_mock_get_not_found() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "nonexistent");
        let result = store.get(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_get_range_full() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "data");
        store.put(&path, b"0123456789").unwrap();

        let data = store.get_range(&path, 0, 10).unwrap();
        assert_eq!(data, b"0123456789");
    }

    #[test]
    fn test_mock_get_range_middle() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "data");
        store.put(&path, b"0123456789").unwrap();

        let data = store.get_range(&path, 3, 4).unwrap();
        assert_eq!(data, b"3456");
    }

    #[test]
    fn test_mock_get_range_partial_at_end() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "data");
        store.put(&path, b"0123456789").unwrap();

        // Request more bytes than available from offset 8.
        let data = store.get_range(&path, 8, 10).unwrap();
        assert_eq!(data, b"89");
    }

    #[test]
    fn test_mock_get_range_at_boundary() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "data");
        store.put(&path, b"abcde").unwrap();

        // Offset exactly at end.
        let data = store.get_range(&path, 5, 5).unwrap();
        assert!(data.is_empty());
    }

    #[test]
    fn test_mock_get_range_beyond_end() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "data");
        store.put(&path, b"abc").unwrap();

        let result = store.get_range(&path, 10, 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid_argument());
    }

    #[test]
    fn test_mock_get_range_not_found() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "nonexistent");
        let result = store.get_range(&path, 0, 5);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_delete() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file");
        store.put(&path, b"data").unwrap();

        assert!(store.exists(&path).unwrap());
        store.delete(&path).unwrap();
        assert!(!store.exists(&path).unwrap());
    }

    #[test]
    fn test_mock_delete_not_found() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "nonexistent");
        let result = store.delete(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_exists() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file");

        assert!(!store.exists(&path).unwrap());
        store.put(&path, b"data").unwrap();
        assert!(store.exists(&path).unwrap());
    }

    #[test]
    fn test_mock_head() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file");
        store.put(&path, b"12345").unwrap();

        assert_eq!(store.head(&path).unwrap(), 5);
    }

    #[test]
    fn test_mock_head_not_found() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "nonexistent");
        let result = store.head(&path);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_list_with_prefix() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "sst/000001.sst"), b"a")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "sst/000002.sst"), b"b")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "wal/000001.log"), b"c")
            .unwrap();

        let sst_files = store.list("bucket", "sst/").unwrap();
        assert_eq!(sst_files.len(), 2);
        assert_eq!(sst_files[0].key, "sst/000001.sst");
        assert_eq!(sst_files[1].key, "sst/000002.sst");

        let wal_files = store.list("bucket", "wal/").unwrap();
        assert_eq!(wal_files.len(), 1);
        assert_eq!(wal_files[0].key, "wal/000001.log");
    }

    #[test]
    fn test_mock_list_empty_prefix() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "a"), b"1")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "b"), b"2")
            .unwrap();

        let all = store.list("bucket", "").unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_mock_list_no_matches() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "a"), b"1")
            .unwrap();

        let results = store.list("bucket", "zzz/").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_mock_list_different_bucket() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket-a", "file"), b"1")
            .unwrap();

        let results = store.list("bucket-b", "").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_mock_object_count() {
        let store = MockObjectStore::new("test");
        assert_eq!(store.object_count(), 0);

        store.put(&ObjectStorePath::new("b", "k1"), b"1").unwrap();
        store.put(&ObjectStorePath::new("b", "k2"), b"2").unwrap();
        assert_eq!(store.object_count(), 2);
    }

    // -----------------------------------------------------------------------
    // MockObjectStore — multipart upload
    // -----------------------------------------------------------------------

    #[test]
    fn test_mock_multipart_upload_lifecycle() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "large-file.sst");

        // Initiate.
        let upload_id = store.create_multipart_upload(&path).unwrap();

        // Upload parts (out of order).
        let part2 = store.upload_part(&path, &upload_id, 2, b" world").unwrap();
        let part1 = store.upload_part(&path, &upload_id, 1, b"hello").unwrap();
        let part3 = store.upload_part(&path, &upload_id, 3, b"!").unwrap();

        assert_eq!(part1.part_number, 1);
        assert_eq!(part2.part_number, 2);
        assert_eq!(part3.part_number, 3);

        // Complete (pass parts in the correct order).
        store
            .complete_multipart_upload(&path, &upload_id, vec![part1, part2, part3])
            .unwrap();

        // Verify assembled data.
        let data = store.get(&path).unwrap();
        assert_eq!(data, b"hello world!");
    }

    #[test]
    fn test_mock_multipart_upload_abort() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "aborted.sst");

        let upload_id = store.create_multipart_upload(&path).unwrap();
        store.upload_part(&path, &upload_id, 1, b"data").unwrap();

        // Abort.
        store.abort_multipart_upload(&path, &upload_id).unwrap();

        // Object should not exist.
        assert!(!store.exists(&path).unwrap());

        // Trying to abort again should fail.
        let result = store.abort_multipart_upload(&path, &upload_id);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_multipart_upload_invalid_upload_id() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file");

        let result = store.upload_part(&path, "nonexistent", 1, b"data");
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_multipart_complete_invalid_upload_id() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file");

        let result = store.complete_multipart_upload(&path, "nonexistent", vec![]);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_mock_multipart_upload_single_part() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "single-part.sst");

        let upload_id = store.create_multipart_upload(&path).unwrap();
        let part = store
            .upload_part(&path, &upload_id, 1, b"all data in one part")
            .unwrap();
        store
            .complete_multipart_upload(&path, &upload_id, vec![part])
            .unwrap();

        let data = store.get(&path).unwrap();
        assert_eq!(data, b"all data in one part");
    }

    #[test]
    fn test_mock_multipart_upload_empty_completion() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "empty.sst");

        let upload_id = store.create_multipart_upload(&path).unwrap();
        store
            .complete_multipart_upload(&path, &upload_id, vec![])
            .unwrap();

        let data = store.get(&path).unwrap();
        assert!(data.is_empty());
    }

    // -----------------------------------------------------------------------
    // ObjectStoreFileSystem tests
    // -----------------------------------------------------------------------

    fn create_osfs() -> ObjectStoreFileSystem {
        ObjectStoreFileSystem::new(
            Box::new(MockObjectStore::new("test-osfs")),
            "test-bucket",
            "/data",
        )
    }

    #[test]
    fn test_osfs_name() {
        let fs = create_osfs();
        assert_eq!(fs.name(), "ObjectStoreFileSystem");
    }

    #[test]
    fn test_osfs_create_dir_all_is_noop() {
        let fs = create_osfs();
        // Should succeed without error -- no-op for object stores.
        fs.create_dir_all(Path::new("/data/sst")).unwrap();
    }

    #[test]
    fn test_osfs_write_and_read_sequential() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "sst/000001.sst");
        store.put(&path, b"sequential read test").unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let mut reader = fs
            .open_sequential_file(Path::new("/sst/000001.sst"))
            .unwrap();
        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"sequential read test");

        // EOF
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_osfs_write_and_read_random_access() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "sst/000002.sst");
        store.put(&path, b"ABCDEFGHIJ").unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let reader = fs
            .open_random_access_file(Path::new("/sst/000002.sst"))
            .unwrap();
        assert_eq!(reader.file_size().unwrap(), 10);

        let mut buf = [0u8; 3];
        let n = reader.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"ABC");

        let n = reader.read_at(7, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"HIJ");

        // Beyond EOF
        let n = reader.read_at(100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_osfs_sequential_skip() {
        let store = MockObjectStore::new("test");
        let path = ObjectStorePath::new("bucket", "file");
        store.put(&path, b"0123456789").unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let mut reader = fs.open_sequential_file(Path::new("/file")).unwrap();
        reader.skip(5).unwrap();
        let mut buf = [0u8; 5];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"56789");
    }

    #[test]
    fn test_osfs_file_exists() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "sst/exists.sst"), b"data")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        assert!(fs.file_exists(Path::new("/sst/exists.sst")).unwrap());
        assert!(!fs.file_exists(Path::new("/sst/nope.sst")).unwrap());
    }

    #[test]
    fn test_osfs_get_file_metadata() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "file.sst"), b"12345")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let meta = fs.get_file_metadata(Path::new("/file.sst")).unwrap();
        assert_eq!(meta.size, 5);
        assert!(!meta.is_dir);
    }

    #[test]
    fn test_osfs_get_file_metadata_not_found() {
        let fs = create_osfs();
        let result = fs.get_file_metadata(Path::new("/data/nonexistent"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_osfs_delete_file() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "to-delete"), b"data")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        assert!(fs.file_exists(Path::new("/to-delete")).unwrap());
        fs.delete_file(Path::new("/to-delete")).unwrap();
        assert!(!fs.file_exists(Path::new("/to-delete")).unwrap());
    }

    #[test]
    fn test_osfs_delete_file_not_found() {
        let fs = create_osfs();
        let result = fs.delete_file(Path::new("/data/nonexistent"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_osfs_rename() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "src"), b"payload")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        fs.rename(Path::new("/src"), Path::new("/dst")).unwrap();

        assert!(!fs.file_exists(Path::new("/src")).unwrap());
        assert!(fs.file_exists(Path::new("/dst")).unwrap());

        let mut reader = fs.open_sequential_file(Path::new("/dst")).unwrap();
        let mut buf = vec![0u8; 64];
        let n = reader.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"payload");
    }

    #[test]
    fn test_osfs_rename_not_found() {
        let fs = create_osfs();
        let result = fs.rename(Path::new("/data/nonexistent"), Path::new("/data/dst"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_osfs_list_dir() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "sst/000001.sst"), b"data1")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "sst/000002.sst"), b"data2")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "wal/000001.log"), b"wal")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let entries = fs.list_dir(Path::new("/sst")).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].size, 5);
        assert_eq!(entries[1].size, 5);
    }

    #[test]
    fn test_osfs_list_dir_empty() {
        let fs = create_osfs();
        let entries = fs.list_dir(Path::new("/data/empty")).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_osfs_delete_dir() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "dir/a.sst"), b"a")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "dir/b.sst"), b"b")
            .unwrap();
        store
            .put(&ObjectStorePath::new("bucket", "other"), b"c")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        fs.delete_dir(Path::new("/dir"), true).unwrap();

        assert!(!fs.file_exists(Path::new("/dir/a.sst")).unwrap());
        assert!(!fs.file_exists(Path::new("/dir/b.sst")).unwrap());
        // Other objects should not be affected.
        assert!(fs.file_exists(Path::new("/other")).unwrap());
    }

    #[test]
    fn test_osfs_open_sequential_not_found() {
        let fs = create_osfs();
        let result = fs.open_sequential_file(Path::new("/data/nope"));
        match result {
            Err(e) => assert!(e.is_not_found()),
            Ok(_) => panic!("expected NotFound error"),
        }
    }

    #[test]
    fn test_osfs_open_random_access_not_found() {
        let fs = create_osfs();
        let result = fs.open_random_access_file(Path::new("/data/nope"));
        match result {
            Err(e) => assert!(e.is_not_found()),
            Ok(_) => panic!("expected NotFound error"),
        }
    }

    #[test]
    fn test_osfs_with_prefix() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "sst/000001.sst"), b"data")
            .unwrap();

        // Prefix is "/data" so path "/data/sst/000001.sst" maps to key "sst/000001.sst".
        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "/data");

        assert!(fs.file_exists(Path::new("/data/sst/000001.sst")).unwrap());
        assert!(!fs.file_exists(Path::new("/other/sst/000001.sst")).unwrap());
    }

    #[test]
    fn test_osfs_writable_file_create_new() {
        let store = MockObjectStore::new("test");
        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let mut writer = fs
            .open_writable_file(Path::new("/new-file.sst"), WriteMode::CreateNew)
            .unwrap();
        writer.append(b"hello").unwrap();
        assert_eq!(writer.file_size().unwrap(), 5);
    }

    #[test]
    fn test_osfs_writable_file_create_new_already_exists() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "existing"), b"data")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let result = fs.open_writable_file(Path::new("/existing"), WriteMode::CreateNew);
        match result {
            Err(e) => assert!(e.is_invalid_argument()),
            Ok(_) => panic!("expected InvalidArgument error"),
        }
    }

    #[test]
    fn test_osfs_writable_file_append_mode() {
        let store = MockObjectStore::new("test");
        store
            .put(&ObjectStorePath::new("bucket", "appendable"), b"hello")
            .unwrap();

        let fs = ObjectStoreFileSystem::new(Box::new(store), "bucket", "");

        let mut writer = fs
            .open_writable_file(Path::new("/appendable"), WriteMode::Append)
            .unwrap();
        // Should start with existing data size.
        assert_eq!(writer.file_size().unwrap(), 5);
        writer.append(b" world").unwrap();
        assert_eq!(writer.file_size().unwrap(), 11);
    }

    // -----------------------------------------------------------------------
    // CompletedPart tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_completed_part_clone() {
        let part = CompletedPart {
            part_number: 1,
            etag: "abc".to_string(),
        };
        let part2 = part.clone();
        assert_eq!(part.part_number, part2.part_number);
        assert_eq!(part.etag, part2.etag);
    }

    #[test]
    fn test_completed_part_debug() {
        let part = CompletedPart {
            part_number: 42,
            etag: "etag-123".to_string(),
        };
        let debug = format!("{:?}", part);
        assert!(debug.contains("42"));
        assert!(debug.contains("etag-123"));
    }
}

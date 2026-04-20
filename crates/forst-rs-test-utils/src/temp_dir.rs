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

//! Managed temporary directory for tests.
//!
//! [`TestDir`] wraps [`tempfile::TempDir`] with convenience methods for
//! creating child paths, writing test files, and ensuring automatic cleanup.

use std::path::{Path, PathBuf};

/// A managed temporary directory that is automatically deleted when dropped.
///
/// Wraps [`tempfile::TempDir`] with convenience methods for common test
/// patterns like creating child paths and writing initial file contents.
///
/// # Examples
///
/// ```
/// use forst_rs_test_utils::TestDir;
///
/// let dir = TestDir::new();
/// let sst_path = dir.child("000001.sst");
/// assert!(sst_path.ends_with("000001.sst"));
/// assert!(dir.path().exists());
/// ```
pub struct TestDir {
    inner: tempfile::TempDir,
}

impl TestDir {
    /// Creates a new temporary directory in the system's default temp location.
    ///
    /// # Panics
    ///
    /// Panics if the temporary directory cannot be created (e.g., filesystem
    /// is full or permissions are insufficient).
    pub fn new() -> Self {
        Self {
            inner: tempfile::TempDir::new().expect("failed to create temp dir"),
        }
    }

    /// Creates a new temporary directory with a name prefix for easier
    /// identification in test output or filesystem browsing.
    ///
    /// # Panics
    ///
    /// Panics if the temporary directory cannot be created.
    pub fn with_prefix(prefix: &str) -> Self {
        Self {
            inner: tempfile::TempDir::with_prefix(prefix)
                .expect("failed to create temp dir with prefix"),
        }
    }

    /// Returns the path to the temporary directory.
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Returns a path to a child file or subdirectory within this temp dir.
    ///
    /// The child is NOT created on disk; this only constructs the path.
    ///
    /// # Examples
    ///
    /// ```
    /// use forst_rs_test_utils::TestDir;
    ///
    /// let dir = TestDir::new();
    /// let manifest = dir.child("MANIFEST-000001");
    /// assert!(manifest.starts_with(dir.path()));
    /// ```
    pub fn child(&self, name: &str) -> PathBuf {
        self.inner.path().join(name)
    }

    /// Creates a child file with the given contents and returns its path.
    ///
    /// Parent directories are created automatically.
    ///
    /// # Panics
    ///
    /// Panics if the file cannot be written.
    pub fn write_child(&self, name: &str, contents: &[u8]) -> PathBuf {
        let path = self.child(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create parent directories");
        }
        std::fs::write(&path, contents).expect("failed to write child file");
        path
    }

    /// Creates a subdirectory within this temp dir and returns its path.
    ///
    /// Intermediate directories are created as needed.
    ///
    /// # Panics
    ///
    /// Panics if the directory cannot be created.
    pub fn create_subdir(&self, name: &str) -> PathBuf {
        let path = self.child(name);
        std::fs::create_dir_all(&path).expect("failed to create subdirectory");
        path
    }
}

impl Default for TestDir {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_directory() {
        let dir = TestDir::new();
        assert!(dir.path().exists());
        assert!(dir.path().is_dir());
    }

    #[test]
    fn test_with_prefix() {
        let dir = TestDir::with_prefix("forst-test-");
        assert!(dir.path().exists());
        let dir_name = dir
            .path()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        assert!(dir_name.starts_with("forst-test-"));
    }

    #[test]
    fn test_child_returns_joined_path() {
        let dir = TestDir::new();
        let child = dir.child("000001.sst");
        assert_eq!(child, dir.path().join("000001.sst"));
    }

    #[test]
    fn test_child_does_not_create_file() {
        let dir = TestDir::new();
        let child = dir.child("nonexistent.dat");
        assert!(!child.exists());
    }

    #[test]
    fn test_write_child_creates_file() {
        let dir = TestDir::new();
        let path = dir.write_child("test.dat", b"hello");
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"hello");
    }

    #[test]
    fn test_write_child_creates_parent_dirs() {
        let dir = TestDir::new();
        let path = dir.write_child("a/b/c/deep.txt", b"deep");
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), b"deep");
    }

    #[test]
    fn test_create_subdir() {
        let dir = TestDir::new();
        let sub = dir.create_subdir("data/sst");
        assert!(sub.exists());
        assert!(sub.is_dir());
    }

    #[test]
    fn test_default_trait() {
        let dir = TestDir::default();
        assert!(dir.path().exists());
    }

    #[test]
    fn test_dropped_directory_is_cleaned_up() {
        let path = {
            let dir = TestDir::new();
            let p = dir.path().to_path_buf();
            assert!(p.exists());
            p
        };
        // After drop, the directory should be removed.
        assert!(!path.exists());
    }
}

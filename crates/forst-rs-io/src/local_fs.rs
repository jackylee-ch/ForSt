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

//! Local POSIX filesystem backend for ForSt-RS.
//!
//! Provides [`LocalFileSystem`], the default filesystem implementation that
//! delegates to the standard library's `std::fs` and `std::io` primitives.
//!
//! File types:
//! - [`LocalSequentialFile`]: Forward-only reading via `BufReader<File>`.
//! - [`LocalRandomAccessFile`]: Positioned reads via `Seek` + `Read`.
//! - [`LocalWritableFile`]: Append-only writes via `BufWriter<File>`.

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use forst_rs_common::error::ForstResult;

use crate::filesystem::{
    map_io_error, FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile,
    WriteMode,
};

// ---------------------------------------------------------------------------
// LocalSequentialFile
// ---------------------------------------------------------------------------

/// A sequential (forward-only) file backed by a buffered `std::fs::File`.
pub struct LocalSequentialFile {
    reader: BufReader<File>,
}

impl LocalSequentialFile {
    fn new(file: File) -> Self {
        Self {
            reader: BufReader::new(file),
        }
    }
}

impl SequentialFile for LocalSequentialFile {
    fn read(&mut self, buf: &mut [u8]) -> ForstResult<usize> {
        self.reader
            .read(buf)
            .map_err(|e| map_io_error(e, "sequential read"))
    }

    fn skip(&mut self, n: u64) -> ForstResult<()> {
        self.reader
            .seek(SeekFrom::Current(n as i64))
            .map(|_| ())
            .map_err(|e| map_io_error(e, "sequential skip"))
    }
}

// ---------------------------------------------------------------------------
// LocalRandomAccessFile
// ---------------------------------------------------------------------------

/// A random-access file backed by `std::fs::File`.
///
/// Each `read_at` call seeks to the requested offset then reads. A `Mutex`
/// protects the file handle so `read_at` can be called from multiple threads
/// (the trait requires `Send + Sync`).
pub struct LocalRandomAccessFile {
    file: std::sync::Mutex<File>,
    size: u64,
}

impl LocalRandomAccessFile {
    fn new(file: File) -> ForstResult<Self> {
        let size = file
            .metadata()
            .map_err(|e| map_io_error(e, "random access file metadata"))?
            .len();
        Ok(Self {
            file: std::sync::Mutex::new(file),
            size,
        })
    }
}

impl RandomAccessFile for LocalRandomAccessFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        let mut file = self
            .file
            .lock()
            .map_err(|e| forst_rs_common::error::ForstError::corruption(format!("lock poisoned: {}", e)))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| map_io_error(e, "random access seek"))?;
        file.read(buf)
            .map_err(|e| map_io_error(e, "random access read"))
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.size)
    }
}

// ---------------------------------------------------------------------------
// LocalWritableFile
// ---------------------------------------------------------------------------

/// A writable file backed by a buffered `std::fs::File`.
pub struct LocalWritableFile {
    writer: BufWriter<File>,
    bytes_written: u64,
}

impl LocalWritableFile {
    fn new(file: File) -> ForstResult<Self> {
        // If opened in append mode, start from current file size.
        let initial_size = file
            .metadata()
            .map_err(|e| map_io_error(e, "writable file metadata"))?
            .len();
        Ok(Self {
            writer: BufWriter::new(file),
            bytes_written: initial_size,
        })
    }
}

impl WritableFile for LocalWritableFile {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        self.writer
            .write_all(data)
            .map_err(|e| map_io_error(e, "writable file append"))?;
        self.bytes_written += data.len() as u64;
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        self.writer
            .flush()
            .map_err(|e| map_io_error(e, "writable file flush"))
    }

    fn sync(&mut self) -> ForstResult<()> {
        self.flush()?;
        self.writer
            .get_ref()
            .sync_all()
            .map_err(|e| map_io_error(e, "writable file sync"))
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.bytes_written)
    }
}

// ---------------------------------------------------------------------------
// LocalFileSystem
// ---------------------------------------------------------------------------

/// The default POSIX local filesystem implementation.
///
/// All operations delegate directly to `std::fs`. This is the primary
/// backend for development and single-node deployments.
#[derive(Debug, Default)]
pub struct LocalFileSystem;

impl LocalFileSystem {
    /// Creates a new `LocalFileSystem`.
    pub fn new() -> Self {
        Self
    }
}

impl FileSystem for LocalFileSystem {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        let file = File::open(path)
            .map_err(|e| map_io_error(e, &format!("open sequential file: {}", path.display())))?;
        Ok(Box::new(LocalSequentialFile::new(file)))
    }

    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        let file = File::open(path).map_err(|e| {
            map_io_error(e, &format!("open random access file: {}", path.display()))
        })?;
        Ok(Box::new(LocalRandomAccessFile::new(file)?))
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let file = match mode {
            WriteMode::CreateNew => OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path),
            WriteMode::CreateOrTruncate => OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(path),
            WriteMode::Append => OpenOptions::new()
                .create(true)
                .append(true)
                .open(path),
        }
        .map_err(|e| map_io_error(e, &format!("open writable file: {}", path.display())))?;
        Ok(Box::new(LocalWritableFile::new(file)?))
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        match fs::metadata(path) {
            Ok(meta) => Ok(meta.is_file()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(map_io_error(
                e,
                &format!("file_exists: {}", path.display()),
            )),
        }
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        let meta = fs::metadata(path)
            .map_err(|e| map_io_error(e, &format!("get_file_metadata: {}", path.display())))?;
        Ok(FileMetadata {
            path: path.to_path_buf(),
            size: meta.len(),
            is_dir: meta.is_dir(),
        })
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        let entries = fs::read_dir(dir)
            .map_err(|e| map_io_error(e, &format!("list_dir: {}", dir.display())))?;
        let mut result = Vec::new();
        for entry in entries {
            let entry =
                entry.map_err(|e| map_io_error(e, &format!("list_dir entry: {}", dir.display())))?;
            let meta = entry.metadata().map_err(|e| {
                map_io_error(
                    e,
                    &format!("list_dir metadata: {}", entry.path().display()),
                )
            })?;
            result.push(FileMetadata {
                path: entry.path(),
                size: meta.len(),
                is_dir: meta.is_dir(),
            });
        }
        Ok(result)
    }

    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        fs::create_dir_all(dir)
            .map_err(|e| map_io_error(e, &format!("create_dir_all: {}", dir.display())))
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        fs::remove_file(path)
            .map_err(|e| map_io_error(e, &format!("delete_file: {}", path.display())))
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        if recursive {
            fs::remove_dir_all(path)
                .map_err(|e| map_io_error(e, &format!("delete_dir recursive: {}", path.display())))
        } else {
            fs::remove_dir(path)
                .map_err(|e| map_io_error(e, &format!("delete_dir: {}", path.display())))
        }
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        fs::rename(src, dst).map_err(|e| {
            map_io_error(
                e,
                &format!("rename: {} -> {}", src.display(), dst.display()),
            )
        })
    }

    fn name(&self) -> &str {
        "LocalFileSystem"
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_fs_and_dir() -> (LocalFileSystem, TempDir) {
        let dir = TempDir::new().expect("failed to create temp dir");
        (LocalFileSystem::new(), dir)
    }

    // -- Sequential file tests -----------------------------------------------

    #[test]
    fn test_sequential_read_entire_file() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("seq.txt");
        let data = b"hello sequential world";
        std::fs::write(&path, data).unwrap();

        let mut file = fs.open_sequential_file(&path).unwrap();
        let mut buf = vec![0u8; 64];
        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&buf[..n], data);

        // EOF returns 0
        let n2 = file.read(&mut buf).unwrap();
        assert_eq!(n2, 0);
    }

    #[test]
    fn test_sequential_read_in_chunks() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("chunks.txt");
        std::fs::write(&path, b"abcdefghij").unwrap();

        let mut file = fs.open_sequential_file(&path).unwrap();
        let mut buf = [0u8; 4];

        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf[..n], b"abcd");

        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf[..n], b"efgh");

        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..n], b"ij");
    }

    #[test]
    fn test_sequential_skip() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("skip.txt");
        std::fs::write(&path, b"0123456789").unwrap();

        let mut file = fs.open_sequential_file(&path).unwrap();
        file.skip(5).unwrap();
        let mut buf = [0u8; 5];
        let n = file.read(&mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"56789");
    }

    #[test]
    fn test_sequential_open_nonexistent() {
        let fs = LocalFileSystem::new();
        let result = fs.open_sequential_file(Path::new("/nonexistent/path/xyz.sst"));
        match result {
            Err(e) => assert!(e.is_not_found()),
            Ok(_) => panic!("expected NotFound error"),
        }
    }

    // -- Random access file tests --------------------------------------------

    #[test]
    fn test_random_access_read_at() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("rand.dat");
        std::fs::write(&path, b"ABCDEFGHIJ").unwrap();

        let file = fs.open_random_access_file(&path).unwrap();
        let mut buf = [0u8; 3];

        let n = file.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"ABC");

        let n = file.read_at(7, &mut buf).unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"HIJ");
    }

    #[test]
    fn test_random_access_read_at_offset_beyond_eof() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("short.dat");
        std::fs::write(&path, b"AB").unwrap();

        let file = fs.open_random_access_file(&path).unwrap();
        let mut buf = [0u8; 4];
        let n = file.read_at(100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_random_access_file_size() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("sized.dat");
        std::fs::write(&path, b"twelve bytes").unwrap();

        let file = fs.open_random_access_file(&path).unwrap();
        assert_eq!(file.file_size().unwrap(), 12);
    }

    // -- Writable file tests -------------------------------------------------

    #[test]
    fn test_writable_create_new() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("new.dat");

        let mut file = fs.open_writable_file(&path, WriteMode::CreateNew).unwrap();
        file.append(b"hello").unwrap();
        file.flush().unwrap();
        assert_eq!(file.file_size().unwrap(), 5);

        file.append(b" world").unwrap();
        file.sync().unwrap();
        assert_eq!(file.file_size().unwrap(), 11);

        // Verify on disk
        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"hello world");
    }

    #[test]
    fn test_writable_create_new_already_exists() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("existing.dat");
        std::fs::write(&path, b"data").unwrap();

        let result = fs.open_writable_file(&path, WriteMode::CreateNew);
        match result {
            Err(e) => assert!(e.is_invalid_argument()),
            Ok(_) => panic!("expected InvalidArgument error"),
        }
    }

    #[test]
    fn test_writable_create_or_truncate() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("trunc.dat");
        std::fs::write(&path, b"old content that is long").unwrap();

        let mut file = fs
            .open_writable_file(&path, WriteMode::CreateOrTruncate)
            .unwrap();
        file.append(b"new").unwrap();
        file.sync().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"new");
    }

    #[test]
    fn test_writable_append_mode() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("append.dat");
        std::fs::write(&path, b"first").unwrap();

        let mut file = fs.open_writable_file(&path, WriteMode::Append).unwrap();
        assert_eq!(file.file_size().unwrap(), 5);
        file.append(b"second").unwrap();
        file.sync().unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"firstsecond");
    }

    // -- FileSystem directory operations -------------------------------------

    #[test]
    fn test_file_exists() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("exists.txt");

        assert!(!fs.file_exists(&path).unwrap());
        std::fs::write(&path, b"data").unwrap();
        assert!(fs.file_exists(&path).unwrap());

        // Directories are not files
        assert!(!fs.file_exists(dir.path()).unwrap());
    }

    #[test]
    fn test_get_file_metadata() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("meta.dat");
        std::fs::write(&path, b"12345").unwrap();

        let meta = fs.get_file_metadata(&path).unwrap();
        assert_eq!(meta.size, 5);
        assert!(!meta.is_dir);
        assert_eq!(meta.path, path);

        // Directory metadata
        let dir_meta = fs.get_file_metadata(dir.path()).unwrap();
        assert!(dir_meta.is_dir);
    }

    #[test]
    fn test_get_file_metadata_not_found() {
        let fs = LocalFileSystem::new();
        let result = fs.get_file_metadata(Path::new("/nonexistent/xyz"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_create_dir_all_and_list_dir() {
        let (fs, dir) = create_fs_and_dir();
        let nested = dir.path().join("a").join("b").join("c");
        fs.create_dir_all(&nested).unwrap();
        assert!(nested.is_dir());

        // Create files in the parent dir
        let parent = dir.path().join("a").join("b");
        std::fs::write(parent.join("f1.txt"), b"one").unwrap();
        std::fs::write(parent.join("f2.txt"), b"two").unwrap();

        let entries = fs.list_dir(&parent).unwrap();
        assert_eq!(entries.len(), 3); // f1.txt, f2.txt, c/
        let names: Vec<String> = entries
            .iter()
            .map(|e| e.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains(&"f1.txt".to_string()));
        assert!(names.contains(&"f2.txt".to_string()));
        assert!(names.contains(&"c".to_string()));
    }

    #[test]
    fn test_delete_file() {
        let (fs, dir) = create_fs_and_dir();
        let path = dir.path().join("deleteme.txt");
        std::fs::write(&path, b"data").unwrap();
        assert!(path.exists());

        fs.delete_file(&path).unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn test_delete_file_not_found() {
        let fs = LocalFileSystem::new();
        let result = fs.delete_file(Path::new("/nonexistent/file.txt"));
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_delete_dir_non_recursive() {
        let (fs, dir) = create_fs_and_dir();
        let sub = dir.path().join("empty_dir");
        std::fs::create_dir(&sub).unwrap();

        fs.delete_dir(&sub, false).unwrap();
        assert!(!sub.exists());
    }

    #[test]
    fn test_delete_dir_recursive() {
        let (fs, dir) = create_fs_and_dir();
        let sub = dir.path().join("full_dir");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("file.txt"), b"data").unwrap();

        // Non-recursive should fail on non-empty dir
        let err = fs.delete_dir(&sub, false);
        assert!(err.is_err());

        // Recursive should succeed
        fs.delete_dir(&sub, true).unwrap();
        assert!(!sub.exists());
    }

    #[test]
    fn test_rename() {
        let (fs, dir) = create_fs_and_dir();
        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");
        std::fs::write(&src, b"payload").unwrap();

        fs.rename(&src, &dst).unwrap();
        assert!(!src.exists());
        assert!(dst.exists());
        assert_eq!(std::fs::read(&dst).unwrap(), b"payload");
    }

    #[test]
    fn test_name() {
        let fs = LocalFileSystem::new();
        assert_eq!(fs.name(), "LocalFileSystem");
    }
}

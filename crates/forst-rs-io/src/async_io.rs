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

//! Async I/O traits for ForSt-RS.
//!
//! This module provides asynchronous reader and writer traits for non-blocking
//! I/O operations. These traits are designed for use with remote storage
//! backends (e.g., S3, OSS) where blocking I/O would be unacceptable.
//!
//! The traits use manual [`BoxFuture`] return types instead of the `async_trait`
//! macro, keeping the dependency footprint minimal while remaining fully
//! object-safe and `Send`-compatible.
//!
//! # Traits
//!
//! - [`AsyncSequentialReader`] -- Forward-only async reads (WAL replay over
//!   remote storage).
//! - [`AsyncRandomReader`] -- Positioned async reads (SST block fetches from
//!   S3).
//! - [`AsyncWriter`] -- Append-only async writes (uploading SST files).

use std::future::Future;
use std::pin::Pin;

use forst_rs_common::error::ForstResult;

// ---------------------------------------------------------------------------
// BoxFuture type alias
// ---------------------------------------------------------------------------

/// A type-erased, heap-allocated, `Send` future.
///
/// Used as the return type for all async trait methods so that the traits
/// remain object-safe without requiring the `async_trait` proc-macro.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// ---------------------------------------------------------------------------
// AsyncSequentialReader
// ---------------------------------------------------------------------------

/// Asynchronous sequential (forward-only) reader.
///
/// This is the async counterpart of [`crate::SequentialFile`]. Implementations
/// are expected to buffer internally and return data as it becomes available.
pub trait AsyncSequentialReader: Send + Sync {
    /// Reads up to `buf.len()` bytes into `buf`.
    ///
    /// Returns the number of bytes actually read. A return value of `0`
    /// indicates end-of-file.
    fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxFuture<'a, ForstResult<usize>>;

    /// Skips `n` bytes forward in the stream.
    ///
    /// Implementations may discard data or seek, depending on the backend.
    fn skip<'a>(&'a mut self, n: u64) -> BoxFuture<'a, ForstResult<()>>;
}

// ---------------------------------------------------------------------------
// AsyncRandomReader
// ---------------------------------------------------------------------------

/// Asynchronous random-access reader.
///
/// This is the async counterpart of [`crate::RandomAccessFile`].
/// Implementations must be safe to call from multiple tasks concurrently
/// (the trait requires `Send + Sync`).
pub trait AsyncRandomReader: Send + Sync {
    /// Reads up to `buf.len()` bytes starting at `offset`.
    ///
    /// Returns the number of bytes actually read.
    fn read_at<'a>(&'a self, offset: u64, buf: &'a mut [u8]) -> BoxFuture<'a, ForstResult<usize>>;

    /// Returns the total file size in bytes.
    fn file_size(&self) -> BoxFuture<'_, ForstResult<u64>>;
}

// ---------------------------------------------------------------------------
// AsyncWriter
// ---------------------------------------------------------------------------

/// Asynchronous writer.
///
/// This is the async counterpart of [`crate::WritableFile`]. Designed for
/// uploading data to remote storage where each operation may involve network
/// round-trips.
pub trait AsyncWriter: Send + Sync {
    /// Writes all bytes in `data` to the underlying sink.
    fn write_all<'a>(&'a mut self, data: &'a [u8]) -> BoxFuture<'a, ForstResult<()>>;

    /// Flushes internal buffers to the underlying sink.
    ///
    /// This does NOT guarantee durability -- use [`sync`](AsyncWriter::sync)
    /// for that.
    fn flush(&mut self) -> BoxFuture<'_, ForstResult<()>>;

    /// Ensures all written data is durable on persistent storage.
    fn sync(&mut self) -> BoxFuture<'_, ForstResult<()>>;

    /// Closes the writer, releasing any held resources.
    ///
    /// Takes `Box<Self>` so the writer is consumed and cannot be used after
    /// closing. The returned future has a `'static` lifetime because the
    /// writer is moved into it; implementors must ensure all captured state
    /// is `'static` (i.e., no borrowed references from the caller).
    fn close(self: Box<Self>) -> BoxFuture<'static, ForstResult<()>>;
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use forst_rs_common::error::ForstError;

    // -----------------------------------------------------------------------
    // MockAsyncReader -- in-memory sequential reader for testing
    // -----------------------------------------------------------------------

    /// A simple in-memory async sequential reader backed by a `Vec<u8>`.
    struct MockAsyncReader {
        data: Vec<u8>,
        pos: usize,
    }

    impl MockAsyncReader {
        fn new(data: Vec<u8>) -> Self {
            Self { data, pos: 0 }
        }
    }

    impl AsyncSequentialReader for MockAsyncReader {
        fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxFuture<'a, ForstResult<usize>> {
            Box::pin(async move {
                let remaining = &self.data[self.pos..];
                let to_read = buf.len().min(remaining.len());
                buf[..to_read].copy_from_slice(&remaining[..to_read]);
                self.pos += to_read;
                Ok(to_read)
            })
        }

        fn skip<'a>(&'a mut self, n: u64) -> BoxFuture<'a, ForstResult<()>> {
            Box::pin(async move {
                let n = n as usize;
                if self.pos + n > self.data.len() {
                    return Err(ForstError::invalid_argument(format!(
                        "skip {} bytes would exceed data length {}",
                        n,
                        self.data.len()
                    )));
                }
                self.pos += n;
                Ok(())
            })
        }
    }

    // -----------------------------------------------------------------------
    // MockAsyncRandomReader -- in-memory random-access reader for testing
    // -----------------------------------------------------------------------

    /// A simple in-memory async random-access reader backed by a `Vec<u8>`.
    struct MockAsyncRandomReader {
        data: Vec<u8>,
    }

    impl MockAsyncRandomReader {
        fn new(data: Vec<u8>) -> Self {
            Self { data }
        }
    }

    impl AsyncRandomReader for MockAsyncRandomReader {
        fn read_at<'a>(
            &'a self,
            offset: u64,
            buf: &'a mut [u8],
        ) -> BoxFuture<'a, ForstResult<usize>> {
            Box::pin(async move {
                let offset = offset as usize;
                if offset >= self.data.len() {
                    return Ok(0);
                }
                let remaining = &self.data[offset..];
                let to_read = buf.len().min(remaining.len());
                buf[..to_read].copy_from_slice(&remaining[..to_read]);
                Ok(to_read)
            })
        }

        fn file_size(&self) -> BoxFuture<'_, ForstResult<u64>> {
            Box::pin(async move { Ok(self.data.len() as u64) })
        }
    }

    // -----------------------------------------------------------------------
    // MockAsyncWriter -- in-memory async writer for testing
    // -----------------------------------------------------------------------

    /// A simple in-memory async writer backed by a `Vec<u8>`.
    struct MockAsyncWriter {
        data: Vec<u8>,
        flushed: bool,
        synced: bool,
    }

    impl MockAsyncWriter {
        fn new() -> Self {
            Self {
                data: Vec::new(),
                flushed: false,
                synced: false,
            }
        }
    }

    impl AsyncWriter for MockAsyncWriter {
        fn write_all<'a>(&'a mut self, data: &'a [u8]) -> BoxFuture<'a, ForstResult<()>> {
            Box::pin(async move {
                self.data.extend_from_slice(data);
                Ok(())
            })
        }

        fn flush(&mut self) -> BoxFuture<'_, ForstResult<()>> {
            Box::pin(async move {
                self.flushed = true;
                Ok(())
            })
        }

        fn sync(&mut self) -> BoxFuture<'_, ForstResult<()>> {
            Box::pin(async move {
                self.synced = true;
                Ok(())
            })
        }

        fn close(self: Box<Self>) -> BoxFuture<'static, ForstResult<()>> {
            Box::pin(async move { Ok(()) })
        }
    }

    // -----------------------------------------------------------------------
    // AsyncSequentialReader tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_mock_sequential_reader_read_all() {
        let mut reader = MockAsyncReader::new(b"hello world".to_vec());
        let mut buf = [0u8; 64];

        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 11);
        assert_eq!(&buf[..n], b"hello world");
    }

    #[tokio::test]
    async fn test_mock_sequential_reader_read_in_chunks() {
        let mut reader = MockAsyncReader::new(b"abcdef".to_vec());
        let mut buf = [0u8; 3];

        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"abc");

        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(&buf[..n], b"def");

        // EOF
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_mock_sequential_reader_read_empty() {
        let mut reader = MockAsyncReader::new(Vec::new());
        let mut buf = [0u8; 16];

        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_mock_sequential_reader_skip() {
        let mut reader = MockAsyncReader::new(b"hello world".to_vec());
        reader.skip(6).await.unwrap();

        let mut buf = [0u8; 16];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"world");
    }

    #[tokio::test]
    async fn test_mock_sequential_reader_skip_beyond_end() {
        let mut reader = MockAsyncReader::new(b"short".to_vec());
        let result = reader.skip(100).await;
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // AsyncRandomReader tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_mock_random_reader_read_at_beginning() {
        let reader = MockAsyncRandomReader::new(b"hello world".to_vec());
        let mut buf = [0u8; 5];

        let n = reader.read_at(0, &mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");
    }

    #[tokio::test]
    async fn test_mock_random_reader_read_at_offset() {
        let reader = MockAsyncRandomReader::new(b"hello world".to_vec());
        let mut buf = [0u8; 5];

        let n = reader.read_at(6, &mut buf).await.unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"world");
    }

    #[tokio::test]
    async fn test_mock_random_reader_read_at_past_eof() {
        let reader = MockAsyncRandomReader::new(b"abc".to_vec());
        let mut buf = [0u8; 5];

        let n = reader.read_at(100, &mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn test_mock_random_reader_read_at_partial() {
        let reader = MockAsyncRandomReader::new(b"abcdef".to_vec());
        let mut buf = [0u8; 10];

        // Read starting at offset 4, only 2 bytes remain.
        let n = reader.read_at(4, &mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..n], b"ef");
    }

    #[tokio::test]
    async fn test_mock_random_reader_file_size() {
        let reader = MockAsyncRandomReader::new(b"hello".to_vec());
        assert_eq!(reader.file_size().await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_mock_random_reader_empty_file_size() {
        let reader = MockAsyncRandomReader::new(Vec::new());
        assert_eq!(reader.file_size().await.unwrap(), 0);
    }

    // -----------------------------------------------------------------------
    // AsyncWriter tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_mock_writer_write_all() {
        let mut writer = MockAsyncWriter::new();
        writer.write_all(b"hello").await.unwrap();
        writer.write_all(b" world").await.unwrap();
        assert_eq!(writer.data, b"hello world");
    }

    #[tokio::test]
    async fn test_mock_writer_flush() {
        let mut writer = MockAsyncWriter::new();
        assert!(!writer.flushed);
        writer.flush().await.unwrap();
        assert!(writer.flushed);
    }

    #[tokio::test]
    async fn test_mock_writer_sync() {
        let mut writer = MockAsyncWriter::new();
        assert!(!writer.synced);
        writer.sync().await.unwrap();
        assert!(writer.synced);
    }

    #[tokio::test]
    async fn test_mock_writer_close() {
        let mut writer = Box::new(MockAsyncWriter::new());
        writer.write_all(b"data").await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_mock_writer_write_empty() {
        let mut writer = MockAsyncWriter::new();
        writer.write_all(b"").await.unwrap();
        assert!(writer.data.is_empty());
    }

    // -----------------------------------------------------------------------
    // Object safety tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_sequential_reader_is_object_safe() {
        let mut reader: Box<dyn AsyncSequentialReader> =
            Box::new(MockAsyncReader::new(b"test".to_vec()));
        let mut buf = [0u8; 4];
        let n = reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"test");
    }

    #[tokio::test]
    async fn test_random_reader_is_object_safe() {
        let reader: Box<dyn AsyncRandomReader> =
            Box::new(MockAsyncRandomReader::new(b"test".to_vec()));
        let mut buf = [0u8; 4];
        let n = reader.read_at(0, &mut buf).await.unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"test");
    }

    #[tokio::test]
    async fn test_writer_is_object_safe() {
        let mut writer: Box<dyn AsyncWriter> = Box::new(MockAsyncWriter::new());
        writer.write_all(b"hello").await.unwrap();
        writer.flush().await.unwrap();
        writer.sync().await.unwrap();
        writer.close().await.unwrap();
    }
}

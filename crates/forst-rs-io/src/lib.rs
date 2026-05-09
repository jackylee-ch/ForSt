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

//! ForSt-RS I/O Layer
//!
//! This crate provides file and storage abstractions for the ForSt-RS
//! storage engine, including async I/O operations via Tokio.

#![forbid(unsafe_code)]

pub mod async_io;
pub mod filesystem;
pub mod local_fs;
pub mod memory_fs;
pub mod object_store;
pub mod opendal_backend;
pub mod ownership;
pub mod router;

pub use async_io::{AsyncRandomReader, AsyncSequentialReader, AsyncWriter, BoxFuture};
pub use filesystem::{
    map_io_error, FileMetadata, FileSystem, RandomAccessFile, SequentialFile, WritableFile,
    WriteMode,
};
pub use local_fs::LocalFileSystem;
pub use memory_fs::MemoryFileSystem;
pub use object_store::{
    CompletedPart, MockObjectStore, ObjectStore, ObjectStoreFileSystem, ObjectStorePath,
};
pub use opendal_backend::OpendalFileSystem;
pub use ownership::{FileOwnership, FileOwnershipTracker, OwnedFile};
pub use router::{file_locality, FileLocality, FileSystemRouter};

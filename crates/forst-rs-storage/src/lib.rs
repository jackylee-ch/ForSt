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

//! ForSt-RS Storage Layer
//!
//! This crate provides SST file format building blocks including data blocks,
//! schema definitions, compression utilities, and file header/footer handling.

#![forbid(unsafe_code)]

pub mod cache;
pub mod cached_fs;
pub mod iter;
pub mod local_cache;
pub mod memtable;
pub mod merge_operator;
pub mod sst;
pub mod version;

pub use iter::{IterChunk, NativeIter};

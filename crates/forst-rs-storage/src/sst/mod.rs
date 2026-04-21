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

//! SST file format definitions and utilities.

pub mod compression;
pub mod schema;

pub use compression::{compress, decompress};
pub use schema::{
    sst_schema, BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA, FILE_HEADER_SIZE, SST_FORMAT_VERSION,
    SST_MAGIC,
};

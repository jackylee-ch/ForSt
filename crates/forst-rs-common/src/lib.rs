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

//! ForSt-RS Common Library
//!
//! This crate provides shared types, error definitions, and configuration
//! used across all ForSt-RS crates.

#![forbid(unsafe_code)]

pub mod arena;
pub mod checksum;
pub mod coding;
pub mod config;
pub mod error;
pub mod metrics;
pub mod types;

pub use arena::Arena;
pub use checksum::{crc32c, crc32c_extend, mask_crc, unmask_crc};
pub use coding::{
    get_fixed32, get_fixed64, get_varint32, get_varint64, put_fixed32, put_fixed64, put_varint32,
    put_varint64, varint32_length, varint64_length, MAX_VARINT32_LEN, MAX_VARINT64_LEN,
};
pub use config::{
    CfOptions, EngineOptions, EngineOptionsBuilder, ReadOptions, ReadTier, WriteOptions,
};
pub use error::{ForstError, ForstResult};
pub use metrics::{Counter, Gauge, Histogram, HistogramSnapshot};
pub use types::{
    ColumnFamilyId, CompressionType, FileNumber, InternalKey, KeyRange, Level, OpType,
    SequenceNumber, DEFAULT_CF_ID, MAX_LEVELS, MAX_SEQUENCE_NUMBER,
};

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

//! ForSt-RS Test Utilities
//!
//! This crate provides test helpers, temporary directory management,
//! and shared test fixtures for ForSt-RS integration and unit tests.
//!
//! # Key Components
//!
//! - [`TestDir`]: Managed temporary directory with convenience helpers.
//! - [`generate_kv_pairs`]: Deterministic KV data generator for tests.
//! - [`generate_sequential_keys`]: Sequential key generator with zero-padded
//!   keys for sorted iteration testing.
//! - [`assert_bytes_eq`], [`assert_kv_pairs_sorted`]: Common assertion helpers.

#![forbid(unsafe_code)]

pub mod assert_helpers;
pub mod kv_gen;
pub mod temp_dir;

pub use assert_helpers::{assert_bytes_eq, assert_kv_pairs_sorted};
pub use kv_gen::{generate_kv_pairs, generate_sequential_keys};
pub use temp_dir::TestDir;

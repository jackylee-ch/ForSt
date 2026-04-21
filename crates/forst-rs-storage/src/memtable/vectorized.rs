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

//! [`VectorizedMemTable`] — hybrid Sorted Run Array + BTreeMap implementation.
//!
//! Columnar storage (key, value, sequence, op_type) mirrors the SST Arrow
//! schema. A BTreeMap sorted index enables O(log N) point lookups over
//! already-merged data, while a HashMap buffers recent unsorted writes.

use super::MemTableConfig;

/// A vectorized MemTable using columnar storage + BTreeMap sorted index.
pub struct VectorizedMemTable {
    _config: MemTableConfig,
}

impl VectorizedMemTable {
    /// Creates a new, empty VectorizedMemTable.
    pub fn new(config: MemTableConfig) -> Self {
        Self { _config: config }
    }
}

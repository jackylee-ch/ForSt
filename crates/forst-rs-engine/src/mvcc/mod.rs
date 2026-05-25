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

//! MVCC (Multi-Version Concurrency Control) primitives.
//!
//! See spec `2026-05-10-forst-rs-checkpointable-keyed-backend-design.md`
//! §6a.2 for the snapshot type and registry contract. This module owns:
//!
//! - [`DbId`] — opaque DB instance identifier; bound into every snapshot
//!   so `release` operations against a foreign DB are caught at the FFI
//!   boundary (spec §15 "Same-DB" invariant).
//! - [`Snapshot`] — RAII snapshot handle. `Drop` releases the
//!   ref-count back to the issuing registry.
//! - [`SnapshotRegistry`] — tracks the set of live snapshot sequence
//!   numbers for compaction's `min_active_snapshot` query.

pub mod compaction_policy;
pub mod reader;
pub mod snapshot;

pub use compaction_policy::should_drop;
pub use reader::{get_at, get_at_with_merge, VersionedEntry};
pub use snapshot::{
    DbId, Snapshot, SnapshotAgeWarning, SnapshotRegistry, DEFAULT_MAX_AGE_MS, SNAPSHOT_AGE_HINT,
};

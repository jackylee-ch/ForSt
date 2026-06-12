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

//! ForSt-RS Engine Layer
//!
//! This crate implements the LSM-tree engine read/write paths. It wires
//! together [`forst_rs_storage`] primitives (MemTable, SST, VersionSet,
//! BlockCache) into a cohesive database engine exposing `put`, `get`,
//! `delete`, `merge`, `batch_write`, and `batch_get` operations.
//!
//! # Module Map
//!
//! - [`column_family`] — ColumnFamilyHandle, ColumnFamilyDescriptor,
//!   ColumnFamilyData (active memtable + immutable list + snapshot cache).
//! - [`write_batch`] — WriteBatch / WriteBatchEntry for grouped writes.
//! - [`write_controller`] — back-pressure (stall / slowdown / L0 triggers).
//! - [`snapshot_view`] — Immutable snapshot view over a column family.
//! - [`db`] — `DbImpl`, the top-level engine struct.
//!
//! # Storage backends
//!
//! Two equivalent injection paths exist for the on-disk layer; pick the
//! one that fits your call site:
//!
//! 1. **Legacy direct injection** — call
//!    [`db::DbImpl::open_with_fs`] with any
//!    `Arc<dyn forst_rs_io::FileSystem>`. Native backends shipped in
//!    `forst-rs-io` are [`forst_rs_io::LocalFileSystem`] (default for
//!    [`db::DbImpl::open`]) and [`forst_rs_io::MemoryFileSystem`]
//!    (used by [`db::DbImpl::open_default`] for tests). Lowest
//!    overhead — no async bridge, no operator dispatch.
//!
//! 2. **OpenDAL universal backend** — call
//!    [`db::DbImpl::open_with_opendal`] with an
//!    [`opendal::Operator`], or one of the targeted helpers
//!    [`db::DbImpl::open_local_opendal`] /
//!    [`db::DbImpl::open_memory_opendal`] /
//!    [`db::DbImpl::open_s3`]. Internally these wrap
//!    [`forst_rs_io::OpendalFileSystem`] as
//!    `Arc<dyn forst_rs_io::FileSystem>` and dispatch through the
//!    legacy path, so behavior is identical from the engine's
//!    perspective — only the I/O substrate differs.
//!
//! ## Supported OpenDAL services
//!
//! The workspace enables only the services we exercise in tests today:
//! `services-fs`, `services-memory`, and `services-s3`. To target other
//! services (GCS, Azure Blob, OSS, R2 native, …) build the
//! [`opendal::Operator`] yourself and hand it to
//! [`db::DbImpl::open_with_opendal`]; the engine never sees the
//! service-specific surface.
//!
//! ## Performance note
//!
//! [`forst_rs_io::OpendalFileSystem`] adds ~1 layer of indirection
//! (sync→async bridge via `tokio::runtime::Handle::block_on`) versus a
//! native [`forst_rs_io::LocalFileSystem`]. The cost is dominated by
//! the per-operation operator dispatch and is invisible at the SST
//! granularity, but for hot-path point lookups against local SSD
//! prefer [`db::DbImpl::open`]. For remote object stores OpenDAL is
//! the primary supported path.

#![forbid(unsafe_code)]
// FRS clippy policy: allow a few pedantic/stylistic lints that the engine uses
// deliberately — markdown lists in doc comments (doc_lazy_continuation), boxed
// trait-object iterator return types (type_complexity), and index-based loops in
// tests / bounds-checked hot paths (needless_range_loop). These are style-only and
// do not affect the e2e-vectorized / zero-copy / batched design.
#![allow(clippy::doc_lazy_continuation)]
#![allow(clippy::type_complexity)]
#![allow(clippy::needless_range_loop)]
// Doc comments cross-reference private internals (documented via --document-private-items);
// such links don't resolve under the strict rustdoc gate (doc-rendering cosmetics only).
#![allow(rustdoc::broken_intra_doc_links)]
// Workspace-wide ForstError is intentionally large; see forst-rs-common rationale.
#![allow(clippy::result_large_err)]

pub(crate) mod bg_pool;
pub mod checkpoint;
pub mod column_family;
pub mod compaction;
pub mod compaction_filter;
pub mod db;
pub mod file_deletion_guard;
pub mod flush;
pub mod list_merge;
pub mod mvcc;
pub mod runtime_tuning;
pub mod snapshot_view;
pub mod wal;
pub mod write_batch;
pub mod write_controller;

pub use checkpoint::{CheckpointManifest, CHECKPOINT_BLOB_NAME};
pub use column_family::{CfLifecycle, ColumnFamilyData, ColumnFamilyDescriptor, ColumnFamilyHandle};
pub use compaction::{compaction_output_path, CompactionJob};
pub use compaction_filter::{
    decode_ttl_payload, encode_ttl_value, CompactionDecision, CompactionFilter,
    CurrentTimeSupplier, FlinkTtlCompactionFilter, ListElementOffsetSupplier, TtlCompactionFilter,
    TtlStateType,
};
pub use db::{
    lifecycle_segments_enabled, s2_pinned_enabled, set_lifecycle_segments_override,
    set_s2_pinned_override, DbImpl, FillOutcome, IncrementalCheckpointResult,
    LinkedCheckpointDiscard, LiveFileInfo, PrefixScanStream, RowSink, DEFAULT_CF_NAME,
};
pub use file_deletion_guard::{FileDeletionGuard, PinHandle};
pub use flush::{sst_file_path, FlushJob};
pub use list_merge::ListMergeCombiner;
pub use mvcc::{DbId, Snapshot, SnapshotRegistry};
pub use runtime_tuning::WriteBufferManager;
pub use snapshot_view::SnapshotView;
pub use write_batch::{WriteBatch, WriteBatchEntry};
pub use write_controller::WriteController;

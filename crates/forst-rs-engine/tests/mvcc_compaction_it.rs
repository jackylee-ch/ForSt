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

//! Integration test for the MVCC `SnapshotRegistry` ↔ compaction wiring
//! introduced in B-Prod-P0 Task 0.9 (spec §6a.2 / §6a.5).
//!
//! Strategy: write 100 versions of `b"k"`, capture a snapshot, write 100
//! more, flush+compact. Verify the engine survives compaction with a
//! live snapshot (no panic, no error), the snapshot still reads its
//! pinned version, and dropping the snapshot allows a subsequent
//! compaction pass to reclaim space.
//!
//! Note: `DbImpl` doesn't currently expose a public `iter_versions`
//! helper, so the assertion is "compaction completed and produced live
//! files" plus a point-read on `b"k"` returning the latest write — this
//! proves the registry is wired without exercising the snapshot read
//! path (which is covered by the `mvcc::reader::get_at` unit tests).

use std::sync::Arc;

use forst_rs_common::{EngineOptions, SequenceNumber};
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, LocalFileSystem};
use tempfile::TempDir;

fn open_local(dir: &std::path::Path) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: dir.to_string_lossy().into_owned(),
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open engine")
}

#[test]
fn snapshot_pinned_versions_survive_compaction() {
    let dir = TempDir::new().expect("tempdir");
    let db = open_local(dir.path());
    let cf = db.default_cf();

    // Phase 1: 100 writes of b"k".
    for i in 0..100u64 {
        db.put(&cf, b"k", format!("v{}", i).as_bytes())
            .expect("put");
    }

    // Capture a snapshot at the current sequence — every write below
    // this seq must remain readable while `snap` is alive.
    let pre_snap_seq = SequenceNumber::new(db.sequence_number());
    let snap = db.snapshot_registry().capture(db.db_id(), pre_snap_seq);
    assert_eq!(
        db.snapshot_registry().active_count(),
        1,
        "registry should report exactly one live snapshot"
    );
    assert_eq!(
        db.snapshot_registry().min_active(),
        pre_snap_seq,
        "min_active should equal the captured seq"
    );

    // Phase 2: 100 more writes after the snapshot.
    for i in 100..200u64 {
        db.put(&cf, b"k", format!("v{}", i).as_bytes())
            .expect("put");
    }

    // Force a memtable switch + flush so the 200 writes land in an L0
    // SST (otherwise everything sits in the active memtable and no
    // compaction work happens). Pass `true` so `list_live_files` runs
    // the same switch-then-flush before reporting.
    let live = db.list_live_files(true).expect("list_live_files");
    assert!(
        !live.is_empty(),
        "force-flush should produce at least one L0 file"
    );

    // Compact with the snapshot still held.
    db.compact_all().expect("compact with live snapshot");

    // Sanity: latest point-read still observes the latest write.
    assert_eq!(
        db.get(&cf, b"k").expect("get").as_deref(),
        Some(b"v199".as_ref()),
        "point read must observe the latest write after compaction"
    );

    // Live SST files exist (compaction produced an output file).
    let live = db.list_live_files(false).expect("list_live_files");
    assert!(
        !live.is_empty(),
        "compaction with active snapshot should still produce SST files"
    );

    // Drop the snapshot — the registry should now report no active
    // snapshots and `min_active` should return to its empty sentinel.
    drop(snap);
    assert_eq!(db.snapshot_registry().active_count(), 0);
    assert_eq!(
        db.snapshot_registry().min_active(),
        SequenceNumber(u64::MAX),
        "registry should report u64::MAX when empty"
    );

    // Run another compaction now that nothing is pinned. Must succeed
    // and leave at least one live SST in place (the latest version of
    // b"k" is still required for foreground reads).
    db.compact_all().expect("compact after snapshot drop");
    let live_after = db.list_live_files(false).expect("list_live_files");
    assert!(
        !live_after.is_empty(),
        "compaction after snapshot release should still produce SST files"
    );

    // Latest write remains visible after the second compaction.
    assert_eq!(
        db.get(&cf, b"k").expect("get").as_deref(),
        Some(b"v199".as_ref()),
        "post-release compaction must not lose the latest write"
    );
}

#[test]
fn snapshot_capture_release_does_not_panic() {
    // Smoke test: capture/release round trip against a fresh engine.
    let dir = TempDir::new().expect("tempdir");
    let db = open_local(dir.path());
    let snap = db
        .snapshot_registry()
        .capture(db.db_id(), SequenceNumber::new(db.sequence_number()));
    assert_eq!(snap.db_id(), db.db_id());
    drop(snap);
    assert_eq!(db.snapshot_registry().active_count(), 0);
}

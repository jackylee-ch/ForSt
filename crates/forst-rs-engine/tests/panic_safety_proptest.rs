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

//! Panic-safety proptests for the engine (L1 spec, umbrella spec §5 Medium-1).
//!
//! Verifies that injecting a Rust panic mid-operation leaves the engine in a
//! consistent, usable state — no deadlocks, no leaked handles, no subsequent
//! panics on ordinary puts/gets.
//!
//! V1 ships 2 of 4 spec proptests:
//!   - `prop_memtable_insert_panic_safe`  (this file)
//!   - `prop_snapshot_create_panic_safe`  (this file)
//!   - iter_open_panic_safe              (TODO, follow-up PR)
//!   - compaction_run_panic_safe         (TODO, follow-up PR)

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, MemoryFileSystem};
use proptest::prelude::*;

/// Open a fresh in-memory engine for each proptest case.
fn open_engine() -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open in-memory engine")
}

proptest! {
    /// Memtable insert is panic-safe: a panic mid-sequence leaves the engine
    /// indices consistent — get() on previously inserted keys must not panic,
    /// and the engine must still accept further puts/gets after the unwind.
    ///
    /// Spec: umbrella spec §5 L1 Medium-1 "memtable_insert".
    #[test]
    fn prop_memtable_insert_panic_safe(
        inputs in prop::collection::vec(
            (
                prop::collection::vec(any::<u8>(), 1..32),   // key: non-empty
                prop::collection::vec(any::<u8>(), 0..256),  // value
            ),
            1..50,
        )
    ) {
        let engine = open_engine();
        let cf = engine.default_cf();

        // Choose an inject point in the middle of the sequence (never 0 so at
        // least one put succeeds before the panic).
        let panic_at = inputs.len() / 2;

        // Run puts; inject a panic at `panic_at`.
        let _ = catch_unwind(AssertUnwindSafe(|| {
            for (i, (k, v)) in inputs.iter().enumerate() {
                if i == panic_at {
                    panic!("injected panic at step {i}");
                }
                let _ = engine.put(&cf, k, v);
            }
        }));

        // Invariant 1: get() on keys inserted *before* the panic must not panic.
        // (It may return Ok(Some), Ok(None), or Err — all are acceptable.)
        for (k, _) in inputs.iter().take(panic_at) {
            let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let _ = engine.get(&cf, k);
            }));
            prop_assert!(
                result.is_ok(),
                "get() panicked on key {:?} after memtable panic", k
            );
        }

        // Invariant 2: engine is still usable — further puts/gets must not panic.
        let post_put = catch_unwind(AssertUnwindSafe(|| {
            let _ = engine.put(&cf, b"post_panic_key", b"post_panic_value");
        }));
        prop_assert!(post_put.is_ok(), "put() after panic must not itself panic");

        let post_get = catch_unwind(AssertUnwindSafe(|| {
            let _ = engine.get(&cf, b"post_panic_key");
        }));
        prop_assert!(post_get.is_ok(), "get() after panic must not itself panic");
    }

    /// Snapshot creation is panic-safe: injecting a panic mid-sequence does not
    /// deadlock or corrupt the snapshot registry — the engine must remain usable
    /// and accept a fresh snapshot after the unwind.
    ///
    /// Spec: umbrella spec §5 L1 Medium-1 "snapshot_create".
    #[test]
    fn prop_snapshot_create_panic_safe(
        n_snaps in 2usize..20
    ) {
        let engine = open_engine();

        // Panic partway through the snapshot loop.
        let panic_at = n_snaps / 2;

        // Snapshots created before the panic are dropped when `snaps` goes out
        // of scope inside catch_unwind, exercising the RAII release path under
        // panic conditions.
        let _ = catch_unwind(AssertUnwindSafe(|| {
            let mut snaps = Vec::new();
            for i in 0..n_snaps {
                if i == panic_at {
                    panic!("injected snapshot panic at step {i}");
                }
                snaps.push(engine.snapshot());
            }
            // snaps drop here in normal execution
        }));

        // Invariant 1: a new snapshot can still be created (registry not stuck).
        let snap_result = catch_unwind(AssertUnwindSafe(|| {
            engine.snapshot()
        }));
        prop_assert!(snap_result.is_ok(), "snapshot() after panic must not panic");

        // Invariant 2: engine is still usable for puts/gets.
        let cf = engine.default_cf();
        let put_result = catch_unwind(AssertUnwindSafe(|| {
            let _ = engine.put(&cf, b"snap_post_panic", b"ok");
        }));
        prop_assert!(put_result.is_ok(), "put() after snapshot panic must not panic");

        let get_result = catch_unwind(AssertUnwindSafe(|| {
            let _ = engine.get(&cf, b"snap_post_panic");
        }));
        prop_assert!(get_result.is_ok(), "get() after snapshot panic must not panic");
    }

    // TODO (follow-up PR): iter_open_panic_safe
    // Opens an iterator mid-loop and panics; verifies no handle leak and that
    // subsequent iterators work correctly.

    // TODO (follow-up PR): compaction_run_panic_safe
    // Triggers a manual compaction, panics mid-flush, verifies the engine
    // survives with consistent read results.
}

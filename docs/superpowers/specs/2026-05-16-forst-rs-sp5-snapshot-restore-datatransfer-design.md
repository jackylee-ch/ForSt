# SP5 — Snapshot / Restore / DataTransfer

**Date:** 2026-05-16
**Status:** Design — implementation pending
**Parent:** `2026-05-15-forst-rs-whole-program-vectorization-design.md` §SP5

## Goal

Bring forst-rs to parity with forst on snapshot / restore strategies and on
the async SST transfer path used for disaggregated state. Today forst-rs has
a single full-snapshot strategy and a single restore op; forst has incremental,
native-full, heap-timers-full, none-restore, plus Copy / Reusable data-transfer
strategies.

## Approach (incremental, recommendation = staged)

Three logical groupings, each landing independently:

### 5a — Incremental snapshot strategy

Use the engine's `frs_create_incremental_checkpoint_at` (already exported)
plus a new `frs_snapshot_export_arrow` that returns a list of SST file names
+ sizes + checksums as an Arrow RecordBatch. Java enumerates and assembles a
`KeyedStateHandle` referring to those files.

Pairs with `ForStRsIncrementalSnapshotStrategy` (port from forst).

### 5b — Async SST transfer

Move SST files from local cache to/from remote storage (S3 / HDFS / GCS) on
a background thread without blocking the slot's main task thread. New FFI:

```c
typedef void (*FrsTransferCallback)(int64_t bytes_done, int64_t bytes_total, int rc);

int frs_sst_upload_async(
    const char* file_path,
    const char* target_uri,
    FrsTransferCallback callback,
    void* callback_ctx);

int frs_sst_download_async(
    const char* source_uri,
    const char* file_path,
    FrsTransferCallback callback,
    void* callback_ctx);
```

The callback runs on a Rust-side thread; Java receives it via an FFM upcall
stub installed at backend init. The upcall pushes the result onto a Java
queue that the user's `CompletableFuture` drains.

Pairs with `ForStRsDataTransferStrategy` (Copy / Reusable variants, port
from forst).

### 5c — Restore strategy diversification

Three restore impls:
- `ForStRsIncrementalRestoreOperation` — uses the incremental handle from 5a.
- `ForStRsHeapTimersFullRestoreOperation` — heap-managed timer restore.
- `ForStRsNoneRestoreOperation` — empty initial state.

Selected at `ForStRsKeyedStateBackendBuilder` based on the `StateHandle`
type. Ports the forst dispatch logic.

## Components

### Rust FFI (new)

- `frs_snapshot_export_arrow(db, snapshot_handle, out_array, out_schema)` —
  Arrow C Data Interface, schema = `{file_name: Utf8, size: Int64, checksum: Binary}`.
- `frs_sst_upload_async(...)` / `frs_sst_download_async(...)` — see 5b above.

### Engine

`frs_snapshot_export_arrow` walks the SST manifest of a snapshot and emits
one row per SST file. `crates/forst-rs-engine/src/version.rs` already has
the snapshot manifest; just need an exporter.

For async transfer, lean on the `forst-rs-io` crate's `FileSystem` trait
(OpenDAL-backed). Spawn a Tokio task per transfer; callback fires on task
completion. ~300 LOC including the upcall plumbing.

### Java

- `ForStRsIncrementalSnapshotStrategy` — implements
  `SnapshotStrategy<SnapshotResult<KeyedStateHandle>, SnapshotResources>`.
- `ForStRsIncrementalRestoreOperation`, `ForStRsHeapTimersFullRestoreOperation`,
  `ForStRsNoneRestoreOperation`.
- `ForStRsDataTransferStrategy` (Copy + Reusable variants).
- `ForStRsStateDataTransfer` — the orchestrator that drives transfers via
  the strategy.
- `ForStRsPathContainer` — manages local-cache paths (port from forst,
  ~120 LOC).

### Wire-up

`ForStRsKeyedStateBackend.snapshot(...)` currently throws
`UnsupportedOperationException`. Wire the new strategy. `restore(...)` picks
a restore op based on the `StateHandle` shape.

## Tests

- Round-trip: open backend, insert N=10k entries, snapshot, close, restore from
  snapshot, verify all entries visible.
- Incremental: 2 successive snapshots; only changed SSTs re-uploaded.
- Concurrent transfer: 4 parallel uploads, all complete; no data loss.
- Callback delivery: verify the Java-side queue receives completion events.

## Bench gates

- 1 GB state snapshot in ≤ 30 s (local FS — measures engine + Java orchestration).
- Restore from 1 GB snapshot in ≤ 30 s.
- No regression on existing write-only benches when checkpointing is enabled.

## Risks

1. **Upcall stub lifetime** — the FFM upcall stub must outlive every transfer
   it might fire on. Mitigation: pin to the long-lived `VectorizedRuntime` arena.
2. **Transfer cancellation** — Flink may cancel a checkpoint mid-transfer.
   The FFI must support `frs_sst_transfer_cancel(token)` returning quickly;
   the engine drops the file partially uploaded and the next attempt restarts
   cleanly. Mitigation: return a token from `_async` calls.
3. **Snapshot vs concurrent writes** — engine snapshot already takes an MVCC
   snapshot so reads are stable, but a write that lands between
   `frs_db_snapshot` and `frs_create_incremental_checkpoint_at` could end up
   in the next checkpoint instead of this one. That's the intended behaviour
   but should be documented.
4. **SST file naming** — forst's incremental handle references SSTs by a
   stable file number; forst-rs uses the same scheme so resume across forst
   and forst-rs is feasible IF the schema versions match (a separate
   compat-cycle item; not in scope here).

## Sequencing

Within SP5: 5a (snapshot) → 5b (transfer) → 5c (restore). Each ships its own
spec-implementation cycle. 5a is the smallest and unlocks the most testing
(any restore op needs a working snapshot to exercise).

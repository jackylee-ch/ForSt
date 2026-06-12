# disagg-java — Phase-2 LINK-mode checkpoint adoption package (Flink side)

**Status:** adoption COPIES — the Flink repo is read-only for this track; these
fragments are pasteable into
`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/`.
Engine + FFI side is MERGED in this repo (see
`crates/forst-rs-ffi/src/lib.rs` section 8c and
`crates/forst-rs-ffi/tests/linked_checkpoint_ffi_it.rs`).

Design contract: `docs/superpowers/specs/2026-06-13-phase2-disaggregated-state-design.md`
(§3 link-based checkpoint, §9 decisions D1–D12).

## What this enables

| Today (upload mode) | LINK mode |
|---|---|
| checkpoint uploads every new SST byte-for-byte (`ForStRsSstUploader`) | checkpoint `link()`s the live set — O(files) metadata, ZERO data upload (measured flat 16 ms across a 16× state-size sweep) |
| restore downloads every SST | instant restore adopts physicals — O(files), ~12 ms flat |
| memtable durability = flush-on-barrier or full Arrow artifact | dual-mode: FLUSH (default) or WAL-DELTA (`frs_db_attach_wal`; unflushed tail captured as `WAL.delta`, replayed on restore) |

## New FFI surface (already merged, `forst-rs-ffi` section 8c)

```
frs_create_incremental_checkpoint_linked(db, snap, ckpt_id, base_id, out FrsLinkedCheckpointResult*)
frs_db_linked_checkpoint_result_free(out*)
frs_db_discard_linked_checkpoint(db, ckpt_id, out_unlinked*, out_physicals_deleted*)   // retried → NOT_FOUND
frs_db_open_from_linked_checkpoint_instant(ckpt_dir, target_dir, out_handle*)          // local FS
frs_db_open_from_linked_checkpoint_instant_remote(uri, cfg_json, cache_dir, cache_cap, ckpt_dir, target_dir, out*)
frs_db_adopted_residual(db, out u64*)        // 0 ⇒ weaned off the restore source
frs_db_attach_wal(db, wal_path)              // per-DB WAL-DELTA opt-in (env-free)
```

`FrsLinkedCheckpointResult` C layout (24 bytes, allocate ≥24):

```
struct FrsLinkedCheckpointResult {
    char*            manifest_path;       // 8  — engine-FS blob path (has bytes)
    FrsLiveFileList* linked_new_ssts;     // 8  — chk-namespace LOGICAL paths (metadata-only!)
    FrsLiveFileList* linked_shared_ssts;  // 8  — registry-registration hint split
};
```

The existing `readCString` / `readSstList` strategy helpers work unchanged
(same `FrsLiveFileList` shape); sizes in the list entries are the live-set
logical sizes.

## Integration files in this package

1. `ForStRsLinker.linked-fragment.java` — downcall bindings + wrappers for the
   7 entry points (paste into `ffm/ForStRsLinker.java`: declarations into the
   constructor bind block after `frsDbIncrementalCheckpointResultFree`,
   wrappers after `dbOpenFromIncremental`).
2. `LinkedSstStateHandle.java` — NEW class: the JM-side handle for a linked
   (metadata-only) SST path. Discard is DELEGATED (no-op on the JM); see
   protocol below.
3. `ForStRsSnapshotStrategy.link-mode-fragment.java` — the zero-upload branch
   of `asyncSnapshot` + config plumbing.
4. `ForStRsRestoreOperation.download-skip-fragment.java` — the instant-restore
   branch of `restoreNoRescaling`.

## Protocol decisions (binding for the integration)

- **D-J1 (discard delegation, paper §5.2 / design §9 D4).** A linked SST
  handle's JM-side `discardState()` is a NO-OP. Actual cleanup is TM-side:
  `notifyCheckpointSubsumed/Aborted(id)` → `linker.dbDiscardLinkedCheckpoint(db, id)`
  (manifest-driven unlink; physical deleted exactly once at refs==0). A
  TM that dies before the notification leaks the chk-k links — bounded by
  the startup sweep (`frs_db_sweep_abandoned_checkpoints`, see below):
  leak-over-data-loss per design R1.
- **D-J2 (link mode is gated to incremental sharing).** LINK mode applies
  only under `SharingFilesStrategy.FORWARD/FORWARD_BACKWARD`. NO_SHARING
  (full checkpoint / canonical savepoint) MUST keep the upload path — a
  self-contained snapshot cannot reference mapping-resolved physicals.
- **D-J3 (manifest still uploaded).** The CHECKPOINT.blob (KBs) keeps the
  existing EXCLUSIVE upload so Flink's metadata contract is untouched; SST
  DATA upload is what link mode eliminates. The restore side does NOT use
  the uploaded copy — it reads the blob from the chk dir on the engine FS.
- **D-J4 (Flink restore mode).** Instant restore implies Flink CLAIM-mode
  discipline: the restore-source checkpoint must stay retained until
  `frs_db_adopted_residual` reports 0 (compaction weans it naturally).
  NO_CLAIM requires the copy-restore (`open_from_linked_checkpoint`,
  not exposed via FFI yet) or waiting for residual==0 before first ckpt.
- **D-J5 (config keys).**
  - `forst.rs.checkpoint.link-mode: true` → strategy calls
    `createIncrementalCheckpointLinked` (explicit API; the engine
    auto-attaches the FileMappingManager — no `FRS_CKPT_LINK_MODE` env
    needed end-to-end).
  - `forst.rs.wal.dir: /local/nvme/frs-wal` → backend calls
    `linker.dbAttachWal(db, dir + "/db-" + subtaskId + ".wal")` at open
    (BEFORE serving writes; the native side seals+flushes pre-WAL state).
    With a WAL attached, linked checkpoints run WAL-DELTA automatically
    (design §9 D10); without it, FLUSH-on-barrier. 8c/32g boxes should
    stay FLUSH (recorded q4 trade-off: flush is load-bearing for memory).

## Correctness gate (MUST pass before any perf claim — recorded rule)

5M-event NexMark sweep, link-mode ON, fs-emulation or local:

1. `q0–q22` finish with per-query output counts EXACTLY equal to the
   rocksdb baseline (seeded datagen, same input bytes). Recorded baseline
   procedure: `nexmark/measure-sql.sh`; q3 exact count 2,201,068 at 5M is
   the historical reference assert.
2. Kill-and-restore IT on a stateful query (q4 or q9 class) at a mid-run
   checkpoint: restored counts byte-exact vs uninterrupted run; restore
   must take the instant path (assert no `_restore_dl` downloads).
3. WAL-DELTA cell: same two gates with `forst.rs.wal.dir` set.
4. Chained checkpoints: assert object-count (physical files) <
   handle-count across ≥3 checkpoints (the zero-reupload structural
   assert), and JM discard of ckpt N−2 deletes nothing while N references
   the physicals.
5. Existing upload-mode defaults UNCHANGED (flag off ⇒ byte-identical
   behavior) — run one q4 5M cell with the flag off as the control.

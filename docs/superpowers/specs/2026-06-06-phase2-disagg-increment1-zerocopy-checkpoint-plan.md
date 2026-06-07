# Phase 2 — Disaggregated State, Increment 1: zero-copy checkpoint registration

**Date:** 2026-06-06
**Status:** DESIGN (implement only after Phase-1 local gate: forst-rs total < rocksdb on q0–q22, all finished + accurate).
**Inputs:** Flink-2.0 disagg paper §5.2 / Fig. 8 (UFS hard-link checkpointing);
`2026-06-06-q4-rocksdb-parity-memory-model-design.md` §Phase 2;
`2026-06-05-forst-rs-disaggregated-state-and-nexmark-4x-gap-analysis.md` §P2;
ForSt backend (`ForStIncrementalSnapshotStrategy`, `DataTransferStrategyBuilder`,
`ReusableDataTransferStrategy`); current forst-rs engine.

## Goal of this increment

Eliminate the per-checkpoint **copy** of SST bytes on the checkpoint path. Today
forst-rs *stages a local copy* of every new SST per checkpoint
(`stage_checkpoint_artifacts_local`, db.rs) for the Java uploader, then the
uploader copies again to the checkpoint store. ForSt instead **hard-links /
registers** already-materialized state files into the checkpoint via the UFS,
turning checkpointing from data-intensive copy into a metadata operation
(paper §5.2: checkpoints complete in seconds regardless of state size; Fig. 9).

Increment 1 brings the *registration* semantics to the forst-rs local/remote
paths so a checkpoint references existing engine SSTs by handle+refcount instead
of copying their bytes — while keeping Flink's standard
`IncrementalRemoteKeyedStateHandle` contract (already emitted by
`ForStRsSnapshotStrategy`).

## What already exists (do not rebuild)

- `IncrementalCheckpointResult { new_ssts, shared_ssts }` — engine already
  distinguishes newly-created vs. base-shared SSTs (db.rs:360).
- `shared_ssts` are already referenced by handle, never re-staged
  (stage_checkpoint_artifacts_local stages only `new_ssts`).
- `FileDeletionGuard` / `PinHandle` (file_deletion_guard.rs) — engine-side pin
  refcounting so a referenced SST is not deleted under a live checkpoint.
- `FileSystem`/OpenDAL abstraction with `supports_atomic_rename()` and
  `await_upload()` — the local-vs-remote seam Phase 1 preserved.
- Bounded tiered `LocalCache` + `LocalFirstSstFile` (cached_fs.rs) — the hot
  cache tier the paper's local-cache layer needs.

## The gap (what Increment 1 builds)

1. **Reference-not-copy for the local UFS-compatible case.** When the engine FS
   and the checkpoint store are the same local filesystem (the local A/B config),
   `new_ssts` should be **hard-linked** into the checkpoint dir (or registered by
   absolute path) instead of byte-copied by the staging step. Add an engine API
   `register_checkpoint_artifacts` that, when `fs.supports_hardlink()` &&
   same-volume, links new SSTs into the checkpoint namespace and returns those
   paths; falls back to the existing copy-staging otherwise. This subsumes and
   then removes the GC'd copy from `2026-06-06-ckpt-staging-gc-fix.md`.
2. **Refcounted SST lifecycle / SharedStateRegistry authority.** Each SST gets a
   stable identity `(db_id, generation, file_number)` (not a temp-URI suffix).
   A checkpoint pins the identities it references; an SST is deletable only when
   no live checkpoint and no live version pins it (extend `FileDeletionGuard`).
   This is the object-state model (writing→visible→pinned→shared→obsolete) the
   P2 doc calls the "missing center."
3. **Java side:** `ForStRsSnapshotStrategy` registers linked/handle SSTs with the
   `SharedStateRegistry` so reused SSTs across checkpoints are not re-uploaded —
   mirroring `ReusableDataTransferStrategy`. No change to the operator/runtime
   contract (hard boundary: Flink runtime untouched).

## Correctness invariants (verified, not deferred)

- A checkpoint references only SSTs that are fully materialized (await_upload /
  fsync barrier already present) — no half-visible SST.
- A linked SST's bytes are immutable for the life of every checkpoint that pins
  it (LSM SSTs are write-once; compaction outputs are new file numbers).
- Refcount reaches zero ⇒ exactly one deletion; restore from a checkpoint that
  pins an SST must find it present (no premature GC). Tests assert link count and
  no-premature-delete under interleaved checkpoint/compaction.
- Byte-equivalence: a DB restored from a registered (linked) checkpoint reads
  identical values to one restored from the copy-staged checkpoint.

## Verification plan (independent, no S3 perf)

- Engine unit/integration tests (MemoryFileSystem + local OpenDAL `services-fs`):
  link-not-copy chosen on same-volume local; refcount pin/unpin; no-premature-GC
  under compaction; restore byte-equivalence vs. copy path.
- Functional benchmark (partial, local): checkpoint duration before/after on a
  heavy query (q4/q11) — expect a drop (copy → link). Accuracy: q0–q22 finish +
  row-count parity unchanged.
- S3 perf explicitly out of scope this phase (paper-deferred, goal Phase 3).

## Sequencing

This is increment 1 of the Phase-2 roadmap. Subsequent increments (separate
specs, each one-step-complete + documented + verified):
2. No-claim reusable recovery (map/link remote SSTs vs. full download) — Fig. 10.
3. Async coalesced remote reads + range prefetch on the tiered cache.
4. Remote-primary SST namespace + object lifecycle/lease + manifest publication.

Each must hold end-to-end vectorization / zero-copy / batch and pass accuracy +
(local) performance verification before integration.

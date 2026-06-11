# streaming-read-java — P0 Java drain copies (NEED INTEGRATION)

These are COPIES of files owned by the flink repo
(`/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/...`),
edited here because another session owns that repo. They implement the Java
half of P0 (EOF flag + auto-close) from
`docs/superpowers/specs/2026-06-11-streaming-read-redesign-design.md` §2.3.

## Files and deltas vs. the flink-repo originals (copied at flink HEAD of 2026-06-11)

1. `ForStRsLinker.java`
   (→ `.../forstrs/ffm/ForStRsLinker.java`)
   - `FRS_CHUNK_EOF = 1` constant (bit 0 of `FrsChunk._reserved`).
   - `FRS_CHUNK_RESERVED` VarHandle + `getFrsChunkReserved(chunks, i)` +
     `isFrsChunkEof(chunks, i)` accessors.

2. `ForStRsDBIterRequest.java`
   (→ `.../forstrs/ForStRsDBIterRequest.java`)
   - MAP_IS_EMPTY path: skip `frsVecIterPrefixClose` when `handle == 0`
     (engine auto-closed).
   - `process()` single-shot drain: `eofAtOpen = firstChunkFromOpen && handle == 0`
     → skip the mandatory trailing `next()` AND the `close()` (both the normal
     and the exception paths).
   - `processFromBatchedOpen(...)`: new 8-arg overload with `boolean eofAtOpen`
     (the probe's `FRS_CHUNK_EOF` bit); old 7-arg signature delegates with
     `false`. When set: skip drain-loop `next()` and `close()`.

3. `VectorizedExecutor.java`
   (→ `.../forstrs/VectorizedExecutor.java`)
   - `executeItersBatchedParallel`: read `ForStRsLinker.isFrsChunkEof(outChunks, i)`
     and pass it to the new `processFromBatchedOpen` overload.
   - `dispatchIterPrefixBatch` (legacy `IterPrefixRequest`/`FrsIterHandle` path)
     and `dispatchIterRange`: NO behavioral change needed (auto-close is
     backward compatible: `next` on an auto-closed/0 handle reports clean EOF,
     `close` is a no-op). Inline `P0 NOTE` comments mark the optional follow-up
     (skip FrsIterHandle/slotScope registration when EOF-flagged / handle==0).

## Engine-side contract these rely on (committed in this worktree, forst-rs FFI)

- Single-shot `frs_vec_iter_prefix_open` / `frs_vec_iter_range_open`: on
  exhausted-in-first-chunk with no pending error, `*out_handle = 0` and the
  iterator is auto-closed (never registered). Rows are still in the chunk.
- Batched `frs_vec_iter_prefix_open_batch[_parallel]`: same condition sets
  `FrsChunk._reserved |= FRS_CHUNK_EOF` and returns a fresh NON-ZERO,
  UNREGISTERED handle (non-zero because legacy Java treats handle==0 as a
  per-row failure).
- `frs_vec_iter_prefix_next` / `frs_vec_iter_range_next` on an unknown
  (auto-closed) handle: returns `Ok` + 0 rows (EOF) — was `IterCursorInvalid`
  (201). `_abort` keeps the 201-on-unknown contract (watchdog unchanged).
- EOF/auto-close is NEVER signalled when a deferred error is stashed
  (partial-chunk-then-error keeps the registered handle so `_next` surfaces
  the error).

## Integration checklist

- [ ] Apply the three diffs to the flink repo (diff against its current HEAD —
      these copies were taken 2026-06-11; rebase if the originals moved).
- [ ] Run the Java backend suite (114/0 expected) + q7 output exactness vs a
      RocksDB-seeded run; q3/q4 no-regression (design §4 Stage-0 gates).
- [ ] Watchdog review: auto-closed handles are never registered with
      IterLifetimeWatchdog (they are never wrapped in FrsIterHandle on the
      executeItersBatchedParallel path only when the follow-up note is taken).

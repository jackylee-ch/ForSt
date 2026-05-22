# Round 3 — Agent E — Flink Real-Time Streaming

**Date:** 2026-05-22
**Angle:** ckpt-barrier alignment / stop-with-savepoint / CheckpointStreamFactory hygiene / AsyncRetryStrategy / state schema evolution. Round 1 + Round 2 findings excluded.
**Method:** focused cold-read of `flink-statebackend-forst-rs/`. Targeted greps for the 5 brief topics; cross-checked against Flink 2.2 `CheckpointOptions.CheckpointType` / `TypeSerializerSnapshot` contracts.

---

## Summary

| Sev | Count |
|---|---|
| HIGH | 5 |

---

### E3-HIGH-1 — `CheckpointOptions` is silently ignored; SAVEPOINT/SYNC_SAVEPOINT/FORCED indistinguishable from periodic ckpt

**Files:**
- `ForStRsAsyncKeyedStateBackend.java:343-388` — `snapshot(id, ts, f, o)` never inspects `o`.
- `ForStRsSnapshotStrategy.java:158-163, 185-…` — `asyncSnapshot(...,CheckpointOptions checkpointOptions)` takes the param but never reads it (`isSavepoint()`, `getCheckpointType()`, alignment mode all unread).

**Evidence:** zero matches for `isSavepoint|getCheckpointType|CheckpointType\.SAVEPOINT|SYNC_SAVEPOINT|TERMINATE_WITH_SAVEPOINT` anywhere under `flink-statebackend-forst-rs/src/main/java/`. A savepoint trigger from the JM arrives via `o.getCheckpointType() == CheckpointType.SAVEPOINT` with the expectation that the backend produces the **canonical Flink savepoint format** (length-prefixed kv per key-group, restore-by-any-backend); incremental forst-rs SST handles do not satisfy this contract — they are project-private (`ForStRsIncrementalKeyedStateHandle`).

**Streaming impact:** users invoking `bin/flink savepoint <jobid>` or `bin/flink stop --savepoint` receive a "successful" savepoint blob that no other backend can read AND that the same job cannot restore from after a backend switch. Worse: the resulting handle masquerades as a normal incremental checkpoint, so a future restore that picks the savepoint will silently try to reuse SSTs that may have been GC'd from the SHARED scope at savepoint cutover. Cross-cluster job migration is impossible.

**Fix shape:** `if (checkpointOptions.getCheckpointType().isSavepoint()) { → drive FullSnapshotResources canonical path }` else continue with incremental. Even if the canonical path is V1.1, at minimum throw a clear "savepoint not yet supported by forst-rs backend" rather than silently emit a non-portable blob.

**Certainty:** HIGH.

---

### E3-HIGH-2 — `stop --savepoint` partial-drain on the **V2 async** path is bypassed; in-flight `StateRequest`s straddle the terminal barrier

**File:** `ForStRsAsyncKeyedStateBackend.java:343-388` (the `snapshot()` body) — three `flushDirty()` calls, all confirmed no-ops (Round 1 E-HIGH-4). The V2 abstract `savepoint()` throws `UnsupportedOperationException` (Round 1 E-HIGH-1) but `stop --savepoint` triggers via the *checkpoint coordinator* using `CheckpointType.SYNC_SAVEPOINT_TERMINATE_WITH_SAVEPOINT`, which routes through `snapshot()` — NOT through `savepoint()`. So the UnsupportedOperationException is never thrown; the no-op flush returns success.

**Streaming impact:** `bin/flink stop -d -p s3://.../sp <jobid>` on a V2 job returns success without:
1. Awaiting in-flight async-state continuations (E-HIGH-4 already on file).
2. Distinguishing this as a *terminal* barrier — after a SYNC_SAVEPOINT all input is drained and tasks finish; any unflushed RMW accumulator (`ForStRsReducingStateV2`/`AggregatingStateV2` open accumulators) is silently lost because the engine SSTs were not produced and the backend is `close()`'d on task finish.

**Fix shape:** in `snapshot()`, detect `o.getCheckpointType() == SAVEPOINT_TERMINATE` and synchronously block on a real future from each `VectorizedExecutor` (replace no-op `flushDirty` with `awaitInFlight()`). Same fix unblocks E-HIGH-4.

**Certainty:** HIGH.

---

### E3-HIGH-3 — No participation in `AsyncRetryStrategy`; one transient S3 4xx aborts the entire checkpoint

**Files:**
- `ForStRsSstUploader.java:63-78, 85-97` — `uploadBlocking` opens one `CheckpointStateOutputStream` and lets any `IOException` propagate as exceptional completion of the future. No retry wrapper.
- `ForStRsRestoreOperation.java:238-285` — `downloadHandleStrict` opens `handle.openInputStream()` once; any I/O fault throws `ForStRsCheckpointRestoreException` strict-fail.

**Evidence:** zero matches for `Retry|AsyncRetryStrategy|RetryStrategy|RetryPredicate` in the entire `flink-statebackend-forst-rs/` module. Flink 2.2 ships `AsyncRetryStrategies` (`org.apache.flink.streaming.util.retryable.AsyncRetryStrategies`) — the canonical retry surface for async state ops; the backend does not consult it. The SST uploader's virtual-thread executor (line 66-77) wraps the blocking call but does not retry transient failures.

**Streaming impact:** in a noisy BOS/S3 environment (typical: 0.1-1% 503 rate under load), every checkpoint has a 1-(1-p)^N chance of failure where N = num SSTs uploaded. For N=200 SSTs and p=0.005, that's a 63% per-ckpt fail rate. Each failed ckpt counts toward `tolerable-failed-checkpoints`; job restart-loops. This converts a recoverable storage hiccup into job downtime.

**Fix shape:** wrap `uploadBlocking` in a bounded exponential backoff (3-5 tries, 100ms→2s) for `IOException` whose cause indicates retryable storage error. Same shape on `downloadHandleStrict`. Flink-canonical: take a `AsyncRetryStrategy` from `ReadableConfig` so the user can tune.

**Certainty:** HIGH (architectural gap; failure mode well-known from RocksDB-on-S3 operations).

---

### E3-HIGH-4 — No state schema evolution: changing a `TypeSerializer` after restore reads garbage, not a controlled error

**Files:**
- `ForStRsAsyncKeyedStateBackend.java:199-251` — `getOrCreateKeyedState` / `createStateInternal` constructs state classes using **the descriptor's current serializer** with zero consultation of any restored `TypeSerializerSnapshot`.
- `ForStRsRestoreOperation.java:138-231` — `restore()` reads `cfMap` (state-name → column-family-id) but never reads `RegisteredKeyValueStateBackendMetaInfo` or `StateMetaInfoSnapshot` from the manifest blob. Greps confirm zero hits for `TypeSerializerSnapshot|RegisteredKeyValueStateBackendMetaInfo|StateMetaInfoSnapshot|resolveSchemaCompatibility|isCompatibleAfterMigration` in the whole module.

**Streaming impact:** classic Flink scenario — user evolves a POJO state field (adds a column to `Bid` record). RocksDB backend detects the serializer change via `TypeSerializerSnapshot.resolveSchemaCompatibility`, runs a migration sweep, or fails fast with `StateMigrationException`. forst-rs's V2 state classes read raw bytes from the engine and hand them to the **new** serializer, which deserializes garbage (likely an `EOFException` or worse, a silently mis-aligned record producing wrong-typed fields). The error surfaces somewhere downstream as a random `ClassCastException` or wrong-value-in-business-logic — debugging this in production is multi-day.

**Fix shape:** persist `StateMetaInfoSnapshot` per state into the engine manifest (or alongside it as an `EXCLUSIVE` blob). On restore, for each registered state, call `restoredSnapshot.resolveSchemaCompatibility(newSerializer)`; if `incompatible` throw `StateMigrationException` (fail fast — matches RocksDB), if `requiresMigration` run the migration sweep, if `compatibleAfterReconfiguration` adopt the reconfigured serializer. **Minimal**: just persist + check + fail-fast; migration sweep can come later.

**Certainty:** HIGH.

---

### E3-HIGH-5 — `CheckpointStreamFactory` is used correctly BUT scope choice for the manifest is wrong; cross-ckpt manifest sharing breaks restore-after-subsume

**File:** `ForStRsSnapshotStrategy.java:215-217`

```java
CompletableFuture<StreamStateHandle> manifestFut =
        uploader.upload(manifestPath, streamFactory, CheckpointedStateScope.EXCLUSIVE);
```

**Evidence:** the manifest is uploaded with `CheckpointedStateScope.EXCLUSIVE`, which means **per-checkpoint deletion**: when checkpoint N is subsumed by N+1, the EXCLUSIVE blobs of N are GC'd. The `ForStRsIncrementalKeyedStateHandle` stores a reference to the manifest blob in `metaDataStateHandle` — this handle survives in the JM's completed-checkpoint store for retained checkpoints, but the underlying EXCLUSIVE blob has been deleted by the storage layer when N was subsumed.

Compare RocksDB: the metadata blob is also EXCLUSIVE, BUT the RocksDB backend's `RocksIncrementalSnapshotStrategy` writes a **fresh metadata for every checkpoint**, not a delta — so the deletion of N's metadata when N+1 supersedes is safe (N+1 has its own metadata).

For forst-rs the same is true IF `dbIncrementalCheckpointResultFree` truly produces a complete (not delta-w.r.t.-base) manifest. Reading `createIncrementalCheckpointAt(db, snapshot, ckptId, baseCkptId, result)` (line 194-199) — the `baseCheckpointId` argument suggests the manifest IS a delta from the base. If the manifest is a delta and N's manifest is GC'd on subsumption, then a retained-checkpoint restore from N+k (k>0) cannot reconstruct the LSM history.

**Streaming impact:** `state.checkpoints.num-retained=3` + `RETAIN_ON_CANCELLATION` users who try to restore from the 2nd-most-recent checkpoint hit `ForStRsCheckpointRestoreException("Strict restore: handle for 'CHECKPOINT.blob' produced 0 bytes")` (the strict empty-blob check at line 263-269 catches this case but reports it as "deleted upstream", not as the design bug).

**Fix shape:** either (a) make `createIncrementalCheckpointAt` produce a fully-self-describing manifest (no `baseCheckpointId` dependency) and keep EXCLUSIVE scope, or (b) use `CheckpointedStateScope.SHARED` for the manifest and rely on registry ref-counting to keep it alive across subsumption. Option (a) is cleaner; (b) bloats SST registry.

**Certainty:** MED-HIGH (depends on engine-side manifest format; flagged because `baseCheckpointId` arg strongly suggests delta semantics).

---

## Cross-references

| New | Round 1 / Round 2 link |
|---|---|
| E3-HIGH-1 (savepoint type ignored) | NEW; orthogonal to R1 E-HIGH-1 (savepoint() throws) — R1 covered the `savepoint()` method; this covers the `snapshot(o)` path that JM actually uses for `stop --savepoint` |
| E3-HIGH-2 (SYNC_SAVEPOINT terminal drain) | EXTENDS R1 E-HIGH-4 (no in-flight await) with the terminal-barrier-specific corruption mode |
| E3-HIGH-3 (no retry) | NEW |
| E3-HIGH-4 (no schema evolution) | NEW |
| E3-HIGH-5 (manifest scope) | NEW; orthogonal to R1 E-CRIT-1 (empty snapshot) which would block this from manifesting until V1.1 |

---

## Recommendation ordering

1. **E3-HIGH-4** — schema evolution fail-fast. One-day add: persist `StateMetaInfoSnapshot`, check on restore, throw `StateMigrationException`. Without it, every user-driven serializer change is a silent data corruption.
2. **E3-HIGH-1 + E3-HIGH-2** — inspect `CheckpointOptions` in `snapshot()`. Distinguish SAVEPOINT/SYNC_SAVEPOINT_TERMINATE; either drive canonical path or fail fast. Couples with R1 E-HIGH-1.
3. **E3-HIGH-3** — wrap upload/download in `AsyncRetryStrategy`. Operational maturity unlock for S3-backed deployments.
4. **E3-HIGH-5** — validate manifest-is-self-describing (or migrate to SHARED scope). Wakes up only after R1 E-CRIT-1 lands.

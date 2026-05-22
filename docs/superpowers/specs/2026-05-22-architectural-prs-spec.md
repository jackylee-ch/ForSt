# ForSt-RS — Architectural PR Decomposition Spec

**Date:** 2026-05-22
**Purpose:** Decompose the 63 architectural HIGH issues into implementable PRs (each ~1-3 engineer-days). Each PR has scope, files, tests, dependencies, and acceptance gate.
**Estimated total:** 25-30 engineer-days for 1 engineer; 15-20 days for 2-engineer team in parallel.
**Companion docs:** `2026-05-22-all-high-issues-catalogue.md` (issue list), `2026-05-22-high-issue-remediation-spec.md` (phase design), `2026-05-22-forst-rs-readiness-and-perf-roadmap.md` (VP view).

**Cumulative surgical fixes already shipped this session (11):**
- R1: A1-H5, C-H4, D-H1
- R2: A2-H1, A2-H2, A2-H3
- R3: A3-H1, A3-H2, S1-7 (drainTo rename), V2-9 (MemorySegment.mismatch), C-R2-H1 (Vec capacity)

**63 remaining HIGHs** are organized into **27 PRs** below, ordered by dependency.

---

## Phase A — Correctness / Durability (12 PRs, blockers)

### PR-A1: `snapshot()` real impl with engine FFI
**Closes:** S1-1, S1-2, S1-3, S1-9, E3-HIGH-5
**Days:** 3
**Files:**
- `keyed/ForStRsAsyncKeyedStateBackend.snapshot()` (rewrite)
- `VectorizedExecutor.flushDirty()` (implement; drain all executors' in-flight batches + state buffers + timer queues)
- `ffm/ForStRsLinker.frsCreateCheckpoint(...)` (new FFM binding — engine already has frs_create_checkpoint)
- `keyed/ForStRsSnapshotStrategy.java` (wire SST → IncrementalKeyedStateHandle)
- New `restore/ForStRsRestoreOperation.restoreFromHandle(...)` (sister method)
**Tests:**
- `SnapshotRestoreCorrectnessTest`: 10K events → snapshot → kill TM → restart → verify state.
- `IncrementalCheckpointTest`: 3 successive snapshots, verify only delta SSTs upload.
- `PreSnapshotFlushTest`: in-flight LIST_ADD batch + snapshot → restore returns those values.
**Acceptance:** Round-N Agent A sees S1-1/2/3/9 closed.
**Dependencies:** None.

### PR-A2: V2 keyed-state namespace encoding
**Closes:** S1-4 (E2-CRIT-1)
**Days:** 2
**Files:**
- `state/ForStRsValueStateV2.serializeKey` (append namespace bytes)
- `state/ForStRsAsyncMapStateV2.serializeKey`
- `state/ForStRsAsyncListStateV2.serializeKey`
- `state/ForStRsAsyncReducingStateV2.serializeKey`
- `state/ForStRsAsyncAggregatingStateV2.serializeKey`
- Each class needs `TypeSerializer<N> namespaceSerializer` field
**Tests:**
- `MultiNamespaceCollisionTest`: same key + 2 namespaces → distinct values.
- Snapshot-restore from v3.x: documented break; release notes; new test confirms loud failure not silent corruption.
**Acceptance:** Round-N Agent E sees E2-CRIT-1 closed.
**Dependencies:** None.

### PR-A3: V1-sync key-group routing wired through ForStRsKeyedStateBackend constructor
**Closes:** S1-6 (E-CRIT-3)
**Days:** 1.5
**Files:**
- `keyed/ForStRsKeyedStateBackend.java` — add `KeyGroupRange keyGroupRange, int numberOfKeyGroups` to constructor params
- `offheapKeyGroupSupplier` now uses `KeyGroupRangeAssignment.assignToKeyGroup(getCurrentKey(), numberOfKeyGroups)`
- Update all test instantiations (~5 sites)
**Tests:** `V1SyncRescaleTest` — parallelism 4 → 8, verify all keys hit correct keygroup.
**Dependencies:** None.

### PR-A4: ForStRsKeyGroupedInternalPriorityQueue currentKeyGroup supplier accepts InternalKeyContext
**Closes:** S1-5 (E2-CRIT-2)
**Days:** 2
**Files:**
- `timer/ForStRsKeyGroupedInternalPriorityQueue.java` — add `InternalKeyContext<?> keyContext` constructor param; the existing `currentKeyGroupSupplier` becomes derived
- `keyed/ForStRsAsyncKeyedStateBackend.create(...)` — pass the backend's keyContext
**Tests:** `MultiKeygroupTimerFireTest` — parallelism 2, 4 keygroups; fire timers in 3 distinct keygroups; verify all 3 deliver.
**Dependencies:** None.

### PR-A5: ValueStateV2 cache slot keyed by stateName
**Closes:** S1-10 (A1-H6)
**Days:** 1
**Files:**
- `state/ForStRsValueStateV2.java` — replace single-slot `RecordContext.extra` write with `Map<String, byte[]>` keyed by stateName.
**Tests:** `MultiValueStateOperatorTest` — 2 ValueStates in same operator with same currentKey, distinct values → reads correct.
**Dependencies:** None.

### PR-A6: MapStateCache clear hook via setCurrentNamespace
**Closes:** S1-11 (E2-HIGH-2), A3-H3
**Days:** 2
**Files:**
- `state/ForStRsMapStateV2.java` — override `setCurrentNamespace(N)`; when namespace changes, invalidate cache entries for the prior namespace.
- `cache/MapStateCache.java` — add `clearForNamespace(N namespace)` API.
- Since `asyncClear()` is `final` on AbstractKeyedState, we cannot override it. Workaround: clear-on-namespace-change covers the window-end case; clear-on-key-change covers operator key transitions.
**Tests:** `MapStateCacheClearStateTest` — populate cache, asyncClear via framework (which triggers namespace transition), verify no stale reads.
**Dependencies:** PR-A2 (namespace encoding).

### PR-A7: State TTL respect (decorator + cleanup hook)
**Closes:** S1-12
**Days:** 3
**Files:**
- `keyed/ForStRsAsyncKeyedStateBackend.getOrCreateKeyedState` — read `desc.getTtlConfig()`, wrap state.
- New `state/ttl/TtlAwareValueStateV2.java`, `TtlAwareMapStateV2.java`, etc. (decorators).
- Hook into Flink's background cleanup mechanism (`TtlStateFactory` pattern).
**Tests:** `StateTtlExpiryTest` × per state type — write at t=0, read at t > TTL → null.
**Dependencies:** None.

### PR-A8: Stop-with-savepoint correctness
**Closes:** E3-HIGH-2
**Days:** 1.5
**Files:**
- `keyed/ForStRsAsyncKeyedStateBackend.snapshot()` — branch on `CheckpointType.SYNC_SAVEPOINT`; full barrier-await + RMW drain before returning the handle.
- Implement real `savepoint()` (currently throws UnsupportedOperationException).
**Tests:** `StopWithSavepointTest` — Flink job under load, stop --savepoint, verify no in-flight requests lost.
**Dependencies:** PR-A1 (need real snapshot path first).

### PR-A9: CheckpointOptions handling (SAVEPOINT vs CHECKPOINT branching)
**Closes:** E3-HIGH-1, E3-HIGH-5 (incremental scope)
**Days:** 1.5
**Files:**
- `snapshot()` — read `CheckpointOptions.getCheckpointType()`; for SAVEPOINT, emit canonical Flink savepoint format (not proprietary forst-rs blob); for CHECKPOINT, use incremental SHARED scope.
**Tests:** `SavepointPortabilityTest` — savepoint loadable by community ForSt with documented caveats.
**Dependencies:** PR-A1, PR-A8.

### PR-A10: Per-row failure propagation for PUT/DELETE/GET batches
**Closes:** S1-9 fully (extends A1-H5 to non-APPEND_MERGE paths)
**Days:** 2
**Files:**
- `VectorizedExecutor.executePuts/Deletes/Gets` — adopt same per-row future-check pattern that A1-H5 added for APPEND_MERGE.
**Tests:** `BatchedPutFailurePropagationTest` etc.
**Dependencies:** None.

### PR-A11: TypeSerializerSnapshot integration (state migration)
**Closes:** E3-HIGH-4
**Days:** 3
**Files:**
- `state/*StateV2.java` — expose `getStateSerializer()` and integrate with Flink's `TypeSerializerSnapshot` framework.
- On state-read with mismatched serializer-config, throw `StateMigrationException` per Flink contract.
**Tests:** `SerializerEvolutionTest` — change serializer signature, verify migration or clean exception.
**Dependencies:** PR-A1 (real snapshot format).

### PR-A12: AsyncRetryStrategy for S3 SST upload/download
**Closes:** E3-HIGH-3
**Days:** 2
**Files:**
- `restore/ForStRsRestoreOperation` — wrap SST download in `AsyncRetryStrategy` with bounded retries + exponential backoff.
- `snapshot()` — same for SST upload.
- Rust side: verify opendal client retry middleware is enabled.
**Tests:** S3 fault-injection test (latency + transient errors) — verify retry vs fail-fast.
**Dependencies:** PR-A1, PR-A8.

---

## Phase B — V1 Vectorization Fast Paths (4 PRs)

### PR-B1: V5 zero-copy GET-result decode
**Closes:** V2-6, C-H1, C-H6, V2-10 (Linker Segment overloads)
**Days:** 2
**Files:**
- New default method on `ForStRsInnerTable`: `Object deserializeValue(MemorySegment buf, long offset, int len)`.
- `VectorizedExecutor.executeGets` — pass MemorySegment slice directly to the new overload.
- Each state class overrides with `MemorySegmentDataInputView` (already exists for V1-sync).
- `Linker.{get,put,delete}Segment` — true MemorySegment FFM signature (no byte[] marshaling internally).
**Tests:** `MemorySegmentGetParityTest` — random keys round-trip through old vs new path produce equal results.
**Dependencies:** None.

### PR-B2: SIMD hash + critical-mode FFI
**Closes:** V2-11, D-H2, D-R3-1, D-R3-3, V2-15 (write_chunk_into_buf SoA)
**Days:** 2.5
**Files:**
- `ArrowBinaryBuffer.hash` — `LongVector.fromMemorySegment(SPECIES_PREFERRED, ...)` blocks + scalar tail.
- `ArrowTimerBuffer.hashOf` — same pattern (Q12 timer hot path).
- `FlatStateCache.readInt/writeInt` — `MethodHandles.byteArrayViewVarHandle(int[].class, BIG_ENDIAN)`.
- `Linker.bind*()` — add `Linker.Option.critical(true)` for `frsVecMergeAppendBatch`, `frsVectorizedBatchPut`, `frsVectorizedBatchDelete`, `frsBatchGetArrow` (verify each is non-blocking).
- Rust `write_chunk_into_buf` — switch to SoA `[lens[N]][bytes[N]]` layout.
**Tests:** criterion `hash_128b`, `timer_hash_64b`, `int_pack_unpack` ≥ 2× scalar baseline.
**Dependencies:** None.

### PR-B3: V1-sync ValueState heap-alloc cleanup
**Closes:** B3-H3, B3-H4
**Days:** 1
**Files:**
- `state/ForStRsKeyGroupedSerializer.encodeForState/Map` — cache `stateName.getBytes(UTF_8)` once at registration.
- `state/ForStRsValueState.update/getAndUpdate` — drop `getCopyOfBuffer()` on V1-sync hot path.
**Tests:** `EncodeForStateZeroAllocTest` (allocation-counting via JOL).
**Dependencies:** None.

### PR-B4: JAVA_INT_UNALIGNED dialect consistency
**Closes:** D-R3-2
**Days:** 0.5
**Files:** `VectorizedExecutor.java`, `ColumnarBatchBuffer.java` — all `JAVA_INT` indexed-offset accesses → `JAVA_INT_UNALIGNED`. Matches the Linker side.
**Tests:** `OffsetSegmentAlignmentTest`.
**Dependencies:** None.

---

## Phase C — MapState V2 + ListState V2 off-heap (3 PRs, BIGGEST PERF LEVERAGE)

### PR-C1: MapState V2 ArrowBinaryBuffer (per-instance)
**Closes:** V2-8, Z3-6, C-H5 (V8 in audit), V2-5 (recordAppendMerge 4-copy), C-H7
**Days:** 4
**Files:**
- New `state/MapStateArrowBuffer.java` (extends ArrowBinaryBuffer pattern; per state-instance).
- `state/ForStRsMapStateV2.asyncPut/asyncGet/asyncContains/asyncRemove` — route through MapStateArrowBuffer fast-path.
- Pre-snapshot flush hook (extends b3b9d7f2a6c pattern).
- `ForStRsMapStateV2.deserializeUser{Key,Value}` — use MemorySegmentDataInputView (V8 fix).
**Tests:** `MapStateArrowBufferParityTest` — random-op sequence equivalent to V1-sync MapState. `Q16BenchGateTest` ≤ 150 s.
**Acceptance:** Q16 ≤ 150 s (audit-design Phase C target).
**Dependencies:** PR-A2, PR-B1.

### PR-C2: ListState V2 off-heap (replicate C1 pattern)
**Closes:** V2-14, B2-H3 (combine_slices per-key alloc — partially)
**Days:** 3
**Files:**
- Same shape as PR-C1 for ListState V2.
- ListState format already Format B (multi-chunk); just route through off-heap buffer.
**Tests:** Q19BenchGateTest ≤ 60 s.
**Acceptance:** Q19 ≤ 60 s (audit-design Phase C target).
**Dependencies:** PR-A2, PR-B1, PR-C1.

### PR-C3: Reducing/Aggregating V2 RMW cache + flushOnBarrier
**Closes:** B3-H1, B3-H2, V12 in audit
**Days:** 3
**Files:**
- `state/ForStRsAsyncReducingStateV2.java` — implement the documented (but missing) RMW cache.
- `state/ForStRsAsyncAggregatingStateV2.java` — same.
- Both classes' `flushOnBarrier()` integrates with `ForStRsAsyncKeyedStateBackend.snapshot()`.
**Tests:** `ReducingStateRmwCacheTest` parity vs naive read-modify-write.
**Acceptance:** Reducing/Aggregating-heavy queries see ≥1.3× lift (Q4/Q11 measure points).
**Dependencies:** PR-A1.

---

## Phase D — Rust Engine S3 Zero-copy (5 PRs)

### PR-D1: opendal Bytes everywhere (storage layer)
**Closes:** Z3-2, Z3-3, Z3-4
**Days:** 3
**Files:**
- `crates/forst-rs-storage/src/opendal_*.rs` — replace `Vec<u8>` with `bytes::Bytes` throughout.
**Tests:** Criterion `s3_read_64MB` ≥ 1.5× lift.
**Dependencies:** None.

### PR-D2: SST reader/writer streaming + `Arc<Bytes>` rows
**Closes:** Z3-10, Z3-11, C-R3-H1, C-R3-H2, C-R3-H3
**Days:** 4
**Files:**
- `crates/forst-rs-engine/src/sst/reader.rs` — return `RowView<'a>` borrowing from `Arrow BinaryArray`.
- `crates/forst-rs-engine/src/sst/writer.rs` — stream to opendal sink instead of buffering full Vec.
- `compaction.rs`, `flush.rs`, `memtable/vectorized.rs` — same pattern.
**Tests:** Criterion `sst_write/100MB` ≥ 2× throughput. End-to-end SST round-trip parity test.
**Dependencies:** PR-D1.

### PR-D3: Engine `multi_get` for batched GET
**Closes:** V2-2 (V10 in audit), C-R2-H2 (related)
**Days:** 1.5
**Files:** `crates/forst-rs-ffi/src/lib.rs:frs_vectorized_batch_get` — replace per-key db.get loop with `db.multi_get(keys)`.
**Tests:** Criterion `batch_get/1024` ≤ 10 µs.
**Dependencies:** None.

### PR-D4: Chunked iter zero-clone + per-thread handle registry
**Closes:** V2-4 (B-H4), B2-H2, B2-H5
**Days:** 3
**Files:**
- `crates/forst-rs-ffi/src/lib.rs:frs_vec_iter_prefix_open` — no longer materialize full prefix; maintain engine cursor.
- iter handle registry → per-thread index instead of global `Mutex<HashMap>`.
**Tests:** Multi-slot concurrent iter open test (no contention).
**Dependencies:** None.

### PR-D5: WAL/checkpoint streaming + bloom filter zero-copy
**Closes:** Misc Rust engine paths flagged in Round 3 Agent C
**Days:** 2
**Files:** WAL path (if any), bloom filter loader, index loader — apply Bytes pattern.
**Dependencies:** PR-D1.

---

## Phase E — Flink Streaming Semantics (4 PRs)

### PR-E1: Parallel SST restore + batched copyKeyGroup
**Closes:** F5-6 (E-HIGH-2), E2-HIGH-3
**Days:** 3
**Files:**
- `restore/ForStRsRestoreOperation.restoreWithRescaling` — `ForkJoinPool` parallel SST downloads.
- `copyKeyGroup` path → `frs_batch_put` (no per-record FFM).
**Tests:** Rescale 4 → 8 completes ≤ 30s.
**Dependencies:** PR-A1.

### PR-E2: Async dispatch in-flight parallelism
**Closes:** F5-3 (E-HIGH-3)
**Days:** 2.5
**Files:** `VectorizedExecutor.executeBatchRequests` — return early; complete container future from worker thread.
**Tests:** Async dispatch micro shows ≥4 in-flight ops/dispatcher.
**Dependencies:** PR-A1.

### PR-E3: Batched iter prefix_open
**Closes:** E-HIGH-5 (F5-4)
**Days:** 1.5
**Files:** New `frs_vec_iter_prefix_open_batch` FFI; `VectorizedExecutor.executeIters` calls it.
**Dependencies:** PR-D4.

### PR-E4: V1-sync state-class `getCopyOfBuffer` mass-purge
**Closes:** Z3-related, B3-H4 (covers update/getAndUpdate which PR-B3 partially addressed; this is the full sweep)
**Days:** 2
**Files:** All V1-sync state classes (Value, Map, Reducing, Aggregating) — 30+ getCopyOfBuffer() call sites.
**Dependencies:** PR-B1.

---

## Phase F — Fold-in Cleanup (5 PRs)

### PR-F1: Classifier perfect-hash dispatch
**Closes:** V2-7 (B-H7)
**Days:** 1.5
**Files:** `VectorizedClassifier.offer` switch → precomputed dispatch table.
**Dependencies:** None.

### PR-F2: Manual pointer arithmetic → typed FFM layouts
**Closes:** J4-6 (D-R2-2)
**Days:** 1
**Files:** `ffm/ForStRsLinker.java` — 6+ sites; replace magic-number offsets with `ValueLayout.ADDRESS.withTargetLayout(FRS_BYTES_LAYOUT)`.
**Dependencies:** None.

### PR-F3: Off-heap MapStateCache (open-addressed + clock-sweep)
**Closes:** V2-8 (V7 in audit), B3-H5 (LinkedHashMap relink), V2-9 ext.
**Days:** 2
**Files:** `cache/MapStateCache.java` — replace `LinkedHashMap accessOrder=true` with open-addressed hash table backed by MemorySegment.
**Dependencies:** PR-B1.

### PR-F4: JMH benchmark rewrite (observability)
**Closes:** B3-JMH (NOT a HIGH but key observability gap)
**Days:** 2
**Files:** `flink-state-backends/flink-statebackend-forst-rs/src/test/java/.../jmh/` — actually use `@Benchmark`, exercise `vectorizedBatchPut`, `executeBatchRequests`, `MapStateCache`, V2 async dispatch.
**Dependencies:** None.

### PR-F5: dispatchAppendMergePerRow dead-code removal
**Closes:** V2-1 (B-H1, D-H3)
**Days:** 0.5
**Files:** `VectorizedExecutor.dispatchAppendMergePerRow` — delete if no caller after Round 3 verification.
**Dependencies:** None.

---

## Cross-cutting MEDIUM follow-ups (3 PRs, bundleable)

### PR-M1: Anonymous Arena.ofShared leak fix
**Closes:** D-R2-5
**Days:** 0.5
**Files:** `keyed/ForStRsKeyedStateBackend.java:228` — ThreadLocal of `(Arena, MemorySegment)` pair; close arena in `close()`.

### PR-M2: HashSet race conditions audit
**Closes:** Various MED race-condition findings.
**Days:** 1
**Files:** `listStateNames` etc. — ConcurrentHashMap.newKeySet() audit + ensure publish-after-init semantics.

### PR-M3: getCopyOfBuffer() bulk migration
**Closes:** 30+ MEDIUM sites from R1 Agent C
**Days:** 1
**Files:** Bulk find-and-replace in V1-sync serialize paths after PR-B1 lands.

---

## Dependency-respecting schedule

```
Week 1  (3-engineer team, 5 days):
  PR-A1 (3d)        PR-A4 (2d)        PR-D1 (3d)
  PR-A2 (2d)        PR-A5 (1d)        PR-D3 (1.5d)
  PR-A3 (1.5d)      PR-A10 (2d)       PR-D4 (3d)
                    PR-B1 (2d)
                    PR-B2 (2.5d)
                    PR-B3 (1d)
                    PR-B4 (0.5d)

Week 2:
  PR-A6 (2d)        PR-D2 (4d)        PR-A11 (3d)
  PR-A7 (3d)        PR-D5 (2d)        PR-E2 (2.5d)
  PR-A8 (1.5d)
  PR-A9 (1.5d)

Week 3:
  PR-A12 (2d)       PR-C1 (4d)        PR-E1 (3d)
                    PR-C2 (3d)        PR-E3 (1.5d)
                    PR-C3 (3d)        PR-E4 (2d)
                    PR-F1..F5 (7d)

Week 4 (cleanup + bench):
  PR-M1..M3 (2.5d)
  Full Q0-Q23 × 3-backends sweep
  Production-readiness sign-off
```

**Single-engineer total:** 27 PRs × avg 2.0 days = ~54 days
**Three-engineer parallel:** ~15-20 days

---

## Acceptance gate to terminate the multi-round review

After all 27 PRs land:
- Round N+1: Agents A, E find ≤ 2 HIGH (residual MEDIUM only)
- Round N+2 to N+5: 0 HIGH per round
- Then the multi-round review terminates per the "5 consecutive clean rounds" criterion specified in the original brief.

---

## What to bench WHEN

Per user directive: **no bench until all 63 architectural HIGHs land.**

After PR-A1..A12 land (Phase A complete):
- Smoke bench Q11/Q12/Q15 to verify durability+correctness fixes didn't regress perf.
- No portfolio-wide bench yet.

After all 27 PRs land (Phases A-F complete):
- Full Q0-Q23 × 3-backends sweep.
- Expected: 20/23 wins vs RocksDB; Q19 ≥ 2.6× rocksdb; Q16 ≥ 2.4×.
- Production-readiness review sign-off.

---

## Cross-references

- Issue catalogue: `2026-05-22-all-high-issues-catalogue.md`
- Phase design: `2026-05-22-high-issue-remediation-spec.md`
- VP roadmap: `2026-05-22-forst-rs-readiness-and-perf-roadmap.md`
- Audit-design baseline: `2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`
- v3.8 bench: `2026-05-21-forst-rs-benchmark-report-v3.8.md`

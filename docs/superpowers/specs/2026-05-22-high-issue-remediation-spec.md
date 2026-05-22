# ForSt-RS — HIGH-Issue Remediation Spec

**Date:** 2026-05-22
**Companion catalogue:** `docs/superpowers/specs/2026-05-22-all-high-issues-catalogue.md` (52+ HIGH/CRIT enumerated)
**Goal:** Close all open HIGH/CRIT findings via 6 phased work streams. Each phase has clear scope, files, acceptance gates, risk surface, and dependencies. After all 6 phases land, the multi-round review's "5 consecutive clean rounds" termination becomes reachable.

---

## §0 — Phase overview & dependency graph

```
Phase A (correctness durability)
  ├── snapshot() real implementation                     [S1-1, S1-2, S1-3, S1-9]
  ├── V2 namespace encoding                              [S1-4]
  ├── Timer kgSupplier correctness                       [S1-5, S1-6]
  ├── MapStateCache clear hook                           [S1-11]
  └── ValueStateV2 cache slot keying                     [S1-10]

Phase B (V1 vectorization fast paths)                    depends on: none
  ├── executeGets zero-copy decode (V5)                  [V2-6 / Z3-1]
  ├── MemorySegment.mismatch + ByteVector hash           [V2-9]
  └── critical(allowHeapAccess) on batched FFIs          [V2-11]

Phase C (MapState V2 + ListState V2 off-heap)            depends on: Phase A namespace fix
  ├── MapState V2 ArrowBinaryBuffer (per-instance)        [V2-8, audit V2]
  ├── ListState V2 off-heap (V11 in audit)               [V2-14, B2-H3]
  └── Slice-decoder for iter entries (V8)                [Z3-6, C-H5]

Phase D (Rust engine S3 zero-copy)                       depends on: none (engine-side)
  ├── frs_vectorized_batch_get → engine multi_get        [V2-2 / V10 audit]
  ├── frs_vec_iter_prefix_open chunked + per-thread      [V2-4, V2-12]
  ├── SST writer/reader Arc<Bytes> instead of Vec        [Z3-2..4, Z3-9..11, C-R3-H1/2/3]
  └── write_chunk_into_buf SoA layout                    [V2-15 / B2-H4]

Phase E (Flink streaming semantics)                       depends on: Phase A
  ├── State TTL respect                                  [S1-12]
  ├── savepoint() implementation                         [F5-1]
  ├── restoreWithRescaling parallel SSTs                 [F5-6 / E-HIGH-2]
  ├── executeIters batched prefix_open                   [F5-4]
  └── Async dispatch in-flight parallelism                [F5-3]

Phase F (Misc / fold-in)                                 depends on: Phase B
  ├── Classifier perfect-hash dispatch                   [V2-7]
  ├── Linker.{get,put,delete}Segment true zero-copy      [V2-10]
  ├── Manual pointer arithmetic → typed layouts          [J4-6]
  └── ArrowTimerBuffer.drainTo rename + ordered overload [S1-7]
```

Phases A and D are independent and can run in parallel; B is a prerequisite for C and F.

---

## §1 — Phase A: Correctness & Durability (BLOCKER)

**Goal:** Close every Section-1 HIGH/CRIT finding (S1-1..S1-12 minus the already-fixed S1-8). State that survives Flink TM restart with correct cross-window isolation.

### A.1 — Real `snapshot()` impl

**Closes:** S1-1, S1-2, S1-3, S1-9

**File:** `flink-state-backends/.../keyed/ForStRsAsyncKeyedStateBackend.java:344-388`

**Design:**
1. Drain all in-flight batches via a real `VectorizedExecutor.flushDirty()` that:
   - Walks `managedExecutors`; for each, if `lastDispatchedFuture` is incomplete, await it (blocking — Flink barrier path is allowed to block briefly).
   - Walks all registered V1-sync state classes with an `ArrowBinaryBuffer` and calls each one's `flushToEngine()`.
   - Walks all timer queues and calls their existing `flushPendingToEngine()` hook.
2. Call engine FFI `frs_create_checkpoint(checkpoint_id, target_dir)` → returns a list of SST file paths.
3. Wrap those SST paths in a `KeyedStateHandle` (use `IncrementalKeyedStateHandle` for incremental support) and return via `RunnableFuture` that completes successfully.
4. Register the SST handles for `notifyCheckpointComplete` ref-counting.

**Tests:**
- `SnapshotRestoreCorrectnessTest`: 10K events → snapshot → kill TM → restart → verify all 10K events readable.
- `IncrementalCheckpointTest`: verify SSTs are uploaded incrementally not fully re-shipped each time.

**Acceptance:** Round 4 Agent A confirms S1-1, S1-2, S1-3 closed.

### A.2 — V2 namespace encoding

**Closes:** S1-4

**Files:** `state/ForStRsValueStateV2.java`, `state/ForStRsAsyncMapStateV2.java`, `state/ForStRsAsyncListStateV2.java`, `state/ForStRsAsyncReducingStateV2.java`, `state/ForStRsAsyncAggregatingStateV2.java` (`serializeKey` methods)

**Design:**
Storage key layout becomes: `KEY_PREFIX + key + SLASH + stateName + SLASH + namespace_bytes`

- New `TypeSerializer<N> namespaceSerializer` field per state class
- `serializeKey` writes `keyOut.write(namespaceBytes(request))` between stateName and the trailing SLASH

Backward-compatibility: this is a hard format break — document in release notes. Snapshot-restore from v3.x snapshots will not work unless the namespace was VoidNamespace (since it's a no-op encoding).

**Tests:**
- `MultiNamespaceCollisionTest`: same key, two different namespaces, two distinct ValueStates → verify writes don't collide.

**Acceptance:** Round 4 Agent E confirms E2-CRIT-1 closed; new test green.

### A.3 — Timer / state-key key-group correctness

**Closes:** S1-5, S1-6

**Files:**
- `keyed/ForStRsAsyncKeyedStateBackend.java:436` — replace `() -> keyGroupRange.getStartKeyGroup()` with a per-event lookup using Flink's internal key→keygroup mapping: `KeyGroupRangeAssignment.assignToKeyGroup(currentKey, totalKeyGroups)`.
- `keyed/ForStRsKeyedStateBackend.java:232` — same fix for V1-sync `offheapKeyGroupSupplier`.

**Tests:**
- Rescale test: parallelism 4 → 8, verify all timers + state retain correct keygroup assignment.

**Acceptance:** Round 4 Agent E confirms E-CRIT-3, E2-CRIT-2 closed.

### A.4 — MapStateCache `asyncClear()` hook

**Closes:** S1-11

**Files:** `state/ForStRsAsyncMapStateV2.java` (`asyncClear`), `cache/MapStateCache.java` (add `clearForKey(operatorKeyContext)` method)

**Design:**
- Override `asyncClear()` to invoke `mapStateCache.clearForKey(getCurrentKeyContext())` before delegating to `super.asyncClear()`.
- `clearForKey` removes all cache entries for the given operator-key from the LinkedHashMap.

**Tests:** `MapStateCacheClearStateTest` — populate cache for window W → asyncClear() → verify no stale entries.

**Acceptance:** Round 4 Agent E confirms E2-HIGH-2 closed.

### A.5 — ValueStateV2 cache slot keying

**Closes:** S1-10

**Files:** `state/ForStRsValueStateV2.java` (`serializeKey`)

**Design:** Cache slot keyed by `(operatorKeyContext, stateName)` tuple. Either:
- (a) Use `RecordContext.extra` as a `Map<String, byte[]>` keyed by `stateName`; or
- (b) Allocate distinct `RecordContext.extra` slots per state at registration time.

Option (a) is simpler; option (b) is faster but requires Flink-runtime cooperation.

**Tests:** `MultiValueStateOperatorTest` — two distinct ValueStates in the same operator with same `currentKey`, distinct values → verify reads return correct per-state values.

**Acceptance:** Round 4 Agent A confirms S1-10 closed.

### A.6 — State TTL respect

**Closes:** S1-12

**File:** `keyed/ForStRsAsyncKeyedStateBackend.java` `getOrCreateKeyedState` (and V1-sync sibling)

**Design:**
- Read `desc.getTtlConfig()` at state-creation time
- If non-null, wrap the underlying state in a TTL-aware decorator that:
  - On write: stores `(value, expiryTimestamp)` tuple
  - On read: checks expiry vs current event/processing time
  - Lazy cleanup via Flink's existing TTL background cleanup mechanism

**Tests:** `StateTtlExpiryTest` — write at t=0 with 100ms TTL → read at t=200ms returns null.

**Acceptance:** Round 4 Agent E confirms S1-12 closed.

### Phase A acceptance gate

After A.1–A.6 land:
- `SnapshotRestoreCorrectnessTest` green
- `MultiNamespaceCollisionTest` green
- Rescale test green
- `MapStateCacheClearStateTest` green
- `MultiValueStateOperatorTest` green
- `StateTtlExpiryTest` green
- Round 4 Agent A + Agent E find 0 new HIGH in Sections 1, 5

**Wall-clock estimate:** 5-7 engineer-days

---

## §2 — Phase B: V1 Vectorization Fast Paths

**Goal:** Close V2-6, V2-9, V2-11 — the universal-impact zero-copy & SIMD wins on the GET-result + hash paths.

### B.1 — V5 zero-copy GET-result decode

**Closes:** V2-6, Z3-1, Z3-7

**Files:** `VectorizedExecutor.executeGets` (lines 321-322), state-class `deserializeValue` methods

**Design:**
- New overload `Object deserializeValue(MemorySegment buf, long offset, int len)` on `ForStRsInnerTable`. Default impl falls back to byte[].
- `MemorySegmentDataInputView` (already exists for V1-sync) becomes the canonical zero-copy decoder.
- Each state class overrides `deserializeValue(MemorySegment, off, len)` to use the view.

### B.2 — `MemorySegment.mismatch()` + `ByteVector` hash

**Closes:** V2-9, J4-5

**Files:** `buffer/ArrowBinaryBuffer.java` (hash, keysEqual), `timer/ArrowTimerBuffer.java` (hashOf, rowKeyEquals)

**Design:**
- `keysEqual(seg1, off1, seg2, off2, len)` → `seg1.asSlice(off1, len).mismatch(seg2.asSlice(off2, len)) < 0 || >= len`. JIT-friendly intrinsic since JDK 22.
- `hash(seg, off, len)` → unroll into `LongVector.fromMemorySegment(LongVector.SPECIES_PREFERRED, ...)` blocks; fall back to scalar for tail.

### B.3 — `Linker.Option.critical(allowHeapAccess=true)` on batched FFIs

**Closes:** V2-11, D-H2

**Files:** `ffm/ForStRsLinker.java` — `frsVecMergeAppendBatch`, `frsVectorizedBatchPut`, `frsVectorizedBatchDelete`, `frsBatchGetArrow`

**Design:**
- Add `Linker.Option.critical(true)` for all batched ops that take packed memory layouts.
- Verify each FFI function does NOT block on internal flush/network — required by `critical` mode.
- Heap-access allowed for the small offset arrays; the value bytes are off-heap.

### Phase B acceptance gate

- New `MemorySegmentDataInputViewSmokeTest`
- Criterion micro: `hash_128b` < scalar baseline by ≥ 2×
- Round 4 finds 0 new HIGH in V2-6, V2-9, V2-11

**Wall-clock:** 3-4 engineer-days

---

## §3 — Phase C: MapState V2 + ListState V2 off-heap (audit-design Phase C)

**Goal:** Close audit-design V2/V11/V12 + V2-8/V2-14 + Z3-6/Z3-8. Targets Q16, Q19 bench wins.

**Reference:** The original audit-design spec at `2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md` §5 V2/V11/V12 already laid out the design. This remediation just runs the existing plan.

### C.1 — Per-instance MapState V2 ArrowBinaryBuffer

**Closes:** V2-8 / audit V2

**Files:** `state/ForStRsAsyncMapStateV2.java`, new `state/MapStateArrowBuffer.java` extending the ArrowBinaryBuffer pattern.

**Design:** Per state-instance buffer (NOT cross-state-shared — v3.3 attempt regressed Q5 to 586s). Pre-flush hook before snapshot.

### C.2 — ListState V2 off-heap (audit V11)

**Closes:** V2-14, B2-H3

**Files:** `state/ForStRsAsyncListStateV2.java`

**Design:** Replicate C.1 pattern for ListState; integrate with APPEND_MERGE dispatch.

### C.3 — Slice-decoder for iter entries

**Closes:** Z3-6, C-H5 (audit V8)

**Files:** `state/ForStRsAsyncMapStateV2.deserializeUser{Key,Value}`

**Design:** Replace `byte[] buf = new byte[rangeLen]` with `MemorySegmentDataInputView` reading directly off the iter chunk buffer.

### Phase C acceptance gate

- Q16 ≤ 150 s (Phase C target)
- Q19 ≤ 60 s (Phase C target)
- No regression >5% on any other Nexmark query
- Round 4 Agent B/C finds 0 new HIGH in V2-8, V2-14, Z3-6

**Wall-clock:** 7-10 engineer-days (largest phase)

---

## §4 — Phase D: Rust engine S3 zero-copy

**Goal:** Close Z3-2/3/4/9/10/11 + V2-2/4/12/15 + C-R3-H1/H2/H3. Rust engine zero-copy from S3 read through SST through Java FFI.

### D.1 — opendal Bytes everywhere

**Closes:** Z3-2, Z3-3, Z3-4, Z3-9

**Files:** `crates/forst-rs-storage/src/opendal_*.rs`, `cached_fs.rs`

**Design:** Replace `Vec<u8>` with `bytes::Bytes` throughout the storage layer. `Bytes` is ref-counted and slice-able without memcpy. opendal's `Buffer` type is already `Bytes`-compatible.

### D.2 — SST reader/writer Arc<Bytes>

**Closes:** Z3-10, Z3-11, C-R3-H1, C-R3-H2, C-R3-H3

**Files:** `crates/forst-rs-engine/src/sst/reader.rs`, `sst/writer.rs`, `compaction.rs`, `flush.rs`, `memtable/vectorized.rs`

**Design:**
- SST reader returns `RowView<'a>` borrowing from the underlying Arrow `BinaryArray`.
- SST writer streams to opendal sink instead of buffering the whole file in `Vec<u8>`.
- Memtable scan returns Iterator<RowView<'a>>.
- Compaction streams entries from inputs to outputs without buffering.

### D.3 — `frs_vectorized_batch_get` engine multi_get

**Closes:** V2-2 / V10 in audit

**Files:** `crates/forst-rs-ffi/src/lib.rs:~2415-2436`

**Design:** Replace per-key `db.get` loop with `db.multi_get(keys)` which the engine pipelines through the block cache.

### D.4 — chunked iter zero-clone + per-thread handles

**Closes:** V2-4, V2-12

**Files:** `crates/forst-rs-ffi/src/lib.rs` iter handle registry, `frs_vec_iter_prefix_open`

**Design:**
- iter handle becomes a raw pointer or thread-local index — eliminates the global `Mutex<HashMap>`.
- `frs_vec_iter_prefix_open` no longer materializes full prefix; returns first chunk + maintains the iterator's internal cursor.

### D.5 — `write_chunk_into_buf` SoA wire format

**Closes:** V2-15 / B2-H4

**Files:** `crates/forst-rs-ffi/src/lib.rs` `write_chunk_into_buf`, Java-side `IterPrefixBatchBuffer` reader

**Design:** Switch from `[len][bytes][len][bytes]...` to `[lens[]][bytes[]]` SoA. Java side reads lens via `IntVector` + bytes via direct slice.

### Phase D acceptance gate

- criterion: `batch_get/1024` ≤ 10 µs
- criterion: `sst_write/100MB` shows ≥2× throughput improvement
- S3 SSE-C transfer benchmark shows ≥1.5× throughput
- Round 4 Agents B/C find 0 new HIGH in Sections 3, 4 Rust-side

**Wall-clock:** 8-12 engineer-days (Rust-side)

---

## §5 — Phase E: Flink streaming semantics

**Goal:** Close F5-1/2/3/4/6 + S1-12. Depends on Phase A snapshot impl.

### E.1 — savepoint() implementation

**Closes:** F5-1

**Files:** `keyed/ForStRsAsyncKeyedStateBackend.java`

**Design:** Implement using the same SST-handle pattern as A.1 but with `CheckpointType.SAVEPOINT` metadata. Output canonical Flink savepoint format so the savepoint is loadable by other backends (with documented data format limitations).

### E.2 — restoreWithRescaling parallel SSTs

**Closes:** F5-6 / E-HIGH-2

**Files:** `restore/ForStRsRestoreOperation.java`, `keyed/ForStRsAsyncKeyedStateBackend.java`

**Design:**
- Parallel `dbOpen` + SST downloads via a `ForkJoinPool` keyed on source-handle.
- Use `frs_batch_put` for the copyKeyGroup path (no per-record FFM).

### E.3 — Async dispatch in-flight parallelism

**Closes:** F5-3

**Files:** `VectorizedExecutor.executeBatchRequests`

**Design:**
- Return early after submitting the batch; complete the container future from a worker thread when all per-row futures resolve.
- Flink's async-state V2 framework already supports the async-return contract.

### E.4 — executeIters batched prefix_open

**Closes:** F5-4

**Files:** `VectorizedExecutor.executeIters`, new Rust FFI `frs_vec_iter_prefix_open_batch`

**Design:** Multiple prefix opens in a single FFI; engine pipelines block-cache loads.

### Phase E acceptance gate

- `SavepointLoadCrossBackendTest` (savepoint produced by forst-rs loads in community ForSt — with documented caveats)
- Rescale 4→8 completes in ≤30s (vs current serial unknown)
- Async dispatch micro shows in-flight parallelism ≥4 ops/dispatcher

**Wall-clock:** 6-8 engineer-days

---

## §6 — Phase F: Fold-in cleanup

**Goal:** Close V2-7, V2-10, J4-6, S1-7 + selected MEDIUMs.

### F.1 — Classifier perfect-hash dispatch
- Files: `VectorizedClassifier.java` offer switch
- Design: replace switch+contains with a precomputed dispatch table indexed by state-name hash

### F.2 — Linker.{get,put,delete}Segment true zero-copy
- Files: `ffm/ForStRsLinker.java`
- Design: bind separate FFI symbols that take MemorySegment offsets directly (no byte[] marshaling)

### F.3 — Manual pointer arithmetic → typed layouts
- Files: `ffm/ForStRsLinker.java` (6+ sites cited in J4-6)
- Design: `ValueLayout.ADDRESS.withTargetLayout(FRS_BYTES_LAYOUT)` for typed-pointer access

### F.4 — ArrowTimerBuffer.drainTo rename + ordered overload
- Files: `timer/ArrowTimerBuffer.java`
- Design: rename current implementation to `drainUnordered`; add a new `drainTo` that pops via removeMin in strict timestamp order

### Phase F acceptance gate
- All Round 4 reviewers find ≤2 HIGH across the entire codebase
- Bench portfolio holds the v3.8 baseline ±5%

**Wall-clock:** 3-5 engineer-days

---

## §7 — Total budget & sequencing

| Phase | Wall-clock | Parallelizable? |
|---|---:|---|
| A — Correctness/Durability | 5-7 days | No (blocking) |
| B — V1 Vectorization | 3-4 days | Yes, with A |
| C — MapState/ListState V2 off-heap | 7-10 days | After A.2 (namespace) |
| D — Rust engine zero-copy | 8-12 days | Yes, with A,B,C |
| E — Flink streaming semantics | 6-8 days | After A.1 |
| F — Fold-in cleanup | 3-5 days | After B |

**Sequential minimum:** A (7d) + C (10d) + E (8d) + F (5d) = 30 engineer-days.
**Parallel (B + D running alongside):** ≈ 20 engineer-days for 2-engineer team.

---

## §8 — Multi-round review termination

After Phases A–F land:
- Expected Round N+1: ≤2 HIGH (residual MEDIUMs only)
- Round N+2 to N+5: 0 new HIGH expected per round → multi-round review terminates per the "5 consecutive clean rounds" rule.

If new HIGHs surface during Phases A–F, append them to the catalogue and integrate into the active phase.

---

## §9 — Cross-references

- Catalogue: `docs/superpowers/specs/2026-05-22-all-high-issues-catalogue.md`
- Round 1 reports + summary: `docs/superpowers/specs/review-rounds/round-1-*.md`
- Round 2 reports + summary: `docs/superpowers/specs/review-rounds/round-2-*.md`
- Round 3 reports (partial): `docs/superpowers/specs/review-rounds/round-3-*.md` (incremental as agents return)
- Prior audit-design (Phase B/C/D/E source): `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`
- v3.8 bench baseline: `docs/superpowers/specs/2026-05-21-forst-rs-benchmark-report-v3.8.md`
- v20 ListState format unification: `docs/superpowers/specs/2026-05-22-v20-liststate-format-unification-design.md`

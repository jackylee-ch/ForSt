# Round 4 — Reviewer B (End-to-End Vectorization), post-27-PR verification

Scope: Section 2 + Round-3 deltas (B3-H1…B3-H6). Java files under
`/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs/`;
Rust FFI in `crates/forst-rs-ffi/src/lib.rs`.

HIGH count: **6** (3 carried-over Round-3 deltas not closed by the 27 PRs + 3 new regressions
introduced by the Round-3 fixes themselves).

---

## Per-PR verification status (PASS / FAIL)

- **PR-F1 — `VectorizedClassifier.offer` 5-way switch.** **PASS.** `DispatchKind[] DISPATCH_TABLE`
  built once in static init at `VectorizedClassifier.java:354-388`; `offer()` line 406 does a
  single `DISPATCH_TABLE[type.ordinal()]` load + 5-case switch (`GET/PUT/DELETE/ITER/APPEND_MERGE_CANDIDATE`).
  V2-7 closed.
- **PR-B1 — `deserializeValue(MemorySegment, off, len)` overrides.** **PASS for the override existence,
  PARTIAL for the alloc claim.** All five V2 classes override:
  `ForStRsValueStateV2.java:227`, `ForStRsMapStateV2.java:432`, `ForStRsAsyncListStateV2.java:431`,
  `ForStRsAsyncReducingStateV2.java:407`, `ForStRsAsyncAggregatingStateV2.java:395`. Each instantiates
  `new MemorySegmentDataInputView()` **per row** inside the batched-GET loop (see below — B4-H1).
- **PR-B2 — `Linker.Option.critical(true)` on the four batch handles + FlatStateCache VarHandle.**
  **PASS.** `bindCritical` calls at `ForStRsLinker.java:569 (frsBatchGetArrow)`, `:764 (frsVectorizedBatchPut)`,
  `:783 (frsVectorizedBatchDelete)`, `:1040 (frsVecMergeAppendBatch)`. `FlatStateCache.java:49-50` —
  `byteArrayViewVarHandle(int[].class, BIG_ENDIAN)` replaces the manual shift-pack readInt.
  V2-11/D-H2/V2-15 closed.
- **PR-C1 — `MapStateArrowBuffer` routing for ForStRsMapStateV2.** **PASS.** `offHeapBuf` field at
  `:103`; `asyncGet/Put/Remove/Contains` (`:209,236,274,287`) all probe/stage via `offHeapBuf` before
  falling through to `super`. V2-8 routing closed (cache layer issues — see B4-H1).
- **PR-C2 — `ListStateArrowBuffer` + classifier routing for List V2.** **PASS.** `ForStRsAsyncListStateV2.java:102`
  has the `buffer` field; `:236 buffer.append(...)`; `:170 buffer.flushTo(...)`. Classifier wires
  per-row off-heap arrays at `VectorizedClassifier.java:114, 139`. **Caveat:** the classifier-side
  gating predicate is a `Set<String>.contains(String)` per record — see B4-H3.
- **PR-D3 — `frs_vectorized_batch_get` routes through `db.batch_get`.** **PASS.**
  `crates/forst-rs-ffi/src/lib.rs:2420-2432` — single `keys_vec` built; one `db.batch_get(cf, &keys_vec)`
  call replaces the per-key loop. V2-2 closed.
- **PR-D4 — 16-shard handle registry + chunked open.** **PASS.** `lib.rs:3563-3578` —
  `ITER_SHARDS: OnceLock<[Mutex<HashMap>; ITER_SHARD_COUNT]>`, `ITER_SHARD_COUNT` power-of-two.
  `frs_vec_iter_prefix_open` (`:3651`) uses `fill_chunk_from_iter` directly into the caller's buffer
  (zero-clone first chunk) and `shard_for(handle_id).lock().unwrap().insert(...)` at `:3704-3707`.
  V2-4 / B2-H2 / B2-H5 closed.
- **PR-E3 — single batched `frsVecIterPrefixOpenBatch` FFI.** **PASS.** `VectorizedExecutor.java:888-971`
  builds SoA prefixes from the long-lived executor `arena` (no per-request `Arena.ofShared()`), then
  issues exactly one `linker.frsVecIterPrefixOpenBatch(...)` call (`:963-971`). Per-row failures
  propagated via `outHandles[i] == 0L`. F5-4 / E-HIGH-5 closed.
- **PR-F3 — off-heap MapStateCache.** **PASS.** `cache/MapStateCache.java:91-115` — fully off-heap
  via `Arena.ofShared()`: `keyOffsets / keyLengths / keyData / hashIndex / accessTime` are
  `MemorySegment`s; no `LinkedHashMap`, no `BytesKey` wrapper. `MemorySegment.mismatch` for key
  compare. V2-8 cache layer + B3-H5 closed.

---

## Section 2 + Round-3 deltas NOT closed by the 27 PRs

- **B4-H1 (carry from B3-H2)** `state/ForStRsAsyncReducingStateV2.java:427-440` and
  `ForStRsAsyncAggregatingStateV2.java:415-428` — `buildDBGetRequest` / `buildDBPutRequest` still
  allocate `byte[] key = serializeKey(request)` + `byte[] value = serializeValue(...)` + a fresh
  `new ForStRsDBGetRequest<>` / `new ForStRsDBPutRequest<>` wrapper on EVERY request. No PR in the
  27 routes these state classes through the `ColumnarBatchBuffer.append` path that
  `ForStRsValueStateV2`/`MapStateV2` use. Reducing/Aggregating V2 are still on the per-row alloc path.
- **B4-H2 (carry from B3-H4)** `state/ForStRsValueState.java:365-370` — the **buffered** V1-sync
  `update()` path still calls `computeKey()` (returns fresh `byte[]`) and then
  `outputBuffer.getCopyOfBuffer()` for the buffered-put accept. The Round-3 ticket noted "per-event
  two heap allocations on the path Q11 is pinned to"; PR-B3 only optimized the **immediate** path
  (`:374-381`, uses `getSharedBuffer()`) — the buffered path (the one Q11 actually rides per
  `MEMORY.md:project_q11_v1_sync_finding.md`) is unchanged.
- **B4-H3 (carry from B3-H6)** `VectorizedExecutor.java:333-341 executeRequestSync` — every sync
  request allocates `new VectorizedClassifier(getKeys, putKeys, putValues, deleteKeys)` (`:335`) and
  invokes `single.initNewKindBuffers(arena)` (`:341`) per call. The classifier carries 5
  `ColumnarBatchBuffer` references + 4 parallel object arrays (`get/put/delete/appendMergeRequests`),
  all freshly allocated then immediately discarded for a single-row request. No PR pooled this.

## NEW vectorization regressions introduced by the 27 PRs

- **B4-H4 (NEW — Set.contains with new String key per record, via PR-C2/PR-F1)**
  `VectorizedClassifier.java:90` declares `private final Set<String> listStateNames = ConcurrentHashMap.newKeySet()`
  and the APPEND_MERGE_CANDIDATE branch at `:438-439` does
  `String name = table.getStateName(); if (name != null && listStateNames.contains(name)) { ... }`
  on EVERY LIST_ADD / LIST_ADD_ALL request. `ConcurrentHashMap.newKeySet().contains` is a
  volatile-read + hash on the String + chain walk. This is exactly the pattern called out in the
  review brief's regression checklist ("`Set.contains` / `Map.get` with new String keys per record").
  Hot for Nexmark queries using list-typed state (Q5/Q8/Q11 ListState paths).
- **B4-H5 (NEW — per-row object alloc inside batched-GET loop, via PR-B1)** All five V2
  `deserializeValue(MemorySegment, long, int)` overrides allocate
  `new org.apache.flink.state.forstrs.v1sync.MemorySegmentDataInputView()` per row
  (`ForStRsValueStateV2.java:232-233`, `ForStRsMapStateV2.java`, `ForStRsAsyncListStateV2.java`,
  `ForStRsAsyncReducingStateV2.java:412-414`, `ForStRsAsyncAggregatingStateV2.java`).
  The view itself is heap-light (a couple of int fields + a MemorySegment ref), but the alloc
  happens N times per batched-GET — exactly the per-row alloc the PR claimed to eliminate.
  Should be a single reused view per `executeGets` invocation (analogous to the `reusableHit` pattern
  in MapStateCache).
- **B4-H6 (NEW — Map.get with String key per record, via PR-B3 carry-over in
  `ForStRsKeyGroupedSerializer`)** `keyed/ForStRsKeyGroupedSerializer.java:71-80` —
  `stateNameBytes(stateName)` is called from `encodeForState(int, K, String)` (`:96`) and
  `encodeForMap(int, K, String, ...)` (`:269`) on the heap path (V1 ValueState `computeKey`, all four
  V2 inner-table `serializeKey` calls when no off-heap variant is wired). It performs
  `STATE_NAME_BYTES_CACHE.get().get(stateName)` — a per-thread `HashMap.get(String)` lookup per
  record. PR-B3 only cached the byte[] result; the **lookup** is still a HashMap.get with a String
  key per event. ThreadLocal.get() also has its own cost. The off-heap variants
  (`encodeForStateOffheap`/`encodeForMapOffheap`, `:136, :197`) correctly take pre-encoded bytes,
  so the fix is to expose the byte[] overload uniformly and stop routing through the String overload
  from inner-table `serializeKey`.

---

## Cross-check: regression-checklist findings summary

| Regression class                                            | Where present                                           | Severity |
|-------------------------------------------------------------|---------------------------------------------------------|----------|
| Per-row `Arena.ofConfined()` outside long-lived arena       | NONE on the executor batched paths                      | clean    |
| Per-row `Arena.ofShared()` outside long-lived arena         | NONE on prefix-iter (PR-E3 closed it); `:1064` range-iter still does it but spec says iter-lifetime arena, not per-row alloc churn — accept | accept   |
| New `byte[]` allocations on per-event V2 path               | Reducing/Aggregating V2 buildDB*Request (B4-H1); MapStateV2 `serializeMapEntryKey` returns `getCopyOfBuffer()` at `:202` | HIGH     |
| Virtual call hotpoints inside batched loops                 | `deserializeValue(...)` is interface dispatch; mitigated because monomorphic per state class | accept   |
| `Set.contains` / `Map.get` with new String keys per record  | `listStateNames.contains(name)` (B4-H4); `STATE_NAME_BYTES_CACHE.get().get(stateName)` (B4-H6) | HIGH     |
| Per-row object alloc inside batched-GET loop                | `new MemorySegmentDataInputView()` per row (B4-H5)      | HIGH     |

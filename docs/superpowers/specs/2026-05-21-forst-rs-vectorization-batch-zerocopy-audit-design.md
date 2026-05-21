# ForSt-RS — Full Vectorization / Batch / Zero-Copy Audit + Remediation Design

**Date:** 2026-05-21
**Branch baseline:** `forst-rs-jdk25` @ `a5fd9f70dd6` (Flink), `forst-rs` @ `67d914558` (ForSt)
**Predecessor audit:** `2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md`
**Bench baseline:** `2026-05-21-forst-rs-benchmark-report-v3.8.md` (forst-rs wins 18/23 vs RocksDB; sub-1.0× on Q0, Q1, Q3, Q10, Q13; marginal Q16=1.83×, Q19=1.08×)
**Status:** design — no code lands from this document; implementation deferred to a writing-plans hand-off

> **Naming convention** — to avoid collision with `CONTRIBUTING.md`'s `L1/L2/L3/L4` benchmark-level vocabulary (engine micros / Flink backend / Stateful E2E / Nexmark), this document uses **Tier 1–5** for the code-layer model and reserves the `L`-prefix exclusively for benchmark levels.

---

## §0 — Goal, non-goals, assumptions

### Goal
Confirm that every per-event hot-path line in `flink-state-backends/flink-statebackend-forst-rs/` (Java) and the `forst-rs-ffi` Rust engine FFI surface fits the three architectural principles — **end-to-end vectorization**, **batch execution**, **zero memory copy**. For every line that does not fit, catalog the violation with file/line evidence and design a fix that preserves correctness, snapshot durability, and the four critical invariants from the batched-timer spec (add-remove cancellation, ordering, snapshot, advance() strict order).

### Non-goals
- **No code lands from this spec.** Implementation work is gated on a separate writing-plans hand-off.
- **No Flink-runtime changes.** Per user directive (carried from earlier sessions), all fixes must live inside `flink-state-backends/flink-statebackend-forst-rs/` and the forst-rs Rust engine. Flink-runtime issues (e.g., the absent `AsyncStateSlicingSharedWindowProcessor`) are documented and deferred upstream.
- **No new state types.** Audit covers existing Value / Map / List / Reducing / Aggregating; introducing new state types is out of scope.

### Hard assumptions (must hold before any fix proceeds)
1. **JDK 25 + G1GC required** — `MemorySegment.mismatch()` and the `--add-modules jdk.incubator.vector` SIMD intrinsics referenced by `VectorizedClassifier` are JDK 22+ APIs; JDK 25 is the validated minimum. `-XX:+UseCompactObjectHeaders` and ZGC remain prohibited (proven harmful in Q5/Q11/Q12 sessions).
2. **forst-rs config defaults at HEAD `a5fd9f70dd6`** — `forst.rs.timer-service.factory=FORSTRS` is the new code default (`PriorityQueueSetFactory.FORSTRS`); the batched off-heap timer ships with the backend. No external override required.
3. **V1-sync correctness assumption is NOT yet validated** — see §9 open item OQ-1. The "V1 sync ValueState is already off-heap correct" claim was twice falsified in this project's history (the PR-A revert; the always-on write-buffer breaking Q11/Q12). This audit cannot assume V1-sync is fix-free.
4. **Engine merge-operator semantics** — the `[count][elem*]` concatenation format used by `frsVecMergeAppend` is honored by the Rust merge-operator. Fixing V3 (ListState APPEND_MERGE wiring) depends on this; an end-to-end correctness test must accompany the wiring.

---

## §1 — Architectural principles (concrete & measurable)

The three principles are not aesthetic ideals; each has a falsifiable predicate that automated tooling could check.

| Principle | Concrete predicate | Counter-example |
|---|---|---|
| **End-to-end vectorization** | No per-event sync FFM/JNI crossing on the V2 async path. All state ops on the V2 async path land in `ColumnarBatchBuffer` and dispatch through `VectorizedExecutor.executeBatch()`. | `ForStRsLinker.put(byte[], byte[])` called from inside a per-record `processElement` — bypasses the columnar dispatcher. |
| **Batch execution** | No FFM call inside a per-event `for (req : reqs)` loop. The dispatched FFI primitive consumes the entire batch in one crossing. | `for (row : count) linker.frsVecMergeAppend(...)` — `VectorizedExecutor.dispatchAppendMerge` lines 356-440 today. |
| **Zero memory copy** | Per-event hot path performs zero `byte[] = new byte[len]` allocations; values flow through `MemorySegment` slices end-to-end (engine output → Java deserializer view) without intermediate copies. | `byte[] raw = new byte[len]; MemorySegment.copy(out, ..., raw, 0, len);` — `VectorizedExecutor` GET-result allocation. |

The three are independent dimensions. A code path can satisfy one and violate the others (e.g., batched FFM that still allocates `byte[]` per result violates zero-copy but satisfies batch). The §4 matrix tracks the orthogonal compliance per violation.

### Out-of-bound: per-call-cost regime
Per the prior `4.6 µs engine-per-call` finding ([[project_nexmark_q3_optimization]]), even a fully principle-compliant per-record path is bound by ~4.6 µs FFM crossing cost. Any audit finding that proposes "make per-record FFM faster" without batching is rejected — the regime correction is **always batch first, then optimize per-call cost only if batching is impossible**.

---

## §2 — Code-layer model: Tier 1 → Tier 5

Five tiers, ordered from "closest to user code" to "closest to the OS / engine":

| Tier | Name | Representative files | Responsibility |
|---|---|---|---|
| **T1** | **State API** | `state/ForStRs{Value,Map,List,Reducing,Aggregating}State{,V2}.java` | Implements Flink's `KeyedStateBackend` state-handle contracts. V1 = sync, V2 = async via `StateRequest`. |
| **T2** | **Cache** | `cache/MapStateCache.java`, `RecordContext.extra`, write-buffer caches | Per-state hot-path cache to amortize per-event lookups. |
| **T3** | **Off-heap buffer** | `buffer/ArrowBinaryBuffer.java`, `buffer/ArrowBinaryBufferAutoTuner.java`, `timer/ArrowTimerBuffer.java` | Off-heap Arrow `BinaryArray` (variadic bytes + offsets) + open-addressed `long→int` hashIndex; the zero-copy intermediate. |
| **T4** | **V2 dispatch + vectorized executor** | `ForStRsAsyncKeyedStateBackend.java`, `VectorizedExecutor.java`, `ColumnarBatchBuffer.java`, `VectorizedClassifier.java` | Accumulates V2 `StateRequest`s into a columnar batch; dispatches via batched FFM primitives. |
| **T5** | **Linker FFM + Rust engine** | `ffm/ForStRsLinker.java`, `crates/forst-rs-ffi/src/lib.rs`, the Rust engine | JNI/FFM crossing. Rust engine provides `frs_*` primitives. |

### Per-state-type hot operations (compact reference, embedded per §0(4))

| State type | Hot op @ per-event | T1 read path | T1 write path |
|---|---|---|---|
| ValueState (V1 sync) | `value()` / `update()` | `ForStRsValueState.value()` → off-heap ArrowBinaryBuffer get | `update()` → ArrowBinaryBuffer insert (off-heap) — **principle-compliant since commit `537c1403f2f`** |
| ValueState (V2 async) | `asyncGet` / `asyncUpdate` | `ForStRsValueStateV2` → V2 dispatch path → vectorized batch | `asyncUpdate` → `serializeValue` → `byte[]` → enqueue (heap-copy, then batched) — **partial zero-copy violation** |
| MapState (V1 sync) | `get/put/contains/remove/iterator` | `ForStRsMapState` → ArrowBinaryBuffer get (1c.1 ship `633af3d3be1`) | `put` → ArrowBinaryBuffer insert |
| MapState (V2 async) | `asyncGet/Put/Contains/Remove/Entries/Keys/Values` | `ForStRsMapStateV2` → MapStateCache + `serializeMapEntryKey` (per-call `byte[]`) | `asyncPut` → `serializeValue` → `byte[]` → V2 batch — **no ArrowBinaryBuffer fast path** |
| ListState (V2 async) | `asyncGet/Add/Update/AddAll/Clear` | `ForStRsAsyncListStateV2.asyncGet` → full `ArrayList<V>` materialization | `asyncAdd` → full-list overwrite PUT (V3 root cause of Q19 regression) |
| ReducingState / AggregatingState (V2 async) | `asyncGet/Add` | Built on ValueState V2 internals — inherits its zero-copy gap | `asyncAdd` → reduce + asyncUpdate — inherits ValueState V2 write copy |

---

## §3 — Violations catalog

**Numbering:** Vn (where n is the violation id). Severity tiers: **CRIT / HIGH / MED / LOW**.

**Query-impact subcolumns** (per §0(4)):
- **Hit set** = which Nexmark queries traverse this code line at per-event rate.
- **Worst-case query** = the query with the largest current absolute wall-clock cost contribution attributable to this violation (named & in seconds).
- **Estimated relief** = seconds reclaimed if this violation is fully fixed (best-effort estimate from Amdahl + analogous fixes; ±50 %).
- **Certainty** = `HIGH` (similar fix proven elsewhere in this codebase), `MED` (clear principle but novel fix shape), `LOW` (depends on engine semantics not yet validated).

The **leverage score** is `worst_case_wallclock × relief_fraction × certainty_weight` where `certainty_weight = {HIGH: 1.0, MED: 0.6, LOW: 0.3}`. §5 ranks fixes by this score.

| ID | File:line | Tier | Principle violated | Severity | Hit set | Worst-case query | Est. relief (s) | Certainty | Leverage |
|---|---|---|---|---|---|---|---:|---|---:|
| **V1** | `state/ForStRsMapStateV2.java:114-130` (`serializeMapEntryKey()`) | T1 | zero-copy | MED | Q16, Q19, Q3 | Q16 (199.5 s) | 12 | HIGH | 12.0 |
| **V2** | `state/ForStRsMapStateV2.java:205-247` (`buildDBGetRequest` / `buildDBPutRequest`) | T1 | zero-copy | HIGH | Q16, Q19, Q3 | Q16 (199.5 s) | 50 | HIGH | 50.0 |
| **V3** | `state/ForStRsAsyncListStateV2.java:135-159` (`asyncAdd` → full-PUT) | T1 | batch | **CRIT** | Q19 | Q19 (135 s) | 55 | HIGH | 55.0 |
| **V4** | `VectorizedExecutor.java:356-440` (`dispatchAppendMerge` per-row loop) | T4 | batch | HIGH | Q19, future ListState users | Q19 (135 s after V3) | 25 | MED | 15.0 |
| **V5** | `VectorizedExecutor.java:313-326` (GET-result `byte[] = new byte[len]`) | T4 | zero-copy | HIGH | all V2 GET paths — Q16, Q19, Q11, Q12, Q15, Q17, Q21, Q22 | Q16 (199.5 s) | 30 | HIGH | 30.0 |
| **V6** | `state/ForStRsAsyncListStateV2.java:201-217` (full-`ArrayList` materialization on `asyncGet`) | T1 | zero-copy | MED | Q19 | Q19 (135 s, after V3) | 15 | MED | 9.0 |
| **V7** | `cache/MapStateCache.java:66-79` (heap `LinkedHashMap<BytesKey, Object>`) | T2 | zero-copy | MED | Q16, Q19 | Q16 (199.5 s) | 20 | MED | 12.0 |
| **V8** | `state/ForStRsMapStateV2.java:346-380` (slice-decoder still `byte[] buf = new byte[rangeLen]`) | T1 | zero-copy | LOW-MED | Q16 iterator path | Q16 (199.5 s) | 8 | HIGH | 8.0 |
| **V9** | `ffm/ForStRsLinker.java:2541-2542` (per-iter-step `MemorySegment.copy` to `byte[]`) — **carried from prior audit** | T5 | zero-copy | MED | Q9, Q20, Q16 iter | Q16 (199.5 s) | 10 | HIGH | 10.0 |
| **V10** | `crates/forst-rs-ffi/src/lib.rs:~2415-2436` (`frs_vectorized_batch_get` naive per-key db.get() loop) — **carried from prior audit** | T5 | batch | HIGH | all V2 batched GET | Q16 (199.5 s) | 35 | MED | 21.0 |
| **V11** | `state/ForStRsAsyncListStateV2.java` (NO `ArrowBinaryBuffer` fast path; T3 unused) | T1↔T3 | zero-copy + vectorization | HIGH | Q19 | Q19 (135 s after V3) | 30 | MED | 18.0 |
| **V12** | `state/ForStRs{Reducing,Aggregating}StateV2.java` (inherit ValueState V2 write-copy gap) | T1 | zero-copy | LOW-MED | Q4-style aggregations (already at 5.37× — secondary lever) | Q4 (46.9 s) | 5 | MED | 3.0 |
| **V13** | V2 async snapshot path — does the columnar buffer flush before the snapshot barrier? **Open**, see §9 OQ-2 | T4 | correctness (not principle) | gating | all V2 snapshots | n/a | n/a | n/a | n/a |
| **V14** | V1-sync code paths — claim "already off-heap correct" not validated since `537c1403f2f`; see §9 OQ-1 | T1 | unknown | unknown | Q5, Q13 (V1-sync HOP/JOIN) | Q13 (41.1 s) | unknown | LOW | TBD |

Total catalog: **14 violations** (8 from today's audit + 2 carried + 4 newly-derived; V13/V14 are gating items, not fix candidates).

---

## §4 — Orthogonal matrix: violation × tier

Rows = violations. Columns = the tier(s) in which the violation lives or where the fix must be applied. ✓ = the fix touches that tier. **bold** = the **dominant** tier (where the fix is most concentrated). The bottom row sums tier-touches weighted by violation leverage — the **tier-fix leverage** — turning the matrix into a decision tool: high tier-fix-leverage means "one focused refactor at that tier closes many violations."

| ID | Lev | T1 State API | T2 Cache | T3 Buffer | T4 V2/Executor | T5 Linker/Engine |
|---|---:|:---:|:---:|:---:|:---:|:---:|
| V1  | 12.0 | **✓** | — | — | — | — |
| V2  | 50.0 | **✓** | — | ✓ | ✓ | — |
| V3  | 55.0 | **✓** | — | — | ✓ | ✓ |
| V4  | 15.0 | — | — | — | **✓** | ✓ |
| V5  | 30.0 | ✓ | — | — | **✓** | — |
| V6  | 9.0 | **✓** | — | — | — | — |
| V7  | 12.0 | — | **✓** | ✓ | — | — |
| V8  | 8.0 | **✓** | — | — | — | — |
| V9  | 10.0 | — | — | — | — | **✓** |
| V10 | 21.0 | — | — | — | — | **✓** |
| V11 | 18.0 | **✓** | — | ✓ | — | — |
| V12 | 3.0 | **✓** | — | ✓ | — | — |
| **Tier-fix leverage** | **(Σ touched × lev)** | **185.0** | **12.0** | **83.0** | **150.0** | **101.0** |

### Reading the matrix
- **T1 (State API, 185) is the highest-leverage tier** — concentrating fixes at the V2 async state-class entry points closes V1/V2/V3/V5/V6/V8/V11/V12, i.e. 8 of the 12 fix-candidate violations. This validates §5's ranking: the first phase should rewrite the V2 async per-state-class entry to use the same off-heap pattern V1-sync ValueState already uses.
- **T4 (V2 dispatch/executor, 150)** comes second because V2/V3/V4/V5 all need the columnar dispatcher to learn new ops (append-merge batch, slice-decoder views). One executor refactor unlocks four violations.
- **T5 (Linker/Engine, 101)** carries the new Rust FFI primitives (V3 wiring of `frs_vec_merge_append_batch`, V10 fix of `frs_vectorized_batch_get`). These are engine-side and need new test coverage but the surface is small.
- **T3 (Off-heap buffer, 83)** is the cross-cutting refactor — V2/V7/V11/V12 want to **replicate the per-state-instance ArrowBinaryBuffer pattern** (one buffer per registered state, NOT one pool shared across states). The v3.3 cross-state-shared buffer attempt regressed Q5 to 586 s; the audit rejects that approach and adopts the per-instance pattern proven at `537c1403f2f`.
- **T2 (Cache, 12)** has lowest leverage — V7 is the only resident — meaning the cache fix can ship independently without coordinating with other phases.

---

## §5 — Per-violation fix design (ranked by leverage)

**Fix order** is the leverage column, descending:
**V3 (55) → V2 (50) → V5 (30) → V10 (21) → V11 (18) → V4 (15) → V1 (12) → V7 (12) → V9 (10) → V6 (9) → V8 (8) → V12 (3).**

Each subsection: (a) current data flow, (b) proposed data flow, (c) required Tier-by-Tier changes, (d) new engine FFI primitives if any, (e) test plan, (f) regression risk.

### Fix V3 — wire `ListState.asyncAdd` to APPEND_MERGE

(a) **Today**: `ForStRsAsyncListStateV2.asyncAdd(v)` serializes `v` to `byte[]`, builds a `StateRequest<MAP_PUT>` whose value payload is `[count=1][elem_bytes]`, and the classifier (`VectorizedClassifier.java:307-321`) routes it to `recordPut`. The engine receives a destructive full-PUT — the previously-stored list is overwritten with the single-element payload. To preserve list semantics, the code must first `asyncGet` the full list, append, and re-PUT — but the Flink-runtime `UpdatableTopNFunction` does that on the user's behalf, so each event becomes **GET (full list) + PUT (full list with +1 element)**.

(b) **Proposed**: `asyncAdd(v)` builds an `AppendMergeRequest` (already exists in the standalone V2 `ForStRsListStateV2.java:110-121` but unused from the Flink-runtime-integrated variant) carrying just the single-element payload `[count=1][elem_bytes]`. The classifier routes it to a new dispatcher path that issues `frs_vec_merge_append_batch(...)` (a batched form of the existing `frsVecMergeAppend`, see V4). The engine's merge-operator concatenates `[count][elem*]` segments without ever materializing the full list on the engine side.

(c) **Tier touches**:
- T1: new override `ForStRsAsyncListStateV2.asyncAdd` bypasses `super.asyncAdd` and uses `AppendMergeRequest`. Same change to `asyncAddAll`.
- T4: classifier registers ListState names in a new `appendMergeStates` set so it routes correctly; `VectorizedExecutor.dispatchAppendMerge` becomes the entry point.
- T5: see V4.

(d) **New engine FFI**: V3 alone uses the existing `frsVecMergeAppend` per-row primitive (acceptable as Phase 1 before V4 lands). Batching is V4.

(e) **Tests**: (1) snapshot/restore correctness — write 1000 entries to a single key via `asyncAdd`, snapshot, restart, verify `asyncGet` returns all 1000 in order. (2) interleaved add/clear/add — verify the merge state is properly cleared on `asyncClear`. (3) Nexmark Q19 wall-clock — gate ≤ 90 s on the bench.

(f) **Regression risk**: medium. The engine's merge-operator is exercised today only by the timer queue's compacted format; ListState merge correctness across snapshots is novel. Mitigated by snapshot/restore unit test.

### Fix V2 — MapState V2 async off-heap ArrowBinaryBuffer wiring

(a) **Today**: `ForStRsMapStateV2.buildDBPutRequest` serializes `value` to `byte[]` via `DataOutputSerializer`, packs into `ForStRsDBPutRequest`. The V2 dispatch later copies the byte[] into `ColumnarBatchBuffer` then into a confined `Arena`. Two heap-copies per put.

(b) **Proposed**: introduce a backend-shared `MapStateArrowBuffer` (extends the same off-heap pattern as `ArrowBinaryBuffer`). `asyncPut(uk, v)` writes the serialized value directly into the off-heap buffer (via the existing `serializeValueInto(DataOutputView)` plumbing pointed at a `MemorySegmentDataOutputView`). On `flushDirty()` (snapshot or buffer-full), the buffer dispatches a batched `frs_map_state_put_arrow(buffer_handle, n_entries)` FFI to the engine. Reads (`asyncGet`) check the off-heap buffer first; on miss, route through the V2 columnar dispatch.

(c) **Tier touches**: T1 (new state-class internal buffer + override), T3 (generalize `ArrowBinaryBuffer` into a per-state-instance pool, see §2 T3 description), T4 (classifier learns map-state-arrow ops).

(d) **New engine FFI**: `frs_map_state_put_arrow(buffer_handle: u64, n: u32) -> i32` (Rust side reads the buffer's hash + key + value layout directly via zero-copy `MemorySegment` aliasing). Replaces the per-event PUT path for MapState V2.

(e) **Tests**: (1) parity with V1-sync MapState — same key sequence, same final value-bytes when read back. (2) snapshot/restore — fill 100K entries, snapshot, restart, verify all present. (3) Q16 bench gate ≤ 150 s.

(f) **Regression risk**: medium. The "share ArrowBinaryBuffer across multiple state types" was previously attempted at v3.3 and regressed Q5 (586 s). Mitigation: per-state-instance buffer, NOT cross-state-shared; pre-flush before snapshot must extend the V1-sync pattern at `b3b9d7f2a6c`.

### Fix V5 — eliminate `byte[]` allocation on V2 GET-result decode

(a) **Today**: `VectorizedExecutor.java:321-322`: `raw = new byte[len]; MemorySegment.copy(outData, ..., raw, 0, len);` for every GET response. `completeGet(reqs[i], tables[i], raw)` then wraps in `DataInputDeserializer`.

(b) **Proposed**: introduce `MemorySegmentDataInputView` (already exists at `v1sync/MemorySegmentDataInputView.java` for V1-sync ValueState) and a `ForStRsInnerTable.deserializeValue(MemorySegment buf, long off, int len)` overload. `executeGets` passes the slice directly — zero heap allocation per GET.

(c) **Tier touches**: T1 (each state class gains the `(MemorySegment, off, len)` overload), T4 (VectorizedExecutor dispatches the new overload).

(d) **New engine FFI**: none.

(e) **Tests**: (1) parity — for each state class, GET via the new path produces equal value to the old path for 1000 random keys. (2) Q16 bench gate ≤ 170 s; Q19 ≤ 110 s (since both have hot GET paths).

(f) **Regression risk**: low. The V1-sync analogue has shipped since `5ad6e12abb8` — pattern is proven.

### Fix V10 — `frs_vectorized_batch_get` true batched implementation

(a) **Today**: `crates/forst-rs-ffi/src/lib.rs:~2415-2436` reads each key from the input buffer and calls `db.get(key)` in a loop — no engine-side batching. Effectively N JNI crossings collapsed into 1, but N RocksDB-level lookups.

(b) **Proposed**: use the engine's `multi_get` / `get_pinned_iter` (already exists for the timer-queue prefix scan) to batch N lookups into one engine call. Layout: input `keys_offsets[N+1]`, `keys_data`; output `vals_offsets[N+1]`, `vals_data`.

(c) **Tier touches**: T5 only. (Java side already calls the batched name.)

(d) **New engine FFI**: replaces the body of existing `frs_vectorized_batch_get` — no new symbol.

(e) **Tests**: criterion micros — `batch_get/1024` should drop from current ~42 µs to ~10 µs based on the engine point-lookup floor. Nexmark Q16, Q19 ≤ 5 % wall-clock improvement (already partially batched at the JNI level).

(f) **Regression risk**: low. Engine `multi_get` is well-tested.

### Fix V11 — ListState off-heap via `ArrowBinaryBuffer`

(a) **Today**: `ForStRsAsyncListStateV2` serializes elements to `byte[]` and routes through V2 dispatch heap-copies (same gap as V2 for MapState). No off-heap fast path.

(b) **Proposed**: per-state-instance `ArrowBinaryBuffer` accumulates element bytes; `asyncAdd` (post-V3) routes to the buffer; on flush, dispatches `frs_vec_merge_append_batch` (V4).

(c) **Tier touches**: T1, T3.

(d) **New engine FFI**: none beyond V4.

(e) **Tests**: same as V3.

(f) **Regression risk**: low post-V3.

### Fix V4 — batched `frs_vec_merge_append`

(a) **Today**: `VectorizedExecutor.dispatchAppendMerge` lines 356-440 iterates per-row, allocating an `Arena.ofConfined()` per row and calling `frsVecMergeAppend` for each. ~1 µs FFM crossing × N rows.

(b) **Proposed**: new `frs_vec_merge_append_batch(keys_offsets, keys_data, ops_offsets, ops_data, n_rows) -> i32`. Java side packs all rows into the ColumnarBatchBuffer layout (same as `vectorizedBatchPut`); single FFM crossing.

(c) **Tier touches**: T4, T5.

(d) **New engine FFI**: `frs_vec_merge_append_batch` — required.

(e) **Tests**: criterion micros — N=1024 should be <10 µs total. Nexmark Q19 ≤ 70 s post-V3+V4.

(f) **Regression risk**: low. Mirrors existing `vectorizedBatchPut` pattern.

### Fix V1 — eliminate `serializeMapEntryKey()` per-call `byte[]`

(a) **Today**: every `asyncGet/Put/Contains/Remove` on MapState V2 calls `serializeMapEntryKey()` which returns a fresh `byte[]` from `keyOut.getCopyOfBuffer()`. The byte[] is wrapped in a `BytesKey` for the cache lookup.

(b) **Proposed**: switch cache-key representation to `(long hash, MemorySegment scratch, int off, int len)` triple, reusing a `ThreadLocal` scratch buffer per task thread. The cache's `Map<BytesKey, Object>` becomes an open-addressed `long→int` index pointing into the scratch buffer (V7 fix is the same data structure).

(c) **Tier touches**: T1, T2.

(d) **New engine FFI**: none.

(e) **Tests**: cache hit/miss correctness — random key sequence, verify same `get/put/remove` outcomes via both paths. Nexmark Q16 ≤ 5 % improvement marginal — V1 is a fold-in win on top of V2/V5.

(f) **Regression risk**: low.

### Fix V7 — off-heap MapStateCache

(a) **Today**: `MapStateCache` uses heap `LinkedHashMap<BytesKey, Object>` with 256K cap. Per-entry: `Entry` heap node + `BytesKey` wrapper + `byte[]` reference + LRU pointers.

(b) **Proposed**: open-addressed `long→int` index in `MemorySegment` + value-bytes in a separate `MemorySegment` slab. Same layout as `ArrowBinaryBuffer`'s `hashIndex`. Cap raised to 1M (matches `ArrowBinaryBuffer.MAX_CAPACITY`). LRU replaced with a single `long lastAccessTimeNanos` per slot for clock-sweep eviction.

(c) **Tier touches**: T2.

(d) **New engine FFI**: none.

(e) **Tests**: cache hit-rate parity at 256K cap (must be ≥ current); at 1M cap, hit rate measured (expect ≥ 90 % on Q16). Nexmark Q16 ≤ 5 % improvement marginal — V7 is a fold-in win post-V2.

(f) **Regression risk**: low.

### Fix V9 — `MemorySegment.copy` per-iter-step (carried from prior audit)

Subsumed by V5 — fixing V5 (slice-decoder views) eliminates the per-iter-step `MemorySegment.copy` to heap `byte[]`.

### Fix V6 — full-`ArrayList<V>` materialization on `ListState.asyncGet`

(a) **Today**: every `asyncGet` allocates `ArrayList<V>(count)` and deserializes all N elements via per-element `elementSerializer.deserialize(valueIn)`.

(b) **Proposed**: lazy `Iterable<V>` returned by `asyncGet` that defers per-element deserialization until the user iterates. Internally backed by a `MemorySegment` slice and a "current offset" cursor.

(c) **Tier touches**: T1.

(d) **New engine FFI**: none.

(e) **Tests**: parity — iterator yields same sequence as eager ArrayList. Nexmark Q19 ≤ 5 % improvement marginal (Q19 actually iterates all elements for the Top-N decision, so the lazy path doesn't fully amortize — but the GC pressure relief is real).

(f) **Regression risk**: low.

### Fix V8 — slice-decoder still allocates per-call

Documented TODO at `ForStRsMapStateV2.java:342-343`. Identical pattern to V5 — subsumed by the `MemorySegmentDataInputView` introduction in V5.

### Fix V12 — Reducing/Aggregating inherit ValueState write-copy

Post-V2 (since both build on ValueState internals), this is automatically closed. No separate work.

---

## §6 — New Rust engine primitives required

Aggregated from §5. **Three new FFI symbols** + **one replacement body**:

| Symbol | Status | Used by | Notes |
|---|---|---|---|
| `frs_map_state_put_arrow(buffer_handle: u64, n: u32) -> i32` | NEW | V2 | Reads Arrow `[hashIndex][keys][values]` layout directly via zero-copy aliasing |
| `frs_vec_merge_append_batch(keys_off, keys_data, ops_off, ops_data, n) -> i32` | NEW | V3, V4, V11 | Batched form of existing `frs_vec_merge_append` |
| `frs_vectorized_batch_get` | REPLACE | V10 | Replace naive `for k in keys { db.get(k) }` loop with `multi_get` |
| (existing) `frs_batch_put`, `frs_vectorized_batch_*`, `frs_batch_prefix_scan` | KEEP | all | Already principle-compliant — proven by batched-timer commit `a5fd9f70dd6` |

**Engine API budget:** **+2 new symbols, 1 body replacement**. Conservative — most fix leverage comes from re-using the existing batched FFI primitives.

---

## §7 — Phased implementation plan (preview only)

The writing-plans hand-off will produce the full per-PR breakdown. This preview groups fixes into **phases ordered by tier-fix leverage** (§4 row) and inter-phase dependencies:

### Phase A: T4 + T5 batched merge-append (V3 + V4)
- V3 wiring (ListState `asyncAdd` → APPEND_MERGE)
- V4 batched FFI (`frs_vec_merge_append_batch`)
- **Acceptance**: Q19 ≤ 70 s, Q19/rocksdb ≥ 2.07×, no regression > 5 % on any of Q11/Q12/Q15/Q20/Q23
- **Why first**: highest single leverage (V3=55, V4=15), self-contained (no T1/T3 changes outside ListState).

### Phase B: T1 + T4 zero-copy GET-result (V5 + V8 + V9)
- `MemorySegmentDataInputView` introduction in V2 async state classes
- Slice-decoder rewrite in MapState V2
- Subsumes carried V9
- **Acceptance**: Q16 ≤ 170 s, no regression > 5 % anywhere; criterion micros for `batch_get/1024` improve ≥ 20 %.
- **Why next**: T4 already touched in Phase A — reduce context switching.

### Phase C: T1 + T3 MapState V2 off-heap (V2 + V11 + V12)
- Per-state-instance `MapStateArrowBuffer`
- ListState off-heap (V11) follows the same pattern
- ReducingState/AggregatingState (V12) inherit
- **Acceptance**: Q16 ≤ 150 s (Q16/rocksdb ≥ 2.43×), Q19 ≤ 60 s, no regression > 5 %.
- **Why next**: T3 work is the broadest refactor; ship after V3/V4 prove the merge path stable.

### Phase D: T1 + T2 cache + key-buffer reuse (V1 + V7)
- ThreadLocal scratch + composite-key intern
- Off-heap `MapStateCache`
- **Acceptance**: Q16 ≤ 145 s, no regression > 5 %.
- **Why last**: fold-in win on top of Phase A/B/C; lowest individual leverage.

### Phase E: T5 engine `multi_get` batched (V10)
- Replace naive `frs_vectorized_batch_get` body
- **Acceptance**: criterion `batch_get/1024` ≤ 10 µs; Nexmark wall-clocks unchanged (already partially batched).
- **Why orthogonal**: Rust-only change; can ship parallel to any other phase.

### Gating items (§9 must close before any phase ships)
- OQ-1: V1-sync off-heap correctness check (else Q5/Q13 may regress)
- OQ-2: V2 async pre-snapshot flush verified (correctness gate)

---

## §8 — Risk & test matrix

For each phase, the regression-risk vector and required gates.

| Phase | What can regress | Required new unit tests | Bench gate | Merge criteria |
|---|---|---|---|---|
| A (V3+V4) | Snapshot/restore for ListState; merge-operator correctness across compaction | (1) `AppendMergeSnapshotTest` — 1000-entry restore parity; (2) `MergeAppendBatchTest` — N=1, N=1024, N=1M-entry batch; (3) `ClearThenAddTest` — verify clear-state semantics | Q19 ≤ 70 s, Q11/Q12/Q15/Q20/Q23 within 5 % | All 3 tests green + bench gates met |
| B (V5+V8+V9) | GET-result deserialization correctness across all 5 state types | (1) `MemorySegmentDataInputViewTest` — 1000 random keys, byte-equal to old path; (2) iterator-path parity on Q9-shape workload | Q16 ≤ 170 s, Q19 ≤ 110 s, criterion `batch_get/1024` ≥ 20 % faster | All tests green + bench gates met |
| C (V2+V11+V12) | MapState/ListState snapshot/restore; pre-flush correctness; shared-buffer interaction with V1-sync | (1) `MapStateArrowSnapshotTest` — 100K entries; (2) `PreFlushBeforeSnapshotTest`; (3) `V1V2InteropTest` — same backend, V1-sync state + V2-async state side-by-side | Q16 ≤ 150 s, Q19 ≤ 60 s, no regression > 5 % | All tests green + bench gates met + OQ-1 closed |
| D (V1+V7) | Cache-key hash collisions; ThreadLocal lifecycle on slot-thread reuse | (1) `OffHeapMapStateCacheTest` — collision injection + LRU correctness; (2) `ThreadLocalScratchTest` — verify reset across task-thread reuse | Q16 ≤ 145 s | All tests green + bench gate met |
| E (V10) | RocksDB `multi_get` semantics in engine — pinned-buffer lifetimes | (1) `EngineMultiGetTest` — criterion micro vs prior; (2) memory-leak check via valgrind under N=10M iterations | criterion improvement only; Nexmark unchanged | criterion gate + memory test green |

### Cross-phase acceptance gate
After all 5 phases land, the **portfolio gate** is: `forst-rs wins ≥ 20 of 23 Nexmark queries vs rocksdb` (today: 18) AND `no query worse than 0.85× rocksdb` (today's worst is Q10 at 0.68×, but Q10 is stateless — see §9 OQ-3).

---

## §9 — Open questions (pre-populated, must close before Phase A)

### OQ-1 — Is V1-sync genuinely "already off-heap correct"?

The claim that V1-sync ValueState / MapState are principle-compliant since commits `537c1403f2f` and `633af3d3be1` has been twice falsified in this project's history:
1. **PR-A revert (commit `cb077849...`)** — the "always-on write buffer" caused Q5 to regress to 586 s; reverted to adaptive-disable.
2. **Large-buffer-breaking-Q11/Q12** — increasing buffer capacity sped up Q5 but tanked Q11/Q12.

Action: a 1-day code re-audit of `ForStRsValueState` + `ForStRsMapState` V1-sync paths against the same Tier 1–5 model. If found compliant, document the audit as evidence. If not, add V1-sync violations to §3 as V15+ before any phase ships.

### OQ-2 — V2 async pre-snapshot flush

For Phase C (off-heap MapState V2) and Phase A (off-heap ListState merge state) to be correctness-safe, the V2 async pre-snapshot flush hook must guarantee that the off-heap buffer is fully drained to the engine before the snapshot barrier completes. The V1-sync ValueState off-heap got this via the `b3b9d7f2a6c` hook; the V2 async equivalent has NOT been verified.

Action: a focused code read of `ForStRsAsyncKeyedStateBackend.snapshotState()` + the V2 async dispatcher's `flushDirty()` path. Document the answer as a 2-page sub-spec; if a gap is found, treat as Phase A blocker.

### OQ-3 — Q10 stateless regression — bench methodology or real?

Q10 SQL is `INSERT INTO csv_sink SELECT * FROM bid` — no state ops. v3.2 = 7.87 s; v3.8 = 34.85 s. The 27 s delta cannot be a state-backend code issue. Hypothesis: fresh-cluster strategy's 12 s overhead amortized differently across the two bench runs.

Action: run Q10 standalone on v3.8 HEAD with the same hot-cluster strategy v3.2 used. If Q10 still > 15 s, deepen. If Q10 returns to ~8 s, document as methodology artifact and remove from §8's portfolio gate exception list.

### OQ-4 — Q13 V1-sync JOIN — is there a forst-rs-scope fix?

Q13 (0.84× rocksdb) was documented at v3.3 as a structural gap (`AsyncStateSlicingSharedWindowProcessor` missing in flink-table-runtime). However, the v3.8 wall-clock (41.1 s) is close enough to rocksdb (34.5 s) that a focused MapState/ValueState fast-path tweak might close it without touching flink-table-runtime.

Action: 0.5-day trace of Q13's runtime call path through the V1-sync state classes; identify any heap-copy or per-call FFM that could be eliminated within forst-rs scope. Report finding as either a new V15+ violation row or a confirmation that no forst-rs-scope lever exists.

---

## §10 — Cross-references

- `2026-05-19-vectorization-violation-audit-q8q9q11q12q20.md` — predecessor audit (5 violations, partially addressed)
- `2026-05-21-batched-engine-timer-design.md` — reference design for Tier 3 + Tier 5 batched FFM
- `2026-05-21-forst-rs-benchmark-report-v3.8.md` — current baseline
- `2026-05-21-arrow-buffer-size-aware-autotune-design.md` — Tier 3 size-aware buffer design
- `2026-05-21-valuestate-shared-buffer-hybrid-design.md` — V1-sync ValueState off-heap design (referenced by OQ-1)
- `2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md` — V1-sync state cache design (referenced by OQ-1)
- `CONTRIBUTING.md` — bench-level vocabulary L1–L4 (preserved; collides with prior code-layer L1–L5 naming, resolved by Tier 1–5 in this spec)

---

## End-of-spec checklist

- [x] §0 hard assumption (JDK 25) explicit
- [x] §2 includes per-state-type access pattern (compact 2D table)
- [x] §3 4-subcolumn query-impact (Hit / Worst-case / Relief / Certainty) with leverage formula
- [x] §4 matrix includes tier-fix-leverage summary row
- [x] §5 fix ordering matches §3 leverage ranking
- [x] §6 engine API budget enumerated (3 symbols)
- [x] §7 phases mapped to writing-plans hand-off
- [x] §8 acceptance gates embedded (no separate §7.5)
- [x] §9 open questions pre-populated (OQ-1, OQ-2, OQ-3, OQ-4)
- [x] §10 cross-refs listed
- [x] Tier 1–5 naming used throughout (no L-prefix collision with CONTRIBUTING.md benchmark levels)

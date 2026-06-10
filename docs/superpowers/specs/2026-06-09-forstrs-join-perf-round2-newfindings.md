# ForStRS Join Performance — Round-2 NEW Findings (Q7 / Q9 / Q20)

> **Type:** findings deliverable (single document, per the HARD RULE).
> **Method:** 5 perspectives (A Vectorization · B Memory/Zero-copy · C Arrow/Columnar · D CPU/SIMD · E LSM/Data-layout) × 10 rounds + adversarial-verify + synthesize, run as a deterministic Workflow over **current** `forst-rs` HEAD and `flink` HEAD. 91 raw findings harvested → deduped/clustered into 24 OPT-N items + a PORT catalog + a verdict on v1's thesis.
> **Baseline ("v1"):** `2026-06-09-forstrs-join-performance.md` (12 OPTs; thesis = depth-1 inline FFM dispatch is the wall, LSM read is cheap). This document is **additive**: every item is tagged vs v1.
> **Design/plan:** `2026-06-09-forstrs-join-perf-round2-design.md` (the methodology contract; not a findings doc).
> **Benchmark:** NexMark. Join-heavy queries: **q7** (multi-way + self-join), **q9** (six-way + LIKE), **q20** (correlated subquery, no-UK bid side). q4 cited where write-path-adjacent.
> **Codebases:** Flink `/Users/lijunqing/Code/stczwd/flink` (HEAD `01f2064dfde`); ForSt-C++ = this repo `main` (`git show main:<path>`); forst-rs = this repo `forst-rs` HEAD. **Priority/Status left blank by design** — fill after profiling.

---

## 1. Tag contract & how to read this

Every item carries a tag vs v1:

- **`NEW`** — a surface v1's OPT-01…12 never mapped.
- **`REFINES`** — v1 had the area; Round-2 adds fresh evidence / a correction / a quantification.
- **`OVERTURNS`** — a v1 claim (usually a stale `file:line` anchor) is wrong on current code.
- **`PORT`** — a concrete ForSt-C++ / RocksDB technique to borrow (`file:line` on both sides).

`FACT` = read directly from source this round. `HYP` = inference still to be profiled. All `file:line` are from the HEADs above; verify before editing (the tree moves).

---

## 2. Verdict on v1's thesis (the headline)

**CONFIRMED and REFINED — not overturned.**

The default measured chain for q7/q9/q20/q4 is **byte-for-byte v1's depth-1 inline chain**: `createStateExecutor` returns the plain `VectorizedExecutor` unless `FRS_RS_PARALLEL_EXECUTOR=1` (`ForStRsAsyncKeyedStateBackend.java:1149-1157`); `executeBatchRequests` drives `executePuts/Deletes/Gets/Iters` inline (`VectorizedExecutor.java:460-463`) and returns `completedFuture(null)` (`:552`); `fullyLoaded()` is hard-coded `false` (`:1023-1025`). q9/q20 joins use `MapState<RowData,RowData>` → the exact MAP_ITER path v1 traced (`JoinRecordStateViews.java:116/133`). [#21 #55 #60 #65 #74 #85]

Two adversarial rounds **tried to falsify** "read is cheap, the wall is dispatch" and **could not** (#31): each memtable-miss probe's residual read CPU is `O(log N)` entry-parses + a short prefix-reconstruction scan + one shard `RwLock` atomic — a real **micro-opt surface worth ~10-25% combined**, but per-probe-bounded and dwarfed by the per-record structural dispatch cost (inline depth-1, per-row retraction FFI, `ceil(N/128)` iterator re-dispatch). Engine `get_internal` short-circuits at the active memtable (`db.rs:8401-8424`) for the hot-ingest case, so the SST seek/walk cost only bites once keys flush.

**But Round-2 substantially relocates where the recoverable time is.** v1 framed the levers as "make the engine faster (vectorize/zero-copy/SIMD)". Round-2's biggest *net-new* surface is **above the executor**, in the Flink streaming-join operator and the AEC iterator framework — code v1 stopped short of (it ended at the engine call boundary). The join operator **fully materializes the entire other-side record set into an on-heap `ArrayList` before it inspects a single match, applies the join condition row-by-row in Java, and re-dispatches every 128-entry chunk as a fresh controller request.** None of that is engine work, and none of it is in v1.

So: **dispatch is still the dominant lever (v1 right), but the second-largest lever is operator-side materialization/continuation, not engine micro-ops (v1's framing miss).**

### Stale v1 anchors corrected this round (already-fixed; do not re-spec)

| v1 claim | Current reality | Evidence |
|---|---|---|
| `frsVecIterPrefixOpenBatchParallel:4056` is **UNUSED** | **WIRED** — called from `executeItersBatchedParallel` (`VectorizedExecutor.java:1535`), backed end-to-end by an engine read-pool. Gated off + BUILD-only (see OPT-N21). | #18 #56 #61 #83 |
| `RoutingStateExecutor` is "gated-off scaffolding"; key-group-affine routing is "a spec" | `RoutingStateExecutor` is **fully built** (real `fullyLoaded()=free.isEmpty()`, worker offload), wired behind `FRS_RS_PARALLEL_EXECUTOR=1`. Still **key-AGNOSTIC** `leaseWorker` + opt-in (see OPT-N22). | #74 #85 |
| Iterator decode has a "residual per-row `byte[]` copy" | **Zero-copy** for the join's `ForStRsMapStateV2` — it overrides both view decoders to read off the chunk `MemorySegment` (`:792/:857`). The lingering comment `ForStRsDBIterRequest.java:57` is stale. | #57 #62 #91 |
| Per-probe 64KiB native iter-chunk alloc | **Gone** — `drainIterSerial` reuses one per-executor arena buffer (`VectorizedExecutor.java:1457-1460`, commit `53722e117d6`). | #58 #63 |

---

## 3. Q7 / Q9 / Q20 path deltas vs v1 (current code)

Only the deltas; the v1 §3 chains otherwise stand.

- **q7** — planner picks `JoinKeyContainsUniqueKey` for at least one side → that side is backed by **`ValueState`** (`JoinRecordAsyncStateViews.java:73-118`), which on forst-rs has **neither a value-read cache nor a write-staging buffer** (`ForStRsValueStateV2`, unlike `ForStRsMapStateV2`). So q7's UK side pays a raw engine GET per probe and an un-coalesced PUT per add (OPT-N07). The other side and the self-join still hit MAP_ITER.
- **q9** — six-way join; count/aggregating sides are **Merge-typed**, so every batch GET that resolves to a Merge operand **shatters the batch into N serial `get_internal` walks** (OPT-N24). ROW_NUMBER/window prefixes re-scan the active-memtable snapshot per record (OPT-N12).
- **q20** — bid side keyed by `auction` has **no unique key** → `InputSideHasNoUniqueKey`: probe = full `asyncEntries()` prefix range-scan (OPT-N01/N02), mutation = `asyncGet→asyncPut` **RMW** per bid (OPT-N04), and the match list is **`cnt`-replicated** (OPT-N06). Hot auctions = many serial 128-cap continuations for one probe.

Shared by all three on the **default** path: synchronous depth-1 inline executor, single shared iter chunk buffer (concurrent iterators structurally impossible, OPT-N21/N23), and full-set materialization before first emit (OPT-N01).

---

## 4. Master table (OPT-N01 … OPT-N24)

Priority/Status intentionally blank.

| ID | Title | Tag | Dim | Queries | Conf | Pri | Status |
|---|---|---|---|---|---|---|---|
| [OPT-N01](#opt-n01) | Join fully materializes other-side into on-heap ArrayList before first match (no lazy/short-circuit) | NEW | Batch/Operator | q7/q9/q20 | high | | |
| [OPT-N02](#opt-n02) | 128-entry iterator soft-cap → ceil(N/128) serial AEC re-dispatches per hot key | NEW | Iterator-lifecycle | q9/q20 | high | | |
| [OPT-N03](#opt-n03) | AbstractStateIterator.onNext O(N²/128) ArrayList result-copy on continuation | REFINES | Iterator-lifecycle | q7/q9/q20 | high | | |
| [OPT-N04](#opt-n04) | No-UK join state = per-record asyncGet→asyncPut RMW; no engine merge/add | NEW | Operator/Write | q9/q20 | high | | |
| [OPT-N05](#opt-n05) | No predicate pushdown — JoinCondition applied row-by-row in Java after full decode | NEW | Vectorization | q7/q9/q20 | high | | |
| [OPT-N06](#opt-n06) | No-UK probe replays each record `cnt` times → O(multiplicity) OuterRecord allocs | NEW | CPU/alloc | q20 | high | | |
| [OPT-N07](#opt-n07) | ValueState join side (q7) has NO read cache and NO write-staging buffer | NEW | Batch/Cache | q7 | high | | |
| [OPT-N08](#opt-n08) | getIterPrefix force-drains write buffer per probe; checkpoint force-drains all MapStates | NEW | Batch/Checkpoint | q7/q9/q20 | high | | |
| [OPT-N09](#opt-n09) | statebuf forEachEntry linearly rescans ALL liveRows (whole-state) per probe | NEW | Memory/scan | q9/q20 | high | | |
| [OPT-N10](#opt-n10) | Retraction batches collapse to per-row sync FFI via requiresOrderedDispatch | NEW | Vectorization | q7/q9/q20 | high | | |
| [OPT-N11](#opt-n11) | Active-memtable point GET = BTreeMap descent + shard RwLock + key-copy bound (not O(1)) | REFINES | CPU/read | q4/q9/q20 | high | | |
| [OPT-N12](#opt-n12) | Memtable prefix-scan rebuilds 16-shard locked snapshot+sort+Arc-copy per probe | NEW | CPU/read | q9/q20 | high | | |
| [OPT-N13](#opt-n13) | k-way merge does 2× O(k) linear peek per emitted key; BinaryHeap deferred | NEW | CPU/read | q9/q20 | high | | |
| [OPT-N14](#opt-n14) | batch-GET L0 has no newest-first short-circuit; per-probe L0 re-sort + linear find_sst | OVERTURNS | LSM/read-amp | q4/q9/q20 | high | | |
| [OPT-N15](#opt-n15) | KV-block (default) walk_key reconstructs ≤15 keys + re-parse per probe on cache hit | REFINES | CPU/read | q4/q9/q20 | high | | |
| [OPT-N16](#opt-n16) | Engine point-get to_vec() at every tier; borrowed primitives + batch_get_arrow unwired | NEW | Zero-copy | q7/q9/q20 | high | | |
| [OPT-N17](#opt-n17) | Iterator buffers Arc::from(key)+Arc::from(value) per row even on cache hit | REFINES | Zero-copy | q9/q20 | high | | |
| [OPT-N18](#opt-n18) | Value-carrying merge BYPASSED for memtable-resident keys → get_internal re-walk + copy | OVERTURNS | Read-amp | q4/q9/q20 | high | | |
| [OPT-N19](#opt-n19) | Iterator wire format row-major AoS (GET is columnar); full key incl shared prefix re-copied | NEW | Arrow/Columnar | q9/q20 | high | | |
| [OPT-N20](#opt-n20) | SST bloom = scalar 8-probe loop + re-hash per L0 file + 256-bit unaligned blocks | NEW | CPU/SIMD | q9/q20 | high | | |
| [OPT-N21](#opt-n21) | Parallel-iter path WIRED but gated-off, BUILD-only, unreachable from sync values()/entries() | OVERTURNS | Vectorization | q7/q9/q20 | high | | |
| [OPT-N22](#opt-n22) | RoutingStateExecutor built but key-AGNOSTIC + opt-in → can't fix windowed joins | OVERTURNS | Scheduling | q7/q9/q20 | high | | |
| [OPT-N23](#opt-n23) | Read pool hard-clamped to 4 threads, process-global shared vs ForSt per-executor | REFINES | Scheduling | q9/q20 | high | | |
| [OPT-N24](#opt-n24) | Merge-stored state de-vectorizes batch GET into N serial get_internal walks | NEW | Vectorization | q9/q20 | high | | |

---

## 5. OPT-N details

### <a id="opt-n01"></a>[OPT-N01] Join fully materializes the other side into an on-heap ArrayList before the first match

- **Dimension**: Batch Execution / Operator state-access · **Queries**: q7/q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Every join probe drains the **entire** matching-key record set into a heap `ArrayList<OuterRecord>` (running the generated `JoinCondition` per row) and wraps it as `AssociatedRecords` *before* `processJoin` emits anything. No early-exit, no streaming, no short-circuit — a 1-of-N-match probe over an N-row bucket scans, deserializes, and boxes all N rows. This is the operator-level barrier in front of every join output and the GC/alloc pressure that shows up on q9/q20 at scale.

#### Affected Code Paths
- Async path: `AsyncStateStreamingJoinOperator.doProcessElement` chains `associatedRecordsFuture.thenAccept(records -> joinHelper.processJoin(...))` (`AsyncStateStreamingJoinOperator.java:163-176`); `AssociatedRecords.fromAsyncStateView` = `findMatchedRecords(...).thenApply(AssociatedRecords::new)` (`AssociatedRecords.java:143-146`); `findMatchedRecords` accumulates into `new ArrayList<>()` inside `onNext` (`JoinRecordAsyncStateViews.java:166-180`, no-UK `234-252`).
- Sync path: `ForStRsMapState.values()/entries()` build a `java.util.ArrayList` via `forEachEntry` (`state/ForStRsMapState.java:619-623`); `AbstractStreamingJoinOperator.iterator` pulls `getRecords().iterator()` per record (`:137-162`).
- **ForSt-C++ contrast (PORT):** `ForStSyncMapState.RocksDBMapIterator.loadCache` loads ≤`CACHE_SIZE_LIMIT` entries per batch from a **resumable** seek cursor (`ForStSyncMapState.java:637-686`), so a lazy join filter pulls bounded windows and **breaks early** once the condition is satisfied.

#### Root Cause
FACT. The `StateFuture`/`AssociatedRecords` contract returns a fully-resolved collection; the future does not complete until `AbstractStateIterator.onNext` has recursively drained all continuation chunks. `forEachEntry` has no window — it materializes the full prefix before returning the iterable.

#### Proposed Optimization
Expose a **lazy, cancellable** probe: have `findMatchedRecords` / `getRecords` return a streaming iterator that pulls one 128-cap chunk at a time and lets the operator short-circuit (inner-join: stop after first match where semantics allow; semi/anti: stop after existence proof). For the async view, push a "stop predicate" into the `onNext` consumer so the continuation chain halts. Mirror ForSt's `loadCache` resumable-cursor model.

#### Expected Impact
For low-match-ratio probes over high-fan-in keys (q20 hot auctions, q9 hot bidders), cuts per-probe deserialization + `OuterRecord` allocation from O(bucket) toward O(matches). Compounds with OPT-N02/N03 (fewer continuations) and OPT-N06 (less replication).

#### Complexity & Risks
High — touches Flink table-runtime join operators (shared with RocksDB/ForSt backends). Must preserve exact join semantics (outer-join null-padding, retraction counts). Best landed as a backend-agnostic streaming-probe API so all backends benefit. Rollback = keep the eager path behind a flag.

#### Validation Plan
Flight-record `ArrayList` allocation + `JoinCondition.apply` call counts per probe on q20; expect both to drop with match-ratio. A/B end-to-end q20/q9 wall + GC time.

#### Dependencies
OPT-N02, OPT-N03 (continuation cost), OPT-N06 (replication).

---

### <a id="opt-n02"></a>[OPT-N02] 128-entry iterator soft-cap → ceil(N/128) serial AEC re-dispatches per hot key

- **Dimension**: Iterator lifecycle · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
A join key with N build-side records does **not** drain in one engine call. `ForStRsDBIterRequest` soft-caps at `CACHE_SIZE_LIMIT=128` decoded rows, stashes the native handle, and the next 128 come back as a **fresh `ITERATOR_LOADING` StateRequest** that re-traverses classifier → `VectorizedExecutor` → `executeIters` → FFM. So a hot key costs `ceil(N/128)` serial blocking round-trips, each re-entering the depth-1 inline executor; the native iterator is reused (no engine re-seek) but the Java framing recurs.

#### Affected Code Paths
- `ForStRsDBIterRequest.java:63` (`CACHE_SIZE_LIMIT=128`), `:370` drain `do { frsVecIterPrefixNext } while (decodedCount < CACHE_SIZE_LIMIT)`, `:386-392` stash `existingVecHandle`.
- `AbstractStateIterator.onNext:157-161` → `asyncNextLoad().thenCompose(itr -> itr.onNext(...))` (recursive re-dispatch per chunk); `ForStRsMapIterator.java:85-96` `hasNextLoading`/`nextPayloadForContinuousLoading`.
- **ForSt-C++ contrast (PORT):** `ForStDBIterRequest.process:120-123` opens+seeks only when `rocksIterator==null` and the request object is reused as the continuation; `ForStIterateOperation` submits the **whole drain per request to the read pool** (`ForStIterateOperation.java:61-94`), so continuations run off-mailbox in parallel.

#### Root Cause
FACT. The cap bounds inline mailbox occupancy but converts a large bucket into many controller round-trips on the single inline executor.

#### Proposed Optimization
(a) Raise/auto-tune the cap when the operator is known to consume the whole set (eager joins), or (b) keep the iterator request *resident* and drain continuations on a read-pool thread instead of a fresh AEC request (ForSt's model), or (c) combine with OPT-N01 lazy probe so most buckets never reach the second chunk.

#### Expected Impact
Removes `ceil(N/128)-1` per-probe Java dispatch round-trips for hot keys. Direct on q20 hot auctions / q9 hot bidders.

#### Complexity & Risks
Medium-high. Raising the cap trades mailbox occupancy for fewer round-trips — must not starve other keys' fairness. Off-mailbox drain needs the OPT-N22/N23 executor work to be safe.

#### Validation Plan
Histogram `ITERATOR_LOADING` requests per probe vs bucket size on q20; confirm `ceil(N/128)` relationship, then measure reduction.

#### Dependencies
OPT-N01, OPT-N03, OPT-N21/N22/N23.

---

### <a id="opt-n03"></a>[OPT-N03] AbstractStateIterator.onNext rebuilds the full result list at every 128-chunk boundary → O(N²/128) copies

- **Dimension**: Iterator lifecycle / copy-count · **Queries**: q7/q9/q20 (UK-side variants) · **Tag**: REFINES · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
The value-returning `onNext` overload (used by `JoinKeyContainsUniqueKey`/`HasUniqueKey` probes) copies the **entire accumulated result collection** into a new `ArrayList` at every continuation level. For a key drained in `C=ceil(N/128)` chunks the level-k combine re-copies all `k·128` already-accumulated elements → `Σ k·128 = O(C²·128) = O(N²/128)` element copies. The framework flags it as an un-done TODO.

#### Affected Code Paths
`AbstractStateIterator.java:127-136` — `thenCombine((a,b) -> { result = new ArrayList<>(a.size()+b.size()); result.addAll(a); result.addAll(b); ... })` with literal `// TODO optimization: Avoid results copy.` (`:131`). Driven by `JoinRecordAsyncStateViews.java:166-180`.

#### Root Cause
FACT. Quadratic re-copy in the continuation combiner, compounding OPT-N02 for unique-key join variants that return per-entry futures.

#### Proposed Optimization
Accumulate into a single mutable collection threaded through the continuation (no per-level re-copy), or a chunked rope/iterator that never concatenates. This is the exact TODO at `:131`.

#### Expected Impact
Drops continuation copy cost from O(N²/128) to O(N) for large UK-side buckets.

#### Complexity & Risks
Low-medium, localized to `AbstractStateIterator` (runtime-shared). Preserve collection-completion semantics.

#### Validation Plan
Micro-bench `onNext` with synthetic 1k/4k-entry keys; count element copies before/after.

#### Dependencies
OPT-N02.

---

### <a id="opt-n04"></a>[OPT-N04] No-unique-key join state = per-record asyncGet→asyncPut RMW; no engine merge/add

- **Dimension**: Operator fusion / Write path · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
On a no-UK side (the common NexMark case), `addRecord`/`retractRecord` maintain a `<record,count>` map via `asyncGet(record).thenApply(cnt+1).thenCompose(asyncPut(record,cnt))` — a strictly **ordered GET-then-PUT per record**. The put cannot dispatch until the get resolves, so q20's write path is read-bound (must read before every write) and the framework cannot fuse get+put into one engine op.

#### Affected Code Paths
`JoinRecordAsyncStateViews.java:201-213` (`asyncGet.thenApply(cnt+1).thenCompose(asyncPut)`); outer variant `OuterJoinRecordAsyncStateViews.java:231-245`. forst-rs makes the `asyncGet` resolve from the off-heap staging buffer (`ForStRsMapStateV2.asyncGet:356-364`) so the GET is cheap, but the increment is a Java closure (`cnt+1`) the engine cannot push down, and the resulting PUT is a distinct row — no engine-side add/merge operator is wired, so write coalescing degenerates to one logical PUT per record-value update.

#### Root Cause
FACT. Count semantics force read-modify-write; there is no engine `Merge`-typed add operator wired for the join count map.

#### Proposed Optimization
Wire an engine-side **add/merge operator** for the count map so `addRecord` becomes a blind `Merge(+1)` (no dependent GET), resolved at read/compaction time — exactly RocksDB's merge-operator pattern. Keeps writes coalescible and removes the GET→PUT dependency.

#### Expected Impact
Halves state round-trips on q20 bid ingest (eliminates the dependent GET) and restores write batching on the count map.

#### Complexity & Risks
Medium. Needs a merge operator with correct retraction (negative deltas) + snapshot-correct read-time resolution. Risk: merge-chain read cost (see prior memory note on un-combined merge chains) — must verify partial-merge/compaction collapses chains.

#### Validation Plan
Count engine GET ops on q20 bid path before/after; A/B wall.

#### Dependencies
OPT-N24 (merge resolution must stay vectorized).

---

### <a id="opt-n05"></a>[OPT-N05] No predicate pushdown — JoinCondition applied row-by-row in Java after full decode

- **Dimension**: Vectorization / predicate pushdown · **Queries**: q7/q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
The state backend is a dumb prefix-scan: it returns and decodes **every** user-key for a join key, then the generated `JoinCondition` filters in a Java `onNext` callback. For joins whose join-key prefix groups many rows that the residual condition rejects (interval/windowed joins where a ts-range or extra predicate is the real filter), the engine pays full decode + heap allocation for rows discarded immediately.

#### Affected Code Paths
`JoinRecordAsyncStateViews.java:166-180` (`onNext` applies `condition` after drain); `OuterJoinRecordAsyncStateViews.java:283-304`. forst-rs decodes every chunk row to detached on-heap UK/UV in `decodeChunkDirect` (`ForStRsDBIterRequest.java:645-681`, per-row `deserializeUserKey`+`deserializeUserValue` unconditional, `:663-669`). No mechanism to push even a byte-range or ts-bound into the prefix scan.

#### Root Cause
FACT. The `JoinCondition` is a generated closure the backend never sees; there is no predicate-pushdown channel from operator to scan.

#### Proposed Optimization
Add a **byte-range / suffix-bound predicate** to the prefix-scan request (the planner already knows the residual condition's key-range component for interval joins). Engine evaluates a cheap range/equality on the encoded key before decode, skipping rejected rows. Full arbitrary-predicate pushdown is out of scope; target the common ts-bound/equality case.

#### Expected Impact
For low-selectivity residual predicates, cuts decode + alloc proportional to rejected-row fraction. Largest on interval/windowed joins.

#### Complexity & Risks
High — needs planner→backend predicate plumbing and an engine-side encoded-key matcher. Risk: correctness of the encoded-bound translation. Start with the simplest ts-bound case.

#### Validation Plan
On an interval-join query, measure decoded-rows vs emitted-rows ratio before/after.

#### Dependencies
OPT-N01 (lazy probe), OPT-N19 (key layout for cheap bound check).

---

### <a id="opt-n06"></a>[OPT-N06] No-UK probe replays each record `cnt` times → O(multiplicity) OuterRecord allocations

- **Dimension**: CPU / allocation amplification · **Queries**: q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
For joins without a unique key the async probe does `for (i=0;i<cnt;i++) matchedRecords.add(new OuterRecord(record))` — one wrapper allocation per logical duplicate of the same deserialized row. Per-probe alloc/GC scales with average multiplicity, worse than row count.

#### Affected Code Paths
`JoinRecordAsyncStateViews.java:244-249` (cnt-replication into `matchedRecords`); state stores multiplicity not duplicates (`:200-213`). Downstream emits one output per wrapper (`StreamingJoinOperator.java:283-286`).

#### Root Cause
FACT. State compresses duplicates to a count, but the probe re-expands to `cnt` `OuterRecord` heap objects.

#### Proposed Optimization
Carry multiplicity through the match list (a `(record, cnt)` pair) and let the emit loop iterate `cnt` without allocating `cnt` wrappers; allocate the output `RowData` lazily per emit.

#### Expected Impact
Drops `OuterRecord` allocations on q20 from O(total-multiplicity) to O(distinct-matches).

#### Complexity & Risks
Low-medium, localized to the no-UK view + emit loop. Preserve retraction/outer semantics.

#### Validation Plan
Count `OuterRecord` allocations per probe on q20 vs average bucket multiplicity.

#### Dependencies
OPT-N01.

---

### <a id="opt-n07"></a>[OPT-N07] ValueState join side (q7) has NO read cache and NO write-staging buffer

- **Dimension**: Batch / Cache asymmetry · **Queries**: q7 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
When the planner chooses `JoinKeyContainsUniqueKey`, that side is a **`ValueState`**. `ForStRsValueStateV2` (unlike `ForStRsMapStateV2`) has **neither a value-read cache nor a `MapStateArrowBuffer` write-staging buffer** — it caches only the serialized composite *key* in a per-context slot. So each q7-style probe is an un-cached engine GET and each add is an un-coalesced PUT, with only key-serialization amortized. The "join key contains unique key" planner optimization paradoxically gets the **least** backend batching.

#### Affected Code Paths
`JoinRecordAsyncStateViews.java:73-118` (`ValueState recordState`, `asyncUpdate`/`asyncValue`); `ForStRsValueStateV2.java:144-190` (Slot[] caches only the key), `:258-266` (`buildDBPutRequest` straight to `VALUE_UPDATE`, no staging). Contrast `ForStRsMapStateV2.java:344-364/397-424` (has both `MapStateCache` and `offHeapBuf`).

#### Root Cause
FACT. Feature asymmetry: V2 MapState got read-cache + write-staging; V2 ValueState did not.

#### Proposed Optimization
Add a per-context value-read cache and a write-staging buffer to `ForStRsValueStateV2`, mirroring `ForStRsMapStateV2`. For the join-UK pattern (write-then-read of the same key) the read cache resolves the probe locally and staging coalesces writes to the watermark.

#### Expected Impact
Removes one engine GET per q7 UK-side probe and coalesces the PUTs — directly targets q7's regression vs the MapState path.

#### Complexity & Risks
Medium. Cache invalidation on update/clear; correctness under retraction. Reuse the MapStateV2 machinery to limit risk.

#### Validation Plan
A/B q7 wall + engine GET/PUT counts on the ValueState CF.

#### Dependencies
None hard; shares infra with OPT-N08.

---

### <a id="opt-n08"></a>[OPT-N08] getIterPrefix force-drains the write buffer per probe; checkpoint force-drains all MapStates

- **Dimension**: Batch execution / Checkpoint × join · **Queries**: q7/q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Two write-coalescing defeats:
1. **Per-probe:** `ForStRsMapStateV2.getIterPrefix` calls `flushOffHeapBuffer()` before returning the scan prefix (the engine scan can't see staged writes), so the per-state staging buffer is drained to the engine on **every probe of the iterated side**. For interleaved joins (q20 bid↔auction, q9 self-windowed) staged PUTs from prior records flush one-batch-per-probe instead of accumulating to the watermark — collapsing coalescing to near per-record PUT batches.
2. **Per-checkpoint:** `flushAllMapStates()` serially iterates the whole `mapStateRegistry` calling `flush()→flushMapWriteCache()→vectorizedBatchPut` for every join state **on the snapshot thread** before the snapshot proceeds — checkpoint latency includes draining the entire un-flushed build side.

#### Affected Code Paths
`ForStRsMapStateV2.java:745-748` (`getIterPrefix` first line `flushOffHeapBuffer()`), `:531-535` (drain). `ForStRsKeyedStateBackend.java:1708-1721` (`flushAllMapStates` loops registry on snapshot thread); `ForStRsMapState.java:1094-1096/1050-1051` (`flush→vectorizedBatchPut`); legacy probe path force-flushes inline (`:819`).

#### Root Cause
FACT. Read-after-write visibility forces the per-probe flush; the checkpoint contract force-drains synchronously.

#### Proposed Optimization
(a) Make the engine prefix-scan **merge the staged off-heap buffer** with the LSM scan (a memtable-like overlay) so a probe needn't flush — read staged + persisted in the iterator. (b) Move `flushAllMapStates` off the critical snapshot path or make it incremental (only dirty states). 

#### Expected Impact
Restores write coalescing on the iterated join side (per-probe), and removes a synchronous checkpoint stall proportional to build-side backlog.

#### Complexity & Risks
High — the overlay-merge changes scan semantics (must dedup staged-vs-persisted, honor deletes). Checkpoint change must preserve snapshot consistency.

#### Validation Plan
Measure PUT-batch sizes on the iterated side across a watermark interval (expect larger after); measure checkpoint sync-phase duration on q9/q20.

#### Dependencies
OPT-N01 (probe path), OPT-N09 (statebuf scan).

---

### <a id="opt-n09"></a>[OPT-N09] statebuf forEachEntry linearly rescans ALL liveRows (whole-state) per probe

- **Dimension**: Memory / scan complexity · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
In the **default** statebuf mode, each probe's `entries()/values()` walks the **whole shared `ArrowBinaryBuffer`** (`statebuf.liveRows()`) doing a per-row prefix-compare to keep only rows matching the current key, plus a `HashSet<ByteArrayKey>` build — so per-probe cost scales with **total un-flushed write volume across all keys**, not the probed key's fan-out. The buffer is one instance per `getMapState`, shared across all keyed entries, so `liveRows` contains other keys' rows too.

#### Affected Code Paths
`ForStRsKeyedStateBackend.java:765-806` (statebuf is the default; legacy gated by `-Dforst.rs.mapstate.legacy`); `ForStRsMapState.java:760-790` (forEachEntry statebuf branch loops all `liveRows`, `segmentStartsWith` filter, builds `seenMapKeys`), `:719-733` (`statebufHasPrefix` also O(liveRows)), `:735-746` (`segmentStartsWith` per-byte loop).

#### Root Cause
FACT. The staging buffer has no per-key index; probe-time filtering is a full linear scan of a cross-key buffer.

#### Proposed Optimization
Index the statebuf by composite-key prefix (e.g., a per-key offset map maintained on write) so a probe seeks its own rows in O(matches) instead of O(liveRows). Or flush-and-index more aggressively when `liveRows` grows.

#### Expected Impact
Removes a Java-side O(total-staged-rows) scan per probe — a cost orthogonal to the LSM profile and invisible to v1's engine-only analysis.

#### Complexity & Risks
Medium. Maintain the index on every staged write/delete; memory overhead for the index.

#### Validation Plan
Profile `forEachEntry` self-time vs `liveRows` size on q9/q20; confirm linear relationship, then index and re-measure.

#### Dependencies
OPT-N08 (shares the staging buffer).

---

### <a id="opt-n10"></a>[OPT-N10] Retraction batches collapse to per-row sync FFI via requiresOrderedDispatch

- **Dimension**: Vectorization / ordered-replay amplification · **Queries**: q7/q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
When a join batch carries a DELETE/CLEAR (`retractRecord`) **and** a same-prefix GET/PUT, `requiresOrderedDispatch` routes the **whole batch** to `executeBatchInOfferOrder`, a per-row `executeRequestSync` loop = N separate engine round-trips with no coalescing. The in-code comment quantifies "100×–1000× amplification … ~50s into >600s timeout."

#### Affected Code Paths
`VectorizedExecutor.java:443-449` (`if (requiresOrderedDispatch) return executeBatchInOfferOrder`), `:598-600` (per-row `executeRequestSync` loop), `:622-682`/`:685-709` (`requiresOrderedDispatch`/`hasDeleteOrderingHazard`), `:633-635` (amplification comment). Streaming-join views issue `remove()/clear()` on retract (`JoinRecordStateViews.java:99/146`).

#### Root Cause
FACT. A delete that prefix-collides with a get/put in the same batch trips the ordering hazard, disabling vectorization (and the parallel-iter path) for the whole batch.

#### Proposed Optimization
Finer-grained hazard detection: only the colliding sub-sequence needs ordered replay; non-colliding rows can still vectorize. Or reorder/split the batch so deletes form their own ordered segment while gets/puts batch. Retraction-heavy joins (changelog inputs) benefit most.

#### Expected Impact
Recovers vectorization for retraction-mixed batches — turns N round-trips back into a few batched calls.

#### Complexity & Risks
Medium-high. Correctness of ordering is subtle (delete-before-read semantics); needs careful hazard partitioning + tests.

#### Validation Plan
Count `executeRequestSync` invocations on a retraction-heavy join; A/B wall.

#### Dependencies
OPT-N21 (vectorized/parallel path it currently disables).

---

### <a id="opt-n11"></a>[OPT-N11] Active-memtable point GET is a BTreeMap descent + shard RwLock + per-probe key-copy — not O(1)

- **Dimension**: CPU / read path · **Queries**: q4/q9/q20 · **Tag**: REFINES · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Every memtable-resident probe (the hot-ingest windowed-join working set) costs: one `shard_for_key` hash + a `shards[idx].read()` `RwLock` atomic (even uncontended), an `O(log N)` BTreeMap tree descent with a **full user-key `memcmp` at each visited node**, and a per-probe `KeyBuf::from_slice(key)` copy to build the range lower-bound. The struct doc still advertises "O(1) hash_index" but `hash_index`/`find_latest` were **removed** — the code is an ordered-tree walk.

#### Affected Code Paths
`vectorized.rs:836-842` (`get` → `idx_newest_visible`), `:1719-1728` (`self.index.range(InternalKey::range_start(key)..)`), `:98-103` (`range_start` builds `KeyBuf::from_slice` per probe), `:114-121` (`Ord` compares full user_key per node), `:196` (BTreeMap is the sole index), stale doc `:136`. Shard lock: `sharded.rs:512-516` (`read()` per probe), `:47` (BTreeMap shard). Batch re-acquires per key (`db.rs:7920-7938`).
- **ForSt-C++ contrast:** memtable is a skiplist with cached key-prefix comparisons (and optional hash-skiplist); the per-Get is a Seek with no per-probe bound allocation.

#### Root Cause
FACT (cost is real) / partly REFINES v1's "memtable get is O(1)". The cost is per-probe-bounded (one atomic + log-N memcmp + one small copy), not quadratic — see the OPT-N13/#31 adversarial verdict.

#### Proposed Optimization
(a) Restore a hash index (or a hash-skiplist) for point lookups so the common case is O(1) without the tree memcmp chain; (b) avoid the per-probe `KeyBuf::from_slice` bound copy by seeking with a borrowed key; (c) amortize the shard lock across a batch (acquire once per shard per batch).

#### Expected Impact
Cuts per-probe memtable read CPU; batch lock-amortization removes K−1 atomics per K-key batch.

#### Complexity & Risks
Medium. A hash index reintroduces the structure FRS-C2 removed — must justify with profiling and keep memory bounded. Borrowed-key seek needs lifetime care.

#### Validation Plan
perf-annotate `idx_newest_visible` + `RwLock::read` on q9/q20; measure self-time share.

#### Dependencies
OPT-N12 (same shard/lock infra).

---

### <a id="opt-n12"></a>[OPT-N12] Memtable prefix-scan rebuilds a 16-shard locked snapshot + sort + Arc-copy per probe

- **Dimension**: CPU / read path (iterator) · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Every `getIterPrefix`/`values()` probe acquires **16 shard read-locks**, clones each shard's matching keys into a fresh `Vec<Arc<[u8]>>` (`Arc::from(uk)` = heap alloc + full key memcpy per distinct key), and **sorts each shard** — rebuilt from scratch per probe even when the same partition is re-probed. The "zero-copy-key / refcount-bump" doc is stale: keys went inline (`KeyBuf`), so the Arc can't borrow them. The sort is also redundant — the skiplist range already yields ascending distinct keys.

#### Affected Code Paths
`sharded.rs:703-731` (per-shard loop: 16 `read()` locks, `prefix_scan_keys` clone, `keys.sort()`, push), `:712` (the 16 locks), `:725-729` (redundant `keys.sort()`), stale doc `:739-741`. `vectorized.rs:762-766` (`Arc::from(uk)` per distinct key), `:758-761` (comment: inline KeyBuf forces a copy per distinct key), `:666-667` (prefix_index fast path **removed**), `:737-738` (range already yields sorted distinct → sort is redundant). Built per stream at `db.rs:6457-6469`. Active diag `sharded.rs:699-744` (FRS-CURSOR-SUBCAUSE, dated 2026-06-09) — the team is actively measuring this.

#### Root Cause
FACT. The sharded memtable has no single ordered structure to iterate, so each prefix scan must lock-all-shards, materialize, and merge-sort. The post-scan sort re-sorts already-sorted data.

#### Proposed Optimization
(a) **Drop the redundant `keys.sort()`** (immediate, the range output is already ASC distinct) — pure win. (b) Memoize the per-(prefix, snapshot) cursor so re-probes of the same partition reuse it. (c) Longer-term: a single ordered memtable (skiplist) iterator returning borrowed slices (PORT #36/#71) instead of a 16-shard Arc-copied snapshot.

#### Expected Impact
(a) removes an O(K log K) per shard per probe immediately. (b)/(c) remove the per-probe snapshot+Arc-copy entirely for hot partitions.

#### Complexity & Risks
(a) low (delete a sort, verify ordering invariant). (b) medium (cursor invalidation on writes). (c) high (memtable redesign — overlaps the sharded-vs-skiplist tradeoff that motivated sharding).

#### Validation Plan
The team's own FRS-CURSOR-SUBCAUSE diag already splits lock/scan/construct time — use it to A/B the sort removal and cursor memoization.

#### Dependencies
OPT-N11 (shard locks), OPT-N17 (Arc copies), PORT #36/#71.

---

### <a id="opt-n13"></a>[OPT-N13] k-way merge does 2× O(k) linear peek per emitted key; BinaryHeap deferred

- **Dimension**: CPU / read path (merge) · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
`LazyPrefixIter::next_with_value` — the merge driving every MapState/join prefix probe — runs **two full O(num_tiers) linear passes per emitted key**: Phase A min-find, Phase B winner-select+advance, each an enum-dispatch `peek()` plus `Arc<[u8]>` memcmp. With many tiers (large memtables + L0 + imm) this is a branch-heavy, pointer-chasing per-row cost. The code itself admits "Swapping in a BinaryHeap would save ~3×; defer until profiling demands it." Worse: when an SST source's buffer is exhausted, `peek()` decodes the next block and Arc-copies all its rows **inline on the drain thread** (no read-ahead).

#### Affected Code Paths
`db.rs:10387` (the "~3× / defer BinaryHeap" comment), `:10503-10548` (Phase A linear min-find), `:10555-10569` (Phase B re-peek+advance), `:10174-10213` (`TierKeySource` enum dispatch per peek), `:10251-10318` (SST peek replenishes by decoding next block + Arc-copy inline).

#### Root Cause
FACT. Flat `Vec<TierKeySource>` linearly rescanned every row; no heap; block-boundary decode on the critical drain thread.

#### Proposed Optimization
(a) Replace the linear scan with a `BinaryHeap`/loser-tree (the deferred ~3× win) — single O(log k) advance per emitted key. (b) Read-ahead the next SST block on `bg_read_pool` so block-boundary decode overlaps merge CPU (PORT #82).

#### Expected Impact
~3× on the merge compare cost per the in-code estimate; removes synchronous block-decode stalls at boundaries.

#### Complexity & Risks
Medium. Heap must preserve tie-handling (same key across tiers → newest wins) and the value-decision/fallback logic. Read-ahead needs the OPT-N23 pool headroom.

#### Validation Plan
perf-annotate `next_with_value` self-time on q9/q20; A/B heap vs linear at varying tier counts.

#### Dependencies
OPT-N17, OPT-N18, OPT-N23, PORT #82.

---

### <a id="opt-n14"></a>[OPT-N14] batch-GET L0 has no newest-first short-circuit; per-probe L0 re-sort + linear find_sst

- **Dimension**: LSM read-amplification · **Queries**: q4/q9/q20 · **Tag**: OVERTURNS (v1 "read is cheap") · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
The **production** join GET path (`batch_get_vectorized` Phase-5 L0) probes `get_versions` on **every L0 SST for every pending key** and collects all versions, then sorts per key — there is **no** newest-first lazy walk and **no early break** on the first Put/Delete base. The sister per-key `sst_get` path *was* explicitly rewritten (FRS-L0-SHORTCIRCUIT) to sort L0 by `max_sequence` DESC and stop at the first base ("turns O(L0) data-block reads into ONE") — **that fix never reached the default batch path.** A hot ValueState key present in all ~40 L0 SSTs (L0 holds 40–64 files before stall) costs ~40 data-block reads per probe instead of 1. Compounding: each `get_internal`/`sst_get` re-collects+re-sorts L0 and re-runs an O(L0) disjoint-range check **per probe** (the Version caches no newest-first order); and the point/batch L1+ file pick is a **linear O(files) scan** (`find_sst_for_key_in_cf`) while the analogous range path got the `partition_point` binary search.

#### Affected Code Paths
`db.rs:8044-8062` (L0 loop, no early break), `:8072` (per-key sort after reading all files), `:8629-8643`/`:8594-8608` (the `sst_get` short-circuit the batch path lacks), `:8609-8624` (per-call L0 re-collect+sort), `:8624` (per-call disjoint re-check), `version/mod.rs:100-115` (no cached newest-first L0). L1+: `version/mod.rs:345-352` (linear `find_sst_for_key_in_cf`) vs `:381-390` (`overlapping_ssts_in_range` binary-searches). FFI: `lib.rs:3157` → `db.batch_get:7841`. `write_controller.rs:94-95` (L0 40/64 triggers).
- **ForSt-C++/RocksDB contrast:** files sorted once at version-append (`version_set.cc` `SortFileByOverlappingRatio`); per-Get L0 walk iterates the pre-sorted vector and stops at the first hit.

#### Root Cause
FACT. The short-circuit + once-per-version sort + binary file-pick all exist for the range/per-key paths but were never applied to the default batched join GET path.

#### Proposed Optimization
(a) Port the FRS-L0-SHORTCIRCUIT newest-first lazy L0 walk into `batch_get_vectorized`. (b) Cache the newest-first L0 order + disjoint property **once per Version** (immutable). (c) Migrate `find_sst_for_key_in_cf` to `partition_point` binary search (same sorted invariant the range path uses).

#### Expected Impact
Turns ~40× L0 data-block reads per hot probe into ~1; removes per-probe re-sort/re-validate; turns L1+ file pick from O(files) to O(log files). Largest on high-L0-fan-out hot-ingest (q4/q9/q20).

#### Complexity & Risks
Medium. (a) must respect Put/Delete/Merge base semantics within the batch. (b) version-construction change. (c) low. All three are well-scoped ports of existing forst-rs code.

#### Validation Plan
Instrument data-block reads per probe vs L0 file count on q9; expect collapse to ~1 after (a). A/B q4/q9/q20 wall.

#### Dependencies
OPT-N24 (merge keys also reach this path), PORT #25/#30.

---

### <a id="opt-n15"></a>[OPT-N15] KV-block (default) walk_key reconstructs ≤15 keys + re-parses entry headers per probe, even on cache hit

- **Dimension**: CPU / read path (SST block decode) · **Queries**: q4/q9/q20 · **Tag**: REFINES · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
On the **default** v2 KV block format, every join probe that reaches a flushed SST walks key-by-key from the restart point — each step does `key_buf.truncate(shared)` + `extend_from_slice(suffix)` + a full `Ordering::cmp(key)` — so up to `KV_RESTART_INTERVAL-1 = 15` keys are prefix-reconstructed and byte-compared before the target. The `seek_restart` binary search **re-parses the full entry header** (varint shared/non_shared/value_tag + fixed64 seq + op byte) at each mid just to read the key, decoding seq/op/value-tag it throws away. **All of this runs on a decoded-block-cache HIT** — the cache saves decompress, not the per-probe key reconstruction. The Arrow path is no better on a hit: 4× `as_any().downcast_ref` dynamic checks + a 5th in `search_key_in_batch` + a `memcmp` binary search, none cached alongside the batch.

#### Affected Code Paths
`kv_block.rs:413-438` (`walk_key` truncate+extend+cmp per entry), `:65` (`KV_RESTART_INTERVAL=16`), `:444-471` (`seek_restart` full `parse_entry` per mid), `:517-567` (`parse_entry` decodes 3 varints + fixed64 + op), `:75-92` (KV default-on since 2026-06-04). Arrow hit: `reader.rs:355-357` (returns un-projected RecordBatch clone), `:604-631` (4× downcast per probe), `:118-147` (5th downcast + memcmp binary search).
- **ForSt-C++ contrast:** `block.cc` `BinarySeek`/`DecodeKeyFunc` decodes only shared+non_shared (never seq/value) in the search loop; seq/op live **after** the key, not before.

#### Root Cause
FACT. forst-rs's KV entry layout places seq(8B)+op **before** the key, so the key is unreachable without parsing the value-tag varint first; and the typed Arrow columns are re-derived per probe rather than cached with the batch.

#### Proposed Optimization
(a) Reorder the KV entry so the key is reachable after only shared+non_shared (C++ layout), so `seek_restart` decodes only key-length varints. (b) Smaller restart interval or store full keys at restarts to shrink the linear reconstruction run (PORT #49). (c) Cache the projected typed columns alongside the Arrow batch in the block cache (compute the 4 downcasts once).

#### Expected Impact
Cuts per-probe SST decode CPU on cache hits (the q9 steady state). Bounded per-probe (not quadratic — see #31), so a contributor to the ~10-25% read micro-opt envelope, not the dominant lever.

#### Complexity & Risks
(a) SST format change → needs versioned read compatibility. (b)/(c) lower risk. All change on-disk/decoded layout — verify byte-equivalence on read of existing SSTs.

#### Validation Plan
perf-annotate `walk_key`/`seek_restart`/`downcast_ref` on a cache-resident q9 working set.

#### Dependencies
OPT-N16 (value materialization), PORT #49.

---

### <a id="opt-n16"></a>[OPT-N16] Engine point-get to_vec() at every tier; borrowed primitives + batch_get_arrow exist but are unwired

- **Dimension**: Zero-copy · **Queries**: q7/q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Every resolved point-get copies the value into a fresh `Vec<u8>` at **every** tier — `VectorizedMemTable::get` `value_at(..).to_vec()`, `KvBlock::lookup` `payload[s..e].to_vec()`, SST `get_versions` `values.value(row).to_vec()` — **even on a pure block-cache hit** (the bytes already live in an Arc-shared, immutable Arrow `Buffer`/`Arc<KvBlock>`). The FFM bridge then `ptr::copy_nonoverlapping`s the same bytes a **second** time into the Java buffer. The default `VectorizedExecutor` GET routes through this `Vec<Option<Vec<u8>>>` path; the already-built **zero-copy `batch_get_arrow`** (builds Arrow output in-place, "eliminates all per-value memcpy") is only reachable from the **non-default** `ForStRsStateExecutor`. Borrowed memtable primitives (`get_borrowed`, `MemtableValueRef::Inline`, `get_pinned_ptr`) likewise exist but are "currently unused on the hot path."

#### Affected Code Paths
`vectorized.rs:836-842` (`to_vec`), `kv_block.rs:383` (`to_vec`), `reader.rs:645`/`:540` (`to_vec`), `:98-100` (`LookupResult.value: Option<Vec<u8>>` owned), `db.rs:8392-8397` (`get_internal` returns owned). Second copy: `lib.rs:3195` (`copy_nonoverlapping` into out_buf). Unwired zero-copy: `db.rs:8218` (`batch_get_arrow`), `lib.rs:2579` (`frs_batch_get_arrow`), `ForStRsStateExecutor.java:359` (only caller, not default); default seam `VectorizedExecutor.java:2557` → `vectorizedBatchGet` (Vec path); `ForStRsAsyncKeyedStateBackend.java:1157`. Unused borrows: `vectorized.rs:904-925` (`get_borrowed`), `mod.rs:64-67`, `sharded.rs:543-545` (`get_pinned_ptr`).
- **RocksDB/ForSt-C++ contrast (PORT #44):** `MultiGet` writes `PinnableSlice* values` that pin the block-cache entry — zero value copy on a hit.

#### Root Cause
FACT. The copy-elimination work is **not something to build (v1's OPT-03 framing) — it is built and merely unrouted.** `LookupResult.value: Vec<u8>` locks the whole stack into owned copies.

#### Proposed Optimization
(a) **Wire the default executor to `batch_get_arrow`** (the lowest-risk immediate win — it exists end-to-end). (b) Change `LookupResult`/`GetResult` to a pinned-slice type (`Arc<Buffer>` + offset/len) so cache hits borrow (PinnableSlice semantics). (c) Use the existing `get_borrowed`/Inline primitives on the memtable hot path.

#### Expected Impact
Removes 1–2 per-value heap allocs + memcpys per probe on the steady-state cache-hit join regime. (a) is a routing change, not new copy-elimination logic.

#### Complexity & Risks
(a) low-medium (verify `batch_get_arrow` parity + correctness under the default classifier). (b) medium (lifetime threading of the cache Arc through the FFI boundary). 

#### Validation Plan
A/B q9/q20 with default executor routed to `batch_get_arrow`; count allocs via heaptrack on the read path.

#### Dependencies
OPT-N17, OPT-N19, PORT #44/#82.

---

### <a id="opt-n17"></a>[OPT-N17] Iterator buffers Arc::from(key)+Arc::from(value) per row even on a cache hit

- **Dimension**: Zero-copy (iterator path) · **Queries**: q9/q20 · **Tag**: REFINES · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
The join-probe iterator buffers each SST row as `SstHeadRow { key: Arc::<[u8]>::from(view.key), value: view.value.map(Arc::<[u8]>::from) }` — **one heap alloc + memcpy for the key AND one for the value per row** — even though `for_each_row_in_block` serves the block zero-copy from the decoded-block cache. The value is allocated **unconditionally**, even when the join filters on the key and never reads the value. The FFM then memcpys those Arc bytes a 3rd/4th time into the chunk buffer. This is the symmetric iterator-path twin of OPT-N16's point-get `to_vec`.

#### Affected Code Paths
`db.rs:10311-10318` (`SstHeadRow` key+value both copied per row), `:10205-10232` (eager unconditional value Arc). Cache serves zero-copy: `reader.rs:790-801` (RowView borrow tied to block lifetime, dropped next iteration → forces the copy-out), `:857-868` (`scan_borrowed` zero-copy callback "skips the per-row allocation, prefer in new code" — exists but the iterator build uses the owning path). FFM re-copy: `lib.rs:4947-4949`.
- **PORT #82:** ForSt-C++ iterators return `Slice` borrowing into the pinned cached block.

#### Root Cause
FACT. The RowView borrow can't outlive the block, so `TierKeySource::Sst::peek` copies into owned Arcs. The owning scan path is used instead of the existing `scan_borrowed`.

#### Proposed Optimization
Return an `Arc<KvBlock>`-pinned RowView (Arc-clone of the block + offset range) instead of copying bytes, so the row borrows-or-pins the cached block and outlives the block clone — the prerequisite (Arc-shared cache entries) already exists. Gate value materialization behind actual use (don't Arc the value if the consumer only needs the key).

#### Expected Impact
Removes 2 heap allocs + 2 memcpys per buffered row on cache hits — the steady-state join scan. Value-gating removes the value copy entirely for key-only joins.

#### Complexity & Risks
Medium. Threading the block Arc through `TierKeySource`/`SstHeadRow` and the merge; lifetime/ownership care. 

#### Validation Plan
heaptrack the iterator drain on q9/q20; count per-row allocs before/after.

#### Dependencies
OPT-N13 (merge consumes these), OPT-N16, PORT #82.

---

### <a id="opt-n18"></a>[OPT-N18] Value-carrying merge is BYPASSED for memtable-resident keys → full get_internal re-walk + extra copy

- **Dimension**: Read-amplification · **Queries**: q4/q9/q20 · **Tag**: OVERTURNS (the "q4 2× read-path fix is general") · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
The value-carrying merge optimization (inline Put resolution that was supposed to remove the per-key double-walk) **only fires when the newest tier is an SST**. `next_with_value` forces `ValueDecision::Fallback` whenever any source at the min key is a `MemCursor` (`head_sst_info` returns `None` for memtable sources), and the caller then re-resolves via a full `db.get_internal(key, u64::MAX)` — re-walking active+imm+resident+SST — plus an extra `Arc::<[u8]>::from(value)` per emitted key. **For q7/q9/q20 windowed joins the probed key was just written, so it lives in a memtable → Fallback is the COMMON path, not the rare one** the comment ("a cheap memtable hit") implies. Cheap in I/O, but a second memtable probe + alloc per row on a write-then-read join side.

#### Affected Code Paths
`db.rs:10560-10575` (`mem_present` → `ValueDecision::Fallback`), `:10371` (`MemCursor → None` in `head_sst_info`), `:6367-6373`/`:6892-6897` (Fallback → `get_internal` + `Arc::from`), `:8399-8424` (`get_internal` restarts at active memtable). Justifying comment `:10364-10366`.

#### Root Cause
FACT. The fast path discards the memtable cursor it already peeked and re-issues a full tier walk for memtable-resident winners — exactly the hot windowed-join case.

#### Proposed Optimization
Carry the value **directly from the peeked memtable cursor** when the winner is memtable-resident (the cursor already located it), instead of falling back to `get_internal`. Resolve Merge winners in-place where possible; only true fallbacks re-walk.

#### Expected Impact
Removes a second tier-walk + an Arc copy per emitted key on the common (memtable-resident) join path — directly the q4/q9/q20 hot-ingest regime.

#### Complexity & Risks
Medium. Must preserve MVCC/snapshot correctness (the cursor's value at `read_seq`), and Merge-operand resolution semantics.

#### Validation Plan
Count `get_internal` Fallback invocations vs emitted keys on q9 (expect near-1:1 today, near-0 after); A/B wall.

#### Dependencies
OPT-N13 (same merge), OPT-N17 (value carry).

---

### <a id="opt-n19"></a>[OPT-N19] Iterator wire format is row-major AoS (GET is columnar SoA); full key incl shared prefix re-copied per row

- **Dimension**: Arrow / Columnar · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Two asymmetries on the join-dominant iterator path:
1. **Layout:** `executeGets` returns results in a **columnar SoA** buffer (`outOffsets`/`outData`/`outValidity` — the exact Arrow varbinary triple), but the **iterator** drain that dominates q7/q9/q20 uses an **interleaved row-major** chunk (`[klen u32][vlen u32][key][value]` per row), decoded by a serial branchy per-row pointer walk that can't SIMD/bulk-skip. The two paths solve the same "return N varbinary values to Java" problem with opposite layouts; the slower one is the hot one.
2. **Redundant prefix:** every entry ships its **complete** key (`keyGroup|stateName|namespace|userKey`), but all entries in one probe share the identical scan prefix, which Java immediately strips via `deserializeUserKey(view, prefixLen)`. For a windowed-join MapState the prefix is tens of bytes, byte-identical across all K entries — so the engine memcpys `K × prefixLen` guaranteed-dead bytes per probe and ships them across FFM.

#### Affected Code Paths
GET columnar: `VectorizedExecutor.java:1306-1318`. Iterator row-major: `ForStRsDBIterRequest.java:654-678` (per-row klen/vlen walk); engine writes `[klen LE][vlen LE][key][value]` at `lib.rs:4579/4891/4936-4950`. Redundant prefix: `lib.rs:4612-4640` (full key as `IterKey::Arc`), `:4947` (copies full klen incl prefix), Java strips at `ForStRsDBIterRequest.java:455/665`; prefix is non-trivial (`ForStRsMapStateV2.java:260-262/325-333`).

#### Root Cause
FACT. The iterator chunk format predates / diverges from the columnar GET format and has no shared-prefix elision.

#### Proposed Optimization
(a) Switch the iterator chunk to the **same columnar SoA** (offsets+data, optional separate key/value columns) so Java decodes with no per-row length headers and can bulk-copy. (b) **Elide the shared scan prefix** — ship it once per chunk + per-row suffixes only; Java reconstructs (or just uses the suffix, since it strips `prefixLen` anyway). 

#### Expected Impact
(a) removes the serial per-row header walk (enables bulk/SIMD decode). (b) removes `K × prefixLen` redundant memcpy + FFM transfer per probe, largest on high-fan-in heavy joins.

#### Complexity & Risks
Medium. Both change the FFM iterator wire format → coordinated Rust+Java change + version care. (b) needs the engine to know the common prefix length (it does — it's the scan prefix).

#### Validation Plan
Measure bytes transferred per probe vs fan-out on q9/q20 (expect drop ∝ prefixLen); micro-bench columnar vs row-major decode.

#### Dependencies
OPT-N16 (columnar GET already proves the layout), OPT-N17.

---

### <a id="opt-n20"></a>[OPT-N20] SST bloom = scalar 8-probe loop + re-hash per L0 file + 256-bit unaligned blocks

- **Dimension**: CPU / SIMD · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
The per-probe bloom test is a **scalar, branchy, non-prefetched 8-probe loop** (`wrapping_mul`+shift+data-dependent early-return per salt) that the optimizer can't vectorize. The batch join probe **re-hashes the same key with xxh64 once per L0 SST file** (N×M redundant hashes for N keys over M overlapping L0 files) — a `check_hash(u64)` primitive exists to hash-once-and-reuse but no batch path uses it. Blocks are 256-bit (32 B) in a `Vec<[u32;8]>` (4-byte aligned), so a block at an odd index **straddles two 64 B cache lines** → up to 2 L1 misses per probe; no prefetch anywhere (no SIMD intrinsic exists in `sst/` or `engine/`).

#### Affected Code Paths
`bloom_filter.rs:63-71` (scalar branchy 8-probe loop), `:136-138` (`check` always re-hashes), `:123-128` (`check_hash` exists, unused by batch), `:80`/`:102-103` (`Vec<[u32;8]>` 32 B blocks), `:49-51` (no alignment guarantee). Re-hash sites: `db.rs:8044-8061` (per-L0-file loop), `reader.rs:580` (`get_versions` re-hashes per call).
- **ForSt-C++ (PORT #6):** `bloom_impl.h:231-320` `FastLocalBloomImpl::HashMayMatchPrepared` — branchless AVX2 (`_mm256_mullo_epi32`+`_mm256_sllv_epi32`+`_mm256_testc_si256`), 8 probes/op; `:130-132` block tied to a 64 B cache line; `:225-282` `PrepareHash` software-prefetches the cache line.

#### Root Cause
FACT. Scalar implementation + per-file re-hash + sub-cache-line block layout.

#### Proposed Optimization
(a) Precompute one bloom hash per key up front and pass it via `check_hash` to every file (kills the N×M re-hash). (b) 64 B cache-line-aligned blocks + a `PrepareHash`-style prefetch. (c) SIMD the 8-probe test (portable-simd / target_feature AVX2/NEON).

#### Expected Impact
(a) removes M−1 hashes per key per batch (biggest, simplest). (b)/(c) cut per-probe bloom cycles + halve worst-case L1 misses. Bounded per-probe — part of the read micro-opt envelope.

#### Complexity & Risks
(a) low (use existing `check_hash`). (b) layout change (re-gen blooms or read-compat). (c) medium (SIMD portability; the codebase currently forbids `unsafe` in places — use portable-simd).

#### Validation Plan
perf cache-miss + cycles on bloom on q9; count xxh64 calls per batch before/after (a).

#### Dependencies
OPT-N14 (same L0 batch loop), PORT #6.

---

### <a id="opt-n21"></a>[OPT-N21] Parallel-iter path is WIRED but gated-off, BUILD-only, and unreachable from sync values()/entries()

- **Dimension**: Vectorization / parallelism · **Queries**: q7/q9/q20 · **Tag**: OVERTURNS (v1 "frsVecIterPrefixOpenBatchParallel UNUSED") · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
v1's anchor "`frsVecIterPrefixOpenBatchParallel:4056` UNUSED" is **stale**. It is now wired into a complete `executeItersBatchedParallel` path backed end-to-end by an engine read-pool (`bg_read_pool`, the ForSt read-io-parallelism analog). **But it cannot help q7/q9/q20** because: (a) it's env-gated `FRS_RS_PARALLEL_ITER=1` (default OFF) **and** `fresh.size()>1`; (b) even ON it parallelizes only the first-chunk **BUILD** (overlapping-SST locate + reader open) — Pass-3 first-chunk fill is serial single-threaded, and every `_next` continuation (the common multi-chunk join case) drains serially on the coordinator thread; (c) it's reachable only from the executor's batched-iters path — the **synchronous `MapState.values()/entries()`** the join actually calls always routes to the serial single-handle `frsVecIterPrefixOpen` and never the parallel variant. The in-code comment itself says "which is why q9/q7/q20 saw no speedup."

#### Affected Code Paths
`VectorizedExecutor.java:1374` (`PARALLEL_ITER` gate), `:1398-1440` (only fresh opens parallel; rest `drainIterSerial`), `:1494/1535` (`executeItersBatchedParallel` → `frsVecIterPrefixOpenBatchParallel`), `:1395-1397` ("no speedup" comment). Engine: `lib.rs:5575`/`:5650-5661` (Pass-2 parallel BUILD-only, Pass-3 serial fill), `db.rs:6137-6164` (`batch_open_prefix_iters_parallel` submits BUILD to `bg_read_pool`), `:9805-9811` (pool clamp 1..4). Unreachable from sync path: `ForStRsMapState.java:619-623/868-935` (`values()`→`forEachEntry`→`frsVecIterPrefixOpen` single serial handle, `:881`), join entry `AbstractStreamingJoinOperator.java:137-162`, `JoinRecordStateViews.java:150-152`.
- **PORT #84/#89:** ForSt-C++ `ForStIterateOperation.process:61-94` submits the **whole drain per request** to the read pool (full parallel drain, not just build); `ForStGeneralMultiGetOperation` splits GET batches into `readIoParallelism` coalesced `multiGetAsList` slices.

#### Root Cause
FACT. The parallelism is BUILD-only and on a path the synchronous join probe doesn't take; the continuation drain — where the time is — stays serial.

#### Proposed Optimization
(a) Make `MapState.values()/entries()` reachable to the parallel/batched path (or give the sync probe a batched-iter entry). (b) Parallelize the **drain**, not just the build — submit the whole per-iterator drain to the read pool (ForSt's model, PORT #84). (c) Re-evaluate the default gate once correctness (OPT-N22) and the pool ceiling (OPT-N23) are fixed.

#### Expected Impact
Unlocks the only existing parallelism for the actual join path; overlaps continuation I/O with operator CPU.

#### Complexity & Risks
High. Drain parallelism interacts with the depth-1 mailbox model and the OPT-N22 correctness race — must be co-designed. The BUILD-only scaffolding is reusable.

#### Validation Plan
With `FRS_RS_PARALLEL_ITER=1` measure that continuations still serialize today; prototype drain-parallel and A/B q9 with correctness checks.

#### Dependencies
OPT-N22, OPT-N23, OPT-N02, PORT #84/#89.

---

### <a id="opt-n22"></a>[OPT-N22] RoutingStateExecutor is fully built but key-AGNOSTIC + opt-in → can't fix windowed joins

- **Dimension**: Scheduling / thread-affinity · **Queries**: q7/q9/q20 · **Tag**: OVERTURNS (v1 "gated-off scaffolding; affine routing is a spec") · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
`RoutingStateExecutor` is now a **complete N-worker offloading executor** with a real `fullyLoaded()=free.isEmpty()` and a depth>1 path, wired behind `FRS_RS_PARALLEL_EXECUTOR=1`. But (a) it is **opt-in** — the default is still the inline depth-1 `VectorizedExecutor` — and (b) its `leaseWorker` pops the next **free** worker with **no key-group affinity**, so disjoint batches of the same key can land on different workers with **disjoint per-worker `MapStateCache`s** → the documented **q8 windowed-join ~40% under-emit** (3,010,888 → 1,819,576), which is exactly why Flink HEAD `e2aa17ade11` reverted OPT-01 to opt-in. The spec'd key-group-affine routing exists **only in docs** (commits `c8e8ec5f5..a865dc057` are all `docs(forst-rs)` — no code landed).

#### Affected Code Paths
`RoutingStateExecutor.java:147-180` (real worker offload), `:205-209` (`fullyLoaded()=free.isEmpty()`), `:138-145/252-266` (`createRequestContainer`→`leaseWorker`→`free.removeFirst()`, key-agnostic). Default wiring: `ForStRsAsyncKeyedStateBackend.java:1149-1160` (returns `VectorizedExecutor` unless `FRS_RS_PARALLEL_EXECUTOR=1`), `:1142-1148` (q8 under-emit rationale). Revert: Flink HEAD `e2aa17ade11`.

#### Root Cause
FACT. Per-worker `MapStateCache` + key-agnostic routing fragments a key's cached state across workers → windowed-join under-emit. The fix (key-group-affine routing so a key-group always lands on the same worker) is unimplemented.

#### Proposed Optimization
Implement **key-group-affine `leaseWorker`** (`worker = keyGroup % N`, or a stable hash) so a key's batches always hit the same worker's cache — the documented design. Then the offload executor (depth>1) becomes correctness-safe and can be re-defaulted, unblocking real async overlap (the v1 dispatch lever).

#### Expected Impact
This is the **dominant lever** per the v1-confirmed thesis: a correct depth>1 executor removes the inline-mailbox serialization that gates all three queries. Affinity also makes per-worker caches effective.

#### Complexity & Risks
High + correctness-critical. Must reproduce q8 deterministically first (a known nondeterministic race per project memory) and prove the affine router fixes it before default-on. Per-worker cache coherence under rescaling.

#### Validation Plan
Build the deterministic q8 repro; verify affine routing restores exact emit count; A/B q7/q9/q20 wall with depth>1.

#### Dependencies
OPT-N21, OPT-N23; gated on the q8 deterministic repro.

---

### <a id="opt-n23"></a>[OPT-N23] Read pool hard-clamped to 4 threads, process-global shared vs ForSt per-executor

- **Dimension**: Scheduling · **Queries**: q9/q20 · **Tag**: REFINES · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
`bg_read_pool()` is a single **process-global** `OnceLock` `WorkerPool` clamped to `cores.clamp(1,4)`. Every parallel open from every join subtask contends on these ≤4 threads, while ForSt gives **each** `ForStStateExecutor` its own `readIoParallelism` pool. On an 8c/32g box with multiple parallel join subtasks, forst-rs build parallelism saturates at 4 regardless of subtask count.

#### Affected Code Paths
`db.rs:9805-9812` (static `OnceLock`, `n=clamp(1,4)`), `:6151` (`batch_open_prefix_iters_parallel` uses the shared pool), `:9799-9804` (doc: "mirrors ForSt's read-io-parallelism" — but it sits below the FFM boundary). Contrast `ForStStateExecutor.java:128-133` (per-executor fixed read-thread pool).

#### Root Cause
FACT. Single shared, hard-capped pool vs per-executor pools.

#### Proposed Optimization
Make the pool size configurable beyond 4 and/or per-backend-instance (or scale with available cores and subtask count), matching ForSt's per-executor model. Co-design with OPT-N21 drain parallelism (more threads only help once the drain is parallel).

#### Expected Impact
Lifts the parallelism ceiling for multi-subtask join workloads on bigger boxes.

#### Complexity & Risks
Low-medium. Oversubscription risk if every backend gets a full pool — needs a global budget or work-stealing.

#### Validation Plan
On 8c/32g with parallelism>4, measure read-pool occupancy vs subtask count; A/B pool sizing.

#### Dependencies
OPT-N21.

---

### <a id="opt-n24"></a>[OPT-N24] Merge-stored state de-vectorizes batch GET into N serial get_internal walks

- **Dimension**: Vectorization / batch-shatter · **Queries**: q9/q20 · **Tag**: NEW · **Confidence**: high (FACT) · **Priority**: · **Status**:

#### Symptom & Impact
Whenever a probed key resolves to a **Merge** operand (aggregating/list join sides, count maps), **every phase** of `batch_get_vectorized` falls back to per-key `get_internal`, which re-captures memtables and re-walks the LSM **independently** — discarding the batch's version snapshot, prefetch warmup, and per-file reader grouping. A Merge-heavy join side collapses the batch into N serial single-key reads. The docstring concedes merge batching "has no proven win" — i.e. it was never vectorized.

#### Affected Code Paths
`db.rs:7934` (phase1 Merge→`get_internal`), `:7972` (phase3), `:8086/8116/8166/8194` (L0/L1+), `:8392-8431` (`get_internal` re-captures memtables independently), `:7858` (docstring admission).
- **PORT #25:** ForSt-C++ `FilePickerMultiGet` (`version_set.cc:352/561/587`) threads one `MergeContext` per key through a single descending file-pick pass, so a merge key never restarts the walk.

#### Root Cause
FACT. Merge keys are ejected from the vectorized pass into independent top-of-LSM walks.

#### Proposed Optimization
Port the per-key-merge-context-in-range model: carry a per-key `MergeContext` + a shrinking key range down the levels in one pass (FilePickerMultiGet), so merge keys stay on the vectorized path and accumulate operands as the same descent proceeds.

#### Expected Impact
Keeps Merge-heavy join sides (q9 counts/aggregates) vectorized instead of O(N) serial walks — directly the q9 regression regime.

#### Complexity & Risks
High. Significant batch-read restructuring; must preserve snapshot-correct merge-operand collection. The C++ design is the proven template.

#### Validation Plan
On q9, count `get_internal` fallbacks per batch (expect ~N today, ~0 after); A/B wall.

#### Dependencies
OPT-N04 (count map becomes merge-typed), OPT-N14 (shared L0 walk), PORT #25.

---

## 6. ForSt-C++ / RocksDB PORT catalog

Concrete techniques to borrow, `file:line` both sides. Cross-referenced to the OPT-N items that consume them.

| PORT | Technique | C++ source | forst-rs gap | Feeds |
|---|---|---|---|---|
| P1 | **PinnableSlice** — pin block-cache entry, return borrowed `Slice` (zero value copy on hit) | `include/rocksdb/db.h:717-723`, `slice.h:138-160` | `LookupResult.value: Vec<u8>` always owns | OPT-N16, N17 |
| P2 | **MultiGetContext** — stack `LookupKeys`, `Mask` skip-bitvector, per-SST range narrowing, MAX_BATCH 32 | `table/multiget_context.h:103/161-186` | per-probe `Arc<Mutex>` error slot + `prefix.to_vec()` + mpsc, no grouping | OPT-N14, N24 |
| P3 | **FilePickerMultiGet** — per-key `MergeContext` threaded through one descending file pass | `db/version_set.cc:352/561/587` | merge keys restart `get_internal` per key | OPT-N24 |
| P4 | **Block-grouped MultiGet** — sort batch keys, one block-iterator pass per data block | `table/block_based` + MultiGetContext | each key re-runs bloom+index+seek per file | OPT-N14 |
| P5 | **Parallel iterate drain** — `executor.execute` per iter request (full drain off-mailbox) | `ForStIterateOperation.java:61-94` | only BUILD+first-chunk parallel; drain serial | OPT-N21 |
| P6 | **read-io-parallelism per-executor pool** | `ForStStateExecutor.java:128-133`, `ForStOptions.java:286` | one process-global pool clamp(1,4) | OPT-N23 |
| P7 | **Single skiplist MemTableIterator** — Seek-once, borrowed-Slice stream, no per-probe sort/copy | C++ skiplist memtable model | 16-shard snapshot+sort+Arc-copy per probe | OPT-N12 |
| P8 | **Bounded resumable MapState cache** — `loadCache` ≤CACHE_SIZE_LIMIT, resumable cursor, early-break | `ForStSyncMapState.java:637-686` | `forEachEntry` materializes full prefix eagerly | OPT-N01 |
| P9 | **Resume one live iterator across continuations** (no re-seek/re-dispatch) | `ForStDBIterRequest.java:120-123` | continuation re-enters classifier/executor per 128 | OPT-N02 |
| P10 | **64-byte cache-line bloom + AVX2 8-probe + PrepareHash prefetch** | `util/bloom_impl.h:130-132/225-320` | 32 B unaligned blocks, scalar branchy loop, no prefetch | OPT-N20 |
| P11 | **index_block_restart_interval=1** — full keys in index, no reconstruction on navigation | `include/rocksdb/table.h:289-292` | single restart interval 16, in-block linear reconstruction | OPT-N15 |
| P12 | **adaptive_readahead / async_io** — prefetch next block off the critical read path | `include/rocksdb/options.h:1604/1725` | next-block decode synchronous on drain thread | OPT-N13, N17 |

---

## 7. RocksDB / ForSt absolute-advantage refresh (where forst-rs structurally lags)

1. **Borrowed reads (PinnableSlice) everywhere.** RocksDB/ForSt never copy a cache-resident value until the API boundary; forst-rs copies at every tier and again across FFM (OPT-N16/N17). This is the single most pervasive structural gap.
2. **MultiGet as a first-class coalesced+parallel primitive.** Stack-allocated key batch, skip-bitvector, per-SST range narrowing, per-key merge context, split across read threads (P2/P3/P4/P6). forst-rs treats each probe as independent and shatters on Merge (OPT-N24).
3. **Off-mailbox read parallelism at the framework layer.** ForSt's `ForStStateExecutor` returns futures the mailbox never blocks on; forst-rs blocks the mailbox on the FFM downcall and only forks a BUILD-only pool *below* the boundary (OPT-N21/N23). This is the engine-side mirror of the v1 dispatch thesis.
4. **Single ordered memtable with borrowed-Slice iteration.** C++ skiplist Seeks once and streams borrows; forst-rs's 16-shard design must snapshot+sort+copy per probe (OPT-N12, P7).
5. **Once-per-version file ordering + binary file-pick + L0 short-circuit on every path.** C++ sorts files at version-append and every Get reuses it; forst-rs re-sorts/re-validates per probe and the short-circuit didn't reach the batch path (OPT-N14).
6. **Lazy, bounded, resumable, early-breakable iteration** at both the sync MapState layer (P8) and the request layer (P9) — forst-rs materializes eagerly and re-dispatches per chunk (OPT-N01/N02).

**Where forst-rs is at parity or ahead (do not re-spec):** zero-copy iterator decode for `ForStRsMapStateV2` (§2), per-executor reusable iter chunk buffer (§2), columnar GET return path (already SoA — the lever is to extend it to the iterator, OPT-N19), and an existing-but-unwired `batch_get_arrow` (OPT-N16 is a routing change, not new work).

---

## 8. Scope notes

- Single document, per the HARD RULE. Priority/Status blank by design.
- No 8c/32g benchmarks were run; every item ships a Validation Plan. Confidence is `FACT` (read this round) vs `HYP` (to-profile); all 24 OPT-N items are FACT-grounded on `file:line`.
- The **highest-leverage path** per this round: **OPT-N22 (key-group-affine executor → correct depth>1)** is the dominant lever (confirms v1); **OPT-N01/N02/N03/N04 (operator-side materialization/continuation/RMW)** are the largest *net-new* surface; **OPT-N16 (wire `batch_get_arrow`)** and **OPT-N12 (drop redundant sort)** are the lowest-risk immediate wins. Read micro-ops (OPT-N11/13/14/15/20) are real but bounded — the ~10-25% envelope, not the headline.

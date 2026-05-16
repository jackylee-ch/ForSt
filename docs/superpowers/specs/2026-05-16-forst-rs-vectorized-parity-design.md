# Forst-RS Vectorized Parity Design (V1)

**Status:** Design — ready for implementation
**Date:** 2026-05-16
**Scope:** Close every functional gap between Forst-RS and community ForSt, with end-to-end vectorization and zero-copy as a V1 requirement on every state primitive (no scalar-then-vectorize phases).
**Companion docs:** SP1 (state types V2), SP2 (iterator vectorization), SP5 (snapshot/restore), SP6 (V1 sync vectorization). This spec is the umbrella above those four.

---

## Capability audit — starting point

Forst-RS already has: ValueState V2, MapState V2, ListState V2 (scalar add), snapshot/restore, FFM round-trip, prefix iteration, Q3-class vectorized batch for GET/PUT/DELETE.

| # | Gap vs community ForSt | Severity |
|---|---|---|
| 1 | `ReducingState` — `UnsupportedOperationException` | hard parity break |
| 2 | `AggregatingState` — `UnsupportedOperationException` | hard parity break |
| 3 | `ListState.add` / `addAll` batch path — placeholder only | hot path for Q5/Q7 |
| 4 | Range-bounded iteration — prefix-only today | needed for windowed scans |
| 5 | Vectorized columnar dispatch — only `ValueState`+`MapState`; List/Reducing/Aggregating still per-row | breaks "end-to-end vectorization" goal |
| 6 | On-disk key layout divergence — Forst-RS encodes `stateName` inline; ForSt uses `VoidNamespace`/CompositeKey | blocks cross-backend checkpoint restore — **declared a V1 non-goal** |
| 7 | Off-heap write staging — only MapState has it (SP6 6.3); ValueState/ListState still copy | ~µs per call |

V1 closes all seven on a single unified vectorized dispatch path. Gap 6 is closed by declaring it a non-goal: Forst-RS writes its own format; cross-backend interchange is via Flink savepoint, not raw checkpoint files.

---

## Framework decisions (locked)

| Decision | Choice |
|---|---|
| Scope | One combined spec, vectorized from day one. Every state primitive (existing + new) ships on the unified dispatch path; there is no scalar fallback. |
| On-disk format compatibility with ForSt | **No.** Forst-RS writes its own layout (`kg ‖ serializedK ‖ '/' ‖ stateName ‖ '/'`). Cross-backend interchange is via Flink savepoint only. Raw checkpoint files do not interchange. |
| Approach | (A) Unified Vectorized Dispatch — one columnar path with 8 typed request kinds (6 user-facing + 2 V1.x classifier sub-splits). Every state primitive implements operations as `VectorizedStateRequest` payloads. |
| Merge semantics | `APPEND_MERGE` is **ListState-only** by principled non-goal (see §1 §a). Reducing/Aggregating compose `GET → user-combiner-in-Java → PUT` via a cache-mediated RMW path. |

---

## Section 1 — Architecture & dispatch table

### Tiering of design fixes

| Fix | Tier | Reason |
|---|---|---|
| APPEND_MERGE restricted to ListState | **V1 design-frozen** | Interface contract is irreversible. |
| Iterator lifecycle bound to per-request Arena | **V1 design-frozen** | Native handle leaks are silent and undebuggable. |
| GET split (SAME_KEY vs CROSS_KEY) | V1.x perf follow-up | Classifier-internal; no API impact. |
| DELETE split (POINT vs PREFIX) | V1.x perf follow-up | Classifier-internal; no API impact. |

V1 ships **6 request kinds**; V1.x introduces 2 classifier sub-splits behind the same Java-facing API.

### V1 dispatch table

```
Java state class ──> VectorizedStateRequest ──> VectorizedClassifier
                                                       │
   ┌────────────┬────────────┬────────────┬─────────────────┬──────────────┬──────────────┐
   ▼            ▼            ▼            ▼                 ▼              ▼
  GET          PUT         DELETE     APPEND_MERGE      ITER_PREFIX    ITER_RANGE
   │            │            │       (ListState only)         │              │
   └────────────┴────────────┴────────────┴─────────────────┴──────────────┴──────────────┘
                                          │
                            ColumnarBatchBuffer (per-slot Arena)
                                          │
   ┌────────────┬────────────┬────────────┬─────────────────┬──────────────┬──────────────┐
   ▼            ▼            ▼            ▼                 ▼              ▼
frs_vec_get  frs_vec_put  frs_vec_del  frs_vec_merge       frs_vec_iter_  frs_vec_iter_
                                        _append            prefix         range
```

V1.x classifier sub-splits (Java API unchanged):
- `GET` → `GET_SAME_KEY` (shared `kg ‖ serializedK` prefix → single SST block touch) vs `GET_CROSS_KEY` (Q3 hot path → native shard-coalesced parallel IO).
- `DELETE` → `POINT` (tombstone PUT) vs `PREFIX` (native `delete_range`).

### Dispatch mapping per state type (V1)

| State type | GET | PUT | DELETE | APPEND_MERGE | ITER_PREFIX | ITER_RANGE |
|---|---|---|---|---|---|---|
| ValueState | `value()` ★ | `update()` | `clear()` | — | — | — |
| MapState | `get(uk)`, `contains(uk)` | `put(uk,v)` | `remove(uk)`, `clear()` | — | `keys`/`values`/`entries` | — |
| **ListState** | `get()` | `update(list)` | `clear()` | **`add(v)`, `addAll(vs)`** | — | — |
| **ReducingState** | `get()` (R-M-W) | `put(reduced)` | `clear()` | — *(see §a)* | — | — |
| **AggregatingState** | `get()` (R-M-W) | `put(acc)` | `clear()` | — *(see §a)* | — | — |
| range-scan (future) | — | — | — | — | — | `scan(lo, hi)` |

★ = critical for Nexmark 1.x. Reducing/Aggregating compose `GET → user-combiner-in-Java → PUT`.

### §a — Why Rust does not rescue MergeOperator for Reducing/Aggregating

Three properties intrinsic to the operator pattern, not the implementation language:

1. **Read amplification under unbounded merge chains.** A merge run `[op₁,…,opₙ]` materializes on every read by walking the chain. Even with partial-merge collapsing during compaction, worst-case reads under write skew or slow compaction pay O(N) combine cost. This is LSM mechanics.
2. **Combiner crosses a foreign-language boundary on the read path.** For Reducing/Aggregating the combiner is user JVM code (`ReduceFunction` / `AggregateFunction`). In any engine, that combiner runs on the JVM. Every merge-resolve becomes an FFM/JNI upcall under the storage critical path. Forst-RS has no way to avoid this — the upcall is on the read path.
3. **User combiners are not idempotent under partial-merge schedules.** RocksDB invokes the combiner at multiple sites (compaction, point read, range read). `ReduceFunction` is associative but **not** idempotent; users routinely write non-pure combiners. Embedding such code in the storage hook produced silent correctness regressions in the original Flink experience.

ListState is different on all three counts: combiner is fixed (concatenation), runs in the engine (no upcall), idempotent under any partial-merge schedule, merge-chain length bounded by add frequency. **APPEND_MERGE is safe for ListState; for Reducing/Aggregating it would re-import all three failure modes.**

### §b — Snapshot isolation cost + iterator lifetime bound

Snapshot isolation is not free:
- **Compaction GC blocked.** Any SST referenced by a live snapshot cannot be reclaimed post-compaction. LSM garbage ∝ (snapshot lifetime × write rate). In Forst-RS over S3 this becomes direct storage cost.
- **Write-buffer reclamation blocked.** Memtables pinned by a snapshot cannot be flushed/freed; manifests as RSS growth under heavy iteration.

**V1 hard rule — iterator lifetime bound:**
- Every `FrsIterHandle` is anchored to its StateRequest's per-slot Arena. Arena scope = one async-v2 turn (one batch in flight).
- Watchdog enforces `ITER_IDLE_TIMEOUT_MS = 30 s` (leak detection — time since last `next()`) and `ITER_MAX_LIFETIME_MS = 300 s` (abuse defense — absolute ceiling). Idle is the primary mechanism.
- Long-running scans **must** paginate via repeated `ITER_PREFIX` requests with continuation cursors — one engine snapshot per chunk, not one for the whole scan.
- `iter.snapshot_held_ms_{p95,p99}`, `idle_timeouts`, `max_lifetime_aborts` published per slot.

### §c — Metrics namespace (V1 mandatory)

All dispatch-layer metrics under `flink.state.forstrs.dispatch.<kind>.<metric>`; wired into the keyed-state-backend `MetricGroup`.

- `<kind>` ∈ `{get, put, delete, append_merge, iter_prefix, iter_range}`
- Per-kind: `count`, `rows`, `bytes_in`, `bytes_out`, `batch_size_{p50,p95,p99}`, `latency_ns_{p50,p95,p99}`, `ffi_errors` (labelled by code).
- Per-slot iterator group `flink.state.forstrs.iter.<metric>`: `handles_open` (gauge), `handles_leaked` (counter), `snapshot_held_ms_{p95,p99}`, `idle_timeouts`, `max_lifetime_aborts`.

Cardinality cap: **128 stateNames** per slot per kind. Above that, overflow into `<kind>.overflow.*` aggregated dimension. Worst-case series per slot ≤ `6 × 6 × 129 + 5 ≈ 4 650`.

### Semantic guarantees

| Property | Guarantee |
|---|---|
| **Intra-(state, key) ordering** | Issuance order preserved end-to-end. Two PUTs to the same (state, key) flush in call order. GET followed by PUT to the same (state, key) sees the pre-PUT value. APPEND_MERGE entries preserve insertion order. |
| **Cross-(state, key) ordering** | Free. Classifier may interleave operations across different states or different keys in whichever batching layout minimizes FFI hops. No ordering guarantee exposed to user code. |
| **GET** | Sees prior-batch writes; **does not** see intra-batch writes. Single-threaded per slot. Pure; replayed from upstream record. |
| **PUT** | Last-write-wins per (kg, key, uk); flushed atomically to memtable. Per-slot lock; visible to next turn only. Replayed via checkpoint; idempotent under same input. |
| **DELETE** | Tombstone PUT; same intra-batch rules as PUT. |
| **APPEND_MERGE** | Preserves per-key insertion order; flushed as contiguous merge run; reads concatenate older-first. Replayed via checkpoint; idempotent under input replay. |
| **ITER_PREFIX** | Snapshot at open; intra-slot writes after open are invisible. Handle invalid after Arena scope ends; no cross-slot, no cross-turn use. Iterator state never crosses a checkpoint — re-issued from upstream on restart. |
| **ITER_RANGE** | Same snapshot semantics; bounds `[lo, hi)`. |

**Failover invariant (V1):** no native handle outlives a single async-v2 StateRequest. Per-slot Arena drops on request completion or slot teardown; engine drops the iterator/transaction.

---

## Section 2 — Components

### Java side (`flink-state-backends/flink-statebackend-forst-rs/`)

| # | Component | Status | Responsibility |
|---|---|---|---|
| 1 | `VectorizedStateRequest` (sealed interface) | NEW | Typed envelope: `kind` + Arena-allocated payload columns + `CompletableFuture<Result>`. Subtypes: `GetRequest`, `PutRequest`, `DeleteRequest`, `AppendMergeRequest`, `IterPrefixRequest`, `IterRangeRequest`. |
| 2 | `VectorizedClassifier` | EXTEND (3→6 kinds) | Groups in-flight requests by `(kind, stateName)`; flushes when buffer fills or async-v2 turn ends. |
| 3 | `ColumnarBatchBuffer` | EXTEND | Arrow columnar buffers (`offsets[]` + flat data) in Arena memory; one per `(kind, stateName)` in flight. Adds merge-payload column + iter-cursor column. |
| 4 | `VectorizedExecutor` | EXTEND (3→6 dispatches) | Calls matching `frs_vec_*` FFI symbol on a filled batch; resolves request futures from the result column. |
| 5 | `FrsIterHandle` (`AutoCloseable`) | NEW | Wraps native iterator pointer + per-iterator Arena; tracks open time + last `next()` time; close on Arena drop or watchdog. |
| 6 | `IterLifetimeWatchdog` | NEW | Scheduled per backend instance; enforces idle + max-lifetime; sets `closeRequested` flag (operator-thread observes and closes). |
| 7 | `SlotArenaScope` | NEW | Per-slot long-lived `Arena.ofShared()` with bump-allocated `turnRegion` + LRU-allocated `cacheRegion`; per-turn overflow Arenas + per-iterator Arenas. |
| 8 | `DispatchMetrics` | NEW | Wires `flink.state.forstrs.dispatch.<kind>.*` + `flink.state.forstrs.iter.*` into Flink `MetricGroup` with 128-stateName cardinality cap. |
| 9 | `ForStRsValueState` | EXTEND | Construct `GetRequest` / `PutRequest` / `DeleteRequest`; off-heap value staging via `MemorySegmentDataOutputView` (SP6 6.1/6.2). |
| 10 | `ForStRsMapState` | EXTEND | Same refactor; SP6 6.3 staging behavior retained via `PutRequest` payload column. |
| 11 | `ForStRsListState` | EXTEND | Adds `AppendMergeRequest` for `add()`/`addAll()`; scalar get/update/clear routed via new requests. |
| 12 | `ForStRsReducingState` | NEW | `add()` = cache-mediated R-M-W (`ReducingAggregatingCache`); `get()`/`clear()` direct. Composes via dispatch layer; no native combiner. |
| 13 | `ForStRsAggregatingState` | NEW | Same composition pattern with `AggregateFunction`. Accumulator (de)serialization via `MemorySegmentDataOutputView`. |
| 14 | `ReducingAggregatingCache` | NEW | Per-state, per-slot LRU in `cacheRegion`; default 64 K entries; dirty entries flushed on eviction OR barrier; integrates with `pendingMisses` for convoy coalescing. |
| 15 | `PendingMissTable` | NEW | `ConcurrentHashMap<(stateId,key) → PendingMiss>`; coalesces concurrent cold-miss `add()`s into one GET + one combiner pass + one PUT. |
| 16 | `FatalErrorHandler` integration | EXTEND | `FrsEnginePanicError` → TaskExecutor termination (TM-level restart) on PANIC_CAUGHT or UNKNOWN. |
| 17 | `FrsAbi` + ABI version check | NEW | Compile-time `EXPECTED_ABI_VERSION`; checked at backend init via `frs_abi_version()` FFI; mismatch → `FrsAbiMismatchException` at startup. |

### Rust / FFI side (`crates/forst-rs-*/`)

| # | Component | Status | Responsibility |
|---|---|---|---|
| A | `frs_vec_get`, `frs_vec_put`, `frs_vec_del` | EXTEND | Already exist; tighten error envelope to standardized `FrsErrorCode` set. |
| B | `frs_vec_merge_append` | NEW | One FFI call appends N (key, value-suffix) rows as merge operands to the list-merge run for a single state. |
| C | `frs_vec_iter_prefix_open` / `_next` / `_close` | NEW | Chunked iteration; `_open` returns handle + first chunk + cursor; `_next` fills caller-owned chunk in per-iterator Arena. |
| D | `frs_vec_iter_range_open` / `_next` / `_close` | NEW | `[lo, hi)` bounds; otherwise same shape as (C). |
| E | `crates/forst-rs-storage/src/iter.rs` | NEW | Native iterator handle: snapshot-anchored, chunked-fill, watchdog-abortable. |
| F | `crates/forst-rs-engine/src/list_merge.rs` | NEW | Engine-internal list-append combiner (byte concatenation); runs at compaction + read materialization (hybrid). **No JVM upcall.** |
| G | `frs_abi_version` | NEW | Returns `FRS_ABI_VERSION: u32` constant; called once at Java backend init. |

### `SlotArenaScope` internal layout

```
slotArena = Arena.ofShared()                            // one-time at slot init (µs cost amortized)
├─ turnRegion  = slotArena.allocate(SLOT_TURN_BYTES,  64)   //  8 MiB, bump-allocated, per-turn reset
└─ cacheRegion = slotArena.allocate(SLOT_CACHE_BYTES, 64)   // 64 MiB, LRU-managed, slot lifetime

Independent on-demand Arenas (created/closed per their own lifetime):
├─ overflow Arenas   — created on turnRegion overflow, closed at SlotArenaScope.exit()
└─ iterator Arenas   — created per FrsIterHandle.open(), closed on close()
```

### Lifecycle pseudocode

```
enter(turn):
  assert iterRegistry.isEmpty()                  // contract — no handle outlives a turn
  if !iterRegistry.isEmpty():                    // defense-in-depth
    metric: arena.iter_leak_at_enter += N
    log.error("iter handle leaked across turn boundary", handles)
    foreach h in iterRegistry: h.forceClose()
    iterRegistry.clear()
  markOffset    = bumpOffset
  overflowList  = empty

allocate(nbytes, align) on turnRegion:
  if bumpOffset + nbytes <= SLOT_TURN_BYTES:
    return turnRegion.asSlice(align_up(bumpOffset, align), nbytes)
  else:
    overflowArena = Arena.ofShared()              // µs cost, only on overflow path
    overflowList.append(overflowArena)
    metric: arena.region_overflows += 1
    return overflowArena.allocate(nbytes, align)

exit(turn):
  foreach h in iterRegistry: h.close()            // engine snapshots dropped
  iterRegistry.clear()
  bumpOffset = markOffset                          // turnRegion reclaimed (no zero-fill)
  foreach a in overflowList: a.close()             // overflow released per-turn
```

`iterRegistry` is `ConcurrentHashMap<Long, FrsIterHandle>`; watchdog reads to find idle handles but never calls native close — it sets `AtomicBoolean closeRequested`, and the operator thread observes the flag at next interaction and performs the close (keeps all FFI calls on operator thread).

---

## Section 3 — Data flow

### Trace A — `MapState.put(uk, v)` (Q3 hot path)

```
1.  ForStRsMapState.put(uk, v)
2.    encodeKeyInto(turnRegion)               // kg | sk | '/' | stateName | '/' | uk
3.    encodeValueInto(turnRegion, v)          // via MemorySegmentDataOutputView (SP6 6.1)
4.    request = PutRequest{kind=PUT, state=mapState, keySlice, valueSlice, future}
5.    classifier.submit(request)
6.  [batch fills or turn ends]
7.  VectorizedExecutor.dispatch(batch)
8.    frs_vec_put(batchHeaderPtr)              // single FFI call for N rows
9.  Rust: forst-rs-engine writes to memtable
10. Rust: returns row-result column
11. request.future.complete(...)
```

**Assumptions for the published "sub-µs per put()" envelope:** steady state, value size ≤ 256 B (within SP6 INLINE_THRESHOLD), turnRegion not at overflow, batch ≥ 32 rows so FFI hop is fully amortized. Outside this regime: large values pay serialize cost ∝ size; overflow adds ~µs per Arena create; batches under 8 rows lose 70%+ amortization → per-put rises to ~1.5 µs.

### Trace B — `ListState.addAll(vs)` (APPEND_MERGE)

```
1.  ForStRsListState.addAll(vs)
2.    encodeKeyInto(turnRegion)
3.    foreach v in vs: encodeValueInto(turnRegion, v)
4.    request = AppendMergeRequest{...}
5.    classifier.submit(request)
6.  [flush]
7.  frs_vec_merge_append(batchHeaderPtr)
8.  Rust: list_merge.rs appends merge run in memtable; no JVM upcall
9.  Compaction-time folding collapses merge run; on-read concatenation handles unflushed (hybrid materialization).
```

### Trace C — `ReducingState.add(input)` — cache hit + convoy-coalesced cold-miss

**Hit path (steady state):**
```
entry = rmwCache.get(currentKey)               // O(1) in cacheRegion
if present:
  entry.acc = reduceFunction.combine(entry.acc, input)   // operator thread
  entry.dirty = true
  return
```

**LRU eviction:** dirty evictees → `PutRequest`s via classifier; await on next turn boundary.

**Miss path with convoy coalescing:**
```
pm = pendingMisses.computeIfAbsent((stateId, key), () -> {
       fut = classifier.submit(GetRequest{...}).future
       return new PendingMiss(fut, [])
     })
if pm was just created (first miss):
  pm.getFuture.thenAccept(priorBytes -> {
    prior = deserialize(priorBytes) or seed-from-first-input
    acc = prior
    foreach inp in pm.pendingInputs (arrival order):
      acc = reduceFunction.combine(acc, inp)   // associativity — preserve order
    rmwCache.put((stateId, key), {acc, dirty=true})
    pendingMisses.remove((stateId, key))
    completeAllChainedFutures(pm)
  })
else:
  pm.pendingInputs.append(input)               // join the convoy
return pm.completionToken
```

**Invariant:** at most one GET in flight per (stateId, key). N concurrent `add()`s on the same key fold into one GET + one combiner pass + one PUT.

### Trace D — `MapState.entries().iterator()` (ITER_PREFIX with chunking)

```
1.  ForStRsMapState.entries()
2.    encodePrefixInto(turnRegion)
3.    request = IterPrefixRequest{prefixSlice, chunkBufSlice, future<FrsIterHandle+chunk>}
4.    classifier.submit(request)
5.  [flush]
6.  frs_vec_iter_prefix_open(batchHeaderPtr)   // engine snapshot anchored, per-iter Arena
7.  Rust: iter.rs allocates chunk in per-iter Arena, fills it, returns handle + cursor
8.  Java: FrsIterHandle registered in iterRegistry (idle-time-stamped)
9.  Caller pulls rows from chunk; on drain → next(handle, newChunk, cursor)
       each next() updates handle.lastNextNs
10. Iterator exhausts OR turn ends OR idle > 30 s OR lifetime > 300 s
11. SlotArenaScope.exit() closes handle → frs_vec_iter_close → engine snapshot dropped
       per-iter Arena.close() → chunk memory freed
```

### Trace E — Checkpoint barrier (RMW + write-buffer drain)

Two-phase flush — `classifier.flushAll()` precedes every `await` to avoid deadlock between batches and barriers.

```
snapshotState(checkpointId, ts, factory, options):
  1. classifier.flushAll()                          // PHASE-1: kick all in-flight GET/PUT/etc batches
  2. await drainRmwInFlight():                      // GETs resolve → combiners run → PUTs enqueued
  3. classifier.flushAll()                          // PHASE-2: kick the PUTs just enqueued in step 2
  4. flushRmwCacheDirty():                          // dirty entries that never went through pending-miss
       foreach dirty entry: enqueue PutRequest
       classifier.flushAll(); await all PUT futures
  5. flushOpenWriteBuffers():                       // SP6 staged writes (MapState/ValueState)
       enqueue any pending PUTs from staging
       classifier.flushAll(); await all PUT futures
  6. engineSnapshot = forst-rs.checkpoint(...)
  7. return SnapshotResult
  8. forward barrier downstream
```

**Flush ordering between RMW and SP6 staged writes:** disjoint state types (RMW touches Reducing/Aggregating; SP6 touches Value/Map). Disjoint (state, key) namespaces → mutually safe. Convention: RMW first (smaller batch typically).

**Exactly-once invariant:** on step 6, every state mutation issued by every state op that returned before the barrier is in the engine. Nothing the cache held survives the barrier as unreplicated state.

---

## Section 4 — Error handling

### FFI error envelope

```rust
#[repr(C)]
pub struct FrsRowResult { code: u32, payload_off: u32, payload_len: u32 }

#[repr(u32)]
pub enum FrsErrorCode {
    OK                       = 0,
    NOT_FOUND                = 1,
    KEY_TOO_LARGE            = 100,
    VALUE_TOO_LARGE          = 101,
    BATCH_HEADER_MALFORMED   = 110,
    ITER_EXPIRED             = 200,
    ITER_CURSOR_INVALID      = 201,
    ENGINE_IO                = 300,
    ENGINE_CORRUPTED         = 301,
    ENGINE_OOM               = 302,
    ENGINE_DISK_FULL         = 303,
    PANIC_CAUGHT             = 900,
    UNKNOWN                  = 999,
}
```

### Per-request failure classes

| Class | Codes | Scope | Outcome |
|---|---|---|---|
| Fail-row | NOT_FOUND, KEY_TOO_LARGE, VALUE_TOO_LARGE, ITER_EXPIRED, ITER_CURSOR_INVALID | One row | `request.future` completes exceptionally; other rows resolve normally; engine state unchanged for the bad row. |
| Fail-batch | BATCH_HEADER_MALFORMED, ENGINE_IO, ENGINE_OOM, ENGINE_DISK_FULL, ENGINE_CORRUPTED | Whole batch | Every `request.future` completes exceptionally; treat as if nothing in the batch happened — recovery via Flink restart. |
| Fail-process | PANIC_CAUGHT, UNKNOWN | Engine process | `FatalErrorHandler.onFatalError(...)` → TaskExecutor exits; JM reschedules operator on a different/fresh TM. |

**No in-process retry** for fail-batch — Flink's restart-from-checkpoint is the exactly-once recovery mechanism; sneaking in-process retries underneath would risk double-apply.

### `PANIC_CAUGHT` → TM-level restart (not region restart)

`std::panic::catch_unwind` only protects the FFI boundary; engine internal data structures may sit in an arbitrary intermediate state when the panic unwound through their methods. Drop impls run during unwind but cannot restore invariants the panicking method was halfway through establishing. **The engine after a caught panic is not safe to reuse in this process.**

| Step | Action |
|---|---|
| 1 | Java receives PANIC_CAUGHT |
| 2 | Increment `engine.panic_caught` |
| 3 | Call `FatalErrorHandler.onFatalError(new FrsEnginePanicError(...))` |
| 4 | TaskExecutor exits |
| 5 | JobManager reschedules on a different/fresh TM |
| 6 | New engine reads from last successful checkpoint |

Engine-side **panic safety invariant:** public methods do not hold critical invariants across panic-unwind sites. We write defensive code, but DO NOT rely on it for correctness — the process termination is the recovery. Validated by `proptest` (§5 medium-1).

### `ITER_EXPIRED` user-visible semantics

| Layer | Semantics |
|---|---|
| FFI | Per-row code; subsequent `next()` on same handle also return ITER_EXPIRED |
| Java wrapper | `FrsIteratorExpiredException extends FrsException` thrown on consumer `next()`/`hasNext()` |
| Dispatch | **No auto-reopen** — a new iterator would see a different snapshot (silent correctness violation), the consumer has already advanced past rows, and silent reopen hides leaks |
| Operator | Uncaught → operator failure → Flink restart → fresh iterator on the new attempt |

The 30s/300s timeouts are sized so ITER_EXPIRED **never fires under normal workloads**. If it fires in production, treat as a leak signal — investigate cause, do not raise the timeout.

### `DISPATCH_HANG_MS` derivation

```
DISPATCH_HANG_MS = min(30_000, 0.5 × execution.checkpointing.timeout_ms)
```

Read at backend init; logged; exposed as gauge `dispatch.hang_threshold_ms`. The `min` keeps the upper bound at 30 s for typical deployments; the half-of-checkpoint guard handles deployments with short checkpoint timeouts.

### Cache & in-flight failure recovery

| Failure point | Recovery |
|---|---|
| Cold-miss GET fails (fail-batch like ENGINE_IO) | Propagates as FrsException on pending-miss continuation; chained `add()` futures fail; operator fails → Flink restart |
| Combiner throws during pending-miss fold | Catch → fail all chained futures with original exception; pending entry cleared; cache NOT written; operator fails next turn → Flink restart from prior checkpoint (matches existing Flink contract for user-code throws) |
| LRU eviction PUT fails | Operator fails; cache state lost; Flink restart re-derives via cold-miss path |
| Checkpoint flush fails | `snapshotState` throws → Flink aborts checkpoint; next attempt retries |
| RMW continuation hangs | DISPATCH_HANG_MS detector → operator failure → avoid unbounded checkpoint stall |

Critical invariant: **cache is derived state**. Any failure that drops the cache is recoverable by re-deriving from engine state plus input replay.

### Idempotency under replay

| Op | Idempotent? | Why |
|---|---|---|
| MapState.put, ValueState.update | Yes | LWW deterministic given input-replay determinism |
| MapState.remove, clear, ValueState.clear | Yes | Tombstones absorb |
| ListState.add, addAll | Not under arbitrary replay; **exactly-once via snapshot atomic boundary + deterministic replay**: snapshot captures all `add()`s issued before barrier (Trace E drain); replay starts exactly after barrier; each input contributes one `add()` exactly once across operator lifetime |
| ReducingState.add, AggregatingState.add | Yes via cache-drain | Combiner is associative not idempotent; drain-before-barrier (Trace E) ensures replay starts from a consistent point |

---

## Section 5 — Testing strategy (coverage matrix)

### Test taxonomy

| Level | Scope | Where |
|---|---|---|
| L1 — Rust unit | One fn / struct invariant | `crates/forst-rs-*/src/**/tests.rs` |
| L2 — Java unit | One class | `flink-statebackend-forst-rs/src/test/.../<Class>Test.java` |
| L3 — FFI round-trip | Java → FFI → Rust → Java byte parity | `.../ffm/*ParityTest.java` |
| L4 — State-class integration | One state primitive vs real engine | `.../state/ForStRs<Type>StateIntegrationTest.java` |
| L5 — Fault injection | Error codes, watchdog, panic, barrier failure, concurrency races | `.../faultinjection/*Test.java` + `crates/forst-rs-test-harness/` |
| L6 — Nexmark perf | End-to-end cluster + S3, Q0–Q8 | `~/Downloads/workenv/flink-2.2.1/bin/run-nexmark-matrix.sh` |
| L7 — Soak | 24 h (PR gate) / 72 h (release gate) | Test cluster, Q3+Q5+Q8 continuous, fault injection PROB=0.001 |

### Coverage matrix (feature × level)

| Feature | L1 | L2 | L3 | L4 | L5 | L6 |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| GET dispatch | ✓ | ✓ | ✓ | ✓ | F1 | Q3 |
| PUT dispatch | ✓ | ✓ | ✓ | ✓ | F1 | implied |
| DELETE dispatch | ✓ | ✓ | ✓ | ✓ | F1 | implied |
| APPEND_MERGE | ✓ | ✓ | ✓ | ✓ | merge-run truncation | Q5 |
| ITER_PREFIX | ✓ | ✓ | ✓ | ✓ | F2 | Q5/Q7 |
| ITER_RANGE | ✓ | ✓ | ✓ | ✓ (future) | F2 | (not V1) |
| ValueState refactor | — | ✓ | ✓ | ✓ | ✓ | Q0/Q1 |
| MapState refactor + SP6 6.3 | — | ✓ | ✓ | ✓ | ✓ | Q3 |
| ListState + APPEND_MERGE | — | ✓ | ✓ | ✓ | ✓ | Q5 |
| ReducingState (new) | — | ✓ | ✓ | ✓ | F5 convoy throw | Q4/Q6 |
| AggregatingState (new) | — | ✓ | ✓ | ✓ | ✓ | Q8 |
| SlotArenaScope enter/exit | — | ✓ | — | ✓ | F7 overflow, leak-at-enter | — |
| Per-iterator Arena | — | ✓ | ✓ | ✓ | close-on-exit | — |
| RmwCache hit | — | ✓ | — | ✓ | — | Q4 |
| RmwCache miss + convoy | — | ✓ | — | ✓ | F11 | — |
| Barrier drain (Trace E) | — | ✓ | — | ✓ | F6, F9 | — |
| Fail-row codes | ✓ | ✓ | ✓ | ✓ | F1 all | — |
| Fail-batch codes | ✓ | ✓ | ✓ | — | F1 all | — |
| PANIC_CAUGHT → TM restart | ✓ | ✓ | — | — | F3 | — |
| ITER_EXPIRED no auto-reopen | — | ✓ | — | ✓ | F2 | — |
| DISPATCH_HANG_MS | — | ✓ | — | — | F4 | — |
| Watchdog-vs-operator race | — | ✓ | — | — | **F10** | — |
| ABI version check | ✓ | ✓ | ✓ | — | mismatch test | — |
| Snapshot round-trip (V1 only) | — | — | — | ✓ | partial-restore | — |
| Replay idempotency table | — | — | — | ✓ per-state | replay-after-barrier | — |
| Metrics published | — | ✓ | — | ✓ | — | — |
| Cardinality cap (128) | — | ✓ | — | — | overflow > 128 | — |

### Fault injection (L5)

`crates/forst-rs-test-harness` exposes `FaultInjector` via env vars:
- Probability: `FRS_FAULT_<KIND>_PROB=<0.0..1.0>` (soak, fuzz)
- Deterministic: `FRS_FAULT_<KIND>_AT=<call_index>` (reproducible L5 unit tests)
- Replay: `FRS_FAULT_SEED=<u64>`

| ID | Scenario | Validates |
|---|---|---|
| F1 | FFI return code substitution | Fail-row + fail-batch paths |
| F2 | Watchdog force-drop iterator | ITER_EXPIRED propagation |
| F3 | Engine panic injection | PANIC_CAUGHT → TM restart |
| F4 | Slow FFI return | DISPATCH_HANG_MS detector |
| F5 | Combiner throw at index k | Convoy fold abort |
| F6 | Barrier-time flush failure (PUT batch → ENGINE_IO) | Checkpoint abort, no partial state |
| F7 | turnRegion overflow | Overflow Arena path, per-turn release |
| F8 | Cache eviction with engine error | Operator failure, no silent state loss |
| **F9** | Barrier-while-iterator-active | Iterator close + barrier drain compatibility (no deadlock) |
| **F10** | Watchdog-vs-operator close race | Atomic flag observation, single native close |
| **F11** | Same-key convoy pressure (N≥64 add()s with delayed GET via F4) | pendingMisses coalescing, fold-in-order, all futures resolve once |
| **F12** | Eviction during checkpoint flush | Eviction PUTs reach engine before snapshot capture |

F9–F12 are **V1-mandatory**.

### Rust panic-safety proptests (L1, V1 mandatory)

| Test | Method | Injection points |
|---|---|---|
| `prop_memtable_insert_panic_safe` | `Memtable::insert` | After key-encode, value-encode, index update, WAL append |
| `prop_snapshot_create_panic_safe` | `Engine::snapshot` | Before/after each SST-pin step |
| `prop_iter_open_panic_safe` | `Iter::open` | Before/after snapshot anchor, chunk-buffer alloc |
| `prop_compaction_panic_safe` | `Compactor::run_one` | Mid-merge-run, after partial SST write |

Post-panic invariants asserted: no FD leak; memtable indices consistent; snapshot refcounts balanced; per-iterator Arenas closed.

### Tiered L6 perf gates

| Tier | Queries | Gate (vs RocksDB local JDK 17) | Rationale |
|---|---|---|---|
| **state-heavy** | Q3 (join), Q4 (per-category avg), Q5 (window count), Q8 (window-join) | **≥ 1.20×** | Vectorization payoff direct |
| **state-medium** | Q6 (per-seller avg), Q7 (highest bid per window) | **≥ 1.05×** | Modest improvement; ensures no regression |
| **state-light** | Q0 (passthrough), Q1 (currency), Q2 (filter) | **≥ 0.95× regression guard** | State path incidental |

Q3's 1.25× already met; locked as floor for that query.

**Gates are commitments after measurement, not pre-measurement aspirations.** The ≥ 1.20× state-heavy gate is the design's expected outcome based on Q3's validated 1.25×, but it is not a deduced lower bound for Q4/Q5/Q8 — they touch state differently. **On the first daily L6 miss for any state-heavy query, the response is:**
1. Open an investigation issue (`perf-gate-miss` label) within 24 h.
2. Identify the responsible delta (profile, bisect, attribute to a component or workload property).
3. **Either** fix the regression and re-validate, **or** re-tier the query with a documented rationale (e.g. "Q5's ListState concatenation cost is workload-bounded; re-tier to state-medium with ≥ 1.05× gate").
4. Update this spec's tier table in the same PR.

What is **not** acceptable: silently lowering the gate without investigation. The discipline is "measure, then commit" — gate changes go through review like any spec change.

### Cadence

| Cadence | Scope | Trigger |
|---|---|---|
| Daily 03:00 UTC | Q3, Q4, Q5, Q8 | Cron |
| Weekly Sun 03:00 UTC | Full Q0–Q8 all backends | Cron |
| Bisection | git bisect last N commits, 4h cap | Triggered by > 5% deviation |

### L7 soak

| Property | Value |
|---|---|
| Duration | 24 h (PR gate) / 72 h (release gate) |
| Workload | Q3 + Q5 + Q8 continuous, parallel |
| Fault injection | F1–F12 each at PROB=0.001 |
| Acceptance | RSS stable ± 5 %; `iter.handles_open` ≤ 2× steady; `arena.region_overflows`/h bounded; final-state equality vs RocksDB control; checkpoint failure rate < 0.5 % |

### Migration & Rollback Strategy (V1 contract)

**No-ForSt-forward:** Forst-RS cannot read on-disk state written by community ForSt. Deploying onto an existing ForSt-backed job requires:
1. Drain the job to a savepoint (Flink-level, backend-agnostic).
2. Redeploy onto Forst-RS.
3. Restore from the savepoint.

OR start from scratch (acceptable only for bounded-state jobs).

**No-ForSt-backward:** Once on Forst-RS with ≥1 checkpoint, rolling back to ForSt requires restoring from a savepoint taken **before** the cut.

**Within-Forst-RS rollback:** Standard Flink checkpoint rollback — supported.

**Required functional gate (L4):**
- Forst-RS → Forst-RS round-trip: incremental + full + savepoint export/import. **100 % pass required**, zero flake tolerance.
- Cross-version Forst-RS round-trip (V1 → V1.x → V1): savepoint compatibility tested.
- Forst-RS ↔ ForSt savepoint exchange: **not in V1 scope**.

Release-note language (operator-facing): *"Forst-RS introduces a new on-disk format. Upgrading from ForSt to Forst-RS is a one-way drain-and-restore via savepoint; in-place upgrade is not supported."*

---

## Section 6 — Productization runbook

### 6.1 Deployment checklist

Pre-flight:
```
[ ] Flink version ≥ 2.2.1
[ ] JDK ≥ 25 on every TM (env.java.home pinned)
[ ] libforst_rs_ffi.{dylib,so} present in lib/, ABI version matches expected
[ ] JVM flags on TM:
    --enable-native-access=ALL-UNNAMED
    --add-modules jdk.incubator.vector
    -XX:+UseZGC -XX:+UseCompactObjectHeaders
[ ] S3 plugin = flink-s3-fs-presto (NOT hadoop on JDK 25)
[ ] table.exec.mini-batch.enabled = false
[ ] table.exec.async-state.enabled = true
[ ] state.backend.type = org.apache.flink.state.forstrs.ForStRsStateBackendFactory
[ ] Source backend (if migrating): savepoint taken and verified restorable
[ ] Runbook bookmarked
```

Post-deploy verification:
```
[ ] First checkpoint completes within checkpoint.timeout
[ ] flink.state.forstrs.engine.panic_caught == 0
[ ] flink.state.forstrs.iter.handles_open ≤ slot_count × 8
[ ] flink.state.forstrs.dispatch.*.ffi_errors == 0
[ ] flink.state.forstrs.cardinality_capped == 0
[ ] No FrsException in JM/TM logs
[ ] frs_abi_version match logged at backend init
```

### 6.2 Canary plan (short-window observables only)

**Preconditions — must all hold before any canary TM accepts production traffic:**

```
[ ] Pre-deploy L7 24 h soak passed on a separate test cluster (RSS stable, no leak, no silent state loss)
[ ] All L5 fault injection tests (F1-F12) pass deterministically and probabilistically
[ ] frs_abi_version match verified on every candidate TM
[ ] Pre-deploy L6 daily-cadence baseline established (rolling 7-day median per query)
[ ] §6.13 release readiness checklist signed off
```

If any precondition fails, **do not enter canary** — fix the failing gate first. Stability validation is not run in parallel with production traffic.

After preconditions are met, canary stages use only what can be measured in their hold window:

| Stage | Hold | Promotion criteria |
|---|---|---|
| **Canary 1 TM** | 1 h | (a) `engine.panic_caught == 0`; (b) zero `FrsException`; (c) `dispatch.<kind>.latency_ns_p99` within 1.5× pre-cut baseline; (d) ≥ 2 successful checkpoints; (e) RSS within +10 % of warm-up |
| **10 %** | 4 h | All + (f) `iter.handles_open` slot-mean linear-fit slope ≈ 0 over 4 h; (g) `arena.region_overflows/h` < 10; (h) checkpoint failure rate < 0.5 % |
| **50 %** | 24 h | All + (i) checkpoint failure trend stable; (j) no canary-tier metric fires on the broader fleet |
| **100 %** | — | — |

### 6.3 In-band rollback (drain succeeds)

```
1. flink stop --savepoint-path s3://.../emergency-savepoint <job-id>
2. Update job config: state.backend.type = rocksdb (or forst)
3. flink run -s s3://.../emergency-savepoint <job-jar>
```

Budget: dominated by drain time (≤ `checkpoint.timeout`). ~10 min for typical Nexmark-scale state.

**No in-process fallback** — engines are exclusive. The savepoint-drain procedure IS the kill-switch.

### 6.4 Emergency rollback when drain fails

```
EMERGENCY: drain failed or cluster unreachable

1. flink cancel <job-id>          # abort WITHOUT drain; do not wait

2. Identify last successful Forst-RS checkpoint:
     - JM logs: "Completed checkpoint <N> for job <id>"
     - OR s3://<bucket>/<prefix>/forst-rs-checkpoints-<run-id>/

3. Pick rollback target:
     (a) Restart on Forst-RS from last successful Forst-RS checkpoint
     (b) Restart on RocksDB/ForSt from PRE-CUT savepoint (if one exists)
     (c) Restart with empty state — bounded-state jobs only

4. TRADE-OFF — EXPLICITLY ACCEPTED:
     - (a) and (b) lose all input between last checkpoint and the failure.
     - AT-MOST-ONCE for that window. There is NO exactly-once option when drain fails.
     - Drain IS the exactly-once mechanism.

5. flink run -s <chosen-path> <job-jar>

6. Backfill if exactly-once is required for the lost window:
     - Replay missing input from upstream
     - OR one-shot reprocessing job
     - OR accept loss (explicit business decision)
```

### 6.5 Metrics interpretation (single-metric cheat sheet)

| Metric | Healthy | Alert | Action |
|---|---|---|---|
| `engine.panic_caught` | 0 | Any | TM auto-restarts; check logs for panic message; file P0 |
| `iter.handles_open` (per slot) | ≤ 8 | > 32 | Iterator leak — check `iter.idle_timeouts`/`handles_leaked` |
| `iter.handles_leaked` | 0/h | Any | Operator holding iterator beyond turn — fix operator |
| `iter.snapshot_held_ms_p99` | < 1000 | > 5000 | Long snapshots → compaction garbage → S3 growth |
| `dispatch.<kind>.ffi_errors` | 0 | Any | Triage by code: KEY/VALUE_TOO_LARGE = data; ENGINE_IO = storage flake; DISK_FULL = capacity |
| `dispatch.<kind>.latency_ns_p99` | < 1 ms | > 10 ms | Check ZGC pauses, S3 latency, cache hit |
| `dispatch.put.batch_size_p50` | ≥ 32 | < 8 | Vectorization not amortizing — verify async-state config |
| `arena.region_overflows/h` | < 10 | > 100 | Raise `SLOT_TURN_BYTES`; investigate giant requests |
| `cardinality_capped` | 0 | Any | > 128 stateNames/slot — raise cap or split job |
| `iter.max_lifetime_aborts` | 0 | Any | Pathological consumer holding > 300 s — investigate |
| Checkpoint failure rate | < 0.5 % | > 2 % | Check PUT errors during snapshot, disk_full, S3 throttling |

### 6.6 Combined-symptom diagnosis

| Symptoms (AND) | Diagnosis | Action |
|---|---|---|
| High `dispatch.put.latency_ns_p99` + low `dispatch.put.batch_size_p50` | Batching not amortizing | Verify `mini-batch.enabled=false`, `async-state.enabled=true` |
| High `iter.snapshot_held_ms_p99` + rising S3 bucket size | Snapshots blocking compaction GC | Investigate iterator-holding operator |
| Rising `iter.handles_open` + checkpoint timeout fires | Iterator leak blocks barrier drain | Restart operator; fix in code |
| `engine.panic_caught` > 0 only during deploy + recovers after | ABI mismatch during rolling deploy | V1 hard-checks at startup; confirm dylib version |
| `dispatch.cardinality_capped` > 0 + many small stateNames | Job with many dimensions | Raise cap or refactor |
| `iter.max_lifetime_aborts` > 0 + zero `iter.idle_timeouts` | Tight-loop consumer over 300 s | Pathological; fix code |
| Rising RSS + stable `iter.handles_open` + stable `arena.region_overflows` | Cache region unbounded (potential bug) | Confirm `rmw-cache.bytes` (V1.1) |
| `dispatch.put.ffi_errors` rising during snapshot + `disk_full=0` | Checkpoint-path PUT failing | S3 throttling / transient IO |

### 6.7 Incident runbook (selected scenarios)

**TM keeps restarting with PANIC_CAUGHT** — Check TM logs for panic message; file bug with repro; mitigation: isolate offending operator via savepoint drain.

**`iter.handles_leaked` > 0** — Operator code holding iterators outside `try-with-resources` or beyond one `processElement`. Watchdog force-closed; structural fix in operator.

**Checkpoint timeout fires repeatedly** — Check `dispatch.latency_ns_p99`, `arena.region_overflows`, `iter.snapshot_held_ms_p99`. Raise `DISPATCH_HANG_MS` only if `checkpoint.timeout` was raised.

**`engine.panic_caught` rises during deploy, recovers after** — ABI mismatch on rolling deploy. V1 hard-checks ABI at startup so this should be replaced by `FrsAbiMismatchException` at startup; if you see panics instead, ABI check is missing or disabled.

### 6.8 Escalation matrix

| Level | When | Contact |
|---|---|---|
| L1 — oncall SRE | Initial response; runbook | (deployment fills in) |
| L2 — state-backend team | Runbook unresolved 30 min | Pager: `forst-rs-oncall` |
| L3 — engine maintainer | Suspected engine bug | `<repo>/issues` label `engine-bug` |
| L4 — PMC | Public-facing or data-correctness | PMC mailing list / Slack `#flink-state` |

### 6.9 Known limitations (V1)

| Limitation | Workaround |
|---|---|
| No ForSt-forward (cannot read existing ForSt state) | Drain to savepoint, redeploy on Forst-RS |
| No in-place rollback to ForSt | Drain to pre-cut savepoint |
| 128-stateName cardinality cap per slot | Refactor job or raise cap |
| ITER_RANGE in V1 API but no production query uses it | Use ITER_PREFIX where possible |
| RMW cache fixed 64 MiB per slot | Configurable in V1.1 |
| Reducing/Aggregating no native merge — RMW only | By design (§1 §a) |

### 6.10 Deferred items (V1.x and later)

| Source | Item | Plan |
|---|---|---|
| §2 Medium-1 | RMW cache memory budget tunable | V1.1: `state.backend.forstrs.rmw-cache.bytes` |
| §2 Medium-2 | Cardinality cap default raise to 128 with WARN | V1.1: raise + WARN |
| §1 V1.x | GET split: SAME_KEY vs CROSS_KEY | V1.1: classifier-internal |
| §1 V1.x | DELETE split: POINT vs PREFIX | V1.1: classifier-internal |
| §1 V1.x | ITER_RANGE production wire-up | V1.2: surface in MapState `subMap(lo, hi)` |
| §5 | Cross-backend savepoint ForSt ↔ Forst-RS | V2: explicit non-goal in V1 |
| §5 | Performance bisection automation | V1.1: CI infra |

### 6.11 Explicit non-goals (V1)

- **Not a goal:** ForSt ↔ Forst-RS on-disk format compatibility — greenfield by design (§1).
- **Not a goal:** Running Forst-RS without JDK 25 — FFM + ZGC + CompactObjectHeaders are V1 hard requirements.
- **Not a goal:** In-process fallback to RocksDB — rollback is via savepoint (§6.3, §6.4).
- **Not a goal:** Multi-master writes — Flink contract: single writer per keyed-state partition.
- **Not a goal:** Reducing/Aggregating via native MergeOperator — principled non-goal (§1 §a).
- **Not a goal:** Pre-V1 Forst-RS checkpoint backward compatibility — V1 is the first stable format.
- **Not a goal:** Cross-language combiners — combiners always run on the JVM operator thread.

### 6.12 Runbook maintenance

This runbook is versioned alongside the Forst-RS codebase.

- Every behavior change affecting operator visibility (new metric, changed threshold, new failure mode) updates the relevant runbook section **in the same PR**. PRs without runbook updates require sign-off from the state-backend team lead.
- Quarterly review: state-backend team verifies metrics still emit and thresholds still reflect production reality.
- Cross-reference from every minor-version release-note entry.
- If observed behavior contradicts this runbook, file an issue with `runbook-stale`.

### 6.13 V1 release readiness checklist

"V1 is ready to ship" is a verifiable claim, not a judgment. All of the following must hold before tagging V1:

**Code completeness:**
```
[ ] All 17 Java components (§2 1-17) implemented; PR-merged; no `TODO`/`FIXME`/`unimplemented!` in V1 code paths
[ ] All 7 Rust components (§2 A-G) implemented; cargo build clean; no `panic!()` outside intentional sites
[ ] FRS_ABI_VERSION = 1 locked; bumped on every layout change during development; matches Java EXPECTED_ABI_VERSION
```

**Test coverage:**
```
[ ] L1 Rust unit: all panic-safety proptests passing
[ ] L2 Java unit: every component in §2 has a unit test, ≥ 80 % line coverage
[ ] L3 FFI round-trip: byte-level parity for every frs_vec_* symbol
[ ] L4 state-class integration: each state primitive's coverage matrix row green
[ ] L5 fault injection: F1-F12 each pass deterministically (_AT mode) and probabilistically (_PROB mode)
[ ] L6 daily cadence: rolling 7-day median per query within published tier gate
[ ] L7 24 h PR-gate soak: passed at least once on a release candidate
[ ] L7 72 h release-gate soak: passed on this candidate (RSS stable ± 5 %, no silent state loss)
```

**Operational readiness:**
```
[ ] Runbook (§6) reviewed against current code; quarterly review most recent within 30 days
[ ] Metrics published and visible on dashboard
[ ] Escalation matrix (§6.8) populated for the target deployment
[ ] Release notes drafted with "no in-place upgrade from ForSt" callout
[ ] Migration validation: at least one production-shape job successfully drained-and-restored Forst-RS → Forst-RS
```

**Documentation:**
```
[ ] This spec's V1.x deferred-items list (§6.10) up to date
[ ] Non-goals list (§6.11) reviewed against current product positioning
[ ] SP1-SP6 cross-references (Appendix) version-locked to V1 (see "Documentation maintenance discipline" in Appendix)
```

Sign-off: state-backend team lead + PMC representative. The checklist is the artifact, not the meeting.

### 6.14 Documented trade-off: `frs_get_fast` panic = TM death

`frs_get_fast` deliberately omits `catch_unwind`:
- Saves ~15 ns per call on the per-key lookup hot path (Q3 critical).
- A panic here indicates a logic bug — same conditions would manifest as PANIC_CAUGHT under the catching path and trigger TM-level restart anyway. Same end state, lower normal-path cost.
- Net behavior: panic → unwind crosses FFI → undefined behavior → JVM crash → TM termination → Flink reschedules.

Published as expected behavior, not a bug.

---

## Appendix — Constants (V1 defaults)

| Constant | Default | Tunable in |
|---|---|---|
| `SLOT_TURN_BYTES` | 8 MiB | V1 backend config |
| `SLOT_CACHE_BYTES` | 64 MiB | V1.1 |
| `MAX_RMW_CACHE_ENTRIES` (per state per slot) | 64 K | V1.1 |
| `ITER_IDLE_TIMEOUT_MS` | 30 000 | V1 backend config |
| `ITER_MAX_LIFETIME_MS` | 300 000 | V1 backend config |
| `DISPATCH_HANG_MS` | `min(30 000, 0.5 × checkpoint.timeout_ms)` | derived |
| `MAX_DISPATCH_METRIC_STATES` | 128 | V1.1 |
| `FRS_ABI_VERSION` | 1 | code constant; bumped on layout change |

---

## Appendix — Pre-implementation validation gate

Before writing V1 implementation code, run a **component-boundary microbench** to validate the design's performance claims. The 17 Java components on the hot path are tightly coupled by the dispatch and Arena lifecycle; the published "sub-µs per `put()`" envelope (§3 Trace A) is a design target, not a measurement. **Microbench it before the design is locked into implementation.**

**Schedule:** 6 weeks before V1 implementation start.

**Microbench targets (per-component, isolated, JMH-style):**

| Component / boundary | Measured op | Target envelope | If unmet |
|---|---|---|---|
| `SlotArenaScope.enter()/exit()` | empty turn round-trip | ≤ 200 ns | Consolidate enter/exit into a single MemoryHandle savepoint, drop iterator-registry copy |
| `turnRegion.allocate()` (bump alloc, in-region) | one allocation, 256 B aligned | ≤ 50 ns | Drop align step for power-of-2 default; verify with `AlignmentFinder` |
| `encodeKeyInto(turnRegion)` (Trace A step 2) | full composite key encode | ≤ 100 ns | Inline `serializeK` writeback; cache key prefix per slot |
| `encodeValueInto` for ≤ 256 B value | full encode | ≤ 150 ns | Bypass `MemorySegmentDataOutputView` for small fixed-size types |
| `VectorizedClassifier.submit()` (in-batch) | append one request, no flush | ≤ 100 ns | Pre-size batch buffer per-state to avoid resize-and-copy |
| `VectorizedExecutor.dispatch()` (Java→FFI→Java) | one batch of 64 rows | ≤ 5 µs ÷ 64 = ~80 ns/row | Reduce per-batch FFI overhead (header copy, return-column parse) |
| `ColumnarBatchBuffer` flush serialization | 64-row PUT batch into FFI-ready layout | ≤ 2 µs total | Consider direct off-heap layout (no separate serialization step) |
| `pendingMisses.computeIfAbsent()` | cache-miss path lookup | ≤ 50 ns hit / ≤ 200 ns miss | Replace ConcurrentHashMap with open-addressed slot-local table |

**Acceptance:** sum of measured per-row costs along Trace A ≤ **1 µs** at p99 for 256 B values in 32-row batches. If the sum exceeds the target, **consolidate components during V1 implementation, not after release.** Candidate consolidations:

1. **Merge** `VectorizedStateRequest` envelope into `ColumnarBatchBuffer` directly — skip the per-request object allocation, write straight into the batch buffer.
2. **Inline** `encodeKeyInto` + `encodeValueInto` at the call site for primitive-typed state.
3. **Single-pass classifier** that writes batch + dispatches inline when buffer fills (no separate flush step).

**Deliverable:** `docs/superpowers/specs/2026-04-04-forst-rs-component-microbench-report.md` (or equivalent dated path), with raw JMH numbers and recommended consolidations (if any).

**Block on failure:** if the microbench shows the sum-of-components exceeds 2× target, the design enters revision before V1 implementation begins. The current 17-component decomposition is not load-bearing — clarity-vs-perf can rebalance if the data demands.

## Appendix — Cross-references to companion specs

- **SP1** (state types V2): per-state-type V2 contracts referenced from §2 components 9–13.
- **SP2** (iterator vectorization): foundation for §1 ITER_PREFIX/ITER_RANGE; this spec adds lifetime bound + per-iter Arena.
- **SP5** (snapshot/restore + data transfer): underpins §3 Trace E + §5 migration strategy.
- **SP6** (V1 sync vectorization): `MemorySegmentDataOutputView`/`InputView` primitives reused for §2 off-heap value staging.

### Cross-reference maintenance discipline (version-locked)

This umbrella spec is **load-bearing** for SP1–SP6 — those specs depend on contracts defined here. To prevent silent drift:

1. **Bidirectional citation:** every section of SP1–SP6 that touches a contract defined here cites this spec's section number (e.g. "per umbrella §1 §a — APPEND_MERGE non-goal for Reducing/Aggregating"). Conversely, this spec's components (§2 9–13) cite the SP that owns the implementation.
2. **PR-level sync gate:** any PR that reorganizes a section, renames a constant, or changes a contract semantic in SP1–SP6 **must** update the corresponding citation here in the same PR. The reviewer of the SP change is responsible for verifying.
3. **Quarterly review:** state-backend team performs a quarterly cross-reference walk — every citation in this spec resolves to an existing section in SP1–SP6 with the cited semantics; every back-citation from SP1–SP6 resolves to an existing section here. Stale references file `runbook-stale`-labelled issues.
4. **Version lock:** this spec is tagged at V1 release. Subsequent material changes bump to V1.1, V1.2, etc. SP1–SP6 cite the version they were validated against; cross-version drift triggers re-validation.

Without this discipline, future reviewers will repeatedly raise "doesn't this contradict SP-X?" against statements here that have silently fallen out of sync. The discipline is cheap (PR-time check) and prevents an expensive cleanup later.

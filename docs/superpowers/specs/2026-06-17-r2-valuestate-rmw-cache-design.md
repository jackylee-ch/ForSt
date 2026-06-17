# R-2: batched windowed-RMW via a ValueState write-back cache — inspection, model, design

**PMC-1 — 2026-06-17.** This extends the q11 read-path design
(`2026-06-17-q11-windowed-rmw-readpath-design.md` §R-2). It is the *Flink-side*
backend change: route the per-record synchronous get→fold→put of the windowed /
group aggregation onto a batched, amortized accumulator write-back cache. Profile-
/inspection-first, then design, then implement. Correctness is exact (identical
aggregation output) and the path stays batch-only / zero-copy.

---

## 0. The inspection model (WHERE the per-record sync RMW actually is)

The umbrella audit (`2026-06-01-v1-v2-batch-execution-audit.md`) named "V1
Reducing/Aggregating = per-record sync RMW; needs an accumulator write-back cache,
like the ValueState statebuf." Tracing the **actual NexMark q11/q17 operators**
through Flink SQL changes the target:

### q17 = UNBOUNDED group aggregation → `ValueState<RowData>`
`q17.sql` is `SELECT auction, day, count(*), count(*) filter…, min/max/avg/sum
GROUP BY auction, DATE_FORMAT(dateTime)`. There is **no window** — it is an
unbounded group aggregation. Flink SQL compiles this to
`AsyncStateGroupAggFunction` (the V2 async-state group-agg), which stores the
accumulator as:

```java
private transient ValueState<RowData> accState;       // AsyncStateGroupAggFunction:44
...
processElement(input):
    accState.asyncValue()                              // VALUE_GET  per record
        .thenAccept(acc -> { fold in Java (GroupAggHelper);
                             accState.asyncUpdate(newAcc); });  // VALUE_UPDATE per record
```

### q11 = SESSION window `count(*)` → `ValueState<RowData>` (window-namespaced)
`q11.sql` is a session-window count grouped by bidder. The async window-agg
processor (`AbstractAsyncStateWindowAggProcessor`) stores the window accumulator as:

```java
ValueState<RowData> state = ...getOrCreateKeyedState(
        ..., new ValueStateDescriptor<>("window-aggs", accSerializer));   // line 75-81
this.windowState = new WindowAsyncValueState<>(state);                    // line 82-83
```

i.e. **also a `ValueState<RowData>`**, namespaced by window, plus a
`MergingWindowSet` (a MapState) for session merges.

### The decisive consequence
**Neither q11 nor q17 ever touches Flink's `AggregatingState`/`ReducingState`
API.** Flink SQL keeps the accumulator Row in a **`ValueState`** and does the fold
in generated Java (`GroupAggHelper` / the window agg processor). Therefore the
`ReducingAggregatingCache` that *is* already wired into
`ForStRsAsyncAggregatingStateV2` / `…ReducingStateV2` (full `tryFold` /
`PendingMissTable` / generation-race / barrier-flush machinery, active under the
uniform inline executor since `regimeSwitch == null`) is **NEVER EXERCISED by
q11/q17**. The R-2 lever has to land on **ValueState**, which today has **no
value write-back cache**.

### Why ValueState is NOT batched while MapState/Aggregating are
- `ForStRsMapStateV2` overrides `asyncGet(UK)` (non-final on `AbstractMapState`)
  to consult a per-instance write-through `MapStateCache` (read-your-writes, LRU,
  barrier-flush). Map ops therefore coalesce repeated same-key access to zero
  engine I/O on a hit.
- `ForStRsAsyncAggregatingStateV2` overrides `asyncAdd`/`asyncGet` (non-final on
  `AbstractAggregatingState`) to consult `ReducingAggregatingCache`.
- `ForStRsValueStateV2` has only a per-`RecordContext` **key-bytes** cache
  (`Slot[]` keyed by state ordinal) — it caches the *composite key serialization*,
  NOT the value. Worse, `AbstractValueState.asyncValue()` / `asyncUpdate(V)` are
  declared **`final`**, so the subclass cannot intercept the value the way MapState
  / Aggregating do. Every q11/q17 record therefore issues a real `VALUE_GET`
  (engine point-get) and a real `VALUE_UPDATE` (engine put) — batched into one FFM
  crossing per dispatch batch by the `VectorizedExecutor`, but with **no RMW
  coalescing**: the AEC's key-occupancy serializes same-key records, so the
  second touch of a hot key still pays a full engine point-get + decode for a
  value the operator just wrote.

This is the precise per-record SYNC aggregating-RMW the task names, located on the
ValueState path.

---

## 1. DESIGN — a ValueState write-back RMW cache (R-2)

Mirror the proven `MapStateCache` / `ReducingAggregatingCache` pattern at the
ValueState level:

- **flink-runtime, minimal surface change.** Make
  `AbstractValueState.asyncValue()` / `asyncUpdate(V)` / `value()` / `update(V)`
  **non-final** (they are already trivial one-liners delegating to
  `handleRequest`). This is the same shape `AbstractMapState` / `AbstractAggregatingState`
  already have (their async ops are non-final) — it does not change any default
  behavior; it only permits a backend subclass to intercept. No other runtime
  class depends on them being final.

- **`ForStRsValueStateV2` write-back cache.** Add a per-instance LRU
  `ValueState` cache `(compositeKey → cachedValue, dirty)`:
  - `asyncValue()` override: on hit return `completedFuture(cachedValue)` (zero
    engine I/O — including the negative/tombstone case = "known absent"). On miss,
    fall through to `super.asyncValue()`, and on resolve `putIfAbsent`-seed the
    cache (so a concurrent update wins).
  - `asyncUpdate(v)` override: write `v` into the cache marked **dirty**, return a
    completed future (no engine PUT per record). Read-your-writes: the next
    `asyncValue` for the key sees the dirty value → the changelog UPDATE_BEFORE /
    UPDATE_AFTER the operator computes is correct.
  - `asyncClear()` / `onClear`: tombstone the slot (dirty), generation-bump so an
    in-flight miss-resolve cannot resurrect it (the A6-H1 race guard, copied from
    `ReducingAggregatingCache`).
  - LRU eviction of a dirty entry flushes it to the engine via the deferred-flush
    slot (E8-H1 pattern), so a scattered working set (q11 sessions) stays bounded
    and never loses a write.
  - `flushOnBarrier()`: drain all dirty entries to the engine PUT/DELETE; wired
    into the backend snapshot PHASE 1.d next to the Reducing/Aggregating drains.

- **Reuse, don't reinvent.** The `ReducingAggregatingCache` already implements the
  exact byte[]-keyed LRU + generation-race + deferred-flush + barrier-drain
  semantics this needs (its combiner is `(acc, in) -> in'`; for a pure value cache
  the "combiner" is just "replace with the new value"). The ValueState cache is a
  thin specialization: `tryReplace(key, v)` (always overwrite the slot, mark dirty)
  for `asyncUpdate`, `peek/contains` for `asyncValue`, `invalidate` for clear,
  `flushAllDirty` for the barrier. We back it with `ReducingAggregatingCache`
  parameterized so `combiner = (old, in) -> in` — i.e. last-write-wins replace.

- **Gating (uniform, no per-query branch).** Cache usable ⇔ NOT
  legacy-pipelined/parallel AND (no regime switch OR LIGHT) — the identical
  `rmwCacheUsable()` predicate the aggregating cache uses. Under the uniform config
  (`FRS_RS_EXECUTOR` unset → inline executor → `regimeSwitch == null`) the cache is
  **ON**, which is exactly the regime q11/q17 run in. Under the opt-in parallel /
  pipelined executors the cache is bypassed (the value flows through the existing
  batched VALUE_GET/VALUE_UPDATE path), preserving the proven-correct parallel
  config — so no query is robbed.

### Why correctness is exact
The fold stays byte-for-byte in the generated SQL operator (`GroupAggHelper` /
window agg processor); the backend only decides whether the value read comes from
the cache or the engine, and whether the write lands in the cache (flushed at
barrier) or the engine directly. Read-your-writes within a key-group + barrier
flush + clear-tombstone + generation-race-guard make the cached path observably
identical to the engine path: same value seen by every read, same final persisted
value at snapshot. This is the same correctness contract `MapStateCache` already
satisfies for q11/q12/q16.

### Why it stays batch-only / zero-copy
Cache hits are pure on-heap (no FFM crossing at all). Misses fall through to the
existing batched `VectorizedExecutor` VALUE_GET (one crossing per batch, off-heap
Arrow staging, zero-copy `MemorySegmentDataInputView` decode). Barrier flush
serializes once per dirty key into the per-instance `valueOut` and routes to the
engine PUT — the same mechanism the Aggregating cache flush uses.

---

## 2. Estimated win & honest ceiling (unchanged from §4 of the q11 spec)

R-2 removes the per-record dependent get→put stall and amortizes the FFM crossing
for **hot keys with temporal locality** — strong for q17 (`GROUP BY auction`: bids
for an auction cluster in event time), weaker for q11 (session keys are scattered;
the cache still collapses the multiple out-of-order touches of a live session while
resident, but a session evicted before its next touch pays the engine point-get).
The **dominant** residual is the engine-side scattered inline point-get read-amp
(term 1, ~534 ns — the R-1 engine lever, separate from this Flink change) plus the
irreducible Flink windowed-RMW structure (the AEC per-record async dispatch +
`MergingWindowSet` per-record mapping for q11). ForSt-C++ at q11 133 s (1.24×) is
the realistic engine-parity floor for this shape; **≤1.25× from the backend alone
is not guaranteed** — R-2 is the Flink-side half, R-1 is the engine half.

# Built-but-Unwired API Inventory — L1 + L2 scan

**Date:** 2026-05-19
**Scope:** `flink-statebackend-forst-rs` (Java backend). Rust engine-side scan is a separate ticket.
**Driver:** The 2026-05-19 MapStateCache mis-attribution post-mortem identified that perf work can be wasted on code paths that look wired but aren't. This inventory is the workstream that catches such cases proactively.

This document tracks two layers of "built but inert" code:

- **L1 — Unwired APIs:** declared and implemented but with no production caller. The code is dead until someone wires it up. Pure waste until then; can mask further design errors (e.g., a follow-up may "use" the API without realizing it's the only consumer).
- **L2 — Partial wiring:** the API has a caller, but the caller's hot path bypasses it via an alternative code path that was not updated when the new API landed. Looks alive in commits and tests but is silently bypassed in production.

The second case is far more dangerous because grep alone won't catch it.

---

## L1 — Unwired APIs

### Findings (production code only — `src/main/java`)

| API (`ForStRsLinker.*`) | Production callers | Status |
|---|---|---|
| `frsVecIterPrefixOpen` | 2 (VectorizedExecutor + ForStRsDBIterRequest) | ✅ wired |
| `frsVecIterPrefixNext` | 2 (VectorizedExecutor + ForStRsDBIterRequest via Commit A) | ✅ wired |
| `frsVecIterPrefixClose` | 2 (ForStRs cleanup) | ✅ wired |
| `frsVecIterPrefixAbort` | 1 | ✅ wired |
| `frsVecMergeAppend` | 1 (VectorizedExecutor.dispatchAppendMerge) | ✅ wired |
| `frsVecIterRangeOpen` | 1 (VectorizedExecutor.dispatchIterRange) | ✅ wired (Open) |
| **`frsVecIterRangeNext`** | **0** | **⚠️ UNWIRED** |
| **`frsVecIterRangeClose`** | **0** | **⚠️ UNWIRED** |
| **`frsVecIterRangeAbort`** | **0** | **⚠️ UNWIRED** |

**Severity:** Medium. The ITER_RANGE family has an open-without-close-or-next configuration. Calling `dispatchIterRange` in production would leak the iterator handle (no close) and produce no useful data (no next). This is half-built — either the lifecycle should be finished or the Open should be removed pending real demand.

**Recommendation:**
1. **If ITER_RANGE is a near-term V1.1 use case** (e.g., a user-requested range scan): finish wiring `Next/Close/Abort` in a single follow-up PR. Reference: pattern is already proven by `frsVecIterPrefixNext` (Commit A) + `frsVecIterPrefixClose`.
2. **If not near-term:** delete `dispatchIterRange` and the corresponding FFI declarations to avoid lurking dead code that could mislead future contributors. The Rust side can keep the engine API if a downstream consumer is planned, but the Java glue should not exist without a caller.

This is the **first concrete L1 case** uncovered by the workstream.

---

## L2 — Partial wiring

This was the failure mode of the MapStateCache mis-attribution (2026-05-19): the cache was wired into `ForStRsMapStateV2.asyncGet/asyncPut/asyncRemove/asyncContains`, but those methods are bypassed by Q11/Q12's code path (which uses `ForStRsValueStateV2`). The cache "had read+write methods" but the Q11/Q12 hot path never traversed them.

### Cache field: `RecordContext.extra` (per-record composite-key bytes cache)

This Flink-runtime cache is consumed by each forst-rs state class's `serializeKey` and `serializeKeyInto`. Each implementation must independently consult the cache; there is no inherited default.

Verified consumers (production code only — `src/main/java/org/apache/flink/state/forstrs/state/`):

| State class | `serializeKey` consults cache | `serializeKeyInto` consults cache | Hot path |
|---|---|---|---|
| `ForStRsValueStateV2` | ✅ | ✅ (this session) | RMW chain via WindowAsyncValueState (Q11/Q12) |
| `ForStRsAsyncReducingStateV2` | ✅ (this session) | ✅ (this session) | RMW chain via ReducingState.add |
| `ForStRsAsyncAggregatingStateV2` | ✅ (this session) | ✅ (this session) | RMW chain via AggregatingState.add |
| `ForStRsMapStateV2` | ❌ — full composite includes userKey which varies, so cache wouldn't help much | ❌ — same | Q20 map ops, user-key-varying |
| `ForStRsAsyncListStateV2` | ⚠️ NOT cached — `serializeKey` line 94 builds prefix without consulting cache | ⚠️ NOT cached — `serializeKeyInto` line 110 same | ListState.add appends (same prefix repeated) |
| `ForStRsListStateV2` (non-async V2) | TBD — outside this session's scope | TBD | Likely matches AsyncListState pattern |

**L2 findings:**

1. **`ForStRsAsyncListStateV2` — partial wiring confirmed.** Multiple appends to the same list within one record share an identical composite key prefix (`k/<key>/<stateName>/`). The cache would hit on every append after the first, but neither `serializeKey` nor `serializeKeyInto` consults `RecordContext.extra`. Estimated win: small (key serialization is cheap), but the alignment with the other V2 state classes is a hygiene improvement. **Priority: low; bundle with next ListState perf touch.**

2. **`ForStRsMapStateV2` — partial wiring by design.** The composite key includes the user key, which varies per request, so caching the full composite would rarely hit. However, the **prefix portion** (`k/<operatorKey>/<stateName>/`) is invariant within a RecordContext and currently re-serialized on every request. A prefix-only cache would help Q20-style workloads (many same-bidder lookups). Estimated win: low-to-medium for Q20. **Priority: investigate after §6 measurement.**

3. **V1 state classes** (`ForStRsValueState`, `ForStRsMapState`, `ForStRsListState`, `ForStRsReducingState`, `ForStRsAggregatingState`) — these are the deprecated sync path. Q11/Q12 use V2. Skip auditing them; document as out-of-scope for the V1.1 work.

---

## Procedure (for future runs of this workstream)

The L1 scan is mechanical:

```bash
cd flink-state-backends/flink-statebackend-forst-rs
for m in $(grep -oE "public int frs[A-Za-z]+\(" src/main/java/org/apache/flink/state/forstrs/ffm/ForStRsLinker.java | sed 's/public int //;s/($//'); do
    c=$(grep -rln "\.$m(" src/main/java | grep -v ForStRsLinker.java | wc -l)
    echo "$m: $c caller files"
done
```

Any FFI method showing 0 production callers is L1.

The L2 scan is targeted to cache fields. List every named cache (search for `Cache`, `LRU`, `setExtra`, `getExtra` in state-class files), then for each cache verify:

1. Every state class that *could* benefit reads it on the hot path.
2. Every write to the cache has a corresponding read somewhere (otherwise the cache is write-only — wasted work).
3. The state class implementing the read covers all variants of its hot path (`serializeKey` AND `serializeKeyInto` for vectorized backends).

A miss on (1) or (3) is an L2 finding.

---

## Calibration: what counts as "partial wiring"

This document is conservative. Examples that **don't** count as L2:

- A cache that exists for a future workload but no current workload benefits — that's an inactive but correct cache, captured by [[project_q11q12_speedup_session_2026-05-19]].
- A cache that's deliberately disabled by feature flag — explicit configuration is not partial wiring; the user opts in. (`MapStateCache` is now in this category after `forst.rs.mapstate.cache.enabled` was added.)

The bar is: **the code looks wired (has callers, has tests passing) but the production hot path bypasses it in practice.** That bar is what the L2 scan tests for.

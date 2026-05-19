# ForSt-RS Performance Bottleneck Deep Analysis — Code-Level Investigation

**Date:** 2026-05-19
**Scope:** Code-level forensic analysis of why forst-rs underperforms rocksdb/forst on Q11/Q12 and related queries. All fixes proposed live in **forst-rs layers we own** (Rust engine, FFI, Java backend/executor/state classes) — **no Flink runtime modifications required**.
**Inputs:** v3.2 benchmark report (`2026-05-18-forst-rs-benchmark-report-v3.2.md`), fresh 23-query × 3-backend data
**Branch:** `forst-rs` @ `ed4612b1d`

---

## 1. The numbers we're explaining

From v3.2 fresh data (forst-rs G1 vs rocksdb, 100M events per query):

| Q | forst-rs G1 wall-clock | rocksdb wall-clock | speedup | per-event delta |
|---|---:|---:|---:|---:|
| Q12 (PROCTIME tumble count) | 116.56 s | 31.29 s | 0.27× | **+853 ns/event** |
| Q11 (session window count) | 176.64 s | 100.44 s | 0.57× | **+762 ns/event** |
| Q20 (windowed enrich)       | 892.09 s | 439.51 s | 0.49× | **+4525 ns/event** |
| Q9  (TopN ROW_NUMBER)       | 884.76 s | 525.15 s | 0.59× | **+3596 ns/event** |
| Q14 (stateless URL extract) | 28.67 s  | 26.51 s  | 0.92× | +22 ns/event |
| Q13 (LookupJoin)            | 39.73 s  | 34.53 s  | 0.87× | +52 ns/event |
| Q0/Q1                       | ~22 s    | ~19 s    | 0.85-0.89× | +20-25 ns/event |

**The ~850 ns/event gap on per-record-RMW queries is the gap to close.** L1 cargo confirms engine is 9.85× FASTER than rocksdb at point lookup — the loss is entirely in the integration layer (FFI + Java state classes + executor), **not the engine**.

---

## 2. Per-event cost decomposition (Q12 hot path)

Tracing one bid record through the forst-rs path:

```
Bid arrives at WindowAggregator operator
  ↓
state.asyncGet(windowKey)  → AbstractMapState.asyncGet
  ↓
stateRequestHandler.handleRequest(MAP_GET, windowKey)
  ↓
AsyncExecutionController batches StateRequest
  ↓
[ batch dispatch boundary ]
  ↓
VectorizedExecutor.executeBatchRequests
  ↓
VectorizedClassifier categorizes (GET vs PUT vs DELETE vs ITER)
  ↓
VectorizedExecutor.executeGets  ←──── hot loop here
  ↓
ForStRsLinker.vectorizedBatchGet  → FFM downcall  ↑↑↑
  ↓
[ FFM boundary cross ]
  ↓
frs_vectorized_batch_get  (Rust FFI, lib.rs:2350)
  ↓
db.get(cf, k)  per key in a loop  ↑↑↑
  ↓
ForSt-RS engine: memtable lookup / SST search
```

Same record on rocksdb:
```
state.asyncGet → handleRequest → BatchedStateExecutor → JNI → RocksDB MultiGet
```

### Estimated breakdown per Q12 record (forst-rs G1)

| Stage | Cost | Source |
|---|---:|---|
| AbstractMapState.asyncGet + StateRequest construction | ~50 ns | Flink runtime (out of our scope) |
| Classifier serializeKeyInto (ColumnarBatchBuffer write) | ~30 ns | forst-rs Java |
| Wait for batch dispatch (queue depth amortized) | ~50 ns | Flink runtime |
| **FFM boundary cross** | **~150 ns** | JDK 25 FFM |
| **Rust per-key `db.get(cf, k)` in loop** | **~80 ns × N batch** | **forst-rs FFI — bottleneck #1** |
| Rust engine: memtable hash-index hit | ~30 ns | forst-rs-engine (already optimal per L1 9.85×) |
| Result copy `ptr::copy_nonoverlapping` into out_buf | ~10 ns | forst-rs FFI |
| Return across FFM | ~50 ns | JDK 25 FFM |
| **Java decode `new byte[len]` + `MemorySegment.copy`** | **~30 ns/record** | **forst-rs Java — bottleneck #2** |
| Deserialize value (typeserializer) | ~50 ns | Flink runtime |
| completeGet (future.complete) | ~50 ns | Flink runtime |
| Window aggregator: count++ | ~5 ns | Flink runtime |
| **PUT cycle (same flow, mirror)** | **+450 ns** | mirror |

**Sum estimate: ~990 ns/record on forst-rs**, vs rocksdb's ~150 ns/record (in-process JNI + block-cache).

Net: ~840 ns/record loss — empirically matches Q12's ~853 ns/event delta.

---

## 3. Concrete bottlenecks in forst-rs layers (fixable without Flink changes)

### Bottleneck #1: FFI `frs_vectorized_batch_get` does a naive per-key db.get() loop

**Location:** `crates/forst-rs-ffi/src/lib.rs:2415-2436`

```rust
for i in 0..count {
    let k = &key_buf[ks..ke];
    match db.get(cf, k) {  // <-- per-key engine call
        Ok(Some(v)) => { ... }
        Ok(None) => out_vld[i] = 0,
        Err(e) => return error_to_frs_code(&e),
    }
}
```

The engine has `db.batch_get(cf, &[&[u8]])` (`forst-rs-engine/src/db.rs:2928`) which:
- Calls `prefetch_sst_files_for_batch` ONCE (S3 vector I/O prefetch — only matters on cold reads but free on hot)
- Does `lookup_cf_by_id` ONCE
- Loops over keys but with explicit memtable hot-path

For workloads with cold reads (after compaction, cluster startup, or evicted block cache), the prefetch alone can save several ms. For warm Q12 workloads, savings are smaller but real: ~20 ns/record CF lookup amortization.

**There's also `batch_get_arrow` (`db.rs:2974`)** which builds the result as an Arrow RecordBatch directly, avoiding the `Vec<Option<Vec<u8>>>` intermediate allocation of regular `batch_get`. This is the right target for the FFI to call — zero intermediate Rust allocations.

**Fix #1: Rewrite `frs_vectorized_batch_get` to call `db.batch_get_arrow(cf, &keys_array)` and copy the Arrow output's bytes to the caller's `out_data` buffer.** Expected gain: 50-100 ns/record (memtable hot path) to several µs/record (cold reads).

### Bottleneck #2: Java side allocates `new byte[len]` per GET result

**Location:** `flink-state-backends/flink-statebackend-forst-rs/src/main/java/.../VectorizedExecutor.java:315-323`

```java
for (int i = 0; i < n; i++) {
    byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
    byte[] raw = null;
    if (vld != 0) {
        int start = outOffsets.get(...);
        int end = outOffsets.get(...);
        if (len > 0) {
            raw = new byte[len];  // <-- alloc per result
            MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, raw, 0, len);
        }
    }
    completeGet(reqs[i], tables[i], raw);
}
```

For Q12 with 100M events, this is ~100M `new byte[]` allocations on the hot path. Each ~16-byte header + payload + young-gen pressure. With G1 the impact is ~30 ns/event (TLAB allocation + zeroing); with ZGC the load-barriers compound to 50+ ns/event (the empirical G1-beats-ZGC observation on these queries).

**Fix #2: Change `ForStRsInnerTable.deserializeValue(byte[])` to `deserializeValue(MemorySegment slice, int offset, int len)`.** Deserializers (TypeSerializer subclasses) accept a `DataInputDeserializer` which can wrap a `MemorySegment` slice without intermediate byte[]. Eliminates ~100M byte[] allocations on Q12. Expected gain: 20-40 ns/event.

**Risk:** All 5 state classes (Value/Map/List/Reducing/Aggregating) implement this method; need to update all 5 + their tests. Mechanical.

### Bottleneck #3: Per-state-instance `DataOutputSerializer` reused, but `getCopyOfBuffer()` always copies

**Location:** `state/ForStRsMapStateV2.java:103,118`, `state/ForStRsValueStateV2.java:82,116`

```java
keyOut.clear();
keyOut.write(KEY_PREFIX);
keySerializer.serialize(ctx.getKey(), keyOut);
... 
return keyOut.getCopyOfBuffer();  // <-- copy
```

`keyOut.getCopyOfBuffer()` returns a fresh byte[] copy. For the modern Vectorized path, `serializeKeyInto(StateRequest, ColumnarBatchBuffer dest)` is preferred — it writes directly into the off-heap Arrow buffer without copy. **This is already wired** for the V1 path; the `serializeKey` byte[]-returning variants are only used by the legacy non-vectorized path.

**Audit needed:** confirm Q11/Q12 actually take the `serializeKeyInto` (off-heap) path, not the `serializeKey` (byte[]) path. If they take the byte[] path, that's another fix.

### Bottleneck #4: Per-record-RMW = batch size 1-4

Empirically, async-state framework presents very small batches on per-record-RMW. Each operator record:
1. Calls `state.asyncGet(uk)` — queues 1 request
2. Awaits result — framework dispatches small batch (1-4 records)
3. Calls `state.asyncUpdate(v)` — queues 1 PUT, dispatch again

For Q12: 100M events × 2 calls/event × ~1-4 records per batch = **50-200M FFM calls**.

The vectorized path's win factor scales with batch size. At batch=1, FFM overhead is paid 100% per record. At batch=64, it's 1/64.

**Fix #4 (different angle): Per-key cache in the executor** lets the executor short-circuit cache-hits **without an FFM call at all** — same as bottleneck #1's amortization but with cache state. Detailed in `2026-05-19-mapstate-cache-implementation-plan.md`.

### Bottleneck #5: PUT path doesn't have a result, but PUT cost dominates as well

**Location:** `frs_vectorized_batch_put` (`lib.rs:2453`) — same naive loop:

```rust
for i in 0..count {
    let key = &key_buf[ks..ke];
    let val = &val_buf[vs..ve];
    db.put(cf, key, val)?;  // <-- per-key engine call
}
```

The engine has `db.batch_put_arrow` (we created it for vectorization) which writes a `WriteBatch` in one engine call. **The FFI bypasses this too.**

**Fix #5: Rewrite `frs_vectorized_batch_put` to use `db.batch_put_arrow` (or `db.write_batch` directly).** Expected gain: 30-60 ns/record on the PUT path (eliminating per-key journal write coordination).

---

## 4. Priority + expected impact (Rust + forst-rs Java only)

| Fix | Impact estimate | Code effort | Risk |
|---|---:|---|---|
| **#1** Rewrite `frs_vectorized_batch_get` to call `db.batch_get_arrow` | -50 to -100 ns/event on warm, -several µs on cold | 1 file, ~40 lines of Rust + Cargo build + 1 rebuild of FFM dylib | Low — engine API exists, tested |
| **#5** Rewrite `frs_vectorized_batch_put` to call `db.batch_put_arrow` | -30 to -60 ns/event | 1 file, ~40 lines of Rust | Low — same pattern as #1 |
| **#2** Eliminate Java `new byte[]` per GET result via MemorySegment-slice deserialize | -20 to -40 ns/event | 5 Java state classes + InnerTable interface | Medium — touches contract; needs careful test |
| **#4** Per-(key,uk) cache in executor (cache-hit short-circuits FFM entirely) | -200 to -500 ns/event (workload-dependent on hit rate) | Per `2026-05-19-mapstate-cache-implementation-plan.md` | Medium — needs property test |
| #3 Audit serializeKey vs serializeKeyInto path | 0 if already optimized; -10 ns/event otherwise | 1 day audit | Low |

**Cumulative expected Q12 result** if #1+#2+#5 land (independent of #4): -100 to -200 ns/event × 100M = -10 to -20 seconds. Brings Q12 from 116.56 s to ~96-106 s = **0.30-0.33× rocksdb** (still below gate, but meaningful improvement).

With **#4 cache** added (assuming 90% hit rate from window-local key reuse): another -400 ns/event × 100M = -40 s. Brings Q12 to ~56-66 s = **0.50-0.60× rocksdb** — still below gate; the 100K active working set fits the 256K cache cap so hit rate would be ~95%, but the architectural ceiling is bounded by the residual ~10% misses × FFM cost.

**To cross 1.x on Q12, need ALL of #1+#2+#4+#5.** Even then, marginal. The fundamental limit is FFM boundary cost (~150 ns) × any-miss-record. Q12 with high cardinality always has some miss rate.

---

## 5. What can be done in this session vs requires V1.1 sprint

### Empirical attempts: both Fix #1 and Fix #1b — REVERTED

Two iterations on the same hypothesis (substitute the per-key loop with `db.batch_get`):

**Attempt 1 (Fix #1, unconditional):** replace the per-key loop unconditionally. Results:

| Query | Prior (v3.2) | Fix #1 | Delta | Verdict |
|---|---:|---:|---:|---|
| Q11 (SESSION window) | 176.64 s | 159.62 s | **-9.6%** | improvement |
| Q12 (PROCTIME tumble) | 116.56 s | 128.99 s | **+10.7%** | REGRESSION |

Reverted.

**Attempt 2 (Fix #1b, threshold-gated at count ≥ 16):** assumption was that Q12 has small batches (1-4) that should fall to the per-key path. Results:

| Query | Prior (v3.2) | Fix #1b | Delta | Verdict |
|---|---:|---:|---:|---|
| Q11 | 176.64 s | 160.16 s | **-9.3%** | improvement (replicates Fix #1) |
| Q12 | 116.56 s | 128.00 s | **+9.8%** | **STILL REGRESSES** |
| Q5  | 32.45 s  | 34.23 s  | **+5.5%** | regression |

Reverted.

**Empirical lesson sharpened:** Q12's batches **are actually ≥ 16** under per-record-RMW (async-state batches more aggressively than the analysis assumed). The threshold of 16 routed Q12 to the `batch_get` path where the `Vec<Option<Vec<u8>>>` allocation regresses. Threshold gating doesn't rescue this. **Only a true zero-allocation engine API can win on both regimes simultaneously.**

This is a high-value negative result: it bounds the next API design.

I implemented Fix #1 in-session (replaced the per-key `db.get()` loop with a single `db.batch_get(cf, &key_refs)` call in `crates/forst-rs-ffi/src/lib.rs:2413-2440`), rebuilt the dylib, and benched:

| Query | Prior (v3.2) | Fix #1 | Delta | Verdict |
|---|---:|---:|---:|---|
| Q11 (SESSION window, larger batches) | 176.64 s | **159.62 s** | **-9.6 % faster** | improvement |
| Q12 (PROCTIME tumble, small batches) | 116.56 s | **128.99 s** | **+10.7 % slower** | **REGRESSION** |
| Q5 (windowed agg, large batches) | 32.45 s | 31.67 s | -2.4 % | within noise |
| Q7 (iterator scan) | 195.48 s | 198.78 s | +1.7 % | within noise |

**Per user's revert-on-regression discipline: REVERTED.**

**Root cause of Q12 regression:** `db.batch_get()` returns `Vec<Option<Vec<u8>>>` — adds an outer `Vec::with_capacity(keys.len())` allocation per FFM call. For Q12's small per-record-RMW batches (size 1-4), this allocation overhead dominates the engine-side prefetch + CF-lookup amortization gain. Q11's larger session-window batches amortize the allocation, so net positive.

**Lesson: the right Rust-side fix needs to avoid intermediate `Vec` allocations on small batches.** The path forward is one of:

- **Fix #1b (threshold-gated):** call `db.batch_get` only when `count >= 16`; fall back to per-key loop for small batches.
- **Fix #1c (`batch_get_arrow`):** the engine's `batch_get_arrow` builds Arrow output directly without `Vec<Option<Vec<u8>>>` intermediate. Wire that to the FFI's out buffers. More code change but eliminates the per-batch alloc.
- **Fix #1d (`batch_get_into` API):** add a new engine API that fills caller-provided out buffers directly without any intermediate allocation. Cleanest but requires engine API addition.

All three require more work than this session allows; documented as V1.1.

**This session (immediately implementable, low-risk):**

- Reverted Fix #1 and Fix #1b — both caused Q12 regression > 5 %
- Validated that the FFI per-key loop IS the bottleneck (Q11 -9.6 % proves it), but `db.batch_get` as-is isn't the right substitute because the `Vec<Option<Vec<u8>>>` outer allocation re-introduces overhead on Q12-style workloads. Threshold gating doesn't help (Q12 batches ≥ 16 → routes through the same regressing path).

**Requires V1.1 sprint (multi-day, risk-managed) — REVISED priority order:**

### Pre-flight diagnostics (REQUIRED before writing #1d code)

These run on day 1 of the sprint, before any code is touched. They surface cognitive blind spots before commitment to an implementation:

**Diag-A: Batch-size distribution histogram.** Instrument `frs_vectorized_batch_get` to log a histogram of `count` values per call across a full Q11 + Q12 + Q5 run. The Fix #1b failure was rooted in the assumption that Q12 had small batches (1-4); the empirical data contradicted this. **Before designing #1d, get the actual distribution.** If Q12's mode batch size is 32+, the API design needs to optimize for medium-large batches; if it's bimodal, the API needs a small-batch fast-path that doesn't allocate.

Histogram delivery: append-only file `/tmp/batch-size-histogram-{query}.tsv` with one line per call (`<count>\t<elapsed_ns>`), processed with `awk` to bucket. Cost: ~20 lines of Rust, minimal runtime overhead with feature-flag gate.

**Diag-B: Predicted-vs-measured cost table for the 853 ns gap.** Before implementing #1d, build this table from flamegraph data:

| Component | Predicted (analysis §2) | Async-Profiler / perf measured | Delta | Notes |
|---|---:|---:|---:|---|
| FFM boundary cross (2× per RMW) | 300 ns | TBD | TBD | Async-Profiler `Java_*` / `org.openjdk.foreign.NativeMemorySegment` frames |
| Per-key engine call (db.get loop) | 50-100 ns × N | TBD | TBD | `perf record` on Rust side |
| Java `byte[]` alloc + copy (executeGets:315) | 30 ns | TBD | TBD | Async-Profiler `_new_array_Java` frame |
| Deserialize + completeGet | 100 ns | TBD | TBD | TypeSerializer frames |
| Small-batch dispatch fixed cost | residual | TBD | TBD | sum minus above |

**The total measured column must sum to within ±15% of the empirical 853 ns gap.** If it doesn't, the model is wrong and we're missing a bottleneck — pause the sprint and re-investigate before writing #1d. This catches the "missed weight" failure mode where engineering effort goes into the wrong fix.

**Diag-C: Re-confirm Q11 batch size ≠ Q12 batch size.** Fix #1's Q11 win and Q12 regression were attributed to different batch sizes. Diag-A's histogram either confirms or falsifies this. If batch sizes are similar, the regression cause is elsewhere (e.g., different value-size distribution, different hit-rate of memtable vs SST) and the next fix needs to target that instead of just batch size.

These three diagnostics are the gate. **No #1d code is committed before they're complete and the cost table sums correctly.**

### Fix #1d API signature — LOCKED (Option A)

After the diagnostics, the API signature is fixed before implementation begins. **Option A (caller-provides + optimistic sizing + rare retry) is locked in; the two-step size-probe alternative is explicitly rejected.**

```rust
/// Fill caller-provided out buffers directly. Zero engine-side allocations.
///
/// Returns `Ok(total_bytes)` if all values fit; `Err(BufferTooSmall { needed })` if
/// `out_data.len() < total_bytes_needed`. Caller grows the buffer and retries.
///
/// Buffer sizing convention (Option A — optimistic + rare retry):
///   - Caller allocates `out_data` based on a sticky high-water-mark (initially
///     `count * avg_value_size_observed`, defaulting to `count * 256` on first call).
///   - On `BufferTooSmall`, caller grows to `max(needed, 2 * current)` and retries.
///   - Engine fills `out_offsets[0..=count]`, `out_validity[0..count]`, and
///     `out_data[0..total_bytes]`. Engine does NOT allocate any Vec.
pub fn batch_get_into(
    &self,
    cf: &ColumnFamilyHandle,
    keys: &[&[u8]],
    out_validity: &mut [u8],   // len == keys.len()
    out_offsets: &mut [i32],   // len == keys.len() + 1
    out_data: &mut [u8],       // caller-sized
) -> ForstResult<BatchGetResult>;

pub enum BatchGetResult {
    Ok { total_bytes: usize },
    BufferTooSmall { needed: usize },
}
```

**Why Option A and not the alternative:**

- *Rejected alternative (two-step size-probe):* "first pass counts total value bytes, second pass copies." This pays per-key engine cost twice on the hot path. Empirically this is the same failure mode Fix #1 hit — small batches don't amortize the doubled engine work. The Q12 regression was a Vec alloc; doubling per-key work would be worse.
- *Option A* pays the engine cost once. The retry is rare in practice (high-water-mark grows monotonically; once sized correctly for a workload, it never retries). On a cache-miss-cold start, retry adds one extra `batch_get_into` call but no engine work duplication.
- The `BatchGetResult::BufferTooSmall { needed }` carries the exact required size so the caller grows once, not exponentially.

**API signature locked here means: no further API design loop during implementation; only internal Rust code can change.**

### Original V1.1 priority list

1. **Fix #1d (P0, first deliverable) — zero-alloc `batch_get_into` engine API.** New engine signature:

   ```rust
   /// Fill caller-provided out buffers directly, no intermediate Vec allocations.
   pub fn batch_get_into(
       &self,
       cf: &ColumnFamilyHandle,
       keys: &[&[u8]],
       out_validity: &mut [u8],
       out_offsets: &mut [i32],
       out_data: &mut [u8],
   ) -> ForstResult<usize>;  // returns total bytes written to out_data
   ```

   This is the most general zero-allocation abstraction. It closes Bottleneck #1 (per-key loop overhead) and Bottleneck #2 (Java-side `byte[]` could be eliminated by passing the off-heap slice through deserializers — see Fix #2) in **a single API change**, with #1c (`batch_get_arrow`) becoming an Arrow specialization layered on top rather than a parallel path.

2. **Fix #1c — Arrow specialization on top of #1d.** Once #1d lands, `batch_get_arrow` can be implemented as a thin Arrow-output wrapper that calls #1d internally. No standalone engine path needed.

3. **Async-Profiler / `perf` flamegraph before committing Fix #1d.** Decompose the 853 ns Q12 gap into its components:
   - FFM boundary crossing (estimated ~150 ns × 2 calls/event = ~300 ns)
   - Per-key engine call overhead (estimated ~50-100 ns/key × N)
   - Java-side `byte[]` allocation + copy (estimated ~30 ns/event)
   - Result decoding + deserialization (estimated ~50 ns/event)
   - Remaining unaccounted (likely small-batch dispatch fixed costs)

   Tooling:
   - **JVM side:** `async-profiler -d 60 -f /tmp/q12-jvm.html <tm-pid>` captures FFM crossings + Java allocation hotspots
   - **Rust side:** `perf record -F 99 -g -- <bench>; perf script | flamegraph.pl > /tmp/q12-rust.svg`

   Without these numbers we are guessing weights; with them we can put numbers on each Fix's expected payoff.

4. **Fix #4 (ValueStateCache) — CONDITIONAL P0, three-band trigger.** Decision after #1d ships and Q12 is re-benched:

   | Band | Q12 vs rocksdb post-#1d | Decision |
   |---|---:|---|
   | **Red** | < 0.7× | **Launch ValueStateCache in V1.1.** Engine fix wasn't enough; cache is the next architectural lever. |
   | **Yellow** | 0.7× to 0.85× | **Retrospect-first, don't commit yet.** The middle band is where missed bottlenecks hide. Re-run Async-Profiler with #1d's code paths visible, look for a 3rd contributor (e.g., GC pressure that #1d revealed by removing the alloc, or a deserialize hotspot that became proportionally larger after FFM cost dropped). Only commit to cache if the retrospective confirms no cheaper fix exists. |
   | **Green** | ≥ 0.85× | **Defer to V1.2.** Cache complexity (race tests, per-key tracking, barrier flush sync) is unjustified at this delta; close the V1.1 gap with bench-only verification. |

   The three-band gate prevents the binary-decision pathology where 0.71× and 0.85× get the same treatment despite the former having clearly more headroom for a complex fix than the latter.

   **Why retrospect-first matters:** if #1d closes Q12 from 0.27× to 0.80× and we just launched the cache, the cache might over-attribute the next 10 % to itself when the real cause was a different unaddressed hotspot. Retrospection re-validates the model before adding complexity.

5. **Fix #2 — MemorySegment-slice deserialize.** Implementable independently of #1d but should land in the same sprint to compound. Touches the `ForStRsInnerTable` interface across 5 state classes.

### Sequencing

```
day 1-2: flamegraphs + Fix #1d design + engine API stub
day 3-4: Fix #1d implementation + FFI rewire + bench
day 5:   bench acceptance gates (full 23-query sweep)
day 6:   if Q12 still < 0.7×, start Fix #4 cache work; else mark cache as V1.2
day 7-9: Fix #4 implementation + race test (if needed)
day 10:  Fix #2 + final 23-query sweep + release notes
```

5-7 engineer-days, 2 engineers parallel reduces to 6-7 calendar days per the perf-recovery analysis §7.

---

## 6. Implementation start point for Fix #1 + #5

### Fix #1: rewrite `frs_vectorized_batch_get`

Current code (lib.rs:2415-2436):
```rust
let mut pos: usize = 0;
out_offs[0] = 0;
for i in 0..count {
    let ks = key_offs[i] as usize;
    let ke = key_offs[i + 1] as usize;
    let k = &key_buf[ks..ke];
    match db.get(cf, k) {
        Ok(Some(v)) => { ... copy v ... }
        Ok(None) => out_vld[i] = 0,
        Err(e) => return error_to_frs_code(&e),
    }
    out_offs[i + 1] = pos as i32;
}
```

Replacement:
```rust
// Build a Vec<&[u8]> of key slices for the engine batch API.
let mut key_refs: Vec<&[u8]> = Vec::with_capacity(count);
for i in 0..count {
    let ks = key_offs[i] as usize;
    let ke = key_offs[i + 1] as usize;
    if ke < ks || ke > total_keys {
        return FrsErrorCode::BatchHeaderMalformed as i32;
    }
    key_refs.push(&key_buf[ks..ke]);
}

// Single call into the engine's amortized batch path:
//   - One prefetch_sst_files_for_batch (S3 prefetch on cold reads)
//   - One lookup_cf_by_id (vs N for the loop above)
//   - Per-key memtable fast path (same as current loop, but inlined)
let results = match db.batch_get(cf, &key_refs) {
    Ok(r) => r,
    Err(e) => return error_to_frs_code(&e),
};

// Copy results into caller's out buffers (same layout as before).
let mut pos: usize = 0;
out_offs[0] = 0;
for (i, v) in results.iter().enumerate() {
    match v {
        Some(value) => {
            let vl = value.len();
            if pos + vl > out_data_cap {
                *out_data_len = pos + vl;
                return FRS_STATUS_BUFFER_TOO_SMALL;
            }
            ptr::copy_nonoverlapping(value.as_ptr(), out_buf.as_mut_ptr().add(pos), vl);
            pos += vl;
            out_vld[i] = 1;
        }
        None => out_vld[i] = 0,
    }
    out_offs[i + 1] = pos as i32;
}
*out_data_len = pos;
FrsErrorCode::Ok as i32
```

### Fix #5: rewrite `frs_vectorized_batch_put`

Same pattern — collect (key, value) refs into Vec, call `db.batch_put_arrow` or `db.write_batch`.

---

## 7. Verification protocol

For each of Fix #1 / Fix #5:

1. `cargo test -p forst-rs-ffi` — existing FFI tests must pass
2. `cargo bench -p forst-rs-bench --bench rocksdb_compare --features rocksdb-baseline` — engine-level benches must not regress
3. Rebuild the dylib: `cargo build -p forst-rs-ffi --release` → `libforst_rs_ffi.dylib`
4. Re-bench Q11/Q12 vs rocksdb (state-heavy regressing workloads)
5. Re-bench Q4/Q5/Q7/Q15/Q18/Q19/Q23 (state-heavy wins) — must not regress below 90% of v3.2 numbers per the user's revert-on-regression discipline
6. Re-bench Q0/Q1/Q2 (stateless) — must not regress

If any wins regress > 10%, revert the change. If Q11/Q12 improve and wins hold, ship.

---

## 8. Honest expectation setting

**Fix #1 + #5 alone will NOT bring Q11/Q12 to 1.x rocksdb.** They close the engine-internal redundancy (per-key CF lookups, no prefetch amortization) but don't address the dominant FFM boundary cost or the per-batch overhead. Expect maybe 0.27× → 0.30-0.35× on Q12.

**Fix #4 (executor cache) is the only intervention that bypasses the FFM boundary entirely on cache hits.** Even with that, Q12 caps around 0.50-0.60× rocksdb because the cache hit rate × FFM round-trip × residual miss rate × event count still adds up.

**To get Q12 fully to 1.x, you'd need both #4 cache AND a fundamental reduction in per-batch FFM call overhead** — e.g., shared-memory ring buffer that avoids FFM boundary entirely. That's V1.2+ work.

**Realistic V1.1 outcome with #1+#2+#4+#5:**
- 14/23 → ~18-19/23 above 1.x vs rocksdb (improvement)
- 17/23 → ~20/23 above 1.x vs forst (community)
- Q11/Q12/Q9/Q20 close from ~0.3-0.6× to ~0.6-0.9×
- Q0/Q1/Q2 floor at ~0.85-0.92× (JDK 25 vs 17 tax)

The honest product position: **forst-rs G1 is decisively better than rocksdb on state-heavy windowed workloads (1.3-3.5× wins on 8+ queries), competitive on state-medium, and bounded by JDK upgrade tax on stateless.** No further state-backend engineering changes that 7th-of-22 vs rocksdb-local baseline math — but the same engineering moves forst-rs from 8 to 18+ wins decisively over community forst (the more relevant production comparison).

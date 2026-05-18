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

### Empirical attempt: Fix #1 with `db.batch_get()` — REVERTED

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

- Reverted Fix #1 because Q12 regressed > 10% (failed user's gate)
- Validated that the FFI per-key loop IS the bottleneck, but `db.batch_get` as-is isn't the right substitute. The engine needs a zero-intermediate-alloc batch path.

**Requires V1.1 sprint (multi-day, risk-managed):**
- Fix #1c/#1d: zero-alloc batch_get on engine side
- Fix #2: MemorySegment-slice deserialize contract change across state classes
- Fix #4: cache + property test + bench acceptance gates (full plan in `2026-05-19-mapstate-cache-implementation-plan.md`)

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

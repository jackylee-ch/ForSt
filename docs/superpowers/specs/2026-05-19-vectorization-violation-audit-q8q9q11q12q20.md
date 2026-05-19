# Vectorization-Violation Audit — Q8 / Q9 / Q11 / Q12 / Q20

**Date:** 2026-05-19
**Scope:** Audit forst-rs (`~/Code/stczwd/ForSt` branch `forst-rs`) and flink-statebackend-forst-rs (`~/Code/stczwd/flink` branch `forst-rs-jdk25`) code paths exercised by the five regressing queries Q8, Q9, Q11, Q12, Q20. Goal: identify every per-key operation that **violates the end-to-end vectorization + zero-copy contract** by doing per-record work that should be batched.

**Output:** audit only. No code changes per `/superpowers:brainstorming` user decision.

**Baseline data (v3.2):** Q8 1.29×, Q9 0.59×, Q11 0.57×, Q12 0.27×, Q20 0.49× vs rocksdb. Q11/Q12 are state-heavy windowed-aggregate per-record-RMW; Q9/Q20 add prefix-iteration on top.

---

## SQL → state-shape map (what these queries actually do)

| Q | SQL shape | Dominant state operation | State class hit |
|---|---|---|---|
| Q8  | `TUMBLE(person, 10s) JOIN TUMBLE(auction, 10s)` | Windowed agg + streaming join | MapState (per-window accum) + MapState (join multiset) |
| Q9  | `ROW_NUMBER OVER auction JOIN bid` | TopN rank + streaming join + **prefix iteration** to rebuild rank on emission | MapState + MapState iterators |
| Q11 | `SESSION(bid by bidder, 10s gap)` | Session-window accum per bidder | MapState (per-session accum) |
| Q12 | `TUMBLE(B, PROCTIME, 10s)` | Per-bidder + per-window count | MapState (per-window accum) |
| Q20 | `bid INNER JOIN auction WHERE category=10` | Streaming join with **prefix iteration** on join probe | MapState + MapState iterators |

**Common path:** all 5 hit `ForStRsMapStateV2` (`flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java`). Q9 + Q20 additionally hit `ForStRsDBIterRequest` for prefix iteration. None hit ListState/ReducingState/AggregatingState directly in their hot path (those are wrapped inside Flink's internal window operators which use plain MapState underneath).

---

## VIOLATION #1 (CRITICAL): `ForStRsDBIterRequest.process()` drains iterator one entry at a time

**Location:** `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/ForStRsDBIterRequest.java:144-155`

```java
List<ForStRsLinker.IteratorEntry> entryList = new ArrayList<>(CACHE_SIZE_LIMIT);
boolean encounterEnd = false;
while (entryList.size() < CACHE_SIZE_LIMIT) {              // CACHE_SIZE_LIMIT = 128
    ForStRsLinker.IteratorEntry entry = linker.iteratorNext(iter);   // <<< per-entry FFM call
    if (entry == null) {
        encounterEnd = true;
        iter.close();
        iter = null;
        break;
    }
    entryList.add(entry);
}
```

**Per-entry cost of `linker.iteratorNext(iter)`** (file `ffm/ForStRsLinker.java:2521`):
- 1 confined Arena open (3 native MemorySegment allocations)
- 1 FFM downcall (`frs_iterator_next`)
- 2 `byte[]` heap allocations + memcpy (`copyAndFree` for key + value)
- Arena close on try-with-resources exit

Estimated cost: ~500-800 ns per entry × 128 entries = **64-100 µs per drain**.

**Affected queries:**
- **Q9** (ROW_NUMBER OVER): every output row triggers a prefix scan over the partition's bid history to rebuild the top-1 rank. Per the v3.2 number Q9 = 884.76 s on 100 M events, this is the dominant cost path.
- **Q20** (streaming inner join with WHERE filter): every bid record triggers a prefix scan on the auction side to find matches. Q20 = 892.09 s.
- **Q8** (windowed join, smaller impact): only at window-emit time, not per-record.

**The fix already exists in the codebase but is unwired:**

- **Rust side (`crates/forst-rs-ffi/src/lib.rs:3631`):** `frs_vec_iter_prefix_next(handle, chunk_buf_ptr, chunk_buf_cap, out_row_count, out_bytes_used)` returns up to N rows in **one FFM call** by writing all KV pairs into a caller-provided buffer using Arrow-style packing.
- **Java side (`ForStRsLinker.java:3132`):** `frsVecIterPrefixNext(handle, chunkBuf, chunkBufCap, outRowCount, outBytesUsed)` is wired and tested.

But `ForStRsDBIterRequest.process()` doesn't call it. It still uses the legacy `iteratorNext()` per-entry API. **The chunked API was built, wired, tested — and not connected to the hot path.**

**Expected impact of fixing:** 128 FFM calls per drain → 1 FFM call per drain = ~99% reduction in iterator-stepping overhead. On Q9: estimated -200 to -400 s wall-clock (i.e., 884.76 s → ~500-680 s, lifting Q9 from 0.59× to ~0.8-1.05×). On Q20: similar magnitude.

This is the single highest-impact fix on the audited code.

---

## VIOLATION #2 (HIGH): FFI `frs_vectorized_batch_get` does naive per-key `db.get()` loop

**Location:** `crates/forst-rs-ffi/src/lib.rs:2413-2436`

```rust
let mut pos: usize = 0;
out_offs[0] = 0;
for i in 0..count {
    let ks = key_offs[i] as usize;
    let ke = key_offs[i + 1] as usize;
    let k = &key_buf[ks..ke];
    match db.get(cf, k) {        // <<< per-key engine call
        Ok(Some(v)) => { ... }
        Ok(None) => out_vld[i] = 0,
        Err(e) => return error_to_frs_code(&e),
    }
    out_offs[i + 1] = pos as i32;
}
```

The Java side already batches into Arrow buffers and crosses FFM once. But inside the FFM boundary, the Rust code dispatches the engine call **per key in a loop** instead of using the engine's `batch_get(cf, &refs)` API at `crates/forst-rs-engine/src/db.rs:2928`.

The engine's `batch_get`:
- Does `lookup_cf_by_id` ONCE (vs N times in the per-key loop)
- Does `prefetch_sst_files_for_batch` ONCE (matters on cold reads — S3 vector I/O prefetch)
- Re-uses the active memtable reference across all keys

**Affected queries:** **all 5** — every MapState `asyncGet` ultimately calls this FFI.

**Empirical evidence:** documented in `2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md` §5. Two iterations (Fix #1 unconditional + Fix #1b threshold-gated at count ≥ 16) BOTH reverted because `db.batch_get` returns `Vec<Option<Vec<u8>>>` which adds outer Vec allocation that regresses Q12 (+10.7% / +9.8% respectively). The right fix is **Fix #1d (zero-alloc `batch_get_into` engine API)** locked at API design in the prior spec.

**Expected impact of fixing:** -50 to -100 ns/record on memtable hot path; bigger savings on cold paths via amortized S3 prefetch.

---

## VIOLATION #3 (HIGH): Java executor allocates `new byte[len]` per GET result

**Location:** `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/VectorizedExecutor.java:315-323`

```java
for (int i = 0; i < n; i++) {
    byte vld = outValidity.get(ValueLayout.JAVA_BYTE, i);
    byte[] raw = null;
    if (vld != 0) {
        int start = outOffsets.get(ValueLayout.JAVA_INT, (long) i * Integer.BYTES);
        int end = outOffsets.get(ValueLayout.JAVA_INT, (long) (i + 1) * Integer.BYTES);
        int len = end - start;
        if (len > 0) {
            raw = new byte[len];                            // <<< per-record alloc
            MemorySegment.copy(outData, ValueLayout.JAVA_BYTE, start, raw, 0, len);
        }
    }
    completeGet(reqs[i], tables[i], raw);
}
```

The off-heap GET path delivers results in a single `outData` MemorySegment. The Java code then **allocates a fresh `byte[]` per result and copies from off-heap to heap** before calling `completeGet`. This is a zero-copy violation: data already exists in off-heap; copying it to heap is the opposite of zero-copy.

**Affected queries:** all 5. For Q12 at 100 M events → ~100 M `new byte[]` allocations on the hot path. With G1, ~30 ns/alloc + 30 ns/copy = 60 ns/event × 100 M = **6 seconds of pure allocation overhead** on Q12.

**Why this exists:** `ForStRsInnerTable.deserializeValue` takes `byte[]`. Changing the contract to take a `MemorySegment slice + offset + length` (and updating the 5 state classes' `deserializeValue` implementations to wrap a `DataInputDeserializer` around the off-heap slice) eliminates the alloc + copy entirely.

This is Fix #2 in the deep-analysis doc. Mechanical change, touches 5 files, no race surface.

---

## VIOLATION #4 (MEDIUM): `MemorySegment.copy` rebroadcasts result in `iteratorNext` (related to #1)

**Location:** `ffm/ForStRsLinker.java:2541-2542` (called per iterator step in violation #1)

```java
byte[] keyCopy = copyAndFree(outKey, "frs_iterator_next/key");
byte[] valueCopy = copyAndFree(outValue, "frs_iterator_next/value");
return new IteratorEntry(keyCopy, valueCopy);
```

Each iterator entry causes **2 byte[] allocations + 2 memcpys + 2 native frees**. Compounded by violation #1 (128 entries per drain), this is the actual cost driver for iterator-heavy queries.

**Affected queries:** Q9, Q20.

**Fix:** subsumed by violation #1's fix — once the chunked `frsVecIterPrefixNext` is wired, all entries arrive in one MemorySegment, and the Java-side decode can either:
- (a) defer slicing to deserialize-time (passes `(buf, offset, length)` triples; no byte[] alloc)
- (b) batch-allocate one big array and slice into it

Either way, the per-entry `copyAndFree` becomes a per-chunk single-call.

---

## VIOLATION #5 (LOW-MEDIUM): MapStateV2 has no per-state cache

**Location:** `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/state/ForStRsMapStateV2.java`

Per-record-RMW on the same (keyContext, userKey) hits the engine every time. The cache infrastructure exists for `ReducingState/AggregatingState` (`flink-statebackend-forst-rs/src/main/java/.../cache/ReducingAggregatingCache.java`) but is NOT extended to `ForStRsMapStateV2`.

For Q11/Q12: each bid record for bidder X within window W reads MapState[W]→count, modifies, writes back. Same (X, W) pair gets hit many times. A `Map<(keyContext, userKey), value>` cache eliminates all but the first GET.

**Affected queries:** Q11, Q12.

**Why not addressed in this audit's immediate fix:** documented as the conditional B-2 ValueStateCache in the V1.1 sprint plan (`2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md` §5, three-band trigger). Per the V1.1 plan, this fix only ships if Fix #1d (`batch_get_into`) doesn't close Q12 past 0.85×.

---

## Vectorization-violation summary table

| # | Severity | Location | Per-record cost | Affected queries | Fix |
|---|---|---|---|---|---|
| 1 | **CRITICAL** | `ForStRsDBIterRequest.java:144-155` | 128× FFM per drain instead of 1 | Q9, Q20 | wire `frsVecIterPrefixNext` (already exists, just unwired) |
| 2 | HIGH | `forst-rs-ffi/src/lib.rs:2415-2436` | per-key engine call inside FFM | all 5 | Fix #1d `batch_get_into` (locked API per prior spec) |
| 3 | HIGH | `VectorizedExecutor.java:315-323` | per-result `new byte[]` alloc + copy | all 5 | Fix #2 MemorySegment-slice deserialize (5 state classes) |
| 4 | MEDIUM | `ForStRsLinker.java:2541-2542` | per-iter-entry alloc/copy/free | Q9, Q20 | subsumed by #1 |
| 5 | LOW-MED | `ForStRsMapStateV2.java` | per-record FFM on same (key,uk) | Q11, Q12 | conditional B-2 MapStateCache per tri-state trigger |

**Headline:** the audit produces **one single-commit fix** (violation #1: wire the already-built `frsVecIterPrefixNext` into `ForStRsDBIterRequest.process`) that should materially improve Q9 and Q20 without any new API or contract change. **This is the lowest-risk highest-impact fix on the audit** — the infrastructure to fix it exists, was tested, and was simply not connected to the hot path.

---

## Why violations #2 and #3 weren't surfaced as "vectorization violations" in earlier docs

Prior analysis (`2026-05-17-forst-rs-perf-recovery-analysis.md` + `2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md`) framed these as "FFM boundary cost" and "byte[] allocation" — phrased from a perf-cost angle. The user's reframing ("violation of end-to-end vectorization, no per-key operations") is **the same finding seen from the design-principle angle**: the design says "batch_get, no per-key operations" and the code does per-key operations both at the FFI layer (#2) and at the Java decode layer (#3).

The audit's value vs prior docs: explicit code-grep evidence + violation classification by which design contract is breached, not just by cost.

---

## Recommended action

Per the user's "audit-only" answer to the brainstorming question, this audit does NOT propose immediate implementation. The natural next step is to invoke the writing-plans skill to produce a precise implementation plan for **violation #1's fix** as the single best in-session candidate (Rust is unchanged; Java fix is bounded to one file; existing `frsVecIterPrefixNext` is already tested).

Violations #2, #3 are V1.1 P0 work per the locked Fix #1d / Fix #2 plan in `2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md`.

Violation #5 is conditional on the tri-state trigger after #1d ships.

---

## §A — Built-but-unwired sweep (next-step #2)

**Method:** grep all `pub extern fn frs_vec*` and `frs_vectorized*` exports in `crates/forst-rs-ffi/src/lib.rs`; cross-reference against the `downcallHandle("frs_*")` registrations in `ForStRsLinker.java`; for each match, check if a non-Linker caller in production code paths invokes it (vs only test code or unreachable code paths).

**Sweep result:**

| Rust export | Java linker method exists | Caller from production hot path | Status |
|---|---|---|---|
| `frs_vectorized_batch_get` | ✓ | `VectorizedExecutor.executeGets` | wired and used (carries violation #2 internally) |
| `frs_vectorized_batch_put` | ✓ | `VectorizedExecutor.executePuts` | wired and used (already uses WriteBatch — optimal) |
| `frs_vectorized_batch_delete` | ✓ | `VectorizedExecutor.executeDeletes` | wired and used |
| `frs_vec_iter_prefix_open` | ✓ | `VectorizedExecutor.dispatchIterPrefix` (Path B only) | wired but Flink-framework iteration uses Path A instead |
| `frs_vec_iter_prefix_next` | ✓ | `FrsIterHandle.next` (Path B only) | **CRITICAL — wired but unreachable from Flink-framework MAP_ITER requests** |
| `frs_vec_iter_prefix_close` | ✓ | `FrsIterHandle.close` (Path B only) | same Path B confinement |
| `frs_vec_iter_prefix_abort` | ✓ | `IterLifetimeWatchdog` | wired and used by watchdog |
| `frs_vec_iter_range_open/next/close/abort` | ✓ | `IterRangeRequest` (Path B only) | same Path B confinement |
| `frs_vec_merge_append` | ✓ | `AppendMergeBatchBuffer` | wired |
| `frs_abi_version` | ✓ | called at backend init | wired |

**Discovery — architectural "two iteration paths":**

The code maintains **two parallel iteration paths** that diverged during V1 development:

- **Path A (legacy):** `ForStRsDBIterRequest.process()` → `linker.prefixLookupOpen` → `linker.iteratorNext(iter)` loop. Used for all Flink-async-V2-framework-originated `MAP_ITER`/`MAP_ITER_KEY`/`MAP_ITER_VALUE` requests (the only iteration entry points Flink itself constructs).
- **Path B (vectorized):** `IterPrefixRequest extends VectorizedStateRequest` → `VectorizedExecutor.submitVectorized` → `linker.frsVecIterPrefixOpen` + `linker.frsVecIterPrefixNext` chunked. Only reachable from forst-rs code that explicitly builds an `IterPrefixRequest` (e.g., `submitVectorized()` call sites). **No state class currently does this — they all rely on Flink's framework, which routes through Path A.**

**Conclusion:** the vectorized iterator infrastructure was fully built and tested, but it is **unreachable from the Flink hot path** because state classes (`ForStRsMapStateV2.asyncEntries/asyncKeys/asyncValues`) inherit `AbstractMapState` defaults that construct `StateRequest(MAP_ITER)` objects, which the classifier routes to Path A. **Path B is dead code for production workloads** until either (a) `ForStRsDBIterRequest.process()` is rewritten to call `frsVecIterPrefixNext` directly (the fix proposed in violation #1), or (b) the state classes override `asyncEntries` etc. to construct `IterPrefixRequest` directly.

Option (a) — wire the chunked API into Path A — is the right fix because it preserves the existing Flink-framework integration contract while eliminating the per-entry FFM call.

**Proposed CI lint** (zero-runtime-cost preventive measure):

A small Python/bash script in `.github/workflows/`:

```bash
# scripts/check-built-but-unwired.sh
set -e
RUST_EXPORTS=$(grep -E 'pub.*extern.*fn frs_(vec|vectorized)' crates/forst-rs-ffi/src/lib.rs \
               | sed -E 's/.*fn (frs_[a-z_]+).*/\1/' | sort -u)
JAVA_REFS=$(grep -roE 'frs_(vec|vectorized)[a-z_]+' \
              flink-state-backends/flink-statebackend-forst-rs/src/main/java/ \
              | sort -u | cut -d: -f2)
# Compare and fail if any Rust export has no Java caller.
diff <(echo "$RUST_EXPORTS") <(echo "$JAVA_REFS") || exit 1
```

This wouldn't catch the Path A/Path B reachability issue (since `frsVecIterPrefixNext` IS called from `FrsIterHandle`), but it would catch the broader category of "added Rust FFI without wiring Java side."

**Stronger lint (catches the Path A/B case):** add an annotation marker comment in `ForStRsLinker.java` like `// @ProductionHotPath` next to wrappers expected to be called from hot paths, plus a script that asserts each such wrapper is invoked from at least one non-test, non-Linker class in `org.apache.flink.state.forstrs.*`. V1.1 follow-up.

---

## §B — Violation #5 trigger split by query shape (next-step #3)

The deep-analysis doc's tri-state trigger (Red/Yellow/Green at 0.7×/0.85×/up) treats Q11 and Q12 identically. They aren't:

- **Q12 (PROCTIME tumble per bidder count):** key = `(bidder, window_start, window_end)`. Window changes every 10 s of proc-time. **Each bidder typically appears in 1 active window at a time;** consecutive bids by the same bidder within 10s hit the same key. Cache hit rate ≈ 95 % if working set fits.

- **Q11 (SESSION window per bidder, 10s gap):** key = `(bidder, session_id)`. Session merges as new bids arrive within the gap; **state for a single bidder spans many records before a window closes** (sessions can be much longer than 10s on a hot bidder). Cache hit rate **structurally higher** than Q12 — same `(bidder, session)` is hit many times before the session closes.

**Revised tri-state trigger (split by query shape):**

| Query | Red (launch cache) | Yellow (retrospect-first) | Green (defer V1.2) |
|---|---|---|---|
| Q12 (TUMBLE/PROCTIME) | < 0.7× | 0.7× – 0.85× | ≥ 0.85× |
| Q11 (SESSION) | < 0.7× | 0.7× – 0.85× | **≥ 0.85× still launch — see note** |
| Q9 (ROW_NUMBER) | < 0.7× | 0.7× – 0.85× | ≥ 0.85× |
| Q20 (streaming join) | < 0.7× | 0.7× – 0.85× | ≥ 0.85× |

**Q11 note:** Q11's session-window state has intrinsically higher cache hit rate; MapStateCache ROI is structurally larger. **Q11's effective trigger threshold is `< 0.85×`** (i.e., yellow band shifts to green only if Q11 is at parity or above). Rationale: even at 0.80× rocksdb, Q11 has cheap headroom from cache that other queries don't.

This is recorded as a refinement to `2026-05-19-forst-rs-perf-bottleneck-deep-analysis.md` §5.

---

## §C — Q3 added to next audit's scope (next-step #5)

Q3 (stream-stream join: `auction JOIN bid ON A.id = B.auction`) has state shape **closest to the unaudited surface** in the V1 design — it uses MapState-based stream-stream join with multi-set semantics + watermark-driven cleanup. v3.2 measured Q3 at **1.09× rocksdb** (above gate) and **1.81× forst** — but the head-room above the gate is thin, and the join-state-cleanup path is exercised heavily.

**Hypothesis: Q3 may expose a "join-multiset-cleanup" violation class** that this audit didn't see because Q8/Q11/Q12 use windowed-aggregate (different state-cleanup contract) and Q9/Q20 use streaming-join (Q9 with TopN-style cleanup, Q20 with category-filter probe but no time-windowed cleanup).

**Q3 in scope for next audit:** look for per-record-tombstone-write patterns, watermark-driven prefix-delete paths, and any per-row state cleanup that bypasses `frs_vectorized_batch_delete`.

---

## §D — SQL-to-state-shape mapping template (next-step #4)

The audit's §"SQL → state-shape map" table is being lifted into `CONTRIBUTING.md` as the standard template for query characterization in any future perf work. Long-term goal: auto-generate from Flink logical plans via `EXPLAIN PLAN_WITH_STATE_RESOURCES`, then contribute the tooling upstream to Flink master so other state backends can use the same characterization framework.

See `CONTRIBUTING.md` §"Query characterization template" for the standard form.

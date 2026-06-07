# q15 ArrowBinaryBuffer 2 GiB fix (DONE) + FORSTRS-timer drain fix (PLAN)

**Date:** 2026-06-07

## Part 1 — q15 crash FIXED (verified, deployed)

### Root cause (definitive)
q15 `count(distinct)` GROUP BY day. The DataView **is** state-backed (proven: ttl=1h → `UnsupportedOperationException` on MapState `distinctAcc_0`), so distinct values are per-entry MapState — NOT a giant inline value. The crash was in the **accumulator ValueState** read:
`GroupAggFunction.processElement → ForStRsValueState.value():306-310 → BinaryRowDataSerializer:98
→ v1sync.MemorySegmentDataInputView.readInt underflow @ position 2147483520 (0x7FFFFF80)`.

`ArrowBinaryBuffer` (forst-rs off-heap statebuf) stores values in Arrow BinaryArray layout with
**int32 `valueOffsets`** (`appendValue` returns `int start = (int) valueDataUsed`), while
`valueDataUsed` is a `long` and grows **append-only on every overwrite** (old space reclaimed only
on flush). The flush gates (`needsFlush`, `shouldAutoFlush`) were **row-count only**. q15 has very
few group-keys (days) updated **per-record across ~92M bids** → few rows, but the appended value
bytes cross `Integer.MAX_VALUE` → the `(int)` offset cast wraps → corrupt `valueOffsets` → the read
frames a `MemorySegmentDataInputView` with a bogus offset/limit → underflow at ~2 GiB → crash-loop.

### Fix (one line, complete, in-scope)
`ArrowBinaryBuffer.shouldAutoFlush()` now also returns true when
`valueDataUsed >= VALUE_DATA_FLUSH_BYTES` (1 GiB, safely < 2 GiB int limit). The caller
(`ForStRsValueState`) already checks `shouldAutoFlush()` before every insert and flushes, so the
value-data region is bounded < 2 GiB → offsets stay int-representable. Append-only overwrite space
is reclaimed by the flush (resets `valueDataUsed=0`).

### Verification
- Build: `mvnw -pl flink-statebackend-forst-rs` (JDK25, -Denforcer.skip) BUILD SUCCESS; jar deployed.
- **q15: DNF crash-loop → FINISHED 147s, 0 RESTARTING, full 92M.** BEATS BOTH: rocksdb 266 (1.81×),
  forst 258 (1.76×).
- Regression: q12 = 34s (unchanged) — the new gate only fires at 1 GiB value-data, never for
  normal queries (they flush on row-count first).
- **Impact: q15 swings DNF → −119s vs rocksdb, flipping the local total** (forst-rs ~4003 vs
  rocksdb ~4039, ~36s ahead — thin, pending a clean full re-sweep to confirm).

## Part 2 — FORSTRS timer drain (PLAN; HEAP timer REJECTED by design)

HEAP timer (JVM-heap PQ) is **rejected** — it defeats disaggregated state (timers must live in the
engine). Backend stays on **FORSTRS** (engine-backed) timer. Must fix the FORSTRS timer instead.

### Problem
q11 (session window) FORSTRS ≈ 510s vs rocksdb 122s (≈4×). Two costs: (a) per-record timer
add/extend during ingest (engine writes — already batched via `pendingBuffer`/ArrowTimerBuffer),
(b) the **end-of-stream fire burst**: `poll()` (multi-kg path, line 902) does **one
`linker.deleteSegment` FFM crossing per fired timer** — millions of crossings in the watermark
drain. There is ADD batching (`pendingBuffer`) but **no DELETE batching**.

### Fix design (deferred batch-delete, mirrors the ADD path)
1. Add a `pendingDeletes` off-heap Arrow-layout batch (keyOffsets + keyData) + a
   `flushPendingDeletes()` that issues ONE `linker.vectorizedBatchDelete(db,cf,offsets,data,count)`.
2. In `poll()` engine-head branch: stage `engineHead.composite` into `pendingDeletes` instead of
   `deleteSegment`; then `consumeMultiKgHead`. The resume-cursor already skips fired entries within
   a drain window, so deferral is compatible.
3. **Correctness invariant (critical):** flush `pendingDeletes` BEFORE any engine re-scan, i.e. at
   the start of `flushPendingToEngine()`/`drainPendingBufferInternal()` (the pre-snapshot + pre-close
   hook) AND on every cache-invalidating refill/cursor-reset (`refillMultiKgCache`,
   `invalidateCache`, `invalidateMultiKgCache`). Missing a flush point → a re-scanned fired timer →
   re-fired timer → wrong results. Cap `pendingDeletes` (e.g. 4096) → flush when full.
4. Verify: q11 perf (target ≪510s), AND correctness/perf no-regression on q5/q7/q12 (all timer
   users), AND assert no re-fired timers (row-count parity vs rocksdb).

This is a delicate correctness-critical change to a 2346-line file; implement + verify as one
complete step before integration.

### IMPLEMENTED + VERIFIED CORRECT, but PERF-NEUTRAL on q11 (2026-06-07)
Deferred batch-delete landed: `pendingPollDeletes` staged in `poll()` multi-kg path, flushed via
`vectorizedBatchDeleteKeys` in `refillMultiKgCache` / `drainPendingBufferInternal` /
`invalidateCache` / `invalidateMultiKgCache`. Build SUCCESS; **36 timer unit tests pass**
(`...PriorityQueueBatchedTest` 9, `MultiKeygroupTimerFireTest` 11 — the exact multi-kg drain path) —
**no re-fired/lost timers**. q11 FINISHES correctly (92M).
**BUT q11 = 543s ≈ unchanged (was 508s) — the drain (~240s at rate=0) did NOT shrink.** So the
per-poll `deleteSegment` crossings were NOT the drain bottleneck (hypothesis REFUTED). Rate curve:
ingest ~261s + drain ~240s, same as baseline. The drain cost is the **per-fired-timer state access**
(each session timer firing reads its accumulator over FFM+engine) + the per-poll multi-kg
refill/min-selection machinery — i.e. the q4-class diffuse per-op efficiency gap, NOT deletes.
Kept the change (correct, tested, fewer FFM crossings = better for disagg).

### DATA-BACKED ROOT CAUSE of the q11 drain (2026-06-07, in-code diag)
Added env/property-gated diag (`-Dforst.rs.timer.diag=1`; `TIMER_DIAG`) counting polls/refills +
timing `readRangeIntoCache`. jstack attach is unreliable on this macOS/JDK25 box (3 attempts hung);
the in-code counters worked. Measured during q11 drain:
```
polls=100000 refills=4700 refillMs=111963   (≈112 s in refills at just 100K polls)
polls=200000 refills=4843 refillMs=114855
polls=300000 refills=4989 refillMs=117876
polls=400000 refills=5174 refillMs=171129   (late refills ≈287 ms each vs ≈24 ms early)
```
**The drain IS `refillMs` (`readRangeIntoCache` engine re-scans), NOT deletes nor onTimer state.**
Two compounding bugs in the multi-kg refill path:
1. **Refills ~50× too frequent**: 4700 refills / 100K polls = a refill every ~21 polls (should be
   ~1 per REFILL_BATCH=1024). The multi-kg pollCache exhausts almost immediately → constant
   re-scanning. Suspect: `readRangeIntoCache` returns far fewer than 1024 per refill, or the
   per-kg floor/resume-cursor logic forces tiny refills. INVESTIGATE `readRangeIntoCache` +
   `findGlobalEngineHeadInRangeCached` refill triggering.
2. **Per-refill cost grows** (24 ms→287 ms): the range re-scan walks accumulating fired-timer
   tombstones. A range/prefix delete (one range tombstone) or compaction-on-drain would avoid the
   O(tombstones) walk.
**FIX (next, scoped):** make each multi-kg refill actually fill ~REFILL_BATCH live entries (cut
refill frequency ~50×) and avoid the tombstone walk (range-delete the consumed prefix or seek past
tombstones). Target: drain 240 s → tens of s, q11 toward rocksdb 122 s. Verify q5/q7/q12 + row
parity. The diag (`-Dforst.rs.timer.diag=1`) is retained (off by default) for the next session.
HEAP timer remains rejected (disaggregation).

### UNIFIED ROOT CAUSE + next-session infra (2026-06-07 final)
Measured: q11 drain = `readRangeIntoCache` refillMs (171s, per-refill cost grows 24→414ms). A/B
PROVEN it is scan READ-AMP on growing state, NOT delete-tombstones: deleting consumed entries HELPS
(refill-flush 171s < defer-all 246s — undeleted entries get re-walked). SAME mechanism gates
q4/q9/q19/q20 (join/Top-N scans) → **ONE read-path fix lifts all five**. The two viable fixes both
need NEW infra (confirmed absent today — no `deleteRange`/`prefixDelete` in linker, FFI, or engine):
1. **Add `deleteRange(cf, lo, hi)`** (engine `db.rs` + FFI `lib.rs` + Java `ForStRsLinker`), then
   range-delete the consumed timer prefix per drain window — shrinks the live set with ONE range
   tombstone instead of N point tombstones (no SST/seek growth). Also generally useful for state
   clear/TTL.
2. **Persistent drain iterator** (Java timer-queue): keep ONE engine range iterator open across the
   drain (read sequentially via `frsVecIterRangeNext`) instead of 5300 `frsVecIterRangeOpen`
   re-seeks; bulk-delete (or range-delete) the fired prefix at drain end / snapshot.
Either is multi-layer/multi-session; verify with the diag (refillMs should collapse) + q5/q7/q12
row parity. This is THE single lever gating the local total-win.

### ★ CONCRETE q11 BUG FOUND (2026-06-07 diag, entriesPerRefill counter) — NOT diffuse
Diag at polls=500K: `refills=5291 refillMs=171s entriesPerRefill=993` → **5291 × 993 ≈ 5.25M timer-CF
entries READ for only 500K polls = ~10× READ AMPLIFICATION.** Each refill fills the cache with the
full ~1024 (993) live entries, but the cache is **discarded with ~900 entries unconsumed ~5300
times** — the resume cursor (`multiKgResumeCursor`, set to `last` after each refill at line ~1069)
is being CLEARED by over-frequent `invalidateCache()`/`invalidateMultiKgCache()`, so the next refill
restarts from `floor`/`kgPrefix` and re-reads overlapping ranges. So refillMs (the q11 drain wall)
is ~10× inflated by re-reads, NOT diffuse per-byte cost. **NEXT (concrete, targeted): instrument a
counter on invalidateCache/invalidateMultiKgCache to find WHICH caller clears the cursor ~5300×
during ingest+drain (candidates: poll() line 888, peek paths 1607/1642/1654, drain trailing 1757,
advance 2104), then stop clearing the resume cursor on that path (the cursor stays valid across that
event since deletes are flushed). Expected: refills 5291→~500, refillMs 171s→~17s, q11 529→~360s.**
This reframes q11 from "diffuse, no lever" to a fixable cache-invalidation bug — likely the single
biggest #2 lever, and may share a cause with q9/q19/q20 (same cache/cursor machinery).

### REFINED: 10× = session-merge churn tombstones (not cursor-clearing) + the fix API exists
The invalidateCache/invalidateMultiKgCache callers are all RARE (size/iterator/getSubset/advance-
fallback/poll-exception) — NOT a per-poll hot path. So the ~5.25M reads / 500K polls (10×) is
**session-merge CHURN**: q11 SESSION windows merge on close bids → each merge DELETEs the old timer
+ ADDs a new one, so the timer CF accumulates ~5M timer keys (~10× the ~500K final sessions) as
superseded/tombstoned entries; the drain range scans walk all of them (uncompacted) = the read-amp.
**FIX MECHANISM IDENTIFIED + IMPLEMENTATION-READY:** the engine already has
`frs_compact_cf(db,cf)` / `frs_compact_all` (FFI lib.rs:1917/1934) and engine `compact_once_for`
/`compact_l0`/`compact_all` (db.rs:3684+), but the **Java `ForStRsLinker` does NOT expose
`frs_compact_cf` yet** (only `setCompactionFilterTtl`). NEXT (concrete, one-step): (1) add the
`frsCompactCf` MethodHandle + `compactCf(db,cf)` to `ForStRsLinker`; (2) have the timer queue call
it on the snapshot pre-hook / periodically (every N flushes) to drop churn tombstones; verify q11
drain collapses (diag refillMs) + no ingest regression (q4 lesson: compaction can steal foreground
CPU — gate it to low-write phases / bound it). Generalizes to q9/q19/q20 (same uncompacted-state
read-amp). Risk: over-compaction hurts ingest — measure both phases. This is the single biggest #2
lever and is now implementation-ready.

### ★★ STRUCTURAL ROOT (2026-06-07): timers live in the SHARED defaultCf
`ForStRsAsyncKeyedStateBackend` line ~1703 constructs the timer queue with **`defaultCf`** — the SAME
column family as ALL window/join/agg state. So: (a) timer range scans read-amp over SSTs interleaved
with non-timer state (bigger SSTs, more to merge per scan); (b) `frs_compact_cf(defaultCf)` compacts
the ENTIRE state (expensive — the risk noted above is real); the churn tombstones sit among all
state. **PROPER FIX = a DEDICATED timer column family:** route timer reads/writes to `timerCf`, so
timer scans touch only small timer-only SSTs AND a targeted `compact_cf(timerCf)` is cheap (drops the
~5M churn tombstones without touching window/join state). This isolates the q11 read-amp at the
source and makes the compaction trigger safe. Caveat: it's a structural change with
checkpoint/restore-format implications (new CF in the snapshot manifest) — must preserve
accuracy/restore + key-group isolation; genuinely multi-session, complete-in-one-step + verified.
Likely generalizes: giving heavy operators (join/Top-N) their own CFs isolates q9/q19/q20 read-amp
too. This is the precise structural lever for the local total-win.

**REFINEMENT (which option first):** Option 1 (`deleteRange`) is NOT a small API add — a correct
LSM range-delete needs full RANGE-TOMBSTONE support (memtable range-del map + SST range-del block +
compaction handling + read-path range-del checks); that is a large engine feature on its own.
**Prefer Option 2 (persistent drain iterator) — Java-only, no engine feature required, and it
directly removes the measured cost (5300 `frsVecIterRangeOpen` re-seeks, each re-seeking across the
timer-CF L0 SSTs).** Sketch: when a watermark drain starts, open ONE range iterator per kg over
`[resumeCursor, prefixEnd)` and serve poll()'s cache refills from `frsVecIterRangeNext` on that SAME
handle (no re-open) until exhausted; keep the batched point-delete of consumed keys (it shrinks the
live set and is A/B-better than deferring); close the iterators on drain end / snapshot / cursor
reset. Risk: the engine range iterator is a point-in-time view — fired entries deleted during the
drain are still yielded by the open iterator (correct: we WANT to fire them), and the cache/cursor
bookkeeping must stay consistent with the long-lived handle. Verify with `-Dforst.rs.timer.diag=1`
(refillMs must collapse) + q5/q7/q11/q12 row-count parity vs rocksdb.

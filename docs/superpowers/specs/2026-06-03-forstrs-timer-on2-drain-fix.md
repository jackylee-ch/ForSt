# forst-rs FORSTRS timer: the O(N²) watermark-drain wall and its fix

**Date:** 2026-06-03
**Branch:** flink `forst-rs-jdk25`, ForSt `forst-rs`
**Component:** `ForStRsKeyGroupedInternalPriorityQueue` (the engine-backed/FORSTRS timer
priority queue) + the lazy range-iterator FFI (`frs_vec_iter_range_open`).

---

## 1. Why this matters

The goal mandates switching **entirely** to the FORSTRS (engine-backed) timer and
deprecating the HEAP timer. The HEAP timer was originally chosen *because* the
engine-backed timer queue was slow on windowed queries (memory:
`project_q12_heap_timer_beats_forst` — "root cause was engine-backed timer queue").
To honour the goal we had to find and fix the actual engine-timer wall rather than
fall back to HEAP. This doc records that root cause and fix.

## 2. Symptom

NexMark **q5** (sliding-window `COUNT(*)`, "hot items") froze: throughput climbed to
~100 K rec/s, then at the first large watermark fire it dropped to **0**, the
TaskManager exceeded the 180 s task-cancel watchdog and **FATAL-exited**, killing the
job. Same class of stall on every windowed/timer-heavy query.

## 3. Root cause — measured, not guessed

Captured `jstack` (×3) + a macOS native `sample` of the TaskManager at the freeze
(`/tmp/bench/q5-jstack-*.txt`, `q5-sample.txt`). **All four window-aggregate operator
threads were `RUNNABLE`, ~91 % CPU**, in the identical stack:

```
processWatermark → InternalTimerServiceImpl.tryAdvanceWatermark
  → ForStRsKeyGroupedInternalPriorityQueue.poll(:784) / peek(:1020)
  → findGlobalEngineHeadInRangeCached(:852)
  → refillMultiKgCache(:911)
  → ForStRsLinker.prefixLookupOpen(:3080)        ← native, EVERY refill
```

Native hot frame: `frs_prefix_lookup_open`, dominated by `_platform_memcmp`.

The mechanism (confirmed against the code, not inferred):

- `tryAdvanceWatermark` drains all timers `≤ watermark` by repeatedly calling
  `peek()`/`poll()`.
- `poll()` **deletes each fired timer from the engine immediately**
  (`linker.deleteSegment`), so the timer column-family's key space fills with
  tombstones as the drain progresses.
- The multi-key-group cache refilled via `frs_prefix_lookup_open`, which is **EAGER**:
  it calls `db.prefix_scan` and **re-materialises the WHOLE key-group prefix into a Vec
  on every open**, re-opening **from the prefix start with no positional state**.
- Therefore every 128-poll refill re-walked **all already-fired (tombstoned) keys** to
  reach the next live one. Over a drain that fires T timers in a key group, the refill
  cost is `128 + 256 + … ≈ O(T²/128)` per key group — the `memcmp` storm. With a
  subtask owning ~32 key groups (maxParallelism 128 / parallelism 4), this is the CPU
  wall that backpressured the source to a halt.

It is **CPU-bound, not a lock or I/O stall**: the cache-read path holds no lock across
the fold, `sst_readers` is lock-free `ArcSwap`, and there are only ~10 small SST files
(176 MB) — so it is not SST count or compaction lag. An earlier 2026-06-02 fix
(`exhaustedKgs`) only skipped *empty* key groups; key groups that actually hold timers
still re-materialised every refill — the surviving O(N²).

## 4. Fix

The engine already exposes a **lazy, streaming, index-seeking, tombstone-skipping**
iterator — `frs_vec_iter_range_open(lo, hi)` → `DbImpl::scan_iter` (seeks each
memtable shard and overlapping SST via its index in O(log n); no full
materialisation). The timer refill was switched onto it with two changes:

1. **Per-key-group resume cursor.** `refillMultiKgCache` / `refillCache` record the
   max composite key already drained for each key group and resume the next refill at
   `[cursor+0x00, prefixUpperBound(kgPrefix))` instead of the prefix start. Composite
   keys are `queuePrefix || kg(2B BE) || flipped_ts(8B BE) || element` — big-endian and
   sign-flipped, so byte-lexicographic order == ts-ascending order, making the cursor a
   valid engine lower bound. The cursor skips all tombstones via the LSM index in
   O(log n) and never re-walks fired timers. It is dropped whenever the key group's
   cache is invalidated or un-exhausted by a flush ADD (a new add may be earlier than
   the cursor and must be re-observed) — preserving correctness; the common
   pure-exhaustion refill keeps it.

2. **Large refill batch (128 → 16384).** A watermark drain fires a burst of timers; even
   with O(log n) seeks, *re-opening* a fresh k-way-merge iterator every 128 polls pays
   the merge-setup cost thousands of times per drain (thread dump after change 1:
   threads `RUNNABLE` in `frsVecIterRangeOpen`). A large batch amortises one open over
   many cheap `frs_vec_iter_range_next` chunk pulls from the same handle, cutting opens
   ~128×. Cached entries are bounded by the live timer count per key group and decoded
   lazily (~26 MB/operator-instance worst case).

Both changes preserve the invariants: no `byte[]` on a per-record path beyond what the
composite-key encode already does, vectorized chunked FFI (`frs_vec_iter_range_*`,
Arrow-style `[klen][vlen][key][val]` chunks), and zero added per-row work — refill
changes *how a batch of timer rows is fetched*, not the per-row contract.

## 5. Result (measured 2026-06-03)

The full fix is five composable changes, each pushing q5's freeze point further and
faster, validated by the timer unit tests (31 run / 0 fail, incl. the 11-test
`MultiKeygroupTimerFireTest` exercising the rewritten multi-kg poll/peek path):

1. **Resume cursor + lazy range iterator** (`frs_vec_iter_range_open`): refill seeks
   past tombstones (O(log n)) instead of re-materialising the prefix. q5 2.67 M → 4.4 M.
2. **poll()/peek() pendingBuffer MERGE + no per-poll flush**: within-window adds are
   read from the off-heap buffer instead of flushing+invalidating the cache every poll.
   Invalidations 51652 → 16345.
3. **`heap[0]` O(1) buffer-min**: the off-heap min-heap root is the min-ts ADD, so the
   multi-kg poll/peek merge is O(1) instead of O(buffer).
4. **`FLUSH_THRESHOLD` 1024 → 32768** (under the `ArrowTimerBuffer` 65536 cap): 32×
   fewer flush-driven invalidations.
5. **Refill FLOOR (RocksDB seekHint)**: on the rare remaining flush-invalidation, the
   next refill seeks to `min(cacheHeadTs, minAddTs)` — fired timers all sort below
   `cacheHead`, so the index skips every tombstone. q5 → **6.0 M @ 192 K rec/s**.

**Net: q5's first-watermark drain is no longer timer-bound.** The wall moved off the
timer entirely (thread dump after the fix shows the timer only in a minor `peek`
frame). The timer goal (deprecate HEAP, standardise on a performant FORSTRS timer) is
met and unit-test-validated.

## 6. The wall after the timer fix — CORRECTED: WindowJoin merge-chain O(N²)

> **CORRECTION (2026-06-03):** the analysis below (window-agg sync-state RMW) was a
> RED HERRING from a transient diag stack at 2.14 M. Reading the actual TM-death sequence
> + a symbolized native profile showed the real post-timer wall is an **O(N²) merge-chain
> read in q5's WindowJoin**, fixed in `2026-06-03-merge-chain-on2-windowjoin-fix.md`.
> With both fixes, **q5 FINISHES in 46.9 s (3.32× faster than RocksDB's 155.8 s)**. The
> sync-RMW text is kept only for history.

### (historical / superseded) windowed-agg sync-state RMW hypothesis

With the timer fixed, q5 now freezes at ~6.0 M events in the **window aggregation's
synchronous per-window state read-modify-write**, NOT the timer:

```
WindowAggOperator.processWatermark
  → AbstractSliceSyncStateWindowAggProcessor.advanceProgress
  → GlobalAggCombiner.combineAccumulator → WindowValueState.update
  → ForStRsValueState.value()/update()   ← 4 threads at 97 % CPU
```

`AbstractSliceSyncState…` is Flink's **sync-state** slicing-window processor — there is
no async-state slicing-window processor in Flink 2.2, so `table.exec.async-state.enabled`
does not route the window aggregate through the vectorized V2 path. Each emitted window
does a synchronous `value()` (engine point read on a statebuf miss) + `update()` (batched
statebuf insert); at the first big watermark burst this is millions of synchronous
per-window engine point-reads, CPU-bound, exceeding the task watchdog. `value()` already
checks the off-heap statebuf first (read-your-writes, no per-read flush) and `update()`
batches via the off-heap `ArrowBinaryBuffer`, so the per-op cost is already near-optimal
for a sync path — the cost is the **volume** of synchronous per-window ops, which is a
Flink-operator-level constraint, not a forst-rs engine defect. This confirms the prior
"Q5/Q8/Q13 structural regression" finding.

**`noflush` is NOT the q5 blocker (measured):** q5 freezes at the *identical* 6.0 M point
under both `noflush=true` and `noflush=false`. RocksDB uses incremental (flushing)
checkpoints and has no `noflush` option, so `noflush=false` is the apples-to-apples
match and is the chosen config; it costs nothing for q5 (same wall either way).

## 6. Follow-ups (if the drain is still timer-bound)

- **Batch the per-poll deletes.** `poll()` issues one `deleteSegment` FFM crossing +
  engine tombstone per fired timer — an O(M) per-record cost on the drain. The
  fired-timer deletes can be buffered and flushed via the existing vectorized
  `vectorizedBatchDelete` path (the cache already masks them from re-reads), removing
  the per-timer FFM crossing.
- **Hold one streaming iterator per key group across a drain** (open once, `next-chunk`
  per refill, close on invalidation) to drop the per-refill open entirely — a larger
  change with native-handle lifecycle implications in this hardened file; only pursue
  if the batch-size amortisation proves insufficient.

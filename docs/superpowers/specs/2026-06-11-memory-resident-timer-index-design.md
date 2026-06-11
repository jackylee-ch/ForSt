# Memory-Resident Timer Index (engine as durable log) — Design

**Date:** 2026-06-11 · **Status:** user-approved direction ("fix with architecture design,
not sub class or path fix") · **Repo:** flink-statebackend-forst-rs only.

## 1. Problem (three defect generations from one architecture)
ForStRsKeyGroupedInternalPriorityQueue is a CACHE-OVER-ENGINE design: poll caches + resume
cursors + refill floors + pending buffers over engine-resident timer rows. This produced:
(a) the O(N²) watermark drain (2026-06-03), (b) the refill-floor overwrite correctness bug
(2026-06-11: silently dropped registered timers, up to −60% q8 windows), (c) the fix's I/O
tax (floors honored ⇒ cold engine re-reads ⇒ q17 +300%, q20 +37%, q9 +16%; profile-proven
I/O-wait: TM at 1.7/8 cores during the slowdown, no CPU hotspot).

## 2. The architectural insight
`decodeElement(composite)` — a timer's element is FULLY derivable from its key bytes.
Therefore the engine is never needed to FIRE a timer; it is only needed for durability and
snapshot/restore. The read path can be deleted entirely.

## 3. Design
- **Authoritative live-timer index, off-heap, per queue instance**: a ts-major min-heap of
  composites (Arrow layout — extend/reuse ArrowTimerBuffer's off-heap rows + min-heap +
  tombstone machinery; zero-copy, no per-row heap objects). Holds EVERY live timer with
  `ts < spillHorizon` (normally: all of them).
- **add()**: insert into the index AND stage the engine write exactly as today
  (pendingBuffer → vectorized batch flush). Engine semantics unchanged.
- **remove()**: delete from the index (hash-locate by composite; tombstone slot) AND stage
  the engine delete as today.
- **peek()/poll()**: heap top — pure memory, ZERO engine reads. poll() stages the fired
  timer's engine delete via the existing pendingPollDeletes batch path.
- **Snapshot (Trace E)**: unchanged — flush pending writes; the engine is always a correct
  durable superset (fired timers' deletes flush before snapshot, as today).
- **Restore/open**: ONE bulk kg-range scan of the engine prefix to rebuild the index
  (sequential, once).
- **Spill safety valve (pathological cardinality only)**: `FRS_TIMER_INDEX_MAX` (default
  8M entries ≈ ~400MB). Above the cap, timers with the LARGEST ts spill to engine-only and
  `spillHorizon` = the index's max retained ts. When the watermark approaches the horizon,
  advance it: one sequential range scan [oldHorizon, newHorizon) bulk-loads the next band.
  The horizon is the ONLY cursor and it is MONOTONE — no per-kg cursors, no floors, no
  invalidation, no orphaned spans by construction.
- **DELETED** (the entire fragile layer): multiKgPollCache, multiKgResumeCursor,
  multiKgRefillFloor, exhaustedKgs, cachedHeadEntry/pollCache/cachedKg,
  refillMultiKgCache/refillCache/readRangeIntoCache poll-path use, the floor/merge logic
  in drainPendingBufferInternal's invalidation loop (the flush keeps only: pass A/B engine
  writes + un-spill-horizon accounting).

## 4. Why this beats the alternatives
- vs RocksDB's RocksDBCachingPriorityQueueSet: same cache-over-store fragility we are
  deleting; ours becomes zero-read-amp — structurally faster on every timer-heavy query.
- Memory: live timers × ~50-60B off-heap. NEXMark loads: low-millions live worst case
  (≈100-400MB transiently); the cap + horizon covers pathological jobs.
- The mandate's constraints hold: off-heap Arrow layout, batch-only engine I/O (the
  existing vectorized flush/delete paths), no per-record engine crossings (poll = memory).

## 5. Gates (before/after, all on 8c/32g)
1. All timer suites (MultiKeygroupTimerFireTest 13 incl. both 2026-06-11 regressions,
   Batched 9, ArrowTimerBuffer 5, + new: spill-horizon advance, restore-rebuild, cap-spill).
2. q8@100M ×3 exact (canary) + 10M exactness sweep.
3. THE TAX MEASUREMENT: q17@100M ×3 — must return to the fast class (the +300% dies);
   q9/q20 re-measure with same-hour RDB pins toward the ≤1.05× target.
4. Full UT suite, GHA both repos.

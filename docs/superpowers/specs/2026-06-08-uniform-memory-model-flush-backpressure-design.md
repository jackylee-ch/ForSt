# Uniform Memory Model: ForSt-style Flush + True Backpressure (Phase-1 OOM close)

**Date:** 2026-06-08
**Status:** Design (implementation pending; multi-module, must land + verify together)
**Goal:** Make the unbounded-state NexMark queries (q4/q9/q17/q18/q20) FINISH on 8c/32g under
the 32 GB cgroup with a SINGLE uniform backend config (no per-query tuning), by replicating
ForSt/RocksDB's memory model — flush-on-checkpoint + true write backpressure + flush that keeps
pace — instead of forst-rs's current advisory-only flush.

## Problem (evidence-based, 2026-06-08)

The OOM set OOMs because **memtables grow unbounded**. Precise q9 mem-diag at OOM:
`rss 31572 MB, jemalloc alloc 19314, wbm_memtable 7221, resident_shadow 0`. The shadow is
already capped (0 here); jemalloc retained is transient (eager-return tested, still OOM);
block-cache accounting is correct. The driver is the memtable working set + flush not draining it.

Refuted levers (all with 8c/32g data): resident-shadow retire (shadow=0); jemalloc eager-return
(retained 7.9→3 GB but RSS still 31.5 GB); decoded-block-cache balloon (charge() correct);
compaction max-background 8→3 (still OOM 30.7 GB); WBM hard-cap stall at 5 GB (froze 80–200s then
the 120s backstop released → overran to 7.2 GB → OOM).

### Root cause (code-grounded)

1. **No backpressure by default.** `DbImpl::wait_for_wbm_headroom()` (db.rs:2655) stalls ONLY when
   `runtime_tuning::over_global_hard_budget()` is true, which requires `FRS_WBM_HARD_MB > 0`
   (runtime_tuning.rs) — **default 0 = disabled**. So the write path (db.rs:2634–2641) only
   *triggers* an advisory flush on `over_budget()`; it never waits. Writes outpace flush →
   memtables grow to 7–11 GB → OOM.
2. **Give-up backstop.** Even when the hard cap is set, the stall loop breaks after a 120s
   deadline (db.rs:2669–2673) and proceeds, so memtables overrun anyway. RocksDB's
   `WriteBufferManager` stall has **no give-up**.
3. **Flush doesn't keep pace.** `noflush=true` (8c/32g template leftover) meant checkpoints never
   flushed; even with `noflush=false`, write-heavy q17 overran to 11 GB — flush can't drain fast
   enough, and on 8 compaction + 4 flush threads over 8 cores, flush is starved by compaction.

### What ForSt/RocksDB do (the model to match)

- **Flush on checkpoint.** ForStNativeFullSnapshotStrategy.java:162 "get live files with flush
  memtable"; ForStIncrementalSnapshotStrategy → getLiveFiles (RocksDB default flush_memtable=true).
  ⇒ **ForSt ≡ noflush=false.**
- **True WBM stall** (`allow_stall`, no give-up): writes block until flush drains memtables under
  budget; memtables are a HARD bound.
- **Fast, prioritized flush**: flush outranks compaction so the stall stays brief, not frozen.

## Design — uniform, applied to ALL queries

### Config (already set)
`config-forst-rs-local.yaml.tpl` env.java.opts.taskmanager: `checkpoint.noflush=false` (uniform,
matches ForSt). This is the flush-bounding mechanism, not a per-query knob.

### Module 1 — True write backpressure (db.rs / runtime_tuning.rs)
- Stall on the **actual WBM budget** (`write_buffer_manager.over_budget()` — the configured
  capacity), NOT a separate default-disabled hard cap. The configured `manager.capacity`
  (currently 4096 MB) becomes the real ceiling.
- **Remove the 120s give-up.** Replace with a true wait-until-under-budget loop, with only an
  extreme deadlock-defense deadline (e.g. ≥ the checkpoint interval, and only if NO flush is
  making progress — detect via memtable-bytes monotonically not decreasing).
- Stall must hold no lock (already true: wbm_guard committed, write_mutex released) — preserve.
- Keep `FRS_WBM_STALL=0` escape hatch for A/B.

### Module 2 — Flush keeps pace (flush.rs / bg_pool.rs / db.rs enqueue_flush)
- **Prioritize flush over compaction** on the shared bg pool: flush is urgent (frees memtable RAM
  + unblocks stalled writers); compaction is throughput. On 8 cores, reserve flush capacity so a
  compaction storm cannot starve flush (the q17 failure mode).
- **Flush the over-budget CFs first / largest-memtable-first** when `over_budget()` trips
  (db.rs:2635 currently enqueues only the writing CF) — drain the biggest contributors.
- Ensure enqueue is idempotent and concurrent flushes across CFs proceed in parallel up to the
  flush thread budget.

These two MUST land together: Module 1 without Module 2 freezes the pipeline (proven by the
q9wbm 5 GB test: stall + slow flush = 80–200s freeze). Module 2 without Module 1 = no hard bound.

## Accuracy + Performance verification (8c/32g, per backend+timer, no shortcuts)
- Unit: WBM stall releases exactly when memtable bytes drop under budget; no give-up under
  sustained flush progress; flush-priority ordering picks largest CF.
- e2e 8c/32g, uniform config: q4/q9/q17/q18/q20 FINISH under 32 GB; out_rows == RocksDB
  (seeded datagen) for size-deterministic queries.
- Regression gate: q16/q19/q7/q11/q12 + light queries must NOT regress vs the noflush=true
  baseline beyond the per-query bar (≥0.8× RocksDB OR ≤+50s, AND faster than ForSt).
- Capture per-query forst-rs vs RocksDB(8c/32g) vs ForSt(8c/32g) time + out_rows.

## Out of scope (next lever, separate work)
Heavy-join **read-amp/compaction throughput collapse** (q9 rate 90→17 K/s as state grows) — the
read-path architecture (parallel/coalesced reads, value-carrying scans, compaction efficiency).
This design only bounds MEMORY uniformly so the OOM set FINISHES; closing the per-query SPEED bar
on heavy joins is the subsequent module. See
[2026-06-08-forst-rs-parallel-coalesced-readpath-design.md].

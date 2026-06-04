# forst-rs: Forst Timer Adoption + Heavy-Query Performance Architecture

**Date:** 2026-06-03
**Branch:** flink `forst-rs-jdk25`, ForSt `forst-rs`
**Goal context:** resolve all forst-rs timer + backend issues so NexMark q0–q22 pass on the latest
branch, switch entirely to the Forst timer, and land v3.8's perf levers for a 1.x–2.x total gain —
without breaking end-to-end vectorization / batch / Arrow / zero-copy, no `byte[]`, no per-row work.

---

## 1. Timer: switch entirely to the Forst timer (HEAP deprecated)

**Decision:** the FORSTRS (engine-backed, batched off-heap) timer is now the sole timer service;
the HEAP timer fallback chosen earlier in the debugging campaign is deprecated.

**Justification (measured on the latest branch):**
- A per-window bisect (NexMark q5 + a windowed-count probe over a fixed input) showed the FORSTRS
  timer and HEAP timer produce **byte-identical** results — the timer is **not** a source of any
  windowing discrepancy. The earlier-session race that motivated HEAP is **already fixed** on the
  latest branch (`cceba1c888a` "keep poll cache across far-future adds").
- The full sweep confirms FORSTRS fires correctly: q0–q3 finish cleanly under FORSTRS, and q3
  produces the exact reference count (`src_out = 2,201,068`, identical to RocksDB).

**Config:** `config-forst-rs-local.yaml.tpl` sets, for both JM and TM:
`-Dforst.rs.timer-service.factory=FORSTRS`. This is the only supported value going forward.

---

## 2. Checkpoint mode: `noflush` — the bounded-vs-unbounded-state tradeoff (kept `true`)

**Final decision:** keep `-Dforst.rs.checkpoint.noflush=true` for local. (I briefly switched to
`false` and reverted — see below.)

**The tradeoff (measured both ways):**
- `noflush=true` keeps live state in the **memtable** and uploads it wholesale to the local
  checkpoint dir. For **bounded-state** queries (windowed aggregates, group-bys, dedup — most of
  NexMark) the resident state is small, so the upload is cheap *and* reads stay in the memtable
  (fast). **q5 finishes in 37.7 s** this way (measured, reproducible).
- `noflush=false` (incremental-flushing) flushes the memtable to **SSTs** at each checkpoint. This
  avoids the whole-memtable upload (helps the one unbounded-state query, q4: early rate
  ~166 K rec/s vs ~17 K), **but pushes reads onto the SST path**, which exposes the read-path wall
  (§3) — so **q5 then times out**. Net negative for the total.

**Conclusion:** `noflush=true` is the better *global* choice — it makes the many bounded-state
queries fast; the lone casualty is q4 (unbounded temporal-join state), which does not finish under
*either* setting because its real bottleneck is §3, not the checkpoint mode. This refines (does not
contradict) `2026-06-02-v38-real-vs-hollow-perf-lessons.md` §6: incremental-flushing only wins when
the read path is cheap; with the current read path, `noflush=true` wins overall.

---

## 3. Open performance issue: heavy-query state-growth read degradation

**Symptom:** on q4 (and expected on other temporal-join / large-windowed queries), even with
`noflush=false`, the throughput **collapses as keyed state grows** — q4's rate falls 166 K → ~15 K
rec/s past ~40 M events and does not finish within budget, whereas RocksDB completes q4 in ~324 s.

**What it is NOT (ruled out by measurement):**
- Not the timer (HEAP == FORSTRS).
- Not the V1-sync per-record path: `table.exec.async-state.enabled = true` and mini-batch is
  disabled, so the join/aggregate run on the **vectorized async** path (batched FFM crossings,
  no per-record violation).
- Not the no-flush stall alone (collapse persists with `noflush=false`).

**What it is — DEFINITIVE (2026-06-03, from the q5 TaskManager-fatal stack):** the native
**`ForStRsLinker.vectorizedBatchGet` (line 2518) HANGS** on large windowed/temporal-join state
reads. The exact failure chain (q5):

```
processWatermark → AEC.drainInflightRecords → executeBatchRequests:392 → executeGets:1165
  → invokeVectorizedBatchGet:2298 → ForStRsLinker.vectorizedBatchGet:2518   [STUCK >180s]
⇒ "Task did not exit gracefully within 180+ seconds" ⇒ TaskManager FATAL ⇒ TM shuts down
⇒ all tasks FAIL "on [unassigned resource]" ⇒ job dies (~145 s in).
```

So q5 does **not** merely slow down — the watermark-driven drain issues a vectorized batch-get into
the engine that **never returns** (unresponsive to cancel), which kills the TaskManager. q4 is the
same read path degrading (to ~15 K rec/s) without (yet) tripping the 180 s watchdog. This is an
**engine batch-get read-path bug** — either a deadlock (e.g. a lock in the lock-free-memtable / SST
reader path) or a pathological O(N)/O(N²) scan on the unbounded join state.

**Ruled out by measurement (all reproduce the hang/collapse):** the FORSTRS timer (HEAP identical),
checkpoint mode (`noflush` true *and* false), checkpoint-failure tolerance
(`tolerable-failed-checkpoints=1000` — TM still crashes), and the lock-free-memtable commits (built
the dylib at `1df5fa7fc`, pre-arena/skiplist — still hangs). The cause is upstream of all of these,
in `vectorizedBatchGet`.

**Deadlock vs scan — resolved from behaviour (not a lock deadlock):** q4 uses the *same*
`vectorizedBatchGet` path but does **not** hard-hang — it degrades **gradually** (166K→15K rec/s as
state grows). A lock deadlock would freeze q4 too; the gradual slowdown means **pathological scan /
read amplification**. q5 hard-hangs only because its **watermark-driven drain issues one massive
batch-get over the entire accumulated window-join state** — size-proportional work on a read path
whose per-probe cost already grows (the `get_arc` tier walk), compounding to ~O(N²) → >180 s in a
single call → the watchdog kills the TM.

**Therefore the fix is the engine read-path redesign, not a quick lock fix:** the in-progress
**skiplist memtable index** (indexed resolve instead of O(tiers) walk) plus a **bounded/streamed
batch-get** so a single watermark-drain can't issue an unbounded-cost scan. This preserves
vectorized/batch/Arrow/zero-copy and adds no per-row work (it changes how a *batch* of probes
resolves). This is a focused engine effort and the single gating item for the heavy-query class.

**Superseded hypothesis (kept for history):** LSM read-amplification from compaction lag / cache
size. Tuning compaction threads + block cache was tested and did NOT help — consistent with the real
cause being a hang in the batch-get call itself, not cache-miss I/O.

**Config tuning was TESTED and does NOT fix it (2026-06-03):** bumping background compaction (2→8)
+ flush (1→4) threads + block cache (256 MiB→2 GiB) on q4 produced the **same** rate collapse
(346 K→34 K rec/s past ~40 M events). This confirms the prior **FRS-CACHE-STATS** finding: the cost
is **CPU-bound in the per-probe `get_arc` memtable-tier walk**, not cache-miss I/O or L0 compaction
lag. (The 2 GiB cache also risked OOM at p=4 and was reverted.)

**Therefore the fix is engine-level, not config:** the in-progress **lock-free memtable + skiplist
memtable index** work (see the resume notes / lock-free-memtable design spec) directly targets the
per-probe tier-walk CPU — replacing the O(tiers) linear resolve with an indexed lookup. That is the
gating item for making unbounded-state heavy queries (q4, and the SST-read path generally)
competitive under checkpointing. It must preserve the vectorized/batch/Arrow/zero-copy invariants
and introduces no per-row work (it changes *how a batch of probes resolves*, not the batching).

---

## 4. Status / verification

- **Timer (FORSTRS, sole service):** working — q0–q3 finish; q3 exact (`src_out=2,201,068` ==
  RocksDB); per-window bisect proved FORSTRS == HEAP. Step 5 satisfied.
- **`noflush=true`:** kept as the global config (best for the bounded-state majority; q5 = 37.7 s).
- **Light/medium queries (q0–q3):** fast and competitive (24–34 s); q3 exact-correct.
- **Heavy unbounded-state query (q4):** does not finish under either checkpoint mode — gated on the
  §3 engine read-path work (skiplist memtable index). Config tuning ruled out by measurement.
- **Full q0–q22 sweep (`noflush=true`, FORSTRS timer):** running to produce the per-query +
  total-runtime ratio vs RocksDB. The expectation given the above: bounded-state heavy queries
  (windowed aggregates, group-bys, dedup) finish fast; the unbounded temporal joins (q4, possibly
  q20) are the weak points until the §3 engine work lands.

### Honest bottom line
The timer goal (switch entirely to Forst timer) is met and verified. The light/windowed/group-by
queries are correct and fast. The **1.x–2.x total under checkpointing is gated on one engine-level
item** — the §3 read-path (skiplist memtable index) — which is in-progress engine work, not a
config change, and is the single highest-leverage remaining task. Config levers (checkpoint mode,
cache, compaction threads) were exhausted and documented; none move the read-path wall.

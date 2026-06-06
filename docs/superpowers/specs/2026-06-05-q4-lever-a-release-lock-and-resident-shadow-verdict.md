# q4 — compaction rate-limit (lever A) LANDED + resident-shadow verdict

**Date:** 2026-06-05. Same-machine, full-length (MAXSEC=600, 98M events) A/B. Follows
`2026-06-05-q4-three-engine-codepath-and-rootcause.md`. Pursues the user directive
"keep on with the compaction rate limit and resident shadow."

## TL;DR

| run | finish (98M) | avg | swap (before→after) | verdict |
|---|---|---|---|---|
| RocksDB (reference, same Mac) | 241s | 407K/s | — | target |
| forst-rs BASELINE (no levers) | **564s** | 174K/s | 3811M → 3811M | same-session baseline |
| + **Lever A** (compaction lock-release) ALONE | **538s** | 182K/s | 3811M → 3811M | **−4.6%, KEPT, default-ON** |
| + Lever A **+** Lever C (resident bypass) | 578s | 170K/s | 3819M → 3811M | C drags A back → C REFUTED |

**Machine caveat (decisive for absolute numbers):** this Mac is **swap-poisoned** —
`vm.swapusage` held at ~3.8 GiB used and **did not drain** across all three runs (the
exact condition the prior session flagged as "REBOOT before any bench"). The clean-machine
q4 figure established earlier is ~232K/s RocksDB **parity**; 174–182K/s here is that parity
dragged down by residual swap page-in. ⇒ **absolute finish times are inflated and NOT a
trustworthy parity/flat-curve verdict**; only the **same-session relative deltas** (identical
swap state across runs) are valid. A reboot is required before any absolute parity claim.

## Lever A — compaction rate-limit via flush-lock release (LANDED, default-ON)

**Mechanism.** `compact_l0_for_cf` previously held the per-CF `lock_flush` for the entire
L0→L1/L1→L2 merge (a ~25 s burst at q4 scale). `lock_flush` is shared with the flush worker,
so during a burst **flushes stall → the active memtable cannot roll → foreground writes
back-pressure → the IntervalJoin starves** = the trough. Lever A drops the guard for the
merge and re-acquires it only for the version apply:

```rust
let mut flush_guard = Some(cf_data.lock_flush());
...
if compact_release_lock() { flush_guard = None; }     // release for the long merge
let Some(edit) = job.run()? else { return Ok(None); };
if flush_guard.is_none() { flush_guard = Some(cf_data.lock_flush()); } // re-acquire for apply
...
drop(flush_guard);                                     // hold spans the apply
```

**Correctness (why releasing the guard cannot lose/duplicate state):**
- `version_set.apply` is serialized by its **own** `apply_lock` (version/mod.rs) — independent
  of `lock_flush`.
- Flush is **L0-add-only**; it never deletes the input files this compaction gathered.
- Deletes are serialized by the engine-global `compaction_mutex` (only one compaction at a time).
- `apply_edit` **validates** the gathered input files are still present before applying; a
  concurrent flush's new L0 file is simply not in this edit's delete-set, so it is preserved.
- Engine suite **263/263 green** with it on.

**Result:** isolated full-length A/B (above) = **564s → 538s, −4.6%**, identical swap state.
The ONLY single lever measured this session to improve finish time. Flipped **default-ON**
(`FRS_COMPACT_RELEASE_LOCK`, disable with `=0`). This is a real, compounding, correctness-safe
win — kept per the "small improvements compound" directive.

## Lever C — resident-shadow bypass (REFUTED, kept env-gated default-off)

Skipping the Tier-2 resident shadow on reads (`FRS_RESIDENT_BYPASS=1`) made A+C = 578s
(worse than A alone's 538s and the 564s baseline). **Root cause of the regression:** the
bypass only skips *reading* the shadow — it does **not** stop the shadow being *built*, so the
run pays the full RAM cost AND forces reads down to SSTs (block cache / disk). Worst of both.
Shadow-removal was already proven a regression in the prior session ("shadow IS the parity
lever"). **Abandoned.**

## Resident shadow — the correct lever is ALREADY in place

The right way to bound the shadow's RAM (the actual decay cause = oversized resident shadow →
RSS past physical RAM → swap → page-in stalls) is **not** bypass but a **global byte budget**.
That is already implemented and correctness-safe:
`column_family.rs` `FRS-GLOBAL-SHADOW-BUDGET` — a process-global `GLOBAL_RESIDENT_SHADOW_USED`
atomic, default 2 GiB (`FRS_RESIDENT_SHADOW_TOTAL_MB`), shared across every `DbImpl`, with FIFO
per-CF eviction (evicted entries still live on durable SSTs → Tier-3 reads them, so the budget
can only over-evict, never lose data). This bounds total shadow RAM so it cannot scale with the
~16 q4 CF instances. ⇒ the "resident shadow" half of the directive is done; no further change
warranted on this machine.

## Honest bound (unchanged, now with one lever banked)

The in-scope ceiling remains ~RocksDB parity (per the root-cause doc): q4's IntervalJoin is a
Flink **V1-sync per-record** operator, so the backend cannot batch record *arrival* — surpassing
RocksDB by 2× requires batched state delivery, a Flink-runtime/operator change beyond the state
backend. Lever A moves us toward that parity ceiling (−4.6%) by protecting the foreground from
compaction bursts — the data-pointed direction, opposite of the refuted compaction-parallelism.
**Next trustworthy step requires a machine reboot** to drain the 3.8 GiB stuck swap, then re-run
baseline vs Lever-A to confirm the delta holds on a clean machine and measure true distance to
the 241s RocksDB reference.

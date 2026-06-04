# forst-rs: the O(N²) merge-chain read that killed the TaskManager on q5 (WindowJoin)

**Date:** 2026-06-03
**Branch:** ForSt `forst-rs`, flink `forst-rs-jdk25`
**Component:** engine merge-operand read path —
`DbImpl::peel_merges_from_memtable` + `VectorizedMemTable`/`ShardedMemTable`.

---

## 1. Symptom — the TaskManager DIES (abnormal), not just "slow"

NexMark **q5** froze ~6.0 M events, then the **TaskManager process exited (code 1)**.
A CPU-bound-slow operator would backpressure and eventually finish — a TM *death* is
abnormal and signalled a real fault. The TM log:

```
WARN Task 'WindowJoin[14] -> Calc[15] -> nexmark_q5[16]: Writer' did not react to
     cancelling signal - interrupting; it is stuck for 30 seconds in method:
       ForStRsLinker.vectorizedBatchGet(:2518)
       VectorizedExecutor.invokeVectorizedBatchGet/executeGets/executeBatchRequests
       AsyncExecutionController.drainInflightRecords  (watermark drain)
... (repeats) ... stuck for 180 seconds
ERROR Task did not exit gracefully within 180+ seconds. → Fatal → Terminating TM exit 1
```

So q5's **WindowJoin** (async-state self-join) issues a `vectorizedBatchGet` at the
watermark drain that **never returns and ignores interrupt** → the cancellation
watchdog terminates the whole TaskManager. (The trigger that begins the cancel is the
checkpoint coordinator suspending because the mailbox is blocked by the stuck call.)

## 2. Root cause — O(N²) merge-chain fold in the memtable (symbolized profile)

A `sample` of the stuck TM against a **symbolized** dylib placed **5648/5648 samples**
(100 % CPU, thread RUNNABLE — not a lock/IO block) in:

```
DbImpl::batch_get_vectorized
  → DbImpl::get_internal
    → DbImpl::collect_merge_operands
      → DbImpl::peel_merges_from_memtable
        → ShardedMemTable::get          ← called ONCE PER OPERAND
```

q5's WindowJoin stores **multiple records per join key as a list, via engine `Merge`
(append) operands** (the `LIST_ADD`/`APPEND_MERGE` path — the *only* path that emits
engine Merge ops; the windowed COUNT is Put/RMW and was a red herring). Reading a hot
join key folds its whole operand chain. The old `peel_merges_from_memtable` did:

```rust
loop {
    let hit = mem_arc.get(key, cutoff)?;   // ShardedMemTable::get → find_latest
    ... push operand ...; cutoff = entry.sequence - 1;   // one get per operand
}
```

and `VectorizedMemTable::get` → `find_latest` **scans the key's entire `row_indices`
version list** to resolve `seq <= cutoff`. So a key with **N** merge operands costs
**N gets × O(N) scan = O(N²)** per single read (and re-locks the shard N times). For a
hot join key with a deep chain in the live memtable, one bounded `vectorizedBatchGet`
spins for minutes → the 180 s watchdog → TM kill. (Within one active memtable nothing
collapses the chain — compaction only acts after flush.)

## 3. Fix — single-pass merge-operand collection (O(N²) → O(N log N))

New `VectorizedMemTable::collect_merge_operands(key, cutoff, &mut operands)`
(+ `ShardedMemTable` wrapper): take the shard read lock **once**, gather the key's
versions visible at `cutoff` in **one pass** over `row_indices`, sort them newest-first
(`O(K log K)`), and walk once — pushing every `Merge` operand until the first
`Put`/`Delete` base. `peel_merges_from_memtable` now calls it once instead of looping
N cutoff-gets. Logic is equivalent (same operands, newest-first; same base; the
Merge-with-no-payload corruption guard moved into the new method) — only the complexity
changes. Both the active-memtable and imm-start peel paths route through it.

This preserves all invariants (vectorized batch path, zero added per-row work — it
changes how *one key's chain* resolves, MVCC `cutoff` semantics unchanged) and adds no
`byte[]`.

## 4. Result (measured 2026-06-03)

- **q5: TM-kill / hang → FINISHES in 46.9 s** (6.0 M events, no slow `vectorizedBatchGet`).
- Engine + storage merge unit tests: **92/0** (storage memtable) + **26/0** (engine merge) pass.
- Same class of fix is expected to unblock the other join/list-heavy queries (q7, q8, q11).

## 5. Correction to the prior note

`2026-06-03-forstrs-timer-on2-drain-fix.md` §6 attributed the post-timer-fix q5 wall to
the window-aggregate sync-state RMW. That was a **red herring** from a transient diag
stack at 2.14 M. The TM-death root cause is THIS merge-chain O(N²) on the WindowJoin —
captured only by reading the actual death sequence + a symbolized native profile. The
timer fix (separate, real) and this merge-chain fix are both required for q5 to finish.

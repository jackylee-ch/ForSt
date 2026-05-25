# Round 7 Summary — 5-agent verification of batch 12

**Verifies**: Java commit `0ee83b07fa1` (batch-12 Round-6 HIGHs); Rust commit `ce7be06f1` (batch-12 zero-copy).

**Total**: 14 raw HIGH + 19 MED + 14 LOW; **~13 unique HIGHs after dedup**.

**Agent 7D (JDK 25 / FFM)**: CLEAN (0 HIGH).

## HIGH findings (deduped)

### Correctness (3) — most urgent
- **A7-H1** `VectorizedExecutor.dispatchAppendMergeBatch` lost the per-row try/catch lifted by B12-C. FFI throw leaves `futuresArr` never completed → callers hang forever. **Regression from batch 12 consolidation.**
- **A7-H2** `prefix_scan_iter_owned` only enumerates **active memtable** keys (not imm + SSTs). Q11 `entries()` silently loses rows after first memtable flush. **Correctness regression** for the C6-H3 path.
- **A7-H3** `ForStRsSnapshotStrategy` leaks engine `FrsSnapshot` if `provider.currentBlob()` throws — snapshot pins source seq, blocks compaction until process exit.

### Rust zero-copy (4)
- **B7-H1 / C7-H1** `prefix_scan_iter_owned` emits `(key.to_vec(), value)` per row → defeats `Arc<[u8]>` savings from C6-H3 on every row.
- **B7-H2** `memtable::get()` does `inline_value.to_vec()` per emitted row (paid by every `prefix_scan_iter` lookup).
- **B7-H3 (alarming)** `batch_insert_with_base_seq` / `_with_explicit_seqs` / `batch_put_arrow_with_base_seq` do NOT update `prefix_index` → **C6-H3 fast path is DEAD CODE for Q12 production batch-write path.**
- **C7-H2** `Box::from(prefix)` per-write in entry() pattern (consumes key unconditionally even when prefix already exists).

### Flink runtime (3)
- **E7-H1** `pendingRegistrations.put(checkpointId, list)` unconditionally OVERWRITES on Flink retry → first-attempt ref-count bumps leak on subsequent abort.
- **E7-H2** Rescaling union-merge: last-writer-wins is **non-deterministic** for schema collisions because Flink doesn't guarantee handle iteration order across retries.
- **E7-H3** Distinct `StateSerializerRegistry` per backend (V1-sync vs async) — future cross-path bypass.

## MED findings (highlights)
- ReducingAggregatingCache.generations CHM grows unbounded
- VectorizedExecutor.executeRequestSync doesn't consult batchPoisonCause (invariant break)
- ForStRsRestoreOperation parallel restore leaks already-opened DBs on partial failure
- currentKeyGroup not volatile (E6-M1 carry-over)
- MapStateCache offsets risk JAVA_INT overflow above 2 GiB
- Multiple Rust per-write `Box::from`/clone patterns

## Termination tracker
- **Round streak: 0/5 clean** — Round 7 NOT clean.
- Batch 13 dispatched.

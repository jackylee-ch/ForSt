# Round 8 Summary — 5-agent verification of batch 13

**Verifies**: Java `02f6cff9509`, Rust `84e04a865`.

**Total**: 15 raw HIGH + 19 MED + 11 LOW; **~13 unique HIGHs after dedup**.

No agent fully clean.

## HIGH findings (deduped)

### Rust correctness/perf (4)
- **C8-H1** `prefix_scan_iter` (borrowing variant, used by `frs_prefix_scan_arrow` + `frs_batch_prefix_scan`) STILL only enumerates active memtable — A7-H2 fixed only the owned variant.
- **C8-H2 / A8-H1** prefix_index re-insert-after-tombstone gap: Put→Delete→Put leaves key invisible to fast path in BOTH single-write AND batch paths.
- **C8-H3** A7-H2's eager BTreeSet materialization defeats Q11 streaming guarantee (broke the C6-H1 streaming-via-Arc-closure design).
- **D8-H4** ArrowTimerBuffer.resize allocates 3 segments on newArena BEFORE closing oldArena; throw leaks oldArena + half-mutated state.

### Java correctness (4)
- **B8-H1** `cache.tryFold()` returns `Optional<V>` → allocates per cache-HIT on Q12 hot path (4 V2 state classes).
- **B8-H2** B4-H2 carry-over escalated — `outputBuffer.getCopyOfBuffer()` per Q11 update().
- **A8-H6 / D8-H3** A7-M3 INCOMPLETE — `restoreExec.shutdown()` graceful (not shutdownNow) leaves in-flight futures producing FrsDb/FrsCfHandle never assigned/closed.
- **E8-H3** ForStRsStateExecutor.executeBatchRequests lacks A7-H1 drain pattern → fallback path hangs futures forever on FFI throw.

### Java Flink runtime / lifecycle (4)
- **D8-H1** `frs_vec_merge_append_batch` still in **critical mode** — D6-H2 missed this sibling FFI (same WAL fsync safepoint pin risk).
- **D8-H2** Arrow release callbacks via per-call `Linker.nativeLinker().downcallHandle` in hot loop — class-metaspace pressure.
- **E8-H1** A7-M1 introduced lock-under-FFI: `putIfGen` holds `generationsLock` while invoking flushCallback → engine PUT FFI under lock → mailbox stall.
- **E8-H2** E7-H1 INCOMPLETE — `sstRegistry.register` happens BEFORE `merge`; abort fires in between → `takePendingRegistrations(id)` returns null → orphan ref-count bumps leak forever.
- **E8-H4** BackendPathInvariant OBSERVED static map in system classloader, never cleaned → V1→V2 toggle after restart blocks legitimate restart.

## MED highlights
- E8-M1 rescaling restore-exec uses `shutdown()` not `shutdownNow()` (sibling of A8-H6 on the rescale path)
- E8-M2 `inheritBackendIdentifier` only handles range.equals (E6-M2 carry-over)
- D8-M3 `generationsLock` synchronous overhead while real thread model is single-mailbox (perf-only)
- A8-M2 A7-H1 try-block start is too narrow — `flattenValueSlices`/`flattenHeapFutures` OOM leaks futures
- A8-M6 `currentKey` + `currentKeyGroup` joint atomicity broken (volatile per-field but not jointly)
- B8-M1 BTreeSet cross-tier `to_vec` allocates-then-drops on dup keys

## Termination tracker
- Round streak: **0/5 clean**
- Batch 14 dispatched.

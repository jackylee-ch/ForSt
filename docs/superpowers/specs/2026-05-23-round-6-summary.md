# Round 6 Summary — 5-agent verification of batch 11

**Verifies commits**: Java `1151723e611` (batch-11 Round-5 critical HIGHs), Rust `d6e24999f` (batch-11 zero-copy).

**Total**: 20 HIGH + 21 MED + 16 LOW raw; ~17 unique HIGHs after dedup.

## HIGH findings (deduplicated)

### Correctness (4)
- **A6-H1** Reducing/Aggregating onClear race: concurrent asyncAdd's miss-resolve `cache.put` lands AFTER executeDeletes → next flushOnBarrier resurrects cleared accumulator.
- **A6-H2 / E6-H4** Rescaling restore returns `emptyMap` → silent schema-drift bypass on every rescale (2 agents).
- **A6-H4** onClear FFI throw → partially-built batch silently dropped on next reset() (data loss).
- **E6-H3** Schema-drift codec race: `LinkedHashMap live` iterated on async-snapshot thread while mailbox writes via verifyOrRegister → CME or corrupt blob.

### V1-sync sibling regressions (2)
- **E6-H1** V1-sync `ForStRsAbstractKeyedStateBackend.snapshot()` doesn't check CANONICAL — E5-H1 fix only on async side.
- **E6-H2** V1-sync still uses brittle "registry is already closed" substring — E5-H3 only on async.

### Lifecycle (1)
- **A6-H3 / B6-H2 / D6-H1** (×3): VectorizedExecutor monotonic executor-arena leak — `dispatchAppendMergeBatch/PerRow/IterPrefix` all `arena.allocate(...)` on long-lived executor arena, never reclaimed until backend dispose. Flagged by 3 independent agents.

### JDK 25 FFM (1)
- **D6-H2** `frs_vectorized_batch_put/delete` bound in critical mode → blocks safepoint during WAL fsync (tens-of-ms to seconds), starves GC + sibling tasks.

### Vectorization (5)
- **B6-H1** APPEND_MERGE heap-path 5-alloc burst per LIST_ADD row (V20.1 V2 ListState on Q15/Q19).
- **B6-H3** flushOffHeapListBuffersIfDirty `new IdentityHashMap<>()` per batch + Boolean.TRUE boxing.
- **B6-H4 / B6-H5** B5-H7 half-applied: per-row `valueSliceLists.get(row)` interface dispatch + enhanced-for Iterator on completion loop.
- **B6-H6** MapStateCache evictClockSweep + removeFromHashIndex O(hashSlots) per eviction (4MB reads per insert at 1M cap).
- **B6-H7** MapStateCache.keyEquals per-byte off-heap loop bypasses MemorySegment.mismatch intrinsic (56-byte composite key Q12 hot path).

### Rust zero-copy (3)
- **C6-H1** FFI `frs_vec_iter_prefix_open` still eager `prefix_scan` (B5-H2 documented partial, escalated).
- **C6-H2** `batch_get_arrow` doubles allocates — `Vec<u8>` from `get_internal` then memcpy into BinaryBuilder.
- **C6-H3** `prefix_scan_iter` keys still eagerly via `prefix_scan_keys → Vec<Vec<u8>>`.

## Next: batch 12 — 5 parallel fix agents

# Round 9 Summary — 5-agent verification of batch 14

**Verifies**: Java `0778b694f36`, Rust `edbce05e4`.

**Total**: 14 raw HIGH + 14 MED + 11 LOW; **~10 unique HIGHs** after dedup.

No agent fully clean.

## HIGH findings (deduped)

### Batch-14 regressions (3)
- **E9-H1/A9-H1/H2/H3** ForStRsStateExecutor.executeGets/Puts/Iters per-phase counter is helper's LOCAL variable, never propagated on throw. drainTail with counter=0 double-completes prior rows via spurious exceptionHandler.handleException. **E8-H3 regression.**
- **E9-H2** ReducingAggregatingCache.drainPendingFlush nulls slot BEFORE invoking flushCallback. If FFI throws, data lost (entry removed from entries, slot nulled, flushAllDirty cannot recover). **E8-H1 regression.**
- **A9-H4** ForStRsStateBackend recordBackendPath happens BEFORE try-catch around dbOpen — if ctor throws, OBSERVED slot leaks → false-positive IllegalStateException on Flink-scheduled retry. **E7-H3/E8-H4 regression.**

### V1-sync rescale carry-over (1)
- **E9-H3** ForStRsSnapshotStrategy V1-sync still uses `UUID.randomUUID()` unconditionally on restart — SharedStateRegistry cannot resolve prior session's shared SSTs, full re-upload on first incremental checkpoint. **E6-M2/E7-M2 unaddressed.**

### Primitive autoboxing in HashMap (1 root cause, 2 sites)
- **B9-H1/D9-M2** ForStRsKeyedStateBackend `Map<ByteArrayWrapper, Long> writeBuffer`: Long autoboxing on put/get + new ByteArrayWrapper per call. B8-H2 partial: byte[] alloc eliminated but Long+wrapper allocs reintroduced.
- **B9-H2/D9-M3** ForStRsLinker `ConcurrentHashMap<Long, MethodHandle> arrowReleaseHandleCache`: Long autoboxing per invokeArrowRelease (2× per batchGetArrow).
- **B9-H3** ForStRsValueState `writeBufferGet.apply(lastValueKey)` allocates new ByteArrayWrapper per `value()` call.

### Rust prefix scan partial fix (2)
- **C9-H1** Active-memtable tier NOT lazy at iter construction — `prefix_scan_keys` fully materializes Vec<Arc<[u8]>> per shard upfront. C8-H3 "streaming" promise broken for memtable tiers.
- **C9-H2** SST tier hot path does 2-3 Vec<u8> allocs per emitted key (`buffered.push(view.key.to_vec())` + `last_emitted = Some(key.clone())` + `min_key.to_vec()`).

### Critical mode + bounds (2)
- **D9-H1** ForStRsKeyedStateBackend.flushWriteBuffer triggered at `writeBuffer.size() >= 4096` but flushKeyPtrs/flushValuePtrs MemorySegments sized only 64 — load-bearing invariant unenforced, BoundsCheckException on size>63.
- **D9-H2** `frs_vec_iter_prefix_open_batch` still `bindCritical` — same D6-H2/D8-H1 rationale; batched call holds JVM safepoint under back-pressure.

## MED highlights
- C9-M1 LazyPrefixIter linear-scan dedup redoes peek+dedup per next()
- C9-M3 per-emitted-key db.get full LSM walk
- C9-M4 read_block_at not interacting with ShardedClockCache (no cache reuse for re-scan workloads)
- E9-M1 ArrowTimerBuffer.resize post-commit hashInsert throw → oldArena leak
- A9-M1 writeArenaPos int-overflow on 2GiB+ adversarial value
- A9-M2 flushWriteBuffer reset non-atomic on FFI throw → double-write on retry
- E9-M3 abort marker clear-on-completion race window

## Termination tracker
- Round streak: **0/5 clean**
- Batch 15 dispatched.

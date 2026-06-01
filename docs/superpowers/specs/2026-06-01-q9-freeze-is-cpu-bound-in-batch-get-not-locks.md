# q9 26.5M freeze is CPU-bound in engine batch_get — NOT locks/IO/compaction

Date: 2026-06-01
Status: DIAGNOSED via live stacks (instrument-before-guess). Symbolized rebuild
in flight to name the exact hot function.

## Symptom

q9 (2GB memtable + cache-bypass, S3) reproducibly HARD-FREEZES at ~26.5M source
records / ~161s: source rate drops to EXACTLY 0 and stays 0 for 160s+ (not a
slow grind — a full pipeline flatline). Same point with the old dylib and with
the LocalCache O(1)-LRU dylib, so the LocalCache LRU was NOT the cause.

## Live diagnosis (frozen process, captured in situ)

jstack of the TaskManager during the freeze:
- All 4 `Source` threads: TIMED_WAITING (parking) on the mailbox — i.e. idle /
  backpressured because the Join stopped consuming.
- `Join[10] -> Calc[11]` thread: **RUNNABLE inside the FFM downcall**
  `ForStRsLinker.vectorizedBatchGet:2518` ← `VectorizedExecutor.invokeVectorizedBatchGet:2298`
  ← `executeGets:1165` ← `executeBatchRequests:392`. The batch GET FFI call is
  NOT returning.

Native `sample` of the same process (4s): the dominant stack (weight 2662) is
**37+ frames deep inside `libforst_rs_ffi.dylib`, continuously on-CPU** — NOT
parked on a mutex (`__psynch_mutexwait` absent), NOT in `block_on`/opendal S3,
NOT in compaction. `frs_vec_iter_prefix_open` appears in the stack.

## Conclusion

The freeze is an **algorithmic blowup (CPU-bound) inside the engine's batch_get
read path at large spilled state** — one `batch_get` call for a single batch
takes 160s+. This RULES OUT the audit's lock/IO/compaction items (TIER-1-B
LocalCache lock, C compaction-on-flush-worker, D sst_readers lock) as the freeze
cause. They remain valid improvements but are not THIS wall.

Most likely (consistent with the appearance of `frs_vec_iter_prefix_open` and the
prior deep note in MEMORY): batch_get / the join-probe read fans out into
prefix-iteration or an unsorted/linear scan that is O(state) PER key, so at 26.5M
it explodes. The dylib is stripped (`???`) so the exact function is unconfirmed.

## Next (decisive)

1. Symbolized release dylib (CARGO_PROFILE_RELEASE_DEBUG=line-tables-only) —
   building now.
2. Deploy + re-run q9; `sample` the TM DURING the freeze → read the named hot
   frames → identify the exact O(N)-per-probe function in batch_get.
3. Fix that function (e.g. ensure point batch_get uses the sorted index /
   hash index, not a linear unsorted scan or per-key prefix open).

This is the real heavy-query wall. The LocalCache O(1)-LRU and streaming-snapshot
fixes are kept (correct + tested) but are not what unblocks q9.

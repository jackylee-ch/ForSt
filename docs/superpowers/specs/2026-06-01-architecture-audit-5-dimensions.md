# Architecture audit — vectorization, zero-copy, Arrow/total-buffer, locks, async

Date: 2026-06-01
Status: AUDIT (5 parallel agents, file:line-cited). Prioritized roadmap below.
Read-only audit; no code changed in this pass. Fixes sequenced after.

## Cross-cutting conclusion

The async **V2 path is already compliant** (VectorizedExecutor batches PUT/GET/
DELETE/APPEND_MERGE one FFM call per op-type; ForStRsDBIterRequest drains via
chunked frsVecIterPrefixNext; off-heap staging + borrowed-slice sinks). The
violations cluster in (a) the **V1-sync legacy state path**, (b) the **engine
read/cache plumbing** (locks + sync I/O + memory accounting). Three independent
audits converge on the heavy-query read/write wall, and the memory audit
**explains today's 4 GB OOM**.

## TIER 1 — actionable now, highest leverage (explains OOM + heavy-query gap)

### A. Memory accounting — the 4 GB OOM root cause (arrow-audit H1/H2/H3)
- `db.rs:5985/6048` `add_resident_flushed_with_bounds` RETAINS the flushed
  memtable in RAM as a "resident shadow" AND `write_buffer_manager.release()`s
  those exact bytes — so resident RAM stops counting against WBM. Shadow has its
  own per-CF 1 GiB cap (`column_family.rs:57`) × ~8 instances = ~8 GiB invisible.
- No total budget: WBM + block cache + resident shadow + disk LRU are 4
  independent per-instance pools, uncoordinated (`db.rs:440-442`,
  `column_family.rs:57`, `local_cache.rs:132`). 8 instances × (WBM+block+shadow)
  ≈ 14 GiB native stacked on the 8 GiB JVM → OS OOM-kill (today's signature).
- `vectorized.rs:396` WBM counts only `key+value+57`, NOT the `sorted_index`
  BTreeMap (Arc<[u8]> + Vec<RowIndex> + node overhead, 50-150 B/key) → real RSS
  overshoots the cap before flush fires.
- FIX: one process-wide budget object (mirror ForSt `ForStSharedResources`/
  `Cache`); WBM + block cache + resident shadow reserve/release against it; charge
  the shadow instead of releasing it; add index overhead to WBM counting. This is
  what makes ANY larger-memtable / resident strategy safe.

### B. LocalCache O(N) LRU under one global Mutex, per block read (lock-audit H1)
- `local_cache.rs:72` single `Mutex<Inner>`; `touch_lru` (`:96-101`) does a LINEAR
  `VecDeque::iter().position()` + `remove(pos)` (O(N) scan+shift) UNDER the global
  mutex on EVERY cache hit. `RangeCachedRandomAccessFile::read_at`
  (`cached_fs.rs:908`) calls this for every SST block / every overlapping SST /
  every join probe (q4/q7/q9/q16). Same pattern as the prior prefix_scan write-
  lock freeze. Likely a large part of the "diffuse ~7× per-probe constant."
- FIX: O(1)-touch LRU (intrusive linked list or clock/generation counter) and/or
  shard LocalCache by key hash. Disk I/O is already outside the lock; only the
  bookkeeping is broken. **Lowest-risk high-impact fix — do first.**

### C. Compaction runs INLINE on the single flush worker (async-audit #1/#2)
- `db.rs:8041` `run_flush` → `maybe_auto_compact` → `compact_l0_for_cf` on the
  SAME single `forst-rs-flush` worker, synchronously. While it reads inputs +
  k-way merges + streams output to S3, it can't drain the flush queue → imms pile
  up → `WriteController` stalls writers. Global `compaction_mutex` (`db.rs:368`)
  also serializes all CFs.
- FIX: background compaction pool (honor max_background_compactions); run_flush
  enqueues, doesn't run, compaction; make compaction_mutex per-CF.

### D. get_or_open_sst_reader holds sst_readers.write() across S3 I/O (lock-audit H2)
- `db.rs:7340-7390` takes the global reader-cache write lock across
  `await_upload` + `open_random_access_file` + footer/index read (all S3). Every
  other reader lookup (even hits) blocks during a cold open. The compaction path
  (`db.rs:3590`) already does it right (I/O outside lock, brief insert).
- FIX: mirror compaction path — I/O outside lock, re-acquire write() only to
  insert (double-check racing inserter).

## TIER 2 — deeper, multi-session

### E. Async read-ahead / prefetch on scans (async-audit #4/#5)
No cross-block prefetch: block i+1's S3 GET only issues after block i returns;
decode/decompress is synchronous first-touch. This IS the per-probe S3 latency.
FIX: prefetch next N blocks + decode async so reader overlaps GET(i+1) with
decode(i). Ensure decoded-block cache always wired.

### F. V1-sync per-record FFI (vec-audit #1-4)
V1-sync ValueState/MapState/ListState + engine timer queue do single-key get/
put/delete + per-entry iteratorNext (one FFM crossing per record); the chunked
drain + classifier already exist on V2. Routing V1-sync through them is the fix
(structural; q5/q8/q11). MapState.clear() does 2×O(entries) crossings.

### G. Zero-copy engine read API (zerocopy-audit #1/#2/#4)
`db.rs:6340` `get` returns owned `Vec<u8>` (2 copies/point-get); `frs_get_pinned`
hard-disabled (`lib.rs:1517`, use-after-free risk) → V1-sync read always copies;
SST point-read `to_vec()` per value (`reader.rs:454/533`). FIX: borrowed
ValueSink through db.get + safe pinned-get via Arc handle + frs_release.

### H. Checkpoint SST copy in sync phase (async-audit #3)
`db.rs:3731` `copy_live_ssts` (whole-SST S3 GET+PUT, serial) runs in the sync
snapshot phase. FIX: move to async materialization phase, parallelize across files.

## Dead code to delete (zerocopy-audit #11)
ForStRsStateExecutor + ForStRsStateRequestClassifier (never instantiated),
byte[][] batchGet/batchPut FFI, ForStRsDBIterRequest.completeBatch,
ColumnarBatchBuffer.copyAt — shrink the byte[] surface.

## Sequencing
1. **B (LocalCache O(1) LRU)** — first: lowest risk, on every block read.
2. **A (unified memory budget + charge shadow + WBM index count)** — unblocks any
   resident/larger-memtable strategy and stops the OOM.
3. **C + D (decouple compaction; reader-open outside lock)** — ingest + cold-read.
4. Tier 2 E/F/G/H as multi-session work.

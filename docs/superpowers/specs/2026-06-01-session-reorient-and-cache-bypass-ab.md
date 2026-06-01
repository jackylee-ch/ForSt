# 2026-06-01 — Session reorient + cache-bypass A/B (measured)

Date: 2026-06-01
Status: IN PROGRESS. Reorientation complete; running measured A/B on the
JFR-identified per-record `MapStateCache` lever.

## Reorientation (synthesis of prior sessions)

Disk cleaned: removed ~5.6 GB stale `/tmp/flink-forst-rs-*` + `nexmark-*`
bench dirs; 507 GiB free. Build current: deployed dylib (Jun 1 11:17) == built.
S3 env set. Harness (`scripts/measure-sql.sh`) runnable: real JM wall-clock,
direct sql-client submit. Resource envelope (8c32g, 4 slots, 2048mb memtable,
cache-bypass disk LRU 128 GiB) already in both configs.

### Correctness: DONE
q9 runs clean past all prior crash points (achieved over prior sessions).

### Two distinct perf bottlenecks (NOT one wall)
1. **q4-style streaming join** = per-record `MapStateCache` point-lookup compare.
   JFR (2026-06-01) attributes ~40 % of the join thread to `findRow` /
   `keyEquals` / `MemorySegment.mismatch` over the ~56-byte composite key, plus
   cache write-side maintenance (`putIfAbsent`/`put`/clock-sweep evict ≈ 3.4K
   samples). This is a **modifiable backend** cost and matches the user's
   "no per-key, all batch" directive.
2. **q9-style TopN** = prefix-iterator O(K²) freeze at large hot partitions.
   `RetractableTopNFunction` emits row numbers → re-iterates each partition
   O(K)/record; forst-rs per-scan cost (BTree range + 16-shard cursor snapshot
   + FFM + Arrow encode) is ~100× rocksdb's skiplist `next()`, so at large K it
   flatlines (the 26.5M freeze). Snapshot-reuse does NOT help (state mutates
   every record → snapshot stale each time).

### Confirmed negative results (do NOT repeat)
- Memtable `prefix_scan_keys` IS O(log N + K) via sorted BTree index + bounded
  O(4096) unsorted filter — NOT the O(N²) the older memory feared.
- value-carrying prefix scan, CF-hoist, Arc-key: all measured perf-NEUTRAL.
- shared cross-instance block cache, L0-trigger=4, unsorted merge-cap: neutral.
- v3.8 "wins" were a nexmark peak-TPS extrapolation artifact (burst-then-collapse
  inflated forst-rs Time 6–100×); honest wall-clock has been ~0.3–0.75× on the
  state-heavy half. BUT checkpoint-without-flush keeping state RAM-resident let
  q4 momentarily hit 554 K/s (faster than rocksdb) — the resident path is
  genuinely fast; blockers are (a) per-record cache cost and (b) 4 GB OOM on
  spill (large memtable + 3× snapshot materialization).

## The A/B (cache-bypass) — already gated, now measuring

`ForStRsMapStateV2.asyncGet` has `FRS_DISABLE_MAPSTATE_CACHE=1` (2026-06-01):
skips the per-record `MapStateCache` lookup + `putIfAbsent`, routing to the
off-heap buffer + engine `batch_get_vectorized`. Correctness-safe (asyncPut
writes through off-heap buffer + engine). The cache cap is 1M entries; q4's
auction working set (~6M auctions / 100M events) EXCEEDS it → the cache
**thrashes** (every lookup misses + evicts, pure overhead before falling to the
engine anyway). Hypothesis: bypassing removes that overhead.

### q4 baseline (cache ON, forst-rs-ffm-s3, S3)
| t (s) | src_out | rate (/s) |
|---|---:|---:|
| 42 | 5.86M | 279K |
| 62 | 11.4M | 278K |
| 82 | 16.8M | 269K |
| 102 | 22.1M | 262K |
(burst ~279K, slow decline as auction state crosses the 1M cache cap;
rocksdb q4 sustains ~405K/s = 246s/100M.)

### Next
1. Finish baseline curve (cache ON) to ~480s.
2. Re-run q4 with `FRS_DISABLE_MAPSTATE_CACHE=1` over the same window.
3. If bypass raises sustained rate → make it the default (or auto-enable when
   working set > cap). If neutral → the cost is the off-heap buffer's own
   per-record compare; next lever = namespace-scoped compare (cut the 56-byte
   compare to the differing mapkey suffix) or batching the off-heap lookup.

## MEASURED RESULTS (2026-06-01)

### A/B 1 — cache-bypass (`FRS_DISABLE_MAPSTATE_CACHE=1`)
| | cache ON | cache OFF |
|---|---|---|
| rate held ~255K until | 31M | **65M** |
| records @ ~264s | 42.9M | **63.6M (1.48×)** |
| then | slow decline → 18K floor | HARD FREEZE → 0/s at 65M |

Cache-bypass is a real throughput win but exposes a hard freeze.

### Decisive diagnosis — FRS-ITER-DIAG instrumentation
Added a gated diagnostic to `build_lazy_prefix_key_stream` (db.rs) logging
elapsed + tier counts + result size when a build exceeds 1 ms. Live capture of
the frozen q4 TM (job still frozen on the cluster) revealed TWO costs:

1. **Per-probe fixed cost (the decline):** builds with `mem_keys=0/1,
   sst_sources=0, resident_shadowed=0` took 1–5.6 ms — a fixed cost independent
   of result size and tier count. Root cause: `prefix_scan_keys` linearly scans
   `unsorted_lookup` per shard ×16, and the threshold grew with the memtable
   (`(sorted_count×ratio).clamp(1024,4096)`) so the scan ramped 1→5.6 ms as state
   grew. **FIX: FRS-UNSORTED-FIXEDCAP** — fixed cap (default 256, env
   `FRS_UNSORTED_CAP`); amortized write cost unchanged (O(log N)/insert ∀ cap).
   → q4 burst **doubled to 577K/s** (vs 282K). KEEP.

2. **The FREEZE (rate→0):** the 4 slowest builds each took **~114 SECONDS**, all
   with `sst_sources=1`. Root cause: `get_or_open_sst_reader` (db.rs:7400) calls
   `fs.await_upload()` which **blocks on the freshly-flushed SST's full S3
   upload** (under the `sst_readers.write()` lock). The flush writes ONE SST per
   memtable, so the 2048mb memtable → a ~2 GiB SST → ~114 s upload (≈18 MB/s).
   Once the 1 GiB resident shadow evicts that data mid-upload, every join probe
   touching its key range blocks ~114 s → source freezes. This is the same
   phenomenon as the q9 26.5M freeze.

### Freeze fix (in test)
- Targeted: shrink memtable 2048mb→256mb → ~256 MiB SSTs → ~14 s upload (~8×
  shorter await), resident shadow holds ~4 recent flushes. (measuring)
- Structural (follow-up): roll flush output into target-sized SSTs (rocksdb
  `target_file_size`) so upload time is bounded regardless of memtable size; and
  serve reads local-first / move `await_upload` off the `sst_readers.write()`
  lock (audit item D).

## Landed this session (tested: storage 331/0, engine 255+/0)
1. **FRS-UNSORTED-FIXEDCAP** (vectorized.rs) — fixed merge cap (default 256, env
   `FRS_UNSORTED_CAP`) at all 6 threshold sites. q4 cache-off burst **282K→543–577K/s
   (2×)**. Helps the resident phase of every prefix-scan-heavy query.
2. **Audit-D SST-open lock fix** (db.rs `get_or_open_sst_reader`) — await_upload +
   open moved OUTSIDE the `sst_readers.write()` lock (re-take only to insert,
   double-check racer). Removes the serialization that totally stalled the 256mb run
   at 32M. Serves the "no large lock" directive. (No q4-2048mb curve change — q4
   blocks on ONE SST, not the many-SST serialization audit-D addresses.)
3. **FRS-ITER-DIAG** (db.rs, gated `FRS_ITER_DIAG=1`) — the instrumentation that
   produced the diagnosis. Zero cost when off.

## Combined q4 curve (cache-off + fixed-cap + audit-D, 2048mb)
burst 543K → decline → FREEZE at 64.6M/362s (checkpoint-drain await_upload). Same
freeze point as every 2048mb variant — confirms the freeze is the uplink, not the
levers tried. NOT a regression; the burst/sustained phase is faster.

## THE freeze fix (next session) — targeted local-first
The freeze is `prepareSnapshotPreBarrier → drainInflightRecords → frsVecIterPrefixOpen
→ get_or_open_sst_reader → await_upload` blocking ~114 s on a ~2 GiB flushed SST's S3
upload at ~18 MB/s (the BOS uplink; opendal already uses concurrent multipart, so the
rate is the network). The only copy of that key's latest value during the upload window
is the uploading SST (resident shadow evicted, memtable flushed). Fix = keep a LOCAL
copy of in-flight-upload SSTs and serve reads from it without awaiting S3:
- The `Buffered` writer (opendal_backend.rs) holds the whole SST in `buf` during upload
  then drops it. Surgically: write that buf to the local cache dir (or keep it) keyed by
  file_number until `await_upload` would succeed, then evict. Bounded by
  MAX_INFLIGHT_UPLOADS × SST size (NOT the full LRU churn that got write-through reverted
  — only in-flight uploads are retained).
- `get_or_open_sst_reader` / `open_random_access_file`: if a local copy exists, open it
  and SKIP `await_upload`. This is the local-first read that decouples the hot/checkpoint
  path from the ~18 MB/s uplink — the structural unlock for "S3 ≈ local" on heavy queries.
- Validate q4 (must cross 64.6M without freeze), then q9 (same freeze class), then G2/G3.

## ★ CORRECTED FREEZE DIAGNOSIS (native sample of the frozen local-first TM)
The `await_upload`/S3-uplink hypothesis was WRONG. A `sample` of the frozen q4 TM
(local-first dylib) shows the freeze stack is:
`frs_vec_iter_prefix_open → … → _xzm_malloc_large_huge → _xzm_segment_group_clear_chunk`
— i.e. a **CPU/allocation blowup**: repeated large `malloc` + page-zeroing (+ Arrow
decode), NOT `block_on`/socket/await (no I/O-wait frames, threads on-CPU). The
114 s "build" measured by FRS-ITER-DIAG was this allocation work, not upload wait.

Root cause: `SstReaderImpl::read_data_block` (reader.rs:347) does
`vec![0u8; block_size]` (64 KiB default) **per block read** — the zeroing is wasted
(immediately overwritten by the read) — then decompress + Arrow-decode. The
heavy-join prefix scan at large spilled state iterates a HUGE key range → reads
**millions of 64 KiB blocks**, each a large alloc+zero+decode. The decoded-block
cache doesn't save it because the scan touches each block ~once (no repeat) and/or
thrashes. This is the heavy-join O(state) iteration meeting the per-block SST decode
cost — storage-independent CPU, consistent with the long-standing ~20 K/s wall.

Consequence: the **fixed-cap (2× burst) is a real win and KEPT**. audit-D +
local-first reads are CORRECT improvements (align with upstream ForsT, cut real I/O
coupling, tests green) but do NOT fix THIS freeze. The freeze fix is in the SST
read/decode path, NOT I/O:
- Eliminate the per-block `vec![0u8; …]` zeroing (read into uninitialized capacity).
- Decode only the columns the probe needs (key/value/seq), not the whole RecordBatch.
- Reduce blocks touched per probe (the join iterates O(state); cap/stream the scan).
- Re-symbolize the `???` libforst offsets (0x77f48/0x94f0c/0x8ecb4) to name the exact
  decode hotspot before optimizing.
NEXT SESSION: instrument/symbolize the decode path, attack the per-block alloc+decode.

## ★★ ROOT CAUSE FOUND (JM checkpoint stats, data-backed): the freeze is a NON-INCREMENTAL CHECKPOINT
`GET /jobs/<id>/checkpoints` on the frozen q4 job:
- ckpt 1: dur **1.2 s**, state_size **1.7 GB**
- ckpt 2 (fired at exactly +300103 ms = the 300 s interval): dur **97,209 ms (97 s)**,
  state_size **13.66 GB**.
The 97 s checkpoint duration == the freeze window (342 s→~440 s). So the heavy-join
freeze is the **periodic checkpoint snapshotting the ENTIRE 13.66 GB state and blocking
the pipeline for 97 s**: `prepareSnapshotPreBarrier` drains all in-flight async iters
(the Arrow-decode + `malloc_large_huge` the native sample caught) and then materializes
the full state. It grows each checkpoint (1.7 GB→13.66 GB) so the freeze worsens over
the run.

This explains why EVERY config froze at ~64 M (the record count reached when the 300 s
checkpoint fires) and why none of the read-path/alloc fixes moved it — they don't touch
the checkpoint. It also reconciles the earlier signals: the ~114 s "build" and the
`prepareSnapshotPreBarrier` jstack were the checkpoint drain; the malloc was the drain
decoding the inflight backlog.

**THE FIX = INCREMENTAL CHECKPOINT (the upstream-ForsT mechanism the user asked about).**
forst-rs snapshots the WHOLE state every checkpoint; rocksdb (base) and upstream ForsT
checkpoint INCREMENTALLY — only NEW SSTs since the last checkpoint, via
`FileMappingManager` linking/reference-counting (remote→remote links, no re-copy). A
13.66 GB full snapshot every 300 s is the heavy-query drag; an incremental delta would
be small + fast. This is the highest-leverage next change and directly serves the goal's
"Batch Checkpoint" principle. Engine touch points: `db.rs copy_live_ssts` (audit item H —
currently copies whole SSTs in the sync phase) → link/reference unchanged SSTs +
materialize only the new ones, async. Validate q4 crosses 64 M without the 97 s stall,
then G2/G3.

MEASURED NEGATIVES this session (instrument-driven, ruled out as the freeze): larger
resident shadow (4 GB), local-first SST reads, audit-D lock fix, no-zero/reused block
buffer — all correct + kept, NONE fix the freeze because the freeze is the checkpoint,
not the read path. The FRS-UNSORTED-FIXEDCAP burst win (2×) stands.

## EXACT FIX LOCATION + plan (next focused session)
`ForStRsSnapshotStrategy.java:666` calls `createIncrementalCheckpointAtNoflush` (shares
already-flushed SSTs incrementally — cheap) and then `snapshotMemtablesToDir` (line 688)
which uploads the LIVE memtable as **EXCLUSIVE** state EVERY checkpoint (its own comment:
"re-uploaded every checkpoint … NOT shared"). Under checkpoint-without-flush ~all state
is in the memtable → that exclusive memtable artifact IS the 13.66 GB / 97 s freeze. The
SST-incremental machinery (`createIncrementalCheckpointAt`, SharedStateRegistry,
`ForStRsIncrementalKeyedStateHandle`) already exists and works — it's just bypassed for
the memtable.

STATUS: TRIED + REVERTED (2026-06-01) — MEASURED REGRESSION. Switched
`ForStRsSnapshotStrategy:666` to the FLUSHING `createIncrementalCheckpointAt` (+ dropped
the memtable artifact), rebuilt the Java jar (JDK25), deployed, ran q4: it froze at
49.5 M **PERMANENTLY** (640 s+, never recovered) — WORSE than no-flush. ROOT CAUSE: the
flushing checkpoint flushes the live memtable to a new L0 SST and the checkpoint WAITS
for that SST (+ the async upload backlog) to upload to S3 at the ~18 MB/s uplink, vs the
no-flush path writing the memtable artifact to the LOCAL ckpt dir at ~140 MB/s. So on an
S3-state backend, "incremental flush" trades a bounded 97 s LOCAL write for an UNBOUNDED
S3-upload wait → permanent freeze. REVERTED to no-flush.

KEY LEARNING: the checkpoint is bound by (state_size ÷ write-bandwidth). no-flush →
LOCAL ckpt dir (140 MB/s) is the FASTER target; flushing → S3 (18 MB/s) is slower. The
ONLY real lever is REDUCING the snapshot SIZE: make the no-flush memtable artifact
INCREMENTAL (diff vs the last checkpoint's memtable, write only the delta), OR keep the
live memtable small by flushing to LOCAL-cached incremental SSTs during ingest so the
per-checkpoint artifact is small. Both are non-trivial (memtable-diff is novel;
small-memtable hit write-stall at 256mb). The 13.66 GB snapshot size itself comes from
the resident-state strategy (keep state in RAM for fast reads) — which directly inflates
the checkpoint. Fundamental tension: resident state (fast reads, huge ckpt) vs spill
(slow S3 reads — now mitigated by local-first — small ckpt).

PLAN (NOT YET VALIDATED — the flushing approach above FAILED on S3):
1. Switch the snapshot strategy to the FLUSHING variant `createIncrementalCheckpointAt`
   (folds the live memtable into a new L0 SST) and DROP the per-checkpoint
   `snapshotMemtablesToDir` exclusive upload. Now every checkpoint ships only the NEW
   SSTs since the last one (shared, ref-counted) — a small delta, like rocksdb → no 97 s
   stall. Update the restore path to stop replaying memtable artifacts.
2. The reason no-flush existed (slow S3 SST reads → spill collapse) is now cured by the
   **local-first SST reads landed this session** (reads hit local NVMe, not S3) +
   decoded-block cache. So flushing no longer collapses reads.
3. This is a CHECKPOINT-CORRECTNESS change (zero-tolerance) + a Java (maven) rebuild —
   do it carefully in a focused session. Validate: q4 crosses 64 M with checkpoints
   ≤ a few seconds (not 97 s), output byte-identical to rocksdb, then G2/G3, then the
   binding q0–q22 total vs the rocksdb base on the 8c32g/200g envelope.

## Method
Trust ONLY real JM wall-clock / Source write-records rate. Measure every change;
prior micro-opts were neutral. Instrument before guessing — the FRS-ITER-DIAG
build-cost attribution is what turned "the iterator is slow" into two precise,
addressable root causes.

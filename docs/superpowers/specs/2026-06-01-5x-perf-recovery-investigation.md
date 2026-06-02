# forst-rs 5× perf-recovery investigation (2026-06-01)

**Goal:** forst-rs+local+JDK25 ≥ **5×** vs rocksdb+local+JDK17 (total q0-q22); forst-rs+S3+JDK25 = **1–2×** vs rocksdb+local+JDK17. (v3.8 already hit ~3×; target is 5×.)

**Autonomy directive (user, 2026-06-01):** "no more interact until the goal achieves… keep on and try your best… write docs in docs/superpowers for confirms/performances/redesigns." Bisect-against-v3.8 first to pin the regression.

## Status of the non-benchmark goal parts (DONE this session)
- E2E vectorization / batch / zero-copy (V2 multiGet-batched; V1 statebuf/append-merge/chunked-iter/batch-delete) — done + verified.
- All flink/forst GHA green (ForSt ci-rust 8/8, ci-security, ci-cross-engine-bench; flink ci-forst-rs @7c1a6a9ab after reverting a CI-fork-destabilizing test).
- ForsT capability parity incl. **incremental shared-SST checkpoints verified** (ckpt2 referenced 34193 SST bytes / 0 re-uploaded, uploaded only the 920 B delta).

## Benchmark ground truth (local, 100M, this session)
rocksdb-JDK17 baseline (all completed):
| query | rocksdb time | throughput |
|---|---|---|
| q7 (tumble+join) | 941.1 s | 106 K/s |
| q9 (join) | 522.2 s | 191 K/s |
| q16 (join) | 314.1 s | 318 K/s |
| q20 (join) | 417.9 s | 239 K/s |
| q3 (light) | 25.2 s | 3.97 M/s |
| q12 (light) | 32.5 s | 3.08 M/s |

forst-rs-JDK25:
| query | forst-rs | ratio |
|---|---|---|
| q3 | 29.4 s | 0.86× (source-bound — expected, can't drive 5×) |
| q12 | 33.5 s | 0.97× (source-bound) |
| **q7** | **>1200 s → TIMED OUT (watchdog-killed)** | **<0.78×, regressed** |
| q9/q16/q20 | not run (q7 timeout aborted forst-rs side) | — |

**Conclusion: forst-rs is in a heavy-query regression, NOT 5×.** Light queries are source-bound parity (the 5× can only come from heavy joins/windows). q7 timing out reproduces the project's 2026-05-29 finding (0.75× total, 8 heavy-query timeouts) vs the earlier v6f sweep (4.69× total, q7 ~22×).

## Bisect anchors
- **v3.8 baseline = flink commit `e0809571483`** ("size-aware AutoTuner + ValueState", `ForStRsKeyedStateBackend` = 1177 lines — matches the project's v3.8 marker).
- Growth `1177 → 1859` = the "fix(forst-rs-backend): harden …" batches (batch-14 … batch-66) — regression suspects (53+ commits of defensive guards with hot-path cost). This session added `1859 → 2192` (V1 vectorization).
- Per-step git-bisect with a 100M heavy sweep (~1–2h/step) is infeasible → using **hot-path diff + sampling profiler** instead.

## Config facts (config-forst-rs-local.yaml.tpl)
- `table.exec.async-state.enabled: true`, `mini-batch.enabled: false` → **joins use the V2 async (vectorized/batched) path**.
- BUT Flink has **no async-state window operator** (no AsyncStateSlicingSharedWindowProcessor) → **window ops (TUMBLE in q7) fall back to V1-sync per-record aggregation** → prime regression suspect.
- Checkpoint interval 30s EXACTLY_ONCE — matches rocksdb (not the differentiator).
- `setCurrentKey` hot method is lean (serialize-key + bytesEqual + generation bump) — no smoking gun.

## Method
1. **Sampling profiler** (`scripts/profile-q7-jstack.sh`) on a short forst-rs-local q7 run → find the dominant hot frames (window-agg? join? GC? FFM? lock? merge-chain?).
2. Cross-reference the hot frame with the v3.8→HEAD diff of that method → confirm added cost.
3. Strip the per-record hot-path cost; re-validate with a heavy sweep (q7).
4. Iterate; document each finding/decision here.

## Honest constraints
- **S3 1–2× is not validly measurable on this dev Mac** (~10 MB/s uplink to remote BOS → all S3 runs uplink-bound). That target needs the co-located 8c32g box.
- 5× is a deep, partly-structural recovery (V1-sync window path) that prior sessions did not fully close; each validation is a ~1–2h heavy sweep. Progress will be incremental and is documented here.

## Re-validation: the q7 timeout is REAL (not the port artifact)
A zombie NexMark `Benchmark` JVM held metric port 9098, making queries fail-to-bind and hang →
watchdog-killed as "timeout". After killing it + freeing 9098, a clean forst-rs q7 run **still
timed out (>1100s, job RUNNING, warmup done)** vs rocksdb 941s. So forst-rs genuinely has bad
heavy-query performance. (Harness lesson: `pkill run_query.sh` leaves the child Benchmark JVM
holding 9098 + the profiler needs the SQL gateway on 8083 started, not just start-cluster.)

## Profiler result — ROOT CAUSE (jstack, 25 samples of a RUNNING q7)
```
q7 = AsyncStateStreamingJoinOperator (V2 async join path — confirmed, NOT V1-sync)
  VectorizedExecutor.executeBatchRequests   85
    executeIters                            65   ← DOMINANT
      ForStRsDBIterRequest.process          65
        openVecIterIntoBuf                  61
          ForStRsLinker.frsVecIterPrefixOpen 61  (FFM downcall)
    executeGets / vectorizedBatchGet        20
```
**The q7 (and likely q9/q16/q20 join) bottleneck is per-record join-probe prefix-iterator OPENS**
(`frsVecIterPrefixOpen`), not gets or puts. The async join issues a prefix-scan per probe to find
matching records under the join key; each scan OPENS a fresh prefix iterator = a k-way merge over
(active memtable + every L0 SST), seeking each source. The OPEN dominates (61/85), not NEXT (which
is already chunk-batched). As periodic checkpoint flushes accumulate L0 SSTs, each open touches
O(num_L0) sources → the wall (matches the recorded "join probe opens O(num_L0) SSTs" finding).

**Highest-impact lever to investigate:** does the prefix-iterator open BLOOM-PRUNE non-matching L0
SSTs (rocksdb's key advantage on lookups) or blindly seek every SST? If no pruning, every open
pays O(num_L0); adding bloom-prune-on-open would cut it toward O(matching-SSTs). Also: block-cache
reuse of index/data blocks across opens; keeping L0 small via compaction.

## Drill-down 1 — the OPEN build is NOT the cost (microbench, disproves a hypothesis)
`crates/forst-rs-bench/benches/join_probe_open.rs` reproduces the adversarial join shape (records
round-robin across 4096 join keys, flushed per round so every L0 SST spans the full key range →
coarse range-skip cannot prune). Result: the prefix-iterator OPEN build is **flat and cheap** —
~0.7–1.4 µs whether 1 or 128 overlapping SSTs, warm/in-memory. So the per-open bookkeeping
(`live_sst_file_numbers()` HashSet build + `resident_flushed_visible_entries()` clone +
`build_lazy_prefix_key_stream`) does NOT scale with fan-out and is NOT the regression. The build is
also lazy (no block reads — just range checks + `first_block_ge` binary search); blocks are read in
`peek()`/`get_arc` during the DRAIN, after the build returns.

## Drill-down 2 — the cost is the get-per-key DRAIN (confirmed by the code itself)
The FFI open (`frs_vec_iter_prefix_open`) routes through `prefix_scan_iter_owned_arc_with_error_slot`,
whose k-way merge emits **keys only**; for each emitted key it then calls **`db.get_arc(key)`** — a
full second LSM traversal to fetch the value. The SST tier source already decoded the value
(`for_each_row_in_batch` exposes `view.key` AND the value) but discards it; `get_arc` re-reads the
same block to recover it. The in-code note at `db.rs:8352` says it outright: the merge compares are
*"dwarfed by the `db.get(key)` resolve."* So a join probe reads its SST blocks twice. Warm (decoded-
block cache hit) this is just CPU; in the q7 regime (state ≫ RAM) it doubles cold disk block reads.

## Drill-down 3 — the RAM-budget asymmetry (rocksdb parity gap, low-risk lever)
`config-forst-rs-local.yaml.tpl` sets **none** of the `state.backend.forst-rs.*` tuning keys, so q7
runs on engine defaults: **64 MiB write buffer × 4 + 256 MiB block cache ≈ 512 MiB RAM per engine**.
The TM has **12 GiB**. The rocksdb backend, by contrast, funds its block cache + write buffers from
Flink **managed memory** (~0.4 × usable ≈ 4–5 GiB, shared across all slots via
`RocksDBSharedResources`). Two compounding gaps:
1. **Absolute size**: forst-rs's 512 MiB vs rocksdb's ~4–5 GiB. Tiny memtables → constant flushing →
   many cold L0 SSTs; a 256 MiB block cache thrashes against multi-GB join state → the drain's
   block reads miss → cold disk I/O. This is the most plausible q7 regression driver.
2. **Per-instance multiplication**: each `ForStRsStateBackend` opens its OWN engine with its OWN
   cache (`dbOpenRemoteWithOptions(..., blockCacheCapacityBytes, ...)`). With 4 slots × (join+window)
   keyed ops ≈ up to 8 engines on one TM, the cache budget can't simply be scaled up per-instance
   without 8×-ing total RAM. rocksdb shares ONE pool. **forst-rs lacks a process-shared block
   cache + write-buffer-manager (the `RocksDBSharedResources` equivalent)** — a real ForsT/RocksDB
   capability gap and the principled fix.

**Plan:** (a) confirm via `FRS_ITER_DIAG=1` time-boxed q7 (`scripts/diag-q7-iter.sh`) that builds
are fast + how many SST sources per probe; (b) modest, safe RAM bump in the template (bigger write
buffer to cut flush frequency + larger block cache, bounded by the WBM cap so total stays well under
12 GiB) and re-measure q7 vs the 941 s rocksdb baseline; (c) if that confirms the lever, design a
process-shared cache/WBM (one budget across all engine instances) for true rocksdb parity.

### Diag result (FRS_ITER_DIAG=1, ~200 s of q7 steady state)
- **Only 2 builds exceeded 1 ms** in the whole window → the prefix-iterator OPEN build is fast in
  ~every probe (confirms the microbench; build is exonerated as the regression).
- Both slow builds: `us=60262` / `us=46662` (**46–60 ms**) with `sst_sources=1, mem_sources=0,
  resident_shadowed=16`. ONE SST source, not fan-out — so the 46–60 ms is a **cold SST-reader open**
  (`get_or_open_sst_reader` reading footer+index, the first probe to touch a freshly-flushed SST
  before its local-cache write-through is warm). Rare (2 in 200 s) but on the critical path; the 30 s
  checkpoint cadence force-flushes → a new SST → a periodic cold-open stall. Worth a follow-up
  (prewarm the reader at flush-completion) but NOT the steady-state driver.
- Net: steady-state cost is the DRAIN (per-probe block reads + `get_arc`), unmeasured by the build
  timer. `resident_shadowed=16` ⇒ ~1 GiB of recently-flushed state is being kept in RAM per engine,
  i.e. the 64 MiB write buffer is flushing ~every 64 MiB → many small SSTs. This is exactly what the
  RAM bump (drill-down 3) targets: bigger write buffer ⇒ fewer flushes ⇒ fewer SSTs/cold-opens;
  bigger block cache ⇒ the drain's repeat block reads hit RAM.

### Applied change (config-forst-rs-local.yaml.tpl, under `state.backend.forst-rs`)
```yaml
      writebuffer:
        size: 256mb        # was 64mb default → flush ~4× less often → ~4 resident memtables not 16
        count: 3
        manager:
          capacity: 512mb  # caps active+immutable so total RAM stays bounded
      cache:
        block:
          capacity: 512mb  # was 256mb default → drain's repeat block reads hit RAM
```
Per-engine RAM ≈ ≤512 MiB memtable + 1 GiB resident shadow + 512 MiB block cache ≈ 2 GiB; safe
across slots on the 12 GiB TM. Validated by `scripts/validate-q7.sh` (runs q7 to completion, reports
wall time vs the 941 s rocksdb baseline). Zero engine-code risk — pure config.

### CORRECTION — the drain is CPU-bound, NOT I/O (block cache hit ≈ 100%)
The first tuned run printed `FRS-CACHE-STATS: hits=… misses=~21k hit_rate=99.9–100.0%` continuously.
So the decoded-block cache was **already saturated at the 256 MiB default** — the join-probe drain's
repeat block reads are RAM hits, not disk I/O. This **disproves drill-down 3's "tiny cache thrashes"
sub-hypothesis** and means:
- The block-cache bump was pointless (reverted to default).
- The drain cost is **CPU**: `get_arc` walking ~16 resident tiers per key (the `resident_shadowed=16`
  signal). The write-buffer bump (256 MiB ⇒ ~4 tiers) is still the right lever — but it attacks
  per-`get_arc` CPU, not I/O. The deeper CPU fix is the value-carrying merge (drop the second
  traversal entirely).

### Measurement note — use `scripts/measure-completion.sh`, not the nexmark metric reporter
The nexmark `Benchmark` CPU-metric monitor is broken on this box (`Current Cores=0 (0 TMs)` for the
whole run even though the TM is alive — cache stats flowing). `scripts/measure-completion.sh`
(built 2026-05-29 for exactly this) ignores the metric client and polls the Flink JM REST for the
job's FINISHED wall-clock `duration` + verifies the source emitted 100 M records. My hand-rolled
`validate-q7.sh` reinvented this badly (and used the broken monitor) — removed. Tuned q7 is being
re-measured via `QUERY=q7 CONFIG=forst-rs-ffm-local measure-completion.sh`.

## ★★★ DEFINITIVE ROOT CAUSE (2026-06-02) — checkpoint blocks on compaction uploads ★★★
Hard JobManager `CheckpointCoordinator` evidence from a clean q7 measurement job (`c1fd1e3…`):

| ckpt | state size | duration |
|---|---|---|
| 1 | 560 MB | **1764 ms** |
| 2 | 992 MB | **1553 ms** |
| 3 | 1268 MB | **332272 ms (332 s!)** — `request time in queue: 302273` |

State grew only +276 MB from ckpt 2→3, but duration exploded **~200×**. Checkpoint 3 is the first
one that overlaps a **major L0→L1 compaction**. Cause (confirmed in code): the async snapshot calls
the *flushing* `createIncrementalCheckpointAt` → `create_incremental_checkpoint_impl` →
**`self.fs.await_all_uploads()`**, and `await_all_uploads` (opendal_backend.rs:1449) **drains EVERY
in-flight upload** — including background **compaction** output SSTs that are NOT part of this
checkpoint's pinned version. So once compaction starts (~3rd checkpoint), each checkpoint stalls for
the entire compaction-upload duration (~330 s). With a 30 s interval that means the pipeline is
frozen ~95% of the time → q7 never finishes. This is the heavy-join "freeze."

**This overturns every prior sub-hypothesis** (get_arc drain CPU, block-cache thrash, resident-tier
count). Those were red herrings sampled during the brief fast windows. It also answers the headline
question: **v3.8/v6f's q7 "8×" was measured ckpt-OFF**; the ckpt-ON requirement exposes this
checkpoint↔compaction coupling. v3.8 (flink `e0809571483`) already had this exact snapshot strategy,
so it is NOT a v3.8→HEAD code regression — it is a latent checkpoint-architecture defect that only
bites under ckpt-ON + heavy compacting state.

### Fix design (checkpoint-correctness-sensitive — needs review before landing)
The checkpoint pins the SSTs live at `snapshot.seq` (`deletion_guard.pin_batch`) and flushes the
memtable to new SSTs that ARE in its version. It MUST await the uploads of **only those files** so
the manifest never references an un-uploaded SST. It must NOT wait for unrelated background
compaction uploads (their outputs post-date / aren't in the pinned version).
- **Targeted fix (preferred):** replace the blanket `await_all_uploads()` in
  `create_incremental_checkpoint_impl` with a per-file `await_upload(path)` loop over exactly the
  file numbers in `version_snapshot.version.live_sst_files()`. Correctness-preserving (still awaits
  everything the manifest references) and decouples checkpoint latency from compaction I/O.
  RISK: if a referenced SST's upload is registered under a path the loop misses → manifest references
  an un-uploaded file → restore data loss. Must enumerate the pinned set exactly + add a restore test.
- **Alternative:** wire the existing `frs_create_incremental_checkpoint_at_noflush` + capture the
  live memtable as an Arrow-IPC artifact (keeps memtable RAM-resident, skips the flush+upload). Larger
  change; the no-flush memtable artifact is itself re-uploaded each ckpt (non-incremental for the
  memtable delta) — the true-incremental-memtable design is the harder follow-up.
- Secondary: investigate why a LOCAL (file://) compaction upload of ~1 GB takes ~330 s (~3 MB/s) —
  the OpenDAL file-backend write path may itself be pathologically slow / serialized, which would be
  an independent win (and aligns with the no-per-record / no-copy mandate on the I/O path).

NOTE: the heavy-query 5× lever is THIS (checkpoint↔compaction decoupling), not the join drain. The
join itself runs at 270–370K/s (rocksdb-competitive) between checkpoints. Diagnostic harness bug
found + to fix: `diag-ckpt-long-q7.sh` latched onto the nexmark **warmup** job id (canceled after
120 s) instead of the measurement job — track the job whose source emits the most records instead.

## ✅ FIX #1 LANDED + VALIDATED (2026-06-02) — checkpoint↔compaction decoupled
Commit `a76a52f8a` (engine). create_incremental_checkpoint_impl now awaits only its pinned
version's SST uploads (per-file `await_upload` over `live_sst_files`, after the snapshot+pin); the
blanket `await_all_uploads()` was removed there AND from `wait_for_pending_flushes` (now flush-
counter-only; shutdown durability moved to an explicit await in `DbImpl::drop`). TDD:
`test_incremental_checkpoint_awaits_only_referenced_ssts` (RED await_all_count=2 → GREEN 0); full
engine suite 256/256.
**Validated on q7 (clean measurement job):** checkpoint 3 — the one that was **332 s** — now
completes in **1157 ms** at 1.16 GB (ckpts 1–3 all ~1.1–1.3 s). The 332 s freeze is GONE (~286×).

## SECOND wall exposed (2026-06-02) — local SST cache cap = 1 GB
With the checkpoint freeze fixed, q7 STILL doesn't finish: throughput collapses to ~100/s at a hard,
reproducible ceiling of **~21M records** (identical before and after fix #1 → independent
bottleneck the freeze was masking). Correlation: ckpt3 state = 1.16 GB right at the wall, and
`state.backend.forst-rs.storage.cache-capacity-mb` defaults to **1024 MB**. Hypothesis: state
crosses the 1 GB local NVMe SST cache → reads hit the eviction-fallback open (which awaits the slow
OpenDAL open) → collapse. Mirrors the recorded q9-collapse (fixed there with a large cache).
ACTION: bumped `cache-capacity-mb: 65536` in the local template; re-validating q7. If confirmed,
the deeper question is why a LOCAL (file://) cache-miss read is slow at all — the eviction-fallback
should read local NVMe directly, not await an upload (independent engine win, aligns with the
no-per-record/no-copy I/O mandate).

### SECOND wall — cache size was NOT it (2026-06-02 update)
Bumping `cache-capacity-mb` 1024→65536 moved the wall only marginally (~21M→~23M) and q7 still
collapses to ~100/s and does not finish (ran to MAXSEC). So the local SST-cache size is not the
cause. A **jstack of the live stalled job** shows: join thread sits in `frsVecIterPrefixOpen` (the
FFM downcall into Rust), block-cache hit rate still **99.9%** (not block-decode), most other threads
parked. jstack can't see into native code, so the Rust side is blocking on something Java stacks
can't resolve. Leading hypothesis (consistent with the 46–60 ms cold reader-opens seen via
FRS_ITER_DIAG + the 30 s ckpt force-flush minting L0 SSTs): **slow OpenDAL local I/O (~3 MB/s
measured for the ~1 GB checkpoint copy) starves compaction → L0 SSTs accumulate unbounded → each
join probe opens ever more overlapping SST readers → ~100/s collapse.** NEXT STEP (deliberate, not a
blind run): add native timing inside `frs_vec_iter_prefix_open` to split build vs drain vs
reader-open, and instrument the OpenDAL file-backend write/read latency — the root may be a
pathologically slow OpenDAL local path (independent engine win; aligns with the no-per-record/
no-copy I/O mandate). The checkpoint fix (#1) stands as a validated, banked win regardless.

## ★ SECOND wall ROOT CAUSE (2026-06-02) — L0 fan-out on EMPTY join probes
`FRS_PROBE_DIAG` split of `frs_vec_iter_prefix_open` over ~480 s of q7 (23,090 slow probes >5ms):
```
total_us  p50=46086  p90=79066  max=186307
build_us  p50=24     p90=38     max=275      (negligible)
fill_us   p50=46057  p90=79040  max=186287   <-- 99% of cost
```
Every slow probe has **rows=0** — they are EMPTY join probes (a join key with no matching records)
that still take **46 ms**. The merge BUILD is lazy/cheap (24µs); the cost is the first-chunk FILL,
which must `peek()` every overlapping L0 SST source — reading/scanning each one's first in-range
block — only to find nothing. `may_contain_range` (coarse per-block key-range index) lets wide L0
blocks that straddle the 37-byte prefix through as false positives, so the probe fans out across
many L0 SSTs. As L0 accumulates (the 30 s checkpoint force-flush mints a new L0 SST each time +
compaction not keeping up), fan-out grows → 46 ms per empty probe → the ~100/s collapse. Confirms
the recorded "join probe opens O(num_L0) SSTs" wall, now precisely localized to the FILL/peek path.

**Fix candidates (decreasing leverage / increasing risk):**
1. **Bound L0** so fan-out stays small — the real lever. Either (a) the checkpoint stops minting L0
   every 30 s (wire `frs_create_incremental_checkpoint_at_noflush` + capture the live memtable as an
   Arrow-IPC artifact, so the hot memtable is NOT sealed to a new L0 SST each checkpoint — review-
   gated, restore must reconstruct the artifact), or (b) more aggressive L0→L1 compaction (a
   level0_file_num_compaction_trigger) so L0 never grows deep. Need to confirm what currently bounds
   L0 and whether compaction is keeping up.
2. **Prefix bloom on SSTs** so an empty prefix probe skips non-matching L0 SSTs without a block read
   (RocksDB prefix-bloom equivalent; full-key blooms don't answer prefix queries) — larger engine
   change.

## ✅ FIX #2 LANDED + VALIDATED (2026-06-02) — upper-bound early-termination
Commit `34f24163a` (engine + reader). `TierKeySource::Sst::peek` now stops at the first key `>= upper`
(marks the source drained) instead of skipping `>= upper` rows and reading the next block to EOF. TDD:
`test_empty_prefix_scan_stops_at_upper_bound` (restore-fresh straddling SST, empty gap probe) RED 16/31
blocks → GREEN ≤3. Suites: engine 257, storage 331, ffi 96. Added `SstReaderImpl::blocks_read()`.

### q7 end-to-end with BOTH fixes — the wall is BROKEN
| metric | before fixes | after fix#1+#2 |
|---|---|---|
| ckpt 3 duration | 332 s | 1.16 s |
| join throughput | ~100/s (collapsed) | **~8–16K/s sustained** |
| records before stall | hard wall ~21M | **42.4M @ 1300 s, still progressing** |

**~100× throughput recovery** on the q7 ckpt-ON join. q7 no longer freezes/collapses. BUT it still does
not FINISH within 1300 s, and steady-state ~8–10K/s is still ~10–13× below rocksdb's 106K/s, and it
**decays** as state grows (16K→7.7K from 37M→42M). The decay = the LSM deepening + L0 SSTs still being
minted by the 30 s checkpoint force-flush (`createIncrementalCheckpointAt` flushes the memtable every
checkpoint) faster than size-only compaction drains them → per-probe source/level count creeps up.

**Next lever (review-gated): the no-flush checkpoint.** Wire `frs_create_incremental_checkpoint_at_noflush`
+ capture the live memtable as an Arrow-IPC artifact, so checkpoints STOP sealing the memtable into a new
L0 SST every 30 s. This should flatten the throughput decay. Restore-sensitive (must reconstruct the
memtable artifact) → a focused TDD+restore-test session. Secondary: an L0-file-count compaction trigger
(compaction is currently size-only) to bound fan-out; and the OpenDAL local I/O speed.

## No-flush checkpoint — foundation PROVEN, Java wiring scoped (2026-06-02)
The next lever for the q7 throughput decay is the no-flush checkpoint (stop minting an L0 SST every
30 s; capture the live memtable as an Arrow-IPC artifact instead). Status:
- **Engine + FFI: complete.** `create_incremental_checkpoint_noflush`, `snapshot_memtables_to_dir`,
  `open_from_incremental`, `replay_memtable_artifacts_from_dir` all exist; FFI exports + **Java
  `ForStRsLinker` bindings exist** (`frs_snapshot_memtables_to_dir`, `frs_replay_memtable_artifacts`,
  `createIncrementalCheckpointAtNoflush`).
- **Foundation PROVEN (commit `9590ff91b`):** `test_noflush_checkpoint_combined_restore_round_trip`
  — 50 SST-resident + 50 live-memtable keys → no-flush checkpoint + memtable artifact →
  `open_from_incremental` + `replay_memtable_artifacts_from_dir` → all 100 keys restored. The
  data-loss-sensitive combined restore is verified; neither tier is lost.

**Remaining: Java wiring (3 sites + 1 design decision).** Review-gated — this is the redesign the
user set up brainstorming for.
1. `ForStRsSnapshotStrategy.asyncSnapshot` (line ~660): swap `createIncrementalCheckpointAt` →
   `createIncrementalCheckpointAtNoflush`; after it, call `snapshotMemtablesToDir(db, stagingDir,
   snapshotSeq)` and upload the per-CF `memtable-cf<id>.arrow` artifacts.
2. **Design decision**: how the memtable artifact rides in the checkpoint handle. Cleanest fit is a
   **private-state entry** in the `IncrementalRemoteKeyedStateHandle` (alongside SST private state),
   so it is exclusive to this checkpoint (the live memtable is NOT shared/incremental across
   checkpoints — it is re-captured each time; that re-upload cost is the known no-flush tradeoff vs
   the L0-fan-out it removes — measure it).
3. `ForStRsRestoreOperation` (line ~75/260): after `dbOpenFromIncremental` (SSTs), download the
   artifact private-state and call `replayMemtableArtifacts(dir)`.
Then: Java snapshot+restore roundtrip test, build+deploy dylib, re-validate q7 (expect the
throughput decay to flatten — fewer L0 SSTs minted per checkpoint).

### Honest scale check
rocksdb q7 = 941 s (106 K/s); forst-rs ≈ >1200 s (<83 K/s) ⇒ a **~1.3× regression**, not a 10×
collapse (the watchdog "timeout" is >1200 s, but the job was progressing). Reaching the 5× TOTAL
goal is structurally hard: light queries (q0-q3) are source-bound parity and cap the average, so the
5× must come from heavy queries running multiple× FASTER than rocksdb — forst-rs must genuinely beat
rocksdb on joins, not just reach parity. The RAM bump + cold-open prewarm + (eventually) the
shared-cache + value-carrying-merge are the levers; each needs a ~20 min heavy run to validate.

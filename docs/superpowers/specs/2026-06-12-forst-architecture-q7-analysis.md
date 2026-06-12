# ForSt (C++) vs forst-rs — q7-class architecture deep-dive + remote q7 pin status

Date: 2026-06-12 · Author: PMC architect agent · Worktree: ForSt fork @ upstream `main` (783f0fcb1)

Scope: WHY does ForSt (C++, RocksDB-derived) beat forst-rs on q7-class workloads
(NexMark q7 ≈ interval/window join = millions of short prefix probes + heavy
write/compaction churn), and what is ForSt's REAL q7 @8c/32g on the binding remote box.

Codebases cited:
- **ForSt C++**: this worktree (upstream ForSt `main` — RocksDB layout: `db/`, `table/`, `util/`, plus ForSt-specific `env/flink/`).
- **ForSt Flink backend (Java)**: `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst` (read-only).
- **forst-rs**: `/Users/lijunqing/Code/stczwd/ForSt/crates/*` (branch `forst-rs`) + `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`.

---

## 0. Executive summary

The q7 gap is **not** JNI-vs-FFM boundary cost (ForSt's iterator path crosses JNI
*per entry*; forst-rs drains chunks per crossing — forst-rs is *better* at the boundary),
and — per the S2 falsifier — **not** probe-merge CPU. The three deltas that remain standing,
ranked:

1. **H1 — Sorted-run discipline (read-amp → I/O) under churn.** RocksDB/ForSt's leveled LSM
   bounds the per-probe source fan-out (L0 stalls at 20/36 files; L1..Ln are non-overlapping,
   one `LevelIterator` per level). forst-rs tolerates L0 up to 40/64 plus a size-tiered
   L1..Ln plus a resident-shadow tier — per-probe open is O(all overlapping SSTs).
   At remote-NVMe scale this turns q7 into an I/O-stall workload (io_uring = finish-vs-DNF).
2. **H2 — Pipelined probe depth.** ForSt's executor = 1 coordinator + 3 read-IO threads,
   non-blocking, in-flight depth ≥ 3. forst-rs default = depth-1 inline `VectorizedExecutor`
   on the mailbox thread — every probe's I/O stall sits on the critical path. H1×H2 compound:
   ForSt *hides* its (smaller) per-probe I/O behind parallelism; forst-rs serializes its
   (larger) per-probe I/O.
3. **H5 — C++ engine constant factor** (diffuse per-op cost: sync C++ get/iter on reader
   threads vs tokio task-per-op + FFM dispatch) — the same class as the proven q4 1.39×
   "no single lever ≥2%" residue.

Remote ForSt q7 pin: **REMOTE-PIN-PENDING** (see §5 — jar found+deployed mid-session by the
q5-trio agent, but the bridge stayed at its 3-agent cap with two long runs in flight for the
whole session). Mac reference: **ForSt 586.8 s vs forst-rs 1441.6 s (2.46×)**.

---

## 1. q7 evidence ledger (recorded, all same-population unless noted)

| Cell | Result | Source |
|---|---|---|
| q7 remote 8c/32g, frs `FRS_IO_URING=on` | **FINISHED 2376.4 s**, out_rows 92,000,002 | SWEEP:2142-2148 (`docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md`, main checkout) |
| q7 remote, frs `FRS_IO_URING=off` | **DNF @2400 s** | SWEEP:2146 |
| q7 remote, RocksDB | **1367.6 s**, out exact == frs | SWEEP:2147 → ratio **1.74×** |
| q7 Mac, ForSt (forst-local) | **586.8 s** | master-strategy §A table row q7 (`2026-06-12-master-strategy-beat-forst-rocksdb.md:41`) |
| q7 Mac, frs best | 1441.6 s | same row → **2.46× vs ForSt-Mac** |
| q7 S2 (loser-tree + pinned rows) falsifier @100M | **+3.1% only** (ON 75.63M vs OFF 73.33M events, ratio-fair pair) — 6-9× probe-open micro win did **not** transfer | SWEEP:2274-2284 |
| jstack during q7 | 61/85 executor samples in `openVecIterIntoBuf → frsVecIterPrefixOpen` | master-strategy:115-116 |
| B1 micro | batched probe-open ≈ 104.2 µs/probe, FFM crossing ≈ free — cost IS engine scan-build | master-strategy:117-118 |

Interpretation chain: jstack said "probe-open dominated" → S2 attacked the open's merge CPU
and won 6-9× in micro → q7@100M moved only +3.1% ⇒ **the wall is the I/O/stall regime or the
framework floor, not open CPU** (SWEEP:2278-2280). io_uring's finish-vs-DNF independently
proves a real I/O-stall component. A q7 re-profile agent is capturing the real remote 100M
hot path right now (perf/pidstat/iostat → `/ssd2/jackylee/frs-bench/logs/q7prof-*`); its
output adjudicates H1-H4 below.

**Late-breaking corroboration (q7-profile agent, observed in `/tmp/relay-out.log` ~15:31,
marker Q7P32, mid-run frs q7 @1209 s / 52.0M events):** iostat on the bench disk
(`nvme2n1`) shows **98-99% utilization** — ~190-292 MB/s reads AND **435-682 MB/s writes**
(await up to 38 ms, queue ~96-230), box avg-cpu ~18% idle / 8-10% iowait. The frs q7 run is
**disk-saturated, and write volume exceeds read volume** ⇒ first direct evidence for
H1 (read-amp) **plus a larger-than-expected H4 component (flush/compaction write-amp)**;
CPU-side hypotheses (H2/H5) cannot be primary while the disk is the binding resource —
H2 still matters as latency-hiding once volume is reduced. The agent's perf flat/graph
reports (`q7perf-tm{1,2}-{flat,graph}.txt`) will complete the picture.

---

## 2. ForSt architecture, q7-relevant (evidence-cited)

### 2.1 Execution model (Flink backend, Java)

- **Thread topology**: 1 coordinator thread + dedicated read-IO pool + write path inline on
  the coordinator. `ForStStateExecutor.java:84-146` (pools), defaults
  `state.backend.forst.executor.read-io-parallelism = 3` (`ForStOptions.java:297-300`),
  `write-io-inline = true` (`ForStOptions.java:286-289`), coordinator not inline
  (`ForStOptions.java:275-278`).
- **Batching/JNI for GETs**: the AEC batch is classified, then split into
  `readIoParallelism` sub-batches (`ForStGeneralMultiGetOperation.classifyAndSplitRequests`,
  `ForStGeneralMultiGetOperation.java:182-191`); each sub-batch is **one JNI crossing**:
  `db.multiGetAsList(readOptions, cfs, keys)` (`:117`) on a read thread, with
  `readahead_size=0` (`:84-85`).
- **Iterators (the q7 probe path)**: one request = new `RocksIterator` + `seek(prefix)`
  (`ForStDBIterRequest.java:116-122`), then **per-entry JNI**: `isValid()/key()/value()/next()`
  loop capped at `CACHE_SIZE_LIMIT = 128` entries per `process()`
  (`ForStIterateOperation.java:34`, `ForStDBIterRequest.java:127-142`). Each iter request runs
  as its own task on the read pool (`ForStIterateOperation.java:66-94`) ⇒ **probe-level
  parallelism = 3**, per-entry crossing cost accepted.
- **Pipelining/backpressure**: `fullyLoaded()` returns true only when ongoing read
  sub-processes ≥ readThreadCount (`ForStStateExecutor.java:288-291`) — the AEC keeps
  **multiple batches in flight**; the mailbox is never blocked by state I/O.

**forst-rs contrast**: default executor is **depth-1 inline** — every batch runs on the
mailbox thread and returns an already-completed future
(`ForStRsAsyncKeyedStateBackend.java:1206-1219`: "Default REMAINS inline until the PR-1
Task-5 gates pass"; the in-flight-depth==1 constraint documented at
`RoutingStateExecutor.java:55-58`). The parallel models exist but are opt-in:
`RoutingStateExecutor` (synchronous, N=3 workers to match ForSt,
`RoutingStateExecutor.java:83-86`) and `CoordinatedStateExecutor` (the non-blocking ForSt
model, "target default once Task-5 gates pass"). At the FFM boundary forst-rs is *more*
batched than ForSt (chunked `frsVecIterPrefixOpen/Next` drain, N entries/crossing) — so the
boundary itself is exonerated; the missing piece is **cross-probe overlap**, not crossing cost.

### 2.2 Data layout

| Axis | ForSt (C++) | forst-rs |
|---|---|---|
| Block format | 4 KiB blocks, restart-point prefix compression (`include/rocksdb/table.h:276`; Flink sets blocksize 4kb, metadata 4kb — `ForStConfigurableOptions.java:263-274`) | Arrow-flavored KV data blocks + 16-byte header (`crates/forst-rs-storage/src/sst/{kv_block,data_block,block_header}.rs`) |
| Index | binary-search index block; **partitioned index/filters default ON in Flink** (`ForStOptions.java:228-235` `USE_PARTITIONED_INDEX_FILTERS` default true; pinning tiers `table.h:90-178`) | sparse index: one `SparseIndexEntry` (last_key/offset/size) + `BlockStats` (min/max key, seq range) per data block (`sparse_index.rs:26-54`) — structurally similar |
| Filters | bloom available but **Flink default OFF** (`ForStConfigurableOptions.java:296-299` `USE_BLOOM_FILTER=false`); memtable prefix bloom default OFF (`advanced_options.h:472` ratio=0.0, `db/memtable.cc:125-129`) | prefix bloom v3 + N14 depth-gated **shipped default-ON** (master-strategy B.1) ⇒ filters are NOT ForSt's edge — if anything frs has more |
| Memtable | `InlineSkipList` lock-free concurrent insert (`memtable/inlineskiplist.h:61`); 64 MB × 2 buffers (`ForStConfigurableOptions.java:239-250`) | sharded BTreeMap + prefix-index cursors (`forst-rs-engine/src/db.rs:6845-6860`), plus a **resident-flushed shadow tier** with no native index gate (`db.rs:6870-6960`) |
| LSM shape | **leveled** (Flink `COMPACTION_STYLE` default LEVEL, `ForStConfigurableOptions.java:143-146`); L0 trigger 4 / slowdown 20 / stop 36 (`advanced_options.h:583,590`); L1..Ln **non-overlapping** ⇒ scan = ≤4 L0 sources + **one `LevelIterator` per level** (`db/version_set.cc:939`); target file 64 MB, level base 256 MB (`ForStConfigurableOptions.java:215-230`) | L0 rollup + **size-tiered L1..Ln** (`forst-rs-engine/src/compaction.rs:21`); L0 trigger 4 but slowdown **40** / stop **64** (`write_controller.rs:95-97`) ⇒ under q7 churn the sorted-run count runs far deeper; probe-open enumerates **every overlapping SST** (`db.rs:7034 overlapping_ssts_in_range_for_cf`) |
| Compaction | leveled, overlap-bounded inputs, `max_background_jobs=2` default (`ForStConfigurableOptions.java:72-75`), snappy per level (`:177-181`) | per-CF jobs, single output stream per job (`compaction.rs:23-26`), shared bg pool; history: trigger=4 alone regressed q7 248K→13K when one bg thread couldn't keep up (`write_controller.rs:74-84` comment) |

### 2.3 Read path

- **Block cache**: sharded LRU with a high-priority pool (default ratio 0.5,
  `include/rocksdb/cache.h:237`) for index/filter blocks
  (`table.h:149-178`); Flink sizes it from **slot managed memory** (default
  `state.backend.forst.memory.managed=true`, `ForStOptions.java:171-178`; write-buffer ratio
  0.5, high-prio 0.1, `:206-225`) — i.e. the cache is *large* in production, not the 8 MB
  standalone default (`ForStConfigurableOptions.java:280-283`).
- **Prefetch**: `FilePrefetchBuffer` auto-readahead ramps only after
  `num_file_reads_for_auto_readahead` (=2) sequential reads in a file, then doubles up to
  `max_auto_readahead_size` (`table/block_based/block_prefetcher.cc:99-137`) — short prefix
  probes (a handful of `next()`s) deliberately **don't** pay readahead; Flink's multiGet also
  pins `readahead_size=0`.
- **MultiGet coalescing**: batches of ≤32 keys (`table/multiget_context.h:103`), batch bloom
  probe `FullFilterKeysMayMatch` (`table/block_based/block_based_table_reader.cc:2389`) and
  coalesced data-block retrieval per SST.
- **Bloom-before-index shortcuts** exist (`FullFilterKeyMayMatch`
  `block_based_table_reader.cc:2033`, `PrefixRangeMayMatch` `:1916`) but are inert under
  Flink defaults (no filter policy, no prefix extractor configured —
  `ForStResourceContainer.createBaseCommonColumnOptions` `:474-476` sets nothing).
- **OS page cache as L2**: ForSt reads via buffered POSIX I/O (`env/fs_posix.cc`,
  `file/file_prefetch_buffer.*`) ⇒ the *whole box's* free RAM caches SST bytes under the
  block cache. forst-rs reads ride opendal + its own caches
  (`forst-rs-storage/src/{cached_fs,local_cache}.rs`) + `BlockPrefetcher` pool
  (`sst/prefetch.rs:285`) + optional io_uring backend (`forst-rs-io-uring/src/lib.rs:15-36`).
- **Disaggregation**: ForSt's remote-FS hook is `FlinkFileSystem : FileSystemWrapper`
  bridging to a Java `FileSystem` via JNI (`env/flink/env_flink.h:31`) with a Java-side
  `FileBasedCache` LRU of local file copies (`...forst/fs/cache/FileBasedCache.java:59`);
  in `forst-local` mode (the 586.8 s Mac cell) the primary path is local disk — none of the
  remote machinery is in the hot path.

### 2.4 The per-probe open, side by side (the q7 inner loop)

ForSt: `seek(prefix)` on an `ArenaWrappedDBIter` (`db/arena_wrapped_db_iter.h:36`) →
`MergingIterator` (`table/merging_iterator.cc:52`) over [memtable + imm + ≤4-36 L0 iterators +
1 `LevelIterator` per level]; each SST seek = (cached, partitioned) index binary search +
one block-cache lookup; subsequent `next()` = heap pop.

forst-rs: `build_lazy_prefix_key_stream` (`forst-rs-engine/src/db.rs:6766-7050`) assembles,
**per probe**: active-memtable cursor (`:6845-6860`) + one cursor per imm memtable
(`:6866-6873`) + a clone of ALL resident-flushed shadow entries under a RwLock
(`resident_flushed_visible_entries`, `:6915-6920` — O(N) per probe, bloom/range-gated since
FRS-RESIDENT-BLOOM-SKIP `:6975-6990`) + the overlapping-SST loop (`:7034`). The code itself
names the gap: the per-probe cost decomposition diags (`FRS-A-SPLIT`, `:6810-6824`) exist
precisely because A_fanout = locate + per-SST loop grows with sorted-run count, and
`db.rs:6245` documents the bg_read_pool batched-open as "overlapping the per-probe LSM
build+drain across cores (ForSt's [model])".

---

## 3. Ranked root-cause hypotheses (each with its falsifier)

Constraints any hypothesis must satisfy: (a) S2 falsifier — probe-merge CPU is NOT the wall
at 100M (+3.1% only); (b) io_uring A/B — there IS a first-order I/O-stall component
(finish-vs-DNF); (c) frs ≈ exact-output parity (correctness exonerated); (d) ForSt-Mac is
2.46× faster than frs-Mac — so part of the gap survives even on a fast local disk.

**H1 — Sorted-run / read-amp discipline under churn (top, ~weight 35%).**
ForSt leveled shape + write-stall economics keep per-probe sources ≈ (≤4 L0 + #levels);
forst-rs runs L0 up to 40-64 (`write_controller.rs:95-97`) + tiered L1..Ln + resident
shadow ⇒ each of q7's ~10⁸ probes touches a multiple of the files/blocks ⇒ I/O-volume
multiplier that explains (b) and degrades with state size.
*Falsifier:* during remote q7, dump the already-instrumented `n_overlap`/`n_overlap_l0`
histograms (`db.rs:373-399`, FRS-A-SPLIT) + iostat read-bytes/event vs RocksDB (the running
q7-profile capture includes pidstat/iostat). Then A/B `FRS_L0_SLOWDOWN_TRIGGER=20
FRS_L0_STOP_TRIGGER=36` (+1 compaction thread). If fan-out is already ≈ RocksDB's and read
bytes/event are at parity → refuted.

**H2 — In-flight probe depth = 1 (weight ~30%).**
ForSt overlaps ≥3 probe batches (read pool) and never blocks the mailbox
(`fullyLoaded`); forst-rs default depth-1 inline puts every block-miss stall on the
critical path. Compounding with H1: same stall count × no overlap. The class is proven
elsewhere (q11 318.9→135.7 s under routing; q9 TM at 1.7/8 cores = latency-bound,
master-strategy:109-111).
*Falsifier:* remote q7 under `FRS_RS_EXECUTOR=coordinated` (or `adaptive`) vs `inline`,
same session. No improvement → refuted. (Cheap: env-var only; correctness pre-validated
routing-async ×5 exact post timer-fix.)

**H5 — C++ engine constant factor / sync-thread dispatch (weight ~15%).**
ForSt executes gets/iters as plain synchronous C++ calls on its reader threads; forst-rs
pays tokio task-per-op + async coordination + FFM completion plumbing per batch — the same
diffuse per-record overhead that capped q4 at 1.39× with "no single lever ≥2%" (memory:
2026-06-06 rigorous A/B). Explains the *Mac* 2.46× residue more than the remote DNF.
*Falsifier:* the remote ForSt q7 pin (§5) + the q7-profile perf flat report: if frs TM CPU
is dominated by executor/runtime frames rather than I/O wait, H5 (and H2) rise; if cores are
idle and iostat is saturated, H1 rises.

**H3 — Page-cache leverage delta (weight ~10%).**
RocksDB/ForSt buffered preads make free box RAM an automatic second-level block cache with
kernel readahead; forst-rs's opendal/own-cache path may re-read bytes the kernel would have
kept. *Falsifier:* `cachestat`/`vmstat` page-cache hit during remote q7 + engine read-bytes
counter vs physical-read bytes. Parity → refuted.

**H4 — Write/compaction interference (weight ~10%, RISING — see Q7P32 iostat above:
write MB/s > read MB/s at 99% disk util mid-q7).**
q7's churn keeps flush+compaction continuously active; frs compaction merge floor is
6.5-6.8 ns/byte idle (B3) but the recorded live number was 21 ns/byte under saturation, and
compaction shares the same disk the probes stall on. RocksDB bounds this with 2 bg jobs +
stalls; frs history shows the exact failure mode (q7 248K→13K decay,
`write_controller.rs:76-84`). *Falsifier:* iostat write-volume share + L4
compaction-windowed A/B (L4-SPEC) on q7; if probe latency is uncorrelated with compaction
bursts → refuted.

**Refuted / closed (do not re-chase):** probe-merge CPU (S2, SWEEP:2274-2284); JNI/FFM
crossing mechanism (ForSt is per-entry JNI on iters — `ForStDBIterRequest.java:127-142` —
yet 2.46× faster; also the 2026-05 JNI-experiment memory); bloom-filter availability (Flink
ForSt default has bloom OFF, frs has prefix-bloom ON); block-cache bypass (fixed 27ae792c3,
already in baseline).

---

## 4. Recommended lever / measurement list for forst-rs (ordered)

1. **Consume the in-flight q7 re-profile** (`/ssd2/jackylee/frs-bench/logs/q7prof-{perf,iostat,pidstat,top}*`)
   before building anything — it directly ranks H1 vs H2 vs H3/H4 (CPU-idle+disk-saturated
   ⇒ H1/H4; CPU-busy-in-runtime ⇒ H2/H5).
2. **Executor depth A/B (H2)** — env-only: `FRS_RS_EXECUTOR=coordinated` q7 remote vs inline.
   Cheapest possible falsifier of the second-ranked hypothesis.
3. **Sorted-run discipline A/B (H1)** — `FRS_L0_SLOWDOWN_TRIGGER=20 FRS_L0_STOP_TRIGGER=36`
   (+ optionally one more compaction worker) with the n_overlap histogram logged; if fan-out
   is the driver, follow with leveled (non-overlapping) L1..Ln as a design change.
4. **ForSt remote q7 pin (§5)** — decides whether the bar is 1367.6 (RDB) or ~ForSt's number,
   i.e. how much of the 2376.4 must be clawed back (GOAL-1 crux per master-strategy §D).
5. **Page-cache / read-volume accounting (H3)** — engine logical-read bytes vs iostat physical
   bytes, frs vs RDB, same window.
6. Keep S2 default-ON-gated as "never-losing" but deprioritized (its own verdict), and gate
   L4 (compaction-windowed reads) on H4's iostat share, not on q7 hopes.

---

## 5. PART 2 — Real ForSt q7 @8c/32g remote: **REMOTE-PIN-PENDING**

Status at session end (2026-06-12 ~15:30 local):

- The relay bridge (`/tmp/relay-cmd.fifo` → `/tmp/relay-out.log`) stayed at its 3-agent cap
  the whole session: resident + q5-trio + q7-profile agents active; **no sustained quiet
  window** observed (per protocol, no fifo writes were made).
- **Jar status — RESOLVED mid-session by the q5-trio agent**: the relay log first shows
  "ForSt C++ backend jar: MISSING in FLINK/lib → forst-local SKIPPED" (11:04 campaign
  header), then later `Q5Z-JARS-RESTORED`: `flink-statebackend-forst-2.2.1.jar` +
  `forstjni-0.1.8.jar` copied from `/ssd2/jackylee/frs-bench/q5z-jars/` into
  `$HOME/workenv/flink-2.2.1/lib/` and a `run q5 forst-local 1500 q5z` launched. **The ForSt
  backend is now deployable on the box.**
- Concurrent load makes any number taken now invalid anyway: q7-profile's frs q7 run
  (45.4M events @988 s at last read) and the q5z forst-local run share the disk.

**Ready-to-run command for the pin** (first sustained-quiet window, FRESH marker, nohup):

```
echo 'nohup env PATH=/home/work/dockerd/bin:$PATH REPO=/ssd2/jackylee/frs-bench/ForSt \
  WORKENV=$HOME/workenv FLINK=$HOME/workenv/flink-2.2.1 \
  IMG=flink:2.2.1-jdk17-forst-bench-tools-20260609 PLAT=linux/amd64 TOPO=split \
  NEXMARK_HOME=$HOME/workenv/nexmark-flink \
  bash <runner> run q7 forst-local 2400 q7forst > /ssd2/jackylee/frs-bench/logs/q7forst.log 2>&1 &' \
  > /tmp/relay-cmd.fifo
```

References for the pin: frs 2376.4 R / RDB 1367.6 R / ForSt-Mac 586.8 M. Decision value:
if ForSt-remote lands ≪ 1367.6, GOAL-1's q7 row needs more than S2+L4 (Stage-6 re-profile
path); if it lands ≥ RDB, the q7 bar is RDB's 1367.6 and the H1+H2 levers above are sized
to close 2376.4 → ~1450-1900 (master-strategy §D row q7).

---

## Appendix — citation index (one line per claim class)

- ForSt executor topology: `flink-statebackend-forst/.../ForStStateExecutor.java:84-146,288-291`
- read-io-parallelism=3 / write-inline / coordinator: `ForStOptions.java:275-313`
- multiGet single-JNI sub-batches: `ForStGeneralMultiGetOperation.java:80-161,182-191`
- per-entry-JNI iterator, 128 cap: `ForStIterateOperation.java:34,66-94`; `ForStDBIterRequest.java:116-142`
- ForSt CF/format defaults: `ForStConfigurableOptions.java:143-314`; managed-memory cache: `ForStOptions.java:171-235`
- RocksDB layout: `include/rocksdb/table.h:276,90-237`; `advanced_options.h:461-472,583,590,705`
- read path: `cache/lru_cache.*` + `include/rocksdb/cache.h:237`; `table/block_based/block_prefetcher.cc:99-137`; `table/multiget_context.h:103`; `block_based_table_reader.cc:1916,2033,2076,2389`
- iterators: `db/version_set.cc:939` (LevelIterator); `table/merging_iterator.cc:52`; `db/arena_wrapped_db_iter.h:36`; `memtable/inlineskiplist.h:61`; `db/memtable.cc:125-129`
- ForSt disagg: `env/flink/env_flink.h:31`; `...forst/fs/cache/FileBasedCache.java:59`
- forst-rs executor default: `flink-statebackend-forst-rs/.../keyed/ForStRsAsyncKeyedStateBackend.java:1188-1219`; `exec/RoutingStateExecutor.java:41-99`
- forst-rs probe open: `crates/forst-rs-engine/src/db.rs:6232,6245,6766-7050,7034`; diags `:373-399,6810-6824`
- forst-rs LSM/write control: `crates/forst-rs-engine/src/compaction.rs:15-26`; `write_controller.rs:32-101`
- forst-rs SST: `crates/forst-rs-storage/src/sst/sparse_index.rs:26-54`; `sst/prefetch.rs:285`; `crates/forst-rs-io-uring/src/lib.rs:15-36`
- recorded q7 numbers: `docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md:2135-2152,2274-2284`; `2026-06-12-master-strategy-beat-forst-rocksdb.md:41,115-118,161-180,310`

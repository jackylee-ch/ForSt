# Phase-2 Competitive Deep-Dive: ForSt's Disaggregated Loss Decomposition vs forst-rs-on-S3

**Date:** 2026-06-13
**Status:** PMC analysis (evidence-cited; no code changes)
**The question:** the paper's best disaggregated configuration lands at ≈0.85× RocksDB(local)
on the heavy-I/O query class — can forst-rs reach **≥1.0× RocksDB(local-state, S3-checkpoint)
on S3**, and how?

**Sources** (every claim cited as `[paper p.NNNN]` = PVLDB 18(12) page, or `file:line`):

1. *Disaggregated State Management in Apache Flink 2.0*, Mei et al., PVLDB 18(12):4846–4859.
2. ForSt C++ — this repo's `origin/main` (ForSt 0.1.8, RocksDB fork; `git show origin/main:<path>`).
3. ForSt Java — `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst` (read-only).
4. Our Phase-2 design: `docs/superpowers/specs/2026-06-13-phase2-disaggregated-state-design.md`.
5. Our ledger: `docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md` (CURRENT
   STATUS block), `2026-06-12-sorted-run-discipline-design.md`, `2026-06-12-forst-architecture-q7-analysis.md`.

---

## 0. Executive verdict (details in §4)

**YES, ≥1.0× RocksDB(local) on S3 is achievable for the majority query classes — and
forst-rs can structurally BEAT the RocksDB(local-state + S3-checkpoint) baseline on
checkpoint/recovery cost — but it is CONDITIONAL on the sorted-run discipline landing
(write-amp 7.68× → ≤4×) for the join class, and on local cache ≥ hot working set.**
The single biggest risk is **write-amplification turning into continuous S3 upload
amplification** (every flush/compaction byte becomes an upload byte under remote-primary;
at 7.68× this is ~2× the bytes ForSt/RocksDB would stream and is exactly the disk-saturation
loop already recorded on q7 — `2026-06-12-forst-architecture-q7-analysis.md:63-71`).

The crucial asymmetry exploited: the paper's 0.15× loss is dominated by costs forst-rs's
architecture **already avoids** (per-block JNI crossing, Java-stream remote reads, GC'd
Future objects); the one cost where forst-rs is **worse** (write discipline) has a designed,
measured fix; and the paper's own data shows the disaggregated side **wins** the moment
checkpoint/recovery/rescale enter the score [paper p.4854-4855].

---

## 1. PART A — Decomposing ForSt's 0.15× loss, from the paper's own data

### 1.1 The paper's measured deltas

| # | Component | Paper's own measurement | Where |
|---|---|---|---|
| L1 | **Raw remote latency** | NVMe 68 µs → HDFS 1.5 ms (22×) → OSS 23 ms (338×) per read | [paper p.4850, Table 1] |
| L2 | **Sync remote = collapse** | Flink 2.0-HDFS-sync: "severe performance degradation"; −48% avg throughput for heavy-I/O queries without cache | [paper p.4856] |
| L3 | **Async recovers ~2×** | HDFS-async ≈ 2× over HDFS-sync (latency hiding, not elimination) | [paper p.4856, Fig. 13a] |
| L4 | **Cache recovers up to 3.7×** | HDFS-async-cache up to 3.7× over async; with only a 1 GB disk cache vs working sets q7 2.25 GB / q9 4.48 GB / q18 1.05 GB / q19 1.52 GB / q20 2.95 GB | [paper p.4856-4857] |
| L5 | **Residual CPU tax +30%** | Async model adds ~30% CPU for stateful operators: context-switch 30% + AEC intra-task scheduling 20% + I/O classification & parallel-exec batching 20% + **Java GC of Future objects 30%** | [paper p.4857] |
| L6 | **Net steady-state** | Overall avg with cache: −4%…+4% vs local ("outperforms local state setup by 4% on average" overall, but heavy-I/O per-query Fig. 13a sits **below parity** for q9/q20-class — the ≈0.85× the program targets) | [paper p.4856, Fig. 12/13] |
| L7 | **Where disagg WINS** | Checkpoints: all <3 s vs 1.20's 19.7% >30 s tails [Fig. 9]; recovery/rescale 16–49× faster [Fig. 10]; CUs halved 16→8, cost −50% [Table 2]; Flink 1.20 uses **9% more CPU** in production (more TMs + checkpoint-triggered compaction spikes, Fig. 11) | [paper p.4854-4855] |

**Top-3 of the 0.15× loss, ranked by the paper's own profiling:**

1. **Cache-miss remote reads on the heavy-I/O tail (L1×L4):** with state > cache, every miss
   pays 22–338× latency; async hides depth-3 of it (their executor: 1 coordinator +
   `readIoParallelism`=3 read threads, `ForStStateExecutor.java:59-66,84-146`) but cannot hide
   a working set that doesn't fit. This is the dominant term — their own Fig. 13a steps
   (sync→async 2×, async→cache 3.7×) show I/O exposure dwarfs everything else.
2. **The +30% async-runtime CPU (L5):** context switching, AEC scheduling, request
   classification, and Future-object GC. Explicitly flagged "we plan to further reduce this
   CPU cost in future work" [paper p.4857].
3. **Per-block JNI + Java-stream read path (unmeasured by the paper, structural):** every
   C++ block read of a non-local file goes `RocksDB C++ → JNI → ForStFlinkFileSystem (Java)
   → CachedDataInputStream → (local cache file | remote FS client)` —
   `origin/main:env/flink/env_flink.h:28-34` (`FlinkFileSystem : FileSystemWrapper`
   "delegate necessary methods to Flink FileSystem based on JNI"), JNI helpers
   `env/flink/jni_helper.cc`. Even a **cache hit** crosses JNI and reads through a Java
   `FSDataInputStream` with a semaphore-guarded eviction protocol
   (`CachedDataInputStream.java:33-37,100-135`). The paper's L5 GC/context-switch numbers
   partially proxy this tax.

What they **left as future work** [paper p.4855, p.4857]: reduce the +30% async CPU; merge
metadata reads at startup (OSS restore pays +10-20 s on small-object metadata ops); remote
compaction is "experimental" [paper p.4853, ref 36 = a feature branch, not mainline —
the C++ side just reuses RocksDB's `CompactionService`
(`origin/main:db/compaction/compaction_service_job.cc`)].

### 1.2 Per-component: does forst-rs avoid it, share it, or lack it?

| Paper loss component | forst-rs status | Evidence |
|---|---|---|
| L1/L3 remote-latency hiding (async exec) | **SHARED (same AEC runtime), executor better at the boundary** — V2 batched FFM executor drains chunks per crossing vs ForSt per-entry JNI; FFM crossing measured ≈ free (probe-open 104.2 µs is engine scan-build, not boundary) | `2026-06-12-forst-architecture-q7-analysis.md:18-19,54,106-108` |
| L1 per-block JNI/Java-stream read path | **AVOIDED ENTIRELY** — native Rust I/O stack: `CachedFileSystem` (`crates/forst-rs-storage/src/cached_fs.rs:66`), opendal S3 (`crates/forst-rs-io/src/opendal_backend.rs:198,377`), io_uring vectored reads (q7 finish-vs-DNF), `BlockPrefetcher` remote regime (`crates/forst-rs-storage/src/sst/prefetch.rs:341,356`); zero-copy chunked iteration into Java | SWEEP CURRENT STATUS:22-23; q9 1.63×→**1.14×** after streaming-read P0/P1 + prefetcher + io_uring |
| L5 async CPU tax (+30%) | **PARTIALLY SHARED** — Flink-side AEC/Future costs identical (same fork); backend-side classification/batching is ours and measured cheap; engine-side tokio task-per-op dispatch is our own version of this tax (H5, weight ~15%) | q7-analysis:33-35,199-205 |
| L4 cache-miss policy (1 GB cache, history-based) | **NOT BUILT (Stage 5)** — ours is pure-LRU whole-file cache; miss on over-budget file = remote pass-through every read; no frequency re-admission, no admission-gated fetch | phase2-design §1.2-C, §4.1-4.2; `crates/forst-rs-storage/src/local_cache.rs:69` |
| L7 checkpoint linking (UFS) | **STAGE-1 MERGED, Stage 2/3 pending** — `FileMappingManager` link/refcount/journal/tombstone/GC landed inert; link-vs-copy measured **3172×** (1.4 µs/link vs 4.5 ms/copy) | `crates/forst-rs-io/src/file_mapping.rs:490-538,555,595`; phase2-design §8 Stage-1 |
| Write discipline (not a paper loss — their inherited RocksDB strength) | **WORSE — 7.68× vs 3.91× write-amp (1.96× the physical bytes)**, probe p50 4.0× slower at equal throughput; sorted-run design staged, unbuilt | `2026-06-12-sorted-run-discipline-design.md:94-100,125-135,380-384` |
| Remote compaction | **NOT BUILT** (deferred; experimental upstream too) | phase2-design §1.2-K |

---

## 2. PART A′ — What ForSt actually built that our Phase-2 design missed (implementation catalog)

Hunted through `flink-statebackend-forst` + `origin/main` C++. Items **already covered** by
our design (FileMappingManager refcounts, link-based checkpoint, handle-delegated discard,
restore-by-link, history-based policy as Stage 5) are not repeated. The genuinely missed or
under-specified mechanisms:

### 2.1 Cache: it's not "an LRU with a history policy" — five specific mechanisms

ForSt's `FileBasedCache` is a **two-list LRU** (`DoubleListLru`): a *hot* first list
(= files physically cached, counted against the limit policy) and a *cold* second list
(= metadata-only entries tracking evicted/never-loaded files):

1. **Write-only admission.** "Only newly generated SSTs are written to the cache, the file
   reading from the remote will not" (`FileBasedCache.java:43-44`); reads of uncached files
   accumulate **access counts** in the cold list and only get promoted (background-loaded)
   after `accessBeforePromote` touches (`:75-77,378-387`). Our Stage-5 sketch has the
   admission gate but not the *write-only + count-to-promote* split.
2. **Thrash blocking via `promoteLimit`.** A file evicted ≥ `promoteLimit` times is
   permanently blocked out of the hot list (`:79-82,385-386`) — the anti-thrash guarantee
   the paper alludes to, implemented as an evict-count cap, not a frequency window.
3. **Epoch-decayed counts.** Cold-list access counts reset when the entry drifts into the
   last half of the cold list (`secondAccessEpoch` arithmetic, `:377-387`) — a cheap sliding
   window without timestamps. Cheaper than our proposed per-file 1-minute window.
4. **Only Flink threads affect LRU order.** `isFlinkThread` ThreadLocal set on state-executor
   threads (`:65,146-164`; wired via `ForStExecutorThreadFactory`,
   `ForStStateExecutor.java:97-99`): **background compaction reads don't promote cache
   entries or pollute hit metrics**. Our ShardedClock/LocalCache has no requester-class
   distinction — compaction scans can evict the operator hot set. *Missed entirely.*
5. **Eviction is non-blocking for readers.** `CachedDataInputStream` holds a per-stream
   semaphore + 3-round retry: evictor flips entry to `CACHED_CLOSING`, readers drain,
   position is carried over to the original (remote) stream mid-read
   (`CachedDataInputStream.java:33-37,49-57,100-135`). Whole-file eviction never stalls a
   read. Our `LocalCache` eviction-vs-read interplay is fd-cache-based; the *mid-stream
   switch* semantic is absent (less critical for us — our reads are positional, not
   streaming — but the restore-seeding below is not).
6. **Restore pre-seeding:** `registerInCache(path, size)` inserts restored files into the
   cold list with `accessCount = accessBeforePromote − 2` so the *first* touches trigger
   background load-back ASAP (`FileBasedCache.java:249-256`). Pairs with instant-link
   restore: link first, cache warms by-demand-but-eagerly. Our Stage-3 says "cache warms
   lazily" with no eager-on-first-touch mechanism.
7. **Dual limit policies:** size-based AND space-based (min free disk headroom) bundled
   (`ForStFlinkFileSystem.java:144-168`, `SpaceBasedCacheLimitPolicy` probes the actual
   filesystem). We only budget bytes (`local_cache.rs:69` `capacity_bytes`).

### 2.2 File mapping: ownership is type-driven and checkpoint-handle-backed

- **`FileOwnershipDecider`: non-SST files (MANIFEST/CURRENT/LOG/OPTIONS) are ALWAYS local +
  private** (`FileOwnershipDecider.java:28-52`) — only `.sst` is shareable/remote. The DB's
  chatty small-file traffic never touches S3 metadata ops. Our mapping layer is
  path-agnostic; we should adopt the same suffix policy when wiring Stage 2 (cuts the OSS
  metadata-latency exposure the paper flags at restore, [paper p.4855]).
- **`HandleBackedMappingEntrySource`:** a restored mapping entry can be backed directly by a
  Flink `StreamStateHandle` rather than a path (`FileMappingManager.java:84-119`), and
  `giveUpOwnership(path, handle)` flips a live working file to checkpoint-owned
  (`NOT_OWNED`) re-pointing its source at the checkpoint handle (`:335-352`). This is the
  exact JM↔TM ownership handshake; our tombstone protocol covers deletion but not the
  *source rebind* (restore-then-keep-reading-from-checkpoint-namespace). Stage-2/3 wiring
  should mirror it.
- **Physical names are UUIDs** (`toUUIDPath`, `FileMappingManager.java:358-360`): logical
  rename/link never needs a remote rename (S3 has none). Our `register(logical,
  physical_key)` already permits this — make UUID-keying the default at Stage-2 wiring.
- **Directory ops are mapping-table ops** (`renameFile`/`deleteFileOrDirectory` with
  deferred parent-dir deletion, `:198-307`) because RocksDB does directory renames at
  restore. Engine-side forst-rs controls its own layout, so we likely never need this — but
  it explains why their layer must sit *under* the whole FS API while ours can stay a
  metadata sidecar (cheaper, fewer hazards).

### 2.3 Executor: stream-object pooling we don't have

`ByteBufferReadableFSDataInputStream` keeps a **pool of up to 32 input streams per file**
(`ForStFlinkFileSystem.java:72-73`) so the 3 read-IO threads + compaction never serialize on
one stream object. forst-rs positional reads via pread/io_uring don't need stream pools —
already structurally avoided, listed for completeness.

### 2.4 What the catalog does NOT change

Their state executor (`ForStStateExecutor.java:148-233`) is a coordinator thread +
fixed pools + `WriteBatch` — there is **no special cache-miss path in the executor**; miss
latency is simply absorbed by read-thread parallelism (depth ≥ 3) and the AEC's 6000
in-flight records [paper p.4851]. Confirms our two-regime-executor track is aimed at the
right mechanism (H2: depth-1 inline vs their depth-3).

**Net additions for our backlog (none invalidate the Phase-2 staging):**
(a) requester-class cache exemption (compaction must not evict the operator hot set),
(b) write-only admission + count-to-promote + promoteLimit thrash cap as the concrete
Stage-5 policy, (c) suffix-driven always-local rule for non-SST files at Stage-2,
(d) UUID physical keys at Stage-2, (e) handle-backed source rebind at Stage-3,
(f) restore pre-seed into the admission tracker at Stage-3.

---

## 3. PART B — The S3 performance model for forst-rs (profiling-grounded)

### 3.1 Recorded inputs (all same-box remote x86/NVMe unless noted)

| Measurement | Value | Source |
|---|---|---|
| FFM boundary tax | ≈ 0 (probe-open 104.2 µs = engine scan-build) | q7-analysis:54 |
| Streaming-read P0/P1 + prefetcher + io_uring | q9 1.63×(Mac legacy) → **1.14× PASS**; io_uring = q7 finish-vs-DNF | SWEEP:13-26 |
| S2 pinned-rows/loser-tree (flag-OFF) | micro: probe-open ssts_128 9.2×, churn scan −30%, iter_drain −10%; **q7@100M only +3.1%** (I/O-bound, not CPU) | SWEEP:30-32; q7-analysis:52 |
| Write-amp (churn_probe, n=3 medians) | frs 7.68 vs RocksDB 3.91 (1.96× bytes); probe p50 649 µs vs 162 µs (4.0×) at equal 200 K rows/s | sorted-run:94-100,380-384 |
| L0-depth dose-response | probe p50 ~linear in run count; at L0=40 probes 4.6× slower | sorted-run:107-119 |
| q7 disk profile mid-run | nvme 98-99% util, **writes 435-682 MB/s > reads 190-292 MB/s** — write-amp saturates the disk, compounding loop | q7-analysis:63-71 |
| Link vs copy | 1.4 µs/link vs 4.5 ms/copy = **3172×** (10k files, fs-emulation) | phase2-design §8 |
| ForSt remote q7 (the only ForSt-remote pin) | **1379.7 s ≈ RocksDB 1367.6 s** — ForSt-on-local-primary ≈ RDB on this box; the de-facto q7 bar | sorted-run:23,39 |
| Current frs remote scoreboard | q3 1.16×, q9 1.14× (r1; r2 regression bisect in flight), q20 1.29×, q7 1.74× | SWEEP:10-19 |

### 3.2 The model

Remote-primary S3 mode on the online box (50 Gb/s ≈ 6 GB/s NIC, co-located object store)
changes three terms relative to today's local-primary runs:

```
T_s3(q) ≈ T_local(q)
        + δ_miss(q)   [cold/over-budget block reads served from S3]
        + δ_upload(q) [flush+compaction outputs streamed to S3: write_amp × ingest_bytes / BW, as *interference*]
        − δ_ckpt(q)   [checkpoints become links: today's upload+await cost removed]
```

- **δ_miss ≈ 0 while cache ≥ hot set.** Write-through means every flushed/compacted SST is
  a local hit at birth (`cached_fs.rs:115-130,1345`); NexMark working sets (paper: 1–4.5 GB
  at their scale; tens of GB at our 100M) fit the box NVMe. The S3 read path only fires on
  restore and on cache-budget overflow — the paper's L4 trap, which we dodge by *not*
  running a 1 GB cache. **Assumption A1 (needs online-box validation):** S3 GET p99 at
  50 Gb/s intra-DC ≤ ~2 ms so the prefetcher's 4 MiB remote ramp (`prefetch.rs:23-35`)
  keeps even miss bursts off the critical path.
- **δ_upload is the dangerous term and it scales with write-amp.** Upload bytes/s =
  write_amp × ingest. q7-class ingest ~40-80 MB/s ⇒ at 7.68× ≈ 300-600 MB/s sustained
  PUT traffic (exactly the 435-682 MB/s the iostat capture showed going to local disk).
  A 6 GB/s NIC absorbs it in *bandwidth*, but: (i) those bytes still hit local disk too
  (write-through cache) — the **disk stays the binding resource**, same compounding loop;
  (ii) S3 PUT-op latency/inflight limits add scheduling pressure. At 3.9× (RocksDB parity)
  the same term halves. **Assumption A2:** upload pipelining (existing async upload +
  per-file `await_upload`, phase2-design §3.4) keeps δ_upload as background interference
  only — requires the no-`await_all_uploads` invariant (kept) and WBM-governed stall.
- **δ_ckpt is a credit, and it's against the *baseline* too.** RocksDB(local+S3-ckpt)
  uploads every new SST at checkpoint time (the paper measured 19.7% of checkpoints >30 s
  at 1.89 GB increments, p.4854); frs link-mode uploads zero at the barrier (uploads
  already happened at flush, links cost 1.4 µs + journal). For checkpoint-interval-dominated
  walls (q4-class: flush-on-barrier load-bearing, q4 ckpt-30s 557 s vs ckpt-OFF 1486 s)
  this term is a real wall-clock win, not just an ops win.

### 3.3 Projected NexMark-on-S3 ratios vs RocksDB(local-state, S3-checkpoint)

| Query class | Local ratio today (remote box) | Post Phase-2 stages (S2-S5) on S3 | + sorted-run (write-amp ≤ ~4) | Confidence / what must hold |
|---|---|---|---|---|
| **Point-get / light state (q3-class; also q0-q2,q10,q12-14,q21,q22)** | 1.16× (q3); light queries ≤1.0× | **≈1.0-1.16×** — state fits RAM/cache, δ_miss=δ_upload≈0, δ_ckpt credit small | ~same | **HIGH**. A1 only. Source-bound queries are parity by construction. |
| **Window/agg & dedup (q17/q15/q16/q18-class)** | 0.70-1.03× (beats RDB on q15/16/18; q17 parity) | **0.7-1.05×** — working set cache-resident via write-through; δ_ckpt credit visible on flush-heavy ones | improves toward ≤1.0× where currently >1 | **MED-HIGH**. A1 + A2; q17-class R rdb pin still owed (SWEEP:60). |
| **OVER-window (q19, q11)** | 0.40-0.59× (FAIL local already) | no better than local — these are engine read-path gaps, not disagg-sensitive | +prefetcher/S2 levers apply; unproven | **LOW** for ≥1.0×; not a disagg question. Excluded from the S3 verdict bar. |
| **Join / long-scan (q7/q9/q20-class)** | q9 1.14× (r1), q20 1.29×, q7 1.74× | q9 ≈1.15-1.3×, q20 ≈1.3-1.4×, q7 ≈1.8× — δ_upload makes the disk-bound loop *worse* at 7.68× | **q7 → ~1.2-1.4×, q20 → ~1.0-1.2×, q9 → ~1.0-1.15×** — halving write volume directly relieves the measured binding resource (disk 98-99% util, writes>reads) and the L0-depth probe curve | **MED**. Needs: sorted-run M1-M5; A2; q9 r2-regression bisect resolved; ForSt-remote corroborates the bar is ~RDB (1379.7≈1367.6), so beating RDB ≈ beating ForSt here. |

**Assumptions register (validate on the online box):**
A1 S3 GET latency/inflight at 50 Gb/s intra-DC (Stage-0 probe, must re-run there — the
recorded 10.2 MB/s BOS number is dev-Mac-only); A2 upload-as-interference-only (watch
WBM stall counters + iostat during q7-class on S3); A3 PUT/DELETE metadata-op cost at our
SST counts (sorted-run M-moves shrink file counts; 1 G memtables already cap them);
A4 cache ≥ hot set on the box NVMe (q9 ~tens of GB at 100M — holds; NOT true at the paper's
1 GB cache scale); A5 read-after-write visibility strong (probe records it; eventual ⇒
bounded-retry on restore only).

---

## 4. PART C — Verdict

### 4.1 Is ≥1.0× RocksDB achievable? YES, conditionally — per class

- **Already structurally there (no new work):** the light + point-get class and the
  window/agg class — 13 queries PASS the local bar today (SWEEP:67-70) and the S3 deltas
  for them are ≈0 (δ_miss=0 cache-resident, δ_upload small, δ_ckpt a credit). On the
  full-sweep *total* (the original program metric) these classes dominate the wall.
- **Conditional (the real fight):** the join class. The model says ≥1.0× needs
  **sorted-run discipline** (halves the binding write volume; also flattens the L0 probe
  curve), with the two-regime executor (depth>1 latency hiding, H2) as the second lever
  once volume is fixed. ForSt's own remote pin (1379.7 ≈ RDB 1367.6) proves ~1.0× is what
  a disciplined LSM + depth-3 async achieves on this box — there is no evidence of a
  ForSt-only ingredient beyond what's in our designs.
- **Out of scope of the disagg question:** q19/q11 fail locally for engine read-path
  reasons; S3 changes nothing for them either way.
- **Where we beat the baseline outright:** checkpoint duration (link vs upload — the
  paper's Fig. 9 shape, our 3172× link bench), restore/rescale (Stage 3), and steady-state
  CPU (no JNI-per-block, no Java-stream reads, FFM tax ≈0 measured). Any scoring that
  includes checkpointing/recovery — the paper's own headline metrics — tilts further to us.

### 4.2 Conditions for the verdict to hold

1. Sorted-run M1-M5 lands and reproduces churn_probe write-amp ≤ ~4-5 + the G1/G2 gates
   (sorted-run:338-342), then transfers to q7 (the +3.1%-S2 lesson says verify at 100M).
2. Phase-2 Stages 2-3 (link checkpoint, link restore) land — δ_ckpt credit + zero-upload
   barrier; Stage 5 cache policy with the §2.1 catalog items (write-only admission,
   promote-count, requester-class exemption) for any state ≫ cache regime.
3. A1-A5 validated by the Stage-0 probe + one q7-class A/B **on the online 50 Gb/s box**;
   no S3 perf claims from the dev Mac (standing rule).
4. q9 round-2 regression (+358 s) bisected back out — the projection uses the r1 tip.

### 4.3 The single biggest risk

**Write-amp × remote-primary = continuous S3 upload amplification feeding the same
disk-saturation loop that already DNF'd q7.** It is measured (7.68× intrinsic, writes >
reads at 98-99% disk util), it directly multiplies under remote-primary (every byte written
is also uploaded **and** still written locally by the write-through cache), and its fix
(sorted-run) is designed but unlanded — the largest unproven dependency on the critical
path. Mitigation order: sorted-run first, *then* S3 A/B; an S3 race before sorted-run lands
would mis-measure the architecture.

### 4.4 Ranked work list for the 2-week S3 goal

1. **Sorted-run discipline M1-M5** (gates G1/G2; q7 100M A/B after) — unblocks the join
   class AND halves δ_upload. Everything else is additive around it.
2. **Stage-2 link-based checkpoint** (engine `link_mode` + zero-upload snapshot branch;
   adopt §2.2 catalog: UUID physical keys, non-SST always-local rule) — converts the
   architecture's headline win into wall-clock; flat-vs-state-size bench is the Fig.-9
   reproduction.
3. **Online-box Stage-0 probe + q7-class S3 A/B** (validates A1-A3 with real numbers;
   first true frs-on-S3 datapoint vs RDB-local on the same box).
4. **Stage-3 instant-link restore** (+ §2.1.6 pre-seed) — the 16-49× class win; cheap once
   Stage-2 lands.
5. **q9 r2-regression bisect to green** (protects the one join PASS we project from).
6. **Stage-5 cache policy** per the §2.1 catalog (write-only admission, count-to-promote,
   promoteLimit, requester-class exemption) — required only for state ≫ cache; on the
   current box it's insurance, so it ranks last. Two-regime executor (H2) slots here too —
   after volume (1) is fixed, latency-hiding is the next join lever.

---

## 5. Final answers (one screen)

- **ForSt's 0.15×-loss top-3 (their own data):** (1) cache-miss remote reads on the
  heavy-I/O tail (1 GB cache vs 2-4.5 GB working sets; sync→async→cache steps of 2× and
  3.7× show I/O exposure dominates) [p.4856-4857]; (2) +30% async-runtime CPU
  (context-switch/AEC/classification/Future-GC) [p.4857]; (3) structural per-block
  JNI + Java-stream read path (`env_flink.h:28-34`) that even cache hits pay.
- **What they built that we missed:** no architectural surprises — but six concrete
  mechanisms worth copying (§2): requester-class cache exemption (compaction can't evict
  the operator hot set), write-only admission + count-to-promote + promoteLimit thrash cap,
  always-local non-SST suffix rule, UUID physical keys, handle-backed source rebind at
  restore, restore cache pre-seeding. Remote compaction is experimental even for them.
- **Projected frs-on-S3 vs RocksDB(local):** light/point-get ≈1.0-1.16× (HIGH conf);
  window/agg 0.7-1.05× (MED-HIGH); joins ~1.0-1.4× post-sorted-run (MED, the fight);
  plus outright wins on checkpoint/restore that the local baseline cannot match.
- **Verdict:** ≥1.0× is achievable on the majority of the sweep and credible on the join
  class **iff sorted-run + Stage-2/3 land and A1-A5 validate on the 50 Gb/s box**; the top
  risk is write-amp-driven upload/disk saturation (7.68× intrinsic, measured) — fix the
  write discipline before racing on S3.

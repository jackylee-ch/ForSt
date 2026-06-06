# q4 beat-RocksDB-on-local: definitive gap root-cause (2026-06-06)

After finishing the memory-model spec (C1+C2+C3), the value-carrying merge, and the
jemalloc-macOS-crash fix, q4 on the host (local dir) is **522s** vs RocksDB **243s** =
**2.15× slower**. This documents — with measurements — exactly why, so the remaining
work is scoped, not guessed.

## What is NOT the cause (ruled out with data)

- **Reads / prefix-scan** — was the dominant CPU; the value-carrying merge collapsed it
  (profile: prefix-scan frames 4700→<73). Read path is now RocksDB-parity.
- **Memory / swap** — C1+C2+C3 fit q4 in 8c/32g (peak 24.7GB, no OOM). Not swapping on host.
- **Checkpoint *cost* being the whole story** — disabling checkpoints made it 2.7× SLOWER
  (checkpoint-flush is load-bearing; bounds memtables).
- **A single hotspot** — the steady-state CPU profile is DIFFUSE (top frame ~88 of a large
  multi-thread sample). There is no 50%-function to optimize.

## The two structural gaps vs RocksDB (measured)

q4 522s ≈ **~144s checkpoint + ~378s steady-state**.

### UPDATE (2026-06-06): the WAL-equivalent ALREADY EXISTS — enabling it cut 160s
forst-rs has `create_incremental_checkpoint_noflush` + `replay_memtable_artifacts_from_dir`
(db.rs:4731/4146): with `forst.rs.checkpoint.noflush=true` the checkpoint captures the live
memtable as an Arrow-IPC artifact instead of force-flushing+compacting, and replays it on
restore — exactly the WAL's role. **Measured host A/B:** noflush=false **522s** →
noflush=true **362s** (−160s, confirms gap 1's ~144s). forst-rs vs RocksDB **2.15× → 1.49×**
(362 vs 243). q4 finished 98M correctly. **Tradeoff:** noflush keeps the memtable resident +
writes the artifact → peak RSS 44GB (fine on 64GB host; EXCEEDS 8c/32g → would OOM). So it's
a per-deployment config: **noflush=true for host/large-RAM throughput; noflush=false for the
8c/32g fit.** Not made default (memory-unsafe on the constrained target). A true incremental
WAL (append deltas, not full-memtable artifact) would give the speed WITHOUT the 44GB —
that's the real WAL project, now lower priority since noflush captured most of gap 1.

### 1. No WAL → expensive checkpoints (~144s, 26%) [LARGELY CLOSED via noflush artifact]
`noflush=false` forces a full memtable flush (+ resulting compaction) at every 30s
checkpoint — measured ~8s marginal per checkpoint (15s-interval A/B: +145s for ~18 extra
checkpoints). RocksDB has a **write-ahead log**: a checkpoint just fsyncs the WAL and
references existing SSTs — sub-second. forst-rs has no WAL, so the memtable can only be
made durable by flushing it. **Fix = add a WAL** so checkpoints stop forcing flush+compaction.

### 2. Per-op async dispatch overhead (~378s steady-state vs RocksDB's 243s)
Steady-state ~188K/s vs RocksDB ~403K/s, with CPU cores NOT saturated (18-core host) →
coordination/backpressure-bound, not compute-bound. The profile shows the cost spread
across the async machinery:
- tokio task spawn/poll/park churn (~200 samples) — a task per async op.
- **67 `forst-rs-opendal` threads** (per-DbImpl runtimes × ~12 instances) + opendal
  indirection on every SST read/write, even on local dir.
- C2's BTreeMap point-get (~200 samples) — the O(log n) cost traded for memory (wall-neutral
  on the 18-core host; matters more on 8 cores).
RocksDB uses a tight native synchronous path. **Fixes (in leverage order):**
(a) one shared tokio/opendal runtime per slot instead of per-DbImpl (extends C3/Component A's
shared-resource model — fewer threads, less scheduling churn);
(b) a synchronous local-FS fast path that bypasses opendal+tokio for local-dir SST I/O
(keep opendal behind the FileSystem trait for the S3/disagg Phase-2 seam);
(c) deeper request batching so fewer async tasks are spawned per record batch.

## Host q4 progression (this session, measured)
| config | wall | vs rocksdb 243s |
|---|---|---|
| VCM only, noflush=false | 557s | 2.29× |
| + C2 (hash_index removed) | 522s | 2.15× |
| + noflush=true (WAL-equiv artifact) | 362s | 1.49× |
| + async-state batching (in-flight 30k, buffer 4k) | **342s** | **1.41×** |
Net: 557→342s (1.63× faster), gap 2.29×→1.41×. Recommended host/large-RAM config:
`forst.rs.checkpoint.noflush=true` + `execution.async-state.{in-flight-records-limit:30000,
buffer-size:4000}`. (noflush=true peaks ~44GB → host only; 8c/32g uses noflush=false.)
Remaining 342 vs 243 = steady-state async coordination (per-op tokio+opendal on local I/O);
batching helped only ~20s → the rest needs the sync-local-write path (gap 2b), multi-session.

## CORRECTED steady-state root cause (profiled the 342s best config, 2026-06-06)
Per-thread analysis of the q4 operator task threads (not the idle pools): the Source +
Join(×4) + GroupAggregate threads are **CPU-BUSY** (Join work-frames 1284-1758 vs park 5-8),
NOT parked/coordination-bound. The **native forst-rs engine CPU is now small** (top native
frame 55; memtable_get total ~256; the biggest native cost is C2's BTreeMap point-get, minor).
So the remaining 342-vs-243 gap is **Java-side per-record CPU on the Join operator**: the
Flink `AsyncStateStreamingJoinOperator` logic + the forst-rs backend state serialization +
FFM boundary crossings — i.e. the integration layer, NOT the engine. The engine is at/near
RocksDB parity after VCM+C2. Prior sessions concluded this boundary cost (FFM-per-call > JNI;
Flink CopyingChainingOutput) is near its limit and largely out of forst-rs-engine scope.
**Implication:** forst-rs's structural advantage is async/disaggregated (S3) state where
remote-latency overlap wins; on low-latency LOCAL dir the Java+FFM per-record overhead is the
binder and RocksDB's tighter JNI/sync path wins. Closing it further means reducing FFM
crossings / backend serialization per record (deep, partly Flink-bounded).

## Config-tuning floor reached: 342s (2026-06-06)
The Join thread is forst-rs-ENGINE-bound (93,374 cumulative native samples vs 8,794
backend-Java) but DIFFUSE — no single hotspot; cost spread across memtable BTreeMap gets,
the per-probe prefix-scan merge BUILD over many SSTs, alloc, hash. A smaller WBM (1024) to
shrink the memtable gave NO change (342s) — memtable size is not the binder; the diffuse
per-probe scan/merge/alloc cost is. All config levers are exhausted at 342s. Closing
342→243 (29%) needs a sustained MICRO-OPTIMIZATION campaign on the hot get/scan/merge/alloc
path (each function is small; the aggregate is the cost), e.g.: fewer SSTs per probe
(more aggressive compaction vs the bounded pool — read/write tradeoff), per-probe scan-build
caching, alloc elimination in get_internal/prefix-scan. No single change closes it. This is
the honest floor for engine+config work; the engine is at near-parity and the residual is
broad constant-factor per-op cost — partly inherent to the async/disaggregated design on
LOW-LATENCY local dir (where RocksDB's sync/JNI path has less per-op overhead). forst-rs's
design advantage is S3/disaggregated state (the stated final target), where async overlap of
remote latency should put it AHEAD of RocksDB — that A/B is the higher-value next measurement.

## FAIR same-machine A/B (2026-06-06, the corrected verdict)
The 243s RocksDB number was stale (earlier machine state). Re-measured RocksDB q4 on the
CURRENT clean machine (Docker off, JDK17, incremental, 30s ckpt, local): **283s**.
forst-rs best config (noflush=true + aggressive async-state batching: in-flight 60k /
buffer 16k): **322s**. So the real same-machine gap is **1.14× (39s)** — NOT the 1.41×/2.15×
cited against the stale number. forst-rs progression this session: 557→522→362→342→**322s**
(1.73× faster). More compaction threads (14) did NOT help (342s — steals foreground CPU);
smaller WBM did NOT help (342). Best = 322s; config space exhausted.
**Remaining 322 vs 283 = 39s (12%)** = diffuse engine per-op cost (BTreeMap point-get from
C2, per-probe scan-merge build, alloc) + inherent async-on-local overhead. No single config
lever closes it; needs an engine micro-opt campaign (or it is partly the async-design floor
on low-latency local). forst-rs is now WITHIN 14% of RocksDB on local dir, and its design
advantage (S3/disagg async overlap) is unmeasured here.

## SHARPEST root cause (the WAL is the required lever) — 2026-06-06
The 322s floor is a fundamental LSM tradeoff forst-rs cannot escape by config:
- **noflush=false (522s):** state stays COMPACT (flush+compact) → fast scans, BUT the forced
  per-checkpoint flush creates L0 SSTs → compaction churn that competes with the CPU-saturated
  foreground.
- **noflush=true (322s):** no forced flush → cheap checkpoint, BUT the memtable stays RESIDENT
  and UNCOMPACTED (44 GB vs RocksDB 6.4 GB) → scans walk bloated state → more CPU.
Each extreme pays a different CPU tax; 322 (bloated-scan) < 522 (flush-compaction-churn), so
noflush wins — but neither gives BOTH compact state AND cheap checkpoint. RocksDB gets both
via its **write-ahead log**: WBM flushes keep the memtable compact (fast scans) while the
checkpoint just fsyncs the WAL (cheap, no forced flush). **forst-rs has no true WAL** (only
the noflush full-memtable artifact, which is the bloated-state branch). **THE lever to beat
RocksDB on local is a true delta WAL**: append per-write deltas → keep the memtable compact
(frequent background WBM flush+compact) AND make checkpoints cheap (sync the WAL, don't force
flush). That is a substantial, correctness-critical engine subsystem (append, group-commit
fsync, crash recovery/replay ordering, segment GC tied to flush) — a multi-session project,
not a config or single-function change. It is the definitive, bounded path to <283s.

## WAL hypothesis TESTED end-to-end — disproven as the beat lever (2026-06-06)
Built + benchmarked the WAL (Phases 1-3: writer, write-path append, checkpoint fsync-WAL
instead of forced flush). Result: **q4 322s, peak 43 GB — TIES noflush=true (322s), does
NOT beat RocksDB (283s).**
- First attempt fsynced per write-batch → ~170× collapse (q4 ~1.8K/s). Fixed: append-only
  on the write path, fsync ONLY at checkpoint (all Flink exactly-once needs).
- Corrected WAL run: 321-322s, same as noflush=true. **Why no beat:** the checkpoint cost
  was ALREADY removed by noflush=true (artifact) — the WAL merely matches it with cleaner
  recovery semantics. The state is NOT compact under WAL either (43 GB: the working set is
  async in-flight buffers + join state + memtable, not just the memtable artifact; bounding
  WBM to 2 GB did not reduce the 43 GB or the wall).
**Corrected conclusion:** the residual 39 s (322→283) is NOT checkpoint cost (noflush AND
WAL both eliminate it) — it is **steady-state per-record CPU** (diffuse engine + async
coordination) on the CPU-saturated 18-core host, where forst-rs does more work per record
than RocksDB. The WAL is a correct, valuable feature (durability; recovery-clean cheap
checkpoint) but is NOT the beat lever. Beating RocksDB needs lowering the diffuse per-record
CPU — a micro-optimization campaign with no single lever, possibly partly inherent to the
async design on low-latency local dir (forst-rs's advantage is S3/disagg, unmeasured here).

## Honest conclusion
Beating RocksDB on q4 local is **not reachable by tuning or incremental fixes** — it needs
the WAL (gap 1) and the async-dispatch/opendal reduction (gap 2). Each is a substantial,
correctness-sensitive subsystem (WAL recovery ordering; shared-runtime lifecycle), i.e.
multi-session work. Everything cheaper has been done and measured. The read path is
RocksDB-parity, memory fits 8c/32g, and q4 finishes reliably at 522s — a solid, correct
baseline to build the two structural levers on.

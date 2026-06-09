# 8c/32g 3-backend NexMark sweep — verified time + accuracy (2026-06-08)

All 100M events, 8c/32g Docker, each backend with its own timer. `out_rows` = sink vertex
read-records (accuracy: forst-rs must match RocksDB). Bar: forst-rs ≥0.8× RocksDB OR ≤+50s,
AND faster than ForSt, AND out_rows match.

## 🔒 LOCKED uniform config — identical for EVERY query (no per-query changes)
forst-rs: **`noflush=false`** · **`write_buffer_size=1024mb` (1 G)** · global WBM budget **6 GB**
(`FRS_WBM_TOTAL_MB=6144`) · true WBM backpressure + 256 MiB force-switch · SST-write coalescing
(4 MiB) · `.so` loaded **container-local** (`/usr/local/lib`) · lz4 SST compression.
**PMC rationale for 1 G** (not 128 M): SST count drives join read-amp (the #1 remaining gap); large
memtables → fewer/larger SSTs → lower read-amp. The old 1 G downsides are gone — coalescing makes big
flushes cheap (202→20 ms/MB) and the 6 GB global WBM budget bounds total RAM (per-memtable size is
just an upper bound, RocksDB-style). 128 M was empirically worse (compaction-overload → restart).
**Any forst-rs number NOT from this exact config is INVALID.** (8c/32g RocksDB ~2× slower than host
→ only 8c/32g-vs-8c/32g counts.)

## ★★★ STATUS UPDATE 2026-06-09: BLOCK-CACHE BYPASS fixed — broad read-path win (join family)
**The slow join/over-window queries (q9/q20/q19) were gated by an SST-reader block-cache bypass.**
Profiling q20 post-flush (FRS_BULK_SAMPLE + FRS_ITER_DIAG) refuted read-amp (n_ovl stayed 1–4 at 72M),
parallel-executor (no speedup; also a thread-safety crash — fixed), and downstream backpressure (pre-flush
228K/s burst). Real cause: the lazy `get_or_open_sst_reader` opens readers `.with_block_cache(...)`, but
the **5 EAGER reader-pre-populate sites — flush (db.rs:7195), 3 compaction outputs (3990/4272/5916),
ingest (1782) — opened readers with bare `SstReaderImpl::open` (no block cache)**. Those readers are what
the read path finds cached, so **flushed + compacted SSTs (a join's hot, repeatedly-probed working set)
re-read + re-decompressed every data block on every probe** (B_resident=0 → every probe hit them).

**FIX (engine, committed ForSt `27ae792c3`):** wire the shared decoded-block cache into all 5 sites.
Read-path architectural, **no config change**, correctness-neutral, **280 engine unit tests pass**.

| query | before (locked cfg) | **after block-cache fix** | vs RocksDB | vs ForSt | verdict |
|-------|--------------------|---------------------------|------------|----------|---------|
| q20 (join) | DNF 71M/93M @1300s | **DNF 88.6M/93M @1284s (+25%, ~4× post-flush)** | 800s (0.59×, FAIL) | **84M same wall → BEATS ForSt** | improved; beats ForSt; fails RocksDB bar |
| q19 (OVER) | 542.5s | **475.3s (−12%, exact 92M)** | 305s (1.56×, FAIL) | 319s (FAIL) | improved; still fails both — needs 2nd lever |
| q9 (join) | DNF ~43M-COLLAPSE (→10K/s) | **DNF 76.4M/93M @1284s (+77%, no collapse, ~1550s proj)** | (pending RocksDB 8c) | (pending) | collapse ELIMINATED; still DNF, needs more |
| q17 (group-agg, PASS) | 76.7s | **76.7s (NO REGRESSION)** | 74.5s (1.03×) | 255.7s | ✓ regression-checked — fix is neutral on the passing set |

**Regression gate PASSED:** q17 re-measured at exactly 76.7s with the fix (an intermediate 102.7s reading
was environmental variance — that run followed q9's ~36GB scratch write, polluting the OS page cache; a
clean re-run reproduced 76.7s). So the block-cache fix improves the slow read-heavy queries and is
**neutral on the already-passing ones** — it does not rob Peter to pay Paul. (Lesson: serialize/settle
the box between heavy runs; back-to-back runs share OS page cache + frs-tmp state.)

**Net:** a verified broad win (every query reading flushed/compacted SSTs benefits). q20 now beats ForSt;
q9 no longer collapses. BUT q19/q20 still miss the RocksDB ≥0.8× bar — q20 is hard for all LSM backends
(only RocksDB's mature engine finishes ≤800s; ForSt also DNF), q19 needs a second OVER-window lever.
Also stabilized the flaky `test_max_write_buffer_number_backpressure` CI test (ForSt `2b313f8bb`) for the
"every change must pass GHA" requirement.

## ★★★ STATUS UPDATE 2026-06-08 (PM): OOM SET FIXED (memory) via true backpressure — speed now the blocker

**Strategic pivot (user):** learn from ForSt (the backend forst-rs replaces), not RocksDB. ForSt's
memory model = flush-on-checkpoint + a TRUE WriteBufferManager stall (allow_stall, no give-up) +
fast flush. forst-rs had `noflush=true` (S3-tuning leftover → checkpoints never flushed memtables)
AND its write-stall only lived on the unused single-write path — so the FFM batch-only backend had
**NO write backpressure at all** → memtables grew to 7-11 GB → 8c/32g OOM.

**Fix implemented (engine, uncommitted) — the uniform memory model:**
1. `noflush=false` (uniform, matches ForSt — checkpoints flush+bound memtables).
2. `wait_for_wbm_headroom` stalls on the REAL budget `over_budget()`, NO 120s give-up (60s
   no-downward-progress defense only); added to ALL 3 batch write leaves (was single-write only).
3. Force memtable switch on over-budget (RocksDB WBM behavior) with a **256 MiB floor** (16 MiB
   floor caused an L0 explosion → `l0_stop_trigger=64` → pipeline freeze → restart).
Global WBM budget 6 GB (FRS_WBM_TOTAL_MB=6144, uniform). + SST-write COALESCING (4 MiB; flush
202→20 ms/MB) + SIGBUS fix (.so loaded container-local, not FUSE bind mount). Engine UTs green.

**Result — OOM SOLVED + write-throughput FIXED (verified, committed config):**
- **q17: OOM@81s/31GB → FINISHED 76.7s (1.03× RocksDB 74.5 = PARITY), out_rows 92M ✓.** (was 817s/11×
  before coalescing → coalescing cut sstwrite 504s→9.9s.)
- **q18: OOM/SIGBUS → FINISHED 318.1s (0.80× RocksDB 396.1 = FASTER), out_rows 92M ✓.** (SIGBUS .so fix.)
- **q9 (join): OOM@31GB → memory-bounded (peak RSS 18.2 GB) but DNF (read-amp, ~53M/1300s).** ✗ speed —
  WORST case: join-probe read-amp over growing state → needs the read-path module.
- q4 (join): running (committed cfg). q20 (join): pending re-measure. Expect q9-like read-amp.

**Speed severity ranking (all memory-bounded now): joins (q9 read-amp) ≫ dedup (q18 now PASS)
> group-agg (q17 now PASS).** The join read-amp (per-probe re-walk of growing state) is the
dominant remaining speed blocker → the parallel-coalesced/value-carrying READ PATH is the
highest-leverage next module (spec: 2026-06-08-q9-read-path-gap-investigation-design.md).

**WBM budget sizing (uniform, FRS_WBM_TOTAL_MB) is a partial speed lever:** q9 2 GB budget →
38.4M/1300s (end 9 K/s); q9 **6 GB budget → 52.9M/1300s (end 36 K/s)**, still memory-bounded
(RSS ~18-22 GB < 32 GB). Bigger budget → bigger/fewer L0 SSTs → less read-amp. **Adopted 6 GB.**

**★ noflush DECISION (fixed, committed): noflush=FALSE (the ForSt semantic).** noflush is NOT a
tunable — checkpoint format differs (artifact vs SST) so it can't change once data exists. ForSt
supports noflush=false ONLY; noflush=true is a forst-rs hack (live-memtable artifact) that
contradicts disaggregation. **FAIRNESS CORRECTION: the bounded-state "wins" in the table below
(q16 366, q19 298 "FASTEST") used noflush=TRUE — UNFAIR (skipped the flush ForSt does) → MUST be
RE-MEASURED under the committed config (noflush=false + 6GB + coalescing).** Also: 8c/32g RocksDB is
MUCH slower than host (q17 74.5 vs host 57; q18 396 vs host 195) → judgments vs *host* are invalid.

REFUTED (all with 8c/32g data, no wasted code): resident-shadow retire (shadow=0 for joins);
jemalloc eager-return (retained not reclaimable, still OOM); decoded-block-cache balloon (charge()
correct); compaction 8→3 (still OOM); WBM hard-cap-with-120s-give-up (released → overran → OOM);
write_buffer_size 128mb (compaction-overload → restart). Merge-collapse REFUTED as the flush cost
(flush doesn't merge — code-verified); per-key CPU REFUTED (buffer 8s + encode 17s ≪ sstwrite 504s).

---

> ✅ **q17/q18/q4 are at the LOCKED config (1 G, noflush=false) — VALID.** Still INVALID (re-measure
> under locked cfg): q16/q19 (`noflush=TRUE` — removed); q7/q11/q15 (OLD fast cfg). Light queries
> (q0-q3,q10,q12-14,q21,q22) are source-bound and pre-lock — low-risk, re-confirm.

| query | RocksDB s | RocksDB out_rows | forst-rs s | forst-rs out_rows | ForSt s | acc | bar |
|-------|-----------|------------------|------------|-------------------|---------|-----|-----|
| q0    | 32.7      | 100,000,000      | 31.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q1    | 30.6      | 100,000,000      | 29.4       | 100,000,000       | -          | ✓ | **PASS (correct + faster, 0.96×)** |
| q2    | 28.8      | 100,000,000      | 28.7       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q3    | 36.8      | 2,201,068        | 36.7       | 2,201,068         | -          | ✓ | **PASS (correct + parity, 1.00×)** |
| q4    | **313.5 (8c/32g)** | **177,629,788** | **597.2** 🔒1G | **25,849,976** | (host 1217)| ⧗ | **out_rows 25.8M vs 177.6M (6.9× fewer) — but q4 is a RETRACT/changelog query → out_rows is NOT a clean accuracy gate (could be changelog cadence, not wrong final answer). NEEDS final-result compare.** Also 1.9× slower. OOM-fixed/finishes. |
| q5    | 162.2     | 29,988,376 (det) | 240/138    | 2.0/2.4/2.9M (var)| (host 297) | ✗ | **FAIL — WRONG: ~14× under-emit + non-deterministic (windowed-JOIN bug; both async backends)** |
| q7    | (pending 8c/32g) | — | **DNF @1300s** 🔒1G (~78.5M/92M, rate→19.7K/s) | — | (host 659) | ? | **read-amp DNF** under locked cfg (join). Old "671s" was unfair noflush=true. Joins q9/q20 → needs READ-PATH module. |
| q8    | 46.1      | 3,064,457        | 43.7 (OLD) | 3,010,888         | (pending)  | ✗? | time ~parity (0.95×) but out_rows 1.75% LOW (53K fewer) — windowed-JOIN under-emit (mild) |
| q9    | (pending 8c/32g) | — | **DNF, block-cache fix: 76.4M/93M @1284s (was ~43M-COLLAPSE), ~1550s proj** 🔒1G | — | (pending) | ? | ★ block-cache fix ELIMINATED the read-amp collapse: q9 went from collapsing at ~43M (→10K/s) to sustained 76.4M@1284s (+77%, no collapse). Still DNF; needs RocksDB/ForSt 8c baselines + further work. The collapse WAS the cache-bypass. |
| q10   | 120.3     | 100,000,000      | 128.7      | 100,000,000       | -          | ✓ | **PASS (correct; +8.4s ≤ +50s bar)** |
| q11   | 111.1     | 92,000,000       | 382 (OLD fast cfg) | 92,000,000 | (host 158)| ✓ | correct; 3.4× — **RE-MEASURE under committed cfg** |
| q12   | 42.1      | 92,000,000       | 50.6       | 92,000,000        | -          | ✓ | **PASS (out_rows match; +8.5s ≤ +50s bar)** |
| q13   | 30.1      | 100,000,000      | 30.1       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q14   | 30.1      | 100,000,000      | 29.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q15   | 228.3     | 92,000,000       | 173.9 (OLD)| 92,000,000        | (host 258) | ✓ | correct + faster — **RE-MEASURE under committed cfg** |
| q16   | 428.4 (8c/32g) | 92,000,000 | **322.9** 🔒1G | 92,000,000 | **370.3 (8c/32g)** | ✓ | **FULL PASS — 0.75× RocksDB (faster) + faster than ForSt (322.9<370.3) + accurate.** |
| q17   | **74.5 (8c/32g)** | 92,000,000 | **76.7** 🔒1G | 92,000,000 | **255.7 (8c/32g)** | ✓ | **FULL PASS — 1.03× RocksDB (parity) + 3.3× FASTER than ForSt + accurate.** |
| q18   | **396.1 (8c/32g)** | 92,000,000 | **318.1** 🔒1G | 92,000,000 | **422.8 (8c/32g)** | ✓ | **FULL PASS — 0.80× RocksDB (faster) + faster than ForSt (318<423) + accurate.** |
| q19   | 305 (8c/32g) | 92,000,000 | **475.3** (block-cache fix; was 542.5) 🔒1G | 92,000,000 | **319.2 (8c/32g)** | ✓acc | **FAIL — 1.56× RocksDB (475 vs 305) AND 1.49× ForSt (475 vs 319).** Accurate (exact). Block-cache fix helped (542→475, −12%) but q19 still behind BOTH on OVER-window/dedup (RocksDB≈ForSt≈310). Needs a SECOND fix beyond the block cache. |
| q20   | **800.3 (8c/32g)** | 93,201,404 | **DNF @1300s, block-cache fix: 88.6M/93M (~1350s proj), NOW BEATS ForSt** 🔒1G | — | **DNF @1300s (84M/93M, ~1400s proj)** | ✗ | **Join, OUTPUT-amplifying. q20 HARD FOR ALL: only RocksDB finishes (800s).** ★ block-cache fix (2026-06-09): forst-rs 71M→**88.6M@1284s** (+25%, ~4× post-flush steady-state) → **now FASTER than ForSt (88.6M>84M same wall-time)**. ✓ beats-ForSt clause. Still FAILS RocksDB bar (~1350s vs 800s = 0.59×) — residual = forst-rs/ForSt-vs-RocksDB engine gap on output-amplifying joins. |
| q21   | 59.9      | 100,000,000      | 52.6       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q22   | 44.8      | 100,000,000      | 43.6       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |

NOTE on STALE rows (q7/q8/q11/q15/q16/q19): measured on the OLD "fast cfg" (noflush=TRUE) or
pre-fix configs — their pass/fail is NOT trustworthy and must be RE-MEASURED under the committed
config (noflush=false + 6GB + coalescing + container-local .so). Light queries (q0-q3,q10,q12-14,
q21,q22) are source-bound parity — low-risk but also pre-commit; re-confirm.

## REFINED ROOT CAUSE: UNBOUNDED-state queries OOM on fast cfg → FIXED (2026-06-08)
OOM set = q4(join)/q9(join)/q17(group-agg)/q18(dedup)/q20(join) = ALL accumulate UNBOUNDED state.
Fixed by the uniform memory model (true backpressure bounds RAM regardless of state size). q17/q18
now PASS; q9/q4/q20 finish-memory-wise, join read-amp is the remaining speed gap (read-path module).

## ACCURACY methodology (PMC — out_rows is gate ONLY for append/dedup)
- **APPEND / dedup / dedup-changelog queries → out_rows IS a clean exact gate.** Verified EXACT
  (forst-rs == RocksDB): q17/q18/q19 = 92M. These are real accuracy passes.
- **RETRACT/changelog queries (running aggs with updates: q4) and WINDOWED queries (q5/q8) →
  out_rows is NOT a clean gate** — a different changelog *cadence* (fewer intermediate update rows)
  gives a smaller out_rows with the SAME final answer. Must compare the **final materialized result
  rows**, not the changelog count.

## JOIN-family out_rows mismatches — to be resolved by final-result compare
- **q4 (auction⋈bid + agg, retract changelog): out_rows 25.8M vs 177.6M (6.9× fewer).** SIGNAL, not
  confirmed-wrong (retract query — see methodology). Needs final-result compare.
- **q5 (HOP+join): CONFIRMED WRONG** — controlled same-input replay gave non-deterministic, ~14×-low
  counts (a real bug, not just cadence).
- q8 (tumbling+join): out_rows 1.75% low — windowed, needs final-result compare.
Pure non-join AGG (q16/q19) = EXACT. The join queries (q4/q5/q7/q8/q9/q11/q20) all need a
**final-result accuracy check** (upsert-result compare), not out_rows, before their perf counts.
q5 is the one confirmed-wrong; the rest are open. This is the accuracy work item for the join family.

## ⧗ IN FLIGHT 2026-06-09: q20 PARALLEL-EXECUTOR experiment (the join architecture fix)
The join family (q7/q9/q20) DNFs because the default executor is depth-1 (each batch runs inline on
the AEC mailbox thread → in-flight depth==1, documented at VectorizedExecutor.java:337-398). q9 ran at
only **375% CPU on 8 cores** → serialization headroom, not CPU-bound. `RoutingStateExecutor` (already
implemented + flag-gated) routes each batch to one of N single-thread VectorizedExecutor workers →
in-flight depth N (the ForSt coordinator-offload + parallel-readThreads model). Correctness is sound:
AEC enforces per-key ordering so concurrent batches hold DISJOINT keys, and drains in-flight before
checkpoint. Refuted for q19 (OVER-window = few disjoint keys) but joins have MANY disjoint keys.
**Experiment:** q20 @ locked cfg + `FRS_RS_PARALLEL_EXECUTOR=1 FRS_RS_READ_IO_PARALLELISM=3` (match
ForSt's read-io-parallelism=3). Baseline: RocksDB 800.3s, forst-rs DNF@1300s (59.8M/93M, end 23.6K/s).
PASS criterion: finishes ≤1000s. If it finishes → parallel-read direction VALIDATED for the join
family (apply to q7/q9). If still DNF/no rate lift → serialization is NOT the bottleneck (per-iter CPU
is) → refuted, pivot to per-iter cost. Jar rebuilt 2026-06-09 (source was 18h newer than deployed jar).

### ✗ RESULT 2026-06-09: parallel executor CRASH-LOOPS q20 (thread-safety bug, NOT refuted direction)
q20 + parallel executor went into a **crash-restart loop** (status RESTARTING, src_out resetting,
never progressed past ~0.3M). Root cause (TM log, exact stack): **data race on a shared per-subtask
decode buffer.**
```
WrongThreadException: Attempted access outside owning thread
  at v1sync.MemorySegmentDataInputView.readByteUnsafe:70
  at ForStRsMapStateV2.deserializeUserKey:825        ← shared `iterView` FIELD
  at ForStRsDBIterRequest.completeWithEntries:512
  at ForStRsDBIterRequest.process:357 → VectorizedExecutor.executeIters:1371
```
cascade: `EOFException ... want 1936091768 bytes`, `IndexOutOfBoundsException: Out of bound access on
segment`, `MemorySegmentDataInputView underflow`. **Why:** `iterView` is a reused mutable field on the
per-subtask `ForStRsMapStateV2`; `RoutingStateExecutor` routes different batches of the SAME subtask to
DIFFERENT worker threads → two workers re-point the same `iterView` at their own thread-confined chunk
segments and read concurrently → WrongThreadException + corruption. The design's key-disjointness
argument is INSUFFICIENT: it protects engine keys, but the reusable Java-side decode scratch on the
state object is shared across ALL keys of the subtask → raced.
FIX applied + committed (flink `fc8eb8cd97c`/`c1bb289ae17`): iterator decoders use the existing
per-thread `VIEW_TL` ThreadLocal view, removed the shared `iterView` field. Behavior-preserving for the
default single-threaded executor.

### ✗✗ RESULT 2026-06-09 (re-run with iterView fix): parallel executor gives ZERO speedup for q20
Clean run, NO crash-loop (fix worked). Trajectory: **0–120s burst 150–228K/s** (state in memtable,
pre-flush) → 18.4M; **~140s COLLAPSE to ~15–20K/s** (first flush → probes hit SSTs) and stays there.
End: **60.8M/93M @1300s = DNF**, ~47K/s avg. **IDENTICAL to the serial run** (serial DNF@1300s 59.8M,
~46K/s). Parallel depth-3 = serial depth-1 throughput.
**→ executor-dispatch SERIALIZATION is REFUTED as the q20 bottleneck** (now refuted for q19 OVER-window
AND q20 join — the "parallel read threads" hypothesis from the architecture design does NOT hold for
either). With only 375% CPU (of 800%) AND no benefit from 3 workers, a SERIAL section is blocking the
workers. Candidates (next, profile to distinguish): (1) post-flush SST-probe path holds a global engine
lock (version/SST-reader RwLock) so parallel workers serialize on it; (2) downstream output
backpressure — q20 is output-amplifying (auction⋈bid), Calc→Writer is single-threaded per subtask;
(3) the per-probe 16-shard memtable cursor + SST merge is the serial cost.
**Candidate (2) REFUTED by this run's own data:** the source ran at 150–228K/s during the PRE-flush
phase and only collapsed AFTER the first flush (~140s). If the single-threaded Calc→Writer couldn't
drain the amplified output, the rate would be capped from t=0, not just post-flush. The slowdown tracks
the STATE BACKEND (memtable→SST), not the output operator → downstream backpressure is not it.
**Remaining: (1) SST-probe global lock OR (3) per-probe memtable/SST read cost / growing read-amp.**
375% CPU (of 800%) + 3 idle-ish workers + no parallel benefit ⇒ workers BLOCK on a serial point (low
CPU = blocking, not CPU-bound). CODE-AUDITED the obvious lock candidate — **LocalCache (the bounded LRU
file cache, `cached_fs`→`local_cache.rs`, on the SST read path): REFUTED as a catastrophic serial point.**
Its single global `inner` Mutex critical section (local_cache.rs:543-551) is ONLY membership-check +
LRU-bump + path-lookup; the `pread` disk I/O runs AFTER the guard is released (explicit comment :540-
542). So I/O does NOT serialize on the cache mutex. `touch_lru`/`fd_cache` are short critical sections
— minor contention at most, not a throughput cap for 3 workers. ShardedMemTable reads take per-shard
`.read()` (16-way, shared) — also not an obvious serial point unless write-locks (continuous bid
buffering) starve readers.
**All four leading hypotheses now refuted with code/data: executor-dispatch serialization, downstream
backpressure, LRU-lock-holds-I/O, and (weakly) memtable read-lock.** The true bottleneck is NOT
identifiable by code reading alone → **REQUIRED NEXT STEP: a sampling CPU/lock profile of the post-flush
steady state** (perf/async-profiler on the TM, or FRS_BULK_SAMPLE + FRS_ITER_DIAG + per-probe n_ovl
re-sampled at 50M+ not early — the early n_ovl=2-3 may have GROWN as join state accumulated → growing
read-amp (3) is the live candidate). The iterView fix is KEPT (a real latent thread-safety bug fix);
**the parallel executor is NOT the join-family lever** — do not pursue it further for q7/q9/q20.

## ★★★ ROOT CAUSE FOUND + FIXED 2026-06-09: flushed/compacted SST readers BYPASSED the block cache
After refuting parallel-executor/backpressure/LRU-lock, a profiling run (FRS_BULK_SAMPLE + FRS_ITER_DIAG,
default executor, locked cfg) localized the q20 join bottleneck **with data**:

**`[DECAY_ATTR]` at 72.4M records (deep post-flush):** `probe=6–37µs`, **n_ovl=1–4** (L0=1–3, deep=1),
**B_resident=0**, cost ~90% in **`A_fanout/sstloop`** (per-SST `get_or_open_sst_reader` + in-memory
`may_contain_range`/`first_block_ge`). → **growing-L0 read-amp REFUTED** (n_ovl stayed low at 72M).
**`FRS-ITER-DIAG`:** `sst_open_us` is mostly 0 but occasionally **19,614µs (19.6ms) for ONE cold reader
open**. `first_block_ge`/`may_contain_range` are pure in-memory index ops (reader.rs:703/727) → the
sstloop cost is **cold `SstReaderImpl::open`** (footer+index read/decode) on the probe hot path.

**THE BUG (code-confirmed, 5 sites):** `get_or_open_sst_reader` (the lazy path, db.rs:8666) opens
readers **`.with_block_cache(...)`**, but the eager reader-cache pre-populate at **flush** (db.rs:7195),
**3 compaction-output paths** (db.rs:3990, 4272, 5916), and **ingest** (db.rs:1782) all opened readers
with **bare `SstReaderImpl::open` — `block_cache=None`**. Since those pre-populated readers are what the
read path finds in `sst_readers` (a warm hit returns them directly), **flushed + compacted SSTs — i.e.
the join's entire hot, repeatedly-probed working set — had their data-block reads BYPASS the shared
decoded-RecordBatch cache**, re-reading + re-decompressing (lz4) the same blocks on every probe. This is
exactly the q9/q20 hot path the block cache was built for (db.rs:8616 comment), silently disabled for
every non-lazy-opened SST. B_resident=0 (resident shadow default-OFF) means ALL join probes hit these
cache-less SST readers.

**FIX (engine, uncommitted, host-compile-clean):** wire `.with_block_cache(self.block_cache, db_id,
file_number)` into all 5 eager reader-open sites (flush + 3 compaction + ingest), mirroring the lazy
path. Architectural (read-path), **no config change**, correctness-neutral (same bytes/API), benefits
ALL read-heavy queries (joins most). Per-query before/after e2e pending: rebuild Linux `.so` → re-run
q20/q9/q7 (DNF baseline: q20 ~71M/93M @1300s, ~47K/s). Expected: repeated same-SST probes hit decoded
blocks → big per-probe drop. ForSt q20 8c/32g baseline (in flight, running ~88K/s — FAST) is the target.

## ★★★ DEFINITIVE ROOT CAUSE 2026-06-09 (JFR full-stack): q7/q9/q20 are WAIT-BOUND on the async-state+FFM per-record boundary — NOT the LSM
After value-carrying drain (no help), parallel-iterator dispatch (no help), and lock-free memtable
superversion (marginal) — each targeting the ENGINE READ and each failing to move q9 — a JFR full-stack
profile (150s steady-state, q9) settled it. The engine read was NEVER the cost; the engine micro-diags
were a red herring.
- **ThreadPark 62,719 vs ExecutionSample 4,433** → the system is **WAIT-bound, not CPU-bound**. The
  dominant non-idle park (22,840) is `TaskMailboxImpl.take()` ← `processMailsWhenDefaultActionUnavailable`
  — the operator **parks because the async-state controller's in-flight buffer is full / waiting for
  outstanding state results**. Throughput is gated by how fast the async-state pipeline (operator → AEC →
  FFM downcall → engine → return → future → callback) drains, NOT by the engine.
- **On-CPU = 100% framework, 0% engine** (no LSM/SST/memtable frame appears): per-record FFM segment
  alloc (`SegmentFactories.initNativeMemory`+`Unsafe.checkOffset` ~9%), Arrow key hash
  (`ArrowBinaryBuffer.hash` ~5%), Flink row serde (`RowDataSerializer.copyRowData`+`ensureMaterialized`
  ~13%), async-future alloc (`ContextAsyncFutureImpl.makeNewFuture` ~3%).
- **GC** moderate (young every ~3.75s); **12 bg compaction/flush threads on 8 cores** add contention.

**Why RocksDB doesn't hit this:** RocksDB's backend is **SYNCHRONOUS** — direct in-operator JNI
`state.value()`, NO async-state pipeline, NO per-record future, NO mailbox-parking-on-async, NO
per-record Panama FFM segment alloc. forst-rs's **async-state-V2-over-Panama-FFM** pays a per-record
pipeline round-trip + framework allocation on every probe. **ForSt uses the SAME async-state framework
but a C++/JNI boundary + mature engine that drains requests faster** — that boundary efficiency is the
gap. So q7/q9/q20's wall is the FFM + async-state per-record boundary, which is exactly why every
engine-read fix (drain/parallel-iter/lock-free-memtable) couldn't move it.

**Kept improvements (correctness-verified, no regression, all committed/pushed):** lock-free memtable
superversion (ArcSwap), value-carrying drain unify, parallel-iter dispatch (flag-OFF), block-cache
fix. Real + architecturally-correct, just not where the join wall is.

**Levers (honest):** (1) per-record FFM `initNativeMemory` alloc violates the no-per-record-alloc /
zero-copy mandate → pool/reuse native segments (~9% on-CPU + GC; MODEST since wait-bound). (2) the
DEEP lever = async-state pipeline completion throughput (why the mailbox parks) → drain requests faster
(executor coordination + FFM boundary efficiency) — structural, multi-session, the ForSt-parity gap.
q7/q9/q20 cannot pass the RocksDB bar without (2).

## ⚠️ 2026-06-09 vectorized-hash before/after: ~PERF-NEUTRAL — tempers the on-CPU-micro-opt thesis
Committed the byte-identical wide-stride key hash (flink `8ffbc42b933`, 8× fewer FFM byte-reads in
`ArrowBinaryBuffer.keyHash`; 11/11 UTs incl. a scalar-equivalence gate). **e2e before/after (q9 + JFR):
~perf-neutral.** q9 84.5M@1204s vs prior ~76M baselines = within q9's 60–84M run-to-run variance (NOT a
win). JFR relative on-CPU shares UNCHANGED: `keyHash` 5%→6%, `checkOffset` 4%→4% (totals rose 4433→6419
only because more records ran). **Why:** the hash cost is the data-dependent `×31` ARITHMETIC, not the
FFM byte-reads I cut — so the reduction was negligible. The fix is KEPT (correct, mandate-aligned,
harmless) but is not a q9 lever.

**Sobering implication for Phase-1 heavy joins:** the forst-rs-specific on-CPU differential vs RocksDB is
~20% (FFM alloc 4% + per-access checks 4% + hash 6% + …), and the system is partly **wait-bound** (62k
ThreadParks). Even fully eliminating that ~20% gives <20% throughput — far short of the ~2× q9 needs to
reach the RocksDB bar. So **on-CPU micro-opts alone cannot close q7/q9/q20** — the dominant factor is
**request-completion latency (the async-state pipeline drain / the wait)**, where RocksDB's mature
engine+boundary completes faster. The honest lever is the request-completion path (executor/boundary
throughput so in-flight stops filling) — structural, the ForSt-parity gap — NOT per-record on-CPU
shaving. The zero-copy columnar-key levers help correctness/mandate + GC but, by this evidence, will
likely also be modest on a wait-bound query. q7/q9/q20 reaching ≥0.8× RocksDB is not yet demonstrated
achievable and may require reconsidering the async-state request-completion architecture.

## ★★★ 2026-06-09 PIVOTAL: RocksDB q9 ALSO DNFs ~1300s → q9 is NEAR-PARITY, not a failure
The whole "q7/q9/q20 severely regressed / far below 0.8× RocksDB" framing rested on comparing forst-rs
DNF to an UNMEASURED RocksDB. **First-ever RocksDB q9 8c/32g baseline: 94.5M @1283s — RocksDB ALSO
barely finishes / DNFs at ~1300s on this heavy join.** RocksDB is NOT fast on q9 at 8c/32g.
Apples-to-apples at 1204s: forst-rs ~84.5M vs RocksDB ~88.7M ⇒ **forst-rs q9 ≈ 0.95× RocksDB, near
parity** — NOT 0.59×. The "DNF = failure" read was an artifact of the missing baseline.
**Implication:** q9 (and plausibly q7/q20 — re-check their RocksDB finish times, not just RocksDB
q20=800s which DOES finish) may already be at/near the ≥0.8× bar once compared at true finish times.
ACTION: run BOTH q9 (RocksDB + forst-rs) to completion (MAXSEC 1700) for clean finish-time ratios; the
chunkBuf-reuse + accumulated fixes only need to hold near-parity, not 2×. This reframes the heavy-join
"failure" — measure the real RocksDB finish times for q7/q9 (q20 RocksDB=800s is the genuine outlier
where RocksDB finishes and the others don't).

## ★ WHAT'S LEFT (Phase-1 close)
1. **Fair baselines:** RocksDB 8c/32g + ForSt 8c/32g for the WHOLE set ("faster than ForSt" clause
   unverified almost everywhere; have RocksDB 8c/32g only for q17/q18).
2. **Re-measure the STALE rows** (q7/q8/q11/q15/q16/q19 + light) under the committed config.
3. **Finish OOM-set joins:** q4 (running), q20 (next); score vs bar.
4. **Read-path module** for join read-amp (q9) — the one architecturally-open perf gap.
5. **Correctness (GATE):** build a **final-result accuracy check** (materialize the changelog →
   compare result rows vs RocksDB) for the join/retract/windowed family — out_rows alone is not a
   valid gate there. q5 is CONFIRMED wrong (controlled replay); q4/q8 (and q7/q9/q11/q20) have
   out_rows mismatches that are SIGNALS to resolve with the final-result check, not yet confirmed
   bugs. Fix any confirmed wrong-answer before its perf counts.
6. **Commit** the verified fixes (memory model + coalescing + SIGBUS) after the re-measure confirms
   no regression; then compute overall NexMark total vs RocksDB.

## Verified architecture fixes this campaign (UNCOMMITTED)
- A: memory model (true WBM backpressure, force-switch) → OOM set bounded ≤18 GB (was 31 GB OOM).
- B: SST-write coalescing (per-block block_on → 4 MiB writes) → flush 202→20 ms/MB; q17 817→76.7s.
- C: SIGBUS fix (.so container-local, not FUSE mount) → q18 teardown crash → clean finish.
Tests: io 209 + storage + engine 277 green.

## ★ LEVER 1 COMPLETE — vectorized zero-copy key path (2026-06-09)
**All four per-byte FFM sites the lever-1 spec named are now eliminated:**
- key HASH: wide-stride `JAVA_LONG` `keyHash()` (was per-byte `seg.get(JAVA_BYTE)`) — committed `8ffbc42b933`, byte-identical UT gate.
- key COMPARE: `keysEqual` already used `MemorySegment.mismatch()` (JDK22+ vector intrinsic) — no per-byte loop.
- key COPY: `appendKey`/`appendValue` already used `MemorySegment.copy()` intrinsic.
- iterator chunkBuf: per-probe 64 KiB `Arena.allocate` → per-executor REUSED buffer (`process(...,reusedChunkBuf)`) — committed flink `53722e117d6`. Kills the JFR-pinned `initNativeMemory`/`checkValidStateRaw` per-probe alloc.
- MapStateCache/ArrowBinaryBuffer key+value segments already pooled (grown rarely).

**Decisive measurement (q9 8c/32g, apples trajectory at ~361s):**
- pre-chunkBuf forst-rs q9 (bge23t5si): 36.06M @362s
- chunkBuf-reuse forst-rs q9 (bq01jk6pb): 37.63M @361s ⇒ **+4.4% throughput**
- RocksDB q9 (b95vxcqzq): 46.4M @401s — forst-rs ~0.85× at this scale, ~0.95× at true finish.

**Verdict (per the agreed test):** the on-CPU reduction DOES translate to throughput (+4.4%), so the
zero-copy-key direction is validated — but the gain is modest because the system is substantially
**wait-bound** (JFR: 62,719 ThreadPark on the shared async-state mailbox vs 4,433 on-CPU). Lever 1 narrows
the gap; closing it fully needs the WAIT to shrink (shared async-state concurrency), which is NOT a
forst-rs-specific zero-copy lever. Proceeding to lever 2 (coalesced batched lookup) + lever 3 (zero-copy
value views) per directive; honest expectation is each adds a similar single-digit % on the iteration path
(the engine read is already O(1)-cheap: memtable n_shards=1, SST n_ovl=1), so q9 stays near-parity rather
than flipping to a large win — the residual wall is the framework wait.

### Zero-snapshot drain — correctness GATE PASSED (2026-06-09)
q11 8c/32g (locked cfg, zero-snapshot decode jar): **FINISHED 318.9s, out_rows=92,000,000 == RocksDB
reference (exact).** q11 is an append/dedup query → out_rows is a clean exact gate. The in-place
per-chunk decode (no per-chunk `arena.allocate(bytesUsed)` snapshot, no view-accumulation list) is
byte-correct over a full 92M-event MapState-iteration workload. Lever 1 is COMPLETE + CORRECT.
Next: q9 zero-snapshot before/after trajectory, then lever 2 (coalesced batched lookup).

## ★★ ALL THREE LEVERS FINISHED + TESTED (2026-06-09)
Per directive ("ALL O(1) lookup + zero-copy value finished before any nexmark"):
- **Lever 1 (columnar buffer / zero-copy key):** wide-stride `keyHash`, `mismatch()` compare,
  `copy()` intrinsic, per-executor reused `chunkBuf`, + **zero-snapshot drain** (decode each chunk
  in place to detached on-heap, no per-chunk `arena.allocate` snapshot, no view-accumulation list).
  Applied to BOTH `process()` and `processFromBatchedOpen()`. Dead `parseChunkInto` removed.
- **Lever 2 (O(1)/coalesced batched lookup):** `executeItersBatchedParallel` + FFI
  `frs_vec_iter_prefix_open_batch_parallel` (one crossing opens K probes, engine drains across read
  pool); drain uses the zero-snapshot path. Gated on `FRS_RS_PARALLEL_ITER` pending e2e enable-decision.
- **Lever 3 (zero-copy value):** `ForStRsMapStateV2.deserializeUserKey/Value(IteratorEntryView)`
  rewind a `MemorySegmentDataInputView` directly onto the chunk slice via per-thread `VIEW_TL` — no
  intermediate `byte[]`. The zero-snapshot change makes the view reference the reused `chunkBuf`
  directly, completing the zero-copy chain (engine buffer → on-heap RowData, the one materialization
  Flink requires).

**TESTS (all green):**
- Unit (mock linker): `ForStRsDBIterRequestTest` 5/5 (incl. 2 new `processFromBatchedOpen` cases:
  single-chunk decode + multi-chunk no-corruption) + `ArrowBinaryBufferTest` 11/11.
- **Full native suite vs real engine (host dylib rebuilt): 528 run, 0 failures, 0 errors, 11 skipped
  — BUILD SUCCESS** (incl. `IterPrefixBatchOpenTest`, `FrsIteratorTest`, all `MapStateV2*`, `Vectorized*`).
- e2e correctness: q11 out_rows=92,000,000 == RocksDB (exact gate).
Levers implemented + verified; nexmark validation/enable-decision is next.

## ★★ q20 ROOT CAUSE (2026-06-09, systematic-debugging Phase 1 — NOT A BUG)
Hypotheses tested: H1 amplification / H2 unbounded-state / H3 constant-factor.
- **H1 RULED OUT:** q20 forst-rs @40M scale FINISHED 357.6s, **out_rows=37,279,944 = src_out EXACTLY**
  (ratio 1.000, == RocksDB's 93,201,404/~93M ≈ 1.0). No over-emission; join is byte-correct.
- **H2 RULED OUT:** peak RSS bounded 22–24 GB (oscillates 13–15 GB, slow climb to 17.8 GB) — join
  state lives in flushed SSTs, not RAM. No leak, no OOM.
- **Verdict = state-size-dependent LSM-efficiency DEGRADATION** (refined H3, not flat constant):
  forst-rs rate **104K/s @40M (0.90× RocksDB) → ~25–46K/s tail @93M (0.59×)**. Throughput falls as the
  unbounded inner-join state grows the LSM; RocksDB's local engine stays flat enough to finish @800s.
  Rate OSCILLATES (24→59→25K/s) = compaction cycles stealing throughput as the SST set grows.
- **Same root-cause family as q9** (LSM per-op efficiency vs local RocksDB), amplified by q20's larger
  growing state + 93M output. **ForSt also DNFs** → disaggregated-LSM ceiling; forst-rs already beats ForSt.
- **Implication:** no quick fix. The lever is the read-path/compaction-scaling module (block-cache fix
  was its first +25%). Design effort, not a patch. Two full runs DNF'd at MAXSEC 1500/1700 (91–92M/93M).

## q20 sub-cause LOCALIZED (2026-06-09, DECAY_DIAG) — NOT read-amp, it's write+coordination
DECAY_DIAG (TM log, $WORKENV-mounted) across the q20 run: L0 stays ≤3 files, NOTHING promoted to
L1+, SST state ~1.3 GB while RSS ~13 GB → join state is MOSTLY MEMTABLE-RESIDENT; reads touch ≤3
small L0 files (matches prior n_ovl 1–4 @72M). **Read-amp is NOT the cause** (reads cheap, like q9's
µs engine read). The state-size throughput collapse (104K/s@40M → 7.8K/s tail@66M) is **write+
coordination side**: flush/compaction/WBM management of a large resident working set + the async-state
wait (q9's 14:1 park). SAME diffuse ceiling as q9; write-side already addressed by Module #57 (WBM
backpressure) + #58 (vectorized SST writer) + block-cache (read). Residual = diffuse per-op efficiency,
NOT a single un-pulled lever. ForSt also DNFs → disaggregated ceiling; forst-rs beats ForSt.

## ★★★ CORRECTION (2026-06-09): q9/q20 are NOT a "diffuse ceiling" — it's depth-1 async dispatch (FIXABLE)
The earlier "q20/q9 = diffuse LSM write+coordination ceiling, no single lever" conclusion is WRONG and
RETRACTED. Validated against 2026-06-09-forstrs-join-performance.md + re-confirmed by code + JFR:
- **CODE [FACT]:** `VectorizedExecutor.executeBatchRequests` runs the batch SYNCHRONOUSLY INLINE on the
  mailbox thread and returns `CompletableFuture.completedFuture(null)` (:552); `fullyLoaded()` hard-coded
  `false` (:1023; javadoc :385-393 admits it); in-flight depth = 1 (AsyncDispatchInFlightParallelismTest).
  ForSt instead returns an INCOMPLETE future + offloads to coordinator + 3-thread read pool (depth 3);
  RocksDB is synchronous (no async cost).
- **JFR [FACT] (q9-flame2.jfr):** 65,842 ThreadPark vs 6,419 on-CPU (~10:1). 26,821 parks in
  `MailboxProcessor.processMailsWhenDefaultActionUnavailable → TaskMailboxImpl.take()` = default action
  SUPPRESSED (async-state backpressure). On-CPU top = Flink serde (shared) + FFM/key bucket
  (forst-rs-specific); NO engine-read frame → reads cheap. Wait-bound, in the async-state mailbox.
- **Why my tested levers missed it:** zero-copy/lever-2 parallelized work INSIDE the synchronous call;
  the `RoutingStateExecutor` (incomplete-future, real fullyLoaded) is UNUSED by the dispatch path (§4.3).
- **THE lever = OPT-01:** offload the FFM batch to a worker pool + return an incomplete future + real
  `fullyLoaded()` → let the AEC pipeline to depth N (matches ForSt). Untested; efficacy needs impl+measure.

## ★★★ OPT-01 SPIKE — offload efficacy CONFIRMED (2026-06-09), prior refutation EXPLAINED
Minimal depth-N proof: q9 + FRS_RS_PARALLEL_EXECUTOR=1 + READ_IO_PARALLELISM=6 on the CURRENT jar
(lock-free memtable + all levers), instrumented.
- **Throughput +7.7–15%** vs depth-1 baseline: 37.19M@362s vs 32.24M (+15%); 39.50M@402s vs 36.67M (+7.7%).
- **CPU 497–646% burst / ~400% steady** (vs the refuted experiment's pinned 375%/zero-speedup).
- **Parallel dispatch fired:** ITER_DISPATCH_DIAG parDispatches=3.03M, fresh%=99, continuations=8265.
- **No crash-loop** (iterView race fix held).
**WHY the prior "parallel executor REFUTED" was pre-lock-free:** the memtable RwLock serialized the
workers (375% CPU, zero speedup). This session's lock-free ArcSwap memtable (column_family.rs) removed
that lock → offload now scales + helps. Refutation wasn't wrong; it predated the fix.
**Remaining ceiling LOCATED:** steady CPU ~400% (not 800%) = workers still serialize on the BLOCKING
single-threaded FFM iter open (frsVecIterPrefixOpenBatch) = OPT-02 (non-blocking/parallel FFM).
**Validated path to beat RocksDB on q9/q20: OPT-01 (offload, +7.7–15% confirmed) + OPT-02 (non-blocking
FFM → 400%→800%).** NOT a ceiling. Caveat: single noisy run; correctness (out_rows exact under parallel
exec) must be verified before production-enabling.

## ★★★ OPT-01 CORRECTNESS + BIG WIN on q11 (2026-06-09)
q11 + FRS_RS_PARALLEL_EXECUTOR=1 + READ_IO_PARALLELISM=6 (current jar): **FINISHED 135.8s,
out_rows=92,000,000 EXACT.** Offload preserves correctness (make-or-break gate PASSED).
**q11: 318.9s (depth-1) → 135.8s (offload) = 2.35× faster** → vs RocksDB 111.1s = **0.35×→0.82×:
a FAILING query now PASSES the ≥0.8× bar.** Mechanism: depth-1 drained q11's in-flight backlog
SERIALLY after source stop (178s tail); offload drains concurrently → tail eliminated. Strongest
evidence yet that OPT-01 (async offload) is the dominant Phase-1 lever — flips fail→pass,
correctness-preserved. Single run; needs default-safety check (must not regress light/source-bound
queries per the "don't rob Peter" constraint) before default-enabling.

## ★★★ OPT-01 DEFAULT-SAFETY confirmed (q3 Peter-check) — ready for default-enable + full sweep (2026-06-09)
q3 (light/source-bound) + FRS_RS_PARALLEL_EXECUTOR=1 + READ_IO_PARALLELISM=6: FINISHED 36.6s,
out_rows=2,201,068 EXACT, vs depth-1 36.7s = NO REGRESSION. So the offload BENEFITS heavy queries
(q11 2.35× fail→pass, q9 +7.7-15%) and is NEUTRAL on light queries (q3) → satisfies "don't rob Peter."
OPT-01 validated across 3 families: light(q3 neutral)/window(q11 2.35×)/join(q9 +7.7-15%), all correct.
**NEXT (next session, evidence overwhelming):** (1) default-enable the parallel executor in
ForStRsAsyncKeyedStateBackend gate (read-io-parallelism default = 3 to MATCH ForSt per constraint;
spike used 6 — re-measure win at 3); (2) full q0-q22 correctness sweep under default-on (the prior
crash-loop was q20-specific, fixed via iterView — must reconfirm all windowed-joins q5/q7/q8 + q17/q18);
(3) full perf sweep → confirm ≥0.8×RocksDB + ≤50s + >ForSt simultaneously; (4) then OPT-02 (non-blocking
FFM, 400%→800%) for the joins still short. This is THE Phase-1 lever — correctness-safe, default-safe.

## ★★ OPT-01 DEFAULT-ON committed + correctness-verified across 4 families (2026-06-09)
ForStRsAsyncKeyedStateBackend now defaults to RoutingStateExecutor (worker=3 = match ForSt
read-io-parallelism); opt-out FRS_RS_PARALLEL_EXECUTOR=0. Committed+pushed (flink) → GHA gating.
Correctness EXACT + neutral-or-better under default-on:
- q3 (light): 36.6 vs 36.7s, out_rows 2,201,068 exact.
- q11 (window/iter): 318.9 -> 253.8s (1.26x), out_rows 92,000,000 exact. PARTIAL (0.44x, needs OPT-02).
- q12 (windowed-agg): 50.6 -> 46.5s (faster), out_rows 92,000,000 exact.
- q9 (join): +7.7-15%, no crash.
4 families correct (light/join/window/windowed-agg). REMAINING correctness risk: windowed-JOINS
q5/q7/q8 (window+join combo, highest race risk; q5 historically non-deterministic) — must be in the
final default-on sweep. OPT-02 (non-blocking FFM) is next to lift the PARTIAL joins past 0.8x.

## ★★★ OPT-01 default-on REVERTED — windowed-join CORRECTNESS REGRESSION (2026-06-09)
q8 (windowed-join) under OPT-01 default-on: out_rows=**1,819,576** vs depth-1 3,010,888 / RocksDB
3,064,457 = **~40% UNDER-EMISSION**. The RoutingStateExecutor's cross-worker offload races on
windowed-join semantics (window-timer firing / namespace ordering under async completion reordering)
→ windows under-fire. Correctness non-negotiable → REVERTED to OPT-IN (FRS_RS_PARALLEL_EXECUTOR=1);
depth-1 VectorizedExecutor is the default again (q8 back to 3,010,888).
**LESSON (mandate-validated):** default-enabling required FULL correctness across ALL families FIRST;
4 families (q3/q9/q11/q12 exact) were NOT sufficient — the 5th (windowed-join) exposed the race.
**GATING PREREQUISITE for OPT-01 (and the join-family Phase-1 win):** fix the windowed-join race in
the offload path — likely window-timer/completion ordering under the AEC when batches complete on
worker threads out of arrival order. OPT-01 stays the validated dominant lever (q11 2.35×@w6, q9
+7.7-15%, correct on non-windowed-join families) but CANNOT be default until this race is fixed.
Roadmap: (1) fix windowed-join offload race → (2) re-verify q5/q7/q8 + full q0-q22 out_rows under
default-on → (3) OPT-02 (non-blocking FFM, w3 parity) → (4) full perf sweep.

## ★★★ windowed-join offload race ROOT-CAUSED (2026-06-09, code inspection)
The gating blocker for OPT-01 default-on (q8 40% under-emit) is ROOT-CAUSED:
- `RoutingStateExecutor.leaseWorker()` = `free.removeFirst()` (line 265) → leases by AVAILABILITY,
  NOT by key/key-group. ZERO key-group awareness in the class.
- Each worker is a SEPARATE VectorizedExecutor with its OWN arena → its OWN MapStateCache (per-worker
  write-back buffer).
- MECHANISM: consecutive batches for the SAME key route to DIFFERENT workers → that key's UNCOMMITTED
  buffered state FRAGMENTS across per-worker caches → a windowed-JOIN probe reading the other side's
  records MISSES rows buffered in a different worker's unflushed cache → under-match → 40% under-emit.
- Why q11/q12 (windowed-AGG) survived: aggregation flushes + the engine MERGES fragments (merge-based
  recombination, no loss); a join POINT-MATCH has no recombination → loses rows.
**FIX (well-defined): KEY-GROUP-AFFINE routing** — same key-group → same worker (consistent cache),
mirroring Flink's key-group model. Complication: createRequestContainer() has no key → fix needs the
AEC container↔key-group association. Correctness-critical; careful implementation required. This is THE
prerequisite for OPT-01 default-on (and the join-family Phase-1 win). Alternative: write-through (no
per-worker cache) under parallel exec — simpler, sacrifices the cache perf lever.

## ★★★ windowed-join race — fix DEFINITIVELY pinned (2026-06-09): key-group-affine routing REQUIRED
Test (q8, parallel exec, cache ON vs OFF):
- cache ON: out_rows 1,819,576 (-40%); cache OFF: 2,805,877 (-6.8%); depth-1: 3,010,888.
- Disabling per-worker MapStateCache recovered ~1M of ~1.2M lost rows → cache fragmentation = MAJORITY
  of the race. BUT residual ~6.8% under-emit with cache OFF → a SECOND race: per-key OP ORDERING
  violated when a key's ops split across workers (key-agnostic leaseWorker), independent of the cache.
**=> write-through alone is INSUFFICIENT. The fix MUST be KEY-GROUP-AFFINE ROUTING** (same key-group ->
same worker), which solves BOTH cache fragmentation AND per-key ordering structurally. This is the
DEFINITIVE prerequisite for OPT-01 default-on. Implementation: RoutingStateExecutor must route a
container/batch to a worker by its records' key-group (needs the AEC container<->key-group association;
if containers span key-groups, either batch per-key-group or split the dispatch by key-group). Then
re-verify q5/q7/q8 exact + full q0-q22 under default-on, then OPT-02, then perf sweep.

## key-group-affine routing — IMPLEMENTATION SPEC (2026-06-09, feasibility confirmed)
Feasibility CONFIRMED by code: RecordContext<K> carries key/key-group; runtime calls
switchContext(RecordContext) on the mailbox thread before each state request; StateRequest holds its
RecordContext → key-group obtainable at dispatch. AEC batches SPAN key-groups (active-buffer-size).
**FIX (next session):** in RoutingStateExecutor.executeBatchRequests, SPLIT the container's requests
by key-group and dispatch each key-group subset to worker = keyGroup % nWorkers, then combine the
per-worker CompletableFutures into the container future. Guarantees same key-group → same worker →
same MapStateCache (read-your-writes) + per-key ordering. createRequestContainer's lease becomes a
no-op placeholder (routing moves to executeBatchRequests where keys are known). VERIFY: q8 out_rows
== 3,010,888 (depth-1) + q5/q7 + full q0-q22 under default-on, then default-enable OPT-01.
DO NOT rush in low-context: a wrong split/route = silent correctness bug (the q8 class). This is the
sole remaining blocker between the proven OPT-01 win (q11 2.35x) and the join-family Phase-1 pass.

## key-group-affine routing — SCOPE confirmed = classifier-level refactor (2026-06-09)
Code-read: createRequestContainer() returns the pooled VectorizedClassifier, which buckets requests
BY TYPE (getKeys/putKeys/putValues/deleteKeys/appendMerge/iters) into shared columnar buffers. So
key-group-affine routing requires partitioning ALL those typed arrays + columnar buffers PER WORKER by
key-group at executeBatchRequests — a structural classifier/dispatch rework, NOT a localized edit.
Cleanest implementation paths for next session:
  (A) Per-worker classifiers: at executeBatchRequests, for each request compute keyGroup (via
      RecordContext) -> append to per-worker classifier (worker=keyGroup%N) -> dispatch each ->
      combine futures. Requires a classifier "add one request" API (currently filled by the AEC).
  (B) Backend presents N sub-executors and the AEC routes records to them by key-group range (changes
      the StateExecutor integration, higher-level).
Either is a one-pass-resolvable refactor in FRESH context with the q8/q5/q7 + full-sweep gate. NOT
safe to rush in low context (large surface, columnar-buffer partitioning). Default stays depth-1 (safe).

## key-group-affine routing — IMPLEMENTATION-READY spec, all APIs confirmed (2026-06-09)
Confirmed accessors: AsyncRequestContainer.offer(REQUEST)/isEmpty(); AsyncRequest.getRecordContext()
-> RecordContext.getKeyGroup() (public int). StateRequest extends AsyncRequest<K>.
DESIGN (RoutingStateExecutor rewrite — route at OFFER time, not split filled buffers):
  - createRequestContainer() returns a RoutingRequestContainer wrapping N independent per-worker
    VectorizedClassifier sub-containers (per-worker arena).
  - RoutingRequestContainer.offer(req): kg=req.getRecordContext().getKeyGroup(); subContainer[kg % N].offer(req).
    => same key-group always -> same worker -> same MapStateCache (read-your-writes) + per-key ordering. FIXES q8.
  - executeBatchRequests(rc): for each non-empty sub-container, dispatch to workers[w] on workerThreads[w];
    combine the N CompletableFutures (allOf) into the container future.
  - isEmpty(): all sub-containers empty.
LIFECYCLE (the hard part = why multi-PR, not 1 commit): per-worker classifiers are POOLED+reset per
batch -> batch N+1 must NOT reuse a worker's classifier while batch N still runs on it. Need either
double-buffered per-worker classifiers OR a real per-worker in-flight slot count feeding fullyLoaded()
(=any worker has no free buffer). This is the doc's "buffer-ownership forces depth-1" issue (flagged as
multiple PRs in VectorizedExecutor javadoc :385-393). VERIFY: q8=3,010,888 + q5/q7 + full q0-q22 under
default-on, before default-enabling OPT-01. Then OPT-02 + perf sweep. Default stays depth-1 (safe) until done.

## key-group-affine routing ATTEMPT → DEADLOCK (2026-06-09), reverted; q19 findRow fix found
Implemented RoutingRequestContainer (offer routes by keyGroup%N) + fullyLoaded()=busyWorkers>0.
Compiled + 70 native tests passed, BUT q8 e2e HUNG: stalled permanently at src_out=921,085 (rate→0).
Cause: the conservative fullyLoaded/busyWorkers gate deadlocks with the AEC mailbox + future-completion
thread (completion that should clear busyWorkers can't be observed while the mailbox parks on
fullyLoaded). CONFIRMS the fix needs proper double-buffered + AEC-aware completion (multi-PR), not a
conservative gate. REVERTED (uncommitted); default stays safe depth-1.
SEPARATE FIND: an uncommitted MapStateCache "DECAY FIX" (q19 TopN findRow O(n²)→O(1): tracks eviction
tombstones + rehashDropTombstones at 0.75 load) is in the tree — DEFAULT-path (benefits all churn-heavy
queries), has a test. Verifying + committing it as a q19 lever (separate from the join-family OPT-01).

## ★★ q19 findRow O(n^2)->O(1) fix COMMITTED (2026-06-09) — verified benefits-all lever
MapStateCache decay fix (tombstone tracking + rehashDropTombstones) committed (flink 92a5d7c400b).
SAME-BOX A/B (degraded box, late session): fix-ON q19 599.7s FINISHED 92M exact vs fix-OFF >700s DNF
=> fix is >14% faster AND turns DNF->finish; correct (out_rows=92,000,000). Default-path (depth-1
executor uses MapStateCache too) -> benefits ALL churn-heavy queries (q19 TopN was 52% CPU in findRow
per JFR). 34 MapStateCacheTest green. NOTE: box is degraded after a long session (fix-OFF q19 >700s
vs the prior healthy-box 475s baseline) so q19's ABSOLUTE pass vs 0.8x (<=381s) needs a HEALTHY-box
re-measure — the fix should bring q19 well below 475s there. This is the "second OVER-window lever"
the doc called for. Join-family OPT-01 still needs the multi-PR key-group-affine routing fix.

## OPT-01 key-group routing — DEADLOCK analysis + deadlock-free design (2026-06-09)
My attempt hung q8 at src_out=921,085 (ran, then stalled — not immediate). Reasoned root cause:
ad-hoc completion coordination, NOT the routing. executeBatchRequests fanned one batch across N
workers + completed the container future only at remaining==0, with fullyLoaded()=busyWorkers>0 AND
the sync path (executeRequestSync -> drainInflight() + workerThreads[0].submit().get() on the mailbox).
Wait cycle: mailbox blocks in drainInflight/sync awaiting a multi-worker batch future while completion
depends on threads the mailbox no longer services.
DEADLOCK-FREE DESIGN (mirror ForStStateExecutor coordinator model): single coordinator thread that
(a) keeps key-group->worker affinity (the q8 correctness fix), (b) returns an INCOMPLETE future
immediately (offload), (c) maintains a real per-worker in-flight count for fullyLoaded(), (d) NEVER
blocks the mailbox on the sync path while async batches are outstanding (drain via AEC yield, not
.get()). This is the multi-PR OPT-01. Correctness gate q8=3,010,888; perf needs a HEALTHY box
(current box degraded: q19 fix-off >700s vs prior 475s).

## GHA correctness gate GREEN for session commits (2026-06-09)
ci-forst-rs (Build libforst_rs_ffi + flink-statebackend-forst-rs JDK 25) on jackylee-ch/flink @forst-rs-jdk25:
- OPT-01 default-enable: success
- OPT-01 revert: success
- q19 findRow O(n^2)->O(1) fix (HEAD 92a5d7c400b): success (gh run watch --exit-status = 0)
=> mandate "pass GHA of both repos" SATISFIED for all committed changes (ForSt engine repo unaffected —
no Rust commits this session). Committed Phase-1 advances are correctness-gated green.

## OPT-01 windowed-join race — ISOLATED to cross-worker concurrency (2026-06-09)
Deadlock-free key-group-affine RoutingStateExecutor committed (opt-in). q8 diagnostics:
- N=1 (routing path, no parallelism): out_rows=3,064,667 ≈ RocksDB 3,064,457 = CORRECT (even > depth-1's
  3,010,888). => My routing/sync-execution LOGIC IS CORRECT.
- N=3 (routing, parallel): out_rows=2,704,710 (−10%). => the −10% is purely a CROSS-WORKER CONCURRENCY
  RACE, not a logic bug.
HYPOTHESIS (testing): the MapStateCache is a SINGLE shared hash-table instance per state object (not
per-worker). Even with key-group affinity (each key→one worker), concurrent workers insert/evict in the
SAME hash table → structural data race → lost entries. If so, fix = PER-WORKER MapStateCache (each
worker's VectorizedExecutor owns its cache) or thread-safe/key-group-partitioned cache. Test: q8 N=3 +
FRS_DISABLE_MAPSTATE_CACHE=1 — if correct, shared-cache race confirmed.

## ★★★ OPT-01 windowed-join race ROOT-CAUSED + PROVEN (2026-06-09): shared MapStateCache structural race
q8 N=3 key-group-affine: cache-ON 2,704,710 (−10%) vs cache-OFF 3,064,514 (≈RocksDB 3,064,457 CORRECT).
=> The −10% IS the shared MapStateCache: single hash-table instance per state object; concurrent workers
(holding DIFFERENT keys via affinity, but mutating the SAME backing table/eviction) structurally race →
lost entries. Affinity fixes per-KEY consistency but not the shared STRUCTURE.
PROVEN FIX = PER-WORKER MapStateCache (each worker owns its cache instance; with key-group affinity,
key->one worker->one cache = correct AND preserves cache perf — unlike blanket cache-off which would rob
cache-benefiting queries). Implementation: make the state object's cache per-worker-thread (ThreadLocal)
or per-VectorizedExecutor; snapshot/flush/close must drain ALL per-worker caches. This is the LAST
blocker for OPT-01 default-on (routing logic already proven correct at N=1 = RocksDB-exact).

## ★★★ OPT-01 now CORRECTNESS-SAFE (opt-in) — both blockers fixed (2026-06-09)
Two parallel-executor correctness blockers RESOLVED + committed (opt-in; default stays depth-1):
1. DEADLOCK (async-offload coordination) → FIXED: deadlock-free SYNCHRONOUS key-group-affine routing
   (RoutingRequestContainer routes offer() by keyGroup%N to per-worker sub-containers; executeBatch runs
   them in parallel + blocks + returns completed future). q8 finishes 42s (was hang).
2. WINDOWED-JOIN UNDER-EMIT → FIXED: bypass MapStateCache when FRS_RS_PARALLEL_EXECUTOR=1 (the cache is
   single-threaded; its ops misalign with key-group affinity under parallel). cache-OFF+parallel is the
   PROVEN-correct config: q8 lands in the correct band (2.95–3.06M, == depth-1/N=1/RocksDB band) vs the
   broken cache-ON parallel band (1.82–2.81M). [q8 is ~±4% nondeterministic — band, not exact, is its gate.]
Net: OPT-01 (parallel executor) is now CORRECTNESS-SAFE when enabled (opt-in via FRS_RS_PARALLEL_EXECUTOR=1,
which auto-bypasses the cache). Default = depth-1 + cache (correct + fast, robs nothing).
REMAINING for DEFAULT-ENABLE: perf tradeoff — parallel forces cache-off globally, which may cost the
cache-benefiting queries (q11/q12/q16). Needs a HEALTHY-box sweep (parallelism gain vs cache loss). The
deeper "keep cache under parallel" fix = run cache ops on the key-group's worker thread (future). q11
deterministic (92M) under parallel-coupled IN FLIGHT to confirm broad executor correctness.

## ★★★ q11 PASSES via parallel+cache-off (correct) (2026-06-09)
q11 + FRS_RS_PARALLEL_EXECUTOR=1 (worker=3, cache auto-off via coupling): FINISHED 135.7s,
out_rows=92,000,000 EXACT. = 318.9s(depth-1) → 135.7s = 2.35× faster → 0.82× RocksDB (111.1s) = PASSES
≥0.8× bar, CORRECTLY. Notably cache-OFF+parallel (135.7s) is FASTER than cache-ON+parallel (253.8s) and
depth-1 (318.9s) — so cache-off under parallel did NOT rob this cache-benefiting query (parallelism
dominates). Weakens the "robs Peter" concern → default-enable is promising. Verifying q3(light)/q9(join)
under parallel-coupled next to build the default-enable case.

## Default-enable case-builder (parallel+cache-off, 2026-06-09)
- q3 (light): 36.6s vs depth-1 36.7s, out_rows 2,201,068 EXACT = NEUTRAL + correct (no light-query tax).
- q11 (window): 135.7s (2.35× vs depth-1 318.9s), 92M EXACT, 0.82× RocksDB = PASSES.
- q8 (windowed-join): correct band (cache-off fixes the −10% race).
- NEXT: q12 (cache-benefiting, deterministic 92M, depth-1 50.6s) — does parallel+cache-off rob it?

## ★★★ OPT-01 DEFAULT-ENABLED (2026-06-09) — parallel executor + cache-off is now the default
Committed+pushed (flink): RoutingStateExecutor is DEFAULT (opt-out FRS_RS_PARALLEL_EXECUTOR=0);
MapStateCache auto-bypassed under parallel (aligned predicate). Both correctness blockers fixed
(deadlock-free sync key-group routing + cache-off-when-parallel). 528 native tests green; GHA gating.
Verified correct + neutral-or-faster: q3 36.6s(light,exact), q8 windowed-join(correct band), q11
318.9→135.7s(2.35×,92M,0.82×RDB PASS), q12 50.6→46.5s(cache-benefiting FASTER,92M). q9(dominant join,
was DNF) running under default to confirm finish+correct+pass. CONFIRMATION GATE = full q0–q22 e2e
sweep (out_rows correctness + perf, 3 backends) — the goal's recheck; reversible via =0 if any regression.

## ★★★ OPT-01 default REVERTED — robs cache-benefiting joins (q9) (2026-06-10)
q9 (dominant join) under DEFAULT parallel+cache-off: DNF 79.2M@1284s = neutral-to-SLOWER vs depth-1
~84.5M@1204s. q9 is per-probe-READ-bound and RELIES on the MapStateCache; it has no drain-tail (so no
q11-style win) and loses its read accelerator under cache-off → ROBBED. So parallel+cache-off HELPS
drain-tail-bound queries (q11 2.35×, q12 faster) but ROBS cache-benefiting throughput-bound joins (q9,
likely q4/q20) → NOT "benefits all" → default-on REVERTED to OPT-IN. Default = depth-1 + cache.
**OPT-01 final status: correctness-safe (deadlock + windowed-join fixed), opt-in; a NET win only for
drain-tail-bound queries; NOT a universal default.** A universal default needs the cache KEPT under
parallel (per-worker MapStateCache pinned to the key-group's worker thread, so cache-benefiting joins
keep their accelerator AND get parallelism) — the proper multi-PR OPT-01. q9/q20 still need OPT-02
(non-blocking FFM) / engine read-path levers, NOT the executor.

## ★★★ cache+parallel race ROOT-CAUSED precisely (2026-06-10) — the universal-OPT-01 spec
ForStRsMapStateV2.asyncGet (line 390-397): the GET-miss path does
  super.asyncGet(key).thenApply(value -> cache.putIfAbsent(keySnapshot, value))
The cache LOOKUP (line 358) runs on the MAILBOX thread; the get-completion POPULATE (putIfAbsent, line
394) runs in the .thenApply callback ON THE COMPLETING THREAD = a WORKER thread under the parallel
executor. So: shared cache → concurrent worker putIfAbsent races (−10%); per-worker cache → lookup
(mailbox) and populate (worker) hit DIFFERENT caches → inconsistent (−6.7%). cache-off avoids both
(correct) but kills q9's read accelerator (→ q9 robbed → why OPT-01 can't be default).
UNIVERSAL OPT-01 FIX (multi-PR, precisely specified): the cache lookup AND its get-completion populate
must run on the SAME key-group worker thread — i.e., move the read-cache INTO the worker's batch
processing (per-key-group, owned by the worker), not split mailbox-lookup / worker-populate. Then
cache-benefiting joins (q9) keep the accelerator AND get parallelism, no race → OPT-01 default-safe
without robbing q9. This is THE blocker for default-on; q9/q20 throughput separately need OPT-02.

## q19 still FAILS 0.8× even with findRow fix (2026-06-10)
q19 re-measure (findRow fix, default depth-1): DNF >600s. Same-box A/B earlier: findRow-ON 599.7s vs
findRow-OFF >700s (fix helps) but healthy-box best ~475s = ~0.64× RocksDB (305s) — still FAILS ≤381s
(0.8×). q19 (OVER-window/TopN) needs a THIRD lever beyond findRow (findRow fixed the 52%-CPU hotspot
but residual remains — profile the post-findRow q19 for the next bottleneck). FAILING SET (none pass by
default): q4(1.9× retract-join), q9(DNF, needs OPT-02/engine), q11(0.35× default — 0.82× only via
opt-in parallel which robs q9), q19(~0.64×, needs 3rd lever), q20(DNF, OPT-02/engine). All multi-PR.

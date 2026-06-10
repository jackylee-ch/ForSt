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

## ★★★★ BREAKTHROUGH: thread-safe MapStateCache → universal OPT-01 default (2026-06-10)
Root cause of windowed-join under-emit was the single-threaded MapStateCache racing under parallel
(the .thenApply get-completion putIfAbsent runs on worker threads). FIX: synchronize MapStateCache's
14 public methods (lookup returns an on-heap value → sound). Cache KEPT under parallel (removed
cache-off coupling). RESULT: q8 windowed-join = 3,064,457 EXACT (== RocksDB; BEATS depth-1's
3,010,888!) with cache KEPT + parallel. This is the universal unlock: cache-benefiting joins (q9)
retain the accelerator AND get parallelism → OPT-01 default-enabled WITHOUT robbing q9. Uncontended
monitor cheap on the default single-threaded path. Committed + pushed (GHA). Verified: q8 EXACT, q11
2.35×/92M/0.82×RDB PASS, q12 faster, q3 neutral. q9 re-test (cache kept) IN FLIGHT to confirm not-robbed.
NEXT: full q0-q22 sweep (3 backends) = the goal's recheck.

## OPT-01 universal default JUSTIFIED — q9 no longer robbed (2026-06-10)
q9 default-on with thread-safe cache KEPT: 81.6M@1184s ≈ depth-1 84.5M@1204s = NEUTRAL (cache-off-
parallel was 79.2M@1284s/61.7K/s → cache-kept 68.9K/s ≈ depth-1 70.2K/s; accelerator restored). So
default-on (thread-safe cache) is NEUTRAL-OR-BETTER across all tested families: q3 neutral, q8 EXACT
(3,064,457), q9 neutral (~0.85× both DNF), q11 2.35× PASS, q12 faster. No rob-Peter → default JUSTIFIED.
REMAINING: full q0–q22 sweep (passing-set no-regression + q20/q4 + 3-backend) = the goal's recheck.

## ★★★ OPT-01 default REVERTED again (3rd) — thread-safe cache robs q17 (2026-06-10)
q17 (group-agg, cache-HOT) default-on with thread-safe cache: FINISHED 270.0s vs depth-1 76.7s = 3.5×
REGRESSION (out_rows 92M correct). The parallel executor + CONTENDED synchronized-cache (q17 hammers
the cache at high hit-rate; multiple workers + mailbox contend the lock) catastrophically robs q17.
=> Every default shortcut robs SOME query: cache-OFF+parallel robs q9 (cache-benefiting joins);
synchronized-cache+parallel robs q17 (cache-hot agg, lock contention). Reverted default-on (git revert
e4d487df177) → known-good opt-in: default depth-1 + unsynchronized cache (q17 76.7s, q9 fast, ALL
correct+fast); OPT-01 opt-in (cache-off+parallel, correct, q11 2.35× when enabled).
**DEFINITIVE: OPT-01 universal default REQUIRES the deep cache-into-worker fix** — a LOCK-FREE
per-key-group cache OWNED by each worker (lookup+populate on the worker thread, no shared lock, no
cross-thread race). Then cache-hot queries (q17) keep zero-overhead cache AND cache-benefiting joins
(q9) keep the accelerator AND windowed-joins (q8) are correct AND drain-tail queries (q11) win — all
without robbing any. That is the multi-PR OPT-01; synchronization is insufficient (q17). Repo safe.

## ★★★ DEFINITIVE root-cause model for OPT-01 non-universality (2026-06-10)
q17/q11/q12 agg-state (ValueState/Aggregating/Reducing V2) do NOT use MapStateCache. So q17's 3.5×
regression (76.7→270s) is THE PARALLEL EXECUTOR ITSELF, not the cache lock. => OPT-01's
non-universality has TWO INDEPENDENT causes:
1. EXECUTOR OVERHEAD: the synchronous-blocking-per-batch RoutingStateExecutor adds per-batch fan-out +
   block latency that HELPS drain-tail-bound queries (q11 2.35×, eliminates the 178s serial drain) but
   CATASTROPHICALLY ROBS non-drain-tail queries (q17 3.5× slower). Independent of the cache.
2. CACHE RACE: the single-threaded MapStateCache races under parallel (q8 −40%); synchronized fixes
   correctness (q8 EXACT) but the lock robs cache-hot MapState queries.
=> TRUE universal OPT-01 needs BOTH: (a) a CORRECT ASYNC-OFFLOAD executor — incomplete future, NO
per-batch block (the deadlock-free-but-non-blocking design; my synchronous version's block is what robs
q17), so it pipelines for q11 WITHOUT adding latency for q17; (b) a LOCK-FREE per-worker cache. Both are
the multi-PR OPT-01 (matches the doc's ForSt-coordinator design). Synchronous-block + sync-cache (this
session's shortcuts) are insufficient. OPT-01 stays OPT-IN (net win only for drain-tail queries).
Default = depth-1 (correct + fast for ALL). Repo safe; 3 reverts each caught a real regression.

## q19 third lever = diffuse serde+engine (2026-06-10, JFR analysis)
Pre-findRow JFR (q19-decay.jfr, Jun8): findRow 52% (34,996/66,989 on-CPU) — the hotspot the committed
findRow O(n^2)->O(1) fix removed. The NEXT frames (the post-findRow residual): Flink serde
(RowDataSerializer.copyRowData ~2480, copy ~2149, StringDataSerializer.copy ~2181, String.charAt ~1287)
+ engine read + MapStateCache.hashOf ~2509 + RowDataEventDeserializer. => post-findRow q19 residual is
DIFFUSE serde + engine-read (same class as q9), NOT a single fixable hotspot. q19's third lever is
multi-PR (engine read-path / serde-reduction), not a quick win. (Current JFR re-capture failed —
container killed at MAXSEC before async dump flushed; needs MAXSEC>=500 to re-capture post-findRow.)
=> ALL failing queries (q4/q9/q11/q19/q20) need multi-PR/diffuse work; none is a quick default-path win.

## CORRECTION (2026-06-10): first "full sweep" ran a STALE PARALLEL jar — invalid; HEAD is safe
The post-revert `run-8c32g.sh jar` was a stale incremental build (didn't pick up the git-revert), so
the first q0-q22 sweep ran a PARALLEL+cache-on-unsync jar (q8=1,938,439 broken cache-race, q11=134.7s
parallel) — INVALID for the committed depth-1 default. CLEAN rebuild (`mvn clean package`) + redeploy;
q8 depth-1 verify = 3,064,445 ≈ RocksDB 3,064,457 → COMMITTED DEFAULT IS SAFE (depth-1, correct).
SIGNAL from the (invalid-cache) parallel sweep: parallel makes q7 FINISH (1052s) and q20 FINISH EXACT
(1610s = 93,201,404 == RocksDB) — both DNF at depth-1. So OPT-01 (parallel) helps the heaviest joins
FINISH (q9 still DNF even parallel @1700s). With the cache correct under parallel (the multi-PR fix),
q7/q20 would finish correctly + parallel. Relaunching the VALID sweep on the clean committed jar.

## ★★ VALID q0-q22 forst-rs sweep (committed depth-1 default, 2026-06-10)
Clean committed jar (depth-1 default). out_rows correct except noted:
q0 30.9s/100M | q1 29.3s/100M | q2 27.8s/100M | q3 35.6s/2,201,068 | q4 373.0s/25,835,082(retract) |
q5 41.6s/808,765(windowed-agg, correctness?) | q7 1441.6s/92,000,002(finishes) | q8 43.6s/3,064,445(✓) |
q9 DNF@1700 | q10 124.2s/100M | q11 264.8s/92M | q12 49.6s/92M | q13 28.5s | q14 29.4s | q15 161.8s/92M |
q16 323.9s/92M | q17 77.7s/92M | q18 222.9s/92M | q19 527.9s/92M | q20 DNF@1700/93.2M(exact when finishes) |
q21 53.9s | q22 43.6s.
PASS (≥0.8×RDB, ≤50s, correct): q0/q1/q2/q3/q8/q12/q17/q18 (+ light q10/q13/q14/q21/q22). 
FAIL: q4 (+59.5s >50s regression), q11 (0.42×), q19 (0.58×), q9 (DNF near-parity), q20 (DNF vs RDB 800s).
q5 correctness suspect. q7 finishes 1441s (RDB baseline pending).
=> 5 failing queries (q4/q9/q11/q19/q20) need the multi-PR levers: universal OPT-01 (parallel+lock-free
per-worker cache → q11 pass, q7/q20 finish faster — the invalid parallel sweep showed q11=134.7s,
q20=1610s EXACT, q7=1052s), OPT-02 (q9), q19 3rd lever (diffuse serde+engine), q4 (-9.5s to clear).
3-backend: RocksDB/ForSt baselines in earlier rows; forst-rs side now complete.

## ★★ SAME-SESSION frs-vs-RocksDB sweep (2026-06-10) — valid apples-to-apples
forst-rs (SWEEP2 depth-1) vs RocksDB (RDBSWEEP), same box/session:
WINS (frs beats RocksDB): q7 (1441.6s FINISHES vs RocksDB DNF@1700!), q15 (161.8 vs 229.7), q16 (323.9
vs 374.1), q18 (222.9 vs 366.4) + light q0/q1/q2/q10/q13/q21/q22 faster.
PARITY-PASS: q3 0.98×, q8 0.97×, q12 0.83×, q14 ~1.0×, q17 0.94×.
FAILS: q4 (+70.3s>50s; out_rows 25.8M vs 177.6M = retract-changelog cadence, needs final-result check),
q9 (DNF vs RocksDB 1420.5s), q11 (0.40×), q19 (0.59×), q20 (DNF vs 859.7s; finishes 1610s EXACT parallel).
★ CORRECTNESS BUG: q5 out_rows 808,765 vs RocksDB 29,988,416 = −97% UNDER-EMIT. q5 (windowed-agg) is
WRONG (confirms historical q5 nondeterminism/under-emit). MUST FIX before its perf counts — top priority
(correctness non-negotiable). Note q5 frs=41.6s (fast but WRONG).
=> frs already WINS q7/q15/q16/q18 vs RocksDB. Genuine remaining: q5 (correctness), q9/q11/q19/q20 (perf,
multi-PR levers), q4 (+marginal). 3-backend: ForSt sweep next to complete the matrix.

# ═══════════════════════════════════════════════════════════════════════════
# CURRENT STATUS — complete same-session 3-backend q0-q22 sweep (2026-06-10)
# ═══════════════════════════════════════════════════════════════════════════
forst-rs = committed DEFAULT (depth-1, opt-in OPT-01). All three on the same box/session (8c/32g).
Goal per query: frs ≥0.8× RocksDB AND ≤50s regression AND strictly faster than ForSt AND correct.

| q  | forst-rs | RocksDB | ForSt   | vs RDB | vs ForSt | VERDICT |
|----|----------|---------|---------|--------|----------|---------|
| q0 | 30.9s    | 31.8s   | 30.7s   | faster | +0.2s    | ~PASS (ForSt parity, 0.2s) |
| q1 | 29.3s    | 30.1s   | 30.1s   | faster | faster   | PASS |
| q2 | 27.8s    | 28.2s   | 28.2s   | faster | faster   | PASS |
| q3 | 35.6s    | 35.0s   | 37.2s   | 0.98×  | faster   | PASS |
| q4 | 373.0s   | 302.7s  | 1042.2s | +70.3s | faster   | FAIL RDB (>50s); out_rows retract-cadence (25.8M vs 177.6M) needs final-result check |
| q5 | 41.6s    | 162.5s  | DNF@400 | —      | —        | ✗✗ CORRECTNESS: 808,765 vs RDB 29,988,416 (−97%) — WRONG |
| q7 | 1441.6s  | DNF     | 586.8s  | BEATS  | slower   | FAIL ForSt (frs 1441 > ForSt 587); beats RocksDB |
| q8 | 43.6s    | 42.5s   | 39.2s   | 0.97×  | +4.4s    | FAIL ForSt (4.4s) |
| q9 | DNF      | 1420.5s | DNF     | DNF    | par      | FAIL RDB (RocksDB finishes, frs DNF) |
| q10| 124.2s   | 129.5s  | 129.3s  | faster | faster   | PASS |
| q11| 264.8s   | 106.1s  | 134.9s  | 0.40×  | slower   | FAIL both |
| q12| 49.6s    | 41.4s   | 39.7s   | 0.83×  | +9.9s    | FAIL ForSt |
| q13| 28.5s    | 29.3s   | 29.5s   | faster | faster   | PASS |
| q14| 29.4s    | 29.6s   | 29.2s   | faster | +0.2s    | ~PASS (ForSt parity, 0.2s) |
| q15| 161.8s   | 229.7s  | 206.8s  | faster | faster   | PASS (beats both) |
| q16| 323.9s   | 374.1s  | 331.7s  | faster | faster   | PASS (beats both) |
| q17| 77.7s    | 72.8s   | 253.0s  | 0.94×  | faster   | PASS |
| q18| 222.9s   | 366.4s  | DNF@400 | faster | BEATS    | PASS (beats both) |
| q19| 527.9s   | 310.2s  | 308.1s  | 0.59×  | slower   | FAIL both |
| q20| DNF      | 859.7s  | 1535.9s | DNF    | (1610 par)| FAIL RDB (DNF; finishes 1610s EXACT under opt-in parallel) |
| q21| 53.9s    | 60.0s   | 58.3s   | faster | faster   | PASS |
| q22| 43.6s    | 44.2s   | 46.3s   | faster | faster   | PASS |

## SUMMARY (current state, committed default)
- PASS (beat RocksDB+ForSt, correct): q1,q2,q3,q10,q13,q15,q16,q17,q18,q21,q22 = **11 clear**.
  (q15/q16/q18 beat BOTH; q7/q18 finish where a competitor DNFs.)
- ~PASS marginal (ForSt parity ±0.2s): q0, q14.
- FAIL — slower than ForSt only (beats RocksDB): q7 (big), q8 (+4.4s), q12 (+9.9s).
- FAIL — RocksDB perf: q4 (+70s), q9 (DNF), q11 (0.40×), q19 (0.59×), q20 (DNF).
- FAIL — CORRECTNESS: q5 (−97% under-emit, windowed-agg bug) — TOP PRIORITY.

## REMAINING WORK to close Phase 1 (precisely scoped)
1. **q5 CORRECTNESS** (top priority): windowed-agg under-emit (−97%). Fix the sliding-window merge/fire.
2. **q11/q20** (and q7-vs-ForSt): universal OPT-01 = non-blocking async executor + LOCK-FREE per-worker
   cache (parallel sweep proved q11→134.7s, q20→1610s EXACT; the executor-block robs q17-class + the
   single-thread cache races — both must be fixed for a default).
3. **q9**: OPT-02 (non-blocking/parallel FFM) — DNF even under parallel.
4. **q19**: diffuse serde+engine read-path (post-findRow profile).
5. **q8/q12 vs ForSt**: small gaps (4-10s) — ForSt's windowed-agg path is faster; investigate.
6. **q4**: −20s+ to clear ≤50s (box-marginal; rigorous best 340s passes).
All committed levers (zero-copy, q19 findRow, deadlock-free routing, OPT-01 opt-in) are GHA-green; the
remaining are multi-PR architectural changes for a healthy box. Phase 1 NOT met: 11 pass, ~12 fail/marginal.

# ═══════════════════════════════════════════════════════════════════════════
# q9 DEEP PROFILE — verify-before-build campaign (2026-06-10)
# ═══════════════════════════════════════════════════════════════════════════
Run: q9 frs default (depth-1), 100M, MAXSEC=2400, FRS_DECAY_DIAG+FRS_RSS_SAMPLE.
- Curve: 65K/s avg to 622s (40.3M) → oscillating decay 18.8-45K/s → 90.1M@2313s,
  cut at 2400s (~91M). TRUE FINISH ≈ 2600s vs bar 1776s (0.8× RDB 1420.5) → needs ~1.47×.
- LSM HEALTHY during decay: L0≤3, sub-GiB/CF (DECAY_DIAG) → NOT compaction/L0 pileup.
- CPU (12s /proc window, deep decay): box ~8% utilized. Compaction 2×~25%-core,
  datagen 4×~3%, opendal pool ~1.4%×10+, join task threads <0.5% in-window;
  cumulative join-thread CPU = 415s/2085s elapsed = 20% duty cycle.
- Thread dumps (×2, all 4 join threads identical signature): RUNNABLE inside ONE
  synchronous FFM downcall — frsVecIterPrefixOpen (ForStRsLinker:3949 ←
  ForStRsDBIterRequest.process:300 ← VectorizedExecutor.drainIterSerial:1460) and
  vectorizedBatchGet (ForStRsLinker:2607 ← executeGets:1235) — invoked from
  AsyncExecutionController.drainInflightRecords ← processWatermark. I.e. EVERY
  watermark forces a FULL inline serial drain of all pending probes on the task thread.
- Engine code: every non-resident block read = handle.block_on(opendal read)
  (forst-rs-io/src/opendal_backend.rs:478-485,683,699) = tokio handoff round-trip
  per op EVEN ON LOCAL DISK (same disease the SST WRITE path had and fixed by
  coalescing ~500× — see :823-827 comment; read path still per-op).

## VERDICT: q9 (and the q7/q20 family) is LATENCY-bound, not CPU/disk/compaction-bound
Per-record cost ≈ 120µs (33K/s ÷ 4 threads) = serial synchronous engine round-trips
× async-dispatch latency floor. Throughput = 1/latency; decays as deeper state adds
block reads per seek. ForSt q9 DNF too (parallel executor alone insufficient);
RocksDB finishes because sync in-process block-cache reads have a µs-class floor.

## Reordered optimization program (verified)
1. ENGINE direct-local-read fast path: bypass block_on/tokio for local-cache/disk
   hits → per-op µs-class. Benefits ALL read queries (q9/q19/q7/q20/q4). NEW co-primary.
2. Coordinated executor (approved design, unchanged): ×~3 latency hiding + non-blocking
   mailbox + backpressure; measured floors q11 134.7 / q20 1610 / q7 1052.
3. Batched iter-open (one FFM call, engine parallel seek) — folded into #2's iter dispatch.
4. Micro-levers (decode allocs, mailbox serialization) — re-rank after 1+2 land.
Estimates (A=executor, B=direct-read): q7 1441→A ~750-1000 → A+B ~500-700 (bar <587);
q9 ~2600→A ~1100-1500 → A+B ~700-1200 (bar 1776); q20 1610(par)→A ~1200-1450 →
A+B ~700-1000 (bar 1074); q11 134.7 measured (bar 132.6). GO on A+B.

# ═══════════════════════════════════════════════════════════════════════════
# q9 READ_AT_DIAG discriminator (2026-06-10) — Stage-2 lever CORRECTED
# ═══════════════════════════════════════════════════════════════════════════
Run: q9 EVENTS_NUM=50M, FRS_READ_AT_DIAG=1. FINISHED 813.5s, out_rows=45,904,788
(fast A/B baseline for read-path levers).
- Histogram (LocalFirstSstFile.read_at — the SYNC local-cache-hit pread path):
  reads=6.3M→9.4M across decay; mean 146µs→191µs; cold(≥20µs) 24.9%→29.1% RISING;
  ~72% warm 1-5µs. ~0.6 preads/record × ~182µs ≈ 110µs/record = the measured floor.
- VERDICT: tokio/block_on REFUTED for local decay (sync fast path already exists and
  is where the time goes). Binder = COLD PREADS once state outgrows container page
  cache (Docker-VM disk ~0.1-1ms). Levers: (a) per-probe SST bloom/range pruning,
  (b) block-cache hit-rate for join hot set, (c) compaction shape. Engine-only.

# ═══════════════════════════════════════════════════════════════════════════
# PR-1 coordinated-executor GATES + bisect (2026-06-10) — two verdicts
# ═══════════════════════════════════════════════════════════════════════════
Jar = clean build @7a14ccaf1a7 (classifier pool + CoordinatedStateExecutor + 3-way gate).
GHA ci-forst-rs GREEN. All runs 8c/32g, 100M, FRS_RS_EXECUTOR as noted.

| run                        | wall    | out_rows   | verdict |
|----------------------------|---------|------------|---------|
| q17 coordinated N=3        | 271.8s  | 92M exact  | ✗ FAIL ≤85s gate (depth-1 = 77s; ForSt = 253s!) |
| q8  coordinated N=3        | 40.6s   | 2,511,323  | ✗ FAIL band (−18% under-emit) |
| q8  routing  N=3 (this jar)| 40.7s   | 3,064,485  | ✓ band (Task-1 pool exonerated) |
| q8  coordinated N=1        | 45.6s   | 3,064,540  | ✓ band (pipelining alone OK) |

## Verdict 1 — q17: the COORDINATOR HANDOFF is the q17 robber, not the blocking
Non-blocking coordinated N=3 = 271.8s ≈ ForSt's own q17 (253.0s). High-rate cheap-state
queries (1.2M rec/s) die on per-batch mailbox→worker→mailbox switches regardless of
blocking. Depth-1 inline is WHY frs beats ForSt 3.3× on q17. ⇒ default must be ADAPTIVE:
inline for get-only/cheap batches, offload iterator-containing batches (every measured
parallel win is iter-heavy; every robbery is cheap-batch handoff).

## Verdict 2 — q8 N=3: write/read ORDER INVERSION via mailbox-direct statebuf writes
MapStateV2 put/remove/clearForPrefix flush DIRECTLY to the engine on the mailbox
(MapStateArrowBuffer.putShared → flushTo :171/:177; clearForPrefix), while engine
reads (iters/gets) queue behind worker backlogs. With run-ahead (N=3; N=1 is throttled
by fullyLoaded=ongoing≥1 into quasi-lockstep) a LATER cleanup-remove lands BEFORE an
EARLIER-created iter executes → iter misses live rows → −18%. Matches all 3 bisect cells.
⇒ Correct fix = reads execute against a CREATION/DISPATCH-time MVCC snapshot (engine
has MVCC + snapshot_view): capture/pin seq per batch on the mailbox at dispatch, workers
read as-of-seq, release after batch. Restores exact depth-1 visibility semantics under
any scheduling. Required for ANY parallel default (incl. opt-in routing? NO — routing
blocks the mailbox, no run-ahead, proven correct).

# ═══════════════════════════════════════════════════════════════════════════
# ADAPTIVE-mode gates (2026-06-10, jar @b1f4a936dd8) — FAILED, two NEW findings
# ═══════════════════════════════════════════════════════════════════════════
| run               | result                       | verdict |
|-------------------|------------------------------|---------|
| q17 adaptive      | WEDGED src_out=0 → MAXSEC300 | ✗ job never progresses (startup wedge) |
| q8  adaptive      | 40.6s, out_rows=2,537,233    | ✗ −17% — SAME family as coordinated-N3 |
| q11 adaptive      | killed early (run aborted)   | — |

KEY IMPLICATION: q8 corrupts under adaptive even though its iter batches use the
EXACT blocking dispatch that measured CORRECT under routing (3,064,485, same jar).
The corruption is therefore NOT unique to non-blocking run-ahead; the INLINE
fast-path for iter-free batches (or its interleaving with worker batches) is
implicated. The run-ahead theory for coordinated-N3 q8 needs re-examination too —
the common factor across BOTH wrong modes is mailbox-executed iter-free batches
coexisting with worker-executed iter batches... but routing (all-on-workers) and
inline (all-on-mailbox) are each correct. NEXT DEBUG STEP: thread-dump a wedged
q17-adaptive TM (task thread stack at src_out=0) — the wedge is likely the same
defect as the corruption, and it is 100% reproducible in ~60s.

BONUS FINDING (q11, from the wedge investigation dump): GroupWindowAggregate runs
on the V1 SYNC ForStRsMapState via ForStRsInternalKvStateAdapters, and burns its
CPU in MergingWindowSet.initializeCache → ForStRsMapState.forEachEntry(:763) PER
ELEMENT — a full session-window map iteration per record (O(N)/record). This is a
REAL q11 lever INDEPENDENT of the executor (cache it per key / avoid re-drain).

STATE: default = depth-1 inline (untouched, safe); adaptive+coordinated+routing all
env-gated opt-in; 534 unit tests + GHA ci-forst-rs green @b1f4a936dd8.

# ADAPTIVE re-gate on LEAK-FIXED jar @1a95d0a010f (2026-06-10)
ROOT CAUSE of wedge+corruption FOUND+FIXED: Task-1 classifier pool leaked a classifier +
4 arena ColumnarBatchBuffers PER BATCH in all non-coordinated modes (incl. depth-1 default!)
→ q17-adaptive TM cgroup-OOM-killed ~13s (the wedge). Fix = SELF-RELEASE in
executeBatchRequests finally (1a95d0a010f) + inline path getNow-not-join.
| run (adaptive, fixed jar) | result | verdict |
|---|---|---|
| q17 | 148.8s, 92M exact | ✗ bar ≤85s — but FINISHES; corruption family CLEARED |
| q8  | 40.6s, 3,064,493  | ✓ IN BAND — adaptive corruption GONE (was leak/join-coupled) |
q17 residual 148.8 vs 77 depth-1: per-batch KEY-GROUP SPLITTING (≤3 sub-classifiers,
3 FFM calls/batch) taxes high-batch-rate queries even inline.
NEXT DESIGN (final iteration): split ONLY iter requests by kg; gets/puts in ONE shared
classifier executed INLINE FIRST (preserves writes-before-iters invariant), then latch
the iter sub-batches to workers. q17 → byte-identical to depth-1; iter queries keep wins.
Then gates: q17≤85, q8 band, q11≤140, q20, q9-noregress → default flip.
⚠ depth-1 DEFAULT also leaked on jars built from d82427efb1c..b1f4a936dd8 (≈1h window,
fixed by 1a95d0a010f) — any perf runs on those jars are SUSPECT; re-measure on fixed jar.

# ADAPTIVE design iterations (2026-06-10 late) — phase-split REFUTED ×2, affinity puzzle
| variant (jar) | q17 | q8 | verdict |
|---|---|---|---|
| full-split + inline subs (1a95d0a010f) | 148.8s | 3,064,493 ✓ | CORRECT, q17 ✗ bar |
| split-iters, main-first (4ec61c554ae)  | 112.7s | 1,441,352 ✗ −53% | fire-then-purge inverted |
| split-iters, iters-first (a11ac06a8c0) | — | 2,147,792 ✗ −30% | reverse hazard real too |
| defer-classify 1-classifier (f4c9d99b468) | 117.7s | 1,831,340 ✗ −40% | kg-UNAFFINE cheap batches corrupt |
| kg-affine restored (8fd6edddea7) | TBD | verifying | = full-split semantics, deferred offers |
LAW (empirical): q8 correct ⟺ EVERY batch executes through per-key-group-affine
classifiers in offer order (routing ✓, full-split ✓). ANY kg-unaffine execution of
cheap batches corrupts — even inline, lockstep, cache-off. ROOT CAUSE OPEN (suspect:
per-executor/per-classifier state coupling not yet identified — needs instrumented
1M-scale q8 bisect next session). Phase-splitting iters from deletes is unsound
(windowed fire-purge emits same-key ITER/CLEAR in both orders).
q17 cost ladder: depth-1 77s | defer-1-classifier 117.7 | full-split-inline 148.8 |
latch-offload 271.8 → the kg-splitting of cheap batches costs ~35-70s on q17; the
correct-and-fast-q17 design needs the affinity puzzle solved first.

# ═══════════════════════════════════════════════════════════════════════════
# FINAL adaptive verdict (2026-06-10 session end): NONDETERMINISTIC race — FLAKY
# ═══════════════════════════════════════════════════════════════════════════
q8 on the offer-time-restored adaptive (@6bfd4354683, semantically = the config that
once measured 3,064,493): **2,328,053 (−24%)**. The earlier in-band pass was LUCK.
q8-adaptive across runs: 2,537,233 / 3,064,493 / 1,441,352* / 2,147,792* / 1,831,340* /
2,328,053 (*=different designs). CONCLUSION: the inline-fast-path+worker mixed regime
has a real RACE; single-run out_rows verdicts are unreliable for executor work.
- MULTI-RUN-VERIFIED modes only: depth-1 inline (default, always in band) and
  routing all-latch (3,064,457 / 3,064,514 / 3,064,485 = three independent passes).
- The "offer-time serialization law" and "kg-affinity law" were derived from single
  runs — treat as HYPOTHESES, not laws, until a deterministic repro exists.
- NEXT SESSION MUST build a DETERMINISTIC q8 repro first (controlled-replay harness,
  sorted CSV input, p=1, byte-compare — the 2026-06-02 /tmp/corr-q5 pattern) before
  ANY further executor iteration. Then bisect inline-vs-latch with it.
- STATE: default depth-1 SAFE+correct; adaptive/coordinated OPT-IN, adaptive FLAKY
  (do not use for benchmarks); routing OPT-IN correct (the measured q11/q20/q7 wins).
  All pushed @6bfd4354683, 534 UTs green.

# q8 race characterization (2026-06-10 session end)
- Reduced-scale probes ALL deterministic+identical: 1M ×4 (inline ref + adaptive ×3) =
  30,390 exact; 10M ×3 (adaptive ×2 + inline) = 306,016 exact. Race is LOAD-DEPENDENT —
  needs full-100M sustained pressure (AEC in-flight saturation + checkpoint overlap +
  watermark drain cadence; reduced runs finish in 3-7s).
- Full-scale q8 = ~40s/run → the repro IS the full-scale run (~3 min cycle).
- VARIABLE ISOLATED: routing (=adaptive minus inline fast-path, all else identical) is
  correct ×3; adaptive wrong in 5/6 full-scale runs (1.44-3.06M spread). The racy
  ingredient = INLINE execution of iter-free batches on the mailbox interleaved (in time)
  with latch-dispatched iter batches. Mechanism candidate for next session: inline
  per-row completions run SYNCHRONOUSLY mid-batch on the mailbox (callbackRunner inline
  when already on mailbox) and can re-enter the AEC/active-container during the inline
  phase — vs the latch path where completions queue as mails. Instrument that re-entrancy
  first (count offers-into-active-container during inline execute).

# q8 race: RE-ENTRANCY REFUTED (2026-06-10 final probe)
FRS_REENTRY_DIAG run: corruption reproduced (q8 adaptive = 2,186,285) with ZERO
re-entrant executeBatchRequests calls. NEXT CANDIDATE (sharper): callback-inlining
asymmetry — InternalAsyncFuture.complete() from the MAILBOX thread may run user
callbacks INLINE-IMMEDIATELY (CallbackRunnerWrapper same-thread path) → user code
(window emit/cleanup, setCurrentNamespace, statebuf staging) runs MID-BATCH during
inline execution; worker-thread completions enqueue as mails instead. Adaptive MIXES
both regimes per batch → callback ordering differs from both pure modes (depth-1
all-inline ✓, routing all-enqueued ✓). NEXT: instrument WHERE per-row callbacks
execute (thread + during-batch flag) under each mode; compare orderings on the 40s
full-scale repro. Probe committed as FRS_REENTRY_DIAG (extend it).

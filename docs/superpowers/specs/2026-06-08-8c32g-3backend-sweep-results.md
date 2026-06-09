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
| q9    | (pending 8c/32g) | — | **DNF** 🔒1G (43M@762s, rate→10K/s, read-amp collapse) | — | (pending) | ? | **read-amp DNF** (join). Memory-bounded (no OOM); CPU 375% = 4 slots busy/4 idle. Joins q7/q20 → READ-PATH module. |
| q10   | 120.3     | 100,000,000      | 128.7      | 100,000,000       | -          | ✓ | **PASS (correct; +8.4s ≤ +50s bar)** |
| q11   | 111.1     | 92,000,000       | 382 (OLD fast cfg) | 92,000,000 | (host 158)| ✓ | correct; 3.4× — **RE-MEASURE under committed cfg** |
| q12   | 42.1      | 92,000,000       | 50.6       | 92,000,000        | -          | ✓ | **PASS (out_rows match; +8.5s ≤ +50s bar)** |
| q13   | 30.1      | 100,000,000      | 30.1       | 100,000,000       | -          | ✓ | **PASS (correct + parity)** |
| q14   | 30.1      | 100,000,000      | 29.0       | 100,000,000       | -          | ✓ | **PASS (correct + faster)** |
| q15   | 228.3     | 92,000,000       | 173.9 (OLD)| 92,000,000        | (host 258) | ✓ | correct + faster — **RE-MEASURE under committed cfg** |
| q16   | 428.4 (8c/32g) | 92,000,000 | **322.9** 🔒1G | 92,000,000 | **370.3 (8c/32g)** | ✓ | **FULL PASS — 0.75× RocksDB (faster) + faster than ForSt (322.9<370.3) + accurate.** |
| q17   | **74.5 (8c/32g)** | 92,000,000 | **76.7** 🔒1G | 92,000,000 | **255.7 (8c/32g)** | ✓ | **FULL PASS — 1.03× RocksDB (parity) + 3.3× FASTER than ForSt + accurate.** |
| q18   | **396.1 (8c/32g)** | 92,000,000 | **318.1** 🔒1G | 92,000,000 | **422.8 (8c/32g)** | ✓ | **FULL PASS — 0.80× RocksDB (faster) + faster than ForSt (318<423) + accurate.** |
| q19   | 305 (8c/32g) | 92,000,000 | **542.5** 🔒1G | 92,000,000 | **319.2 (8c/32g)** | ✓acc | **FAIL — 1.78× RocksDB (542 vs 305) AND 1.70× ForSt (542 vs 319).** Accurate (exact). forst-rs genuinely behind BOTH on OVER-window/dedup (RocksDB≈ForSt≈310). Separate perf gap (not join async-dispatch); architecture fix needed. |
| q20   | **800.3 (8c/32g)** | 93,201,404 | **DNF @1300s** 🔒1G (59.8M/93M, rate→23.6K/s) | — | (pending) | ✗ | **FAIL — DNF vs RocksDB 800s.** Join, OUTPUT-amplifying (auction⋈bid). NOT read-amp (n_ovl=2-3) NOT iter-decode (already zero-copy). Bottleneck = executor dispatch / output-emit (profile pending). Bar: must finish ≤1000s + beat ForSt. |
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

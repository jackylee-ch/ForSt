# Omnipotent architectural performance rethink — crush BOTH RocksDB and ForSt on the heavy NexMark set

**Date:** 2026-06-15
**Author:** PMC-1 (Flink PMC / state-backend + stream-engine architect) — PROFILE + MINI-BENCH + CODE only; NO production behavior change this cycle. Design returns for user approval (hard gate).
**Worktree:** `/tmp/omni-wt`, branch `omni-profile` off `origin/forst-rs` @ `09d231506`.
**Hardware:** Apple M-class Mac, 64 GiB, system allocator, local FS. Same-box ratios only; absolute seconds are Mac-population (the binding population is the remote x86/NVMe box — every wall-clock projection here is flagged for remote confirmation).

**Mandate (verbatim):** make forst-rs *completely crush* both backends on q4/q5/q7/q8/q9/q11/q12/q17/q18/q19/q20 — not "≤1.25×", but BEAT both. The prior "local levers exhausted / plateau" conclusion is REJECTED. This cycle: characterize the wall, find the architectural ceiling, and propose 2-3 AMBITIOUS approaches with mini-bench plans. **Investigation only.**

---

## 0. Where forst-rs actually stands today (the real starting line)

The lever stack already shipped (KV-sep default path, coalesced deref `FRS_VLOG_COALESCE_DEREF`, R1 adaptive S2, R2a routing-adaptive executor, write-amp policy, dynamic levels) has **already made forst-rs the total-time winner** on the binding local 100M sweep (`2026-06-14-nexmark-local-rocksdb-forst-forstrs-results.md`):

- **Total q0-q22: frs 11285.5s vs RocksDB 12436.3s (frs −10.2%) vs ForSt 13512.7s (frs −19.7%).** GOAL-2 is *met on total* already.

So this is NOT a rescue mission. It is a **GOAL-1 mission**: beat both on EVERY heavy query. The rows where forst-rs still loses to one or both backends (100M, local, binding):

| query | frs | RDB | ForSt | frs vs RDB | frs vs ForSt | gap class |
|---|---:|---:|---:|---|---|---|
| **q7** | **1729.4** | 1349.4 | 1336.6 | **1.28× LOSE** | **1.29× LOSE** | THE crux — biggest single gap |
| q9 | 1752.9 | 2240.4 | 1728.0 | 0.78× win | 1.014× ~LOSE | join, marginal |
| q8 | 160.0 | 152.7 | 144.2 | 1.05× LOSE | 1.11× LOSE | windowed-join, small |
| q11 | 342.6 | 439.6 | 307.9 | 0.78× win | 1.11× LOSE | session-window |
| q12 | 161.7 | 155.0 | 134.4 | 1.04× LOSE | 1.20× LOSE | proctime window |
| q18 | 713.2 | 532.3 | 643.7 | 1.34× LOSE | 1.11× LOSE | dedup/OVER |
| q19 | 535.8 | 463.3 | 492.9 | 1.16× LOSE | 1.09× LOSE | OVER-window |
| q5 | 592.9 | 590.8 | 800.6 | 1.004× ~tie | 0.74× win | hopping window |

**The mandate set decomposes into exactly two architectural problems:**
1. **The join/interval-join wall (q7, q9, q20, q4)** — I/O-volume-bound and probe-serialized. q7 is the extreme.
2. **The keyed-window/OVER read-RMW wall (q8, q11, q12, q17, q18, q19)** — per-record state RMW + async-state coordination floor.

q5 already beats ForSt and ties RDB. The incremental levers in the master strategy get these rows to *parity*. The user wants them *crushed*. That requires attacking the STRUCTURE of each wall, below.

---

## 1. Per-class wall decomposition (profile + mini-bench + code evidence)

### 1.A Re-anchored dominant wall — write-amp × read-amp on a saturated disk (THIS cycle's churn_probe)

I re-ran `churn_probe` (q7-interval-join shape, 200K rows/s, ~1 GiB live, single probe thread) on this worktree's tip. **New numbers, this box, this cycle:**

| arm | value B | write-amp | phys MiB | max files | probe p50 late | p50 degradation | vlog MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| default | 200 | **6.57×** | 17559 | 19 | 522 µs | 2.96× | 0 |
| kvsep | 200 | 6.56× | 17548 | 19 | 529 µs | 2.99× | 0 (below 256B gate — NOT separated) |
| **default** | **512** | **7.18×** | **43881** | **69** | **1113 µs** | **5.23×** | 0 |
| **kvsep** | **512** | **1.39×** | **8674** | **6** | **784 µs** | **2.47×** | 2352 |

**Findings (HIGH confidence — measured this cycle):**

1. **At the q7/q9/q20 payload class (512 B), the default LSM is catastrophic:** write-amp **7.18×**, **43.9 GB physical for 6.25 GB logical**, **69 overlapping files**, probe latency degrades **5.23×** (213→1113 µs) as state grows. This is the q7 wall, reproduced in a micro.
2. **KV-sep is not a 10-20% lever — it is a 5.2× structural collapse of the write wall AND a 11.5× collapse of file count (69→6).** write-amp 7.18→1.39, physical 43.9→8.7 GB, files 69→6. The file-count collapse is *why* it also helps the read path (fewer sources to merge per probe).
3. **But KV-sep does NOT close the read wall: kvsep512 probe p50 is still 784 µs and still degrades 2.47×**, and the *early* probe is WORSE (213→318 µs) — the deref tax on the cold scattered case (the coalesce fix addresses the batch case but the single-probe deref still pays). **The read-amp wall survives KV-sep.**

**This is the central architectural fact: forst-rs's two heavy walls are (W) write/rewrite volume and (R) per-probe read-amp, and they are SEPARATE structures.** KV-sep crushes W. Nothing yet crushes R structurally — S2 (loser-tree) shaves the merge CPU within R but, per the q7 100M falsifier, only moved q7 +3.1% because the wall is I/O/source-count, not merge CPU.

### 1.B The join wall (q7/q9/q20/q4) — read-amp is STRUCTURAL, per-probe, and forst-rs-specific

The per-probe open (`build_lazy_prefix_key_stream_sel`, `crates/forst-rs-engine/src/db.rs:10022-10340`) assembles, **on EVERY probe, from scratch**:

- active-memtable prefix cursor (`db.rs:10089-10101`),
- one cursor per immutable memtable (`db.rs:10108-10114`),
- **an O(N) clone of ALL resident-flushed shadow entries under a RwLock read** (`resident_flushed_visible_entries`, `db.rs:10153-10157`), then a per-entry range/bloom/seek loop over them (`db.rs:10179-10242`),
- a per-level walk of EVERY overlapping SST (`overlapping_ssts_in_range_for_cf`, `db.rs:10277-10282`), opened serially in the loop (`db.rs:10314+`).

**Contrast ForSt (C++, `2026-06-12-forst-architecture-q7-analysis.md` §2.4):** a `seek(prefix)` on a *persistent* `MergingIterator` over a **leveled** layout — ≤4-36 L0 iterators + **one `LevelIterator` per level** (`db/version_set.cc:939`), each a cached binary-search index lookup + one block-cache probe; `next()` is a heap pop. No resident-shadow tier, no per-probe O(N) clone, bounded source count by leveled discipline (`advanced_options.h:583,590` L0 stop 36; L1..Ln non-overlapping).

The structural deltas, ranked by the existing evidence + this cycle's confirmation:

1. **Read-amp / sorted-run discipline (top).** forst-rs runs L0 to 40/64 (`write_controller.rs:94-95`) + **size-tiered** L1..Ln (`compaction.rs:21`) → 69 overlapping files in my 512B micro vs RocksDB's leveled bound. Each of q7's ~10⁸ probes pays an O(sources) open. Remote iostat (q7-analysis §1, Q7P32): bench NVMe **98-99% util, writes > reads**, CPU ~18% idle ⇒ **I/O-stall-bound**, io_uring = finish-vs-DNF. This cycle's micro: probe p50 degrades 2.47-5.23× as file count grows — the same shape.
2. **The resident-flushed shadow tier is a forst-rs-specific per-probe O(N) tax** (`db.rs:10153-10242`) with no analogue in ForSt/RocksDB. It has been patched repeatedly (range-prune, bloom-skip, bypass env) — the patches' very existence (`FRS-RESIDENT-BLOOM-SKIP`, `FRS-RESIDENT-BYPASS`, `FRS-B-SPLIT` diags) documents that it is a structural cost center, not a tuned one.
3. **In-flight probe depth = 1.** Default executor runs each batch inline on the mailbox (`ForStRsAsyncKeyedStateBackend.java:1206-1219`); ForSt overlaps ≥3 probes on a read pool (`ForStStateExecutor.java:288-291`). q9 JFR = 65,842 ThreadPark vs 6,419 on-CPU (~10:1) ⇒ **wait/latency-bound, not CPU-bound** (`2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` §1). Every per-probe I/O stall sits on the critical path; ForSt hides its (smaller, leveled) I/O behind 3-way overlap. **H1×H2 compound: bigger stalls AND no overlap.**
4. **C++ constant factor (diffuse).** ForSt-Mac 586.8 vs frs-Mac 1441.6 on q7 = 2.46× even on fast local disk ⇒ a per-op residue survives I/O (the q4 1.39× "no single lever ≥2%" class).

The engine HAS a `bg_read_pool` cross-probe-overlap primitive (`batch_prefix_scan_parallel`, `db.rs:9470-9545`, "ForSt's read-io-parallelism model") — but it is reachable only via the opt-in routing-adaptive executor, NOT the default depth-1 path. **The mechanism for H2 exists and is unused by default.**

### 1.C The keyed-window/OVER wall (q8/q11/q12/q17/q18/q19) — per-record RMW + async-state floor

These are NOT I/O-volume-bound; they are read-path + coordination bound.

- **q17 (group-agg, point-RMW) already BEATS ForSt 3.3×** *because* the depth-1 inline path has zero mailbox→worker→mailbox handoff (`2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` §q17). Its 2.23× vs RDB is RocksDB's synchronous-backend speed on a tight point-RMW loop vs forst-rs's async-state per-batch coordination floor. **This is the structural tension:** the same async-state pipeline that the join wall needs (depth ≥3) is pure overhead on q17's cheap point-RMW. Any default executor change MUST keep q17 on the zero-handoff path.
- **q11 (session window):** depth-1 serial drain tail (measured: routing 318.9→135.7s, **2.35×**, exact 92M) + per-record `MergingWindowSet.forEachEntry` re-walk (`ForStRsMapState.java:725`).
- **q12/q8 (window-agg):** per-record reduce/agg RMW chains pulled to Java (the OPT-N04 target population).
- **q18/q19 (OVER/dedup):** diffuse serde + engine read; q19's findRow O(n²) is FIXED; residual has no single hotspot ≥10%.

**The common structural defect across this class: the accumulator RMW round-trips Java↔engine per record (get accumulator → merge in Java → put accumulator), and the async-state batch coordination adds a fixed per-batch cost RocksDB's synchronous JNI does not pay.** ForSt wins q8/q11/q12 because its synchronous C++ backend has a lower per-op floor.

---

## 2. Batch-execution-violation findings (the hard constraint — each is a win)

I hunted per-record / per-key / per-byte / non-vectorized / copy ops in the hot paths. The classic byte[] copies are already gone (prior lever-1/3 work: zero-copy iterator drain, `mismatch()` compare, reused chunkBuf — `2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` §2). The surviving violations are STRUCTURAL, not copy-in-loop:

- **V-A (join, HIGH severity): the per-probe LSM source set is rebuilt from scratch every probe** (`db.rs:10022-10340`). The active/imm cursors, the O(N) resident-shadow clone, and the overlapping-SST locate are all redone per probe even when consecutive probes hit the same key-group/bucket (q7/q9/q20 probe the SAME bucket's band repeatedly). ForSt's persistent `MergingIterator` + `seek()` amortizes the source-set construction across probes. **This is the single biggest batch-execution violation: O(probes × sources) where it could be O(probes × log + sources-once).**
- **V-B (join, HIGH): the resident-shadow O(N) clone-under-RwLock per probe** (`db.rs:10153-10157`). Even bloom-skipped, the clone of all N entries' metadata happens before the prune. O(N) allocation + lock per probe.
- **V-C (keyed-window, HIGH): accumulator RMW is per-record Java↔engine round-trip, NOT in-engine.** q8/q11/q12/q17 pull the accumulator to Java, merge, and write it back — one dependent get→put chain per record. The engine has merge operators (`NumericAdd[Be]MergeOperator`, RawConcatMergeOperator `lib.rs:162`) but the SQL window-agg path does not route through them (OPT-N04 J1-J5 unbuilt). **The merge is a per-record framework round-trip where it could be an in-engine merge-on-write.**
- **V-D (join, MED): depth-1 — no cross-probe overlap on the default path** (`ForStRsAsyncKeyedStateBackend.java:1206-1219`). The K independent probes in an async-state batch are run serially inline. `batch_prefix_scan_parallel` exists but is unused by default.
- **V-E (join, MED): single-probe scattered deref still pays the cold chunk** (kvsep512 early probe 213→318 µs). The coalesce fix (`FRS_VLOG_COALESCE_DEREF`) helps multi-key batches but the N=1 point-get on a separated value still reads a 64 KiB chunk to return 512 B.

---

## 3. Learn from ForSt (C++) — what it does on the hot path that forst-rs doesn't

| Hot-path behavior | ForSt (C++) | forst-rs | Architectural implication |
|---|---|---|---|
| LSM shape | **Leveled**, L1..Ln non-overlapping, 1 `LevelIterator`/level (`version_set.cc:939`) | L0 + **size-tiered** L1..Ln, O(all overlapping) per probe (`compaction.rs:21`, `db.rs:10277`) | **Bounded vs unbounded per-probe source count.** The read-amp wall's root. |
| Probe iterator | **Persistent** `MergingIterator`, `seek()` reuses it (`merging_iterator.cc:52`, `arena_wrapped_db_iter.h:36`) | **Rebuilt per probe** (`db.rs:10022`) | V-A: source-set construction not amortized. |
| Read parallelism | 3-thread read pool, in-flight ≥3, mailbox never blocks (`ForStStateExecutor.java:288-291`) | depth-1 inline default (pool exists, unused) | V-D: no cross-probe overlap by default. |
| Resident tier | none — block cache + OS page cache as L2 | per-probe O(N) resident-shadow clone (`db.rs:10153`) | V-B: a forst-rs-specific tax. |
| Page cache | buffered POSIX preads → whole box RAM is an automatic L2 SST cache | opendal + own caches + BlockPrefetcher | H3: frs may re-read bytes the kernel would have kept. |
| Per-op constant | synchronous C++ get/iter on reader threads | tokio task-per-op + FFM completion plumbing per batch | H5: diffuse residue (q4 1.39×). |
| Compaction | leveled, overlap-bounded inputs, 2 bg jobs, Snappy per level | per-CF, single output stream, write-amp 7.18× at 512B (this cycle) | W: rewrite volume — already attacked by KV-sep (1.39×). |

**The two things ForSt structurally does that forst-rs does NOT, and that the incremental levers do not change:** (1) a **leveled layout that bounds per-probe source count**, and (2) a **persistent seekable iterator that amortizes source-set construction**. Everything else (merge CPU = S2, write-amp = KV-sep, overlap = executor) forst-rs has an answer for; these two it does not.

---

## 4. THREE ambitious architectural approaches (ranked) — raise the ceiling, don't shave shares

Each: mechanism, queries-helped × magnitude × feasibility, trade-offs, and an **FFI/micro mini-bench plan** (NOT NexMark) to confirm BEFORE any build. The unifying thesis: **the incremental levers (S2/L4/N04) each take a fraction of one measured share and compose to ~parity. The three approaches below each change the STRUCTURE of a wall — they are super-linear, not additive.**

### APPROACH 1 (RECOMMENDED, primary) — Leveled-on-the-hot-CFs + persistent seekable probe iterator: kill the read-amp wall at the root

**The architectural move.** Replace the per-probe rebuilt source set with ForSt's two structural properties, on the join/interval-join CFs:

- **(1a) Leveled L1..Ln on hot-probe CFs** (bound per-probe source count). The size-tiered layout is the read-amp root: 69 files at 512B this cycle. A leveled L1..Ln (non-overlapping runs, one iterator/level) bounds the count to ≈#levels + L0. The compaction picking is *already a near-RocksDB leveled scheme* under `FRS_DYNAMIC_LEVELS` (min-overlap-ratio, compensated sizes, `db.rs:14016-14047`) — but the LAYOUT it produces is still size-tiered/overlapping at the bottom. The move is to make Ln non-overlapping (true leveled) so the locator returns ≤1 file/level. **This composes with KV-sep:** under KV-sep the LSM carries 36 B pointers, so leveled rewrites are cheap (the write-amp objection to leveled evaporates — measured: kvsep512 already at 6 files / 1.39× write-amp).
- **(1b) Persistent per-(CF, key-group) seekable iterator** (amortize source-set construction across probes). Instead of `build_lazy_prefix_key_stream` per probe, hold a `MergingIterator`-equivalent per key-group that `seek(prefix)`s and is rebuilt only when the version changes (flush/compaction). q7/q9/q20 probe the same bucket repeatedly within a key-group — this turns O(probes × sources) into O(probes × log + sources-per-version). Kills V-A and V-B.

**Queries helped × magnitude × feasibility.**
- q7 (the crux): read-amp is the #1 hypothesis (q7-analysis weight 35%) + I/O-stall confirmed. Bounding sources 69→~6 and amortizing construction is the structural counter to a 1.28× gap. **Projected: the largest single mover; target q7 ≤ ForSt's 1336.6** (remote-confirm required). Feasibility MED — leveled bottom-level layout is a real compaction change but the picking machinery is 90% there.
- q9, q20: same probe structure, partly wait-bound (also wants Approach 3). Magnitude MED-HIGH.
- q4: write-heavy join; leveled-under-KV-sep keeps write-amp low while bounding sources. MED.
- q11/q19 (OVER, deep scans): partial — fewer sources per scan. LOW-MED.
- q8/q12/q17: neutral (small state, few sources) — must not regress.

**Trade-offs.** (+) Attacks the read-amp ROOT, not its CPU share (where S2 stalled at +3.1%). (+) Composes with KV-sep so write-amp stays ~1.4×. (+) Eliminates the resident-shadow tier entirely (the persistent iterator over leveled SSTs + a small block cache replaces it — exactly ForSt's model). (−) Leveled compaction on hot CFs is more compaction work than tiered IF values rode the LSM — but under KV-sep they don't, so this is cheap. (−) The persistent iterator must invalidate on version change (flush/compaction) — a real correctness surface (MVCC snapshot + version pin); needs the read-while-compact property test ×5. (−) Per-key-group iterator residency is bounded memory (one MergingIter/active key-group) — must be LRU-capped.

**Mini-bench plan (FFI/micro, NOT NexMark) — confirm BEFORE build:**
1. Extend `join_probe_open` (`crates/forst-rs-bench/benches/join_probe_open.rs`) with a **leveled vs tiered** layout arm at ssts_{8,32,69} (the 512B file counts I measured): assert the leveled arm's per-probe open is O(#levels) not O(#files), and the absolute ns/probe at fan-out-69 drops toward the fan-out-6 number. PASS = leveled-69 ≈ tiered-6.
2. New micro `persistent_probe_iter`: same bucket probed K times; measure ns/probe for (a) rebuild-per-probe (today) vs (b) persistent seek-reuse. PASS = (b) amortizes construction (target ≥3× on K≥100 same-bucket probes — the q7 within-key-group pattern).
3. `churn_probe` at 512B with a leveled-layout arm: confirm write-amp stays ≤1.5× (KV-sep preserved) AND max-files drops to ≈#levels, AND probe p50 degradation drops below the 2.47× tiered-KV-sep floor. This is the decisive micro — it shows the read wall flattening without re-inflating the write wall.

### APPROACH 2 (RECOMMENDED, parallel lane) — In-engine windowed-agg merge: make the keyed-window RMW never leave the engine

**The architectural move.** Route the SQL window-agg / OVER accumulator RMW through **in-engine merge operators** so the accumulator is merged on write (memtable merge) and collapsed at flush/compaction, instead of the per-record Java↔engine get→merge→put round-trip (V-C). The engine merge operators exist (`NumericAdd[Be]`, `RawConcat`, `lib.rs:162`); the missing piece is the Flink-side ReducingState/AggregatingState routing (OPT-N04 J1-J5) PLUS a genuinely in-engine partial-merge so reads don't walk an unbounded operand chain.

**Queries helped × magnitude × feasibility.**
- q12 (proctime window, reduce): −8..15% modeled; loses ForSt 1.20× today — the canary. MED-HIGH.
- q8 (windowed join agg): −5..10%; loses both, small gap. MED.
- q11 (session window): the accumulator side (secondary to its drain-tail; pairs with Approach 3). LOW-MED.
- q17 (group-agg): already beats ForSt — must stay on the fast inline path; in-engine merge could *further* cut its Java round-trip but the risk is regressing the zero-handoff path. Treat as upside-only, gated.
- q5 (hopping window): already wins ForSt; in-engine merge keeps the margin. Neutral-positive.

**Trade-offs.** (+) Removes a per-record dependent round-trip = the structural floor ForSt beats us on for q8/q11/q12. (+) Merge-on-write + flush-collapse is exactly what makes the agg state cheap to read. (−) Read-path merge-chain length between flushes can WORSEN probes if partial-merge doesn't fire (the documented 5M-no-flush artifact); needs the q20 read-cost guard. (−) Restore/checkpoint with pending Merge ops needs the round-trip test. (−) Only i64/decomposable accumulators are mergeable in-engine; non-decomposable aggs stay on the Java path (adaptive by accumulator type, not per-query config).

**Mini-bench plan (FFI/micro, NOT NexMark):**
1. New micro `accumulator_rmw`: simulate q12-shape reduce — K records, same key, accumulator. Arm A = today's get→merge-in-Java→put per record (via the FFI surface). Arm B = `frs_vec_merge_append` (in-engine merge) + one read. Measure ns/record and FFM crossings/record. PASS = B has 1 crossing/record vs A's 2, and ns/record drops ≥30% (the dependent-chain elimination).
2. Read-cost guard micro: after K in-engine merges, measure the read cost of the accumulator with vs without a flush-collapse (partial-merge). PASS = post-flush read is O(1), not O(K) — confirms partial-merge bounds the operand chain (the q20-regression falsifier, before any NexMark).
3. Restore round-trip UT: checkpoint with pending Merge ops, restore, assert byte-identical accumulator.

### APPROACH 3 (RECOMMENDED, enabler for 1 & the join wait-bound rows) — Coordination-free default executor with a content-adaptive fast path

**The architectural move.** Make the **default** executor non-blocking + cross-probe-overlapping for iter-bearing batches (depth ≥3, like ForSt's read pool, riding `batch_prefix_scan_parallel`/`bg_read_pool`) while keeping iter-free point-RMW batches on the **zero-handoff inline path** (the q17-protecting carve-out). This is the R2a/R2b routing-adaptive work made the *default* once the q8 correctness gate clears — but framed architecturally: forst-rs's wait-bound join rows (q9 10:1 park ratio) need overlap, and the only reason they don't have it by default is the unresolved q8 race + the q17 regression risk.

**Queries helped × magnitude × feasibility.**
- q11: 2.35× recorded (318.9→135.7s) — the proven mover. Flips q11 to beat ForSt. HIGH magnitude, but correctness-gated.
- q7/q9/q20: cross-probe overlap hides per-probe I/O latency (the H2 lever, weight ~30% on q7). Composes with Approach 1 (Approach 1 shrinks the stall; Approach 3 overlaps what remains). MED — and the composition note matters: shrinking service time shrinks what overlap hides, so take the measured −4..6% on the heavies, not an idle-core extrapolation.
- q17: defended (must stay 76.7s zero-handoff). Defensive.

**Trade-offs.** (+) Unlocks the engine's existing parallel primitive for the default path. (+) The single highest-confidence recorded win in the whole catalog (q11 2.35×). (−) The q8 nondeterministic under-emit under inline-vs-worker interleaving is UNRESOLVED — this is the gate the whole executor line has repeatedly failed; **do not default-flip without a deterministic q8 repro** (controlled sorted-CSV replay, p=1, byte-compare). (−) q17 regression risk (271s under naive full routing) — the iter-free zero-handoff carve-out is mandatory.

**Mini-bench plan (Java micro + deterministic correctness repro, NOT NexMark):**
1. Deterministic q8 repro (the make-or-break): fixed-CSV replay, p=1, byte-compare across inline-vs-worker iter-free execution to bisect the thread-identity-sensitive primitive. This is a correctness micro, not perf — it is the gate.
2. q17 carve-out trace assertion (`VectorizedExecutorIterBatchRoutingTest` extended): an iter-free single-kg batch incurs ZERO worker dispatch + ZERO classifier alloc (byte-identical dispatch trace to depth-1).
3. Cross-probe overlap micro (`batch_prefix_scan_parallel` at k=3,8,32): confirm wall-time overlap vs serial inline scales with k up to pool width (already partially in `ffi_vectorized iter_open_batch`).

---

## 5. Composition + the recommendation

The three approaches act on **disjoint structures**: Approach 1 = per-probe source count + construction (read-amp), Approach 2 = per-record accumulator round-trip (RMW), Approach 3 = cross-probe overlap (wait). They compose multiplicatively on the join rows and cover the window rows separately:

- **Join wall (q7/q9/q20/q4):** Approach 1 (bound + amortize sources) × Approach 3 (overlap the residual stall), on top of the shipped KV-sep (write wall already crushed to 1.39×). This is the structural attack q7 needs — S2 alone moved it +3.1% because it took only the merge-CPU share; bounding the source COUNT and amortizing construction takes the I/O-volume + per-probe-rebuild structure.
- **Keyed-window/OVER wall (q8/q11/q12/q17/q18/q19):** Approach 2 (in-engine merge, kills the per-record round-trip) + Approach 3 (q11 drain overlap), with q17 defended by the carve-out.

**RECOMMENDATION — sequence:**
1. **Approach 1 first** (highest ceiling, attacks the crux q7's #1 wall, composes with the already-shipped KV-sep). Its mini-bench (§4.1 leveled vs tiered `join_probe_open` + `persistent_probe_iter` + `churn_probe` leveled arm) is the single most decisive pre-build gate — if leveled-69 ≈ tiered-6 and write-amp stays ≤1.5×, the read wall is structurally beatable.
2. **Approach 3 in parallel** (the deterministic q8 repro is the long pole; start it now — it gates the proven 2.35× q11 win and the join-overlap composition).
3. **Approach 2** (in-engine merge) for the q8/q12 window rows — independent lane, canary-gated.

**Why this crushes both, not ties them:** KV-sep (shipped) already beats both on write-amp (1.39× vs RDB 3.91×, ForSt 1.4KB/event). Approach 1 gives forst-rs ForSt's *read* discipline (bounded leveled sources + persistent seek) WITHOUT ForSt's write-amp (because KV-sep keeps the LSM tiny) — i.e. forst-rs gets the best of both layouts, which neither backend has. Approach 2 removes the per-record RMW floor that is the only reason ForSt's C++ backend wins q8/q12. Approach 3 adds the overlap ForSt has and forst-rs's engine already supports. The combination is not "match ForSt" — it is "leveled-read-discipline + KV-sep-write-discipline + in-engine-merge + overlap," a layout no current backend runs.

---

## 6. Evidence ledger + what needs a deeper profile

**Measured this cycle (HIGH):** §1.A churn_probe write-amp/file-count/probe-degradation at 200B and 512B, default vs KV-sep (4 runs, this worktree tip). The 512B numbers (write-amp 7.18→1.39×, files 69→6, probe p50 1113→784µs) are the spine of the whole argument.

**Code-grounded (HIGH):** §1.B/§2/§3 per-probe rebuild, resident-shadow O(N) clone, leveled-vs-tiered, persistent-vs-rebuilt iterator, depth-1 default, merge-operator existence — all cited to `db.rs`/`compaction.rs`/`write_controller.rs`/ForSt sources in the q7-analysis.

**Recorded-prior (cited, not re-run):** q11 2.35× executor win, q9 10:1 park ratio, q7 S2 +3.1% falsifier, ForSt topology, the 100M sweep table.

**NEEDS A DEEPER PROFILE before build (flagged honestly):**
- **A1 (Approach 1 gate):** the leveled-vs-tiered `join_probe_open` arm + `persistent_probe_iter` micro do not yet exist — they are the §4.1 pre-build mini-bench. Until run, "leveled-69 ≈ tiered-6" is a *model*, grounded in the file-count collapse I measured + ForSt's leveled bound, not a forst-rs measurement.
- **A2:** the remote q7 100M hot-path perf flat report (CPU-idle+disk-saturated ⇒ read-amp; CPU-in-runtime ⇒ overlap/constant) ranks Approach 1 vs 3 weight — q7-analysis §1 has partial iostat (99% util, writes>reads) pointing to read-amp+write-amp, but the perf-symbolized share is owed.
- **A3:** the persistent-iterator version-invalidation cost (rebuild-on-flush frequency × source-set size) — a micro is needed to confirm amortization survives q7's flush cadence.
- **A4 (Approach 2):** `accumulator_rmw` micro + the read-cost guard (operand-chain bound post-flush) — neither exists yet; both are pre-build gates in §4.2.
- **A5 (Approach 3):** the deterministic q8 repro is unresolved and is the hard gate; it is a correctness profile, not a perf one.
- **A6:** every wall-clock projection is Mac-population; the binding verdict is the remote x86/NVMe box (same-session pairs, n≥3). No second is claimed here as a remote number.

**Anti-wandering compliance:** every approach is instrument-before-code (the mini-bench precedes the build), each is structurally falsifiable (the leveled micro, the chain-bound guard, the q8 repro), and none re-opens a falsified lever — Approach 1 is NEW (leveled layout + persistent iterator, distinct from S2's merge-CPU lever that the q7 +3.1% falsifier closed); Approach 2 is OPT-N04 reframed as the structural RMW-floor attack with its read-cost guard; Approach 3 is the executor line gated on the SAME unresolved q8 repro the catalog already names.

# q7 / q11 / q17 — root cause + UNIFORM-CONFIG dynamic-adaptive repair design

**Date:** 2026-06-14
**Author:** PMC-1 architecture arm (review + design cycle; NO builds/benches run — perf-sensitive NexMark in progress on this Mac)
**Base:** `origin/forst-rs` @ `15436bfde` (worktree hard-reset to it)
**Engine:** `/Users/lijunqing/Code/stczwd/ForSt` · **Flink backend:** `/Users/lijunqing/Code/stczwd/flink/flink-state-backends/flink-statebackend-forst-rs`

## HARD CONSTRAINT (user directive 2026-06-14)
ALL queries run under the **SAME uniform config**. Per-query config is FORBIDDEN. The only
permitted way to help one query without hurting another is **dynamic / adaptive engine behavior
under one config** — the engine senses access pattern / value size / fan-out / pipeline state at
runtime and switches mechanism, bounded so it never regresses or OOMs another query. Every repair
below is: **one uniform config + runtime-adaptive + flag-gated default-OFF + byte-identical when
OFF**, with an FFI/read micro-bench gate BEFORE any NexMark.

## The data this builds on (clean V3 flag-ON, `2026-06-08-8c32g-3backend-sweep-results.md`)
- **q11**: frs 215.8 vs rdb 103.8 (**2.08× FAIL RDB**) vs ForSt 128.8 (FAIL). Read-bound.
- **q17**: frs 150.7 vs rdb 67.6 (**2.23× FAIL RDB**) vs ForSt 252.2 (beats ForSt). Read-bound.
- **q7** : frs 695.5 vs rdb 599.1 (**1.16× PASS RDB**) vs ForSt 477.8 (**1.46× FAIL ForSt**).
  KV-sep helped it (941.8→695.5) but it still loses ForSt's C++ engine.
- A clean FLAG-OFF pass was running concurrently; not committed to the sweep doc at the time of
  writing — root causes below are reasoned from the flag-ON data + code, and each adaptive lever is
  byte-identical when OFF so the flag-OFF baseline is its A-arm by construction.

## Query shapes (grounds every root cause — verified from the SQL)
- **q11** (`queries/q11.sql`): `GROUP BY bidder, SESSION(dateTime, 10s)` — **session-window group-agg.**
  Per record it merges/splits session windows → Flink `MergingWindowSet`.
- **q17** (`queries/q17.sql`): `GROUP BY auction, DATE_FORMAT(dateTime,'day')` with count/min/max/avg/sum
  + filtered counts — **unbounded keyed group-agg**, a high-rate point-get + RMW of one accumulator
  row per (auction, day). **No iteration in the hot path.**
- **q7** (`queries/q7.sql`): `TUMBLE(10s) MAX(price)` ⋈ `bid` on `price = maxprice` with a
  `BETWEEN … - 10s AND …` band — **tumbling-window agg feeding an interval band-join**: millions of
  short prefix probes over a growing, deep-fan-out SST set; KV-sep-sized payloads.

---

## 1. ROOT-CAUSE MODEL (architecture-first, file:line grounded)

### Cross-cutting fact that frames all three (JFR, sweep doc :330-382, :494-547)
The heavy read queries are **WAIT-bound, not engine-CPU-bound**: q9 JFR = 65,842 ThreadPark vs
6,419 on-CPU (~10:1), the dominant park in `TaskMailboxImpl.take()` ← async-state in-flight buffer
full. The forst-rs DEFAULT executor is **depth-1 inline**: `executeBatchRequests` runs the batch
synchronously on the mailbox thread and returns an already-completed future
(`VectorizedExecutor`, documented at `ForStRsAsyncKeyedStateBackend.java:1282-1310` and
`RoutingStateExecutor.java:55-58`). ForSt instead returns an incomplete future + offloads to a
3-thread read pool (in-flight depth ≥ 3; `ForStStateExecutor.java:288-291`). So a large part of the
gap on the ITER-bearing queries is **no cross-probe overlap**, not per-op cost. This is the single
most important architectural fact: the same mechanism helps q11 and q7 and must be kept OFF the
path of q17.

### q11 — root cause = (A) depth-1 drain tail on iter batches + (B) per-record O(N) session-map walk
**A — drain-tail / depth-1 (PRIMARY, confirmed by measurement).** q11's session windows iterate
the per-bidder session MapState. Under depth-1 the in-flight backlog is drained **serially after
the source stops** (sweep doc :528-535: depth-1 318.9s with a ~178s post-source serial tail;
routing/offload drains concurrently → **135.7-135.8s, exact 92M**, i.e. 2.35×, flipping
0.35×→0.82× RDB). The lever IS executor depth for iter batches; this is *measured*, not modeled.

**B — per-record session-map re-drain (SECONDARY, code-confirmed, executor-independent).** The
windowed group-agg runs on the **V1 sync** path via `ForStRsInternalKvStateAdapters`, and
`MergingWindowSet.initializeCache` calls `ForStRsMapState.forEachEntry`
(`state/ForStRsMapState.java:725`, called from `:582/:591/:598`) **per element** — a full
session-window map iteration **per record**, O(N)/record over the live session set (sweep doc
:1040-1044, the "BONUS FINDING"). RocksDB pays the same Flink structure but its synchronous JNI
`value()/iterator()` has no async-pipeline wait and no MapStateCache layer, so the per-record walk
is cheaper there. This is a real q11-specific lever **independent of the executor**.

→ **q11 ≈ 2× because (A) the iter-batch in-flight tail is serialized at depth-1 AND (B) each record
re-walks the live session map.** Not a byte[]/copy issue (the iterator path is already zero-copy
per the lever-1/3 work, sweep :446-467); the cost is *wait* + *redundant per-record iteration*.

### q17 — root cause = executor-handoff tax on a high-rate iter-FREE point-get/RMW query
q17 is `count/min/max/avg/sum` per (auction, day): one accumulator row per key, **point get + put,
NO iteration**, at ~1.2M rec/s. Here forst-rs already **BEATS ForSt 3.3×** (frs 76.7s vs ForSt
255.7s, sweep :196) — *because* the depth-1 inline path has zero mailbox→worker→mailbox handoff.
Every parallel executor mode REGRESSES q17 precisely by adding that handoff:
- coordinated N=3 → **271.8s** ≈ ForSt's own 253s (sweep :996, :1001-1006);
- full-split inline → 148.8s; defer-1-classifier → 117.7s; latch-offload → 271.8s — the per-batch
  key-group **splitting** of cheap batches costs ~35-70s on q17 even when executed inline
  (sweep :1058-1062, :1081-1083).

So q17 is NOT 2× slower than RocksDB for a forst-rs read-path defect — RDB's synchronous backend is
simply very fast on a tight point-RMW loop, and forst-rs's async-state pipeline adds a fixed
per-batch coordination cost that RDB doesn't have. The 2.23× is the async-state framework floor on
a cheap-batch query, **the same framework whose depth helps q11/q7**. The design imperative for q17
is therefore *defensive*: whatever dynamic mechanism we ship for q11/q7 must **leave q17 on the
exact depth-1, no-split, no-handoff path** (byte-identical to today's 76.7s). No new q17 lever is
warranted; the work is to not break it.

(Unconfirmed sub-claim — **needs profiling run**: whether any residual q17 gap is per-record
MapStateCache `findRow/hashOf` overhead, sweep :113-117 named it ~2400 samples on q19. A q17 JFR
would size it; but since q17 already beats ForSt, this is low priority.)

### q7 — root cause = deep-fan-out read-amp (I/O) × depth-1 probe (no overlap) × C++ constant factor
Ranked, from `2026-06-12-forst-architecture-q7-analysis.md` §3 + the S2 cycle-3 micro-data:
1. **H1 — sorted-run / read-amp under churn (top).** ForSt is leveled (L0 ≤ 4-36, L1..Ln
   non-overlapping → one `LevelIterator`/level, `version_set.cc:939`); forst-rs runs L0 to 40/64
   (`write_controller.rs:95-97`) + size-tiered L1..Ln, so the per-probe open enumerates **every
   overlapping SST** (`db.rs:7034 overlapping_ssts_in_range_for_cf`). q7's ~10⁸ short prefix probes
   each pay a fan-out multiple. Remote iostat (q7-analysis §1, marker Q7P32): bench NVMe at **98-99%
   util, writes > reads**, CPU ~18% idle → **q7 is disk/I/O-stall-bound**, and io_uring is
   finish-vs-DNF. This is read-amp + compaction write-amp on one disk.
2. **H2 — in-flight probe depth = 1.** Same depth-1 default as q11; ForSt overlaps ≥3 probes and
   never blocks the mailbox, *hiding* its (smaller) per-probe I/O behind parallelism while forst-rs
   *serializes* its (larger) per-probe I/O. H1×H2 compound (sweep/analysis §0).
3. **H5 — C++ engine constant factor.** ForSt-Mac 586.8 vs frs-Mac 1441.6 = 2.46× even on a fast
   local disk → a diffuse per-op residue survives (the q4 1.39× "no single lever ≥2%" class).

**What ForSt does on the probe path that forst-rs doesn't:** (a) leveled non-overlapping layout →
fewer sources per probe; (b) 3-thread read pool → cross-probe overlap; (c) coalesced multiGet
(≤32 keys, one bloom + block fetch per SST, `multiget_context.h:103`) — though q7's path is
ITERATOR (prefix probe), not multiGet, so (a)+(b) dominate. **S2 (pinned-block replenish +
loser-tree merge)** is the forst-rs answer to the per-probe merge cost: cycle-3 micro
(`2026-06-13-cycle3-readpath-combined-gate-results.md` §2-3) shows the q20-shape deep probe
`join_probe_open/ssts_128` **7.54µs → 0.90µs (8.4×)** with S2 ON, and **with KV-sep ACTIVE
(q7's payload class) the legacy path EXPLODES to 18.09µs and S2 brings it to 1.00µs (~18×)** — S2 is
what *prevents KV-sep from regressing the deep-fan-out read path*. q7 uses KV-sep. So S2 directly
attacks q7's per-probe cost; H1/H2 attack its I/O-stall and overlap.

→ **q7 loses ForSt by 1.46× because its per-probe sources are deeper (read-amp), serialized
(depth-1), and merged on the legacy O(sources)-twice path that KV-sep makes worse — while ForSt is
leveled + 3-way overlapped + C++.**

---

## 2. Hard-constraint suspects checked (byte[] / per-record / non-batch in the hot read path)

V3 forbids byte[]/copy and per-record/non-batch execution. Audit of these three queries' hot read
paths:

- **byte[] / memory-copy:** NOT the root cause. The iterator path is already zero-copy: wide-stride
  `keyHash`, `MemorySegment.mismatch()` compare, `MemorySegment.copy()` intrinsic, per-executor
  reused `chunkBuf`, and zero-snapshot in-place decode to detached on-heap RowData (sweep :417-467;
  the one materialization Flink *requires*). The engine scan emits via a pinned/`fill_into` sink
  with **0 allocs/row** (cycle-3 §1, iter_drain ~135 ns/row). No per-key byte[][] survives.
- **per-record / non-batch execution:** TWO real instances, both already named here, neither a
  copy-in-loop:
  1. **q11 `MergingWindowSet → forEachEntry` per record** (`ForStRsMapState.java:725`) — a per-record
     *iteration*, not a per-record FFM crossing (the crossing is chunk-batched). It is redundant
     re-drain, addressed by repair §3-q11-B.
  2. **The async-state per-batch coordination** is itself batched (256-request AEC batches); the
     "per-record" wait is the mailbox park, addressed by executor depth (§3). q17 deliberately keeps
     the cheapest per-batch path (no split, inline).
- **Conclusion:** the constraint violations are NOT classic byte[] copies — they are (i) a redundant
  per-record session-map walk (q11) and (ii) the absence of cross-batch overlap (q11/q7). Both are
  the repair targets below; q17's cheap path must be preserved untouched.

---

## 3. DYNAMIC REPAIR DESIGN — one uniform config, runtime-adaptive, flag-gated default-OFF

The infrastructure for the dynamic mechanism **already exists** and substantially supersedes the
"OPT-01 reverted" narrative in the sweep doc. Current code (`RoutingStateExecutor.java`,
`ForStRsAsyncKeyedStateBackend.java:1311-1381`):
- Executor mode is one env switch `FRS_RS_EXECUTOR` (uniform; default `inline` = today's depth-1).
- Routing is now **key-group-affine** (`RoutingRequestContainer.offer` routes by `keyGroup % N`,
  `RoutingStateExecutor.java:615-623`) → a key-group's MapStateCache always lands on ONE worker
  (read-your-writes + per-key order = the windowed-join correctness fix). `routing` mode is
  **multi-run-verified correct** (q8 = 3 independent exact passes 3,064,457/514/485, sweep :1093-1094)
  and gives the q11 2.35× / q7 1441→1052s wins.
- `adaptive` mode (`:1320-1328`) already implements per-batch dispatch: **iter-free batches inline,
  iter batches → workers** — exactly the q17-stays-fast / q11-q7-fan-out shape. BUT adaptive is
  currently **FLAKY** (q8 nondeterministic under-emit, mechanism still unidentified — sweep
  :1085-1143; "the racy ingredient = INLINE execution of iter-free batches interleaved in time with
  latch-dispatched iter batches"). So adaptive is NOT yet safe to ship.

Given that, the dynamic repairs are split into three independently-shippable levers, ordered by
confidence.

### Repair R1 (q7, q11-A) — ADAPTIVE READ-PATH MERGE: per-scan fan-out-gated S2 selection
**Uniform config, runtime-adaptive on access pattern (fan-out depth).** Today S2 is a *static
process flag* (`db.rs:9588,9651,10278` all gate on `s2_pinned_enabled()`, read once from env at
process start — `db.rs:14961-14966`). It is applied to EVERY scan, which is why a global default-ON
would regress the shallow/churn classes (cycle-3 §1: ssts_1 +18%, `hot_prefix_churn` +20%,
`range_scan_multilevel` +17%) even as it gives +8.4-18× on deep probes.

**Dynamic mechanism:** make the pinned/loser-tree choice **per scbn-open**, keyed on the
fan-out the open already computes. At `build_lazy_prefix_key_stream` / range equivalents the engine
already enumerates the overlapping-SST set (`overlapping_ssts_in_range_for_cf`, `db.rs:7034`) and
has the `n_overlap` / `n_overlap_l0` diags (`db.rs:373-399`). Choose:
`use_pinned = s2_global_override.unwrap_or(n_overlap >= FRS_S2_FANOUT_MIN)` with a uniform default
threshold (e.g. 8 — the cycle-3 ssts_64→128 crossover where S2 flips from tax to win). Shallow
probes (q3/q4/q17 point class, n_overlap small) keep the legacy path **byte-identical**; deep probes
(q7/q9/q20 interval-join, n_overlap large) get the loser-tree. This is uniform-config (one
threshold, same for all queries) + runtime-adaptive (per scan, by measured fan-out) + bounded
(falls back to legacy below threshold, so it can never add the shallow/churn tax).
- **Flag gating:** keep `FRS_RS_S2_PINNED` semantics — `unset` = adaptive-by-fan-out (the new
  default behavior, but threshold so high it's effectively OFF until the gate passes; ship as
  `FRS_S2_FANOUT_MIN` default = `u32::MAX` = OFF, byte-identical to today); `=1` forces ON for all;
  `=0` forces OFF. Default-OFF + byte-identical preserved.
- **Correctness:** the byte-equality of S2-vs-legacy is already proven, including KV-sep + 2KiB
  vlog-resident values, by `s2_kvsep_pinned_byte_equality_combined` (cycle-3 §5). The adaptive
  switch only chooses *which proven-equal path* runs per scan, so correctness is inherited; add one
  UT asserting the per-scan selector yields byte-identical output for a fixture spanning the
  threshold (some probes shallow, some deep).
- **Mini-bench gate (FFI/read micro, NOT NexMark):** extend the cycle-3 criterion matrix
  (`join_probe_open/ssts_{1,8,32,64,128}` + `hot_prefix_churn` + `range_scan_multilevel`) with the
  adaptive selector ON. PASS = deep cells (ssts_128) match `wa_s2` (~0.9µs) AND shallow/churn cells
  match `default` (within noise, i.e. the +18%/+20% tax is GONE because those scans took the legacy
  branch). This is the whole point — one config, no shallow tax, full deep win. Only then a remote
  q7/q9/q20 @100M A/B.
- **Local vs disagg:** **helps BOTH.** Read-amp/per-probe merge is heavier on remote/S3 (every
  avoided SST open is an avoided object fetch), so S2's deep-probe win is *larger* in disagg
  (cycle-3/disagg-competitive: S2 is a named disagg lever, `2026-06-13-disagg-competitive-analysis.md:242`).

### Repair R2 (q11, q7) — ADAPTIVE EXECUTOR DEPTH: iter-bearing batches overlap; iter-free stay inline
**Uniform config, runtime-adaptive on batch content (has-iterators).** This is the `adaptive`
executor idea, but the SAFE realization that does NOT trip the q8 race. Two options, in order of
safety:

- **R2a (SAFE, ship first) — `routing` made the dynamic default via a content gate that never
  inlines.** Keep ALL batches on the kg-affine worker path (the multi-run-correct mode), but make
  the executor adaptive on **how many workers a batch fans to**: iter-free batches that touch one
  key-group run on a single worker with NO latch (already the `busy.size()==1` fast path,
  `RoutingStateExecutor.java:413-416`); iter batches fan to the worker pool. The q8-flaky ingredient
  is *mailbox-inline execution of iter-free batches interleaved with worker iter batches* (sweep
  :1110-1117) — R2a **never inlines on the mailbox**, so that ingredient is absent by construction,
  while still giving the iter-batch overlap that won q11 2.35×. The cost vs depth-1 for q17 is the
  worker handoff (q17 regressed to ~271s under full routing) — so R2a alone is NOT q17-safe and must
  be paired with the q17 carve-out below.
- **R2b (the real adaptive, GATED on a deterministic q8 repro) — fan-out only iter batches; keep
  iter-free batches inline + UN-split.** This is the existing `adaptive` mode's intent
  (`RoutingStateExecutor.java:322-339,393-409`): iter-free batch → execute INLINE on the mailbox in
  ONE shared classifier (no kg split → q17 byte-identical to depth-1's 77s); iter batch → kg-split +
  worker fan-out (q11/q7 win). **Blocker:** the q8 nondeterministic under-emit (5/6 full-scale runs
  wrong) whose mechanism is still open — re-entrancy REFUTED, callback-inlining REFUTED (sweep
  :1119-1143). **Prerequisite before this can be the default: a DETERMINISTIC q8 repro** (controlled
  replay, sorted CSV, p=1, byte-compare — the `/tmp/corr-q5` pattern) to bisect inline-vs-worker
  iter-free execution and find the thread-identity-sensitive state primitive. **needs profiling
  run** (deterministic correctness repro, not perf).

**Recommended sequencing:** ship R2a (safe, q11 win, no q8 risk) gated behind
`FRS_RS_EXECUTOR=routing` staying opt-in; pursue R2b only after the deterministic q8 repro clears
the inline fast-path. The q17 carve-out (below) is what lets *either* become a uniform default.

- **q17 carve-out (defensive, the uniform-default enabler):** whichever executor becomes default
  must route q17-class batches (iter-free, point-RMW) to the **zero-handoff, un-split** path so q17
  stays 76.7s. R2b does this by content (iter-free → inline). For R2a, add a one-worker fast-path
  that skips the kg-split classifier allocation when the batch has no iters AND touches one
  key-group. **Mini-bench gate:** a Java microbench (or the existing
  `VectorizedExecutorIterBatchRoutingTest` extended) asserting an iter-free single-kg batch incurs
  ZERO worker dispatch + ZERO extra classifier alloc (byte-identical dispatch trace to depth-1).
- **Flag gating:** `FRS_RS_EXECUTOR` already the single uniform switch; default stays `inline`
  until BOTH the deterministic q8 repro passes AND the q17 carve-out microbench shows no handoff.
  Default-OFF + byte-identical (inline) preserved.
- **Correctness gate (make-or-break):** q8 windowed-join exact band (3,064,4xx) across ≥3 full-scale
  runs under the chosen mode — this is the gate the whole adaptive-executor line has repeatedly
  failed; do not flip the default without it.
- **Local vs disagg:** **helps BOTH** (more, in disagg). Depth hides per-probe latency; remote/S3
  per-probe latency is far higher, so cross-probe overlap matters more there (the q7-analysis H2
  weight rises on the remote NVMe box).

### Repair R3 (q11-B) — per-record session-map walk: amortize `forEachEntry` re-drain
**Uniform, adaptive on access recurrence (cache the session-map per key).** `MergingWindowSet`
re-initializes its cache via `forEachEntry` per record (`ForStRsMapState.java:725`). The dynamic
mechanism: keep the V1-sync session MapState's last-drained snapshot **per active key** and serve
`initializeCache` from it when the key is unchanged since the last record (invalidate on
put/remove/clear for that key). This is engine/Java-side, uniform (same for all queries; only
session-window group-agg ever calls this path), runtime-adaptive (cache hit only when the same key
recurs back-to-back, the q11 pattern), bounded (per-key, LRU-capped like the existing MapStateCache).
- **Flag gating:** `FRS_RS_WINSET_CACHE` default-OFF; OFF = today's per-record `forEachEntry`,
  byte-identical.
- **Mini-bench gate (read micro, NOT NexMark):** a Java microbench over a synthetic
  `MergingWindowSet`/`forEachEntry` loop measuring ns/record with cache ON vs OFF for the
  same-key-recurs pattern; correctness via a UT asserting identical merged-window sets ON vs OFF.
- **Local vs disagg:** **mostly local** (it removes redundant *iteration*, a CPU/per-record cost);
  in disagg it also removes redundant SST/object reads when the session map is not memtable-resident,
  so a secondary disagg benefit. Lower confidence/leverage than R1/R2 — it is the SECONDARY q11
  cause; ship after R2a confirms the bulk of q11's gap is the drain tail.

---

## 4. Local-only vs also-helps-disagg/S3 (summary)

| Repair | Query | Mechanism | Local | Disagg/S3 |
|---|---|---|---|---|
| R1 adaptive S2 (fan-out-gated loser-tree) | q7 (+q9/q20) | per-scan, by `n_overlap` | YES | **YES, larger** (each avoided SST open = avoided object fetch; named disagg lever) |
| R2a/R2b adaptive executor depth | q11, q7 | per-batch, by has-iters | YES | **YES, larger** (overlap hides higher remote per-probe latency) |
| q17 carve-out (zero-handoff iter-free) | q17 | per-batch, defensive | YES | YES (keeps q17 fast everywhere) |
| R3 winset cache | q11 (secondary) | per-key recurrence | YES (CPU) | minor (fewer reads if not resident) |

---

## 5. Per-query headline + which need a confirming profiling run

- **q11 (2.08× FAIL RDB):** ~2× = (A) depth-1 serial drain tail on iter batches [MEASURED: routing
  318.9→135.7s, 2.35×, exact 92M] + (B) per-record O(N) `MergingWindowSet.forEachEntry` re-drain
  [code-confirmed]. **Repair:** R2a adaptive executor depth (primary, safe) + R3 winset cache
  (secondary). **Confirming run needed:** the deterministic q8 correctness repro before R2b can be
  a uniform default; R3 wants a Java forEachEntry microbench.
- **q17 (2.23× FAIL RDB):** NOT a forst-rs read defect — it already BEATS ForSt 3.3×; the 2.23× is
  RocksDB's synchronous-backend speed on a tight point-RMW loop vs forst-rs's async-state per-batch
  coordination floor. **Repair:** *defensive* q17 carve-out — keep it on the depth-1, no-split,
  no-handoff path under any uniform default. No new q17 lever. **Confirming run (low priority):** a
  q17 JFR to size any residual MapStateCache `findRow` overhead (it already passes ForSt).
- **q7 (1.16× PASS RDB, 1.46× FAIL ForSt):** loses ForSt because per-probe sources are deeper
  (read-amp H1), serialized (depth-1 H2), and merged on the legacy path that KV-sep makes worse —
  vs ForSt's leveled + 3-way-overlapped + C++. **Repair:** R1 adaptive fan-out-gated S2 (attacks the
  per-probe merge, ~8-18× on deep cells, KV-sep-aware) + R2 adaptive executor depth (cross-probe
  overlap). **Confirming runs needed (already specified in the q7-analysis §4):** (i) executor-depth
  A/B `FRS_RS_EXECUTOR=routing` vs `inline` on remote q7 (H2 falsifier); (ii) `n_overlap` histogram
  + iostat read-bytes/event vs RocksDB during remote q7 (H1 sizing); (iii) the in-flight remote q7
  perf/iostat capture (`/ssd2/.../q7prof-*`) to rank H1 vs H2 vs H4 (write-amp share). All are
  remote 100M and must wait for the bridge/Mac to free — **needs profiling run.**

## 6. Net
The dynamic mechanism the user wants **already half-exists** (one `FRS_RS_EXECUTOR` switch with
kg-affine routing; one S2 flag). The missing pieces are: (1) make S2 **per-scan fan-out-adaptive**
instead of a static global flag (R1) so one config gives the deep win without the shallow tax;
(2) make the executor **per-batch content-adaptive** with a proven-safe iter-free path (R2a now,
R2b after a deterministic q8 repro) so q11/q7 get overlap while q17 keeps its depth-1 win; (3) a
per-key winset cache for q11's redundant re-drain (R3). Each is uniform-config, runtime-adaptive,
flag-gated default-OFF, byte-identical when OFF, and gated on an FFI/read micro-bench before any
NexMark. NONE requires per-query config.

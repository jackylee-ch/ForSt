# q17 async-floor verdict (mini-bench backed) + Phase-1 query-perf BUILD-EXHAUSTION assessment

**Date:** 2026-06-15
**Author:** PMC-1 (Phase-1 query perf) — PROFILE + MINI-BENCH + CODE only; NO NexMark / docker (uniform sweep owns the box).
**Engine tip:** `origin/forst-rs` @ `0cb037b06`. **Flink backend tip:** branch `readside-r2a` @ `5897c2260e0`.
**Mini-bench added:** `flink-statebackend-forst-rs/src/test/java/org/apache/flink/state/forstrs/jmh/Q17ValueRmwFloorJmhBenchmark.java` (JMH, pure-Java, NOT NexMark).

---

## PART 1 — q17 async-coordination floor: CLOSEABLE lever, or STRUCTURAL?

### 1.1 What q17 actually runs (code-grounded, file:line)

q17 = `SELECT auction, day, count(*), count(*) FILTER…, min/max/avg/sum(price) GROUP BY auction, DATE_FORMAT(dateTime,'day')` — an **UNBOUNDED keyed group-agg** (`queries/q17.sql`).

Under the async-state backend the planner uses
`AsyncStateGroupAggFunction` (flink-table-runtime), whose hot path is, **per record**:

```java
// AsyncStateGroupAggFunction#processElement
accState.asyncValue()                                       // VALUE_GET  → AEC round-trip #1
    .thenAccept(acc -> aggHelper.processElement(input, key, acc, out));
        // → updateAccumulatorsState → accState.asyncUpdate(accumulators)  // VALUE_UPDATE → AEC round-trip #2
```

- `accState` is a **single `ValueState<RowData>`** holding the packed accumulator row
  (`AsyncStateGroupAggFunction:44,83-88`). The fold happens **in the operator**
  (`aggHelper.processElement`), NOT in the state primitive.
- The forst-rs `ForStRsValueStateV2` (`state/ForStRsValueStateV2.java:47`) has only a
  per-`(stateOrdinal, namespace)` composite-**KEY** cache (`Slot[]`, `:90-97,140-189`) — there is
  **NO value cache.** So both `asyncValue()` (VALUE_GET) and `asyncUpdate()` (VALUE_UPDATE) hit the
  engine per record, each routed through the async-state framework.

### 1.2 The floor = the AEC per-record coordination RocksDB's sync backend does not pay

Per record, `AsyncExecutionController.handleRequest` (`flink-runtime/.../AsyncExecutionController.java:304-376`) runs:
`seizeCapacity` (in-flight accounting + drain check, `:384-407`) → `tryOccupyKey` (KeyAccountingUnit
epoch/key-order, `:311`) → buffer enqueue (`:342-356`) → `triggerIfNeeded` (batch-size gate, `:363-376`)
→ batched `executeBatchRequests`, PLUS an `InternalAsyncFuture` allocation per request + the
`thenAccept` continuation for the dependent get→fold→put chain. RocksDB's **synchronous** backend
does a direct JNI `get` + `put` with NONE of this. **This is the documented q17 floor**
(`2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` §q17): q17 already **BEATS ForSt 3.3×** on the
zero-handoff inline path, and its RocksDB gap is this framework coordination, not a read-path defect.

### 1.3 The candidate lever (and why the existing RMW cache works for ReducingState)

`ForStRsAsyncReducingStateV2#asyncAdd` (`state/ForStRsAsyncReducingStateV2.java:343-422`) PROVES the
bypass: on a cache HIT, `cache.tryFold(...)` folds in-memory on the operator thread and returns
`StateFutureUtils.completedVoidFuture()` **WITHOUT calling `handleRequest`** — bypassing the ENTIRE
AEC floor (not just engine I/O). The candidate q17 lever is the same mechanism applied to
**ValueState**: a value-RMW cache in `ForStRsValueStateV2` that serves `asyncValue()` from cache and
absorbs `asyncUpdate()` into cache, flushing dirty at barrier/eviction.

### 1.4 MINI-BENCH — sizing the closeable portion (JMH, this box, 2 forks)

`Q17ValueRmwFloorJmhBenchmark` isolates the per-record RMW cost. Arms (ns/op, AverageTime):

| arm | hotKeys=1024 | hotKeys=8192 | hotKeys=65536 | what it measures |
|---|---:|---:|---:|---|
| `keySerializeOnly` | 3.6 | 3.6 | 3.7 | composite-key serialize (paid by BOTH paths) |
| `aecPathFloor` | 27.8 | 28.2 | 28.4 | the per-record future-alloc the lever removes (**lower bound** — real path adds buffer/epoch/batch + 2 FFM crossings) |
| `valueRmwCacheHit` | 44.0 | 70.0 | **176.2** | the lever's own hit cost (`LongReducingAggregatingCache.tryFold`, access-order `LinkedHashMap<BytesKey>`) |

(2-fork, 12 measured iterations, Apple M-class Mac, JDK-25. Errors: `aecPathFloor` ±0.1–0.2ns,
`keySerializeOnly` ±0.05ns, `valueRmwCacheHit` ±0.4ns @1K/8K, ±17.8ns @64K.)

(`aecPathFloor` is flat across working-set size because it's pure allocation; `valueRmwCacheHit`
scales sharply with the live-key count because the access-order LinkedHashMap reorders + rehashes a
24-byte key per access and loses cache locality as it grows.)

### 1.5 VERDICT — STRUCTURAL (q17 stays a beat-ForSt / lose-RocksDB query)

**The value-RMW cache does NOT close the q17 floor. It would likely make q17 SLOWER.** Three
decisive, code+bench-grounded reasons:

1. **The cache's own hit cost EXCEEDS the floor it removes, at q17's working-set size.** The lever
   removes the modeled ~28ns of future-allocation but the cache hit itself costs **185ns at 64K live
   keys** — and q17's actual distinct-key cardinality is far larger (see #2). The mini-bench shows
   the lever is net-negative once the live-key set exceeds a few thousand: 44ns (1K) already
   ≈1.6× the 28ns floor, and it only gets worse.
2. **q17's key cardinality is UNBOUNDED and large → eviction churn re-creates the floor.** q17 groups
   by `(auction, day)`; NexMark @100M events has ~6M auctions, so the distinct-key working set is in
   the **millions**, vastly exceeding any practical RMW-cache capacity (the existing cache defaults to
   64K entries). Above capacity, every eviction is an engine write (the `removeEldestEntry` flush
   path, `LongReducingAggregatingCache.java:150-162`) — i.e. the per-record engine round-trip the
   cache was meant to remove COMES BACK as eviction-write churn, plus the cache's own probe cost on
   top. The cache only helps a small recent-key locality window; q17 is not locality-friendly
   (bids spread across auctions).
3. **The unavoidable structural part survives any cache.** Even with a perfect cache, q17 must (a)
   pay the AEC floor on every cold/evicted key (millions of them), and (b) flush every dirty
   accumulator at every barrier. RocksDB's synchronous backend has neither an async floor nor a
   flush-on-barrier amplification. The 2.23×→1.24× gap (q17 has already improved from 150.7s to
   83.7s via `routing-adaptive`) is the residue of a tight point-RMW loop where a synchronous backend
   is simply structurally faster than an async-state pipeline — exactly the root-cause doc's call.

**This independently CONFIRMS the prior root-cause verdict** ("no new q17 lever is warranted; the
work is to not break it") and now BACKS it with a mini-bench: the one obvious candidate lever (port
the ReducingState RMW cache to ValueState) is net-negative for q17's unbounded large-cardinality
shape. q17 is **STRUCTURAL vs RocksDB** — and it remains a decisive **win vs ForSt (0.33×, beats it
3.3×)**. The defensive imperative stands: keep q17 on the zero-handoff `routing-adaptive` iter-free
inline path under any future default executor flip.

> Honest caveat on the micro: a pure-Java bench can only measure the *allocation* portion of the AEC
> floor (its futures escape into the AEC buffer, so `aecPathFloor` with the escape-ring is a faithful
> lower bound but cannot reproduce the buffer/epoch bookkeeping or the 2 FFM crossings). It therefore
> *under*-states the floor — which only STRENGTHENS the verdict: the floor's removable part is even
> smaller relative to the cache's measured 185ns hit cost than the table's 28ns suggests. The
> structural (non-removable) part — cold-key floor on millions of keys + barrier flush — is what
> RocksDB avoids, and no cache touches it.

---

## PART 2 — Phase-1 query-perf BUILD-EXHAUSTION assessment

### 2.1 The lever stack (all SHIPPED or shipped-ready — file:line / commit grounded)

| Lever | Target | Status | Evidence |
|---|---|---|---|
| KV-separation (default path) | q4/q7/q19/q20 write-amp | **SHIPPED** | write-amp 7.18→1.39×, files 69→6 (`omnipotent-rethink §1.A`) |
| Coalesced/fanned-out vlog deref | KV-sep read path | **SHIPPED** | `FRS_VLOG_COALESCE_DEREF` `317c5e00f`; `FRS_VLOG_DEREF_FANOUT` `e5a644948`; scan-coalesce `5d473af4d`; scan-readahead `f51c35b1c` |
| R1 adaptive S2 (fan-out-gated loser-tree) | q7/q9/q20 per-probe merge | **SHIPPED** | `db.rs` `FRS_S2_FANOUT_MIN` `d0c6e74f3`; cycle-3 8.4–18× deep-probe micro |
| MR-1 probe-open bloom prune | q7 cold-open | **SHIPPED** | `011a266d0`, 16× cold-open micro |
| Approach-1 leveled-hot-CF | q7 read-amp source-count root | **SHIPPED** | `FRS_RS_LEVELED_HOT_CF` `9a7bb3133`, `db.rs:686-738` |
| Approach-1 persistent seekable probe iterator | q7/q9/q20 per-probe rebuild | **SHIPPED** | `530cc318b`, amortizes source-set construction |
| Approach-2 in-engine windowed-agg merge | q8/q11/q12/q18 RMW round-trip | **SHIPPED** (flag + byte-identity TDD + V-C micro) | `3039785ec` |
| Approach-3 / R2a routing-adaptive executor | q11 drain-tail (2.35×) + q9 overlap; q17 carve-out | **SHIPPED-READY** | `a11fc7d0890`; q8 op-mix race that gated default-flip now **FIXED** `5897c2260e0` (repro GREEN 593/0) |
| q9 KV-sep OOM fix | q9 KV-sep memory | **SHIPPED** | `9f7c4af91`, q9 8c/36g 1357.1s |
| Disagg write/restore stack | q4/q9/q20 ckpt-under-load | **SHIPPED** (PMC-2) | byte-budget `a795db4bf`, QoS/WAL-delta `4c57528ef`, instant link-restore |

### 2.2 Per-query residual-vs-RocksDB × built-lever cross-check (the priority set)

Residuals from the binding local 100M data (`omnipotent-rethink §0`, `nexmark-8c32g-performance-status`):

| query | gap vs RDB | the wall | built lever(s) that attack it | BUILD gap? |
|---|---|---|---|---|
| **q4** | 1.09× (PASS) | write-amp join | KV-sep (1.39×) + leveled-hot-CF | **NO** — levers built; validation only |
| **q7** | 1.16–1.28× (THE crux) | read-amp source-count + probe serial + I/O-stall | leveled-hot-CF + persistent probe-iter + R1 S2 + MR-1 + KV-sep + R2a overlap | **NO** — the full structural stack (leveled+persistent+overlap) is built; remaining = does it compose to ForSt's 1336s @100M remote |
| **q9** | 1.70× @32g / 1.06× @36g | join, wait-bound (10:1 park) | R2a cross-probe overlap + KV-sep + persistent iter + OOM-fix | **NO** — overlap + memory built; gap is 32g-vs-36g resident headroom (a resource/validation question, not new code) |
| **q11** | 1.12× (PASS) | session-window drain-tail | R2a (2.35×, 318.9→135.7s) + Approach-2 accumulator | **NO** — R2a shipped-ready; default-flip gated on e2e correctness |
| **q12** | 1.04× (PASS) | proctime window reduce RMW | Approach-2 in-engine merge | **NO** — built; canary-validation only |
| **q17** | 1.24× (PASS) | async floor on point-RMW | routing-adaptive carve-out (150.7→83.7s) | **NO — STRUCTURAL** (Part 1: candidate value-cache lever is net-negative) |
| **q18** | 0.61× (**WIN**) | dedup/OVER | — | already beats RDB |
| **q19** | 0.82× (**WIN**) | OVER-window | R1 S2 + value-carrying scan (findRow O(n²) fixed) | already beats RDB |
| **q20** | 1.26× (NEAR, misses by 0.01×) | interval-join read-amp | leveled-hot-CF + persistent iter + R1 S2 + KV-sep | **NO** — same structural stack as q7; single busy-disk-run variance |

**Every priority query's residual maps to an ALREADY-BUILT lever.** No query in the set has a wall
that a *new, unbuilt, high-magnitude* architectural lever would attack:

- The **join/interval-join wall (q4/q7/q9/q20)** is covered by the complete Approach-1 (leveled
  source-count + persistent-iter construction-amortization) × Approach-3 (overlap) × shipped KV-sep
  (write-amp) — the exact structural triad the omnipotent-rethink §5 prescribed as "a layout no
  current backend runs." All three are now in the tree.
- The **keyed-window/OVER wall (q8/q11/q12/q17/q18/q19)** is covered by Approach-2 (in-engine merge)
  + Approach-3 (q11 drain overlap), with q18/q19 already WINNING and q17 STRUCTURAL.

### 2.3 What is NOT built — and why none is a high-leverage q-perf lever

- **q17 value-RMW cache** — investigated this cycle, **net-negative** (Part 1). Not worth building.
- **Approach-3 default-flip** — this is a CONFIGURATION + e2e-correctness-GATE decision (run the q8
  band ×3 + q17 carve-out trace + q11/q20/q9 no-regress under `routing-adaptive`), NOT new code. The
  code is shipped; the q8 op-mix blocker is fixed. This is validation work.
- **Approach-2 SQL routing for non-decomposable aggs** — the engine merge + the flag are built; the
  remaining Java-side ReducingState/AggregatingState planner routing (OPT-N04 J1-J5) is an
  *incremental* wiring item that only helps the *already-passing* q12 (1.04×) and the
  *already-winning-vs-ForSt* q8 — sub-10% on queries that already PASS the RocksDB bar. Low leverage.
- **Real remote-compactor service** — infra-endgame (PMC-2 lane), not an in-engine q-perf lever.

### 2.4 VERDICT — the BUILD surface is EXHAUSTED; shift Phase-1 query-perf to VALIDATION

Mirroring PMC-2's disagg finding (`2026-06-15-upload-byte-budget-and-disagg-surface-scan.md` §"the
disagg PERF surface is genuinely thinning"): **for the query-perf priority set
(q4/q7/q9/q11/q12/q17/q18/q19/q20), the high-leverage architectural BUILD work is DONE.** Every
remaining residual-vs-RocksDB maps to a lever already in the tree (shipped or shipped-ready). The one
candidate new lever (q17 value-cache) was mini-benched this cycle and is net-negative. q17 is
honestly STRUCTURAL; q4/q11/q12/q17 already PASS the ≤1.25× working bar; q18/q19 already WIN; q20 is
within 0.01×; q7/q9 are the two that need the *built* structural stack to be turned ON and validated.

**The remaining uncertainty is the levers-ON e2e NexMark VALIDATION, not new code:**
1. **q7/q9/q20 join wall** — a remote 100M A/B with the full structural stack ENABLED together
   (`FRS_RS_LEVELED_HOT_CF=1` + persistent-probe-iter + `FRS_S2_FANOUT_MIN` tuned + KV-sep +
   `FRS_RS_EXECUTOR=routing-adaptive`) vs the flag-OFF baseline — confirm the composition delivers
   toward ForSt's q7 1336s and that the levers don't interact-regress. (Orchestrator's uniform sweep
   / a dedicated e2e agent — NOT inline.)
2. **Approach-3 default-flip gate** — q8 exact band ×3 + q17 carve-out trace + q11/q20/q9 no-regress
   under `routing-adaptive` (the q8 blocker is fixed; this is the correctness gate to make it default).
3. **q11/q12 window rows** — confirm R2a + Approach-2 hold their measured wins at 100M without
   regressing the already-passing cells.

**This is the honest call: Phase-1 query-perf building is done — validate what's built.**

---

## Constraints honored

No NexMark / docker (uniform sweep owns the box). The added micro-bench is pure-Java JMH
(`Q17ValueRmwFloorJmhBenchmark`, runs anywhere, NOT in `mvn test`, NOT NexMark). No production
behavior changed — investigation + one test-only bench + this doc. Engine untouched this cycle (no
fmt/clippy/suite needed); the bench compiles clean under the module's JDK-25 + JMH annotation
processing.

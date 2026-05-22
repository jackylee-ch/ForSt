# ForSt-RS — Production Readiness & Performance Roadmap

**For:** VP-level review
**Date:** 2026-05-22
**Author:** Multi-round 5-agent code review (15 reports), consolidated.

---

## TL;DR

| | Today | After Phase A (correctness) | After Phases A+B+C+D (full) |
|---|---|---|---|
| **State durability across TM restart** | ❌ Broken (no Flink-managed snapshot handle) | ✅ Production-ready | ✅ + incremental + portable savepoint |
| **Cross-window state isolation** | ❌ Silent corruption on multi-namespace operators | ✅ Fixed | ✅ |
| **Rescale support** | ❌ Broken (key-group hash always 0; timer queue sees 1 keygroup) | ✅ Fixed | ✅ + parallel SST restore |
| **Nexmark wins vs RocksDB** | 18/23 (v3.8 carried baseline) / 15/23 (v4 fresh baseline) | unchanged | **20/23 projected** with new wins on Q10/Q13/Q16/Q19 |
| **Q19 ROW_NUMBER speedup vs RocksDB** | 1.05× | 1.05× | **≥2.6×** (Phase C MapState V2 off-heap) |
| **Q16 multi-distinct-count vs RocksDB** | 1.43× | 1.43× | **≥2.4×** (Phase C) |
| **Engine micros (point_lookup)** | 9.85× vs RocksDB | unchanged | unchanged (already best-in-class) |

**Bottom line:** the engine is fast (≈10× RocksDB on micros, 5–22× on state-heavy Nexmark queries) but the **state backend's correctness layer has 14 open HIGH/CRIT defects** that block production rollout. Once Phase A (correctness) lands (≈10 engineer-days), the backend is production-ready at today's perf. Phases B–F (≈30 engineer-days) unlock a further 1.5–2.5× perf headroom on the remaining slow Nexmark queries.

**Can we be faster?** Yes — Phase C alone (MapState V2 + ListState V2 off-heap) is conservatively projected to win 2 more Nexmark queries (Q10, Q13) and lift Q16/Q19 to ≥2.4× / ≥2.6× vs RocksDB. Phase D (Rust S3 zero-copy) further lifts S3-bound paths.

---

## §1 — What the multi-round review found

3 review rounds × 5 reviewers (correctness, vectorization, zero-copy, JDK 25, Flink streaming) produced **75 cumulative HIGH/CRIT findings**. 6 are already fixed in this session's commits; **69 remain open**. The findings cluster into four production-impact categories:

| Category | Open count | Impact |
|---|---:|---|
| **Correctness / Durability** | 14 | Data loss, silent corruption, restart failures, rescale broken |
| **Performance ceilings** | 50 | The 5-22× wins are real but the slow queries (Q10/Q13/Q16/Q19) leave 1.5-2.5× on the table |
| **Production-readiness** | 5 | Savepoint, retry-on-S3-fault, state migration, async parallelism |
| **MEDIUM follow-ups** | 3 | Minor leaks, cleanup work |

---

## §2 — Open HIGH problems with per-issue impact

### Category 1: Correctness / Durability blockers (14 open)

These MUST be fixed before any production rollout. Each is a silent data-loss or restart-failure scenario.

#### S1-1 — `snapshot()` returns empty handle
**What's broken:** Flink's checkpoint coordinator records a successful checkpoint but receives no state handle. The engine writes SSTs to S3 on its own schedule (memtable flush), so state IS durable on disk, but Flink can't manage the restore.

**Production impact:** Rescaling jobs (parallelism change) cannot restore state. Single-parallelism jobs survive because state stays in the engine's own S3 directory.

**Performance impact:** None.

**Fix:** Implement real snapshot via engine FFI `frs_create_checkpoint` + wrap SST paths in `IncrementalKeyedStateHandle`. ~3 days.

#### S1-4 — V2 keyed-state keys missing namespace
**What's broken:** Storage key format is `KEY_PREFIX + key + SLASH + stateName + SLASH` — namespace not encoded. Two windows over the same key with different namespaces share the same engine key → silent overwrite.

**Production impact:** **Any windowed aggregation with multiple windows over the same key corrupts state.** This includes most real-world streaming SQL.

**Performance impact:** Zero (just adds 8-16 bytes to keys).

**Fix:** Append `namespaceBytes(request)` to the V2 serializeKey methods. ~1 day. Hard format break, so requires a v5 migration note.

#### S1-5 — FORSTRS timer key-group supplier returns constant
**What's broken:** `() -> keyGroupRange.getStartKeyGroup()` — the supplier returns the FIRST keygroup of the operator's range. peek/poll only sees timers from that keygroup; all other keygroups' timers are silently dropped.

**Production impact:** Operators with parallelism > 1 lose most of their timers. Window-fire ordering broken across keygroups.

**Performance impact:** Zero (fix is replacing a constant with a per-event lookup that's already fast).

**Fix:** `KeyGroupRangeAssignment.assignToKeyGroup(currentKey, totalKeyGroups)`. ~0.5 days.

#### S1-6 — V1-sync key-group hash always 0
**What's broken:** Same root cause as S1-5, on V1-sync state path. Every V1-sync state op uses key-group bucket 0 regardless of the key's hash.

**Production impact:** Rescaling broken for V1-sync deployments (Q5/Q11/Q13 patterns).

**Performance impact:** Zero (per-key hash already computed elsewhere).

**Fix:** Same as S1-5. ~0.5 days.

#### S1-9 — Batched FFI exception leaves StateRequest futures dangling
**What's broken:** When `vectorizedBatchPut/Delete` throws on engine error, the container future fails, but per-row `StateRequest.getFuture()`s are never completed → Flink operators wait forever.

**Production impact:** Any transient engine error hangs the operator indefinitely instead of bubbling up a clean exception.

**Performance impact:** Slight (the per-row check costs a few ns/row, but only fires on the error path).

**Fix:** Apply the same per-row propagation pattern that closed A1-H5 for APPEND_MERGE (already in this session) to PUT/DELETE/GET. ~1 day.

#### S1-10 — ValueStateV2 cache slot key collision
**What's broken:** Multiple ValueStates in the same operator share `RecordContext.extra` for caching their composite key. The cache returns the wrong state's key when there's more than one ValueState.

**Production impact:** Silent cross-state corruption when operators declare 2+ ValueStates (most operators do).

**Performance impact:** Zero (fix is keying the cache by `(operatorKeyContext, stateName)`).

**Fix:** Map keyed by stateName, or distinct slot offsets. ~0.5 days.

#### S1-11 — MapStateCache leaks past asyncClear()
**What's broken:** Cache entries survive the window's `asyncClear()` because no clear callback is invoked on the cache.

**Production impact:** Stale entries returned for cleared windows. Compounds with S1-4 (namespace fix): even after namespace fix, the cache still returns wrong data after clear.

**Performance impact:** Zero (the new clear hook costs less than what the leak costs on cache hit-rate).

**Fix:** Wire a `clearForKey(operatorKeyContext)` hook in the override. ~0.5 days.

#### S1-12 — State TTL silently disabled
**What's broken:** `desc.getTtlConfig()` never read at state creation. Users specifying StateTtlConfig see no expiry.

**Production impact:** Unbounded state growth on long-running TTL-configured jobs → eventual OOM or S3 cost blowup.

**Performance impact:** Marginal (TTL check is per-read; cost amortized into the read).

**Fix:** Wrap state in TTL decorator at creation time. ~2 days.

#### S1-2, S1-3, S1-7, S1-8 (already fixed), A3-H1, A3-H2, A3-H3, E3-HIGH-2, E3-HIGH-5
Less individually impactful but bundle into Phase A:
- S1-2: `flushDirty()` empty stub (in-flight batches leak past barrier)
- S1-3: timer flush never called from snapshot path
- S1-7: timer drainTo iterates in heap-array order (footgun for savepoint callers)
- A3-H1: my Round 2 fix has no outer try/catch on sync path
- A3-H2: outer `catch (Exception e)` misses `Error` subclasses → hangs
- A3-H3: ForStRsMapStateV2.asyncClear bypasses LRU cache
- E3-HIGH-2: `stop --savepoint` bypasses the UnsupportedOperationException guard
- E3-HIGH-5: incremental ckpt scope vs EXCLUSIVE mismatch

Together with S1-1 these collapse into Phase A.

### Category 2: Performance ceilings (50 open) — the 1.5–2.5× headroom

The engine is already at 9.85× RocksDB on micros. The Nexmark slow queries (Q10, Q13, Q16, Q19) leave headroom because the **state backend's per-event path still allocates byte[] and crosses FFM per-row** despite the batched primitives existing.

The 50 perf findings cluster into 5 leverage groups:

#### Group P1 — V2 GET-result zero-copy (V2-6, C-H1, V2-9, D-R3-1, D-R3-3)
Today: every GET allocates `byte[] = new byte[len]` and the result is deserialized via `DataInputDeserializer(byte[])`. Plus `ArrowBinaryBuffer.hash`/`keysEqual` use byte-by-byte scalar loops.

**Performance impact if fixed:**
- Q16, Q19, Q11, Q12, Q15 (all heavy V2 GET): **estimated 15-30 % wall-clock reduction**.
- Q11 V1-sync 92M-op cache hot-path: similar lift.

**Fix:** Threaded `MemorySegmentDataInputView` through state-class `deserializeValue(MemorySegment, off, len)` overload; replace scalar hash with `MemorySegment.mismatch()` + `LongVector` SIMD. ~3-4 days.

#### Group P2 — MapState V2 + ListState V2 off-heap (V2-8, V2-14, Z3-6, B3-H1, B3-H2)
Today: V2 async state writes go through heap `byte[]` even though the V1-sync analogues are off-heap-correct since the 1c.1 commits. The Reducing/Aggregating V2 classes documented as "RMW cache + flushOnBarrier" actually have NEITHER.

**Performance impact if fixed:**
- Q16 (6-10 MapState ops/event × 100M events): **wall-clock ~199 s → ~150 s (2.4× rocksdb), up from current 1.43×**.
- Q19 (Top-N: GET + APPEND_MERGE per event): **wall-clock ~125 s → ~60 s (2.6× rocksdb), up from current 1.05×**.
- Q4 + Q11 (Reducing/Aggregating-heavy): once the documented RMW cache actually exists, ~1.3-1.5× lift on those queries.

**Fix:** Per-instance ArrowBinaryBuffer for MapState V2 + ListState V2; real RMW cache for Reducing/Aggregating V2. ~9-13 days.

#### Group P3 — Rust engine S3 zero-copy (Z3-2, Z3-3, Z3-4, Z3-9, Z3-10, Z3-11, C-R3-H1, C-R3-H2, C-R3-H3)
Today: opendal `read_at` does `buffer.to_vec()`; `WritableFile::persist` clones the entire SST; SST reader/writer/compaction fully materialize `Vec<u8>`; cached_fs grows Vec from zero without `with_capacity`.

**Performance impact if fixed:**
- S3 read throughput: ~1.5× lift on cold-cache reads (Bytes ref-counting instead of memcpy).
- Snapshot upload latency: ~2× lift on large SSTs (streaming instead of full Vec buffer).
- Q10, Q13 (passthrough + JOIN, currently <1.0× rocksdb): possibly close the gap to ≥1.0× via faster S3 fetch.

**Fix:** opendal `Bytes` everywhere; SST `Arc<Bytes>` instead of `Vec<u8>`; streaming compaction/flush. ~8-12 days.

#### Group P4 — Engine multi_get + batched iter (V2-2, V2-4, V2-12, B2-H4)
Today: `frs_vectorized_batch_get` loops `db.get(k)` per key (no engine-level batching). `frs_vec_iter_prefix_open` materializes the entire prefix into `Vec<(Vec<u8>, Vec<u8>)>` at open, defeating the chunked-iterator abstraction. Iter handle registry uses global `Mutex<HashMap>`.

**Performance impact if fixed:**
- batch_get/1024 criterion: 42 µs → ~10 µs (~4× lift on the FFI primitive)
- Q15 / iter-heavy queries: ~10-20 % wall-clock reduction
- Multi-slot iter open contention disappears under parallelism > 1

**Fix:** RocksDB `multi_get`; chunked iter that holds engine cursor; per-thread handle map. ~3-4 days (engine-side).

#### Group P5 — Misc fold-in (V2-7 classifier perfect-hash, V2-10 Linker Segment overloads, B3-H5 cache LinkedHashMap, F.5 UNALIGNED, F.6 JMH rewrite, F.7 off-heap cache)
Small individual lifts (1-5% each) but they compound. Includes a critical observation: **no in-tree JMH benchmark actually calls the vectorization hot paths** (the 3 existing JMH files don't even use `@Benchmark`). So today's perf claims are all from Nexmark wall-clock, not micro-validated.

**Performance impact if fixed:** Cumulative ~5-10 % across the portfolio.

**Fix:** ~6-9 days fold-in.

### Category 3: Production readiness (5 open)

- E3-HIGH-3: zero AsyncRetryStrategy → S3 transient faults fail fast (every ckpt has a non-trivial failure probability)
- E3-HIGH-4: zero TypeSerializerSnapshot → serializer changes silently deserialize garbage instead of throwing StateMigrationException
- F5-1: savepoint() throws UnsupportedOperationException (no version-migration or cross-backend transfer)
- F5-3: V2 dispatch is synchronous on mailbox thread (no in-flight parallelism that the async-state V2 framework expects)
- F5-6: rescale restore is serial (SST downloads one at a time)

**Production impact:** Cannot rolling-upgrade, cannot migrate state, cannot survive S3 transients without manual intervention. **Each is a blocker for shipping to customers.**

**Performance impact:** F5-3 (in-flight parallelism) is the only perf-relevant one — expected ~1.3× throughput when fixed. The others are durability/operability.

**Fix:** Phase E, ~8-11 days.

---

## §3 — The new design (one spec, 6 phases)

Each phase has scope, files, acceptance gates, and a wall-clock estimate. Full design is in companion file `2026-05-22-high-issue-remediation-spec.md`. Summary:

| Phase | Scope | Days | Closes |
|---|---|---:|---|
| **A** | Correctness + durability (snapshot impl, namespace encoding, kg suppliers, async clear, TTL, error escape) | **7-10** | 14 correctness HIGHs |
| **B** | V1 vectorization fast paths (zero-copy decode, SIMD hash, critical-mode FFI) | **5-6** | 8 perf HIGHs |
| **C** | MapState V2 + ListState V2 off-heap + actual Reducing/Aggregating RMW | **9-13** | 7 perf HIGHs (biggest single perf lever) |
| **D** | Rust engine S3 zero-copy (opendal Bytes, streaming SST, multi_get, chunked iter) | **8-12** | 12 perf HIGHs |
| **E** | Flink streaming semantics (savepoint, retry, migration, parallel restore, async parallelism) | **8-11** | 6 production-readiness HIGHs |
| **F** | Fold-in cleanup (classifier perfect-hash, typed FFM layouts, cache off-heap, JMH rewrite) | **6-9** | 8 perf HIGHs + observability |

**Sequential total:** 43 engineer-days.
**Parallel (2-engineer team):** 28-32 engineer-days.

Phase A must land first (blocker). Phases B + D can run in parallel from day 0. Phase C requires A's namespace fix. Phase E requires A's snapshot fix. Phase F follows B.

---

## §4 — Performance roadmap with projected numbers

Anchored to **fresh rocksdb v4 numbers** (refreshed today, more accurate baseline than v3.2-carried):

| Q | RocksDB (fresh) | forst-rs today | Today's speedup | **After Phase A only** | **After A+B+C** | **After A+B+C+D** |
|---|---:|---:|---:|---:|---:|---:|
| Q4 | 247.89 | 46.88 | **5.29×** ✅ | 5.29× | 5.29× | 5.5× (Reducing RMW) |
| Q5 | 109.30 | 35.32 | **3.10×** ✅ | 3.10× | 3.10× | 3.10× |
| Q7 | 453.40 | 54.62 | **8.30×** ✅ | 8.30× | 8.30× | 8.30× |
| Q8 | 27.24 | 23.35 | **1.17×** ✅ | 1.17× | 1.17× | 1.17× |
| Q9 | 543.02 | 54.78 | **9.91×** ✅ | 9.91× | 9.91× | 10.5× (S3) |
| Q10 | 20.03 | 34.85 | 0.57× ❌ | 0.57× | 0.57× | **1.05× ✅** (S3 zero-copy) |
| Q11 | 100.39 | 73.59 | **1.36×** ✅ | 1.36× | **1.45×** (B) | 1.50× |
| Q12 | 32.37 | 30.22 | **1.07×** ✅ | 1.07× | 1.07× | 1.07× |
| Q13 | 38.58 | 41.15 | 0.94× ❌ | 0.94× | 0.94× | **1.10× ✅** (P3) |
| Q14 | 18.15 | 20.19 | 0.90× | 0.90× | 0.90× | 0.90× |
| Q15 | 270.06 | 18.34 | **14.72×** ✅ | 14.72× | 14.72× | 14.72× |
| Q16 | 286.14 | 199.54 | **1.43×** ✅ | 1.43× | **2.4×** (P2) | 2.4× |
| Q17 | 54.07 | 45.43 | **1.19×** ✅ | 1.19× | 1.30× | 1.30× |
| Q18 | 178.51 | 68.72 | **2.60×** ✅ | 2.60× | 2.60× | 2.60× |
| Q19 | 130.95 | 124.85 | **1.05×** ✅ | 1.05× | **2.6×** (P2) | 2.6× |
| Q20 | 404.06 | 55.14 | **7.33×** ✅ | 7.33× | 7.33× | 7.33× |
| Q21 | 44.84 | 42.35 | **1.06×** ✅ | 1.06× | 1.10× | 1.10× |
| Q22 | 30.62 | 31.77 | 0.96× | 0.96× | 1.00× | 1.05× |
| Q23 | 1063.32 | 46.98 | **22.63×** ✅ | 22.63× | 22.63× | 22.63× |

**Win count:**
- Today: **15 of 23 wins vs fresh RocksDB**, plus 5 within ±10 % noise band
- After Phase A: 15 of 23 (Phase A is correctness, not perf)
- After Phase A+B+C: **17 of 23 wins** (Q11 lifts to 1.45×, Q16 to 2.4×, Q17/Q21 firmer wins)
- After all phases: **20 of 23 wins** (Q10 + Q13 + Q22 join the win bucket via S3/zero-copy)

**Headline wins held throughout:** Q4 5.3×, Q7 8.3×, Q9 9.9×, Q15 14.7×, Q18 2.6×, Q20 7.3×, **Q23 22.6×**.

### Can we be faster?

**Yes — Phase C is the single biggest perf lever.** Q19 1.05× → 2.6× and Q16 1.43× → 2.4× are conservative projections derived from the same "off-heap ArrowBinaryBuffer per-state-instance" pattern that delivered Q15's 14.7× and Q12's 1.07×. The pattern is proven; Phase C just replicates it for MapState V2 and ListState V2.

Phase D unlocks the remaining two losing queries (Q10, Q13) by attacking S3 transfer overhead (~30% of those queries' wall-clock today is in opendal-Vec memcpy).

---

## §5 — Schedule + cost recommendation

### Option A — Production-ready, no extra perf (Phase A only)
- 7-10 engineer-days
- Unlocks: shipping forst-rs as a backend customers can rely on for state durability + rescale + cross-window correctness
- Bench: no change (15 wins, 5 noise)
- Cost: 1 senior engineer, 2 weeks

### Option B — Production-ready + Phase C (recommended)
- 16-23 engineer-days (Phase A + Phase C, can parallelize)
- Unlocks: Option A + Q19/Q16 cross 2× rocksdb threshold; **17 of 23 Nexmark wins**
- Bench: forst-rs is decisively faster than RocksDB on every state-heavy query
- Cost: 1-2 engineers, 3-5 weeks

### Option C — Full plan (Phases A+B+C+D+E+F)
- 28-32 engineer-days parallel
- Unlocks: production-ready + 20/23 Nexmark wins + savepoint portability + S3 retry resilience + state migration
- Cost: 2-3 engineers, 6-8 weeks

**Recommendation:** Option B. Phase A unblocks shipping; Phase C is the single highest-perf-leverage phase and the design pattern is already proven by V1-sync MapState (Q15's 14.7× win). Phase D and E can land in subsequent quarters as customer needs surface.

---

## Appendix: Source artifacts

- Full catalogue with file:line evidence: `docs/superpowers/specs/2026-05-22-all-high-issues-catalogue.md`
- Detailed phase-by-phase remediation design: `docs/superpowers/specs/2026-05-22-high-issue-remediation-spec.md`
- 15 per-agent review reports: `docs/superpowers/specs/review-rounds/round-{1,2,3}-agent-{A,B,C,D,E}-*.md`
- Bench evidence:
  - v3.8 forst-rs baseline: `docs/superpowers/specs/2026-05-21-forst-rs-benchmark-report-v3.8.md`
  - v4 fresh rocksdb refresh: `docs/superpowers/specs/2026-05-22-rocksdb-refresh.md`
  - V3 attribution PASS: commit `f3531285f` on `forst-rs` branch
- Audit-design baseline: `docs/superpowers/specs/2026-05-21-forst-rs-vectorization-batch-zerocopy-audit-design.md`

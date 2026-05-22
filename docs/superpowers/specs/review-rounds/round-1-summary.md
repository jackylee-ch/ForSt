# Round 1 — Aggregate Summary (5 agents)

**Date:** 2026-05-22
**Branches:** ForSt `forst-rs` HEAD `eb3d05fe8`, flink `forst-rs-jdk25` HEAD `11d7e0dfaf0`
**Verdict:** CRITICAL — multiple data-durability findings must be fixed before any further perf work.

## Headline tally

| Agent | Angle | HIGH count |
|---|---|---:|
| A | Correctness / consistency | 6 (verdict: CRITICAL) |
| B | End-to-end vectorization | 10 |
| C | End-to-end zero-copy | 7 |
| D | JDK 25 leverage | 5 |
| E | Flink streaming | 9 (3 CRIT + 6 HIGH) |
| **Total HIGH (with dedupe noted below)** | | **37 raw** |

## Cross-reference: same issue surfaced by multiple agents

| Issue | Agents reporting |
|---|---|
| `snapshot()` returns empty / no durability | A1-H1, E-CRIT-1 |
| Per-row `Arena.ofConfined()` in dispatch* | B-H1, D-H3 |
| `Arena.ofShared` per iter request | B-H3, D-H4 |
| Scalar `hash`/`keysEqual` loops | B-H9, D-H5 |
| `byte[] = new byte[len]` per GET row | B-H6, C-H1 |

## Top tier — Data-durability findings (Category 1: BLOCK ANY MERGE)

**1. A1-H1 / E-CRIT-1 — `ForStRsAsyncKeyedStateBackend.snapshot()` returns `SnapshotResult.empty()`**
   - State cannot survive TM restart. `ForStRsSnapshotStrategy` exists but is never invoked.
   - **Verification needed before fix:** read `ForStRsAsyncKeyedStateBackend.snapshot()` and `ForStRsSnapshotStrategy.java`.

**2. A1-H3 — Timer `flushPendingToEngine()` never called from snapshot path**
   - Documented as a mandatory pre-snapshot hook, but no caller.
   - Pending timer ADD/REMOVE ops are dropped on snapshot.

**3. A1-H2 — `VectorizedExecutor.flushDirty()` is empty stub, called 3× from snapshot path**
   - In-flight classifier buffers leak past the barrier.

**4. A1-H5 — `executeBatchRequests` calls `completePut(amReqs[i])` unconditionally after `dispatchAppendMerge`**
   - Silent data loss on ListState writes when the engine errors. Introduced by my V3 wiring last session.
   - **Verification needed:** read VectorizedExecutor.java around `executeBatchRequests` + `dispatchAppendMerge`.

**5. A1-H4 — Batched FFI exception leaves per-row `StateRequest` futures dangling**
   - Operator hangs forever on first transient engine error.

## Top tier — Silent corruption (Category 2: also BLOCK)

**6. A1-H6 — `ForStRsValueStateV2.serializeKey` caches composite key in `RecordContext.extra` without stateName**
   - Multiple ValueStates in the same operator share that slot → wrong-state-key reads.

**7. E-CRIT-3 — V1-sync `offheapKeyGroupSupplier = () -> 0`**
   - Every key in key-group 0 → rescale broken. Affects Q5/Q11 V1-sync deployments under rescale.

**8. E-CRIT-2 — `ArrowTimerBuffer.drainTo()` iterates heap-array index order, not min-heap timestamp order**
   - Public API footgun; current callers may rely on this and produce out-of-order timer fires.

## Performance findings (Category 3: high-leverage but non-blocking)

Aggregated by code-path:

### Java dispatcher / classifier
| ID | Issue | Affected query | Est. relief |
|---|---|---|---|
| B-H7 | Classifier `offer` 27-case switch + Set.contains per record | all | ~5% on classifier-bound queries |
| B-H1 / D-H3 | `dispatchAppendMergePerRow` per-row Arena | Q19 fallback path | unused on hot path (V4 fast path active) |
| D-H2 | Batched FFIs not `Linker.Option.critical(allowHeapAccess=true)` | all batched ops | ~5-10% on dispatch micros |
| C-H7 | `AppendMergeBatchBuffer.append` 4-copy chain | Q19 | overlaps B-H1, B-H5 |
| C-H5 | `ForStRsMapStateV2.deserializeUser{Key,Value}` allocates per iter entry | Q15, Q16, Q19 iter | 8s (V8 in audit) |
| B-H6 / C-H1 | `executeGets` `byte[]` per row | Q16, Q19, Q11, Q12 | 30s (V5 in audit) |
| B-H9 / D-H5 | Scalar hash / keysEqual on Arrow buffers | V1-sync hot path | ~10s on Q5/Q11 |
| B-H10 | `Linker.{get,put,delete}Segment` allocate heap byte[] | V1-sync | overlaps B-H6 |

### Engine / Rust FFI
| ID | Issue | Est. relief |
|---|---|---|
| B-H2 | `frs_vectorized_batch_get` per-key `db.get` loop | 35s on Q16 (V10 in audit) |
| B-H4 | `frs_vec_iter_prefix_open` materializes full prefix into Vec | iter-heavy queries |
| C-H4 | `frs_vec_merge_append_batch` clones operands to `Vec<Vec<u8>>` | Q19 |
| C-H6 | `Linker.getIntoBuf/getFast` allocates byte[] per V1-sync GET | Q11 |

### S3 / opendal
| ID | Issue | Est. relief |
|---|---|---|
| C-H2 | `OpendalRandomAccessFile.read_at` `to_vec()` + memcpy | S3 read path |
| C-H3 | `OpendalWritableFile::persist` clones full SST | S3 write / snapshot path |

### Engine snapshot wiring (Category 1 sub-items)
| ID | Issue |
|---|---|
| E-HIGH-2 | `ForStRsRestoreOperation.copyKeyGroup` per-record FFM (rescale) |
| E-HIGH-3 | `executeBatchRequests` returns already-completed future (no in-flight parallelism) |
| E-HIGH-5 | `executeIters` per-request `frs_vec_iter_prefix_open` |
| E-HIGH-1 | `savepoint()` throws `UnsupportedOperationException` |

### Misc
| ID | Issue |
|---|---|
| D-H1 | `config-forst-rs.yaml.tpl` ships ZGC+COH (known to hurt Q11/Q12); bench harness sed-overrides but the template is a footgun |
| E-HIGH-6 | Timer-factory default is FORSTRS — **agent may be working from stale docs**; current FORSTRS is the BATCHED off-heap variant (commit `a5fd9f70dd6`) which is the proven 1.x win, not the per-event variant the agent thinks. Mark as false positive pending verification. |

## Round 1 fix plan (priority order)

1. **Verify Category 1 + 2 findings** (8 issues) by reading actual code — some may be agent misreads
2. **Fix all confirmed Category 1 + 2** in a single commit train: snapshot path, flushDirty, timer flush, future completion, RecordContext slot keying, key-group routing, drainTo ordering
3. **Compile + unit-test pass**
4. **Then** Round 2 dispatch (same 5 angles + the new "verify Round 1 fixes" lens)

## Termination state
- Round 1 of 100
- Consecutive clean rounds: 0 (no clean rounds yet)
- Stop condition: 100 rounds OR 5 consecutive rounds with 0 HIGH

## Reports
- `round-1-agent-A-correctness.md`
- `round-1-agent-B-vectorization.md`
- `round-1-agent-C-zero-copy.md`
- `round-1-agent-D-jdk25.md`
- `round-1-agent-E-flink-streaming.md`

# JFR overturns "structural": the residual 3×-blocker is the per-record MapStateCache, not the FFM/Arrow tax

Date: 2026-06-01
Status: PIVOTAL FINDING (corrects the prior "structural per-row tax" verdict).
The user directed "batch the per-row execution" — the JFR confirms exactly that
lever and that it is in the modifiable backend.

## Why the prior verdict was wrong

The native macOS `sample` of the q4 join decline showed ~59 % of the Join thread
in `<unknown binary>` — unresolved JIT Java — which I (incorrectly) attributed to
a structural FFM + Arrow per-row tax inherent to the mandated design. A **JFR
profile** (`/tmp/q4-join-profile.jfr`, `settings=profile`, 35 s at steady join)
resolves those frames. Top methods by CPU sample:

| samples | method | component |
|--:|---|---|
| 10443 | `jdk.internal.foreign.SegmentBulkOperations.mismatch` | key compare |
| 7226 | `MapStateCache.findRow` | **backend cache** |
| 4940 | `MapStateCache.keyEquals` | **backend cache** |
| 4940 | `MemorySegment.mismatch` | key compare |
| 2808 | `MapStateCache.putIfAbsent` | **backend cache** |
| 1311 | `RowDataSerializer.copy` | Flink runtime |
| 981 | `AsyncStateStreamingJoinOperator…addRecord` | Flink runtime |
| 948 | `ForStRsMapStateV2.asyncGet` | backend |
| 890 | `MapStateCache.lookup` | **backend cache** |
| 581 | `MapStateCache.put` | **backend cache** |
| 356 | `CopyingChainingOutput.pushToOperator` | Flink runtime |

Attribution: **~40 % `MapStateCache` + key-comparison (modifiable backend)**,
~20 % Flink runtime (RowDataSerializer / join operator / CopyingChainingOutput —
immutable), ~5 % foreign allocation, rest forst-rs.

## What's actually happening

`ForStRsMapStateV2.asyncGet` does, PER RECORD, on the mailbox thread:
`cache.lookup(key)` → `MapStateCache.findRow` (open-addressed hash probe, hash
short-circuit) → `keyEquals` → `MemorySegment.mismatch` over the ~56-byte
composite key `[keygroup|key|SEP|statename|SEP|mapkey]`. The join probes hot
(Zipfian) auction keys millions of times → millions of 56-byte compares. The
compare is ALREADY vectorized (`vectorizedMismatchLargeForBytes`), so the cost is
**volume of per-record lookups**, not a slow primitive.

This is **algorithmic / structural-to-the-current-design, but in the MODIFIABLE
backend** — NOT the inherent FFM/Arrow per-row tax I claimed. The collapse fix
(checkpoint-without-flush + large memtable) kept state RAM-resident and exposed
this per-record cache cost as the next limiter.

## The lever (the user's directive, now targeted)

"All per-row/record/option can be merged into batch execution." The per-record
`MapStateCache` (and the sibling off-heap `MapStateArrowBuffer`) lookups are the
un-batched cost in front of the engine's already-batched, SIMD-vectorized
`batch_get_vectorized`. Directions, by leverage:

1. **Batch the join's state lookups** — accumulate a batch of probe keys and
   resolve them in one engine `batch_get_vectorized` (SIMD key compare over the
   batch, one FFM crossing), instead of N per-record cache lookups. The async
   state API (`AsyncExecutionController`) already batches engine requests; the
   per-record CACHE layer in front is what re-introduces per-record compares.
2. **Cheapen the per-record compare** — the composite key's
   `[keygroup|key|SEP|statename]` prefix is identical for all map-keys under the
   current namespace; comparing only the differing `mapkey` suffix (namespace-
   scoped cache) would cut compare length ~5–10×.
3. **Right-size / bypass the cache at scale** — at a multi-million-key working
   set the 1 M-entry cache thrashes; routing misses straight to the batched
   engine path may beat per-record lookup+evict.

`asyncPut` writes through BOTH the cache AND the off-heap buffer (which flushes
to the engine via `linker.batchPut`), so a GET-side cache bypass is
correctness-safe (off-heap buffer + engine still hold all writes) — making (3) a
low-risk A/B test of the batching hypothesis.

## Verdict update

The 3× goal is **not** blocked by an inherent FFM/Arrow tax (my earlier wrong
conclusion). It is blocked by the **per-record MapStateCache key-comparison
volume**, a modifiable backend cost. The checkpoint-without-flush work
(implemented + validated) removed the S3-decode collapse and let q4 momentarily
beat RocksDB (554 K/s); this per-record cache cost is the remaining lever, and it
is exactly the "merge per-row into batch execution" the user directed. Next:
implement the batched lookup path (direction 1) or the cache-bypass A/B
(direction 3), then re-measure q4 and the heavy-query group.

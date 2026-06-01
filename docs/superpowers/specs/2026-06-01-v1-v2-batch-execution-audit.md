# 2026-06-01 — V1/V2 state-API batch-execution audit (code-grounded confirmation)

Goal: confirm every state op batches at the forst-backend boundary (Flink may call
per-record under the sync operator model, but the backend must buffer + batch-dispatch
to the engine — no per-key sync FFI — and be zero-copy).

## V2 async backend (`ForStRsAsyncKeyedStateBackend` → `*StateV2`) — ✅ FULLY BATCHED
- PUT/GET/DELETE → `VectorizedExecutor` batches one FFM crossing per op-type over Arrow
  RecordBatch; writes stage in off-heap `MapStateArrowBuffer`/`ListStateArrowBuffer`.
- ListState add → `APPEND_MERGE` (one merge operand, no read).
- Iteration → chunked `frsVecIterPrefixOpen/Next` (N entries/crossing).
- Reducing/Aggregating → RMW-cache (read-once, reduce in-memory, batch flush).
Zero-copy: off-heap staging + borrowed-slice sinks + pinned reads.

## V1 sync backend (`ForStRsKeyedStateBackend` → `ForStRs{Value,Map,List,Reducing,Aggregating}State`)
Flink calls these per-record (sync operator model — IMMUTABLE, can't batch across
records). Per the directive, the backend must batch what it can WITHIN that model:

| State | Writes | Reads | Iterate | Status |
|---|---|---|---|---|
| **ValueState** | off-heap `statebuf` → **batch flush** | zero-copy `getPinnedSegment` | n/a | ✅ batched + zero-copy |
| **MapState** | off-heap `statebuf`/writeCache → **batch flush** | zero-copy `getPinnedSegment` | **chunked drain** (FIXED 2026-06-01) | ✅ batched + zero-copy |
| **ListState** | `add`/`addAll` = **APPEND-MERGE** (frsVecMergeAppend, no read) — FIXED 2026-06-01 | merged concat-decode | n/a | ✅ batched (merge-append, verified round-trip test) |
| **ReducingState** | `add` = **RMW** (sync `lookupKv`+`putSegment` per record) | byte[] | n/a | ❌ **GAP — per-record sync RMW, no accumulator cache** |
| **AggregatingState** | `add` = **RMW** (sync `lookupKv`+`putSegment` per record) | byte[] | n/a | ❌ **GAP — per-record sync RMW, no accumulator cache** |

Production wiring confirmed: ValueState (backend:661) + MapState (backend:805) get the
off-heap `ArrowBinaryBuffer`; the non-statebuf MapState is only a `-Dforst.rs.mapstate.legacy`
diagnostic (default OFF). ListState (716) + Reducing (896) + Aggregating (937) get NO
off-heap buffer.

## The gaps + fixes
1. **V1 ListState** — `add` should append-merge (the FFI `frsVecMergeAppend` exists; V2
   ListState already uses APPEND_MERGE), eliminating the per-add read-modify-write of the
   whole list. Needs `add`→merge-operand + `get`→decode-concatenated-operands (mirror
   `ForStRsListStateV2`) + the engine list-append operator on the CF. Correctness-sensitive
   (operand format + merge resolution) → TDD.
2. **V1 Reducing/Aggregating** — the reduce/agg function is JAVA-side, so the engine cannot
   merge operands; RMW is inherent. But it can be BATCHED via an accumulator cache (read the
   accumulator once per key, apply reduces in-memory across records, flush in batch) — the
   same pattern V2 uses (RMW-cache P7). Currently each record does a sync `lookupKv`+`putSegment`.

## Honest bottom line
- **V2: fully batched/vectorized/zero-copy.** ✅
- **V1 ValueState + MapState: batched + zero-copy** (MapState iteration vectorized this
  session). ✅
- **V1 ListState + Reducing + Aggregating: per-record sync RMW — NOT batched.** ❌
  These are the remaining gaps to close for the "all batched" bar. They're hit only by
  operators Flink routes to the SYNC backend (async-incapable operators); async-capable
  operators already use the fully-batched V2 path.

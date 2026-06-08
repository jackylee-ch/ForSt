# forst-rs async-state parallel + zero-copy architecture upgrade (8c/32g)

**Date:** 2026-06-07
**Status:** Designed; awaiting user spec review → writing-plans.
**Goal gate:** every NexMark query, forst-rs-local under **8c/32g Docker** must be (≥0.8× rocksdb OR
≤+50s) **AND faster than ForSt-Java**, with accuracy. Ideal: ≥1.0× per query. Multiple architecture
modules upgraded **together**. All verification independent (no pending/interactive). Correct backend +
FORSTRS timer per run; no shortcuts.

## Problem (code-confirmed across forst / forst-rs / rocksdb, q19/q20)
q19 (TopN) and q20 (stream join) are per-record state-**read**-heavy. Under 8c/32g, rocksdb (157/99.8/
402s) and ForSt-Java (q19 170, q11 127) stay flat; **only forst-rs decays** (rate 759k→24k/s; q19 462s
lz4). State is small (~1.3GiB) and the engine LSM/read-amp/memory-model are already fine — so the gap is
the **async-state execution architecture**, in four compounding sources:

1. **Synchronous, inline, no-overlap executor (PRIMARY).** The live `VectorizedExecutor`
   (`ForStRsAsyncKeyedStateBackend:1132`) runs all gets/iters/puts **inline on the mailbox thread within
   one turn** and returns `CompletableFuture.completedFuture(null)` (line 543). No coordinator, **no
   read-IO thread pool**, no deferred future. ForSt offloads to a coordinator + **`read-io-parallelism`
   (default 3) read threads** + write threads and returns a *deferred* future → it overlaps the next
   batch and uses multiple cores per subtask for state reads. forst-rs serializes all state work onto the
   one mailbox thread → throughput ceiling + off-CPU stalls.
2. **Iterators bypass the batched vectorized path (SECONDARY).** A vectorized `dispatchIterPrefix`
   (batched FFI) exists, but q19's MAP_ITER falls to the per-request `ForStRsDBIterRequest.process()`
   loop (open+next+close FFM crossings, one iterator at a time).
3. **Per-op FFM-crossing + per-record allocation (CONTRIBUTING).** `byte[]`/`byte[][]` pervade the hot
   path (`ForStRsLinker` 80 refs/14 `new byte[]`; get carries `byte[] serializedKey` + completes
   `byte[] rawValue`; `executeGets` builds `byte[][]`). Per-record Java garbage → GC pressure (the
   smaller-heap config regressed, consistent with GC).
4. (Already fixed) uncompressed SSTs → **lz4** (−36%, keep as default).

## Design — four modules, implemented together, verified incrementally

### M1 — Parallel + overlapped StateExecutor (mirror ForSt)
- Add to `VectorizedExecutor`: a **coordinator** + a fixed **read-IO thread pool** (size = config,
  default min(cores, 4) per backend, shared per slot to bound total threads vs the 8-core cap) + a write
  path. `executeBatchRequests` returns a **deferred** `CompletableFuture` completed when all sub-work
  finishes; the AEC mailbox overlaps the next batch.
- Fan out reads: gets via the existing batched multiget across pool threads; iters across pool threads.
- **Per-thread `Arena`**: the shared `Arena` is not safe for concurrent FFI alloc — each read-pool
  thread gets its own (thread-local) `Arena`/scratch; the engine handle (`FrsDb`/cf) is shared.
- **Engine concurrent-read safety (verify first):** confirm `DbImpl` read paths (`get_arc`,
  `build_lazy_prefix_key_stream`, reader cache `ArcSwap`, MVCC snapshot) are safe under concurrent
  reads on one instance (RocksDB-class invariant). Add engine tests asserting it before enabling the pool.
- **Ordering/drain correctness:** preserve the existing progress-counter + drain-on-throw contract under
  concurrency (per-request completion stays ordered as the AEC requires; failures drain remaining tails
  exceptionally).

### M2 — Unified batched/vectorized iterator dispatch
- Route **all** ITER_PREFIX requests (incl. MAP_ITER / q19) through `dispatchIterPrefix`
  (`frs_vec_iter_prefix_open_batch`) — one FFI call per batch, not per iterator. Remove the per-request
  `process()` hot path (keep only as a correctness fallback if a request can't be batched).
- Combine with M1 so batched iters also run on the read pool.

### M3 — Zero-copy hot path (no byte[]/byte[][], no per-record/key/op allocation)
- **Mandate:** eliminate `byte[]`/`byte[][]` and per-byte/key/op/record heap allocation on the
  per-record path (get/put/iter + Value/Map/List/Reducing/Aggregating state). Cold/setup paths
  (CF create, checkpoint/restore — once per CF/ckpt) are out of the per-record scope but audited.
- **Keys:** serialize directly into a reused off-heap `MemorySegment` (the existing `ArrowBinaryBuffer`/
  statebuf mechanism) — no `byte[] serializedKey` per request; the FFI takes (segment, offset, len).
- **Values/results:** return results as `MemorySegment` views over the FFI chunk buffer (decode via
  offset-based `DataInputView`); no `byte[] rawValue` per get. The MapState iter zero-copy decode
  (`RawRowConsumer` over reused scratch) is the template — extend it to get/put and the V2 batch paths.
- **Linker:** add segment-based FFI variants for the hot calls; retire the `byte[]`/`byte[][]` overloads
  from the per-record path.
- **Audit gate:** `grep -c 'byte\[' + 'new byte\['` on the hot-path files must reach 0 for the
  per-record path; a test/CI check asserts it.

### M4 — Per-op FFM-crossing + GC reduction
- One FFI crossing per request *type* per batch (multiget already batched; batch puts; batch iters via
  M2). Reuse per-thread scratch segments; avoid per-record request/future object churn where the AEC
  contract allows (object pooling for request wrappers if profiling shows it).

### Engine support (forst-rs-engine / forst-rs-ffi)
- Concurrent-read verification + any read-path locking fixes (M1).
- Segment-in/segment-out FFI entry points (M3) — zero-copy across the Panama boundary.
- Keep lz4 default for 8c/32g.

## Correctness & risk
This is the correctness-critical state hot path. Risks: concurrency hazards (ordering, races on shared
engine structures), offset/length errors in zero-copy decode, Arena lifetime. Mitigations: verify engine
concurrent-read with tests BEFORE enabling the pool; keep the ordered-completion + drain contract; the
zero-copy decode reuses the proven offset-based `setBuffer` API; per-thread Arenas with asserted lifetime.

## Verification protocol (independent; per module; gate before stacking the next)
1. **Unit tests:** `forst-rs-engine` + `forst-rs-storage` suites green (currently 378 storage + engine
   all pass); new tests for engine concurrent reads (M1), segment FFI round-trip (M3), batched-iter
   equivalence (M2), executor ordering/drain under concurrency (M1).
2. **Java backend tests:** MapState/Value/List/Aggregating suites green (95/95 MapState today).
3. **e2e accuracy:** 8c/32g NexMark — q11/q12/q15 finish with EXACT 4.6M/92M counts; q19/q20/q9/q4
   output byte-equivalent to rocksdb (controlled replay where needed). No DNF.
4. **Performance:** 8c/32g Docker best-of-3 (NexMark variance), rate trajectory (decay must flatten),
   wall vs targets (beat ForSt-Java + rocksdb 0.8×/+50s). Engine microbench unchanged-or-better.
5. **Zero-copy audit:** hot-path `byte[]` count = 0.
6. **Disk:** host ≥400GB after tests (disk-lean config for q9/q4; compact Docker.raw between large runs).

## Sequencing (all modules this campaign; measured incrementally)
M1 (executor parallel+overlap) → measure (proves PRIMARY) → M2 (batched iters) → measure → M3 (zero-copy)
→ measure → M4 (FFM/GC) → measure. Each lands only when its UT+e2e+perf gates pass. Then full q0–q22
8c/32g sweep + total vs rocksdb/forst, accuracy-verified, before Phase 2.

## Out of scope (this spec)
S3/remote perf (Phase 3). Phase-2 Disaggregated-State feature work (gated on this closing the per-query
bar). Engine LSM/compaction algorithm changes (state is small; not the bottleneck here).

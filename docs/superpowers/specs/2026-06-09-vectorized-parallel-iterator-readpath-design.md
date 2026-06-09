# Vectorized + Parallel Iterator Read Path — join family (q7/q9/q20)

**Date:** 2026-06-09
**Status:** Approved (implement + verify in one pass).
**Scope:** forst-rs engine (`~/Code/stczwd/ForSt`) + forst-rs backend (`flink-statebackend-forst-rs`).

## Root cause (code-confirmed, profiler-corroborated)
Point-gets are vectorized: `executeGets` (VectorizedExecutor.java:1203) coalesces K gets into ONE
`frs_vectorized_batch_get` FFI crossing. **Iterators are NOT:** `executeIters` (:1369) runs a serial
`for` loop, each probe calling `iter.process()` = a separate `frsVecIterPrefixOpen`/`Next` FFI crossing
+ a separate engine `build_lazy_prefix_key_stream` (version derive + overlapping-SST locate + per-SST
reader loop). Joins (q7/q9/q20) are iterator-dominated, so the whole join workload runs **one record at
a time, serially** — violating the batch-only / no-per-record mandate. Profiler corroboration (post
block-cache fix, q20 @87M): `probe≈5–18µs`, cost in `sstloop`, `n_ovl=1`, `B_resident=0` — per-probe
build paid per record, serially; read-amp and re-decompress already eliminated.

ForSt (C++) has the same per-record iterator shape BUT offloads to a coordinator + parallel read
threads (`read-io-parallelism=3`); forst-rs runs them serially on the single coordinator thread.

## Design: one batched primitive, coalesced + parallel INSIDE the engine
Keep the Java coordinator single-threaded and simple; put coalescing AND parallelism in Rust, where the
data structures are already concurrent (`sst_readers` = `ArcSwap`, block cache = sharded, version
snapshot = immutable). This avoids the Java `RoutingStateExecutor` thread-confinement race class
(the `iterView`/shared-per-subtask-buffer bug).

```
Java executeIters(K probes)  ── ONE FFI ──▶  frs_vectorized_batch_iter_prefix(db, cf, prefixes[K], ...)
Rust batch_prefix_scan(prefixes[K]):
   pin version ONCE (shared)  ·  reuse sst_readers + block cache
   K probes fan out across a bounded read pool (rayon), each independent + read-only:
        build_lazy_prefix_key_stream(prefix) + drain  →  per-probe entries
   pack K result sets columnar with per-probe offsets  →  return to Java
```

### Layer 1 — Coalesce
- New FFI `frs_vectorized_batch_iter_prefix`: all K prefixes in ONE crossing (mirrors `batch_get`),
  replacing the per-probe `frsVecIterPrefixOpen`/`Next` loop. Eliminates the per-probe
  `allocPrefixSegment` copy (prefixes packed once) and K-1 FFI crossings.
- Engine pins the version snapshot ONCE for the batch (today each probe re-derives it); shares the
  reader-cache / overlapping-SST locate across probes hitting the same SSTs (common in joins).

### Layer 2 — Parallelize
- The K probes are independent reads on a pinned immutable snapshot → fan out over a bounded engine
  read pool (size = `read_io_parallelism`, default `min(cores,4)`, matching ForSt). Thread-safety is
  free: lock-free `sst_readers`, sharded block cache, immutable version.

### Result buffer / continuation
- The engine writes all K probes' rows into a single result region with a per-probe offset table
  (`offsets[K+1]`) so Java slices each probe's bytes zero-copy (the existing `IteratorEntryView` chunk
  layout, generalized to K segments). Probes whose output exceeds the batch buffer cap carry a
  per-probe continuation handle (rare for joins; bounded match windows) — drained on a follow-up call,
  preserving the existing soft-cap semantics per probe.

## Correctness (GATE)
- Engine UT: `batch_prefix_scan(prefixes)` returns **byte-identical** entries (key+value+seq+op order)
  to K serial `prefix_scan` calls, for overlapping/disjoint prefixes, across memtable+imm+SST tiers.
- Engine UT: parallel vs serial execution of the batch yields identical results (determinism on a
  pinned snapshot).
- Per-probe → `StateRequest` future mapping preserved (results returned in request order).
- e2e: q7/q9/q20 `out_rows` / final result identical to RocksDB on seeded input (join accuracy gate).
- Config UNCHANGED (noflush=false, write_buffer_size=1G). Pure read-path architecture.

## Verification (before/after, recorded in 2026-06-08-8c32g-3backend-sweep-results.md)
- q7/q9/q20 wall + finish vs RocksDB and ForSt 8c/32g.
- Confirm q16/q17/q18 (passing set) do not regress.
- Both repos' GHA pipelines green.

## Phasing (each independently verifiable + committable)
- **A. Engine — DONE (committed `7ae8876ed`).** `batch_prefix_scan_parallel(self: &Arc<Self>, cf,
  prefixes: &[&[u8]]) -> Vec<Result<Vec<(k,v)>>>` fans the K probes across a new process-global read
  pool `bg_read_pool` (`min(cores,4)`, env `FRS_RS_READ_IO_PARALLELISM`). Per-probe error isolation,
  input order, value-carrying owned scan. 2 UTs prove byte-identical to serial `prefix_scan`
  (memtable+SST tiers, overlapping/disjoint/empty); 282 engine UTs green. (Note: distinct from the
  pre-existing SERIAL `batch_prefix_scan` at db.rs:6836, which the `frs_batch_prefix_scan` FFI uses.)
- **B. FFI — TODO.** `frs_vectorized_batch_iter_prefix(db, cf, prefixes_off[n+1], prefixes_data, n,
  out_handle*) -> i32`: calls `batch_prefix_scan_parallel`, stashes the `Vec<Result<Vec<(k,v)>>>` behind
  an opaque handle in the FFI registry (engine-owned, like the iter handles). Companion accessors:
  `frs_batch_iter_probe_count(h)`, `frs_batch_iter_probe_chunk(h, i, chunkBuf, cap, outRow, outBytes)`
  (zero-copy view per probe into the engine-owned bytes, reusing the `IteratorEntryView` chunk layout),
  `frs_batch_iter_close(h)`. Round-trip test in `forst-rs-ffi`.
- **C. Java — TODO.** Rewrite `executeIters` (VectorizedExecutor:1358): collect all `iterRequests`
  prefixes → ONE `frsVectorizedBatchIterPrefix` crossing → for each probe `i`, hand its chunk view to
  the matching `ForStRsDBIterRequest.completeWithEntries` (reuse the existing zero-copy `VIEW_TL`
  decode) → complete its `StateRequest` future, in request order. Preserve the soft-cap/continuation
  contract per probe (bounded join windows make single-drain the common case). Add the linker binding
  + `ForStRsLinker` MethodHandle (mirror `frsVecIterPrefixOpenBatch`).
- **D. e2e — TODO.** Rebuild Linux `.so` + jar → q7/q9/q20 accuracy (`out_rows`/final-result ==
  RocksDB) + perf (before/after) → regression-check q16/q17/q18 → both repos' GHA green → record in
  `2026-06-08-8c32g-3backend-sweep-results.md`.

**Status 2026-06-09:** Phase A complete + verified + pushed. B/C/D are the wiring that makes q7/q9/q20
actually use the parallel path; they form the next implementation pass (TDD per phase, e2e at D).

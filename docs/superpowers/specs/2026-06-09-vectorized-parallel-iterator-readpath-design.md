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
- **B. FFI — DONE (committed `1f4c5cdc2`).** Chose the SIMPLER design that reuses the existing
  handle/chunk ABI + Java drain machinery rather than a new handle+accessor transport:
  `frs_vec_iter_prefix_open_batch_parallel` — identical ABI to the serial `frs_vec_iter_prefix_open_batch`
  (K prefixes SoA → K handles + K first chunks, drained via the existing `frs_vec_iter_prefix_next`/
  `_close`), but builds+drains the K probes via `batch_prefix_scan_parallel`. Invalid descriptors
  validated serially + skipped without scanning; heavy build/drain parallel, unsafe pointer writes
  serial; owned results wrapped as `IterHandle` (`IterKey/Value::Vec`). Round-trip UT proves
  byte-identical to the serial batch open (memtable+SST, 4×3 rows, unique handles); 99 FFI tests pass.
- **C. Java — TODO (atomic with D + .so/jar).** (1) Bind `frs_vec_iter_prefix_open_batch_parallel` in
  `ForStRsLinker` (mirror `frsVecIterPrefixOpenBatch` — field + `bind()` + invoke wrapper). (2) Route the
  join's iterator probes through the batched-parallel open. Two options, decide by decode-compatibility:
  (a) make `dispatchIterPrefix` call the parallel binding (one-line) AND re-classify the join's MapState
  iteration to `IterPrefixRequest` (VectorizedClassifier:653→445) — IF `IterPrefixRequest`'s decode is
  value-carrying + matches `ForStRsDBIterRequest.completeWithEntries`/`VIEW_TL`; or (b) rewrite
  `executeIters` (VectorizedExecutor:1358) to batch-open all `iterRequests` prefixes via the parallel
  binding, then drive each `ForStRsDBIterRequest`'s drain from its handle + first chunk (needs a
  `processFromBatchedHandle` variant that decodes the pre-drained first chunk then continues via `_next`).
  **HIGH BLAST RADIUS:** `executeIters` serves ALL MapState iteration (q3/q11/q12/q15/q16/q19 + joins) —
  any bug regresses many queries, so this MUST be done with full e2e correctness verification, and the
  linker `bind()` MUST ship atomically with a freshly-built `.so` (a jar bound to a missing symbol throws
  at class-load → breaks every query).
- **D. e2e — TODO (with C).** Rebuild Linux `.so` (has A+B symbols) + jar → q7/q9/q20 accuracy
  (`out_rows`/final-result == RocksDB) + perf (before/after) → regression-check q3/q11/q12/q15/q16/q17/q18
  → both repos' GHA green → record in `2026-06-08-8c32g-3backend-sweep-results.md`.

**Status 2026-06-09:** Phases A (engine `batch_prefix_scan_parallel`) + B (FFI
`frs_vec_iter_prefix_open_batch_parallel`) COMPLETE — both verified byte-identical to serial, committed,
pushed; the parallel infrastructure is built. C+D (the Java wiring + e2e) is the remaining atomic pass
that makes q7/q9/q20 actually use it — deferred because `executeIters` is high-blast-radius and the
linker binding must ship with the `.so`+jar+e2e together (correctness is non-negotiable). The clean A+B
checkpoint carries ZERO deployment risk (engine/FFI only, no jar/binding changes).

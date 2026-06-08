# forst-rs async-state parallel + zero-copy — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make forst-rs-local beat ForSt-Java and rocksdb on the read-heavy NexMark queries (q9/q11/q19/q20) under 8c/32g by parallelizing + overlapping the async-state executor, batching iterators, and eliminating per-record `byte[]`/allocation — while preserving accuracy.

**Architecture:** Spec `docs/superpowers/specs/2026-06-07-forst-rs-async-state-parallel-zerocopy-design.md`. Root cause: the live `VectorizedExecutor` runs all state ops inline on the mailbox thread and returns a completed future (no read-IO pool, no overlap). ForSt offloads to a coordinator + read-IO pool + deferred future. Four modules, sequenced **M1→measure→M2→measure→M3→measure→M4→measure**; each lands only when its UT + e2e exact-count + 8c/32g perf gates pass.

**Tech Stack:** Rust engine (`forst-rs-engine`/`-storage`/`-ffi`, cargo, `#![forbid(unsafe_code)]` in storage), Java 25 Panama FFM backend (`flink-statebackend-forst-rs`, maven), NexMark on Flink 2.2.1, 8c/32g arm64 Docker (`scripts/run-8c32g.sh`).

**Decisions locked:** read-IO pool **shared per slot**, default `min(cores,4)`, env-overridable (`FRS_RS_READ_IO_PARALLELISM`). Compression lz4 default for 8c/32g. Targets to beat (8c/32g): q19 ForSt 170s / rocksdb 157s; q11 127/99.8; q20 rocksdb 402. forst-rs lz4 baseline: q19 462, q11 231.

**Status:** M1 engine concurrent-read gate DONE — `crates/forst-rs-engine/tests/concurrent_reads_it.rs` (2 tests green: concurrent `get_arc` + `batch_get` on one `Arc<DbImpl>` correct + panic-free).

---

## Module M1 — Parallel + overlapped StateExecutor

**Why:** primary lever. Offload the batch's engine work off the mailbox thread onto a shared read-IO pool, return a deferred future so the AEC overlaps the next batch, and replicate the executor's reused scratch + `Arena` per pool thread (the current scratch is single-instance, assumes serial execution).

**Files:**
- Create: `flink-state-backends/flink-statebackend-forst-rs/src/main/java/org/apache/flink/state/forstrs/exec/ReadIoPool.java` (shared-per-slot pool + per-thread `Arena`/scratch holder)
- Create: `.../exec/PerThreadDispatchScratch.java` (holds the segments currently single-instance in `VectorizedExecutor`: `outDataLenSeg`, `scratchOpsOffsets`, `scratchHeapFutures`, `scratchValueSlices`, iter scratch — one instance per pool thread)
- Modify: `.../VectorizedExecutor.java` (executeBatchRequests → deferred future on the pool; dispatch uses the calling thread's `PerThreadDispatchScratch` + `Arena`)
- Modify: `.../keyed/ForStRsAsyncKeyedStateBackend.java:1132` (pass the shared `ReadIoPool` into `VectorizedExecutor`)
- Test: `.../test/java/org/apache/flink/state/forstrs/VectorizedExecutorParallelTest.java`
- Test (engine, DONE): `crates/forst-rs-engine/tests/concurrent_reads_it.rs`

- [ ] **Step 1 — Failing test: deferred future + correctness under a pool.** In `VectorizedExecutorParallelTest`, build a classifier with K mixed get/iter/put requests against a populated CF, run `executeBatchRequests` with a 4-thread `ReadIoPool`, assert: (a) the returned future completes (not pre-completed — assert it can be pending), (b) every get/iter result equals the serial-executor result, (c) puts are visible after completion. Use 8 concurrent batches to force pool concurrency.

- [ ] **Step 2 — Run, verify it fails** (`ReadIoPool`/per-thread scratch absent): `cd flink-state-backends/flink-statebackend-forst-rs && JAVA_HOME=$JDK25 ../../mvnw -o -q test -Dtest=VectorizedExecutorParallelTest` → FAIL (compile/behavior).

- [ ] **Step 3 — Implement `PerThreadDispatchScratch`**: move every reused scratch field from `VectorizedExecutor` (the `outDataLenSeg`, `scratchOpsOffsets`, `scratchHeapFutures`, `scratchValueSlices`, iter-dispatch scratch, plus a per-thread `Arena` created from the slot scope) into this class; one instance per pool thread (`ThreadLocal` keyed to the pool, or an array indexed by worker id).

- [ ] **Step 4 — Implement `ReadIoPool`**: a fixed `ExecutorService` (size `min(cores,4)`, env `FRS_RS_READ_IO_PARALLELISM`), each worker owning a `PerThreadDispatchScratch`; a `submit` that runs a batch's dispatch on a worker and completes the batch future; shared per slot (one pool injected via the backend).

- [ ] **Step 5 — Rewire BOTH executor entry points through a single coordinator (CORRECTNESS-CRITICAL).** The AEC runs on one mailbox thread but, with a deferred future, can call `executeRequestSync` (line 856) while a batch future is still in flight — both share the executor's Arena/scratch → race. Therefore route **both** paths through ONE coordinator thread (single-thread `ExecutorService`, per slot): `executeBatchRequests` submits the existing dispatch body to the coordinator and returns that (deferred) future; `executeRequestSync` submits to the coordinator and **blocks awaiting** (preserves its synchronous void contract + ordering). All Arena/scratch/engine access stays confined to the coordinator → no race, scratch stays single-instance (no per-thread replication needed for this increment). Within-batch read fan-out across a read-IO pool is a *follow-up* increment (only if M1-coordinator alone doesn't reach the target). Keep ordered per-request completion + drain-on-throw intact. Shutdown: drain + await coordinator before `arena.close()`.

> **Increment note:** This pins M1 to the ForSt-equivalent "serial batch order + mailbox overlap" first (low risk: scratch stays single-threaded on the coordinator). The read-IO *pool* + `PerThreadDispatchScratch` (Steps 3–4) become the second M1 increment, gated on the coordinator measurement — so `ReadIoPool` may start as a 1-thread coordinator and grow to a fan-out pool only if needed.

- [ ] **Step 6 — Run the parallel test, verify PASS**: same command as Step 2 → PASS.

- [ ] **Step 7 — Regression: full backend unit suite** `../../mvnw -o -q test` (MapState/Value/List/Aggregating) → all green (no concurrency regressions).

- [ ] **Step 8 — Engine gate re-confirm**: `cargo test -p forst-rs-engine --test concurrent_reads_it --release` → 2 passed.

- [ ] **Step 9 — Build + deploy**: `scripts/run-8c32g.sh jar` (host maven) + `scripts/run-8c32g.sh build` (Linux .so unchanged for M1, but rebuild to be safe).

- [ ] **Step 10 — e2e accuracy (8c/32g)**: `FRS_SST_COMPRESSION=lz4 scripts/run-8c32g.sh run q11 forst-rs-ffm-local 700 m1acc` then q12, q15 → all FINISH with exact 92M src_out (MapState exact-count proxy); spot-check q19 output count.

- [ ] **Step 11 — Perf measure (8c/32g, best-of-3)**: q19 + q20 + q9 + q11 forst-rs-ffm-local lz4; record wall + rate trajectory (decay must flatten). Compare to ForSt/rocksdb targets. **Gate: q19 materially below 462 (toward ≤170).**

- [ ] **Step 12 — Document + (ask before) commit**: append M1 result to the spec + memory; do NOT commit/push unless the user asks.

**M1 measurement decision:** if q19/q20 now beat ForSt → proceed to M2 for the remaining gap; if improved but short → M2+M3 needed (expected).

---

## Module M2 — Unified batched/vectorized iterators (plan expanded after M1 measure)

**Why:** q19 MAP_ITER uses the per-request `ForStRsDBIterRequest.process()` loop, not the batched `dispatchIterPrefix`. Route all ITER_PREFIX through the batched FFI (one crossing/batch), running on the M1 pool.

**Tasks (to detail post-M1):** (1) failing test: classifier routes MAP_ITER into the ITER_PREFIX batch buffer; (2) classifier change to populate `ipBuf` for MAP_ITER; (3) ensure `dispatchIterPrefix` handles the continuation/`existingVecHandle` semantics MAP_ITER needs; (4) backend suite green; (5) e2e exact-count q11/q12/q15; (6) 8c/32g perf q19/q9/q20 best-of-3; (7) document.

---

## Module M3 — Zero-copy hot path: eliminate byte[]/byte[][] (plan expanded after M2 measure)

**Why (user mandate):** no `byte[]`/`byte[][]` or per-record/key/op allocation on the hot path. Current: `ForStRsLinker` 80 `byte[]` refs (14 `new byte[]`); get carries `byte[] serializedKey` + completes `byte[] rawValue`; `executeGets` builds `byte[][]`; several state primitives allocate per-op.

**Tasks (to detail post-M2):** per hot-path family (get, put, value, map, list, reducing, aggregating): (1) failing test asserting no per-op allocation + correctness; (2) add segment-in/segment-out FFI linker variants; (3) serialize keys into reused off-heap `MemorySegment` (extend `ArrowBinaryBuffer`/statebuf); (4) return results as `MemorySegment` views (offset-based `DataInputView`, the `RawRowConsumer` template); (5) retire the `byte[]` overloads from the per-record path; (6) **audit gate**: hot-path `grep -c 'byte\['` + `'new byte\['` = 0; (7) backend suite + e2e exact-count + 8c/32g perf.

---

## Module M4 — Per-op FFM crossing + GC reduction (plan expanded after M3 measure)

**Tasks (to detail post-M3):** (1) confirm one FFI crossing per request-type per batch; (2) reuse per-thread scratch (done in M1); (3) profile (JFR) for residual per-record Java garbage; (4) pool/avoid per-record request-wrapper objects where the AEC contract allows; (5) backend suite + e2e + 8c/32g perf.

---

## Final gate (after M1–M4)

- [ ] Full q0–q22 8c/32g sweep, forst-rs-ffm-local (lz4) vs rocksdb vs forst-local, all FINISH, accuracy-verified (output == rocksdb / exact counts).
- [ ] Per-query bar met: each query ≥0.8× rocksdb OR ≤+50s, AND faster than ForSt-Java.
- [ ] Total forst-rs < rocksdb. Host ≥400GB free (compact Docker.raw + clean).
- [ ] Document final table; then Phase 2 (Disaggregated State) is unblocked.

---

## Self-review notes
- **Spec coverage:** M1–M4 + final gate cover all spec modules + verification + disk. M2–M4 are intentionally task-outlined (measure-gated; each expanded into its own concrete plan after the prior module's 8c/32g result, per scope-decomposition — a faithful fully-coded plan for M3's many files would be speculative before M1/M2 land).
- **No placeholders in M1:** M1 tasks have exact files, test intent, and commands. M1 is a complete, testable improvement on its own.
- **Type consistency:** `ReadIoPool`, `PerThreadDispatchScratch`, `FRS_RS_READ_IO_PARALLELISM` used consistently.

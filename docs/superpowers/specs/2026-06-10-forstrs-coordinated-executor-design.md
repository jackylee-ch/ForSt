# forst-rs Coordinated Async Executor — q7/q9/q20 architecture design (2026-06-10)

Status: APPROVED design (user-approved approach (a): staged full program).
Scope: flink-statebackend-forst-rs (Java) + forst-rs engine (Rust, Stage 2 only).
Supersedes the opt-in `RoutingStateExecutor` path as the road to default; subsumes
OPT-01 (universal) and OPT-02 from `2026-06-09-forstrs-join-performance.md`.

## 1. Problem (same-session 8c/32g evidence, sweep doc 2026-06-08)

| q  | frs default | frs opt-in parallel | RocksDB | ForSt  | bar (Phase 1)            |
|----|-------------|---------------------|---------|--------|--------------------------|
| q7 | 1441.6s     | 1052s               | DNF     | 586.8s | < 586.8s                 |
| q9 | DNF (84.5M@1204) | DNF (79.2M@1284) | 1420.5s | DNF    | ≤ 1776s (0.8× RDB)       |
| q20| DNF         | 1610s EXACT         | 859.7s  | 1535.9s| ≤ 1074s (0.8× RDB)       |

Query shapes: q7 = regular join keyed on price (bid × windowed-max, huge per-key
iterate volume); q9 = interval join (auction×bid) + ROW_NUMBER Top-1 rank;
q20 = regular join bid×auction (key = auction id), 40M-key state probes.

Note: ForSt ALSO DNFs q9 — only sync RocksDB finishes it. The executor model is
necessary but provably not sufficient for q9.

## 2. Causal model

### 2.1 Shared root cause: the execution model (async in API only)

forst-rs default path executes every read inline on the mailbox thread:
- `VectorizedExecutor.executeBatchRequests` returns `CompletableFuture.completedFuture(null)`
  (depth-1 synchronous dispatch).
- `fullyLoaded()` hard-coded `false` → AEC admits unbounded in-flight records.
- JFR (q19/q9): 65,842 thread-parks vs 6,419 on-CPU samples; mailbox
  `processMailsWhenDefaultActionUnavailable` dominates.

ForSt (`flink-statebackend-forst/.../ForStStateExecutor.java`) differs on four axes:
1. **Non-blocking coordinator**: single coordinator thread; `executeBatchRequests`
   returns an INCOMPLETE future immediately (:149-232); mailbox never waits.
2. **Read pool** (`read-io-parallelism`=3): GETs chunked across workers, each chunk
   one coalesced `db.multiGetAsList` (`ForStGeneralMultiGetOperation.java:80-152`);
   key serialization happens ON the worker (`buildSerializedKey` :94), off the mailbox.
3. **Iterators = independent parallel worker tasks** (`ForStIterateOperation.java`,
   drain chunk 128) — this is OPT-02's exact blueprint.
4. **Real backpressure**: `fullyLoaded() = ongoing ≥ readThreadCount` (:289-291)
   → AEC admission control engages; in-flight memory bounded.

Our own opt-in spike already proved the payoff shape (q11 2.35×, q20 DNF→1610
EXACT, q7 1441→1052) but blocks the mailbox per batch via CountDownLatch
(`RoutingStateExecutor`), which is exactly what robbed q17 3.5× and blocked
default-on. ForSt never blocks.

### 2.2 Cache race (the q8 corruption cause)

`ForStRsMapStateV2.asyncGet` (~:353-398) does the cache lookup on the mailbox but
populates via `.thenApply(putIfAbsent)` on the completing/worker thread → the
single-threaded `MapStateCache` corrupts under any parallel executor
(q8 3,064,667 → 2,704,710 under shared cache; exact under cache-off).
Synchronizing it was REFUTED (committed e4d487df177, reverted 8e5a057da48): the
q17 regression was the executor's block, not the lock — but a lock also can't
restore the lost mailbox/worker coherence cheaply.

### 2.3 Residuals beyond the executor
- **q9**: ForSt DNF too → per-record async-machinery constant cost on ~92M
  interval-join probes suspected; true-finish-time run (MAXSEC=2400, decay diag,
  RSS) in flight to size the gap and locate decay.
- **q20**: 1610s proven vs 1074s bar → state-size LSM read degradation at 40M keys
  (out_rows exact, L0≤3, RSS bounded — NOT a bug). Verify the join-probe prefix
  path actually consults SST bloom/key-range pruning (engine has blooms configured:
  forst-rs-common/src/config.rs, flush.rs, compaction.rs; reader has block/range
  pruning sst/reader.rs:714-719).

## 3. Design

### 3.1 Stage 1 — `ForstRsCoordinatedStateExecutor` (new DEFAULT)

Port the ForSt architecture, adapted to FFM/vectorized strengths:

- **Coordinator**: single thread per backend instance. `executeBatchRequests`
  classifies the container into GET / ITER / WRITE lists and returns an
  incomplete future; completion via combine-all on the coordinator.
- **GETs**: split by key-group affinity (`kg % W`, stable for the backend's
  lifetime) into ≤W sub-batches; each worker chunk issues ONE engine vectorized
  `batch_get` FFM call (engine already coalesces by SST internally — stronger
  than ForSt's multiGetAsList). Key serialization on the worker.
- **ITERs**: each iter request dispatched as its own task on the read pool
  (= OPT-02), keeping today's chunked zero-copy drain
  (`decodeChunkDirect`/`completeFromDecoded`).
- **WRITEs**: inline on the coordinator (ForSt `isWriteInline` model); writes are
  already batched WriteBatch appends.
- **Backpressure**: `fullyLoaded() = ongoing ≥ readThreadCount`; read pool size
  default 3 (`FRS_RS_READ_IO_PARALLELISM`), matching ForSt.
- **Kill-switch**: `FRS_RS_EXECUTOR=inline` restores depth-1 for A/B.

### 3.2 Stage 1 — worker-confined MapStateCache (lock-free by confinement)

Cache stays (load-bearing for q11/q12/q19 incl. the committed findRow fix) but:
- **One shard per worker**; key-group-affine routing ⇒ every key group's requests
  processed by exactly one worker thread ⇒ each shard touched by exactly one
  thread ⇒ no locks, no cross-thread `.thenApply` populate.
- **Mailbox sync-hit fast path REMOVED**: `asyncGet` always enqueues; the worker
  checks its shard before the engine call, populates after. The engine call is
  still avoided on hit — that's where the win lives.
- **Write-through coherence**: put/remove cache effects are routed to the owning
  worker's queue and applied BEFORE that worker's read chunk of the same batch.
  Coordinator sequences batches; Flink AEC key-accounting guarantees one
  in-flight record per key.
- **Correctness invariant**: same key → same key group → same worker → same
  shard, always in batch order. q8's breakage (key-agnostic routing + shared
  cache) is eliminated structurally, not by synchronization.

### 3.3 Stage 2 — engine direct-local-read fast path (VERIFIED 2026-06-10, promoted to co-primary)

Verification run (q9, 100M, MAXSEC=2400, decay diag + /proc CPU + 2× thread dumps;
full data in the sweep doc "q9 DEEP PROFILE" section) established:
- q9 true finish ≈ 2600s (cut at 2400s @ ~91M); LSM HEALTHY during decay (L0≤3).
- Box ~8% CPU, opendal pool near idle, join task threads 20% duty cycle →
  LATENCY-bound, not CPU/disk/compaction-bound.
- Stacks (all 4 join threads, both dumps): RUNNABLE inside one synchronous FFM
  downcall (frsVecIterPrefixOpen / vectorizedBatchGet) under
  `AsyncExecutionController.drainInflightRecords ← processWatermark` — every
  watermark forces a full inline SERIAL drain of pending probes.
- Engine: every non-resident block read = `handle.block_on(opendal read)`
  (`forst-rs-io/src/opendal_backend.rs:478-485,683,699`) — a tokio handoff
  round-trip per op EVEN ON LOCAL DISK. The write path had this same disease and
  was fixed by coalescing ~500× (:823-827); the read path is still per-op.
- Per-record cost ≈ 120µs (33K/s ÷ 4 threads). Throughput = 1/latency; decays as
  deeper state adds block reads per seek. Explains ForSt's q9 DNF (parallelism
  alone can't fix a high per-op floor) and RocksDB's finish (sync in-process
  block-cache reads, µs-class floor).

**Stage-2 lever — CORRECTED by the FRS_READ_AT_DIAG discriminator (2026-06-10):**
the "bypass block_on/tokio" theory is REFUTED for the local decay regime. The sync
local fast path already exists (`LocalFirstSstFile.read_at_impl` → `get_range_into`
pread, cached_fs.rs:769-789); the histogram on a 50M q9 run shows the latency lives
INSIDE that path: mean 146→191µs and cold(≥20µs) 24.9%→29.1% RISING through decay,
~72% of reads warm 1-5µs. Arithmetic: ~0.6 preads/record × ~182µs ≈ 110µs/record =
the measured floor. Verdict: **cold preads against the (Docker-VM) local disk once
state outgrows the container page cache** — read VOLUME × page-cache-miss, not
dispatch overhead. (50M q9 baseline for A/B: finishes 813.5s.)

**Stage-2 lever (corrected): reduce blocks read per probe** — in priority order:
(a) per-probe SST pruning on the prefix/iter path (bloom + tighter key-range skip
so a probe touches only SSTs that can contain the prefix — RocksDB's actual
advantage on q9/q20-class state); (b) block/chunk-cache effectiveness for the join
hot set (verify what fraction of these preads SHOULD have been block-cache hits);
(c) compaction shape (fewer overlapping tiers per probe). All engine-only.

Deferred follow-ups: decode-per-entry allocations; mailbox serialization; tokio
handoff on the genuinely-remote path (still real for S3/Phase-2, just not the
local-mode binder).

### 3.4 Estimated post-fix performance (A = Stage-1 executor, B = direct-local-read)

| q   | today        | A only        | A + B        | bar     |
|-----|--------------|---------------|--------------|---------|
| q7  | 1441.6s      | ~750-1000s    | ~500-700s    | <586.8s |
| q9  | ~2600s true  | ~1100-1500s   | ~700-1200s   | ≤1776s  |
| q20 | 1610s (par)  | ~1200-1450s   | ~700-1000s   | ≤1074s  |
| q11 | 264.8s       | ~134.7s (measured floor) | ≤134.7s | ≤132.6s |

A-only floors are MEASURED (opt-in parallel results); A improves on them
(non-blocking overlap + backpressure + worker serialization). B's magnitude is
estimated from the latency arithmetic; its direction is verified. GO on A+B.

## 4. Rollout, gates, PR slicing

- **PR-1**: executor skeleton (coordinator + pool + real fullyLoaded), cache
  DISABLED under it, env-gated OFF by default. Gates: q17 ≤ 85s (no robbing);
  q8 = 3,064,445 band; 528 native UTs; GHA green (both repos).
- **PR-2**: worker-confined cache shards. Gates: q11 exact 92M rows and
  ~135s-class time; q12/q19 no-regress.
- **PR-3**: flip default. Gate: same-session 3-backend re-run of
  q7/q9/q11/q17/q20 recorded in the sweep doc.
- **PR-4+**: Stage-2 engine levers per profiles, one lever per PR, each with
  before/after on its target query + no-regress on q17/q8/q3.
- Config untouched (noflush=false, write_buffer 1G — matches ForSt). Pure
  architecture; anything that robs another query is gated out by the PR gates.

## 5. Testing

- Unit: coordinator ordering/exception/shutdown; shard confinement (assert
  worker-thread-only access in debug); routing stability.
- Existing: 528 native-lib tests; MapStateCacheTest; iter zero-copy round-trips.
- Correctness gates per PR (above) + q5dbg inner-window-agg 60,218 as a cheap
  windowed-agg canary.
- All perf numbers recorded in
  `docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md`.

## 6. References
- `2026-06-09-forstrs-join-performance.md` (OPT master table; §4 depth-1 root cause)
- `2026-06-08-forst-rs-parallel-coalesced-readpath-design.md` (C1-C3 analysis)
- ForSt sources: `ForStStateExecutor.java`, `ForStGeneralMultiGetOperation.java`,
  `ForStIterateOperation.java` (flink-statebackend-forst)
- Memory: `project_session_resume_2026-06-10.md` (OPT-01 saga, three reverts)

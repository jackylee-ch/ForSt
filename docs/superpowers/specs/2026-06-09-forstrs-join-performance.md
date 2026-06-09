# ForStRS Join Performance — Diagnosis & Optimization (single document)

> **Status:** Diagnosis complete · 2026-06-09 · scope = join-heavy NexMark **q7 / q9 / q20** (+q4).
> **Method:** synthesis of the existing spec corpus (~38 specs) + ~30 memory facts + JFR full-stack
> profile, augmented by 5 targeted three-way code-path subagents (read / write / async-wait /
> data-layout / LSM-architecture). All `Priority` / `Status` fields are intentionally left blank for
> later re-prioritization. Every claim is tagged **[FACT]** (read in code / measured) or **[HYP]**
> (inferred, to-validate).
>
> **One-line answer:** forst-rs joins are slow because the default state executor dispatches each batch
> **synchronously inline on the Flink mailbox thread through a blocking FFM downcall at in-flight depth 1**,
> so the async-state pipeline (built for 6000 in-flight records) parks instead of pipelining. The LSM
> engine read is *not* the wall (it profiles cheap). On-CPU micro-opts (vectorization / zero-copy / SIMD)
> are real but **modest** (~20% differential on a wait-bound system); the dominant lever is **async-state
> request-completion concurrency**, exactly the axis on which RocksDB (synchronous) and ForSt (offloaded
> coordinator + read pool) both beat forst-rs.

---

## 1. Context & confirmed actual codebase structure

### 1.1 Correction to the task premise

The originating task prompt described **TPC-H** Q7/Q9/Q20 (Volume Shipping / Product Type Profit /
Potential Part Promotion, with `supplier × lineitem × orders × partsupp` topologies). **This is a
framing error.** This repo benchmarks **NexMark q0–q22** exclusively — there is no TPC-H anywhere
(`grep -ri tpch` over `scripts/`, `nexmark/` → empty; `run-8c32g.sh` runs `q4 q9 q19 q20`). The
NexMark q7/q9/q20 are auction/bid/person-stream queries, unrelated to the TPC-H joins of the same
number. The prompt's **per-record state-access thesis is still the right lens**; only the query
identities were wrong. This document analyzes the **actual NexMark join family: q7, q9, q20, q4**.

### 1.2 Confirmed trees & branches

| Layer | Path | Branch / form | Role |
|---|---|---|---|
| Flink (all 3 backends) | `/Users/lijunqing/Code/stczwd/flink` | — | `RocksDB*`, `ForSt*`, `ForStRs*` keyed-state integration |
| ForSt **C++** engine | `/Users/lijunqing/Code/stczwd/ForSt` | `main` | original ForSt LSM (read via `git show main:<path>`) |
| forst-rs **Rust** engine | `/Users/lijunqing/Code/stczwd/ForSt` | `forst-rs` (`crates/`) | the Rust rewrite under test |

(The prompt's `~/code/ztczwd/flink` is a typo; the real path is `…/Code/stczwd/flink`.) RocksDB's
engine is a build dependency, not in-tree; we compare it via its Flink backend + well-known
architecture (tagged **[ARCH]**).

### 1.3 Corrected baselines — the "regression" was an artifact of a missing baseline

8c/32g, 100M events, locked uniform config (`noflush=false`, `write_buffer_size=1024mb`, 6 GB global
WBM budget, true backpressure, SST-write coalescing, container-local `.so`, lz4). Source:
`2026-06-08-8c32g-3backend-sweep-results.md`.

| query | shape | RocksDB 8c | forst-rs 8c | ForSt 8c | corrected verdict |
|---|---|---|---|---|---|
| **q9** | join | **DNF ~94.5M @1283s** | DNF ~84.5M @1204s | (pending) | **≈0.95× = NEAR-PARITY.** RocksDB *also* DNFs q9 → the "0.59× failure" was comparing forst-rs to an **unmeasured** RocksDB. [FACT] |
| **q20** | join (output-amplifying) | **800.3s (finishes)** | DNF ~88.6M @1284s | DNF ~84M @1300s | forst-rs **now BEATS ForSt** (block-cache fix) but **fails the RocksDB bar (~0.59×)**. q20 is hard for *all* LSM backends; only RocksDB's mature engine finishes. [FACT] |
| **q7** | multi-way join | (pending 8c) | DNF @1300s (~78.5M) | (host 659) | read-amp/wait DNF; needs the same lever as q9/q20. [FACT] |
| **q4** | join + retract agg | 313.5s | 597.2s (1.9×) | (host 1217) | finishes, memory-bounded; out_rows 25.8M vs 177.6M is a **retract-changelog cadence** signal, needs final-result compare, not a confirmed wrong answer. [FACT] |

**Takeaway:** q9 is already near-parity; q20 beats ForSt; the genuine remaining gap is "match RocksDB's
finish time on the heaviest joins," not "recover a catastrophic regression."

---

## 2. Methodology overview

Per the chosen scope ("synthesize existing + targeted gaps"), this is **not** a literal 50-agent
sweep. The existing corpus already establishes the engine-side findings (block-cache bypass fix,
value-carrying range scan, lock-free memtable, refuted config levers). The fresh investigation
dispatched **5 targeted three-way subagents** to close the genuinely-uncovered gaps — primarily the
**Flink-backend** comparison and the **async-state wait path**, which prior specs had not mapped at
`file:line`:

| Agent | Perspective | Gap closed |
|---|---|---|
| SA-1 | Read path (A: vectorization/batch) | Three-way GET/iterator copy+alloc counts, sync-JNI vs async-JNI vs async-FFM |
| SA-2 | Write path (B: memory/zero-copy) | Three-way put/WriteBatch/flush; confirmed `add_batch` per-row anti-pattern persists |
| SA-3 | **Async-state wait (the dominant lever)** | Pinpointed *where & why* the 62k parks happen, at `file:line`; ForSt contrast |
| SA-4 | Data layout (C/D: Arrow/CPU) | SST format, key encoding, block size, prefix compression, block cache granularity |
| SA-5 | LSM architecture (E: PMC view) | Compaction/bloom/CF/disaggregation advantage ledger + recommended switches |

Findings below are ranked by **frequency × severity × confidence** and de-duplicated against the
existing corpus. Confirmed-and-refuted levers are retained as a **rejected ledger** (§5.R) so they are
not re-chased.

---

## 3. Execution-path maps (q7 / q9 / q20 / q4)

All four are Flink **streaming join** operators; both input sides buffer in keyed `MapState`
(namespace = join key, entries = buffered rows). Every arriving record triggers a state **prefix
iteration** (probe the other side) + **put** (buffer this side). So the hot path is dominated by
**MapState prefix-scan reads** and **MapState puts**, routed through the async-state pipeline.

### 3.1 The unified state-access chain (all three backends)

```
join operator (per record)
  └─ keyed-state call (MapState.iterator / put)
       └─ TypeSerializer (key+namespace encode, value (de)serialize)
            └─ Backend state object  ── builds a request ──►  StateExecutor
                 └─ BOUNDARY  (RocksDB: in-operator sync JNI │ ForSt: async JNI │ forst-rs: async FFM)
                      └─ Engine  (memtable + SST read / memtable insert + flush/compaction)
                           └─ return bytes ──► future ──► mailbox callback ──► operator resumes
```

The three backends diverge precisely at **StateExecutor + BOUNDARY + completion**:

| | RocksDB | ForSt (C++) | forst-rs (Rust) |
|---|---|---|---|
| Framework | **none (synchronous)** | async-state V2 | async-state V2 |
| Read call site | `RocksDBValueState.value():79`, `RocksDBMapState.get():127` — inline on task thread [FACT] | `ForStStateExecutor.executeBatchRequests:148` → `coordinatorThread` + `readThreads` pool [FACT] | `VectorizedExecutor.executeBatchRequests:408` — inline on mailbox thread [FACT] |
| Boundary | JNI `db.get` (value `byte[]` allocated by JNI) [FACT] | JNI `db.multiGetAsList` — **1 crossing coalesces N keys**, split across `readIoParallelism` threads (`ForStGeneralMultiGetOperation.java:80-182`) [FACT] | **1 FFM `frs_vectorized_batch_get` per batch** (`ForStRsLinker.java:2607`), **single executor thread** [FACT] |
| Future | none | **incomplete future returned immediately** (`:167,:232`) → mailbox freed [FACT] | **already-completed future** (`:552`) → no offload [FACT] |
| `fullyLoaded()` | n/a | **real**: `ongoing.get() >= readThreadCount` (`:289-291`) [FACT] | **hard-coded `false`** (`:1022-1025`) [FACT] |
| In-flight depth | n/a (sync) | up to `readIoParallelism` (default **3**, `ForStOptions.java:286`) [FACT] | **1** (dirac, asserted by `AsyncDispatchInFlightParallelismTest`) [FACT] |

### 3.2 q9 / q7 (symmetric `MapState` joins) — iterator-dominated

Hot op = MapState **prefix scan** of the opposite buffer per probe. forst-rs path:
`ForStRsDBIterRequest.process:204` → `dispatchIterPrefix` (`VectorizedExecutor.java:2219`) → **one FFM
crossing for N prefix opens** at `:2297` → `linker.frsVecIterPrefixOpenBatch` (`ForStRsLinker.java:4026`)
→ blocking `invokeExact:4037` → engine `frs_vec_iter_prefix_open_batch` (`lib.rs:5355`) →
`scan_iter_owned_arc_with_error_slot` (`db.rs:6861`, rows emitted as zero-copy `Arc<[u8]>` into a 64 KiB
chunk). The drain decodes each `IteratorEntryView` zero-copy (`decodeChunkDirect:645`). [FACT]
**The whole batched range-scan build + first-chunk drain runs on the mailbox thread inside the blocking
FFM call** (§4 / OPT-01).

### 3.3 q20 (output-amplifying join) — same read path, extra downstream

q20 amplifies output (auction⋈bid). Prior runs proved the slowdown tracks the **state backend**
(memtable→SST), not the single-threaded `Calc→Writer`: the source ran 150–228K/s pre-flush and only
collapsed to ~15–20K/s **after the first flush**; downstream backpressure is **refuted** (would cap from
t=0). [FACT, sweep doc:205-208] So q20's wall is the same async-state+probe wall as q9, plus an output
operator that is *not* the bottleneck.

### 3.4 q4 (join + retract aggregation) — write-heavy

q4 adds a running aggregate with retractions → heavier **write** path (memtable insert + flush +
compaction). q4's prior gap was diagnosed as flush throughput + steady-state write coordination; the
SST-write coalescing fix cut flush 202→20 ms/MB. Residual = the per-row SST-writer build (OPT-07) +
async-state write dispatch.

---

## 4. THE dominant root cause — async-state depth-1 inline FFM dispatch

This is the headline finding (SA-3, corroborating the JFR profile). **Confidence: high [FACT] for the
mechanism; [HYP] for the precise share each sub-cause contributes.**

### 4.1 The mechanism, at `file:line`

1. **AEC is built to pipeline.** `AsyncExecutionController` bounds in-flight records at
   `maxInFlightRecordNum` = `execution.async-state.total-buffer-size`, **default 6000**
   (`ExecutionOptions.java:197-200`); batch trigger = `active-buffer-size` default 1000. It parks the
   operator in `seizeCapacity → drainInflightRecords` (`:405`) → loop `while inFlightRecordNum > target`
   → `waitForNewMails → notifyLock.wait(1)` (`:453/:478/:498`). **This is the JFR's
   `TaskMailboxImpl.take()` / `processMailsWhenDefaultActionUnavailable` park.** [FACT]
2. **The forst-rs executor defeats the pipeline.** `VectorizedExecutor.executeBatchRequests` runs the
   entire batch **inline** (`executePuts/Gets/Iters`, `:460-463`) and returns
   `CompletableFuture.completedFuture(null)` (`:552`). `fullyLoaded()` returns hard-coded **`false`**
   (`:1022-1025`). [FACT]
3. **The FFM downcall is plain blocking** (deliberately *not* `bindCritical`, comment
   `ForStRsLinker.java:1275-1277`: "Full LSM reads … must remain JVM-safepoint-friendly"), so the calling
   **mailbox thread blocks in native code** for the whole batched range-scan. [FACT]
4. **Result:** in-flight dispatch depth is pinned at **1**. While the single mailbox thread is inside the
   synchronous FFM iterator drain, it cannot run completion callbacks (mails) that decrement
   `inFlightRecordNum`, and cannot admit new records → `inFlightRecordNum` never drains → it parks. The
   62,719 ThreadParks vs 4,433 on-CPU samples are this exact stall. [FACT]

The executor's own design comment is the smoking gun (`VectorizedExecutor.java:367-402`): *"FFI is
synchronous (V1 contract) … `fullyLoaded()` always returns `false` … the depth histogram is a dirac at
1."* The shared executor-owned buffers (`getKeys/putKeys/outData…`, `:163-179`, `reset()` per batch at
`:316-329`) are *why* a batch can't be offloaded while the next is built — the buffer-ownership model
**forces** depth-1. [FACT]

### 4.2 Why RocksDB and ForSt don't hit this

- **RocksDB:** synchronous — `state.value()` is a direct in-operator JNI call (`RocksDBValueState.java:79`).
  No async pipeline, no future, no mailbox-park-on-async, no per-record FFM segment alloc. It simply can't
  park here. [FACT]
- **ForSt (C++):** *same AEC*, but `executeBatchRequests` returns an **incomplete** future and submits to a
  `coordinatorThread` that fans work across a **3-thread read pool** (`read-io-parallelism=3`), with a
  **real `fullyLoaded()`** (`ongoing >= readThreadCount`). The mailbox thread is freed immediately, and
  the AEC backpressures only when the read pool is genuinely saturated. [FACT] *That boundary+offload
  efficiency is the entire ForSt-vs-forst-rs gap.*

### 4.3 Why the prior "parallel executor" experiment didn't fix it (reconciliation)

A `RoutingStateExecutor` *does* exist (N single-thread workers, early-return incomplete future, real
`fullyLoaded()`, `RoutingStateExecutor.java:146-207`) — but it is **gated OFF**
(`FRS_RS_PARALLEL_EXECUTOR` unset by default, `ForStRsAsyncKeyedStateBackend.java:1141-1148`;
`run-8c32g.sh:61` leaves it empty). When it *was* enabled in the campaign it gave **zero speedup**
(sweep doc:193-225). SA-3 explains why [HYP]: each worker still calls the **non-parallel**
`frsVecIterPrefixOpenBatch` (single-threaded engine open) and **still blocks on a synchronous FFM call**;
the engine's intra-batch parallel open (`frsVecIterPrefixOpenBatchParallel`, `ForStRsLinker.java:4056`)
is **unused** by `dispatchIterPrefix:2297`. So cross-batch Java-thread parallelism alone is insufficient —
the workers serialize on blocking native calls, and only ~375% of 800% CPU is used. **The real fix is not
"flip the flag"** (OPT-01 details the actual requirements).

---

## 5. Master optimization table

Search an `OPT-id` to jump to its section. Priority/Status deliberately blank.

| ID | Title | Dimension | Queries | Priority | Status | Confidence |
|---|---|---|---|---|---|---|
| [OPT-01](#opt-01) | **True async-state pipelining (offload + real `fullyLoaded` + non-blocking dispatch)** | Async Pipeline / Batch | q7/q9/q20/q4 | [ ] | [ ] | high (**dominant**) |
| [OPT-02](#opt-02) | Intra-batch parallel iterator open + non-blocking/async FFM boundary | Batch / Zero-Copy | q7/q9/q20 | [ ] | [ ] | high |
| [OPT-03](#opt-03) | Eliminate the 2 engine-side value copies in `batch_get` (return `Arc`/pinned) | Zero-Copy | q7/q9/q20 | [ ] | [ ] | high (impact modest) |
| [OPT-04](#opt-04) | Block size 64 KB → 4–8 KB (per-probe decompress/scan) | Data Layout | q7/q9/q20 | [ ] | [ ] | high (FACT gap) / med (impact) |
| [OPT-05](#opt-05) | Force v2 KV block format (prefix compression, drop Arrow decode ~13%) | Data Layout / Arrow | general | [ ] | [ ] | medium |
| [OPT-06](#opt-06) | Prefix bloom + `prefix_extractor` for MapState namespace scans | LSM Architecture | q7/q9/q20 | [ ] | [ ] | medium |
| [OPT-07](#opt-07) | Vectorize the SST writer (`add_batch` per-row → columnar bulk + SIMD bloom) | Vectorization / CPU/SIMD | q4/q17/write | [ ] | [ ] | high (FACT) / med (impact) |
| [OPT-08](#opt-08) | Write-path per-record cost (BTreeMap insert + dual arena + WAL `to_vec`) | Memory Pool / Batch | q4/write | [ ] | [ ] | medium |
| [OPT-09](#opt-09) | Pool/reuse per-record FFM native segments (`initNativeMemory` ~9%) | Memory Pool | q7/q9/q20 | [ ] | [ ] | high (FACT) / low (impact) |
| [OPT-10](#opt-10) | Complete end-to-end columnar zero-copy key+value path (lever 1/2/3) | Arrow / Zero-Copy | q7/q9/q20 | [ ] | [ ] | high (mostly DONE) |
| [OPT-11](#opt-11) | Decoded-block cache footprint (64 KB decoded → RSS / cached-keys-per-MB) | Data Layout / Memory | q9/q20 | [ ] | [ ] | medium |
| [OPT-12](#opt-12) | Compaction: single-file pick → score/overlap-aware; sub-compactions on | LSM Architecture | q4/q9 | [ ] | [ ] | medium |
| [OPT-R](#opt-r) | **Rejected ledger** (refuted levers — do not re-chase) | — | — | — | rejected | high |

---

## 6. Optimization Spec details

<a id="opt-01"></a>
### [OPT-01] True async-state pipelining — offload + real `fullyLoaded()` + non-blocking dispatch

- **Dimension**: Async-State Pipeline / Batch Execution
- **Queries**: q7 / q9 / q20 / q4 (all async-state queries; joins most)
- **Priority**: [ ] High   [ ] Medium   [ ] Low
- **Status**: [ ] to-validate  [ ] accepted  [ ] rejected  [ ] deferred
- **Confidence**: **high** — mechanism confirmed in code + JFR; this is THE dominant lever.

#### Symptom & Impact
q7/q9/q20 are wait-bound: 62,719 ThreadParks vs 4,433 on-CPU (JFR). Throughput collapses ~4–15× after
the first flush and the operator spends its life parked in `drainInflightRecords`. The AEC is configured
for 6000 in-flight records but achieves dispatch depth 1.

#### Affected Code Paths
- forst-rs: `VectorizedExecutor.executeBatchRequests` (`:408-583`, inline + `completedFuture` `:552`),
  `fullyLoaded()` hard-coded false (`:1022-1025`), shared-buffer reset model (`:163-179, :316-329`).
- forst-rs (the intended fix, already present but OFF): `RoutingStateExecutor.java:146-207` (early-return
  incomplete future, per-worker `Arena.ofShared()`, real `fullyLoaded()=free.isEmpty()`),
  gate `ForStRsAsyncKeyedStateBackend.java:1141-1152`.
- ForSt reference: `ForStStateExecutor.java:148-233` (`coordinatorThread` + `readThreads`, incomplete
  future `:232`, real `fullyLoaded` `:289-291`), `ForStOptions.java:286` (`read-io-parallelism=3`).
- RocksDB reference: synchronous, no executor (`RocksDBValueState.java:79`).

#### Root Cause
**[FACT]** The default executor returns a completed future and reports never-loaded, running each batch
inline on the mailbox thread through a blocking FFM call → depth-1 → mailbox can't drain in-flight → park.
**[FACT]** Enabling the existing `RoutingStateExecutor` alone is insufficient (it gave zero speedup)
because each worker still issues a **blocking** single-threaded FFM open (§4.3).

#### Proposed Optimization
Make forst-rs's dispatch behave like ForSt's: (1) the executor must **return an incomplete future** and
**offload** the batch to a worker/coordinator, freeing the mailbox thread; (2) implement a **real
`fullyLoaded()`** tied to actual outstanding-batch count so the AEC backpressures correctly instead of
force-triggering into depth-1; (3) give each worker **its own buffers/arena** (already true in
`RoutingStateExecutor`); (4) crucially, pair with **OPT-02** so workers don't simply re-serialize on
blocking FFM calls — either make the native iterator dispatch non-blocking, or use the engine's
intra-batch parallel open so one worker's single FFM call already fans K probes across the engine read
pool. Target the ForSt model: incomplete-future + offload + `read-io-parallelism`-wide concurrency.

#### Expected Impact
**[HYP]** This is the only lever with a path to the ~2× q9/q20 needs. The JFR shows the wall is the wait,
not CPU; raising effective dispatch concurrency from 1 toward 3–4 (ForSt parity) directly attacks the
62k parks. Realistic ceiling = ForSt-class throughput (q20 ForSt finishes within the window; q9 ForSt
pending). Not a micro-opt — a structural change.

#### Complexity & Risks
High effort, multi-session, structural. Correctness risks: (a) the **shared mutable decode scratch**
race already bit this (`iterView` `WrongThreadException`, fixed in flink `fc8eb8cd97c`) — any offload
must keep all decode scratch thread-confined (per-worker `Arena`/`VIEW_TL`); (b) per-key ordering must
hold — AEC already guarantees disjoint keys per concurrent batch, but verify; (c) checkpoint must drain
in-flight before snapshot. Rollback = the flag gate already exists.

#### Validation Plan
Re-run q9 + q20 8c/32g with the offloading executor; measure (1) JFR ThreadPark/ExecutionSample ratio
(target: parks ↓, on-CPU share ↑), (2) post-flush steady-state rate (target: no collapse to ~15K/s),
(3) TM CPU% (target: > 375% → toward 600–800%), (4) out_rows == RocksDB. Falsify if parks stay ~62k and
rate stays flat → the wait is elsewhere (then profile for a deeper serial point per OPT-02 candidates).

#### Dependencies
Pairs with **OPT-02** (non-blocking / intra-batch-parallel dispatch). Must keep **OPT-10** zero-copy
decode thread-confined.

---

<a id="opt-02"></a>
### [OPT-02] Intra-batch parallel iterator open + non-blocking FFM boundary

- **Dimension**: Batch Execution / Zero-Copy / FFI boundary
- **Queries**: q7 / q9 / q20
- **Priority**: [ ] · **Status**: [ ] · **Confidence**: high (mechanism FACT; magnitude HYP)

#### Symptom & Impact
Even with offload (OPT-01), each worker blocks the calling thread inside one synchronous native
range-scan, so N workers give < N× concurrency (prior parallel-executor run: ~375% CPU, no speedup).

#### Affected Code Paths
- `dispatchIterPrefix` (`VectorizedExecutor.java:2219-2297`) calls the **non-parallel**
  `frsVecIterPrefixOpenBatch` (`ForStRsLinker.java:4026, blocking invokeExact :4037`).
- The **unused** engine primitive: `frsVecIterPrefixOpenBatchParallel` (`ForStRsLinker.java:4056`) →
  `frs_vec_iter_prefix_open_batch_parallel` (`lib.rs:5575`) "builds + drains K probes concurrently across
  its read pool" — exists, gated `FRS_RS_PARALLEL_ITER`.
- Engine read pool: `bg_read_pool`, `FRS_RS_READ_IO_PARALLELISM` (`db.rs:9796-9812, 6065`).
- Plain `bind()` (no `bindCritical`) `ForStRsLinker.java:1258-1277`.

#### Root Cause
**[FACT]** The default dispatch uses the single-threaded engine open and a blocking downcall; the
engine's own intra-batch parallel open is wired but never called on the hot path.

#### Proposed Optimization
Route join MapState prefix scans through `frs_vec_iter_prefix_open_batch_parallel` so **one** FFM call
fans K probes across the engine `bg_read_pool` — moving the parallelism below the FFM boundary (one
crossing, engine-side concurrency) rather than relying on Java worker threads each blocking on a serial
open. Optionally evaluate a non-blocking completion (engine signals readiness) so the Java thread isn't
parked in native code. Keep reads on plain (safepoint-friendly) downcalls unless proven safe.

#### Expected Impact
**[HYP]** Restores the concurrency OPT-01 needs to translate into throughput; directly addresses the
"3 idle-ish workers blocked on a serial point" observation. Magnitude bounded by read-pool width
(default `min(cores,4)`).

#### Complexity & Risks
Medium. The parallel open shares a pinned version snapshot (lock-free `sst_readers` ArcSwap + sharded
cache) — already designed for this. Risk: per-probe error propagation + ordering of returned chunks.
Validate with the `IterPrefixBatchOpenTest` family (already green) extended for the parallel variant.

#### Validation Plan
A/B `dispatchIterPrefix` parallel vs serial under OPT-01 offload; measure CPU% and post-flush rate.
Falsify if CPU stays ~375% → a lock/serial point below the engine read (re-audit version/SST-reader
locks; LRU mutex already refuted as I/O-serializing, sweep doc:210-218).

#### Dependencies
OPT-01 (offload makes this meaningful).

---

<a id="opt-03"></a>
### [OPT-03] Eliminate the two engine-side value copies in `batch_get`

- **Dimension**: Zero-Copy
- **Queries**: q7 / q9 / q20 (point-get sides) · **Priority**: [ ] · **Status**: [ ] · **Confidence**: high (FACT); impact modest (wait-bound)

#### Symptom & Impact
Every present value in a GET batch is materialized as an owned `Vec<u8>` **then** copied again into the
caller's `out_data` segment — two copies + one engine-heap alloc per row, on a path the iterator side
already does zero-copy.

#### Affected Code Paths
- `db.rs:7835 batch_get` → `batch_get_vectorized:7893` returns `Vec<Option<Vec<u8>>>` (owned). [FACT]
- `lib.rs:3190-3201 frs_vectorized_batch_get`: `ptr::copy_nonoverlapping` each `Vec` into `out_data`. [FACT]
- Contrast: `db.rs:7784 get_arc` returns `Arc<[u8]>` (zero-copy share); the iter path emits `Arc<[u8]>`
  into the chunk (`lib.rs:5011`). Java side already zero-copy via `MemorySegmentDataInputView`
  (`ForStRsValueStateV2.java:238`). [FACT]

#### Root Cause
**[FACT]** `batch_get` returns owned `Vec`s instead of `Arc`/pinned slices, forcing the FFI to re-copy.

#### Proposed Optimization
Have `batch_get` expose `Arc<[u8]>`/pinned slices (like `get_arc`) and let the FFI copy **once** directly
from the engine buffer into `out_data` (or hand back offsets into a pinned region). Removes one alloc +
one copy per present value.

#### Expected Impact
**[HYP] Modest.** On a wait-bound query this is GC/alloc relief, not wall-clock; included for mandate
alignment (no-per-record-copy). Largest where GET (not iterator) dominates.

#### Complexity & Risks
Low–medium. Lifetime of the `Arc`/pinned region must outlive the FFM copy. Correctness-neutral (same
bytes). Covered by existing batch-get tests.

#### Validation Plan
alloc-rate / young-GC frequency before/after on q9; flamegraph share of `copy_nonoverlapping` + `Vec`
alloc. Wall-clock expected flat (confirms wait-bound).

#### Dependencies
None.

---

<a id="opt-04"></a>
### [OPT-04] Block size 64 KB → 4–8 KB

- **Dimension**: Data Layout
- **Queries**: q7 / q9 / q20 (point-get + short prefix scans) · **Priority**: [ ] · **Status**: [ ] · **Confidence**: high (gap FACT); impact HYP

#### Symptom & Impact
A point-get reads + LZ4-decompresses a whole **64 KB** block to extract one ~tens-of-bytes value — 16×
more bytes decompressed/scanned per probe than RocksDB's 4 KB default.

#### Affected Code Paths
- forst-rs default `block_size = 64*1024` (`config.rs:266`, `writer.rs:101`); production flush/compaction
  use `self.options.block_size` (`db.rs:3930,4193,5808,7234`). Tunable `FRS_BLOCK_SIZE_KB`
  (`db.rs:108-118`, range 512B–1GiB). [FACT]
- RocksDB/ForSt default `block_size = 4*1024` (`include/rocksdb/table.h:276`). [ARCH]

#### Root Cause
**[FACT]** 16× larger blocks → 16× decompress+in-block-scan amplification per point read.

#### Proposed Optimization
A/B `FRS_BLOCK_SIZE_KB` at 4/8/16 KB on q9/q20; pick the read-amp/space-amp knee. Pairs with v2 KV blocks
(OPT-05) so intra-block seek is binary-search-over-restarts, not linear.

#### Expected Impact
**[HYP]** Up to ~8–16× less per-probe decompress/scan **on the engine read** — but the engine read
already profiles cheap (n_ovl=1, B_resident), so wall-clock impact is bounded by how much of the wait is
actually block decode vs the async dispatch. Likely **secondary** to OPT-01; still the top *engine-layer*
data-layout lever and cheap to test.

#### Complexity & Risks
Low (config A/B first). Smaller blocks → larger index + more block-cache entries (more metadata). Watch
flush/compaction throughput (more blocks to encode).

#### Validation Plan
`FRS_BULK_SAMPLE`/`FRS_ITER_DIAG` per-probe decompress time + n_ovl; q9/q20 wall + RSS at 4/8/16/64 KB.

#### Dependencies
OPT-05 (v2 KV intra-block seek) amplifies the benefit.

---

<a id="opt-05"></a>
### [OPT-05] Force v2 KV block format (prefix compression; drop Arrow decode)

- **Dimension**: Data Layout / Arrow
- **Queries**: general (all SST reads) · **Priority**: [ ] · **Status**: [ ] · **Confidence**: medium

#### Symptom & Impact
Two block formats coexist; the **v1 Arrow** block stores full keys per row (no prefix compression) and
pays an Arrow `StreamDecoder` (FlatBuffers parse, ~13% of per-block read CPU). The default is ambiguous
in code.

#### Affected Code Paths
- v1 Arrow (`BLOCK_TYPE_DATA=0x01`): full key per row, Arrow decode (`data_block.rs:49-89,176-243`). [FACT]
- v2 KV (`BLOCK_TYPE_DATA_KV=0x02`): RocksDB-style prefix compression + restart array (interval 16),
  pointer-walk decode (`kv_block.rs:29-50,116-199,444-471`) — faithful port of `block_builder.cc`. [FACT]
- Default selector `sst_write_kv_format()` is **ON unless `FRS_SST_KV_BLOCK_FORMAT=0`** (`kv_block.rs:75-94`),
  but doc comments at `kv_block.rs:71-74` / `writer.rs:206-211` still call v1 "the safe default" —
  **contradictory**. [FACT]

#### Root Cause
**[FACT]** Read efficiency depends on which format is live, and the code's stated default is ambiguous.
v1 lacks prefix compression and adds Arrow decode; v2 matches RocksDB.

#### Proposed Optimization
Resolve the ambiguity: confirm v2 KV is the production default, delete/repair the stale "v1 default"
comments, and (if confirmed) make v2 the only writer format (keep v1 read support for old files).
Eliminates full-key-per-row bytes + the ~13% Arrow decode on the read path.

#### Expected Impact
**[HYP]** ~13% per-block read CPU removed (Arrow decode) + fewer bytes to LZ4/scan on prefix-sharing
MapState keys (`namespace|userkey`). Modest on wall (engine read is cheap) but mandate-aligned and
removes confusion.

#### Complexity & Risks
Low–medium. Must keep v1 read path for already-written SSTs (format is dispatched per-block at read,
`reader.rs:399-419`, so mixed files are safe). Verify accuracy on a full q11 (MapState-iteration) run.

#### Validation Plan
Confirm live format via a one-line log of `sst_write_kv_format()`; A/B per-block decode CPU (flamegraph)
v1 vs v2; out_rows == RocksDB.

#### Dependencies
Pairs with OPT-04.

---

<a id="opt-06"></a>
### [OPT-06] Prefix bloom + `prefix_extractor` for MapState namespace scans

- **Dimension**: LSM Architecture
- **Queries**: q7 / q9 / q20 · **Priority**: [ ] · **Status**: [ ] · **Confidence**: medium

#### Symptom & Impact
A join probe whose namespace prefix is absent must still binary-search **every overlapping SST's** index
(cited ~142 overlapping L0 SSTs per probe, `reader.rs:721`), because forst-rs has **no prefix bloom** —
only a coarse decode-free `may_contain_range` min/max prune.

#### Affected Code Paths
- forst-rs has whole-key SBBF (`sst/bloom_filter.rs:15-43`, checked on point `get` `reader.rs:469,580`)
  but **no `prefix_extractor`, no prefix bloom, no partitioned filter** (grep empty); iterator does no
  bloom check (`iter.rs`). [FACT]
- Index prefix seek: `first_block_ge` (`reader.rs:703`) + `may_contain_range` (`reader.rs:727-740`). [FACT]
- RocksDB: `prefix_extractor` + prefix bloom can skip an entire SST on `Seek` without index touch. [ARCH]

#### Root Cause
**[FACT]** No prefix-bloom acceleration for the join's dominant access pattern (MapState prefix scan), so
missing probes do index work on every overlapping SST.

#### Proposed Optimization
Add a `prefix_extractor` (namespace prefix of the MapState composite key) + per-SST prefix bloom; check
it before the index binary-search in the prefix-open path. Skips whole SSTs for absent prefixes.

#### Expected Impact
**[HYP]** The one engine lever with direct join-probe relevance per SA-5: cuts per-probe index work for
the (common) miss case. Still **secondary** to OPT-01 (the wait), but the best *engine* switch.

#### Complexity & Risks
Medium. New filter built at SST write + loaded at open; prefix definition must match the backend's key
encoding (`namespace|userkey`). Risk of wrong prefix length → false skips (correctness) → unit-test
against full-key bloom equivalence.

#### Validation Plan
Per-probe `A_fanout/sstloop` time + SSTs-touched-per-probe before/after on q9 at 50M+ (deep post-flush);
out_rows == RocksDB.

#### Dependencies
None (independent engine module).

---

<a id="opt-07"></a>
### [OPT-07] Vectorize the SST writer (`add_batch` per-row → columnar bulk + SIMD bloom)

- **Dimension**: Vectorization / CPU/SIMD
- **Queries**: q4 / q17 / all write-heavy · **Priority**: [ ] · **Status**: [ ] · **Confidence**: high (FACT); impact medium

#### Symptom & Impact
Flush + compaction write SSTs through `StreamingSstWriter::add_batch`, which — despite receiving Arrow
columns — **loops per-row** `add_internal`, re-appending each cell into 4 new Arrow builders + hashing
the bloom per key. The input is already columnar; this is a redundant per-row copy.

#### Affected Code Paths
- `writer.rs:667-718 add_batch` — explicit per-row loop (`:705-716`) calling
  `add_internal:245-312` (4 builder `append_value` `:270-276`, `Sbbf::hash_key` per key `:304`). [FACT]
  The kept-per-row comment is at `:699-704`.
- Same writer feeds **both** flush (`flush.rs:225`) and compaction (`db.rs:3990/4272/5916`). [FACT]
- Prior measure: SST-write cost was ~95% sink-I/O (fixed by coalescing → flush 202→20 ms/MB); residual is
  this per-row CPU build. [FACT, gap-map RESULTS]

#### Root Cause
**[FACT]** Per-row builder re-append + per-key bloom hash, on already-columnar Arrow input.

#### Proposed Optimization
Build data blocks + bloom **directly from the input Arrow columns in bulk**: operate on column slices
(zero-copy), batch the key-hashing (SIMD over the key column), avoid the 4-builder re-append. Applies to
both flush and compaction writers.

#### Expected Impact
**[HYP]** Cuts the residual per-byte write CPU on q4/q17-family (write-heavy). Less relevant to the
read-bound q7/q9/q20 wall. Target: flush ms/MB toward single digits at the CPU level (I/O already fixed).

#### Complexity & Risks
Medium. Must preserve block-boundary flush semantics (the reason cited for per-row dispatch) and exact
SST bytes (out_rows == RocksDB). Bloom batch-hash must be scalar-equivalent.

#### Validation Plan
flush/compaction ms/MB CPU split (`sstbuf/sstenc`) before/after; out_rows == RocksDB; q4/q17 wall.

#### Dependencies
None.

---

<a id="opt-08"></a>
### [OPT-08] Write-path per-record cost (BTreeMap insert + dual arena + WAL `to_vec`)

- **Dimension**: Memory Pool / Batch Execution
- **Queries**: q4 / write-heavy · **Priority**: [ ] · **Status**: [ ] · **Confidence**: medium

#### Symptom & Impact
Each memtable insert does a per-row **BTreeMap** ordered insert **plus** appends to 4 column arenas
(dual-write), and the WAL append loop does per-row `keys[i].to_vec()` + `values[i].to_vec()` (1–2 heap
copies/record) when WAL is enabled.

#### Affected Code Paths
- `vectorized.rs:1048-1142 batch_insert_with_base_seq` — per-row arena append + `index_insert:429`
  (BTreeMap O(log N), inline `SmallVec<[u8;48]>` key so ≤48 B = no heap alloc). [FACT]
- `db.rs:3408-3417` WAL loop — per-row `to_vec()` copies. [FACT]
- `wait_for_wbm_headroom` is a 1 ms-sleep spin when over-budget (`db.rs:2666-2707`) — coarser than
  RocksDB's condvar `allow_stall`. [FACT]

#### Root Cause
**[FACT]** Broader per-record write constant than the C++ engines (dual structure + WAL copies + spin
stall) — diffuse, not a single hotspot (consistent with prior q4 "diffuse backend constant" finding).

#### Proposed Optimization
(a) If WAL is on for these queries, write the serialized batch buffer directly (avoid per-row `to_vec`).
(b) Evaluate whether the BTreeMap + arena dual-write can share key storage (index references arena spans).
(c) Replace the 1 ms spin stall with a condvar wakeup (latency granularity).

#### Expected Impact
**[HYP]** Modest, write-path only; helps q4 steady-state. Not a q7/q9/q20 lever.

#### Complexity & Risks
Medium; correctness-sensitive (memtable index integrity, WAL recovery). Each sub-item independently
A/B-able.

#### Validation Plan
Per-row write CPU (flamegraph) + q4 wall + recovery test (if WAL touched).

#### Dependencies
None.

---

<a id="opt-09"></a>
### [OPT-09] Pool/reuse per-record FFM native segments

- **Dimension**: Memory Pool
- **Queries**: q7 / q9 / q20 · **Priority**: [ ] · **Status**: [ ] · **Confidence**: high (FACT); impact low

#### Symptom & Impact
JFR on-CPU shows per-record FFM segment alloc (`SegmentFactories.initNativeMemory` + `Unsafe.checkOffset`)
~9% + async-future alloc ~3%. Violates the no-per-record-alloc mandate and adds GC pressure.

#### Affected Code Paths
- Per-probe 64 KiB chunk alloc was already moved to a per-executor **reused** buffer
  (`process(...,reusedChunkBuf)`, flink `53722e117d6`) — partially DONE. [FACT]
- Residual per-record segment/future allocations on the async path (`ContextAsyncFutureImpl` makeNewFuture
  ~3%, `AsyncExecutionController.java:418`). [FACT]

#### Root Cause
**[FACT]** Remaining per-record native-segment + future allocation on the hot path.

#### Proposed Optimization
Extend the reuse model to all per-record native segments; pool/recycle async-future objects where the
framework allows.

#### Expected Impact
**[HYP] Low** on wall (wait-bound; ~9%+3% on-CPU of the ~12% non-idle CPU), real on GC/allocations.

#### Complexity & Risks
Low–medium. Reuse must stay thread-confined (the `iterView` race lesson).

#### Validation Plan
alloc-rate + young-GC frequency; on-CPU share of `initNativeMemory`/`makeNewFuture` before/after.

#### Dependencies
Compose with OPT-10.

---

<a id="opt-10"></a>
### [OPT-10] Complete end-to-end columnar zero-copy key+value path

- **Dimension**: Arrow / Zero-Copy / Vectorization
- **Queries**: q7 / q9 / q20 · **Priority**: [ ] · **Status**: [ ] (mostly DONE) · **Confidence**: high

#### Symptom & Impact
The originating mandate ("all O(1) lookup + zero-copy value before nexmark"). Largely implemented across
three levers; this OPT records the state and the residual, honestly bounded by the wait-bound finding.

#### Affected Code Paths / Status
- **Lever 1 (zero-copy key):** wide-stride `keyHash` (`8ffbc42b933`), `mismatch()` compare, `copy()`
  intrinsic, per-executor reused `chunkBuf`, zero-snapshot drain. **DONE.** [FACT]
- **Lever 2 (coalesced batched lookup):** `executeItersBatchedParallel` + `frs_vec_iter_prefix_open_batch_parallel`.
  Implemented, **gated `FRS_RS_PARALLEL_ITER`** (enable-decision pending → see OPT-02). [FACT]
- **Lever 3 (zero-copy value):** `deserializeUserKey/Value(IteratorEntryView)` via per-thread `VIEW_TL`
  onto the reused chunk slice — no intermediate `byte[]`. **DONE.** [FACT]
- Tests: 528 native tests green; q11 out_rows = 92,000,000 == RocksDB (exact gate). [FACT]

#### Root Cause / Finding
**[FACT]** Measured: zero-copy-key gave **+4.4%** q9 throughput (real but modest); vectorized-hash was
**perf-neutral** (the `×31` arithmetic, not the byte-reads, dominates). Confirms on-CPU shaving alone
can't close the join gap.

#### Proposed Optimization
Land the enable-decision for lever 2 **as part of OPT-01/02** (it only pays off once dispatch is
offloaded + intra-batch-parallel). Keep levers 1/3 (correct, mandate-aligned).

#### Expected Impact
**[HYP]** Single-digit % each on a wait-bound query; collectively narrow but do not close the gap.

#### Complexity & Risks
Low (mostly done). Keep all decode scratch thread-confined.

#### Validation Plan
Already validated for correctness (528 tests + q11 exact). Perf revisit under OPT-01.

#### Dependencies
OPT-01, OPT-02.

---

<a id="opt-11"></a>
### [OPT-11] Decoded-block cache footprint

- **Dimension**: Data Layout / Memory
- **Queries**: q9 / q20 (RSS-pressured) · **Priority**: [ ] · **Status**: [ ] · **Confidence**: medium

#### Symptom & Impact
The block cache stores **decoded/decompressed** blocks (`DecodedBatch`/`DecodedKv`), so a 64 KB block
occupies ≥64 KB resident — fewer cached *keys per MB* than RocksDB's compressed-block cache, raising RSS
on the memory-tight 8c/32g box (q9/q20 RSS ~18–22 GB).

#### Affected Code Paths
- `cache/mod.rs:69-102` (`CacheEntry::{RawBlock, DecodedBatch, DecodedKv}`, charge = decoded size). [FACT]
- Granularity = one SST data block, db_id-qualified key (`reader.rs:308-361`). [FACT]
- The decoded cache is *also a forst-rs advantage* (skips re-decode on hit) — this is a trade, not a pure
  defect. [FACT/ARCH]

#### Root Cause
**[FACT]** Decoded-at-64 KB granularity inflates per-entry footprint vs RocksDB compressed blocks.

#### Proposed Optimization
Pairs with OPT-04 (smaller blocks → smaller entries). Optionally offer a compressed-block cache tier for
memory-constrained slots, or cap decoded-cache charge separately from raw.

#### Expected Impact
**[HYP]** More effective cache per MB → less re-read under RSS pressure. Secondary.

#### Complexity & Risks
Low–medium. Don't regress the decoded-cache hit benefit (the q9 prefix-iter win).

#### Validation Plan
cache hit-rate + RSS at 4/8/64 KB blocks on q9/q20.

#### Dependencies
OPT-04.

---

<a id="opt-12"></a>
### [OPT-12] Compaction: score/overlap-aware multi-file pick; sub-compactions on

- **Dimension**: LSM Architecture
- **Queries**: q4 / q9 (sustained ingest) · **Priority**: [ ] · **Status**: [ ] · **Confidence**: medium

#### Symptom & Impact
forst-rs picks a **single source file** (smallest-key) per compaction and runs sub-compactions only
opt-in/refuted — coarser than RocksDB's score-based, grandparent-overlap-aware, default-on
sub-compaction model → higher write amplification under continuous join ingest.

#### Affected Code Paths
- Single-file pick `db.rs:4114`; level targets `db.rs:4066-4083`; sub-compaction `FRS_COMPACT_PARALLEL`
  default OFF (`compaction.rs:121-124,266-287`). [FACT]
- RocksDB: compaction-priority scoring + grandparent overlap bound + default `max_subcompactions`. [ARCH]

#### Root Cause
**[FACT]** Coarse pick + off-by-default sub-compactions raise write-amp and L0→L1 drain latency that
gates write-stall.

#### Proposed Optimization
Score/overlap-aware multi-file pick + range-partitioned sub-compactions (re-validate — once refuted).

#### Expected Impact
**[HYP]** Improves steady-state write-heavy throughput (q4); modest for read-bound joins.

#### Complexity & Risks
Medium–high (compaction correctness). Sub-compactions were refuted once — re-validate carefully, don't
re-chase blindly.

#### Validation Plan
write-amp + comp ms/MB + q4 wall; L0 file count over time.

#### Dependencies
None.

---

<a id="opt-r"></a>
### [OPT-R] Rejected ledger — refuted levers (do NOT re-chase)

Each below was tested against 8c/32g data or code-audited and **refuted**; retained to prevent rework.

- **Naive parallel executor (flip `FRS_RS_PARALLEL_EXECUTOR`):** zero speedup for q20 (identical DNF), and
  exposed the `iterView` race (fixed). Refuted as a *standalone* fix — workers still block on serial FFM
  opens (→ the real requirement is OPT-01 + OPT-02). [FACT, sweep doc:193-225]
- **Per-record on-CPU micro-opts alone (hash/copy/alloc shaving):** the forst-rs-specific on-CPU
  differential is only ~20%; the system is wait-bound (62k parks). Vectorized-hash measured **perf-neutral**.
  Cannot reach the ~2× joins need. [FACT]
- **Timer-CF (dedicated timer column family):** no q11/q12 win; the −118s was variance. [FACT]
- **Config knobs:** `write_buffer_size=128mb` (compaction-overload → restart), WBM hard-cap with 120s
  give-up (OOM), compaction 8→3 (still OOM), resident-shadow (0 for joins), jemalloc eager-return
  (retained-not-reclaimable). All refuted. [FACT]
- **Downstream backpressure (q20 Calc→Writer):** refuted — pre-flush ran 150–228K/s; collapse tracks the
  state backend, not the output operator. [FACT]
- **LocalCache LRU global mutex as a serial I/O point:** refuted — the `pread` runs *after* the guard is
  released (`local_cache.rs:540-551`); I/O does not serialize on it. [FACT]
- **Merge-collapse on flush as the flush cost:** refuted — `FlushJob::run` does not collapse merge operands
  (code-verified). [FACT]

---

## 7. RocksDB vs ForSt vs forst-rs — comparison & strongly-recommended switches

### 7.1 Where each wins (honest ledger)

| Dimension | Winner | Why |
|---|---|---|
| **Async-state dispatch / boundary** | **RocksDB (sync) ≈ ForSt (offloaded)** ≫ forst-rs | RocksDB has no pipeline to stall; ForSt offloads to coordinator + 3 read threads with real `fullyLoaded()`. forst-rs runs depth-1 inline on the mailbox thread (the wall). [FACT] |
| Block cache | **forst-rs ≥ RocksDB** | sharded clock cache caching **decoded** batches (skips re-decode); RocksDB caches undecoded blocks. [FACT/ARCH] |
| Disaggregation (file cache, parallel read pool) | ForSt (mature) ≈ forst-rs (credible) | forst-rs has `FileSystemRouter` + bounded `LocalCache` + `bg_read_pool`; ForSt's is battle-tested. [FACT/ARCH] |
| Bloom filters | **RocksDB** | ribbon + partitioned + **prefix bloom**; forst-rs has only whole-key SBBF, **no prefix bloom**. [FACT/ARCH] |
| Compaction | **RocksDB** | score/overlap-aware pick + default sub-compactions; forst-rs single-file pick, sub-compactions off. [FACT/ARCH] |
| SST data layout | **RocksDB ≈ forst-rs-v2KV** > forst-rs-v1Arrow | v2 KV is a faithful RocksDB block port (prefix compression, restart=16); v1 Arrow lacks prefix compression + adds decode. Block size 64 KB vs 4 KB is the live gap. [FACT] |
| Write per-record constant | **RocksDB ≈ ForSt (C++)** > forst-rs | forst-rs pays BTreeMap insert + dual arena + WAL `to_vec` + per-row SST build; diffuse. [FACT] |

### 7.2 Strongly-recommended switches (ranked)

1. **[STRUCTURAL — the lever] Offloaded, truly-pipelined async-state dispatch** (OPT-01 + OPT-02): match
   ForSt's incomplete-future + coordinator/read-pool + real `fullyLoaded()`, and push parallelism below
   the FFM boundary (engine intra-batch parallel open). **This is the only path to the RocksDB/ForSt bar
   on q7/q9/q20.**
2. **[ENGINE] Prefix bloom + `prefix_extractor`** (OPT-06): the one engine switch with direct join-probe
   relevance.
3. **[DATA LAYOUT] 4–8 KB blocks + force v2 KV** (OPT-04/05): cheap, removes the 16× block amplification
   + Arrow decode; secondary to #1.
4. **[WRITE] Vectorized SST writer + score-based compaction** (OPT-07/12): for the write-heavy q4 family.
5. **[MODEST/mandate] Zero-copy `batch_get` + native-segment pooling** (OPT-03/09/10): GC/alloc + mandate
   alignment; single-digit % on wall.

### 7.3 The honest bottom line

The prompt's per-record-unit-cost dimensions (vectorization / zero-copy / Arrow / SIMD / data-layout) are
**real and worth doing**, but the evidence (JFR + 5-agent code map) says they are **modest** on a
wait-bound system — collectively well under the ~2× q7/q9/q20 require. **The dominant, structural lever is
async-state request-completion concurrency** (OPT-01/02): forst-rs must stop dispatching joins at in-flight
depth 1 on a blocking mailbox-thread FFM call and instead offload + pipeline like ForSt. Until that lands,
no amount of per-record CPU shaving will close the join gap. After it lands, the engine/data-layout
switches (prefix bloom, block size, v2 KV) become the next, smaller wins. And the corrected baselines
matter: q9 is already ~0.95× (RocksDB also DNFs), q20 already beats ForSt — the goal is matching RocksDB's
finish time on the heaviest joins, a structural async-pipeline problem, not a regression recovery.

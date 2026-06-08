# forst-rs parallel + coalesced read-path architecture (beat ForSt per-query)

**Date:** 2026-06-08
**Goal:** Close the per-query gap to ForSt-Java on the read-heavy queries (q7, q9, q19, q11, q12)
by adopting ForSt's two structural read advantages inside the forst-rs engine layer, plus the
8c/32g memory model so the heavy join queries finish. Target: forst-rs ≥ ForSt on every query,
≥0.8× RocksDB or ≤+50s, accuracy preserved.

## 1. Root cause (code-grounded, 2026-06-08)
ForSt-Java is faster on read-heavy queries because of three structural properties forst-rs lacks:
1. **Parallel read I/O** — `ForStStateExecutor` runs a fixed pool of read threads
   (`state.backend.forst.executor.read-io-parallelism`, **default 3**); each batch of GET/ITER
   requests is split and executed across them (`ForStGeneralMultiGetOperation` +
   `ForStIterateOperation` submitted to `readThreads`). forst-rs serializes ALL state reads
   through ONE VectorizedExecutor thread per slot.
2. **Coalesced native multiGet** — `db.multiGetAsList(cfs, keys)` groups keys by SST: one bloom
   probe + block fetch shared across all keys in that SST. forst-rs `batch_get` walks the LSM
   per key (read-amp / op-count gap, task #55).
3. **Mature C++ engine** — partitioned index+filter, prefix bloom, SIMD, cache-aligned blocks.

Measured per-query deficits (host sweep): q19 379 vs 137 (−242), q7 508 vs 263 (−245),
q9 937 vs 690 (−247), q11 216 vs 158 (−58), q12 44 vs 32 (−12). All read-heavy.

## 2. Architecture (Approach 1 — engine-side, single FFM crossing)

### Component 1 — Coalesced multiGet in the engine (attacks #2)
`DbImpl::batch_get_vectorized(cf, keys: &[&[u8]]) -> Vec<Option<Value>>`:
1. Stable-index the keys (result placement) — NO sort of the caller's order.
2. **Memtable phase:** one pass over active + each immutable memtable index resolving all keys
   (reuse the offset-only index, task #25). Mark resolved; carry merge operands.
3. **SST phase:** per level, binary-search each unresolved key to its candidate SST (task #24);
   **group keys by `FileNumber`**. Per SST: open one reader, one index region lookup, read each
   needed data block ONCE, extract all grouped keys from the decoded block (shared bloom + decode).
4. **Merge phase:** value-carrying operand collect across tiers (reuse #51 ValueDecision).
5. Return via `ColumnarBatchBuffer` offsets (zero-copy; no byte[] per key).
**Invariant:** byte-identical to N independent `get()` calls. **Verify:** UT over a 3-level,
multi-SST CF asserts equality AND a block-read counter shows coalescing (reads < naive per-key).

### Component 2 — Bounded parallel read pool (attacks #1)
- Add a bounded read-thread pool sized by a new option `read_io_parallelism` (default 3; env
  `FRS_RS_READ_IO_PARALLELISM` already plumbed in run-8c32g.sh). Reuse the shared bg-pool infra
  (task #43) or a dedicated pool; process-global, bounded.
- `batch_get_vectorized` splits the coalesced per-SST work units across the pool, joins results.
  Memtable phase stays inline (cheap); SST phase parallelizes (the expensive part).
- Reads are lock-free: ArcSwap `sst_readers`, MVCC snapshot view; safety proven by
  `tests/concurrent_reads_it.rs`. **Verify:** extend that IT to parallel batch_get under
  concurrent writes; assert no torn reads + equality to serial.

### Component 3 — Parallel vectorized iterators (q19/q11/q9)
- The classifier already batches MapState ITER requests (`ForStRsDBIterRequest`). Run independent
  prefix iterations across the read pool (mirror `ForStIterateOperation` on `readThreads`):
  the FFI `frs_vec_iter_prefix_open/next` per request dispatched to the pool, results collected.
- Single large scan (one Top-N rank): pipeline block decode (prefetch next block while decoding
  current). **Verify:** q11/q19 exact 4.6M counts unchanged; rate stays flat at scale (no decay).

### Component 4 — 8c/32g memory model (q9/q20/q4 FINISH)
From `2026-06-08-phase1-close-readamp-memory-model-design.md`: bound compaction transient
(stream inputs / cap in-flight compaction bytes), effective write-stall OR bounded in-flight so
intake self-limits, keep fadvise(DONTNEED) + the 8c/32g config budget. Interleaved with C1–C3
because q9 must finish to be measured.

### q7 separate investigation
forst-rs beats RocksDB 2.4× on q7 but ForSt is 2× faster (508 vs 263). Interval-join + timer.
Profile on 8c/32g AFTER C1–C2 (which may already close it via faster joins) before designing a
q7-specific change.

## 3. Mandates honored
Single batched FFM crossing (no per-key/record crossing); zero-copy via ColumnarBatchBuffer /
Arrow offsets; no byte[]/byte[][]; batch + vectorized only; lock-free concurrent reads. Each
backend tested with its own timer on 8c/32g Docker; no shortcuts; all verification independent.

## 4. Sequencing (each: implement → UT → 8c/32g e2e exact-count + perf → document)
1. **C1** coalesced multiGet, single-threaded → measure q9/q19/q11 read-amp / rate.
2. **C4** memory model (enough to make q9/q20/q4 finish) → so C2/C3 are measurable at 100M.
3. **C2** parallel read pool → measure parallel speedup on q7/q9/q19.
4. **C3** parallel iterators → q19/q11.
5. **q7** profile + fix if still behind.
6. Full 3-backend 8c/32g sweep q0–q22 → confirm forst-rs ≥ ForSt per-query + ≥0.8×/≤+50s RocksDB.

## 4b. FINDINGS during implementation (2026-06-08) — REPRIORITIZE
- **C1 is already largely done.** `batch_get_vectorized` (db.rs:7432) already coalesces by SST:
  opens each L0 file once probing all pending keys; groups L1+ pending keys by file_number, opens
  each candidate file once. Residual per-key cost = `reader.get_versions(k)` not sharing block
  decode across keys in the SAME block (a smaller RocksDB-multiGet refinement, not the big lever).
- **The slow queries are ITERATOR-dominated, not batch_get.** q9 (join), q19 (Top-N rank),
  q11/q7 (session/interval) are MapState range/rank SCANS. So the lever is **C2+C3 (parallel
  iterators over the read pool)**, NOT C1. Lead with C2/C3.
- **⚠ M1 parallel executor was already built (RoutingStateExecutor, FRS_RS_PARALLEL_EXECUTOR)
  and REFUTED for q19.** Building more parallelism blind risks re-refuting. THEREFORE: profile
  q19/q9 on 8c/32g FIRST to determine whether they are (a) serialization-bound → C2/C3 parallel
  iterators is the fix, or (b) per-iteration CPU-bound (engine iterator/merge cost) → C3 must
  also make each iteration cheaper (and explains why M1 thread-parallelism alone didn't help).
  The profile resolves the M1 paradox and orders C2 vs the engine-iterator micro-opt.
- **q4 8c/32g VALIDATED (this session's fixes):** reaches 92M/100M, peak anon 22.3 GB, no OOM →
  finishes ~600s, beats ForSt (1217s host). Confirms iter-leak + fadvise + memory budget make the
  smaller-state join queries pass; q9 (larger state) still needs the read-amp + memory model.

## ★ EMPIRICAL VERDICT (8c/32g, 2026-06-08) — C3 is primary, C2 secondary
q19 serial = 458.3s; q19 parallel (FRS_RS_PARALLEL_EXECUTOR=1, READ_IO_PARALLELISM=3) = 402.9s.
→ parallelism = ~12% (55s), NOT dramatic. q19 is PER-ITERATION-CPU-bound, not serialization-bound.
Even parallel, 402.9 >> ForSt 137 (host). This RESOLVES the M1 paradox: M1's host "no improvement"
was the same ~12% lost in noise. CONCLUSION: **C3 (cheaper per-record Top-N iteration) is the
DOMINANT lever; C2 (parallel pool) is a real but secondary ~12% win** (land it default-on, cheap).
Other 8c/32g baselines this session: q4 621.5s (FINISHED, was DNF), q19 458s serial.
NEXT for C3: CPU-profile ONE q19 iteration to split cost across (a) Rust vec-iter chunk production,
(b) FFM crossing, (c) Java RowData deserialize per entry. The value-carrying fix helped q11
(529→216) but q19 only 428→379 → q19's Top-N access pattern differs; profile before designing C3.

## ★ q19 JFR PROFILE (8c/32g, 2026-06-08) — the gap is NOT mainly the engine read
Top methods (samples): RowDataSerializer.copy 16633, copyRowData 8588, StringDataSerializer.copy
4946, CopyingChainingOutput.pushToOperator/collect ~10K, RowData fieldGetter 4388,
AsyncStateAppendOnlyTopNFunction.processElement 2382. forst-rs-SPECIFIC: MapStateCache
put/hashOf/findRow ~2400, ForStRsMapStateV2.asyncPut 1904, FFM/engine = minority.
INTERPRETATION: q19 per-record CPU is dominated by FLINK-RUNTIME RowData deep-copy
(CopyingChainingOutput, object-reuse OFF — same for all backends) + string serialization + the
async Top-N operator. The ENGINE read is a SMALL slice → C3 "cheaper engine iteration" has a
LOW ceiling for q19. The biggest forst-rs-SPECIFIC slice is MapStateCache (ForSt has none).
→ Next levers for q19 (in priority): (1) A/B FRS_DISABLE_MAPSTATE_CACHE (ForSt-parity, remove the
per-record cache overhead); (2) reduce RowData copy — needs object-reuse / fewer chained copies
(Flink-runtime, may be config: pipeline.object-reuse — NOT set in any template, default OFF for all
3 backends so it's shared, but enabling it would help forst-rs's longer runtime most); (3) engine
iter (C3) is a minor slice for q19. NOTE: none of the templates set object-reuse → testing
`pipeline.object-reuse: true` is a cheap cross-cutting lever for ALL queries (verify correctness:
Top-N/join with reuse can be unsafe if operators retain references — Flink Table is generally
reuse-safe, but VERIFY exact counts).

## Revised sequencing
0. **Profile q19 + q9 on 8c/32g** (CPU: serialization vs per-iteration cost) — resolves M1 paradox.
1. C4 memory model (q9/q20/q4 finish — q4 already does).
2. C3 parallel + cheaper vectorized iterators (the real lever per the profile).
3. C2 read pool (if profile shows serialization-bound).
4. C1 block-shared multiGet refinement (smaller win).
5. q7 profile + fix. 6. Full 3-backend 8c/32g sweep.

## 5. Risks
- Coalescing correctness under MVCC (snapshot seq must be applied per key) — covered by the
  byte-identical UT + concurrent IT.
- Read-pool oversubscription on 8 cores (4 slots × 3 read threads = 12) — size pool per-slot or
  cap globally; measure vs ForSt's same 3×4 layout (ForSt works, so the layout is viable).
- Memory model must land first or C2/C3 can't be measured at 100M (q9 OOM). Hence sequencing #2.

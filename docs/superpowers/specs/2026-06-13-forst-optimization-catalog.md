# ForSt → forst-rs Optimization Catalog (complete, ranked)

**Date:** 2026-06-13
**Status:** PMC-2 (Phase-2 disaggregated state) — SCAN + DESIGN only (no build/bench this cycle; PMC-1 holds the box for a NexMark measurement).
**Base:** `origin/forst-rs` tip `404ff3fde`.
**Sources:** the paper *Disaggregated State Management in Apache Flink 2.0*, PVLDB 18(12):4846–4859 (read in full this cycle); the prior competitive analysis `2026-06-13-disagg-competitive-analysis.md`; the shipped cold-prime design `2026-06-13-concurrent-scan-cold-start-prime-design.md`; PMC-1's read/scan gate `2026-06-13-cycle3-readpath-combined-gate-results.md`; and direct file:line scans of `forst-rs-storage`/`forst-rs-io`/`forst-rs-engine` (this cycle, two Explore fan-outs + targeted reads).

**Scope rule honored:** every "bottleneck" claim without an existing forst-rs measurement is marked **[needs micro-bench]** rather than asserted. Mini-bench plans target a **simulated 50 G/s S3 (`FRS_MODEL_BW_MBPS=6250`)** or a churn/FFI micro-bench — **NOT** NexMark (NexMark runs only after Phase-1 completes, per standing rule).

---

## 0. Headline — top-5 by leverage × feasibility, and the confirmed next build target

| Rank | Optimization | Leverage | Feasibility | Status |
|---|---|---|---|---|
| **1** | **Concurrent SST-reader OPEN fan-out (Cycle-3)** — parallelize the footer+index OPEN of K cold SST sources at scan cold-start | **HIGH** (join/OVER/long-scan: q7/q9/q19/q20; the OPEN RTTs are serial *today even after cold-prime*, which only parallelizes the *first data block*, not the OPEN) | **Refactor** (reuses read-I/O pool + the exact cold-prime guard pattern; timing-only, byte-identical when OFF) | **DESIGNED, NOT BUILT — confirmed still the top unbuilt item (§5)** |
| **2** | **Partial-merge on write/flush** for merge-operand chains | **HIGH** for the aggregating/reducing class (q5/q8/q11/q15 window-agg) — collapses operand chains at flush so reads don't walk them; today collapse happens **only at compaction** | **Refactor** (the `MergeOperator::partial_merge` hook already exists; wire it into the flush write path with MVCC-snapshot safety) | **ABSENT on write path** (merge_operator.rs:19-21; compaction-only) — **[needs micro-bench]** to confirm it's not just a 5M-scale artifact (MEMORY: prior "merge-chain wall" was a no-flush artifact) |
| **3** | **Single-iterator / point-get read-IO parallelism (depth>1)** — ForSt's `read-io-parallelism`=3 hides remote-miss latency on *every* scan; forst-rs has it on the **batch** prefix-scan path (`bg_read_pool`) but a *single* cold iterator/point-get is still serial | **HIGH-MED** (every cache-miss scan; the paper's L1×L4 dominant loss term) | **Refactor** (`bg_read_pool` exists; extend the parallel-source drain to the single-iterator merge, not just the K-probe batch) | **PARTIAL** — batch path PRESENT (db.rs:9085, 14285 `FRS_RS_READ_IO_PARALLELISM`); single-iterator path serial |
| **4** | **Checkpoint upload grouping / parallel multi-file PUT** — group/parallelize SST PUTs at the checkpoint barrier instead of relying only on the 8-wide backend write-back semaphore through a serial copy loop | **MED** (checkpoint-interval-dominated walls, q4-class; the paper's Fig-9 "<3 s checkpoints"), and δ_upload interference on the join class | **Refactor** (raise checkpoint-layer concurrency above the implicit backend semaphore; coalesce small-object PUTs) | **PARTIAL** — serial copy loop (checkpoint.rs:356-373) over an 8-inflight backend (opendal_backend.rs:196) — **[needs micro-bench]** on simulated-S3 |
| **5** | **Suffix-driven always-local rule for non-SST files** (MANIFEST/CURRENT/LOG/OPTIONS never go remote) + true epoch-decayed cold-list counts + space-based cache limit | **MED** (restore latency — the paper's OSS +10-20 s small-object metadata tax; cache accuracy/safety under state ≫ cache) | **Refactor** (small, localized; ownership classifier + cache-policy extensions) | **ABSENT** (no suffix classifier in ownership.rs); epoch-decay is a **FIFO stand-in** (local_cache.rs:74-77); space-based limit absent |

**Confirmed next build target:** **#1 — the Cycle-3 concurrent SST-reader OPEN fan-out.** The scan surfaced no higher-leverage *read-path* item: #2 (partial-merge-on-write) is comparable leverage but for a *different* (write/agg) class and carries an MVCC-snapshot hazard plus an unresolved "is-this-a-5M-artifact" question, so it should follow the OPEN fan-out, not precede it. The full re-rank justification is in §5.

---

## 1. Method & how to read this catalog

For each candidate: **mechanism** (what ForSt does, paper §/file:line) → **forst-rs gap** (PRESENT / PARTIAL / ABSENT with file:line evidence) → **projected benefit + priority query class** → **mini-bench plan** (simulated-50 G/s-S3 or churn/FFI, never NexMark) → **flag name** → **reusability note**. The shipped items are listed in §4 so the catalog is complete (no re-work), and the ranked gaps are §2 (read path) and §3 (write/checkpoint/cache path).

The paper's own loss decomposition (competitive analysis §1.1) ranks ForSt's 0.15× steady-state loss as: **(1) cache-miss remote reads on the heavy-I/O tail (L1×L4)**, (2) +30% async-runtime CPU (L5), (3) per-block JNI/Java-stream read path. forst-rs **avoids (3) entirely** (native Rust I/O, no JNI-per-block) and **shares (2)** on the Flink side. So the leverage in *this* repo concentrates on **(1)** — which is precisely the read-path cluster #1/#3 above. That is why the top read-path items dominate the ranking.

---

## 2. GAP CATALOG — read path (ranked within section)

### 2.1 [#1] Concurrent SST-reader OPEN fan-out (Cycle-3)

- **Mechanism (ForSt):** depth-3 parallel read threads (`ForStStateExecutor.java:59-66`, `readIoParallelism`=3) hide the remote latency of *opening* and *reading* SST files concurrently; opening an SST is itself a remote round-trip (footer read + index/filter block reads) before any data block can be probed.
- **forst-rs gap: ABSENT.** Cycle-2 shipped `FRS_SCAN_COLD_PRIME` (concurrent-scan-cold-start-prime-design.md §5), which parallelizes the **first data-block GET** of K already-open sources. But the K `TierKeySource::Sst` are *built/opened serially* before that — `build_lazy_prefix_key_stream`/`build_lazy_range_key_stream` (db.rs:9588, :10035) construct each `BlockPrefetcher::new(...)` (db.rs:9929-9942) one at a time, and each open does a synchronous footer+index read on the first touch. Over remote state with K overlapping SSTs, the OPENs are **K serial round-trips** that cold-prime does not cover (it primes data blocks of *already-opened* readers).
- **Projected benefit + class:** the OPEN RTT term is the same shape as the cold-prime data-block term (modeled 2–5.4× at K≥4 in concurrent-scan-cold-start-prime-design.md §1.4), additive to it — the join/OVER classes (q7/q9/q19/q20) issue thousands of fresh cold scans, each paying *both* the OPEN and the first-block RTT serially today.
- **Mini-bench plan:** extend `crates/forst-rs-bench/src/bin/scan_cold_start.rs` (already exists for cold-prime) with a `LatencyFileSystem` that injects `FRS_MODEL_RTT_MS` on `open_random_access_file` (the OPEN) *separately* from `read_at` (the block). Measure first-row wall of a fresh cold K-source scan, OPEN-fan-out OFF vs ON, at RTT∈{2,15} ms (2 ms = simulated 50 G/s intra-DC box). Target: reproduce ≥2× at K≥4, bounded at min(K, pool). **No NexMark.**
- **Flag:** `FRS_SCAN_OPEN_FANOUT` (default-OFF, byte-identical when OFF; pairs with `FRS_SCAN_COLD_PRIME`).
- **Reusability:** reuses the read-I/O pool (`prefetch.rs:214-288`) and the exact §2.2 no-op guard set from cold-prime (flag-OFF / K≤1 / local / memtable-only / already-open). Timing-only ⇒ the byte-identity gate is the same harness. This is the lowest-risk high-leverage item by construction.

### 2.2 [#3] Single-iterator / point-get read-IO parallelism (depth>1)

- **Mechanism (ForSt):** `readIoParallelism`=3 applies to *every* read, not just batches — a single iterator's source reads overlap across the 3 threads.
- **forst-rs gap: PARTIAL.** The **batch** prefix-scan path fans K independent probes across `bg_read_pool` (db.rs:9085 `batch_prefix_scan_parallel`, pool at db.rs:14285, `FRS_RS_READ_IO_PARALLELISM` default min(cores,4)). But a **single** cold iterator or point-get drains its sources serially (the per-source `next_decoded` state machine, prefetch.rs). The within-source readahead overlaps blocks, but cross-source first-touch for a *single* scan is not pooled (cold-prime addresses the cross-source *data block*, not steady-state per-source merge).
- **Projected benefit + class:** the paper's #1 loss term (cache-miss tail) for any query that issues large single iterators (q19/q11 OVER-window long scans). **[needs micro-bench]** — q19/q11 are flagged as engine read-path gaps that S3 doesn't change (competitive analysis §3.3), so the *disagg* benefit specifically is the cross-source overlap, which must be measured separately from the CPU-bound merge cost.
- **Mini-bench plan:** a `range_scan_multilevel`-style bench over K cold remote sources behind `LatencyFileSystem`, single-iterator drain, serial-source vs pooled-source. Simulated-50 G/s-S3 RTT. **No NexMark.**
- **Flag:** extend `FRS_RS_READ_IO_PARALLELISM` to the single-iterator merge.
- **Reusability:** same `bg_read_pool`; the merge already pins an immutable version snapshot (concurrency-safe — `sst_readers` is ArcSwap, block cache sharded), so no new locking.

### 2.3 [supporting] True epoch-decayed cold-list counts (vs FIFO stand-in)

- **Mechanism (ForSt):** cold-list access counts reset when an entry drifts into the last half of the cold list (`secondAccessEpoch`, `FileBasedCache.java:377-387`) — a cheap sliding window without timestamps.
- **forst-rs gap: PARTIAL.** `AdmissionTracker` uses **FIFO aging** as the decay stand-in (local_cache.rs:74-77, 142-177) — bounds memory + decays stale counts, but is coarser than ForSt's epoch. The cached comment explicitly names it "the cheap stand-in for ForSt's epoch-decayed cold-list counts."
- **Benefit + class:** cache hit-rate accuracy under state ≫ cache (the paper's 1 GB-cache regime). On the current box (cache ≥ hot set, competitive analysis A4) this is **insurance, not a binding lever** — ranks low.
- **Mini-bench plan:** a synthetic access-trace replay micro-bench (Zipf over file IDs ≫ cache) measuring hit-rate FIFO-stand-in vs epoch. Pure CPU, no I/O, no NexMark.
- **Flag:** `FRS_CACHE_ADMISSION_EPOCH` (extends the existing `FRS_CACHE_ADMISSION`).
- **Reusability:** drop-in inside `AdmissionTracker`.

---

## 3. GAP CATALOG — write / checkpoint / cache-management path (ranked within section)

### 3.1 [#2] Partial-merge on write/flush for merge-operand chains

- **Mechanism (ForSt/RocksDB):** `partial_merge` combines adjacent merge operands at non-bottommost compaction *and* the memtable can combine operands so a read doesn't walk a long operand chain. forst-rs's `MergeOperator` trait defines `partial_merge` (merge_operator.rs:66) but invokes it **only at compaction** (merge_operator.rs:19-21).
- **forst-rs gap: ABSENT on the write/flush path.** Flush writes every `Merge` entry as-is to L0 (flush.rs); chains collapse only when compaction reaches a `Put`/`Delete` base via `full_merge` (compaction.rs:397-399). No `partial_merge` on the memtable-insert or flush path (confirmed: no `partial_merge`/`combine_operands` in `memtable/*.rs` or `flush.rs`).
- **Projected benefit + class:** the aggregating/reducing window-agg class (q5/q8/q11/q15) — a window builds an operand chain per key; reads before compaction walk O(chain). **[needs micro-bench] — CRITICAL:** MEMORY records that the prior "merge-chain is the window-agg CPU wall" finding was a **5M-no-flush artifact** (`2026-06-01-merge-chain-is-the-window-agg-cpu-wall.md`: at 100M, flush+compaction already bound the chain). So this MUST be measured at a scale where flush fires before asserting benefit, and the MVCC-snapshot hazard (partial-merging across a snapshot boundary) must be proven safe.
- **Mini-bench plan:** a churn micro-bench: N merge-operands per key, force a flush, measure read cost of the flushed-but-not-compacted L0 with partial-merge-on-flush OFF vs ON, at a scale where flush actually fires (≥ write_buffer_size worth of operands). Pure engine micro-bench, **no NexMark**. Gate on byte-identical full-merge result.
- **Flag:** `FRS_PARTIAL_MERGE_ON_FLUSH` (default-OFF).
- **Reusability:** the `partial_merge` hook + compaction's chain-collapse logic (compaction.rs) are directly reusable at the flush write path; the work is the MVCC-safety proof, not new merge code.

### 3.2 [#4] Checkpoint upload grouping / parallel multi-file PUT

- **Mechanism (ForSt):** UFS link-mode means checkpoints upload **zero** at the barrier (paper §5.2) — uploads already happened at flush. For the actual upload-at-flush, ForSt streams via the file system layer; the paper's Fig-9 shows all checkpoints <3 s.
- **forst-rs gap: PARTIAL.** Checkpoint copies live SSTs in a **serial `for` loop** (checkpoint.rs:356-373 `copy_live_ssts`), relying on the backend's implicit 8-wide write-back semaphore (`MAX_INFLIGHT_UPLOADS=8`, opendal_backend.rs:196) for concurrency. There is no checkpoint-layer grouping/coalescing of small-object PUTs, and the serial loop can under-fill the 8-wide pipe for large SST counts.
- **Projected benefit + class:** checkpoint-interval-dominated walls (q4-class: q4 ckpt-30s 557 s — checkpoint cadence is load-bearing per MEMORY) and δ_upload interference on the join class. **[needs micro-bench]** — the link-checkpoint Stage-2 (zero-upload barrier) is the *bigger* structural win and is already staged; this PUT-grouping item helps the *flush-time* upload that remains even under link-mode.
- **Mini-bench plan:** a checkpoint micro-bench uploading N SSTs to a **mock S3** (`FRS_MODEL_BW_MBPS=6250`, simulated 50 G/s, with injected per-PUT op-latency), serial-loop vs explicit `join_all` grouped PUTs, measure barrier wall. **No NexMark.**
- **Flag:** `FRS_CKPT_PARALLEL_UPLOAD` (default-OFF).
- **Reusability:** opendal backend already exposes async puts + `await_upload`; the change is at the checkpoint orchestration layer (raise concurrency above the implicit semaphore, batch small objects).

### 3.3 [#5a] Suffix-driven always-local rule for non-SST files

- **Mechanism (ForSt):** `FileOwnershipDecider` forces MANIFEST/CURRENT/LOG/OPTIONS **always local + private**; only `.sst` is shareable/remote (`FileOwnershipDecider.java:28-52`). The DB's chatty small-file traffic never hits S3 metadata ops — directly mitigates the paper's OSS restore +10-20 s small-object metadata tax (paper §6.1).
- **forst-rs gap: ABSENT.** No suffix classifier in `ownership.rs` (grep: no `classify`/`suffix`/`ends_with`/`extension`). The ownership layer is path-agnostic.
- **Projected benefit + class:** restore latency (all queries on restart/rescale — the paper's 16-49× recovery class). **[needs micro-bench]** on restore-time small-object op count.
- **Mini-bench plan:** a restore micro-bench against mock S3 with injected per-metadata-op latency, counting/timing non-SST opens with vs without the always-local rule. **No NexMark.**
- **Flag:** `FRS_NONSST_ALWAYS_LOCAL` (or fold into the ownership classifier default at Stage-2 wiring).
- **Reusability:** small classifier in `FileOwnershipTracker` (file_mapping.rs:158); UUID-keying (file_mapping.rs:642) already shipped, so this completes the §2.2 catalog.

### 3.4 [#5b] Space-based cache limit (min free-disk headroom)

- **Mechanism (ForSt):** dual limit policy — size-based AND space-based (`SpaceBasedCacheLimitPolicy` probes the actual filesystem, `ForStFlinkFileSystem.java:144-168`).
- **forst-rs gap: ABSENT.** Cache budgets bytes only (`capacity_bytes`, local_cache.rs:69); no free-disk-headroom probe.
- **Benefit + class:** safety/robustness (avoid filling the disk under write-through), not a throughput lever. Low rank.
- **Mini-bench plan:** none needed (correctness/safety feature, not perf) — unit test that eviction triggers on low free-disk. **No bench.**
- **Flag:** `FRS_CACHE_SPACE_LIMIT_MB`.
- **Reusability:** add a second limit check in the LRU eviction path (local_cache.rs:1043-1061).

### 3.5 [supporting] Handle-backed source rebind at restore

- **Mechanism (ForSt):** `giveUpOwnership(path, handle)` flips a live working file to checkpoint-owned, re-pointing its source at the checkpoint `StreamStateHandle` (`FileMappingManager.java:335-352`) — restore-then-keep-reading-from-checkpoint-namespace.
- **forst-rs gap: PARTIAL.** `register`/rebind exists (file_mapping.rs:541) and the tombstone protocol covers deletion, but the *source rebind* to a checkpoint handle is not the full ForSt handshake. Instant-link restore already ships (MEMORY), so this is a completeness item for the JM↔TM ownership handshake at Stage-3, not a standalone perf lever.
- **Mini-bench plan:** none standalone — folds into the Stage-3 instant-link restore validation. **No NexMark.**
- **Flag:** n/a (Stage-3 wiring).
- **Reusability:** extend the existing mapping rebind.

---

## 4. ALREADY SHIPPED (do not re-do) — completeness ledger

These were absent/under-specified in earlier analyses but are PRESENT now; confirmed with file:line this cycle:

| Optimization | Evidence | Notes |
|---|---|---|
| Concurrent scan cold-start prime (first data block) | prefetch.rs `prime_first_window`; db.rs `prime_cold_sources_concurrent` | `FRS_SCAN_COLD_PRIME`, Cycle-2; 1.66–3.79× cold-start (real merge bench) |
| Batch prefix-scan read-IO parallelism (K probes) | db.rs:9085, pool db.rs:14285 | `FRS_RS_READ_IO_PARALLELISM`, ForSt read-io-parallelism analogue on the batch path |
| Local file cache (write-through secondary) | local_cache.rs, cached_fs.rs | LRU, generation-stamped O(1) touch |
| Write-only admission + count-to-promote + promoteLimit thrash cap | local_cache.rs:59-198 | `FRS_CACHE_ADMISSION` — the §2.1.1-2.1.3 ForSt mechanisms, shipped since the competitive analysis |
| Requester-class / background cache exemption | local_cache.rs:94-98, 498-504; bg_pool.rs:49-76 | `FRS_CACHE_BG_EXEMPT` — compaction can't evict the operator hot set (§2.1.4) |
| Restore cache pre-seed | filesystem.rs `pre_seed_admission`; db.rs:8010-8011,8105,8240 | warms restored files on first foreground touch (§2.1.6) |
| Block cache (CLOCK, cost-aware by charge) | cache/clock.rs:93-300; cache/mod.rs:88-129 | priority countdowns; weighted by bytes |
| Adaptive prefetch / readahead (cold→ramp→cap, coalesced, vectored) | prefetch.rs:15-90,694-874 | window doubles to 256 KiB local / 4 MiB remote; io_uring vectored runs |
| Hard-link zero-copy checkpoint + refcount deletion | file_mapping.rs:560-589,600-640 | UFS link semantics; 3172× link-vs-copy |
| UUID physical keys | file_mapping.rs:642 (`mint_uuid_physical`) | §2.2d, shipped |
| Remote compaction (offload executor) | compaction_executor.rs:109-174 | `RemoteEmulatedCompactionExecutor`, flag-OFF |
| Bounded background compaction pool (CPU cap) | db.rs:14309 (`bg_compact_pool`) | `FRS_BG_COMPACT_THREADS` — the q4 decay fix |
| Async flush↔upload decoupling | opendal_backend.rs:166-497; checkpoint.rs:97-100 | flush writes local, upload async, `await_upload` gate |
| Zero-copy row views (Arrow-backed) | reader.rs:81-90,136-165 | `RowView` borrows from Arrow buffers; no per-row alloc |
| FileMappingManager / link checkpoints / WAL-delta / rescale-by-clip / instant restore | file_mapping.rs; (per directive) | Phase-2 already-shipped catalog |

**Confirmed-absent-and-not-worth-building this cycle:** mmap remote reads (positional pread/io_uring + Arrow buffers already give zero-copy decode; mmap over remote is the wrong tool — competitive analysis confirms the per-block JNI path is *avoided*, so there is no mmap need). Negative-cache for missing blocks (forst-rs's disjoint-range premise + version pinning means a "missing block" is a structural impossibility on a pinned snapshot, not a repeated miss — no benefit). Stream-object pooling (ForSt §2.3) — forst-rs positional reads need no per-file stream pool (structurally avoided).

---

## 5. Cycle-3 re-rank confirmation (directive item 4)

**The Cycle-3 concurrent SST-reader OPEN fan-out REMAINS the top unbuilt item.** Justification from this cycle's full scan:

1. It attacks the paper's **#1 ranked loss term** (cache-miss remote reads on the heavy-I/O tail, L1×L4 — competitive analysis §1.1) on the **binding query class** (joins/OVER: q7/q9/q19/q20). No other gap touches a higher-ranked loss term in this repo (the #2 +30% async CPU is shared on the Flink side and not engine-fixable here; the #3 JNI path is already avoided).
2. It is **additive to the already-shipped cold-prime** — cold-prime parallelized the first *data block*; the OPEN (footer+index) RTTs are *still serial*, so this captures a latency term that ships today uncaptured.
3. It is the **lowest-risk** high-leverage item: timing-only, byte-identical when OFF, reuses the read-I/O pool and the exact cold-prime guard pattern → the byte-identity gate harness already exists.
4. **Nothing in the scan out-ranks it.** Partial-merge-on-write (#2) has comparable *leverage* but: (a) it's a *different* (write/agg) class, not the binding read class; (b) it carries an MVCC-snapshot hazard; (c) MEMORY warns the merge-chain bottleneck may be a 5M-scale artifact — so it must be **micro-benched before building**, whereas the OPEN fan-out's benefit is already modeled (the cold-prime numbers) and structurally certain. Therefore #2 *follows* #1.

**Recommended build order once the box is green-lit:** (1) Cycle-3 OPEN fan-out → (2) micro-bench partial-merge-on-flush to settle the artifact question, build only if it survives at flush-firing scale → (3) extend read-IO parallelism to the single-iterator path → (4) checkpoint parallel-PUT micro-bench on mock S3 → (5) suffix always-local rule + epoch-decay (the §2.1/§2.2 completeness items).

---

## 6. Constraints carried into any future implementation

E2E vectorized/batch/zero-copy, prefer Arrow, forbid `byte[]`/memory-copy and per-key/record execution. Config matches ForSt (`noflush=false`, `write_buffer_size=1G`); no config change unless it helps ALL queries. Every flag default-OFF and byte-identical when OFF. Mini-benches use simulated S3 (`FRS_MODEL_BW_MBPS=6250`) or churn/FFI micro-bench — never NexMark, never real S3 (env has bad S3 perf). NexMark runs only after Phase-1 measurement completes and is green-lit.

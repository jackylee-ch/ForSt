# Paper Coverage Audit — *Disaggregated State Management in Apache Flink 2.0* vs forst-rs

**Date:** 2026-06-14
**Owner:** PMC-2 (Phase-2 Disaggregated State)
**Paper:** Mei et al., *Disaggregated State Management in Apache Flink 2.0*, PVLDB 18(12):4846-4859, 2025 (read end-to-end this cycle: §1-§9, Figures 1-13, Tables 1-2).
**Base:** `origin/forst-rs` tip `5192e1ba8`.
**Method:** re-read the PDF cover-to-cover; for every disaggregated-state mechanism the paper describes, classify forst-rs as **COMPLETE / PARTIAL / GAP / N-A** with `file:line` evidence. Cross-checked against the shipped ledger, `2026-06-13-forst-optimization-catalog.md`, and `2026-06-14-disagg-s3-lock-assessment.md`. No claim without evidence.

---

## 0. Headline

**There is no genuinely-missing, high-leverage, in-lane (cache/checkpoint/routing) paper optimization left as a GAP** — the disaggregation surface is essentially captured. The audit found:

- **§5 ForSt state backend (the engine-relevant section): fully captured.** UFS file-mapping, link checkpoint (zero-upload), instant restore + rescale, remote compaction, multi-tiered cache, block cache, async flush↔upload — all **COMPLETE** with tests. The two remaining `.vlog`-class lifecycle gaps (GC/tombstone, rescale-clip) identified in the lock assessment are **space-hygiene under disagg, not paper mechanisms** — the paper does not describe KV-separation; that is a forst-rs-specific feature whose disagg-lifecycle parity is an internal completeness item, not paper coverage.
- **§4 Async execution (AEC, Key-Accounting-Unit, async-draining, Epoch Manager, watermark handling): N-A to the storage engine** — these are Flink **runtime** mechanisms (Java `StreamTask`/`AEC`), not state-backend mechanisms. forst-rs is the ForSt-equivalent **storage engine**; the async runtime lives on the Flink/JNI side (shared, per the competitive analysis L5 term). Correctly out of forst-rs scope.
- **One paper cache mechanism was under-captured and is now closed this cycle:** §5.4's **History-Based Policy** has *two* halves — (a) LRU eviction [COMPLETE] and (b) a **frequency-decayed re-admission** signal. forst-rs's `AdmissionTracker` modelled (b)'s decay with a **FIFO stand-in**; the catalog #2.3 item asked whether the true epoch decay is worth building. **This cycle's verify-before-build micro-bench proved it MATERIAL in the paper's target regime (+10.85 pp hit-rate at state ≫ cache), and it is now SHIPPED** (`FRS_CACHE_ADMISSION_EPOCH`, default-OFF, byte-identical). See §3.

**Net:** after this cycle, the **disagg/cache/checkpoint/routing optimization surface is EXHAUSTED** for paper coverage. The remaining engine leverage is in the **read/write path** (PMC-1's domain) and in the **`.vlog` lifecycle parity** punch-list (P1/P2 in the lock assessment — a hygiene track, not a throughput lever). This is the honest finding that informs consolidation: one perf track (read/write path) carries the remaining headline leverage.

---

## 1. Coverage matrix — paper §5 (ForSt disaggregated state backend)

This is the section that maps to forst-rs (the state-store engine). Status legend: **COMPLETE / PARTIAL / GAP / N-A**.

| Paper § | Mechanism | Status | Evidence (`file:line`) |
|---|---|---|---|
| §5.1 | **UFS logical→physical file abstraction**, file-operation delegation | COMPLETE | `forst-rs-io/src/file_mapping.rs:498` (`Mutex<Inner>`), register `:543`, refcount invariant `:31-33` |
| §5.1 | **Hard-link sharing** across DFS backends (UFS link semantics) | COMPLETE | `file_mapping.rs:560` link, `:600`/`:608` unlink; 3172× link-vs-copy bench (ledger) |
| §5.1 | **Consistent object visibility** / reference counts across backends | COMPLETE | refs derived from logical map (idempotent replay) `file_mapping.rs:31-33`; journal-before-mutation `:43-52` |
| §5.2 | **Faster checkpoint = hard-link, zero data copy at barrier** (Fig 8 ①) | COMPLETE | `db.rs` `create_incremental_checkpoint_linked`; link SST `:7260`, link vlog `:7285`; bench asserts 0 upload bytes (`nexmark_disagg_s3.rs:512`) |
| §5.2 | **Register linked copies in JM** (Fig 8 ②) | COMPLETE | mapping trailer embed `db.rs:7295`; `sync_journal` `:7287` |
| §5.2 | **JM delegates deletion to UFS; physical delete only at refcount 0** (Fig 8 ④⑤⑥) | COMPLETE | `file_mapping.rs:600`/`:608` unlink + refcount-0 HARD-STOP `:1000`; tombstone defer `:761` |
| §5.2 | **Instant recovery / rescale via linked copies (no download)** | COMPLETE (SST) | `db.rs` `open_from_linked_checkpoint_instant…`; adopt `:8065`; IT `db.rs:22840` byte-exact; crash-retry idempotent `:22882` |
| §5.2 | **Rescale (scale-in / scale-out / cross-cluster)** | PARTIAL | clipped/rescale restore `db.rs:8202`; layer-1 file clip `:8341`, layer-2 read clip `:8328`; SST clip tested `:23180`. **vlog clip-reclaim is a GAP** (lock-assessment H2 — forst-rs KV-sep, not a paper mechanism) |
| §5.3 | **Remote compaction (offload to stateless compactor service)** | COMPLETE (emulated) | `compaction_executor.rs:109` `RemoteEmulatedCompactionExecutor`, offload `:142-169`; round-trip test `:668`. Real remote endgame user-gated (matches paper's "experimental feature [36]") |
| §5.3 | **Compaction triggered by Flink 1.x mechanism, metadata round-robin to compactors** | COMPLETE (local + emulated offload) | trigger via engine flush/size; offload path serialize-round-trips a descriptor `compaction_executor.rs:111` |
| §5.4 | **Multi-tiered cache (memory LRU + file-based secondary on local disk)** | COMPLETE | block cache (memory, CLOCK) `cache/clock.rs:93-300`; file cache (secondary) `local_cache.rs:242` LRU evict `:1043-1061` |
| §5.4 | **Block-based cache (memory) + memtable + parallel read** (Fig 7) | COMPLETE (block/memtable) / read-path is PMC-1 | block cache `cache/mod.rs:88-129`; **"Parallel Read" (Fig 7)** is the read-path item — batch path PRESENT (`db.rs:9085` `FRS_RS_READ_IO_PARALLELISM`), single-iterator path PARTIAL (catalog §2.2, PMC-1 domain) |
| §5.4 | **History-Based Policy — LRU eviction** | COMPLETE | `local_cache.rs:1043-1061` LRU evict; generation-stamped O(1) touch `:233` |
| §5.4 | **History-Based Policy — frequency-decayed re-admission** ("access frequency over the preceding minute … exceeding a threshold periodically loaded back; mitigates thrashing") | **COMPLETE (this cycle)** | count-to-promote + promoteLimit anti-thrash `local_cache.rs:182-222`; **epoch-decayed eviction credit `local_cache.rs:227-245` (`FRS_CACHE_ADMISSION_EPOCH`, shipped this cycle)** — replaces the FIFO stand-in for the decay half |
| §5.4 | **Pluggable cache policy** | COMPLETE | `CachePolicy` `local_cache.rs:93-140` env-built, flag-gated |

### Read-path mechanisms the paper attributes to the backend (PMC-1 domain — listed for completeness)

| Paper § | Mechanism | Status | Note |
|---|---|---|---|
| §5.4 / Fig 7 | **Parallel read** (depth>1 over remote misses) | PARTIAL | batch path shipped; single-iterator path is catalog §2.2 — **PMC-1's read path**, out of this lane |
| §5.2 / Tab 1 | **Hide remote read latency** (68 µs local vs 23 ms OSS) | PARTIAL | scan cold-prime + open-fanout shipped (`FRS_SCAN_COLD_PRIME`/`FRS_SCAN_OPEN_FANOUT`, `db.rs:209`/`:235`); steady-state single-iter is PMC-1 |

---

## 2. Coverage matrix — paper §4 (Async execution model) — N-A to forst-rs engine

These are **Flink runtime** mechanisms, not state-backend mechanisms. They live in the Java `StreamTask` / Async Execution Controller, above the JNI boundary. forst-rs is the **storage engine** (the ForSt-equivalent), so these are correctly **N-A** to this repo. They are listed so the audit is exhaustive against the paper.

| Paper § | Mechanism | Status | Why N-A to forst-rs |
|---|---|---|---|
| §4.1 | Async Execution Controller (AEC) — non-state / state-access / callback stages | N-A | Flink runtime (Java); the engine just serves async point-gets/scans. The competitive-analysis L5 (+30% CPU) is shared on the Flink side. |
| §4.3 | Key Accounting Unit (per-key in-flight serialization) | N-A | Flink runtime per-key ordering; the engine is key-agnostic at the I/O layer. |
| §4.4 | Async draining at checkpoint barrier (exactly-once compat) | N-A | Flink checkpoint coordinator; the engine provides the link-checkpoint primitive (§5.2, COMPLETE) it builds on. |
| §4.5 | Epoch Manager (watermark / event-time order under async) | N-A | Flink watermark layer. (Note: the timer-index / refill-floor work in MEMORY is the Flink-side ForStRs queue, separate from this engine repo.) |
| §4.2 | Async programming model (`asyncUpdate`/`asyncGetEntries`/`THEN`) | N-A | Flink state API; the engine exposes batch/vectorized get/scan that the async layer drives. |

**Verdict:** §4 is entirely Flink-runtime. forst-rs provides exactly the engine primitives §4 needs (async-friendly batch get/scan, link checkpoint) and avoids the §4 CPU tax's #3 sub-term (per-block JNI) by being native Rust. No GAP in forst-rs's scope.

---

## 3. The one under-captured mechanism — closed this cycle (#2.3)

### 3.1 What the paper says (§5.4)

> "For loading, access frequency over the preceding minute is monitored. Files on the remote store with an access frequency exceeding a predefined threshold are periodically loaded back into the cache. The LRU-based eviction policy, while simple, has proven effective …, while the frequency-based loading policy efficiently mitigates the cache thrashing problem."

So the History-Based Policy = **LRU evict** (COMPLETE) **+ a frequency signal that decays over a sliding window and re-admits / mitigates thrashing** (the half in question).

### 3.2 What forst-rs had

`AdmissionTracker` (`local_cache.rs:142-177`) implements ForSt's `FileBasedCache` count-to-promote + promoteLimit anti-thrash cap, but modelled the **decay** of the per-key eviction (thrash) credit with a **FIFO stand-in**: a credit ages out only when `tracker_cap` (65 536) *distinct* keys push it out of the deque. With `tracker_cap` ≫ working set the credit **never decays within a run** — so a medium-hot key that briefly thrashed early crosses `promote_limit` and is then **permanently blocked from re-admission**, even after it becomes genuinely hot again. That is the opposite of the paper's "mitigate thrashing": it *creates* a permanent cold spot.

### 3.3 Verify-before-build micro-bench (catalog §2.3 mandate)

New bench `crates/forst-rs-bench/src/bin/cache_admission_epoch.rs` — a **contention-robust synthetic Zipf access-trace hit-rate simulation** (NOT NexMark): an LRU cache of `cache_keys` capacity over a Zipf(`theta`) trace of `key_space` ≫ cache keys, replayed identically (seeded xorshift, shuffled rank→key) against **FIFO-stand-in** vs **epoch-decay** (halve all eviction credits every `period` global evictions). Sweeps skew × cache-ratio so the verdict is not a single-point artifact.

**Results (4 M ops, key_space=100 000, promote=2, evict_limit=3):**

```
 theta   cacheN   hit_FIFO  hit_EPOCH   delta_pp   x_more
  0.70     2000      8.37%     16.84%     +8.47   2.012x
  0.70     5000     16.00%     26.27%    +10.28   1.642x
  0.70    10000     25.45%     36.29%    +10.85   1.426x
  0.90     2000     33.61%     42.86%     +9.25   1.275x
  0.90     5000     44.47%     53.23%     +8.76   1.197x
  0.90    10000     54.57%     62.02%     +7.45   1.136x
  1.10     2000     68.62%     73.72%     +5.10   1.074x
  1.10     5000     76.80%     80.54%     +3.75   1.049x
  1.10    10000     83.03%     85.27%     +2.24   1.027x
  1.30     2000     90.41%     91.91%     +1.50   1.017x
  1.30     5000     94.06%     94.67%     +0.60   1.006x
  1.30    10000     96.17%     96.21%     +0.04   1.000x
max delta = +10.85 pp   VERDICT: MATERIAL
```

**Interpretation (intellectually honest):**
- **MATERIAL in the paper's target regime** (state ≫ cache, low-to-moderate skew θ≤0.9): +7 to +11 pp hit-rate. This is exactly the **1 GB-cache-over-terabytes** regime the paper's §5.4 and §6.2 describe (Fig 13: "the 1 GB local disk cache is not enough to hold the state").
- **Negligible when cache ≥ hot set** (high skew θ=1.3 / high cache-ratio): +0.04 to +1.5 pp — which is the current **dev-box** regime (competitive analysis A4). So the benefit is regime-dependent, and the feature is correctly **default-OFF**.
- **Robust to tuning:** re-run with a 4× gentler period (`FRS_CACHE_ADMISSION_EPOCH=40000`) still gives max +10.43 pp — the effect is **structural** (FIFO permanently blocks; epoch lets credit fade), not an artifact of an aggressive period.

Because the charter is to capture **every paper optimization** and this is the paper's explicit cache-thrashing-mitigation mechanism — material in the paper's own regime — it clears the bar.

### 3.4 What was shipped

`FRS_CACHE_ADMISSION_EPOCH` (default-OFF, byte-identical when OFF), an extension of `FRS_CACHE_ADMISSION`:
- `AdmissionParams::epoch_evicts: Option<u64>` (`local_cache.rs`) — `None` = legacy FIFO-only.
- `AdmissionTracker::evicts_since_decay` counter + `decay_evictions()` — halve every eviction credit every `period` global evictions, drop zeroed keys (map + order deque reclaimed → still bounded).
- Wired in `CachePolicy::from_env` (`FRS_CACHE_ADMISSION_EPOCH=0`/unset = OFF; positive = period).
- TDD: 3 unit tests — `admission_epoch_off_never_decays_evictions` (byte-identical-OFF: credit strictly monotonic, key stays blocked, counter never advances), `admission_epoch_on_decays_and_unblocks` (credit halves at boundary, blocked key re-admittable), `admission_epoch_drops_zeroed_keys` (map + deque reclaimed). 16/16 admission tests + 480/480 storage-lib tests green; fmt + clippy clean.

---

## 4. Cross-check against shipped ledger + catalog + lock assessment

| Source | Item | This audit's reconciliation |
|---|---|---|
| Catalog §4 (shipped) | cold-prime, open-fanout, batch read-IO parallelism, local cache, admission, bg-exempt, restore pre-seed, block cache, prefetch, hard-link ckpt, UUID keys, remote compaction, bounded bg compaction, async flush↔upload, zero-copy rows, FileMappingManager/WAL-delta/rescale-clip/instant restore | All confirmed COMPLETE against paper §5 (table §1 above). |
| Catalog §2.1 (#1) | concurrent SST-reader OPEN fan-out | SHIPPED 2026-06-14 (`FRS_SCAN_OPEN_FANOUT`). Maps to §5.4 Fig-7 "Parallel Read" cold-start term. |
| Catalog §3.2 (#4) | checkpoint parallel multi-file PUT | SHIPPED (`FRS_CKPT_PARALLEL_UPLOAD`, commit `307938016`; bench `checkpoint_upload`). Maps to §5.2 flush-time upload. |
| Catalog §3.3/§3.4 (#5a/#5b) | non-SST always-local; space-based cache limit | SHIPPED (`router.rs:186` is_remote_file SST-only + `FRS_REMOTE_NONSST_LOCAL`; `FRS_CACHE_SPACE_LIMIT_MB` commit `5192e1ba8`). Maps to §5.1 file-ownership + §5.4 dual-limit. |
| Catalog §2.3 (#2.3) | epoch-decayed cold-list counts | **CLOSED this cycle** — verified MATERIAL, SHIPPED (`FRS_CACHE_ADMISSION_EPOCH`). Maps to §5.4 frequency-based-loading decay half. |
| Catalog §3.1 (#2) | partial-merge on write/flush | **Read/write path (PMC-1 domain), NOT disagg/cache** — and the paper does not describe it (it is an LSM/merge-operator optimization, not a disaggregation mechanism). Correctly out of PMC-2's lane and out of paper-coverage scope. |
| Catalog §2.2 (#3) | single-iterator read-IO parallelism | Read path (PMC-1). Maps to §5.4 "Parallel Read" steady-state. Out of lane. |
| Lock assessment GAP-A/H1, GAP-B/H2 | vlog GC/tombstone; vlog rescale-clip reclaim | **forst-rs KV-separation lifecycle parity, NOT a paper mechanism** (the paper has no KV-separation). Space-hygiene, not a paper-coverage GAP. Remains the P1/P2 punch-list for the vlog class. |

---

## 5. GAP verdict + exhaustion finding

**Paper-coverage GAPs (in PMC-2's cache/checkpoint/routing lane): NONE remaining.**
Every §5 backend mechanism is COMPLETE or PARTIAL-with-the-remainder-out-of-lane; §4 is N-A (Flink runtime). The last under-captured cache mechanism (§5.4 frequency-decay) is closed this cycle.

**The disagg / cache / checkpoint / routing optimization surface is EXHAUSTED for paper coverage.** What remains is two non-paper tracks:

1. **Read/write path (PMC-1's domain):** the §5.4 "Parallel Read" steady-state single-iterator term (catalog #3), and the non-paper partial-merge-on-flush (catalog #2). These carry the remaining **throughput** leverage. This is where a consolidated single perf track should focus.
2. **`.vlog` lifecycle parity (hygiene, not throughput):** lock-assessment P1 (vlog GC/tombstone) and P2 (vlog rescale-clip reclaim). These prevent a monotonic remote-space leak / rescale-OOM under KV-separation but are **not** throughput levers and **not** paper mechanisms. They gate "lockable," not "faster."

**Recommendation (informing consolidation):** the honest finding is that **PMC-2's lane is done** for paper optimizations. The remaining headline leverage is in the read/write path (PMC-1). Consolidating to one perf track (read/write path) is justified; PMC-2's residual work is the vlog-class hygiene punch-list (a correctness/safety track, separable from perf).

---

## 6. Constraints honored

E2E vectorized/batch/zero-copy; no `byte[]`/copy, no per-key/record exec (the epoch decay is O(tracked-keys) amortized to O(1) per eviction, no per-record cost). Config matches ForSt (`noflush=false`, `wbuf=1G`); the epoch feature is a uniform engine policy, **no per-query config**, default-OFF, byte-identical OFF. Micro-bench is pure-CPU synthetic Zipf — **no NexMark, no real S3**. Flag-gated, TDD, fmt + clippy + suites green.

---

## Appendix — artifacts this cycle
- New bench: `crates/forst-rs-bench/src/bin/cache_admission_epoch.rs` (+ Cargo.toml `[[bin]]`).
- Impl: `crates/forst-rs-storage/src/local_cache.rs` (`AdmissionParams::epoch_evicts`, `AdmissionTracker::{evicts_since_decay, decay_evictions}`, `from_env` wiring, 3 unit tests); `cached_fs.rs` + 3 test initializers updated to `..AdmissionParams::default()`.
- Paper read in full: `/Users/lijunqing/Documents/论文/flink/Disaggregated State Management in Apache Flink®2.0.pdf` (pages 1-14, all sections/figures/tables).

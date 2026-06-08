# Compaction-efficiency implementation plan (Phase-1 close lever)

> **For agentic workers:** Use superpowers:subagent-driven-development or executing-plans. Each task = implement → UT (correctness, in-process) → 8c/32g e2e exact-count (`out_rows` via measure-sql) + time → document. Steps use `- [ ]`.

**Goal:** Cut forst-rs compaction CPU/write-amp so q4/q9/q11/q19/q20 reach the bar (≥0.8× RocksDB or ≤+50s, faster than ForSt) on 8c/32g. This is THE unified lever (see `docs/superpowers/specs/2026-06-08-forst-rs-parallel-coalesced-readpath-design.md` + `2026-06-07-q19-prefix-bloom-vs-compaction-design.md`).

**Architecture:** forst-rs Rust engine compaction is more expensive per byte than mature C++ RocksDB (merge-CPU + write-amp). Default merge = `compaction.rs::run_streaming` (k-way heap over `SstBlockCursor`). Parallelism already refuted → the lever is per-byte COST, not concurrency.

**Verification harness (DONE this session):** measure-sql.sh captures `out_rows` (sink vertex read-records) → backend A/B on output counts. RocksDB q19 8c/32g = 305s / out_rows 92,000,000 (accuracy ref). forst-rs q19 = 455s / 92,000,000 (accurate). q4 = 621s. These are the before-numbers.

---

## Task 1: Profile the compaction threads in isolation (measure before code)
**Files:** none (instrumentation only).
- [ ] Run q19 (or q4) on 8c/32g with the engine's CUM_GATHER_NS-style counters extended to time: heap-key copy, block decode, emit/write, merge-operand collect. (Counters already exist in compaction.rs — CUM_GATHER_NS; add CUM_MERGE_NS / CUM_EMIT_NS.)
- [ ] Attribute compaction wall to: (a) heap-key to_vec, (b) SstBlockCursor block decode, (c) output SST write/encode, (d) merge-operand collection. Pick the dominant for Task 2.
- [ ] Expected: confirms whether the heap-key copy (Task 2) or block-decode/emit (Task 3) dominates.

## Task 2: Eliminate per-key allocation in the streaming merge heap
**Files:** `crates/forst-rs-engine/src/compaction.rs:596-604` (heap construction + advance).
- [ ] Replace `BinaryHeap<HeapKey{key: Vec<u8>, idx}>` (fresh `to_vec()` per key) with an indexed min-heap over cursor indices whose comparator reads a `keys: Vec<Vec<u8>>` slot per cursor; on `advance`, `keys[idx].clear(); keys[idx].extend_from_slice(cursor.key())` — REUSES the per-cursor buffer (zero new alloc/key). Safe (no unsafe): a small hand-written binary heap of `usize` with a `cmp` closure over `&keys`.
- [ ] UT: feed N inputs with known overlapping keys; assert merged output byte-identical to the current heap path (reuse existing compaction tests + add an ordering test).
- [ ] 8c/32g: q19 + q4 time, `out_rows` unchanged (92M / q4 count). Expect compaction-CPU drop.

## Task 3: Lower write-amplification (compaction picking)
**Files:** `crates/forst-rs-engine/src/db.rs` (compaction trigger/picking, ~3960/5564 region).
- [ ] Audit L0→L1 picking: ensure it picks the minimal overlapping L1 set (RocksDB-style) rather than rewriting wide ranges. Measure write-amp (bytes written / bytes ingested) before/after via a counter.
- [ ] UT: picking selects expected file set on a synthetic version.
- [ ] 8c/32g: q4 (write-heavy) time + out_rows; expect the biggest q4 win.

## Task 4 (secondary): CF prefix bloom for prefix-scan SST skip
**Files:** `sst/{writer,reader,footer,bloom_filter}.rs`, CF options plumbing (Java backend → FFI → engine), `db.rs build_lazy_prefix_key_stream`.
- [ ] Per design `2026-06-07-q19-prefix-bloom-vs-compaction-design.md` lever A1: CF option `prefix_bloom_len`; writer inserts `hash(key[..N])`; scan checks `hash(prefix[..N])` when `prefix.len()>=N`. Dual-version SST compat (old SSTs: no prefix bloom → no skip).
- [ ] UT: prefix bloom skips non-matching SSTs; byte-identical scan results.
- [ ] 8c/32g: q19/q9/q20 read-open time; expect ~40-55s on q19.

## Task 5: Memory model so q9/q20 finish 100M (interleave — needed to measure)
**Files:** per `2026-06-08-phase1-close-readamp-memory-model-design.md` (write-stall backpressure + compaction-transient bound). fadvise + 8c/32g config budget already landed.
- [ ] q9 must finish 100M under cgroup 32g to be measured. Bound intake (effective write-stall) so anon stays <28GB.
- [ ] 8c/32g: q9 FINISHES, out_rows == RocksDB.

## Task 6: Full 3-backend 8c/32g sweep with accuracy
- [ ] q0-q22 × {rocksdb, forst-rs, forst} on 8c/32g; capture time + out_rows each.
- [ ] Gate: forst-rs per-query ≥0.8× RocksDB or ≤+50s AND faster than ForSt AND out_rows match. Then Phase 1 closes → Phase 2.

## Constraints
Zero-copy/Arrow/batch only; no byte[]/per-record; `#![forbid(unsafe_code)]` (use safe constructs / nix); each backend with its own timer on 8c/32g; commit only when asked; revert diag instrumentation (mem-diag, jemalloc-ctl, RSS sampler, FRS_DISABLE_MAPSTATE_CACHE passthrough) before commit; keep ≥400GB disk free.

# Batch/Vectorized SST Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development or
> superpowers:executing-plans. Steps use `- [ ]` checkboxes.

**Goal:** Eliminate the SST-writer per-key inner loop so flush AND compaction write SSTs at
RocksDB-class throughput (target flush ≤ a few ms/MB-input, from ~30), closing the Phase-1
write-heavy gap (q17 11×, q18 DNF) — with byte-identical SST output (accuracy preserved).

**Architecture:** `StreamingSstWriter::add_batch` (forst-rs-storage/src/sst/writer.rs:641) takes
Arrow columns but loops per-row through `add_internal` (:225): per-key Arrow-builder re-append
(redundant copy of already-columnar input), per-key `Sbbf::hash_key`, per-row bound checks, periodic
`flush_block` (encode_data_block + lz4 + sink write). Both flush (FlushJob::run) and compaction
(compact_l0_for_cf) write via this writer, so one fix lifts both.

**Tech stack:** Rust, Arrow arrays, forbid-unsafe crate. lz4/zstd compression. Bloom = Sbbf.

---

### Task 1: Confirm the dominant sub-cost (verify-before-fix GATE)
**Files:** Modify `crates/forst-rs-storage/src/sst/writer.rs`; read in `crates/forst-rs-engine/src/db.rs` prof line.

- [ ] **Step 1:** Add storage-crate global atomics (ns) split across the writer phases:
  `SST_BUFFER_NS` (the per-row builder append in add_internal), `SST_ENCODE_NS` (encode_data_block),
  `SST_COMPRESS_NS` (lz4), `SST_SINKWRITE_NS` (sink.out.append). Public getters.
- [ ] **Step 2:** Engine `prof_diag_str` (db.rs) reads + logs them (extend the FRS_MEM_DIAG line).
- [ ] **Step 3:** Build .so, run q17 (FRS_MEM_DIAG=1, committed config) ~120s, read the split.
- [ ] **Step 4:** RANK the phases. The dominant one(s) determine which vectorization to do. DO NOT
  proceed to Task 2 until the hotspot is known. (Expected: encode + buffer-copy dominate; confirm.)

### Task 2: Batch the bloom hashing (low-risk, isolated)
**Files:** `writer.rs` add_batch + `crates/forst-rs-storage/src/sst/bloom_filter.rs`.
- [ ] Add `Sbbf::hash_keys_into(keys: &BinaryArray, out: &mut Vec<u64>)` computing all hashes in one
  pass (SIMD-friendly loop); call it once per add_batch; remove the per-row push in add_internal
  (gate add_internal's push behind the legacy `add()` path).
- [ ] Unit test: bloom built via batch-hash returns the SAME membership as per-row for a fixed key
  set (byte-identical key_hashes vector).

### Task 3: Columnar block construction (the main lever — scope by Task 1 result)
**Files:** `writer.rs` (add_batch, flush_block, data_block encode), `data_block.rs`.
- [ ] Slice the input Arrow key/value columns into block-sized ranges by accumulated size WITHOUT
  re-appending into per-row builders — encode each block directly from the input column slices
  (zero-copy where the data-block format allows; bulk varint/offset writes).
- [ ] Preserve the EXACT on-disk data-block byte format (prefix compression, restarts) — verify by
  a golden test: an SST written via the new columnar path is byte-identical to the old per-row path
  for the same input batch (critical: readers must not change).
- [ ] Keep block-boundary semantics (block_size) identical.

### Task 4: Apply to the compaction writer path
- [ ] Ensure compact_l0_for_cf's SST output uses the same vectorized writer (it already uses
  SstWriterImpl) — confirm no separate per-row path remains.

### Task 5: Accuracy + performance verification (8c/32g, committed config)
- [ ] All storage SST unit/integration tests green (round-trip read == write).
- [ ] e2e: q17 out_rows == RocksDB (92M), flush ms/MB ≤ a few (from ~30), q17 wall ≪ 817s.
- [ ] Regression: q16/q19/q15 + a light query — out_rows correct, not slower.
- [ ] Re-run the OOM set (q9/q18) — measure new wall vs RocksDB/ForSt.

## No-placeholder note
Task 3's exact data-block format edits depend on Task 1's profile + reading data_block.rs encode;
the golden byte-identical test is the safety net that makes the columnar rewrite safe to land.

## Self-review
Covers the verified root cause (per-row writer loop), gates the risky rewrite behind a profile + a
byte-identical golden test (accuracy), and applies to both flush and compaction. write_buffer_size
stays 1024mb (tuned for other queries; not in scope).

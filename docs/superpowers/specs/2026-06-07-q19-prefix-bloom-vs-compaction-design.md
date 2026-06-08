# q19/q9/q20 read-open fan-out: prefix-bloom vs compaction (design + decision fork)

**Date:** 2026-06-07
**Status:** Designed; implementation GATED on the 8c/32g baseline (measure-before-implement).
**Scope:** forst-rs engine (`db.rs build_lazy_prefix_key_stream`, `sst/{writer,reader,footer,bloom_filter}.rs`).

## Measured root cause (host 18c/64GB)
q19 MAP_ITER (~5M calls): the prefix-scan **`open`** is 90% of MAP_ITER cost and grows ~6× with state.
The open loop (db.rs:6196-6241) iterates `overlapping_ssts_in_range`; per SST it does
`get_or_open_sst_reader` + `may_contain_range` (coarse, block-range only) + `first_block_ge` + pushes a
k-way merge source. L0 SSTs are time-slice flushes interleaving many auctions, so the prefix-neighborhood
block straddles `[prefix, prefix_upper)` → `may_contain_range` **false-positives** → many L0 SSTs are
opened/seeked/merged that hold nothing for the scanned auction. Fan-out grows as L0 fills toward
`l0_stop_trigger=64`.

BUT MAP_ITER is only ~22% of q19's 416s wall; the rest is the 92M-put write path + compaction. And the
host L0-cap A/B (64→12) cut the open 31% yet left wall flat (cost shifted to compaction). => the binding
constraint may be **compaction efficiency**, not the read prune.

## Two candidate levers
### A. Per-SST PREFIX BLOOM (read-side, precise prune)
Add a 2nd bloom over key-prefixes; `build_lazy_prefix_key_stream` calls `prefix_bloom.check(prefix)` to
skip SSTs not containing the prefix (vs the false-positive-prone `may_contain_range`).
**Crux = prefix-length policy** (scan prefix is var-len; bloom needs build-time transform len == scan
len). The engine is CF-generic and does not know the Flink key schema. Options:
  - A1 (RocksDB-style): CF option `prefix_bloom_len`; backend sets it per MapState CF (q19 auction key =
    keyGroup(2)+BIGINT(8)+ns → fixed). Build inserts `hash(key[..N])`; scan checks `hash(prefix[..N])`
    only when `prefix.len() >= N`. Needs options plumbing (Java backend → FFI → engine CF options).
  - A2 (generic, no config): at build, for each key insert its prefix at the boundary the WRITER can
    see — NOT possible without the scan boundary. Rejected.
  => A1 is the viable form. Est. q19 save ~40-55s (cuts the ~80s open ~50-70%); does NOT alone flip the
  bar (~208s target from 416s). Helps q9/q20 (same prefix-scan open).

### B. COMPACTION efficiency (write-side, the likely-binding constraint)
The L0-cap A/B says shrinking L0 helps reads but compaction eats the gain → forst-rs compaction is more
expensive per byte than RocksDB (the diffuse q4 finding: merge-CPU + write-amp). Levers: faster merge
(SIMD/vectorized k-way), lower write-amp (better picking), sub-compaction parallelism within the 8c cap.
This attacks the dominant ~78% of q19 AND q4's join write path. Higher ceiling, harder, diffuse.

## DECISION FORK (resolve with the 8c/32g baseline + a fresh in-regime profile)
The host numbers may not transfer (8c/32g + jemalloc-active changes memory pressure + compaction CPU
share). After the baseline:
- If q19/q9/q20 open fan-out still dominates AND compaction has headroom → do **A1** (prefix bloom) first.
- If compaction dominates the in-regime profile → do **B** first (it gates both reads and writes).
- Likely BOTH are needed (stacked campaign, per user directive). Order by the in-regime profile.

## Verification (whichever lever)
Engine unit tests (storage bloom round-trip; build_lazy_prefix correctness on overlapping L0) on host;
then MapState exact-count NexMark (q11/q12/q15) + q19/q9/q20 best-of-3 under 8c/32g (Linux .so).

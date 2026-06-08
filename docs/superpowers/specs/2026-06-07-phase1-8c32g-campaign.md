# Phase-1 8c/32g campaign: close the forst-rs per-query gap (multi-module)

**Date:** 2026-06-07
**Goal gate:** every NexMark query forst-rs-local ≥ 0.8× rocksdb OR ≤ +50s; else stacked architecture
upgrades (multiple modules simultaneously, regardless of cost). Then Phase 2 (Disaggregated State).
**Regime (MANDATORY):** Docker arm64 Linux, hard `--cpus=8 --memory=32g`. Correct backend+timer per run
(forst-rs → FORSTRS timer; no shortcuts). All verification independent (no pending/interactive).

## The regime change is decisive
Prior numbers were on bare host (18c/64GB) and are INVALID for the target. Enforcing 8c/32g exposed a
forst-rs collapse the host masked:

| q19 100M | host 18c/64GB | 8c/32g | 8c/32g + lz4 |
|---|---|---|---|
| rocksdb | 158 | 156.9 (flat) | — |
| forst (ForSt Java) | — | 170.1 (flat) | — |
| forst-rs | 406–449 | 728.6 | 462.3 |

rocksdb AND ForSt-Java stay flat under 8c/32g; **only forst-rs decays** (rate 738k→15k/s as state grows).
=> the decay is the **forst-rs Rust engine's memory behavior**, NOT disaggregated-state-fundamental.

## Root cause: 32g memory budget → OS page-cache starvation → SST reads hit disk
JVM heap 12GB + forst-rs NATIVE (block cache 2GB, resident shadow ≤2GB, memtables, **local SST cache
that DUPLICATES local storage**) all count against the 32g cgroup, squeezing the OS page cache that
serves `/tmp` SST reads. As state grows past what fits, reads go to disk → progressive decay. rocksdb/
ForSt-Java have smaller footprints (compression + mature caching) → fit → flat.

## Ruled out (measured, do not re-chase)
- **bg-pool threads** (compact=2/flush=1): marginal, decay persists → not CPU contention.
- **noflush=false**: decay persists identically → not memtable residency.

## Win #1 (landed as config): SST compression lz4
q19 728.6 → 462.3 (−36%). lz4 > zstd (497, zstd's CPU on 8c outweighs its smaller footprint). forst-rs
ran uncompressed while rocksdb uses snappy — a real disadvantage. **Make lz4 the 8c/32g default.**

## Stacked modules (the campaign — multiple simultaneously)
1. **Compression = lz4** (DONE, config; promote to engine default). −36% q19.
2. **Memory-budget config** (test next, container-gated): smaller TM heap 12288→8192m (state is
   off-heap; frees ~4GB for page cache) + block cache 2048→6144mb + cap local SST cache.
3. **Local-mode cache dedup (engine):** in pure-local mode the `cache-dir` duplicates `storage.uri`
   (file://) — wasted disk + write-amp + RAM. Skip/shrink the cache layer when storage is local. Cuts
   disk (helps the 400GB floor) AND write I/O (less decay).
4. **Read-amp reduction (engine):** prefix-scan `open` fan-out = 90% of MAP_ITER, reads many overlapping
   L0 SSTs/probe (coarse `may_contain_range` false-positives). Precise per-SST prefix-bloom prune (needs
   prefix-length via CF option from backend) → fewer SST block reads/probe → less disk I/O under cache
   pressure. Biggest engine lever; ForSt-Java's RocksDB has prefix bloom, forst-rs does not.
5. **Write-amp / compaction efficiency (engine):** less data written → smaller LSM → less to read.

## Per-query targets (8c/32g, bar = rocksdb×1.0 +50s, ideally ≤ rocksdb)
q19 ≤ ~207s (rocksdb 157); q11 ≤ ~150s (rocksdb 99.8); q20/q9/q4 TBD from baseline. Current best
forst-rs: q19 462 (lz4), q11 231 (lz4) — both still far over → modules 2–5 needed.

## Verification protocol (independent, per module)
- Correctness: engine+storage suites green (CURRENT: 378 storage + engine all pass); MapState exact-count
  NexMark (q11/q12/q15 exact 4.6M/92M) after any read/write-path change; forst-rs output == rocksdb.
- Perf: 8c/32g Docker, best-of-N where variance warrants; rate-trajectory (decay broken?) + wall.
- Disk: keep host ≥400GB (watchdog at 405GB; compact Docker.raw + clean after).

## Disk safety (user hard constraint ≥400GB free)
Docker.raw sparse (8.3GB now) but doesn't auto-shrink → largest run's peak is permanent until compacted.
Watchdog kills containers <405GB. q9/q4 (large state) run with noflush=false + capped local cache.
Post-tests: docker prune + compact Docker.raw + clean /tmp + cargo clean target-linux (+ host target/).

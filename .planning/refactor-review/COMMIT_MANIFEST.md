# ForSt-RS Refactor Review — Commit Manifest

## Goal
- 对 9 个功能 commits 各执行 120 轮 10-agent review loop
- 终止: 连续 10 轮 H=0 M=0 或达 120 轮
- 每 commit 增加 20-40 benchmark，**硬性 3-5x 超过 RocksDB C++**
- 数据一致性 + 准确性 + 极致性能三维并重

## Source
All commits originate from `forst-rs` branch at `~/code/github/ForSt`.
Target worktree: `~/code/github/ForSt-review/` on `review-loop` branch.

## Commits (logical grouping for review, cherry-pick from existing git history)

| # | ID | Scope | Source commits (forst-rs) | Benchmarks | Perf target | Done Criteria | Estimated Nexmark Contribution |
|---|-----|-------|---------------------------|-----------|-------------|---------------|--------------------------------|
| C1 | **common** | types, coding, checksum, arena, metrics, config | parts of `forst-rs-common` history | 20 micro: varint, crc32c, arena alloc | ≥3x RocksDB util | All 13+ benches in `forst-rs-bench/benches/C1/` pass `perf_gate` ≥3x; `H=0 ∧ M=0` × 10 consecutive rounds | Indirect: foundational utilities; perf wins compose into all queries. No direct query-gating |
| C2 | **io** | LocalFS, MemoryFS, AsyncIO, ObjectStore, Router | `forst-rs-io` parts | 15: read/write/seek, prefetch, async | ≥3x Flink FS | All benches in `forst-rs-bench/benches/C2/` pass `perf_gate`; `H=0 ∧ M=0` × 10 | Direct: improves remote-storage read/write throughput. Primarily impacts queries with high remote-state IO (q11–q14 windowed joins, q16 session-window) |
| C3 | **storage: sst** | SST writer/reader, bloom (SBBF), index, compression, block_header, footer, schema | `22290c5` + sst/* from earlier | 30: seq/random read/write, compressed vs uncompressed, bloom FPR | **≥3x RocksDB SST** (scan) / ≥1x (point) | All benches in `forst-rs-bench/benches/C3/` pass `perf_gate`; SST disk-format roundtrip lossless; `H=0 ∧ M=0` × 10 | Direct: gates all stateful queries. Largest single contributor to E2E speedup; impacts q0 through q22 |
| C4 | **storage: memtable + merge** | VectorizedMemTable, ListAppendMergeOperator | `136dde0`, `2bade9e`, memtable history | 20: insert/lookup/merge-chain resolution, Arrow batch vs SkipList | **≥3x RocksDB skiplist** | All benches in `forst-rs-bench/benches/C4/` pass `perf_gate`; ListAppend merge semantics verified against RocksDB; `H=0 ∧ M=0` × 10 | Direct: gates write-heavy queries. Impacts q5 (hot bidder), q7 (highest bid), q8 (monitor new users) |
| C5 | **storage: cache** | ShardedClockCache (16-shard, Clock eviction) | `89afd1c` | 15: hit/miss/insert/shard contention | **≥3-5x LRU**, ≥3x RocksDB block cache | All benches in `forst-rs-bench/benches/C5/` pass `perf_gate`; cache concurrency tested under 16+ threads; `H=0 ∧ M=0` × 10. **Also gated by Nexmark `baseline.json` existence per A1 §4.3** | Direct: gates queries with high temporal locality. Impacts q0, q1, q2 (recent state), q5 (hot bidder), q11 (session windowing) |
| C6 | **storage: version + checkpoint** | VersionSet (ArcSwap), VersionEdit, checkpoint blob | `37453b7` | 10: version switch, edit apply, checkpoint roundtrip | ≥5x RocksDB manifest | All benches in `forst-rs-bench/benches/C6/` pass `perf_gate`; checkpoint roundtrip lossless including empty/large states; `H=0 ∧ M=0` × 10 | Indirect: improves checkpoint latency, not steady-state query latency. Impacts E2E recovery time, not q0–q22 throughput |
| C7 | **engine: core** | DbImpl, ColumnFamily, WriteBatch, SnapshotView, WriteController, read/write/merge paths | `007d74f` (W12) | 40 E2E: put/get/delete/merge, batch × small/large, concurrent | **≥3-5x RocksDB** (batch/scan); ≥1x point | All benches in `forst-rs-bench/benches/C7/` pass `perf_gate`; cross-CF isolation verified; concurrent put/get under 16+ threads; `H=0 ∧ M=0` × 10 | Direct: gates all queries via batch path. Largest single E2E win expected; impacts every query |
| C8 | **engine: background** | Flush, Compaction (incl. SingleDelete conservative elision), CompactionFilter, FileDeletionGuard | `b595036` (W13-W15) + `aa61712` R3 fix | 20: flush throughput, compaction rate, TTL filter | ≥3x RocksDB | All benches in `forst-rs-bench/benches/C8/` pass `perf_gate`; flush+compaction concurrent stress test; TTL filter correctness; `H=0 ∧ M=0` × 10 | Indirect: keeps steady-state perf from regressing under load. Long-running queries (q11 session, q14 windowed joins) most affected |
| C9 | **FFI + Arrow + bench infra** | frs_* C ABI, Arrow C Data Interface zero-copy, forst-rs-bench scaffolding | `ee12651` (W16) + all `fix(ffi,*)` from R1-R2 | 25: FFI overhead, Arrow batch put/get, prefix_scan | ≥5x JNI | All benches in `forst-rs-bench/benches/C9/` pass `perf_gate`; FFI roundtrip lossless across types; Arrow zero-copy verified (no memcpy); `H=0 ∧ M=0` × 10. **Gated by C5 (Nexmark baseline) per A1 §4.3** | Direct via Arrow path: gates Java↔Rust boundary throughput. Impacts every query that crosses FFI (i.e., all of them) |

## Reconstruction plan

Each Cn is produced by:
1. `git cherry-pick` the source commits into `review-loop` branch
2. Squash to one "feature commit" per Cn
3. Add commit Cn+benchmarks: new file(s) under `forst-rs-bench/benches/<Cn>/`

## Review scope per commit

Scope for agent review per Cn:
- Code correctness (memory safety, concurrency, correctness edge cases)
- Data consistency (MVCC, crash-recovery, cross-CF isolation)
- Data accuracy (SST roundtrip, compression lossless, checksum, merge semantics)
- **Performance**: every claimed speedup has a benchmark. `cargo bench` must exit non-zero if perf target missed.
- Documentation, API stability, tests

## Termination

Per Cn:
- Continue reviewing until **10 consecutive rounds with H=0 AND M=0**, OR
- **120 rounds reached** (accept with documented L-level backlog).

Cross-commit: project considered DONE when all 9 Cn reach termination.

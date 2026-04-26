# Benchmark Baseline — RocksDB C++ + ForSt C++

## Baselines to measure against

### B1: RocksDB C++ (primary)
- **Version**: pin `v8.11.3` (stable LTS)
- **Build**: `git clone https://github.com/facebook/rocksdb && cd rocksdb && make release -j$(nproc)`
- **Path**: `~/code/baselines/rocksdb/`
- **C++ driver**: `benches/baselines/rocksdb/driver.cc` — takes workload JSON, emits latency+throughput CSV

### B2: ForSt C++ (secondary, where applicable — Flink state semantics)
- **Source**: `apache/flink` ForSt fork under `flink-statebackend-forst/native/`
- **Build**: Same as RocksDB (ForSt is RocksDB fork)
- **Path**: `~/code/baselines/forst-cxx/`
- **Driver**: similar to B1

## Baseline environment

- **CPU**: pin perf-governor=performance, disable SMT for deterministic numbers
- **Memory**: ensure 32GB+ available, no swap
- **Disk**: tmpfs for SST (`/mnt/tmpfs-bench`) to isolate engine from IO variance
- **Run**: `taskset -c 0-7 cargo bench` + baseline driver on same cores
- **Samples**: 30-50 per measurement, report p50/p99/mean/stddev

## Workload definitions

All benches produce JSON `workload.json`:
```json
{
  "kind": "put|get|scan|batch_put|...",
  "key_size": 16,
  "value_size": 100,
  "num_keys": 1000000,
  "distribution": "sequential|uniform|zipfian:0.99",
  "threads": 1,
  "duration_sec": 10
}
```

Every Rust criterion bench runs ONCE against RocksDB/ForSt C++ driver with the same workload.

## Perf target gate

For each Cn bench, `baseline.json`:
```json
{
  "bench_name": "sst_seq_read_100k",
  "rocksdb_ns_per_op": 850,
  "forst_cxx_ns_per_op": 870,
  "forst_rs_target_ns_per_op": 283,
  "speedup_target": 3.0
}
```

`forst-rs-bench/src/perf_gate.rs` compares criterion output to baseline; **exit 1** if `measured > target`.

## Benchmark categories per commit (210+ total)

### C1 common (20)
- varint encode/decode (u32/u64/zigzag), fixed32/fixed64
- crc32c 1KB/64KB/1MB blocks
- arena alloc 8B/128B/1KB
- metrics counter/gauge inc

### C2 io (15)
- LocalFS read/write 4KB/1MB/16MB
- MemoryFS read/write
- async_read_at prefetch
- ObjectStore mocked throughput

### C3 sst (30)
- **writer**: seq 1M keys, compressed vs uncompressed (LZ4/Zstd)
- **reader**: seq/random point lookup, scan w/ prefix
- **bloom**: build/query FPR
- **index**: seek near/far
- Compare to `rocksdb::BlockBasedTable`

### C4 memtable (20)
- insert 1M seq/random
- lookup 1M hit/miss
- merge chain resolution depth=1/10/100
- batch_insert 100/10k rows

### C5 cache (15)
- hit/miss 10M ops
- insert eviction pressure
- 16-shard contention 8 threads
- vs `rocksdb::LRUCache`, `rocksdb::ClockCache`

### C6 version (10)
- ArcSwap version switch under read contention
- VersionEdit apply 100 files
- Checkpoint serialize/restore 10MB blob

### C7 engine core (40)
- put 1M / get 1M / delete 1M
- batch_write 100-row × 10k iterations
- batch_get 1000 keys × 10k iterations
- merge-heavy workload (CDC-like)
- concurrent 8R+1W

### C8 engine background (20)
- flush throughput 1M rows
- compaction L0→L1 rate
- TTL filter 10M keys
- FileDeletionGuard pin/unpin contention

### C9 FFI (25)
- FFI call overhead (empty catch_unwind)
- frs_put/get overhead vs native Rust
- frs_batch_put_arrow 100k rows zero-copy
- frs_prefix_scan_arrow 1M match

## Publishing perf report

`cargo bench --bench ALL --output-json | tee perf-report-Cn.json`
→ `docs/perf/Cn-vs-rocksdb.md` auto-generated with table of speedups.

Target for project acceptance: **≥70% of benchmarks achieve ≥3x vs RocksDB**, remaining ≥1x (no regressions).

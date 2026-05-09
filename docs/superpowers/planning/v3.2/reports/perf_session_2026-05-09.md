# ForSt-RS perf measurement session — 2026-05-09

**Status:** ⚠️ **Absolute numbers captured; head-to-head RocksDB comparison BLOCKED on `librocksdb-sys` download.**

## What ran

`cargo bench -p forst-rs-bench --bench point_lookup` and `--bench write_throughput` against HEAD `<latest>`.

Hardware: ARM64 macOS (Apple Silicon, single-threaded criterion default).

### Point lookup

| Scenario | Median time | Throughput | Per-op |
|----------|-------------|------------|--------|
| `point_lookup_memtable/100000` | 8.85 ms (100k probes) | **11.30 Melem/s** | **88 ns** |
| `point_lookup_after_flush/1000` | 11.29 ms (1k probes) | 88.6 Kelem/s | 11.3 µs |
| `point_lookup_after_flush/10000` | 313 ms (10k probes) | 31.94 Kelem/s | 31.3 µs |

Memtable hits hit the 88-nanosecond range — that's hash-map speed. After-flush latency rises sharply because the read path traverses the SST + block cache miss path; this is the realistic warm-cache → cold-cache transition and matches the docs/design/2.4 BlockCache plan's expected cliff.

### Sustained write throughput

| Scenario | Median time | Throughput |
|----------|-------------|------------|
| `sustained_put/10000` | 2.95 ms | **3.39 Melem/s** |
| `sustained_put/50000` | 17.09 ms | **2.93 Melem/s** |

Holds 3 million puts/s sustained at 50k, with mild degradation at scale (memtable-flush triggers).

## Why no RocksDB comparison this turn

The `rocksdb_compare` bench scaffold (committed in the prior turn) is gated behind the `rocksdb-baseline` Cargo feature. Building it requires the `librocksdb-sys` crate, which vendors the C++ RocksDB sources (~50 MB compressed). Three consecutive `cargo` download attempts hit `[28] Timeout was reached (failed to download any data for librocksdb-sys v0.16.0+8.10.0 within 30s)` from `static.crates.io`.

This is a network-level issue with the local environment's connection to crates.io's CDN — not a code defect. **Workarounds for next session:**
- Increase `CARGO_HTTP_TIMEOUT` (tried 600s; still failed, suggesting transfer rate is the issue, not initial response)
- Use a vendored fork of `librocksdb-sys` checked into the repo
- Build RocksDB v8.11.3 manually (the source is already cloned at `/tmp/rocksdb-baseline/rocksdb`) and link against the static lib via `RUSTFLAGS="-L /path/to/rocksdb"`
- Use a different rocksdb Rust binding that doesn't vendor sources

## Honest perspective on the 3-5× KPI

The measured forst-rs absolute numbers (88 ns memtable-hit, 3.4 Melem/s sustained writes) are **in the regime where matching or beating RocksDB is plausible**:

- Typical RocksDB memtable point-lookup: ~500 ns – 2 µs on similar hardware (per facebook/rocksdb microbenchmarks)
- Typical RocksDB sustained sequential put: 200 K – 1 M ops/s on similar hardware

If you trust those external baselines as a proxy, forst-rs is **~6–25× faster on memtable hit** and **~3–17× faster on sustained puts**. **But that is a paper comparison** — different hardware, different RocksDB build flags, different Cargo profile. The KPI requires same-hardware, same-process, criterion-paired measurement, which is exactly what the `rocksdb_compare` bench was scaffolded to do.

**I will not claim the 3-5× number until the side-by-side bench actually runs.**

## What's reproducible right now

```bash
cargo bench -p forst-rs-bench --bench point_lookup -- \
  --measurement-time 5 --warm-up-time 2 --sample-size 30
cargo bench -p forst-rs-bench --bench write_throughput -- \
  --measurement-time 5 --warm-up-time 2 --sample-size 30
cargo bench -p forst-rs-bench --bench batch_ops    # additional bench available
cargo bench -p forst-rs-bench --bench checkpoint   # additional bench available
```

To run the comparison once the librocksdb-sys download issue is resolved:

```bash
cargo bench -p forst-rs-bench --features rocksdb-baseline --bench rocksdb_compare
```

The bench file is at `crates/forst-rs-bench/benches/rocksdb_compare.rs` — paired forst_rs / rocksdb measurement groups for `point_lookup` and `sequential_put`. Criterion will compute the delta automatically.

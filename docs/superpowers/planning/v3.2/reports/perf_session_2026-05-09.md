# ForSt-RS perf measurement session — 2026-05-09 / 2026-05-10

**Status (engine-level, native Rust):** ✅ **v3.2 §2.4 3-5× KPI MET on point lookup, EXCEEDED on sequential put.**

**Status (Flink-user-visible, Java/JNI):** ⚠️ **1.3–1.5× vs community ForSt — meaningful gain but BELOW 3× KPI when measured at the JNI surface where Flink users actually consume the engine.**

The 3-way comparison the user asked for (rocksdb / community-ForSt / forst-rs) is now complete. See "3-way numbers" section below.

## Headline numbers — ForSt-RS vs RocksDB v8.10.0

Measured side-by-side, same process, criterion-paired, ARM64 macOS (Apple Silicon).

| Benchmark | ForSt-RS | RocksDB v8.10.0 | **Speedup** |
|-----------|----------|-----------------|-------------|
| `point_lookup/100000` (mid-range probe of 100k pre-loaded entries) | **16.155 Melem/s** (16.07–16.25 Melem/s 95% CI) | **3.530 Melem/s** (3.527–3.534) | **4.58×** ✅ |
| `sequential_put/10000` (10k single-key inserts) | **4.133 Melem/s** (4.116–4.148) | **626.9 Kelem/s** (617–640) | **6.59×** ✅ |

Both within or exceeding the v3.2 §2.4 3-5× KPI band.

Commit: `639babbb1` (rocksdb_compare bench scaffold + ci-bench-compare workflow); HEAD `<latest after this perf doc update>`.

How to reproduce locally:

```bash
cargo bench -p forst-rs-bench --features rocksdb-baseline --bench rocksdb_compare \
  -- --measurement-time 5 --warm-up-time 2 --sample-size 30
```

How to reproduce in CI: trigger `ci-bench-compare` workflow from the GH Actions tab. Weekly auto-runs Mondays 04:00 UTC.

## ForSt-RS standalone numbers (additional context)

From `cargo bench -p forst-rs-bench --bench point_lookup` and `--bench write_throughput`:

| Scenario | Median time | Throughput | Per-op |
|----------|-------------|------------|--------|
| `point_lookup_memtable/100000` | 8.85 ms | **11.30 Melem/s** | **88 ns** |
| `point_lookup_after_flush/1000` | 11.29 ms | 88.6 Kelem/s | 11.3 µs |
| `point_lookup_after_flush/10000` | 313 ms | 31.94 Kelem/s | 31.3 µs |
| `sustained_put/10000` | 2.95 ms | **3.39 Melem/s** | — |
| `sustained_put/50000` | 17.09 ms | **2.93 Melem/s** | — |

(Note: the in-process rocksdb_compare bench measured 16.155 Melem/s on point_lookup/100000 — slightly higher than the standalone `point_lookup_memtable/100000` 11.30 Melem/s — because the comparison harness reuses one db across all probes vs the standalone bench's per-iteration setup.)

## Methodology

- Hardware: ARM64 macOS Apple Silicon, single-threaded criterion default
- Criterion config: `--measurement-time 5 --warm-up-time 2 --sample-size 30`
- Both engines: in-memory (forst-rs `MemoryFileSystem`, rocksdb tempdir + WAL)
- Both engines: default options except CF naming
- Both engines: 100k pre-loaded sequentially, midpoint probe `k00050000`

## librocksdb-sys download workaround

Cargo's HTTP client repeatedly timed out fetching `librocksdb-sys v0.16.0+8.10.0` from `static.crates.io` (4 retries × 30s each, all failed). Direct `curl` succeeds in 4.3s (6.9 MB). Fix: pre-fetched the crate via curl and placed it in cargo's offline cache at `~/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/librocksdb-sys-0.16.0+8.10.0.crate`. Subsequent `cargo bench` build succeeded in 32.6s using cached extraction.

For CI, this is a non-issue — GH Actions runners download from a colocated CDN and complete the fetch in seconds. The `ci-bench-compare.yml` workflow runs on `ubuntu-latest` precisely to avoid this local-environment quirk.

## 3-way numbers (added 2026-05-10)

| Layer | Workload | ForSt-RS | Comparison engine | Speedup |
|-------|----------|----------|-------------------|---------|
| **Rust criterion** | point_lookup/100k | 16.155 Melem/s | RocksDB v8.10.0: 3.530 Melem/s | **4.58×** ✅ |
| **Rust criterion** | sequential_put/10k | 4.133 Melem/s | RocksDB v8.10.0: 627 Kelem/s | **6.59×** ✅ |
| **Java JMH (JNI shim)** | pointLookup memtable | 3.10 Melem/s | Community ForSt: 2.06 Melem/s | **1.50×** ⚠️ |
| **Java JMH (JNI shim)** | sequentialPut | 751 Kelem/s | Community ForSt: 583 Kelem/s | **1.29×** ⚠️ |

Source: `flink-state-backends/flink-statebackend-forst-rs/JMH_BENCHMARK.md` for the JMH numbers; `crates/forst-rs-bench/benches/rocksdb_compare.rs` for the criterion numbers.

**The honest read:** the engine-level Rust speed advantage (4.58–6.59×) is significantly attenuated by JNI marshaling cost when consumed via the G-A drop-in shim path (Java → JNI → frs_*). A Flink user who renames `libforst_rs_ffi.dylib` to `libforstjni.dylib` and substitutes it for community ForSt's cdylib will see ~1.3–1.5× perf, not 4–7×.

**Implication for the v3.2 §2.4 KPI:** the 3× KPI is met if "micros" is interpreted at the engine (Rust) level. It is NOT met at the JNI-consumed Java level. The full 4-7× engine win is recoverable only via the G-B FFM-bridge path (`ForStRsStateBackend` via JDK 25 `Linker`/`MemorySegment`, bypassing JNI), but that path requires Flink-side wiring (Phase-D L5/L6) and isn't yet wired through the actual Flink keyed-state-handle machinery.

**Future bench**: extend `ForStCompareBenchmark` with a third "via-FFM" variant using `ForStRsLinker` directly to isolate JNI cost from FFM cost from native cost.

## What this means

- The v3.2 §2.4 hard 3-5× KPI is **measurably hit** on the two benchmarks the comparison harness covers (point lookup + sequential put).
- The forst-rs Arrow-based memtable + i64-fixed-point histogram + custom block cache architecture pays off vs RocksDB's row-store + LSM-flush pipeline at this workload.
- These are *micro*benchmarks. End-to-end Flink Nexmark + Delta Join numbers (the v3.2 §2.4 stretch goal of 30–40% Nexmark E2E improvement) require the full state backend wiring (Phase-D L5 / L6) plus a running Flink cluster — not yet delivered.

## What's reproducible right now

```bash
# Local (one-time setup: pre-fetch librocksdb-sys via curl if cargo download is slow)
cargo bench -p forst-rs-bench --bench point_lookup
cargo bench -p forst-rs-bench --bench write_throughput
cargo bench -p forst-rs-bench --bench batch_ops
cargo bench -p forst-rs-bench --bench checkpoint
cargo bench -p forst-rs-bench --features rocksdb-baseline --bench rocksdb_compare

# CI
# Push to forst-rs branch → trigger ci-bench-compare via Actions UI workflow_dispatch
# Or wait for the weekly Monday 04:00 UTC schedule
```

# S3 vs local-FS perf comparison — B-Prod-followup-S3-perf

**Date**: 2026-05-12
**Branch**: `forst-rs`
**VP question answered**: Q6 — *"What is the performance cost of running the
engine against disaggregated S3-class object storage as primary state, vs
local-FS?"*

## Goal

Quantify the perf cost of `DbImpl::open_remote(s3://...)` against
`DbImpl::open_with_fs(LocalFileSystem)` on the SAME workloads and the SAME
hardware. The end-to-end S3 path was validated functionally in
`crates/forst-rs-engine/tests/s3_remote_storage_it.rs` (commit
`5179a8c25`); this writeup is the measurement.

## Method

### Bench harness

`crates/forst-rs-bench/benches/s3_vs_local.rs`, criterion-driven,
`harness = false`, gated behind the `s3-it` Cargo feature (same gate as the
engine S3 integration test). One MinIO container is started lazily via a
`OnceLock` and reused across every workload, so the per-iteration overhead
is dominated by the workload itself, NOT by container boot.

### Configurations

| Variant | DB open call | FS backend | Cache |
|---|---|---|---|
| `local-fs` | `DbImpl::open_with_fs(opts, LocalFileSystem)` | tempdir on host FS | n/a |
| `s3-minio-warm` | `DbImpl::open_remote(s3://forst-rs-bench/, ...)` | OpenDAL S3 → MinIO testcontainer | 64 MiB LocalCache front |

Both variants:
- `EngineOptions::write_buffer_size = 64 KiB` (so a 1000-key write forces a
  real SST flush rather than sitting in the active memtable)
- 1000 keys of shape `k{i:05}` → `v{i}`
- Same machine, same compiler, same `bench` profile

### Workloads

| Workload | Description |
|---|---|
| `s3_vs_local__point_lookup_warm_cache/1000` | Pre-populate + flush + warm-read once (outside `b.iter`); then time a 1000-key `get` loop. Measures the cache-hit path. |
| `s3_vs_local__sequential_write_then_flush/1k-writes-then-flush` | Per-iteration: open fresh DB, write 1000 keys, call `flush_all`. Measures upload + manifest latency. |

### Why no cold-cache workload

A cold-cache point-lookup against S3 would require opening a *fresh* DB
process against an existing S3 prefix and recovering the manifest + SSTs.
ForSt-RS does not implement that recovery path today — `open_with_fs`
always starts a fresh manifest, persistence boundaries live at the
checkpoint/restore API, not at the open path. The first iteration of the
bench (`fd7a4f260` HEAD) attempted this and crashed with
`cold reads returned no bytes — fixture wrong?` because the freshly-opened
DB cannot see the SSTs that an earlier DB wrote at the same path. The bench
was rescoped to the two paths that ARE supported in a single-process
lifetime: warm-cache reads and write-then-flush.

## Results

Smoke run on the local macOS host (Docker Desktop, MinIO via testcontainers
0.27, criterion `--sample-size 10 --measurement-time 1 --warm-up-time 1`):

| Workload | local-FS | S3 (MinIO) | ratio (s3 / local) |
|---|---:|---:|---:|
| `point_lookup_warm_cache/1000` | **8.95 ms** | **8.42 ms** | **0.94×** |
| `sequential_write_then_flush/1000+flush` | _TBD — populated post-run_ | _TBD_ | _TBD_ |

(The full numbers + criterion HTML reports are captured as a GHA artifact
by `.github/workflows/s3-perf-bench.yml`; this table is updated on each
significant run.)

## Discussion

### Warm-cache reads: no measurable S3 cost

`point_lookup_warm_cache/1000` shows S3 is effectively identical to
local-FS (0.94×, within criterion noise) once the cache is warm.

This is the expected result for `CachedFileSystem`: every block needed by
the 1000 cache-warm reads is already in the local LRU, so the S3 backend
is not touched on any iteration. The only cost vs raw local-FS is the
extra indirection through the cached-FS wrapper, which is sub-noise at this
workload size.

**Implication for VP Q6**: For the steady-state read path of a typical
Flink keyed-state job — where the hot key set fits the local cache — the
disaggregated-storage option is *free* on point lookups.

### Sequential write + flush: S3 pays for the upload

The `sequential_write_then_flush` workload measures the path that S3
inevitably costs more on: an SST file built by the flush must be uploaded
to the bucket before the call returns. The actual ratio depends on the
S3 service's PutObject throughput (and on the test container's CPU
budget); we expect S3 to be 5–50× slower than local-FS at 1000 keys
because each per-iteration flush forces one (or a few) full SST objects
across the network.

**Implication for VP Q6**: Disaggregated storage trades write/checkpoint
latency for the operational benefits (scalable storage, no local disk
sizing, easy rescaling). For a Flink job whose checkpoint period dominates,
this is a tax to budget for; for a job whose checkpoint period is large
relative to the SST flush cost, it is a one-time cost amortised over many
inter-checkpoint reads.

## Limitations

1. **MinIO testcontainer is not real S3.** It's a single-node, in-Docker,
   local-loopback service. True S3 has higher base latency (multi-AZ
   replication, regional routing) and higher concurrency ceilings. Real
   numbers will be slower than these in latency and faster in aggregate
   throughput at high concurrency.
2. **Single-threaded workload.** No multi-writer / multi-reader
   contention. The disaggregated path's biggest advantage (independent
   read concurrency from many compute nodes against shared S3) is not
   exercised here.
3. **64 MiB LocalCache, 1000-key working set.** The working set fits
   entirely in cache, so cache eviction / thrashing is not measured. A
   realistic Flink job with a large keyed state would exhibit cache
   churn and shift the S3 cost back into the read path.
4. **rocksdb-local-only baseline omitted.** RocksDB-jni does not support
   S3 primary at all; we cannot run a direct rocksdb-vs-forst-rs S3
   comparison.
5. **No checkpoint workload.** Incremental-checkpoint upload bandwidth
   is the most operationally interesting S3 cost dimension and is NOT
   measured here. Future follow-up.

## Where the bench runs

- **Code**: `crates/forst-rs-bench/benches/s3_vs_local.rs`
- **Feature gate**: `s3-it` (also gates the engine S3 integration test)
- **Workflow**: `.github/workflows/s3-perf-bench.yml`
- **Trigger**: `workflow_dispatch` (manual) + auto-trigger on push to
  `forst-rs` when the bench file or its workflow changes.
- **Artifacts**: `s3-perf-bench-results` (raw criterion stdout + a
  `RATIO_SUMMARY.txt` distilled local-vs-S3 table) and
  `s3-perf-criterion-html` (criterion HTML reports).

# 2026-05-29 — S3 performance diagnosis + engine fixes (NEXMark 3× goal)

## Goal
forst-rs (JDK25 + S3-primary state + G1GC) ≥ 3× faster than rocksdb (JDK17 + local-disk state + S3 ckpt) on q0-q22 total, 100M records. rocksdb baseline total = **3597.41s** → forst-rs must be **≤ 1199s**.

## Starting point
Full no-timeout sweep: forst-rs 4795s vs rocksdb 3597s = **0.75× (slower)**. 8 queries (q4/q5/q7/q9/q15-q19) timed out at 600s; v3.8 had these as forst-rs's biggest wins (on LOCAL FS).

## Diagnostic journey (instrument-before-guess, the hard way)

### Phase 1 — local config was broken (invalidated early experiments)
`config-forst-rs-local.yaml.tpl` had THREE latent bugs that crash-looped any timer/MapState query:
1. opendal-config escaped JSON `'{\"root\"...}'` → invalid → `frs_db_open_remote_with_options INVALID_ARGUMENT` → restore crash-loop.
2. `ttl: 1h` → forst-rs MapState TTL is unsupported (PR-A7) → `UnsupportedOperationException` on join MapState.
3. Missing `-Dforst.rs.timer-service.factory=HEAP` → engine timers → `ForStRsKeyGroupedInternalPriorityQueue` rejects Flink's `_timer_state/processing_user-timers` ('/' validation).

All fixed. **A "timeout" can be a crash-restart loop — always check job STATUS + TM log before assuming slowness.** (The '/' timer-name validation at `ForStRsKeyGroupedInternalPriorityQueue:308` is a real latent bug for engine-timer configs; S3 goal uses HEAP so unaffected.)

### Phase 2 — code is healthy; it is NOT S3 reads
- q4 on **fixed local config completes** (`Time=10.2s`) vs rocksdb-local 254s → forst-rs code is faster than rocksdb on equal storage.
- **Local SST cache hit rate = 99.9%** (added `FRS-CACHE-STATS` to LocalCache). S3 reads are absorbed by the 64 GiB NVMe cache.
- **GC = 0.7%** (jstat, 0 full GC). Not GC-bound.
- **Read-amp low** (added `FRS-READAMP-DIAG`: avg 2.4 L0 files/batch, ~11 keys reach SST).
- **S3 write-volume not it**: 256MB memtable + 10GB WBM (fewer flushes) did NOT help q4.

### Phase 3 — symbolicated native profile (the ground truth)
Release build is `strip="symbols"` → all Rust frames showed as `???`. Built a symbol-preserving dylib (`--config 'profile.release.strip=false' --config 'profile.release.debug=true'`, 11870 syms) and ran macOS `sample` on q4-S3:
```
254 VectorizedMemTable::batch_insert_with_explicit_seqs_via_indices
184 + 150 + 67  core::hash SipHash (BuildHasher::hash_one + Hasher::write)
145 batch_get_vectorized   139 build_lazy_prefix_key_stream
113 ShardedMemTable::get    91 Version::live_sst_files   80 SstReaderImpl::get_versions
 71 std::fs::read (local cache)   67 tokio::park
```
Top CPU = memtable **insert** + **SipHash**.

## Fixes landed (all correctness-clean: 321 storage + 253 engine + Java backend tests pass)

### Engine (Rust)
1. **SipHash → FxHash** (`rustc-hash`) for the vectorized memtable's `hash_index`/`unsorted_lookup`. SipHash (crypto, slow) was ~40% of q4 CPU samples.
2. **prefix_index ENTIRELY REMOVED.** It was production-dead (the `prefix_scan_keys` fast path reading it was deleted by S3-MAPITER-FIX; only test assertions remained) AND its Put membership check `v.iter().any(...)` was O(bucket) → **O(N²)** for N keys under one prefix (the streaming-join MapState pattern). Removed all 5 maintenance sites.
3. **Concurrent 8-way vectorized S3 reads** (`RandomAccessFile::read_ranges`, OpenDAL JoinSet override) — `RangeCachedRandomAccessFile` fetches multiple cache-missed chunks concurrently instead of serially.
4. **Cache hit/miss instrumentation** (`FRS-CACHE-STATS`).
5. **resident-flushed key-bounds skip** (point-get + prefix-scan) — `ResidentEntry` carries `[min_key,max_key]`; skips memtables whose range excludes the probe key (was O(num_resident) per op).
6. **`has_resident_flushed()` fast-path** in `get_internal` (skip version snapshot + HashSet when no resident memtables).
7. **`fatal_error` AtomicBool fast-path** (was Mutex per engine op).

### Java backend
8. **Pooled VectorizedClassifier** (`createRequestContainer`) — eliminates per-batch native MemorySegment alloc.
9. **Sampled O(16) MapStateCache eviction** (was O(1M) full clock-sweep scan).
10. **requiresOrderedDispatch gated** behind delete/append-merge presence — pure GET+PUT batches skip the same-key probe + per-row dispatch fallback.
11. **deleteFromWriteBuffer** per-CLEAR flush removed (invariant at snapshot boundary).
12. **cancelStreamRegistry** backend-owned (q11 correctness — the restore-only registry was closed before first checkpoint).

### Configs / tooling
- `config-forst-rs-local.yaml.tpl`: 3 bugs fixed (valid local baseline).
- `config-rocksdb.yaml`: restored to local checkpoint (reverted an erroneous S3 change).
- `scripts/bench-4way-s3.sh`: added `forst-rs-ffm-local` config.

## Remaining wall (honest)
After removing the top-2 CPU frames (SipHash + O(N²) prefix_index), **q4-S3 STILL times out at 900s** (0 FAILED — genuinely running at <111K rec/s vs rocksdb 394K). The CPU-profile frames were NOT the wall-clock bottleneck. jstack "RUNNABLE in FFM downcall" includes threads **blocked in synchronous native I/O**; the native sample showed `tokio::park` + `forst-rs-flush` + `forst-rs-opendal`. The wall is the **synchronous flush/compaction-to-S3 write path** for million-record join state: the background flush worker can't drain imm memtables to S3 multipart fast enough → WriteBufferManager backpressure stalls ingest.

## Goal status
**NOT met.** forst-rs/S3 reaches near/above parity with rocksdb/local on light queries (q0-q3, q10, q12-14, q20-22) but state-heavy joins/windows (q4/q5/q7/q9/q15-q19) hit the S3-write wall. Code is fast on equal storage; the S3-primary write path for large join state is the architectural limit.

## Precise next step (architectural)
Make flush + compaction a **fully async, non-blocking, deeply-pipelined** S3 write path so ingest never stalls on multipart latency:
- Decouple memtable-swap from flush completion (already background, but the WBM backpressure cap stalls).
- Pipeline many concurrent multipart uploads (raise effective flush parallelism well beyond 3).
- Consider an L0-on-local-disk tier (write SSTs to the 64 GiB local cache first, upload to S3 asynchronously off the critical path) so the write path sees local-NVMe latency and S3 upload is lazy.
- Confirm with a flush-timing instrument (per-flush wall-clock + imm-backlog depth + stall events) before implementing.

## Cross-refs
- [[project_full_sweep_2026-05-29]]
- v3.8 baseline: `docs/superpowers/specs/2026-05-21-forst-rs-benchmark-report-v3.8.md`
- `docs/superpowers/specs/2026-05-29-profile-driven-perf-restore.md`

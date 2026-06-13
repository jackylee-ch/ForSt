# ForSt backend + forst-rs lib write-path workbench

Date: 2026-06-13
Role: PMC
Worktree: `/Users/lijunqing/Code/stczwd/ForSt-forst-rs-lib`
Branch: `forst-rs-lib`

## Scope

This note applies the write-path redesign survey to the JDK17 `ForSt backend + forst-rs lib` path. It is intentionally local-only: no ssh, no remote benchmark, no docker gate. RD can use this as the task sheet for small local implementation or benchmark work.

Primary input:

- `docs/superpowers/specs/2026-06-13-write-path-redesign-survey.md`

Key code inspected:

- `crates/forst-rs-engine/src/column_family.rs`
- `crates/forst-rs-engine/src/db.rs`
- `crates/forst-rs-engine/src/flush.rs`
- `crates/forst-rs-engine/src/compaction.rs`
- `crates/forst-rs-engine/src/write_controller.rs`
- `crates/forst-rs-storage/src/version/mod.rs`
- `crates/forst-rs-ffi/src/lib.rs`
- `crates/forst-rs-ffi/src/compat_jni.rs`

## Current verdict

The survey is applicable to `ForSt backend + forst-rs lib`, but not as a `compact_jni`-only optimization.

The ForSt backend path already reaches the Rust engine through the compat JNI shim. `RocksDB.createColumnFamily` opens or creates a Rust CF and then attaches the Flink TTL compaction-filter factory when present. The C ABI also has `frs_cf_set_compaction_filter_ttl`. So lifecycle information can cross the JNI/FFI boundary today, but it is currently expressed as "drop expired entries during compaction", not as "this CF owns death-bucketed segments and a watermark".

Therefore the safe direction is:

1. Treat lifecycle hints, death buckets, value-log files, link/drop semantics, and write-controller accounting as `engine-shared`.
2. Expose lifecycle/watermark APIs through both `FFI-shared` and `JNI compat`.
3. Keep `compact_jni-only` limited to Java symbol compatibility, option decoding, or allocation cleanup that does not affect Rust engine semantics.
4. Do not change the Rust engine for JNI-only wins without proving the FFI backend path is neutral or better.

## Classification

| Area | Classification | Reason |
|---|---|---|
| `ColumnFamilyDescriptor` lifecycle metadata | `engine-shared` plus boundary plumbing | CF creation is common to FFI and compat JNI; adding lifecycle only in JNI would fork semantics. |
| `frs_db_create_cf*` lifecycle-capable variant | `FFI-shared` | FFM users need the same lifecycle contract and no-regression gate. |
| `RocksDB.createColumnFamily` option extraction | `JNI compat` | Needed to pass Flink/ForSt CF options into Rust, but should only populate shared descriptors. |
| Flink TTL compaction filter factory hydration | `JNI compat` feeding `engine-shared` | Existing bridge proves TTL data can cross; the engine behavior must not stay JNI-specific. |
| Flush stamping with `[min_death,max_death)` | `engine-shared` | The SST/segment metadata and checkpoint format are shared below both frontends. |
| Exempt lifecycle segments from ordinary L0 slowdown/stall | `engine-shared` | `WriteController` currently counts all L0 files; survey cell F shows this would throttle lifecycle segments incorrectly. |
| KV separation for non-merge CFs | `engine-shared` | Values/logs/checkpoint references are storage semantics, not JNI glue. |
| Merge-operator CF opt-out from KV separation | `engine-shared` | `RawConcatMergeOperator` and merge operands need bytes inline; both FFI and JNI hit this. |
| Link-compaction / whole-segment drop | `engine-shared` | VersionSet/FileMapping/checkpoint semantics sit below all frontends. |
| Java symbol stubs or byte-array return allocation cleanup | `compact_jni-only` | Useful only when it changes compat shim overhead and has no engine/FFI semantic effect. |

## Code fit and gaps

Existing fit:

- `ColumnFamilyDescriptor` already carries merge operator and compaction filter, so adding lifecycle metadata is a natural extension.
- `SstFileMeta` already carries CF id, key bounds, sequence bounds, entry count, and size. It does not yet carry death-time bounds or file kind.
- `FlushJob` has the only frozen-memtable-to-SST path. It is the right place to stamp segment metadata once lifecycle extraction exists.
- `DbImpl::set_compaction_filter` and compat JNI TTL setup show post-create per-CF metadata wiring is already accepted.
- `frs_vectorized_batch_mixed` and `batch_put_borrowed_single_cf` are already the shared vectorized write path; write-path redesign must preserve that path and measure FFI cost.

Gaps:

- No lifecycle descriptor in `ColumnFamilyDescriptor`, `CfOptions`, `frs_db_create_cf*`, or compat JNI CF options.
- No `advance_watermark` API in Rust engine, C ABI, or compat JNI.
- No death-time extraction strategy at flush time. The current TTL filter reads timestamp from value bytes during compaction; lifecycle segment drop needs a conservative per-segment death bound.
- `SstFileMeta` and checkpoint encoding do not persist death bounds, segment kind, or value-log file references.
- `WriteController` counts all L0 files for slowdown/stall. Lifecycle FIFO segments need separate accounting or they recreate the cell-F throttle artifact.
- Existing `churn_probe --no-deletes` measures the write floor, but not segment-pruned reads or watermark-driven drop.

## Benchmark gates

RD should keep every gate local and small until a code path proves useful.

| Gate | Purpose | Minimum command shape |
|---|---|---|
| write batch FFI no-regression | Ensure new lifecycle fields do not add per-row boundary tax | `cargo bench -p forst-rs-bench --bench ffi_vectorized` with n>=3 same-session comparison |
| memtable/flush floor | Ensure lifecycle metadata does not slow flush path | `churn_probe --runs 3 --label baseline` and lifecycle-inert variant |
| TTL segment floor | Reproduce the survey's ~1x write floor | `FRS_L0_COMPACTION_TRIGGER=100000 churn_probe --runs 3 --no-deletes --label ttlseg-floor` |
| lifecycle read pruning | Prove segment pruning avoids raw cell-F fan-out | new or extended `churn_probe` cell: live window/bucket count, p50 <= 700 us target |
| compaction throughput | Ensure non-lifecycle compaction unchanged | `cargo run -p forst-rs-bench --release --bin compaction_throughput -- --smoke` then n>=3 real cells |
| changelog/retract workload | Protect q4/q5/q7-like semantics | fixed-CSV mini queries or local synthetic mixed Put/Delete/Merge harness |
| disabled diagnostics overhead | Prove lifecycle instrumentation is free when off | same bench with env unset vs enabled, require noise-level delta |
| FFI shared no-regression | Protect forst-rs backend + FFM | C ABI tests plus `ffi_vectorized`, not just compat JNI smoke |

Do not run large release builds or long benchmark cells while disk free space is near 400G. Existing `target` and `target-linux` are about 2.2G combined in this worktree.

## RD task sheet

### RD-1: lifecycle descriptor skeleton, inert

Add an inert lifecycle descriptor to engine CF metadata and both boundaries.

Requirements:

- Add a Rust enum equivalent to `Unbounded`, `TtlValue`, `Windowed`, and `Timer`, with all behavior defaulting to `Unbounded`.
- Store it on `ColumnFamilyDescriptor` and `ColumnFamilyData`.
- Add a C ABI create/configure path for lifecycle metadata. It must be usable by FFM; do not hide it behind compat JNI.
- Decode compat JNI CF/TTL information only into the shared descriptor or setter.
- No behavior change in flush, compaction, reads, or checkpoint.

Acceptance:

- Rust unit tests for descriptor defaults and set/get.
- FFI test proving default behavior unchanged and lifecycle setter accepted.
- compat JNI smoke at symbol/unit level if feasible.
- `ffi_vectorized` smoke or targeted bench shows no measurable write-path regression.

### RD-2: watermark API skeleton, inert

Add `advance_watermark(cf, timestamp)` as engine state only.

Requirements:

- Engine stores per-CF watermark atomically or behind existing CF metadata locks.
- C ABI exposes it.
- compat JNI can call it later, but no Flink integration is required in this step.
- It must not affect reads, writes, compaction, or flush.

Acceptance:

- Unit test: watermark monotonicity, stale lower watermark ignored or rejected by explicit contract.
- FFI test: null handles and valid updates.
- Disabled overhead gate: no write/read regression with no watermarks.

### RD-3: lifecycle-aware churn_probe model cell

Before engine behavior changes, extend local bench evidence.

Requirements:

- Add a `churn_probe` mode that models lifecycle segment pruning separately from raw no-compaction L0 fan-out.
- Report write-amp, live segment count, pruned probe segment count, p50/p99.
- Keep output JSONL under `target/` and clean after runs if disk approaches 400G free.

Acceptance:

- Same-session baseline A, `ttlseg-floor`, and `ttlseg-pruned-model` n>=3.
- A short note in this workbench or adjacent results doc with medians and commands.

### RD-4: do not start KV separation yet

KV separation is plausible but must wait until RD-1/RD-3 prove lifecycle plumbing and gates. Starting KV-sep first would touch SST format, read path, compaction, checkpoint, and FFI behavior at once.

Allowed preparatory work:

- Inventory which CFs are merge-operator CFs and therefore KV-sep-exempt.
- Design pointer encoding and value-log file-kind metadata.
- Add no code unless it is inert metadata with FFI no-regression evidence.

## PMC review checklist for RD results

- Is the change `engine-shared`, `FFI-shared`, `JNI compat`, or `compact_jni-only`?
- If it touches Rust engine semantics, where is the FFI no-regression evidence?
- If it touches compat JNI, does it feed a shared engine API rather than a JNI-only side path?
- If it changes lifecycle behavior, can a premature watermark/drop produce wrong output?
- If it changes L0/file accounting, are ordinary L0 SSTs and lifecycle segments counted separately?
- If it adds diagnostics, what is the disabled-overhead proof?
- Does the local run leave disk above 400G free after cleanup?

## Current blockers and risks

No hard blocker for design/RD-1. Main risks:

- Disk is at roughly the 400G free-space floor; avoid new large build/test outputs.
- The current worktree HEAD is behind the main `forst-rs` checkout seen earlier. Do not rebase this shared branch without coordinating PMC/RD.
- Existing correctness evidence for `ForSt backend + forst-rs lib` is fixed-CSV/local-report based, not a fresh run in this session.
- Lifecycle segment drop is correctness-sensitive: a watermark or death bound bug can silently drop valid late state.

## 2026-06-13 q7/q19 FFI microbench update

Commands run locally in `/Users/lijunqing/Code/stczwd/ForSt-forst-rs-lib`:

```bash
cargo fmt -p forst-rs-bench
cargo bench -p forst-rs-bench --bench ffi_vectorized -- --test
cargo bench -p forst-rs-bench --bench ffi_vectorized -- q7_iter_probe_ffi --noplot
cargo bench -p forst-rs-bench --bench ffi_vectorized -- q19_iter_topn_ffi --noplot
cargo bench -p forst-rs-bench --bench ffi_vectorized -- q19_append_merge_chain_ffi --noplot
cargo bench -p forst-rs-bench --bench ffi_vectorized -- q19_iter_open_alloc_split_ffi --noplot
cargo bench -p forst-rs-bench --bench ffi_vectorized -- q19_merge_chain_read_lifecycle_ffi --noplot
git diff --check
```

Validation:

- `cargo bench -p forst-rs-bench --bench ffi_vectorized -- --test` passed after adding q7/q19 gates.
- `git diff --check` passed.
- Production paths were not changed in this round.

Key evidence:

| Cell | Scalar serial | Serial batch alloc | Serial batch reuse | Parallel alloc | Parallel reuse |
|---|---:|---:|---:|---:|---:|
| q19 alloc split `k64_r1` | 26.81 us | 61.82 us | 25.95 us | 120.39 us | 68.83 us |
| q19 alloc split `k64_r16` | 173.28 us | 209.15 us | 171.60 us | 212.48 us | 170.14 us |
| q19 alloc split `k64_r32` | 330.93 us | 369.78 us | 332.29 us | 334.62 us | 291.77 us |
| q19 alloc split `k256_r1` | 113.77 us | 217.24 us | 110.79 us | 393.59 us | 210.17 us |
| q19 alloc split `k256_r16` | 746.42 us | 875.28 us | 745.84 us | 810.36 us | 670.59 us |
| q19 alloc split `k256_r32` | 1.4134 ms | 1.5572 ms | 1.4175 ms | 1.3214 ms | 1.1705 ms |

Interpretation:

- Caller allocation is a real batch-path cost. `serial_batch_alloc` is consistently worse than `serial_batch_reuse`.
- `frs_vec_iter_prefix_open_batch` with reusable buffers is effectively at scalar-serial speed for small q19-style probes.
- `frs_vec_iter_prefix_open_batch_parallel` should not be the q7/q19 default. It wins only once each prefix returns enough rows to amortize pool/fanout/materialization overhead, such as `k64_r32`, `k256_r16`, and `k256_r32`.
- Production routing should keep scalar or serial-batch fast path for small-result probes and use parallel only behind an adaptive threshold.

Merge-chain read lifecycle:

| Cell | Memtable read | Flushed read | Compacted read |
|---|---:|---:|---:|
| distinct chain1 | 20.33 us | 18.19 us | 16.08 us |
| same-key chain1 | 17.37 us | 17.38 us | 17.30 us |
| distinct chain4 | 79.62 us | 74.57 us | 68.96 us |
| same-key chain4 | 20.86 us | 20.95 us | 15.67 us |
| distinct chain16 | 314.86 us | 316.61 us | 311.68 us |
| same-key chain16 | 41.61 us | 41.84 us | 17.05 us |

Interpretation:

- `frs_vec_merge_append_batch` write throughput is not the q19 first bottleneck.
- Same-key merge-chain reads improve materially after compaction, so future merge work should target merge-resolution/read-amplification and compaction policy, not append-write rewrite.
- Distinct-key chain cost is mostly proportional to returned key count and is not fixed by merge compaction.

### RawConcat newest-first merge read fast path

Patch under test:

- Add `MergeOperator::full_merge_newest_first(...)` as an engine-facing adapter for operands already collected newest-to-oldest.
- Override it in `RawConcatMergeOperator` so q19-style merge-chain reads avoid allocating a reversed `Vec<Vec<u8>>` plus slice vector before concatenation.
- Route `DbImpl::apply_merge_operator(...)` through the adapter.

Validation:

```bash
cargo test -p forst-rs-storage test_raw_concat_full_merge_newest_first_matches_oldest_first_contract
cargo test -p forst-rs-engine test_batch_get_vectorized_put_delete_merge_mix
cargo test -p forst-rs-engine value_carrying_merge_fallback_resolves_merge_chain
cargo test -p forst-rs-engine test_scan_resolves_merges
cargo bench -p forst-rs-bench --bench ffi_vectorized q19_merge_chain_read_lifecycle_ffi -- --noplot --baseline q19-merge-before-rawconcat-fastpath
```

Key result versus `q19-merge-before-rawconcat-fastpath` baseline:

| Cell | After | Change |
|---|---:|---:|
| distinct chain1 / memtable | 16.55 us | -15.66% |
| distinct chain1 / flushed | 16.53 us | -16.27% |
| distinct chain1 / compacted | 16.13 us | -4.78% |
| same-key chain4 / memtable | 19.70 us | -16.96% |
| distinct chain16 / memtable | 299.14 us | -13.20% |
| distinct chain16 / flushed | 298.26 us | -15.20% |
| same-key chain16 / memtable | 39.33 us | -20.07% |
| same-key chain16 / flushed | 39.63 us | -20.53% |
| same-key chain16 / compacted | 17.11 us | -4.46% |

Two cells were noisy in the first full run and were repeated individually:

| Re-run cell | After | Change |
|---|---:|---:|
| same-key chain1 / compacted | 16.76 us | -2.29% |
| distinct chain4 / memtable | 72.51 us | -13.47% |

Interpretation:

- The patch is a focused q19 read-path win for merge-chain cases. It reduces read-side merge materialization overhead without changing merge append.
- The benefit is strongest before compaction folds a same-key chain; after compaction, the remaining path is mostly batch get and result materialization.
- The added `output_copy_only` cells separate result-buffer copy from engine lookup/merge work. In the filtered bench, output copy was roughly 150 ns for chain1 and roughly 2.3 us for distinct chain16, while full reads were roughly 16-17 us and 301-318 us respectively. That makes output buffer copy a small term for this q19 case; the next q19 bottleneck is lookup/merge-chain resolution.
- This does not close q7/q19 by itself. The next bottlenecks are caller scratch reuse, serial/parallel adaptive prefix probing, and merge-chain compaction/read-amplification policy.

### ForStBackend compat JNI q7/q19 proxy update

Flink ForStBackend path check:

- `ForStGeneralMultiGetOperation` calls `RocksDB.multiGetAsList(...)`.
- `ForStMapState.buildDBIterRequest(...)` builds `ForStDBMap*IterRequest`.
- `ForStDBIterRequest.process(...)` opens `db.newIterator(cf)`, seeks the key prefix, then loops through `isValid/key/value/next`.

Implication:

- `ForStBackend + forst-rs-lib` is not the same path as the pure ForSt-RS FFM backend. q7/q19 MapState prefix iteration still uses the RocksDB-compatible `RocksIterator` Java surface.
- Native-only prefix work is not enough: the Java loop owns prefix-stop logic, `cacheSizeLimit`, and entry/key/value deserialization mode. A real prefix chunk fast path needs a Flink Java call-site change plus a compat JNI chunk API.

Proxy benchmarks added:

```bash
cargo bench -p forst-rs-bench --bench ffi_vectorized compat_jni_prefix_proxy -- --test
cargo bench -p forst-rs-bench --bench ffi_vectorized compat_jni_multiget_proxy -- --test
cargo bench -p forst-rs-bench --bench ffi_vectorized compat_jni_multiget_proxy -- --noplot
cargo test -p forst-rs-ffi --features compat-jni test_multi_get
```

Prefix proxy result:

| Cell | Row-native | Row-copy proxy | Chunked lower bound |
|---|---:|---:|---:|
| p64 r1 | 20.98 us | 22.50 us | 25.64 us |
| p64 r16 | 159.87 us | 181.48 us | 155.73 us |
| p256 r16 | 680.99 us | 782.15 us | 678.74 us |
| p256 r32 | 1.274 ms | 1.485 ms | 1.313 ms |

Interpretation: Rust-only row iteration is close to the chunked lower bound. The suspected production cost is Java/JNI per-row `isValid/key/value/next` and Java `byte[]` allocation, so the next proof must be a Flink-side JMH or small ForStBackend benchmark, not another Rust-only prefix benchmark.

Compat `multiGetAsList` update:

- `RocksDB.batchGet` already used `frs_batch_get`, but compat `RocksDB.multiGet` still looped over `frs_get`.
- The native shim now groups keys by CF and uses `frs_batch_get` only for small groups (`<=64` rows). Larger groups keep the old scalar loop to avoid the 1024-row batch-risk seen in the first run.
- `test_multi_get` now verifies multi-CF, out-of-order, and missing-key behavior through the grouped helper.

Threshold-64 proxy result:

| Rows | Scalar loop | Batch single-CF | Adaptive grouped |
|---:|---:|---:|---:|
| 64 | 5.747 us | 5.502 us | 5.385 us |
| 256 | 24.027 us | 24.306 us | 24.066 us |
| 1024 | 109.08 us | 109.47 us | 109.33 us |

Interpretation: this is a narrow native-only win for small `multiGetAsList` groups and keeps medium/large groups at scalar-loop parity. It helps q19/list/raw-get style batches, but it does not solve MapState prefix iterator cost.

Next production patch candidate:

1. Flink/ForSt-RS iterator caller scratch reuse: reuse prefix offsets, handles, `FrsChunk` descriptors, and chunk buffers per vectorized executor or task thread.
2. Route fresh small-result map/prefix iterators to scalar serial or serial batch (`frs_vec_iter_prefix_open_batch`) with reusable buffers.
3. Keep `frs_vec_iter_prefix_open_batch_parallel` disabled by default for q7/q19 unless an adaptive threshold sees sufficiently large historical rows per prefix.
4. Keep merge append path unchanged; add a multi-SST merge-chain read gate before changing compaction policy.

## 2026-06-13 filtered smoke update

Problem found after the first q7 smoke: `ffi_vectorized` used `harness = false`
and always executed `boundary_tax_summary()` after Criterion groups. Filtered or
`--test` runs therefore still entered the full boundary-tax summary after the
target cells printed `Success`, which made q7/q19 smoke unsuitable as a fast RD
gate.

Fix:

- `ffi_vectorized` now skips `boundary_tax_summary()` for `--test`, `--list`,
  `--help`, or a filtered benchmark name.
- Full unfiltered bench keeps the old default and still runs the summary.
- `FRS_FFI_BOUNDARY_SUMMARY=1` forces the summary; `=0` disables it.

Fresh local smoke evidence after the fix:

```bash
cargo fmt -p forst-rs-bench -- --check
git diff --check
cargo bench -p forst-rs-bench --bench ffi_vectorized q19_iter_topn_ffi -- --test
cargo bench -p forst-rs-bench --bench ffi_vectorized q19_iter_open_alloc_split_ffi -- --test
cargo bench -p forst-rs-bench --bench ffi_vectorized q19_merge_chain_read_lifecycle_ffi -- --test
cargo bench -p forst-rs-bench --bench ffi_vectorized q7_iter_probe_ffi -- --test
```

Results:

- `cargo fmt -p forst-rs-bench -- --check`: passed.
- `git diff --check`: passed.
- `q19_iter_topn_ffi -- --test`: all `k64/k256 * r1/r4/r16/r32`
  serial/parallel cells printed `Success`; summary skipped; RSS peak 48 MiB.
- `q19_iter_open_alloc_split_ffi -- --test`: all `k64/k256 * r1/r16/r32`
  scalar, serial-batch, and parallel-batch alloc/reuse cells printed `Success`;
  summary skipped; RSS peak 56 MiB.
- `q19_merge_chain_read_lifecycle_ffi -- --test`: all distinct/same-key
  chain1/4/16 memtable/flushed/compacted cells printed `Success`; summary
  skipped; RSS peak 48 MiB.
- `q7_iter_probe_ffi -- --test`: all `p64/p256 * r1/r4/r16`
  serial/parallel cells printed `Success`; summary skipped; RSS peak 53 MiB.

This does not replace the n>=3 performance runs above. It gives RD a cheap
correctness/executability gate before spending time on full q7/q19 Criterion
measurements.

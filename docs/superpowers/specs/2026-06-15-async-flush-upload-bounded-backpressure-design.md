# Async FLUSH↔UPLOAD pipelining — verify + TRUE bounded-backpressure lever

Phase-2 Disaggregated State · 2026-06-15 · base `origin/forst-rs` f51c35b1c
Flag: `FRS_ASYNC_FLUSH_UPLOAD` (default OFF) · crate `forst-rs-io`

## TL;DR

The headline cycle target — **overlap local SST flush with remote upload** so
flush throughput is not gated by remote-upload latency — was **already shipped**
in the production write path (`OpendalWritableFile::close_writer` spawns the
upload on the bridged runtime and returns on the local serialization). A
verify-before-build mini-bench confirms it hides upload latency (up to **7.9×**
flush-throughput under a throttled remote).

What was NOT actually delivered, despite a doc comment claiming it, is **true
bounded in-flight backpressure**. The `MAX_INFLIGHT_UPLOADS=8` semaphore permit
is acquired *inside* the spawned task, AFTER the full SST buffer has been moved
into the future. So a fast flush loop over a slow remote spawns an unbounded
number of upload tasks that each hold their full SST buffer in RAM while queued
on the semaphore — the cap bounds concurrent *network transfers* but NOT
resident *buffers*. This cycle adds `FRS_ASYNC_FLUSH_UPLOAD` (default OFF): when
ON, the permit is acquired SYNCHRONOUSLY on the flush thread BEFORE spawning, so
a full queue applies backpressure to the flush loop (it blocks briefly) instead
of spawning a buffer-holding task. Peak resident upload memory drops from the
whole working set to `~MAX_INFLIGHT_UPLOADS × SST_size` — measured **7.1× lower
peak** with throughput unchanged.

## Verify-before-build

### 1. The overlap already exists (file:line)

- `crates/forst-rs-io/src/opendal_backend.rs` `close_writer` → `WriterKind::Buffered`
  arm: `self.handle.spawn(async move { … op.write_with(...).await … })` then
  returns. The flush worker returns on the local serialization; SST N+1 flush
  proceeds while SST N uploads.
- Durability barriers: `await_upload(path)` (per-file) and `await_all_uploads()`
  register/join the spawned task via the `pending` registry + watch channel, so
  a checkpoint observes every required upload before it acks
  (`crates/forst-rs-engine/src/db.rs` checkpoint path: per-file `await_upload`
  after the VersionSet snapshot; `await_all_uploads` on shutdown / legacy ckpt).

### 2. The overlap hides upload latency — mini-bench

`cargo run -p forst-rs-io --example flush_upload_pipeline_bench --release`
(self-contained `RemoteFakeFs`: contention-robust, modeled-RTT mock that
reproduces the spawn-and-return + shared-semaphore + await-registry semantics; a
`ThrottledFileSystem` over the in-memory backend can NOT isolate the overlap
because it paces `append()` in the flush thread, not the spawned upload).

`serial` (await each upload before next flush) vs `pipelined` (spawn all, one
barrier at end), 24 SSTs × 4 MiB, 5 ms/SST local flush:

| remote MiB/s | serial | pipelined | speedup |
|---|---|---|---|
| 16   | 6.288 s | 0.813 s | 7.73× |
| 64   | 1.814 s | 0.255 s | 7.10× |
| 256  | 0.660 s | 0.181 s | 3.64× |
| 1024 | 0.302 s | 0.174 s | 1.73× |

Monotonic, matches theory: the slower the remote, the more the pipeline hides.
At 1024 MiB/s the local flush dominates so the win shrinks toward 1×.

### 3. The real gap: soft backpressure → unbounded resident buffers

The permit is `sem.acquire().await` *inside* the spawned task; the `buf` (full
SST, up to `target_file_size_base` = 64 MiB) is moved into the future before the
permit is held. Under a fast flush loop + slow remote, hundreds of tasks queue,
each holding a buffer → RSS grows with the working set, not the cap. The
`MAX_INFLIGHT_UPLOADS` doc comment even *claimed* the bound the code did not
enforce. On 8c/32g this is a real OOM-adjacent hazard for write-heavy disagg
queries.

## Implementation (file:line + flag)

`crates/forst-rs-io/src/opendal_backend.rs`:

- `ASYNC_FLUSH_UPLOAD_ENV = "FRS_ASYNC_FLUSH_UPLOAD"` (pub const) +
  `async_flush_upload_backpressure()` (OnceLock env, with an AtomicU8 test
  override `set_async_flush_upload_override`). Default OFF.
- `close_writer` `WriterKind::Buffered` arm: when the flag is ON, acquire an
  **owned** permit on the flush thread via
  `self.handle.block_on(sem.clone().acquire_owned())` BEFORE `self.handle.spawn`,
  and move it into the task (held for the whole upload, dropped on completion).
  When OFF, `prefetched_permit = None` and the task acquires the permit itself —
  **byte-identical** to the prior behaviour. `block_on` is safe here:
  `close_writer` runs on the synchronous flush thread (same context as the
  existing no-registry fallback).
- FFI: no new symbol — `frs_set_env("FRS_ASYNC_FLUSH_UPLOAD", "1")` flows through
  the existing generic `frs_set_env` bridge (doc updated to list the flag). Must
  be set before the first upload (OnceLock-cached).

The overlap is UNCHANGED — only the admission point moves. Durability/await
semantics are identical (same registry/watch/join registration), so checkpoint
correctness, recovery, and the pinned-version upload-await scope are unaffected.

## RSS A/B (the new lever's evidence)

Same example, RSS section (fast flush 50 µs/SST so the loop outruns the remote),
64 SSTs × 4 MiB, 32 MiB/s remote:

| mode | peak resident buffers | wall |
|---|---|---|
| OFF (soft)  | 256.0 MiB (whole working set) | 1.029 s |
| ON  (true)  |  36.0 MiB (~MAX_INFLIGHT×SST) | 1.033 s |

**7.1× lower peak resident upload memory, throughput unchanged.**

## TDD / correctness

`crates/forst-rs-io/src/opendal_backend.rs` tests (all green; 268 lib + 7 doc):

- `async_flush_upload_off_is_durable_and_byte_identical` — OFF: > MAX_INFLIGHT
  SSTs all upload + read back byte-exact; no loss/dup/truncation.
- `async_flush_upload_on_is_durable` — same under ON (moved admission point).
- `async_flush_upload_on_blocks_flush_when_queue_full` — THE invariant: with the
  permits exhausted, ON makes `close_writer` BLOCK on the flush thread (true
  backpressure) while OFF returns immediately (soft); both stay durable after
  permits free.

`cargo fmt --all --check`, `cargo clippy -p forst-rs-io --all-targets` (clean),
`RUSTDOCFLAGS="-D warnings" cargo doc -p forst-rs-io --no-deps` (clean).
Downstream `forst-rs-engine` + `forst-rs-ffi` build clean.

## Composition

- **QoS upload rate-split** (`FRS_UPLOAD_RATE_SPLIT`): orthogonal — the rate
  split paces bytes on the remote leg; this flag bounds how many buffers are
  resident waiting for that paced bandwidth. Together: paced uploads + bounded
  buffer residency.
- **WAL-DELTA**: orthogonal (checkpoint artifact path).
- **Resident-flushed shadow** + **local-first SST reads**: reads served from RAM
  / local NVMe while the upload is in flight, so the bounded-backpressure delay
  on the flush thread never blocks readers.

## Next-cycle candidate

Negative-cache for known-absent vlog/SST segments on the disagg scan was
assessed and is **weakly justified**: vlog `ValuePointer`s reference live
segments (GC reaps dead segments + rewrites pointers), so absent-segment lookups
are not a recurring hot path. Prefetch-on-restore is **already shipped**
(`FRS_RESTORE_BG_FILL`, paced background fill). The strongest next lever is a
**per-instance, size-aware in-flight upload budget** (bytes, not count): cap
resident upload bytes at a fraction of the WriteBufferManager budget so a few
large SSTs and many small SSTs both stay bounded, and wire the ON-path as a
default once a remote A/B confirms no flush-throughput regression on 8c/32g.

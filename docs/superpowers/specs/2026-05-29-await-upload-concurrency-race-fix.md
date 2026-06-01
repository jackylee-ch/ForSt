# 2026-05-29 — Write-back `await_upload` concurrency race → intermittent NotFound

## Status
ROOT CAUSE FOUND + FIXED. Verification run in progress.

## Symptom
After the opendal `.concurrent()` executor panic fix (q9 no longer crash-loops on
the first flush), q9 on S3 hit a NEW intermittent fatal error around ~1.66M
records, then again at varying points — the job entered a restart loop but
sometimes recovered and ran past the crash point:

```
AsyncException{FrsBackendException: frs_vectorized_batch_get rc=1 errCode=NOT_FOUND (status=ERROR)}
  at VectorizedExecutor.completeGetExceptionally(VectorizedExecutor.java:2554)
  at VectorizedExecutor.executeGets(...)
```

`rc=1` = `FrsErrorCode::NotFound`. The FFI encodes per-key absence as
`out_validity[i]=0` with `rc=Ok`, so `rc=1` is NOT "key absent" — it is the engine
returning `Err(NotFound)` for the whole batch. `error_to_frs_code(is_not_found)`
→ rc=1, and the only `ForstError::not_found` in the read path is
`map_opendal_err(OdErrorKind::NotFound, ...)` (opendal_backend.rs:135) — i.e. a
read hit a **missing S3 object** (an SST that was not yet uploaded).

## Root cause — consume-once `await_upload` under concurrent readers
The write-back flush spawns the SST upload asynchronously and registers a
`JoinHandle` in `pending[path]`. The read path
(`DbImpl::get_or_open_sst_reader`) calls `fs.await_upload(path)` before opening
an SST reader, to guarantee the object is durable on S3 first.

The bug was in `await_upload`:
```rust
let handle = { pending.lock().remove(p) };   // REMOVE under lock
match handle { Some(jh) => block_on(jh), None => Ok(()) }  // join OUTSIDE lock
```
With parallelism ≥ 2, two subtasks read the SAME just-flushed SST concurrently:
1. Reader A: `remove(P)` → gets the handle → starts `block_on(jh)` (upload still
   in flight on S3).
2. Reader B (arrives after the remove, before the join completes): `remove(P)` →
   `None` → returns `Ok(())` early → opens the SST reader → reads the S3 object
   **that A's upload has not finished writing** → opendal `NotFound` → engine
   `Err(NotFound)` → `frs_vectorized_batch_get rc=1` → fatal.

This is exactly the intermittent shape observed (parallelism=4, frequent flushes
under the heavy q9 join, crash at variable record counts, sometimes recovering).

## Fix — broadcast completion via `tokio::sync::watch`
`pending` now maps `path → watch::Receiver<Option<Result<(), String>>>`. The
spawned upload task publishes `Some(outcome)` on a `watch::Sender` when it
finishes. `await_upload`:
- CLONES the receiver (does not remove it), then
- `block_on(rx.wait_for(|v| v.is_some()))` — blocks until the outcome is
  published, then
- removes the entry (idempotent; a late awaiter that missed it returns `Ok`
  because the object is durable by then).

Every concurrent awaiter clones the same receiver and blocks on the SAME upload
completion — no awaiter can race ahead of the upload. `await_all_uploads` (the
checkpoint/shutdown barrier) and the path-reuse supersede path were updated the
same way. The upload error is collapsed to `String` (watch values must be
`Clone`; `ForstError` is not) and re-wrapped as `ForstError::Io` — upload errors
are only ever Io/corruption, never NotFound, so no classification is lost.

The spawned task's `JoinHandle` is now discarded (the task detaches and runs to
completion on the tokio runtime regardless), so the `tokio::task::JoinHandle`
import was removed.

## Verification
- `cargo build -p forst-rs-io`: clean (3 pre-existing dead-code warnings only).
- `cargo test -p forst-rs-io`: 208 pass; only `test_opendal_append_mode` fails,
  which fails identically on baseline (pre-existing, unrelated).
- q9 on S3 re-run to confirm: zero `rc=1 NOT_FOUND`, no restart loop, runs past
  the prior ~1.66M crash point.

## Lesson
Write-back (async upload + read-after-write) demands that the "is it durable
yet?" barrier be a multi-awaiter broadcast, not a consume-once handle. A
single-consume `JoinHandle` is a correctness hazard the moment more than one
reader can await the same path — which is always true at parallelism ≥ 2.

## Cross-refs
- [[project_q9_opendal_panic_fix_2026-05-29]] (the panic fix that exposed this)
- docs/superpowers/specs/2026-05-29-opendal-concurrent-executor-panic-fix.md

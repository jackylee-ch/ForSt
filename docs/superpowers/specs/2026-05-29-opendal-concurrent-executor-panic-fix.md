# 2026-05-29 — Write-back upload PANIC fix: opendal `.concurrent()` needs an Executor

## Status
ROOT CAUSE FOUND + FIXED. Verification run in progress.

## Symptom (what the user predicted)
The user said: *"I don't believe the q9 performance, there must be some correctness
issues, please check for it."* They were right.

Running q9 (and any heavy query) on forst-rs + S3, the measurement job never
completes — it enters a **restart loop**. The Flink JM exception history shows:

```
AsyncException{FrsBackendException: frs_vec_iter_prefix_open rc=300 (status=PANIC)}
  at VectorizedExecutor.executeIters(VectorizedExecutor.java:1330)   (Join[24] -> Calc[25])
AsyncException{FrsBackendException: frs_vectorized_batch_get rc=300 errCode=ENGINE_IO (status=PANIC)}
```

`rc=300` is `FrsErrorCode::EngineIo`, **not** a caught FFI panic (that would be 900).
The "status=PANIC" label on the Java side is misleading — it is an engine `Err`.

## Why the nexmark "10.9s" was a false win
nexmark computes `Time = EventsNum / sampled-peak-TPS` (extrapolation, not
wall-clock). When the job crash-loops, nexmark still samples a brief peak TPS
before the crash and extrapolates a tiny "Time". The job never actually ran to
100M. The JM `duration` for the job is the only trustworthy completion signal —
and it showed the job RESTARTING at 238s, never FINISHED.

## True root cause (from the TaskManager `.out`, instrument-don't-guess)
```
thread 'forst-rs-opendal' panicked at opendal-0.50.2/src/types/execute/api.rs:71:9:
concurrent tasks executed with no executor has been enabled
```
opendal source:
```rust
impl Execute for () {
    fn execute(&self, _: BoxedStaticFuture<()>) {
        panic!("concurrent tasks executed with no executor has been enabled")
    }
}
```

The 2026-05-29 **write-back flush** change uploads each SST with
`op.write_with(path, buf).concurrent(8).chunk(16 MiB)`. opendal's `.concurrent(N)`
requires an `Executor` to spawn the part-upload tasks. With no `.executor(...)`
call **and** the `executors-tokio` feature disabled, the default executor is the
unit type `()`, whose `execute()` **panics on the first concurrent part**.

Chain of failure:
1. SST flush spawns the buffered upload on the bridged tokio runtime.
2. `.concurrent(8)` tries to spawn part #2 → `()` executor panics in the
   `forst-rs-opendal` thread → the upload future errors/aborts → **SST never lands
   on S3**.
3. The Design-A resident-flushed RAM shadow keeps reads correct for a while, but
   any read that reaches the real S3 object (compaction input, evicted shadow,
   `get_or_open_sst_reader` after `await_upload`) gets a missing/short object →
   engine returns `EngineIo` → `frs_vec_iter_prefix_open` / `frs_vectorized_batch_get`
   return rc=300 → the Join async task fails → job restarts → repeat.

This regression was introduced by my own write-back change this session; it
invalidated every S3 number measured after it.

## Fix
`crates/forst-rs-io/Cargo.toml` (workspace `Cargo.toml`): add `"executors-tokio"`
to the opendal feature list. This gates `TokioExecutor` (`tokio::task::spawn`) and
makes `Executor::new()` return a working tokio-backed executor instead of `()`.

`crates/forst-rs-io/src/opendal_backend.rs`:
- `use opendal::Executor;`
- Both concurrent-write sites (the spawned write-back path and the synchronous
  no-registry fallback) now call `.executor(Executor::new())` before
  `.concurrent(...)`.

Both call sites run inside a tokio runtime context (`handle.spawn(...)` and
`handle.block_on(...)` respectively), so `TokioExecutor`'s `tokio::task::spawn`
finds the runtime and never panics.

## Verification
- `cargo build -p forst-rs-io` + `-p forst-rs-ffi --release`: clean.
- Re-run q9 to 100M via `scripts/measure-completion.sh`, polling the JM
  `/jobs/overview` `duration` until FINISHED; confirm:
  - TaskManager `.out` has **zero** `forst-rs-opendal panicked` lines,
  - the Source emits 100M records,
  - the job reaches FINISHED with a real wall-clock duration.

## Lesson (recurring this session)
The reliable completion signal is the Flink JM `duration` of a FINISHED job, not
nexmark's peak-TPS-extrapolated `Time`. And: instrument first — the TM `.out`
panic line named the exact cause; the Java rc=300 alone would have led to guessing
about engine corruption.

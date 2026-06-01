# 2026-05-30 — Checkpoint staging reads SST before its write-back upload completes

## Status
ROOT CAUSE FOUND (chronological log proof) + FIXED. q9 S3 re-verification in progress.

## Symptom
After fixes #1–#3 (opendal executor panic, await_upload race, compaction-deletion
race), q9 on S3 STILL crash-looped with:
```
frs_vectorized_batch_get rc=1 NOT_FOUND
  underlying: open_sequential_file: /db-remote-<hash>/000001.sst: NotFound 404 at stat
```
Always the oldest SST (`000001.sst`), always `open_sequential_file` (= checkpoint
staging, NOT the point-read path). First crash ~100–260s in, before steady state.

## Diagnosis (instrument-before-guess, decisive)
Added temp diags for upload outcome, deletion, retiring-list size, and the
NotFound payload, then read the chronological TaskManager `.out`:
- `retiring.len()` never exceeded 32 → fix #3 is NOT the cause, and not a perf
  regression from it.
- **0 deletions** by the guarded path → `000001.sst` was NOT deleted (rules out
  compaction-deletion AND restore-mismatch).
- `000001.sst` was uploaded **OK** to the SAME `db-remote-<hash>` that later 404s
  (rules out restore-mismatch / per-instance-URI mismatch — same instance).
- **The NotFound line appears BEFORE the upload-OK line in the chronological
  `.out`.** i.e. checkpoint staging read `000001.sst` from S3 *before its
  write-back async upload finished*. The upload completed moments later.

## Root cause
Write-back flush returns on local serialize and uploads the SST to S3
asynchronously. The checkpoint copies live SSTs from the engine FS (S3) into a
local stage dir via `open_sequential_file(info.path)`:
- `DbImpl::stage_checkpoint_artifacts_local` (incremental, db.rs ~4441)
- `checkpoint::copy_file` (full, checkpoint.rs ~98)

`create_incremental_checkpoint` does call `await_all_uploads()` before staging,
but that barrier is insufficient: a flush concurrent with the checkpoint, or one
whose upload registered just after the barrier ran, leaves a snapshot SST's
upload in flight. Staging then `stat`s/reads the not-yet-uploaded S3 object →
404 → `ForstError::not_found` → checkpoint fails → the async backend escalates →
job crash-loop. (Compounded by restarts then hitting the per-instance-URI issue,
which produced the confusing cascade — but the FIRST crash is this staging race.)

## Fix
Await the specific SST's in-flight upload immediately before each staging read,
so the barrier-timing gap cannot matter:
- `db.rs` `stage_checkpoint_artifacts_local`: `self.fs.await_upload(&info.path)?;`
  before `get_file_metadata` / `open_sequential_file`.
- `checkpoint.rs` `copy_file`: `fs.await_upload(src)?;` before
  `open_sequential_file(src)`.

`await_upload` is a default-no-op `FileSystem` trait method (real impl only on the
OpenDAL backend), so this is free on local FS / already-durable objects and, with
the watch-broadcast fix (#2), blocks all callers until the single upload truly
completes.

## Verification
- Builds clean; re-run q9 on S3: expect zero `rc=1 NOT_FOUND`, checkpoints
  succeed, job runs to FINISHED — the first real q9 wall-clock.

## The four bugs, in order discovered (all write-back-introduced)
1. opendal `.concurrent()` executor panic (`executors-tokio` + `.executor`).
2. `await_upload` consume-once race (`tokio::watch` broadcast).
3. compaction deletes SST under concurrent read (version-lifetime / retiring +
   `referenced_file_numbers` deferral).
4. **this** — checkpoint staging reads SST before its async upload completes
   (`await_upload` before each staging read).

## Cross-refs
- docs/superpowers/specs/2026-05-29-opendal-concurrent-executor-panic-fix.md
- docs/superpowers/specs/2026-05-29-await-upload-concurrency-race-fix.md
- docs/superpowers/specs/2026-05-29-compaction-deletion-vs-read-race.md
- [[project_q9_opendal_panic_fix_2026-05-29]]

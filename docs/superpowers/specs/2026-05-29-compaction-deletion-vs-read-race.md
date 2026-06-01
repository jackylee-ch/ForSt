# 2026-05-29 — Compaction deletes SST under concurrent read → NotFound crash-loop

## Status
ROOT CAUSE FULLY DIAGNOSED + FIX IMPLEMENTED (2026-05-30) + unit-tested. q9 S3
end-to-end re-verification in progress.

## Implemented fix (2026-05-30)
`VersionSetImpl` (storage) now retains every replaced version in a `retiring:
Mutex<Vec<Arc<Version>>>`, pruned each `apply`/query by `Arc::strong_count > 1`
(only the Vec holds it ⇒ no reader). New `referenced_file_numbers() -> HashSet`
returns the union of live SST file numbers across the current version + all
retiring versions a reader still holds. The engine's `delete_file_guarded` /
`reap_pending_deletions` now defer a file whose number is in that set (in
addition to the existing checkpoint-pin check), via a shared
`can_reclaim_file(fnum, &referenced)` predicate. A periodic reap was added to the
1 s snapshot-age worker so deferred files drain once their readers finish.

**`Arc::strong_count` IS safe here** — counter-intuitively. A real reader always
holds an owned `Arc<Version>` (every read uses `current()` = `load_full`, never a
`load()` Guard — verified), so a file a reader can open always has `strong_count
≥ 2` ⇒ retained ⇒ never deleted. The only imprecision is `ArcSwap` transiently
parking the just-replaced version in a debt slot (adds a phantom ref), which makes
reclamation *conservative* (a file may linger slightly longer) — never premature.
Under a running query's read volume the slots churn continuously so reclamation
stays prompt and `pending_deletions` bounded; the existing
`test_compact_l0_deletes_old_files_from_disk` was updated to assert *eventual*
(not synchronous) reclamation to reflect this.

Test: `test_compaction_defers_delete_while_read_version_held` — holds a read
version across a compaction, asserts the input SSTs SURVIVE, then (after release +
activity) are reclaimed. Full engine suite green (the one immediate-delete test
updated).

## Original diagnosis (retained for reference)

## Symptom
After fixing (1) the opendal `.concurrent()` executor panic and (2) the
`await_upload` consume-once race, q9 on S3 STILL crash-loops with intermittent:
```
frs_vectorized_batch_get rc=1 errCode=NOT_FOUND   (executeGets → completeGetExceptionally)
```
plus checkpoint-staging `open_sequential_file: .../000001.sst: NotFound 404`.

## Evidence (instrument-before-guess)
Added temp `FRS-UPLOAD-DIAG` (per-upload outcome) + `FRS-NOTFOUND-DIAG` (per
NotFound path) and ran q9:
- **107 uploads OK, 0 ERR** — every SST uploaded successfully.
- The 404'd object (`000001.sst`) was uploaded OK to the **same** `db-remote-<hash>`
  it later 404s on (all 16 NotFound hashes ∈ the upload-hash set).
- 52 distinct `db-remote-<hash>` dirs = ~52 DB opens = heavy crash-looping (each
  restart → new operator-instance UUID → `db_path = /db-remote-{uri_str_hash}`
  changes, lib.rs:704/775 — but this is a *consequence* of the loop, not the cause).
- Earliest trigger = a **normal `batch_get`** NotFound (not checkpoint).
- It is always `000001.sst` — the **oldest** SST, i.e. the FIRST to be compacted
  out of L0.

## Root cause
`DbImpl::compact_*` success path (db.rs ~3558):
```rust
for (_, file_number) in &edit.deleted_files {
    self.delete_file_guarded(*file_number);   // deletes S3 object if !pinned
}
```
`delete_file_guarded` deletes the S3 object immediately unless the file is
**pinned** in the `FileDeletionGuard`. Pins are taken ONLY by checkpoints
(`deletion_guard.pin_batch` under `snapshot_with_locked_view`). **In-flight reads
do not pin.**

A read (`get_internal`, db.rs ~6707) captures `version = version_set.current()`
(an `Arc<Version>` via `ArcSwap`) and then walks `version.l0_files()`, opening each
L0 SST on demand (`get_or_open_sst_reader` → `await_upload` → `open_random_access_file`).
For S3 the open issues a live GET. If a compaction completes mid-read — applies a
new version (removing `000001.sst`), then `delete_file_guarded(000001)` deletes the
S3 object because no checkpoint pinned it — the read's next GET on `000001.sst`
returns 404 → `ForstError::not_found` → `db.batch_get` Err → `frs_vectorized_batch_get`
rc=1 → fatal AsyncException → job restart → repeat.

The `sst_readers` cache eviction (db.rs 3549) does NOT help: an `Arc<SstReaderImpl>`
already pulled by the read still issues on-demand S3 GETs against the now-deleted key.

This is the classic LSM obsolete-file-lifetime problem: an SST's storage must not
be reclaimed while any live `Version` snapshot still references it. RocksDB solves
it with SuperVersion refcounting; forst-rs only has checkpoint pins.

## Fix (design — to implement next, with a failing test first)
Defer deletion of compaction-input SSTs until no live reader references the
*replaced* version:

1. Add `retiring: Mutex<Vec<(Arc<Version>, Vec<FileNumber>)>>` to `DbImpl`.
2. In every compaction/flush apply that deletes files: capture the pre-apply
   `Arc<Version>` (the version that still lists the deleted files), apply, then push
   `(old_version, deleted_file_numbers)` onto `retiring` INSTEAD of deleting now.
3. A `reap_retiring()` (called after each apply + from the flush worker) deletes a
   retiring entry's files — via `delete_file_guarded` (so checkpoint pins are still
   honored) — once no reader holds the old version.

**Caveat — do not use `Arc::strong_count` on the `ArcSwap` value.** `ArcSwap` uses
deferred/hazard-pointer reclamation, so the old `Arc`'s strong count does not drop
to 1 deterministically after a `store`. Instead track reader liveness explicitly:
either (a) an epoch/generation counter that reads enter/exit, reaping a retiring
version once all readers that started before its retirement have exited; or (b) a
registry of handed-out version snapshots with explicit register/unregister around
each read (adds a cheap atomic incr/decr to the hot read path). (a) is preferred to
keep the per-read cost to one relaxed atomic.

Alternative (simpler, less surgical): route ALL compaction-input deletions through
`pending_deletions` and reap only at safe boundaries (checkpoint complete / a bounded
grace after the file leaves the current version). Trades some transient S3 storage
for correctness; acceptable given S3 is the primary store.

## Verification plan
- Failing test: open DB, flush ≥2 SSTs, capture a read `Version`, trigger compaction
  that deletes an input, then read through the captured version — must NOT 404.
- Re-run q9 on S3: zero `rc=1 NOT_FOUND`, no restart loop, runs to FINISHED;
  read the real JM wall-clock.

## Confirmed-and-deployed this session (the two prerequisite fixes)
1. opendal `.concurrent()` executor panic → `executors-tokio` + `.executor(Executor::new())`.
2. `await_upload` consume-once race → `tokio::watch` broadcast (all awaiters block
   until the single upload completes). q9 verified to run PAST the prior crash point
   with these two; this 3rd (compaction-deletion) race is the remaining blocker.

## Cross-refs
- [[project_q9_opendal_panic_fix_2026-05-29]]
- docs/superpowers/specs/2026-05-29-opendal-concurrent-executor-panic-fix.md
- docs/superpowers/specs/2026-05-29-await-upload-concurrency-race-fix.md

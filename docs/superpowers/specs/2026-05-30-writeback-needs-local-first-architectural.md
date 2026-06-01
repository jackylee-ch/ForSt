# 2026-05-30 — Write-back to S3 needs LOCAL-FIRST (architectural root of the NotFound class)

## Status
RESOLVED (2026-05-30) by a LIGHTER read-path fix than the full local-first
redesign: **await-on-404-retry in the cached_fs read path**. q9 on S3 now runs
clean past the 300s periodic checkpoint (0 NotFound / 0 AsyncException / 0 restart
through 320s+ / 6M records) — the checkpoint that crashed EVERY prior run.

## The fix that closed the class (fix #5)
`CachedFileSystem::remote_size_awaiting_upload(path)`: the SST read entry points
(`open_random_access_file` size-stat + `fetch_through_cache` cap-hint stat) now,
on a remote NotFound for an `.sst`, call `self.remote.await_upload(path)` and
retry the stat once. This catches the real race: a concurrent flush's upload
registers in `pending` AFTER the engine's `get_or_open_sst_reader` await ran but
BEFORE the cached_fs stat — so the cached_fs-level await (closer to the actual
read) waits for it. Cheap no-op when nothing is pending; **no LRU churn** (unlike
the reverted write-through caching), so heavy-join read working set is untouched.
Empirically this — not the heavier local-first redesign below — closed the class.

## (Original architectural analysis retained — the heavier redesign is NOT needed
## now that the read-path await-retry works; keep for reference if a residual
## re-appears under different load.)

## The 4 point-fixes landed (all real, all verified at unit + clean-run level)
1. opendal `.concurrent()` executor panic → `executors-tokio` + `.executor(Executor::new())`.
2. `await_upload` consume-once race → `tokio::watch` broadcast.
3. compaction deletes SST under concurrent read → version-lifetime deferral
   (`retiring` versions + `referenced_file_numbers`).
4. checkpoint staging reads SST before its upload completes → `await_upload`
   before each staging read.

After all four, q9 on S3 runs clean past every prior crash point (read path
verified 0 NotFounds for ~300s / 5.8M records) — then crashes at the **300s
periodic checkpoint** with `frs_vectorized_batch_get rc=1 NOT_FOUND` (a 5th
variant: a read racing the checkpoint's force-flush + upload).

## Architectural root cause
The implemented "write-back" does the **async-upload** half but NOT the
**local-first** half of ForSt's model. Concretely
(`opendal_backend.rs:open_writable_file`):
```
use_buffered_object_store = !append && scheme != Fs   // true for S3
```
On S3, an SST write buffers in memory and, on close, uploads to S3 — **no local
copy is retained**. Reads go through `CachedFileSystem`'s block cache, which is
populated only on the first S3 *read*. So a just-written SST exists ONLY on S3
(eventually), and every read / checkpoint-stage of it must hit S3, racing:
- the in-flight async upload (NotFound until upload completes),
- S3 deletion by compaction,
- S3 eventual-consistency / per-instance `db-remote-<hash>` on restart.

Each point-fix closed one race; the class remains because the SST has no local
home to read from.

## The fix: true local-first write-back
Write each SST to LOCAL disk (the 64 GiB cache volume), **serve reads from the
local copy**, upload to S3 asynchronously, and evict the local copy only AFTER
the upload is confirmed durable. Then reads/checkpoint-staging NEVER race S3 —
they read local until the object is provably on S3. This is exactly the model the
goal directive describes ("ForSt's model = write-back = local-first + async
upload").

**Strong lead — the mechanism already exists but is unwired:** `cached_fs.rs:508`
has `CachePopulatingWritableFile::populate_cache`, flagged dead-code ("method
`populate_cache` is never used"). Wiring write-through population (an SST write
inserts its bytes into the local block cache / a local file) so subsequent reads
+ checkpoint staging hit the local copy is the targeted implementation. Combined
with: do not evict a chunk/file from the local cache until its upload's `watch`
outcome is `Ok` (durable on S3).

## The OTHER remaining gap (orthogonal, perf not correctness)
q9's Join hard-stalls in fits at ~3–4M records: sampling the TM shows the hot FFI
ops are the **prefix iterator** (`frs_vec_iter_prefix_open`+`_next`+`_close` ≫
`batch_get`). The streaming join iterates matching records under each join-key
prefix; cost scales O(growing state) per probe. Needs a symbol-preserving
`sample` to find the hot frame inside `libforst` under the iterator
(build_lazy_prefix_key_stream / memtable prefix scan / resident-flushed cursor
merge / SST prefix scan), then optimize (seek via the sorted index, bound
per-open cost). This is the gating PERF issue once correctness (local-first) lands.

## Next steps (in order)
1. Implement local-first write-back (wire `CachePopulatingWritableFile` /
   write-through to local cache; gate eviction on upload-confirmed). Re-run q9 →
   expect zero NotFound at the checkpoint.
2. Symbol-profile the prefix iterator; optimize the hot frame.
3. Re-measure q9 to completion (real wall-clock), then the full q0–q22 sweep for 3×.

## Cross-refs
- docs/superpowers/specs/2026-05-29-opendal-concurrent-executor-panic-fix.md
- docs/superpowers/specs/2026-05-29-await-upload-concurrency-race-fix.md
- docs/superpowers/specs/2026-05-29-compaction-deletion-vs-read-race.md
- docs/superpowers/specs/2026-05-30-checkpoint-staging-reads-sst-before-upload.md
- [[project_q9_opendal_panic_fix_2026-05-29]]

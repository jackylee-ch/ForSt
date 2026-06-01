# LocalCache O(1) LRU — remove O(N) scan under the global mutex on every block read

Date: 2026-06-01
Status: LANDED (engine + 12/12 tests). Audit finding TIER-1-B. Awaiting cluster
re-validation (rebuild dylib → re-run q9).

## Problem (architecture audit, lock-dimension H1)

`LocalCache` (the on-disk hot-cache backing S3, `local_cache.rs`) had ONE global
`Mutex<Inner>` and an LRU stored as `VecDeque<String>`. `touch_lru` did a LINEAR
`lru.iter().position(|k| k==key)` + `VecDeque::remove(pos)` — **O(N) scan + O(N)
shift — under the global mutex, on EVERY cache hit**. The same O(N) scan was in
`drop_entry`, `put` (update + error rollback), and `invalidate`.

`RangeCachedRandomAccessFile::read_at` (`cached_fs.rs`) calls `get_range` for
every SST data block, every overlapping SST, every join probe (q4/q7/q9/q16).
With a multi-GiB disk cache holding tens of thousands of entries and N reader
threads, this is a lock convoy doing O(N) work in the critical section — the same
"global lock + heavy work under it per record" shape as the prior
`prefix_scan_cursor` write-lock freeze. The q9 2GB+cache-bypass run on S3
**hard-stalled at 26.4M records (rate=0 for 100+s)** — a flatline consistent with
this convoy, not slow I/O (disk reads already happen OUTSIDE the lock).

## Fix — generation-stamped lazy LRU (O(1) touch)

- `Entry` gains `gen: u64`; `Inner` gains a monotonic `gen_counter` and the LRU
  becomes `VecDeque<(String, u64)>` (key + generation at push).
- `touch_lru`: O(1) — `gen_counter += 1`, stamp `entry.gen`, push `(key, gen)`.
  No scan, no remove. The entry's prior `(key, old_gen)` ref is left in the deque
  as STALE.
- A deque ref `(key, g)` is the LIVE recency ref iff `entries[key].gen == g`.
  Eviction `pop_front`s and evicts only when `gen` matches; stale refs (a later
  touch exists, or the entry was removed) are skipped. Correctness depends ONLY
  on the gen comparison.
- `drop_entry` / `invalidate` / `put`-update just `entries.remove` + `mark_stale`
  — no O(N) scan; the orphaned ref is reclaimed lazily.
- `maybe_compact`: when stale refs dominate (`stale > 64 && 2*stale > lru.len()`),
  rebuild the deque keeping one live ref per key (preserving order). Amortized
  O(1) per touch; bounds deque memory to ~one ref per entry. Invariant preserved:
  every live entry always has its current-gen ref in the deque, so eviction can
  always reach a real victim (no infinite loop).

Disk I/O remains outside the lock (unchanged). The critical section is now O(1).

## Tests (12/12 in `local_cache::tests`)

All prior tests pass (LRU promotion, eviction order, current_bytes accounting,
16-thread concurrent smoke). New: `lazy_lru_picks_true_victim_after_many_
retouches_and_bounds_deque` — 500 re-touches of /a,/c create hundreds of stale
refs + a compaction; inserting /d still evicts the untouched /b (true LRU), and
the deque is asserted bounded to ≤ entries+64.

## Next

Rebuild + deploy dylib; re-run q9 (2GB + cache-bypass) on S3 and check whether
the 26.4M rate-0 stall is gone / rate sustained. If the stall persists, the
remaining suspects are TIER-1-D (sst_readers write-lock across S3 I/O) and
TIER-2-E (no async read-ahead). Optionally also shard LocalCache by key hash if
single-mutex contention (now O(1) but still one lock) still shows in a profile.

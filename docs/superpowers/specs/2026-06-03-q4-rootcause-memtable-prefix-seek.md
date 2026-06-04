# forst-rs: q4 (interval join) root cause — memtable prefix-scan seek is the wall

**Date:** 2026-06-03
**Status:** ROOT CAUSE CONFIRMED (exhaustive, instrument-first). Fix = cache-friendly memtable
index (design below). Implementation pending (major core-data-structure change).

---

## 1. Symptom

NexMark **q4** (avg winning-bid price per category — an interval join + aggregation) on
forst-rs **decays** 181K → ~7K rec/s and does NOT finish 92M in budget. **RocksDB q4 finishes
in 324.7 s at a STEADY 237K rec/s, no decay** (measured, same local harness). So q4 is a
forst-rs-specific defect, NOT heavy-for-both (unlike q7, where RocksDB also collapses).

## 2. Investigation (systematic, evidence at each step — overturned the prior hypothesis)

The prior assumption (carried from q7/q11) was "L0 read-amp / compaction can't keep up." A
symbolized native `sample` of the decayed q4 TM **refuted** it: the Join operator's hot path is

```
frs_vec_iter_prefix_open → build_lazy_prefix_key_stream
  → ShardedMemTable::prefix_scan_cursor → VectorizedMemTable::prefix_scan_keys
    → collect_distinct_keys_in_range
      → crossbeam_skiplist::map::Range::next → SkipList::search_bound → memcmp   (≈1065/2530)
```

i.e. the **in-memory memtable prefix scan**, NOT SST reads. SST fan-out is already pruned
(`may_contain_range` + `first_block_ge`, db.rs:5615-5642). Then instrumentation of
`collect_distinct_keys_in_range` showed:

- `scanned/emitted ≈ 1.0`, `max_single_scan ≈ 820-864` → scans are TINY (no version/tombstone
  bloat, no whole-memtable-scan bug). ~8.3M+ scans, each emitting ~0.5 keys.
- active-memtable `idx_len` swings 15K↔1M (flush cycle).

So q4 is a **high-frequency, low-yield prefix-scan pattern**: millions of tiny per-record
join-probe scans, each paying the full crossbeam `SkipMap::range` lower-bound seek
(`search_bound`, O(log N)) + per-element `epoch::pin` + `RefEntry` refcount + per-scan
`MemTierCursor`/snapshot-`Vec`/`BinaryHeap` allocation. crossbeam skiplist nodes are
heap-scattered, so once the memtable exceeds cache the seek is cache-miss-bound.

## 3. Every config lever measured and RULED OUT

| Lever | Result |
|---|---|
| block cache 2 GiB + bg-compaction 8 + flush 4 | no change (346K→34K still) — documented in config tpl |
| write buffer size (64 MiB vs 256 MiB) | smaller = MORE resident tiers (16 vs 4) = worse |
| resident RAM shadow | **disabling it made q4 WORSE** (20M@462s vs 42M) — shadow is a mitigation, not the cause |
| memtable shard count | already **1** by default (FRS-MEMTABLE-SHARDS-DEFAULT 2026-05-28); 16× scan-fan-out not active |
| L0 point-read short-circuit (this session) | q4 reads are prefix-SCANs, not point reads → unaffected |

The cost is **irreducible by configuration**: it is the crossbeam-skiplist prefix-scan seek
itself, executed per-record.

## 4. Why RocksDB is steady and forst-rs is not

RocksDB's memtable is an **arena-allocated** skiplist (`InlineSkipList`): nodes are contiguous
in a bump arena → cache-friendly pointer-chase, no per-node refcount, no epoch reclamation on
the read path, and the iterator is a reusable cursor (no per-scan heap allocation). forst-rs
uses `crossbeam_skiplist::SkipMap`: lock-free and correct, but nodes are individually
heap-allocated (scattered), `Range::next` re-pins the epoch guard every element (map.rs:740)
and bumps a `RefEntry` refcount, and the scan path snapshots into a `Vec<Arc<[u8]>>` + a
`BinaryHeap` per call. Same O(log N) algorithm, much worse constant + cache behavior → the
gap widens as the memtable grows → decay.

## 5. Fix (the real lever for q4/q7/q9 join throughput + the 5.x goal)

**Cache-friendly arena memtable index.** Replace the crossbeam `SkipMap` index in
`VectorizedMemTable` with an **arena-allocated skiplist** (RocksDB `InlineSkipList`-style):
nodes in a contiguous arena (the key/value arenas — SegmentedBytes + KEY arena — already exist
from the lock-free-memtable work b08cc21fd), single-guard cursor iteration (no per-element
epoch pin / refcount), and a reusable prefix-scan cursor (no per-scan `Vec`/`BinaryHeap`
allocation). This is the documented "skiplist memtable index / lock-free memtable" direction.

Supporting, lower-risk wins to land alongside/first:
- For `shards == 1` (the default), `prefix_scan_cursor` should skip the `BinaryHeap` k-way
  merge and snapshot machinery and iterate the single shard directly — removes per-scan
  allocation overhead on the hot path.
- Avoid materializing `prefix_scan_keys` into a `Vec<Arc<[u8]>>` when the caller consumes
  lazily; expose a borrowed cursor.

Risk: the memtable index is the core data structure (MVCC, concurrency, flush, every read
path depend on it). This is a major, correctness-critical change — TDD + full engine/storage
suite + per-query benchmark gating, ideally implemented fresh rather than at the tail of a
long session.

## 6. CORRECTION (2026-06-03, async-profiler): the decay is the LOCAL-CACHE READ PATH, not the index

§1-5 correctly located the memtable prefix-scan as A cost and the BTreeMap swap is a real win
(early q4 268K→**618K**, seek `search_bound` 1065→`BTreeMap::range` 350 = 3× cheaper; kept). But
BTreeMap did **NOT** fix the decay (still 618K→25K) → the index is NOT the decay driver. An
async-profiler CPU profile (JIT symbols, decay phase) finally exposed it — overturning a 6th
hypothesis:

- With checkpointing ON, **`pread` dominated (5102)** — but that was the **checkpoint** reading
  SSTs to snapshot; disabling checkpointing dropped `pread` to **0** yet **q4 still decayed**
  (so the checkpoint is a separate, real, periodic cost — not the decay driver).
- With checkpointing OFF, the real driver is exposed: **`read` (10319, sequential) + `__open`
  (8244) + `close`** — i.e. the query is **opening + sequentially reading whole files per block
  access**. This is the local-cache (`cached_fs`/`LocalCache`) READ PATH: q4's block reads MISS
  the cache's whole-SST-pread fast path (`get_range` on `path_key`) and fall to the **chunk path**
  (`serial_read_at`→`chunk_bytes`→`cache_hit({path_key}#c{idx})`), which on a miss **fetches a
  whole 1 MiB chunk (open + read + close) per ~16-64 KB block** — the code's own comment
  (cached_fs.rs:1004-1007) calls this "the profiled q4/q7/q9 wall (64× read amplification)".
- Confirmed NOT fixable by: block cache size (8 GB, uncapped — no help; blocks not reused),
  block size (4 KB — no help; it's pread COUNT not bytes), memtable index (BTreeMap — no help),
  resident shadow (disabling = worse), shard count (already 1), compaction trigger, checkpointing.

**Why the whole-SST-pread misses (to pin in the fix step):** the local cache dir IS populated
(3.9 GB ≈ data dir), SSTs are large (190-202 MB, compacted L1+), yet `get_range(path_key)` misses
→ likely compaction's `StreamingSstWriter` admits large compacted SSTs as 1 MiB **chunks**
(`#c` keys) rather than whole-file `path_key`, OR a cache membership/index gap. As q4's state
migrates to compacted SSTs, every block read takes the chunk/open+read path → decay. RocksDB holds
SST fds (table cache) + reads exact blocks → flat.

**FIX (scoped, the q4/q7/q9 lever):** make the chunk path **positional-`pread` only the needed
bytes with a reused fd** (mirror the whole-SST-pread's FRS-FDCACHE path) instead of
open+read(whole 1 MiB)+close per block; and/or admit compacted SSTs under the whole-file
`path_key` so `get_range`'s fast path hits. Plus separately: make checkpoints incremental
(the `pread`-on-snapshot cost). NOT a correctness defect — q4 is correct, just slow.

**Landed this session:** BTreeMap memtable index (real win, all tests green). The arena skiplist
(§5) is deprioritized — it would NOT fix this decay (the index isn't the driver).

## 7. PINNED single root cause + fix (2026-06-03 — localization cycle, the user's "B")

Per the directive to collapse the "why does the whole-SST-pread fast path miss" into ONE root
cause before cutting the durable-read path, a short instrumented localization run (logging the
write-through admit key/result vs the read-miss key) showed: **zero write-through admits** + all
reads "RangeCached whole-SST MISS". Code trace confirmed the single mechanism:

1. On a LOCAL (POSIX) backend, `flush.rs`/`compaction.rs` write SSTs to a **`.tmp` staging path**
   then `rename` to `.sst` (because `supports_atomic_rename()` is true for local).
2. `CachedFileSystem::open_writable_file` only wraps `.sst` files in `CachePopulatingWritableFile`
   (the write-through). The `.tmp` write fails the `is_sst` check → **plain writable, no
   write-through**.
3. `CachedFileSystem::rename` (cached_fs.rs:550) only **invalidates** cache keys — it never admits
   the renamed `.sst`.
4. ⟹ local SSTs are **never** in the local cache under `path_key`. `get_range(path_key)` always
   misses → every read falls to `RangeCachedRandomAccessFile`'s chunk path → `open`+`read` a whole
   **1 MiB chunk per scattered ~16-64 KB block** = 64× read amplification + an `__open` storm that
   grows with state → the decay. (Write-through works for **S3** because S3 writes straight to
   `.sst`.) The same amplified path also served the incremental-checkpoint's new-SST staging reads,
   which is why `pread` dominated the ckpt-ON profile — ONE read-path cause, not two.

**FIX (FRS-LOCAL-DIRECT-READ, cached_fs.rs `open_random_access_file`):** choose the SST reader by
remote type via `supports_atomic_rename()`. Local (POSIX) → **`LocalFirstSstFile`**: try the cache
`get_range`, else **direct exact-block `pread` on a HELD remote fd** to the durable data-dir file
(the data dir IS the local store), with the OS page cache providing RAM-residency — the v3.8 /
RocksDB direct-local-read model, no 1 MiB chunking, no per-block open, no cache duplication. S3 →
keep `RangeCachedRandomAccessFile` (1 MiB chunks amortize network latency). Tests: storage 339/0 +
engine 259/0. [q4 NexMark trajectory appended once measured.]

**Reconciliation with v3.8 + checkpoint (the user's two cross-checks):**
- *v3.8 q4 (REAL+correct in the lessons doc) was fast for THIS reason* — it read SSTs cheaply
  (direct, predating this S3-oriented chunk-cache layer, and/or noflush=true keeping state in the
  memtable). The decay is a regression from the S3 disaggregation read path applied to local. The
  fix restores v3.8-class local reads the RocksDB-fair way (noflush=false + direct exact-block
  reads) — NOT by reverting to noflush=true (unfair; the q5 merge-chain hang root cause).
- *Checkpoints are ALREADY incremental* (verified earlier: shared-SST reuse). The heavy ckpt
  `pread` was the incremental ckpt staging new SSTs through the SAME amplified read path — so this
  one fix addresses it too. "Make checkpoints incremental" is NOT a fresh fix.

## 8. v3.8 q4 RERUN — EMPIRICAL: v3.8 q4 CRASHES at first flush, never completes 100M (2026-06-03)

Per the directive to *run* v3.8 (not reason about it) and confirm whether its q4 was really fast,
v3.8 was rebuilt from worktrees (`/tmp/v38-flink` @ a5fd9f70dd6, `/tmp/v38-forst` @ 67d914558),
its dylib (5.13 MB) + jar (342 KB) deployed, and q4 run via `measure-sql.sh` (single-job, REAL JM
wall-clock, no warmup/cancel) in a CLEAN environment (all prior Flink killed, memory freed).

**Result — reproduced THREE independent times (measure-completion, then measure-sql twice):**

| t | src_out | rate |
|---|---|---|
| 0 s | 0 | — |
| 21 s | 2,331,267 | 111 K/s |
| 41 s | 6,347,250 | 201 K/s |
| **61 s** | **0** | **−317 K/s** ← counter RESET |
| 83–230 s | 0 | 0/s (hung) |

At ~6.3 M records — exactly where the ~256 MB write buffer fills and the **first flush** fires —
the **TaskManager process dies** (`ps aux` → TM count 0; JM log: *"TaskManager … is no longer
reachable"*, RESTARTING→RUNNING but no TM left in the single-TM standalone → src_out stuck at 0
forever). The TM `.out` shows **no Rust panic, no Java exception, no `hs_err`, no `.ips` crash
report** — a hard native `abort()`/SIGKILL with no flushed diagnostic, the signature of the old
v3.8 flush path that PREDATES this session's flush / SST-corruption / atomic-rename fixes.

**Conclusion (overturns the premise "v3.8 q4 was fast — copy it"):** v3.8 q4 was **never actually
fast**. It bursts to ~6.3 M @ ~200 K/s, then crashes at the first flush and **never completes
92–100 M**. The "46.88 s fast" figure in the lessons doc is the **nexmark peak-TPS monitor
extrapolating that pre-crash burst** (peak sampled TPS × event count) — the same
burst-then-collapse measurement artifact documented in
`2026-05-30-why-v3.8-looked-better-measurement-artifact.md`. There is nothing fast to port.

**This REVISES §5/§7's "v3.8 read SSTs cheaply" reconciliation:** v3.8 didn't read flushed SSTs
*cheaply* — it **crashed before it ever had to read them**. So the CURRENT code is not a
regression from a faster v3.8; it is the FIRST version that *survives* the flush and completes
100 M honestly — and the decay (§6/§7) is the real, honest cost of the local chunk-cache read
path under that completed flush load. The FRS-LOCAL-DIRECT-READ fix (§7) remains the correct
lever; the v3.8 comparison is simply not a valid "faster baseline" — it is a crash.

**Cleanup after this run:** current dylib (10.28 MB) + jar (518 KB) redeployed from
`/tmp/cur-artifacts`; `flink-daemon.sh` jemalloc/DYLD injection reverted; `VectorizedExecutor`
FRS-Q4-DISPATCH-DIAG instrumentation removed; checkpoint interval restored to 30 s. (The dispatch
diag, while live, had already confirmed q4 = 0 ordered batches / 100 % vectorized — so the per-row
sync-dispatch hypothesis was also ruled out.)

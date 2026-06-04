# Lock-free / optimistic-concurrency engine redesign

**Status:** DESIGN — for review. **Date:** 2026-06-02. **Branch:** forst-rs.

## Directive (user, 2026-06-02)

> RocksDB and ForsT are lock-free and non-blocking; adopt a lock-free design to
> maximize performance. Memtables and the read/write hot paths should allow
> fully parallel reads and writes. MVCC should use **optimistic** locking as much
> as possible. **Pessimistic locks and mutexes on the hot path are disallowed.**
> Any method that enables parallelism is allowed.

## Reference: how RocksDB / ForsT achieve this

- **Memtable** = lock-free concurrent skiplist (`InlineSkipList`). `allow_concurrent_memtable_write=true`
  (default): writers insert via CAS; **readers traverse fully lock-free** and never block writers.
- **Sequence numbers**: a single atomic counter; each write claims a seq via fetch-add.
- **Version/superversion**: read path grabs an atomic-refcounted **SuperVersion** snapshot
  (thread-local cached, `arc-swap`-like) — no lock to start a read.
- **Block cache**: sharded, lock-free-ish (LRU/CLOCK with per-shard fine locks held only for metadata).

## Current forst-rs hot-path locks (inventory, 2026-06-02)

| Lock | Location | Protects | On hot path? | Contention evidence |
|---|---|---|---|---|
| `Vec<RwLock<VectorizedMemTable>>` | sharded.rs:75 | per-shard memtable (BTreeMap+unsorted) | **YES** (every read & write) | q7 sample: **NOT lock-contended** (single key, shards=1, CPU was in `BTreeMap::range`/memcmp, not futex). Keyed/concurrent queries: **unmeasured**, likely contended. |
| `RwLock<SharedMemTable>` | column_family.rs:299 | swap active memtable on flush | read = per op (brief) | low; only `write()` on flush |
| `Mutex<BTreeMap<Seq,RegistryEntry>>` | mvcc/snapshot.rs:177 | live snapshot registry (MVCC) | per snapshot create/release | **directive target** (MVCC→optimistic) |
| `Mutex<()>` write_mutex | db.rs:293 | serialize writes / seq assignment | **YES** (every write batch) | serializes all writers |
| `RwLock<HashMap<FileNumber,Arc<SstReader>>>` | db.rs:278 | SST reader cache | **YES** (every block read does `read()`) | read-mostly; uncontended read is ~ns but it's on the q7 path |
| `Mutex<Inner>` + `Mutex<FdCache>` | local_cache.rs:77,94 | cache metadata + fd handles | **YES** (every block read) | brief; fd_cache just added (55187ba86) |
| `Vec<RwLock<ClockCacheShard>>` | clock.rs:413 | block cache | YES (block reads) | sharded; per-shard, brief |
| `Mutex<()>` flush/compaction | db.rs:381, cf flush_mutex | flush/compaction exclusion | NO (background) | cold — keep |

## HONEST PRIORITY NOTE (instrument-first)

The q7 sampled profile (2026-06-02) showed q7's wall was **`LocalCache` open()-per-block**
(fixed in 55187ba86, fd-cache) + a **secondary CPU cost** in `BTreeMap::range`→memcmp under the
memtable read lock. **Neither was lock *contention*** — q7's stateful op is a global aggregate
(parallelism-1, single thread), so the memtable RwLock is essentially uncontended for q7. Therefore:

- **The lock-free memtable does NOT speed up q7 directly.** Its payoff is **concurrent / keyed**
  queries (q3/q4/q8/q9) where many subtasks (or the write + scan within one) hit the same shard.
- Before the biggest/riskiest rewrites, **measure lock contention on a keyed query** (e.g. q3 at
  parallelism 8) with a `sample`/JFR pass — confirm `RwLock`/futex frames dominate. If they don't,
  the lock-free win is marginal and effort should go to the measured CPU costs instead.

This keeps the redesign evidence-driven rather than speculative, while still moving toward the
directive's architecture.

## Target design

1. **Lock-free reads everywhere.** A read (get / prefix-scan) must never take a blocking lock.
2. **Optimistic MVCC.** Snapshot acquire/release via atomics (seq counter + atomic-refcount), not a Mutex.
3. **Atomic sequence assignment.** Writers claim seq via `AtomicU64::fetch_add`; remove `write_mutex` from the seq path.
4. **No `Mutex`/pessimistic `RwLock::write` on the per-record path.** Background-only locks (flush, compaction, version-apply) may remain (cold).

## Migration plan (phased, each TDD + benchmarked; measurement-gated)

**Phase 0 — measure (gate).** `sample`/JFR a keyed query (q3 @ p=8) → quantify RwLock/futex time.
Decides how aggressively to pursue Phase 3.

**Phase 1 — SuperVersion-style lock-free read handle (low risk, high value).**
Replace `sst_readers: RwLock<HashMap>` and the per-read `version_set.current()` clone with an
`arc-swap`(or `ArcSwap`) "current view" the read path loads atomically (no lock). Readers hold the
`Arc` for the scan's duration; writers publish a new `Arc` on flush/compaction. Eliminates the
read-side `RwLock::read` on `sst_readers` and version. **No correctness change to data**; standard
RCU pattern.

**Phase 2 — optimistic MVCC snapshot registry.**
Replace `Mutex<BTreeMap<Seq,RegistryEntry>>` with a lock-free structure: an `AtomicU64` low-water
mark + a lock-free skip-list / sharded atomic set of live snapshot seqs, or an epoch/refcount scheme.
Snapshot create = `fetch_add`+publish; release = atomic dec. Compaction reads the min-live-seq
atomically. **Correctness-critical** (MVCC visibility) → exhaustive TDD incl. concurrent create/release.

**Phase 3 — lock-free / concurrent memtable (highest risk, biggest scope).**
The current `RwLock<VectorizedMemTable>` (BTreeMap + unsorted HashMap) serializes writers vs scanners.
Options, in increasing fidelity to RocksDB:
  - (3a) **RCU read snapshot**: keep the write lock for inserts but let reads load an immutable
    `Arc` snapshot of the sorted view (published after each merge) → reads never block writers.
    Cheapest; preserves the Arrow-vectorized layout. **Recommended first step.**
  - (3b) **Lock-free ordered index**: replace the BTreeMap index with a concurrent skip-list
    (e.g. `crossbeam-skiplist`) keyed by encoded key → concurrent readers + writers, CAS inserts.
    Keep Arrow row storage append-only with atomic row-count. Closest to RocksDB.
  - (3c) shard-count tuning (interim): raise `memtable_shards` so writer/scanner contention spreads —
    cheap mitigation, but multiplies per-probe traversal (why it's 1 today). Not a real fix.

**Phase 4 — drop `write_mutex` from the seq path.** Assign seq via `AtomicU64::fetch_add`; the
memtable insert becomes the only synchronization (lock-free per 3b). WAL/ordering caveats audited.

## Risks

- **Memory reclamation for lock-free structures** (epoch-based GC / hazard pointers) — use
  `crossbeam-epoch`; the dominant correctness hazard.
- **MVCC visibility** (Phase 2): a released snapshot must not let compaction drop a version a
  concurrent reader still needs. Optimistic scheme needs a proven happens-before.
- **ABA** on CAS structures.
- Arrow-vectorized memtable layout must be preserved (off-heap mandate) — favors 3a/3b over a full
  skiplist-of-values rewrite.

## Immediate next actions

1. Confirm q7 fd-cache win (validation run in flight) → push 55187ba86.
2. Phase 0 contention measurement on q3 @ p=8 (gate Phase 3 aggressiveness).
3. Phase 1 (ArcSwap read view) — lowest-risk lock-free win, benefits all read paths.

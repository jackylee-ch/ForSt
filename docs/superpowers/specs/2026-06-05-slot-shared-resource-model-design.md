# forst-rs slot-shared resource model — design (root-cause fix for q4 decay)

**Date:** 2026-06-05. PMC-level root-cause + design. Supersedes the "lever A/C" patches.

## Root cause (proven, clean machine, Apple M5 Pro 18-core, swap=0)

forst-rs is *faster than RocksDB cold* (~628K vs ~445K rec/s) but its q4 throughput
**collapses in a sawtooth** (bursts 500-640K → troughs **38K/48K/110K**), and the average of
that sawtooth falls below RocksDB's flat ~400K. Evidence:

- During every deep trough, **total CPU spikes to 1380-1480% of 1800** while the 4 join
  subtasks stay 100% busy and **swap=0, RSS 27-45 GB** (< 64 GB). ⇒ compute/core contention,
  not swap, not the per-record path.
- Thread profiler: **12 `forst-rs-compact` + 12 `forst-rs-flush` threads, every sample** — one
  flush + one compact thread **per keyed-state `DbImpl`** (Join×4 + GroupAggregate×4 ×2 = 12),
  with **no process-global concurrency bound** (db.rs:6819 spawns per-DbImpl).

**The architectural flaw:** forst-rs replicates a full, independently-scheduled engine per
operator-subtask. Background CPU therefore scales with (operators × parallelism). When the ~12
subtasks reach their flush/compaction thresholds together (same input rate), up to ~9-12
compactions run at once → background CPU eats ~9 cores → the CPU-bound join is starved → collapse.

**Why RocksDB stays flat:** Flink's RocksDB backend cooperates at the **slot** level — ONE
shared block cache + ONE WriteBufferManager from managed memory, and a **bounded shared
background thread pool** (`max_background_jobs`, RocksDB HIGH/LOW Env pools). Total background
CPU is capped regardless of CF count. forst-rs ignores this slot-shared contract.

## Target: slot-shared resource model (4 resources; 2 already done)

| resource | status |
|---|---|
| shared memtable budget (`GLOBAL_WBM_USED`, runtime_tuning.rs) | ✅ DONE (2026-06-03) |
| shared resident-shadow budget (`GLOBAL_RESIDENT_SHADOW_USED`, column_family.rs) | ✅ DONE (2026-06-03) |
| **A. bounded shared background thread pool (flush+compact)** | ❌ **this work — decay fix** |
| **B. shared block cache** | ❌ this work — memory |

### Component A — bounded shared background scheduler (THE decay fix)

Mirrors RocksDB HIGH (flush) / LOW (compaction) Env pools.

- **`BgScheduler`** (process/slot-global via `OnceLock`): a HIGH pool of `N_f` flush workers and
  a LOW pool of `N_c` compaction workers. Each pool = `Arc<(Mutex<VecDeque<Job>>, Condvar)>`
  (MPMC, std-only, keeps `#![forbid(unsafe_code)]`).
- **`Job = { db: Weak<DbImpl>, cf_data }`.** Worker pops a job, upgrades the `Weak`, calls the
  existing per-request body extracted into `DbImpl::run_one_flush(cf)` /
  `DbImpl::run_one_compaction(cf)` (today inside `flush_loop`/`compaction_loop`).
- **Submission:** `enqueue_flush`/`enqueue_compaction` push a `Job` to the shared pool instead
  of the per-DbImpl `FlushQueue`/`CompactionQueue`. The per-CF `compaction_queued` dedup is
  retained (keyed by db+cf) so a DbImpl never occupies >1 compaction slot for the same CF.
- **Cap:** `N_f + N_c` total bg threads, independent of DbImpl count. Defaults reserve the
  foreground: `N_f = max(1, cores/8)`, `N_c = max(2, cores/6)` → on 18 cores ≈ 2 + 3 = 5 bg
  threads (vs 24 today); foreground keeps ~13 cores. Env: `FRS_BG_FLUSH_THREADS`,
  `FRS_BG_COMPACT_THREADS`.
- **Lifecycle:** DbImpl no longer spawns bg threads. On drop, the `Arc<DbImpl>` falls; queued
  `Job`s whose `Weak` no longer upgrades no-op. Scheduler workers live for process lifetime.
- **Correctness:** purely scheduling. Same flush/compaction logic, same per-DbImpl
  `compaction_mutex` serialization, same `version_set.apply` (`apply_lock`). No data-path change
  ⇒ output byte-identical to today; risk is confined to job dispatch/lifecycle.

### Component B — shared block cache (memory; second)

Replace per-DbImpl `ShardedClockCache::with_capacity` (db.rs:641) with one slot/process-global
`Arc<ShardedClockCache>`. **Caveat:** cache keys (`sst_file_num`, `offset`) can collide across
DbImpls → must add a per-DbImpl salt to the key. Memory/efficiency win, not the CPU decay.

## Testing & gate
- **A unit test (TDD):** K DbImpls share one `BgScheduler`; submit > N jobs that block on a
  barrier; assert ≤ N run concurrently (atomic high-water probe) and all complete; Weak-dead
  jobs no-op. Engine suite stays 263 green.
- **A/B gate (clean machine, the real proof):** q4 baseline 545s vs Component-A; expect troughs
  (38-110K) to flatten and finish to drop toward RocksDB 241s. Sweep `N_c ∈ {2,3,4}`.
- **B:** correctness — block-cache round-trip across 2 DbImpls (no cross-instance bleed); memory
  RSS drop; q4 unchanged or better.

## Sequencing (approved)
A (TDD) → clean q4 A/B → B → re-measure → q0-q22 sweep.

# q4 root cause = memory model (43 GB RSS unfit for 8c/32g) — design

**Date:** 2026-06-05. Brainstorming spec. Grounded in fresh back-to-back q4 A/B + `vmmap`.

## The real reason forst-rs trails RocksDB (re-grounded, current data)

Back-to-back on the dev Mac (18c/64g, clean, same harness):

| | RocksDB | forst-rs (sys malloc) | forst-rs (jemalloc) |
|---|---|---|---|
| q4 finish (98M) | **243s, flat** | 461-551s, sawtooth | did not finish 620s (~83M) |
| peak RSS | **6.4 GB** | **43 GB** | 30-34 GB |
| allocator | jemalloc + arena memtables | system malloc | jemalloc |

`vmmap` of the live forst-rs TM at steady state:
- **55.9 million live small allocations**, 16.9 GB allocated in the default malloc zone (~3.5
  allocs/state-row: key `Vec` + value `Vec` + memtable node).
- **MALLOC_SMALL 22.5 GB resident, only 10.6 GB dirty → ~12-14 GB freed-but-retained** by the
  system allocator (never returned to the OS).
- + 12 GB JVM (TM `process.size`).

**Root cause:** forst-rs's q4 working set is ~26 GB live (+14 GB allocator retention = 43 GB RSS)
vs RocksDB's 6.4 GB — because it (a) churns 55.9 M tiny allocations through a non-returning system
allocator and (b) replicates per-DbImpl structures (×~12: own block cache + resident shadow +
memtables + readers) instead of RocksDB's ONE slot-shared, managed-memory-bounded set.

**Why it's fatal on the target:** the benchmark target is **8c/32g**. 43 GB ≫ 32 GB → swap →
collapse (the historical "decay"). On the 64 GB dev box it never swapped, so the severity was
masked — every earlier "contention floor / merge ns/byte" conclusion was measured on a box that
hid the real (memory-fit) binder.

## Critical methodology constraint

**Memory-fit fixes cannot be fairly measured on the 64 GB Mac.** Returning freed memory (jemalloc
decay) only *costs* (re-faults) when RAM is plentiful; its *benefit* (avoiding swap) appears only
when RAM is scarce (32 GB). Proof: jemalloc cut RSS 43→30 GB but ran SLOWER on the 64 GB box
(re-faults, no swap to avoid). **All evaluation must be on a real 8c/32g Linux box** (user
providing). On that box the expected ordering flips: sys-malloc 43 GB swaps/collapses; jemalloc +
arena + shared-resources fits 32 GB and approaches RocksDB.

## The fix — reduce live working set to RocksDB-like ~6-10 GB (staged)

1. **jemalloc global allocator (LANDED).** `forst-rs-ffi` cdylib `#[global_allocator]` =
   `tikv_jemallocator::Jemalloc`, `_rjem_malloc_conf = background_thread:true,dirty_decay_ms:10000,
   muzzy_decay_ms:10000`. Returns freed pages (system malloc never does). Required for the
   constrained target. Decay tuning to be finalized on the 8c/32g box (10s spans the 30s ckpt
   cycle; revisit vs throughput there).
2. **Arena-allocated memtable.** Replace the 55.9 M per-row `Vec` key/value/node allocations with
   pooled big arena buffers (a handful of allocations, sub-allocated). Cuts the 16.9 GB live + the
   allocator pressure that drives retention. The structural core. (Heritage: SegmentedBytes /
   key+value arenas were in progress; finish + make it the memtable's allocation path.)
3. **Slot-shared, enforced-bounded resources (RocksDB managed-memory parity).** ONE block cache +
   ONE resident-shadow budget + ONE write-buffer (memtable) budget shared across all DbImpl
   instances in a slot, sized from a configured fraction of 32 GB — so total native RAM is bounded
   regardless of operator×parallelism count. (Component B + verified-enforced global budgets;
   today the budgets exist but per-instance duplication + churn still dominate.)

## 8c/32g evaluation plan (on the real box)
- Configure both backends to the 8c/32g profile: TM `process.size` + Flink managed memory +
  parallelism + forst-rs bg-pool threads (`FRS_BG_*`) + native budgets, so JVM + native ≤ ~28 GB
  (headroom under 32). Assert peak RSS ≤ 32 GB (no swap).
- Baseline: RocksDB q4 (expect ~flat, ~6 GB, the reference time on 8c).
- forst-rs: (a) sys malloc → expect swap/collapse; (b) jemalloc → fits, measure; (c) +arena;
  (d) +shared resources. Each: finish, RSS curve (swap?), trough count.
- Gate every engine change byte-equivalent (compaction/state suites) before measuring.
- Then q0-q22 sweep on 8c/32g for the headline number.

## Status / readiness
- jemalloc LANDED (uncommitted), ffi builds clean. Streaming merge + fast path + bg pool from
  prior work also landed (all-green). Awaiting the 8c/32g box to measure + then build arena (#2)
  and shared resources (#3). See [[project_q4_binder_is_merge_speed_2026-06-05]] (now superseded on
  the binder: it's MEMORY/RSS, not merge ns/byte — the merge is at its 2.7 ns/byte floor).

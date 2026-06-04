# q4 decay: the two engine drivers vs RocksDB, the resident-bloom-skip fix, and the off-heap over-allocation

**Date:** 2026-06-03 (post-reboot, swap=0 clean machine — first valid q4 profiling this session)
**Status:** Driver 1 FIXED + verified (resident-bloom-skip, tests green). Driver 2 localized (Java off-heap
over-allocation ~22GB for 1.6GB data), OPEN — the dominant remaining limiter.

---

## 1. Why RocksDB / community ForSt stay flat and forst-rs decays (code-verified)

forst-rs q4 bursts ~500K rec/s then decays to ~75–130K; RocksDB holds a STEADY 237K with no decay.
A symbolized native `sample` (clean machine, swap=0) + `FRS_ITER_DIAG` pinned the cause to **per-probe
cost that GROWS with state**, where RocksDB's is bounded. Three structural gaps, all confirmed in code:

| Per-join-probe cost | forst-rs | RocksDB / ForSt |
|---|---|---|
| **Memtable tiers seeked** | 1 active + **4–5 resident-shadow** memtables, each a full BTreeMap lower-bound seek (`btree::search::find_lower_bound_index` = #1 in the profile) | 1 active + ~1 immutable; flushed memtables are **freed** |
| **No skip-gate on resident tiers** | resident memtable has only coarse min/max bounds → pays the O(log N) seek even when the 36-byte prefix is **absent** (`FRS_ITER_DIAG` showed most return 0 keys) | flushed data lives in L0 SSTs with **bloom filters** → absent keys skip without a seek |
| **SST traversal in the scan path** | `build_lazy_prefix` iterates `live_sst_files_iter()` = **ALL live SSTs linearly** (db.rs:5610); point-get binary-searches per level (`find_sst_for_key_in_cf`) but the SCAN path does not | MergingIterator binary-searches **one child per level**; leveled compaction keeps L1+ non-overlapping |
| compaction | "L0 rollup + L1..Ln **size-tiered**" (compaction.rs:21) → overlapping runs per level | **leveled** → 1 non-overlapping run per level |

All of forst-rs's grow with #flushes/#SSTs ⇒ per-probe cost rises ⇒ throughput decays. RocksDB's are
bounded by level count ⇒ steady.

## 2. Driver 1 (CPU) — FIXED: resident-bloom-skip

**Change (db.rs `build_lazy_prefix`, FRS-RESIDENT-BLOOM-SKIP):** before paying the O(log N) BTreeMap
seek on a resident-shadow memtable, consult its source SST reader's decode-free `may_contain_range`
prune (the SAME prune the Tier-3 SST loop already trusts). The resident memtable is byte-identical to
its SST, so when the prune proves the prefix range empty, the key is absent from both → skip the seek
AND keep the SST shadowed (Tier 3 skips it too; no data missed). Falls back to seeking if the reader
isn't cached. Correctness-safe (the prune only skips when provably empty); engine+storage tests green.

**Measured (clean machine, default config, vs the pre-fix post-reboot run):**
- decay onset (60 s): **381K/s vs 210K/s ≈ 1.8×**
- decay window (60→121 s incremental): **192K/s vs 165K/s ≈ +16–18%**
- past ~100 s the win is MASKED by Driver 2 (memory compression), so the standalone gain is modest
  here but real; it will matter more once Driver 2 is fixed and on RAM-tight boxes.

This closes the "no skip-gate on resident tiers" gap — forst-rs now does what RocksDB's L0 blooms do.

## 3. Driver 2 (MEMORY) — OPEN, dominant: off-heap NOT bounded by Flink managed memory

On the clean machine q4 no longer *collapses* (swap stays 0), but it still decays because RSS climbs to
**34 GB** while the on-disk state is only **1.6 GB (66 SSTs)** → macOS compresses ~9 GB → that
compression CPU is what flattens throughput past ~100 s. ~22 GB of that is native (beyond the 12 GB JVM).

**Root cause — NOT a leak; a missing Flink-MemoryManager integration (user-identified).** Flink's TM
budget here is heap 5.25 GB + **managed 4.6 GB** + network 1 GB + direct 1.3 GB ≈ 12 GB; the state
backend's off-heap is SUPPOSED to live inside the **4.6 GB managed** budget. RocksDB's Flink backend
does exactly that: `memoryManager.getSharedMemoryResourceForSlot(...)` → ONE `OpaqueMemoryResource`
sized from managed memory → ONE shared LRUCache + ONE WriteBufferManager shared across ALL keyed
backends in the slot → total bounded ≈ 4.6 GB. **forst-rs does NOT** — `ForStRsSharedResourcesFactory`
self-documents it: *"Today each backend gets its own engine + its own shared-resources view"* (per-slot
sharing is a "future cycle"). So each of q4's ~16 keyed DB instances allocates its OWN, sized from
CONFIG not managed memory: **block cache 256 MiB + WBM 512 MiB + resident shadow ~1 GiB ≈ 1.75 GiB ×
~16 ≈ 28 GiB** — ~6× the managed budget. The **resident shadow (~16 GiB) and WBM (~8 GiB) dominate**;
block cache (~4 GiB) third. NOT the Rust arenas (`INIT_CAPACITY_BYTES = 64 KiB`, grow with data, freed
on drop). This is precisely "the engine should use Flink off-heap, managed by the same memory manager":
today it doesn't, so footprint scales with instance count instead of being capped.

**Prior art / constraint:** a naive *process-shared block cache* was already tried (db.rs:68 /
`2026-05-30-shared-cross-instance-block-cache.md`) and **REVERTED** — it regressed q9 via cross-DB shard
contention (~8 DBs on one `ShardedClockCache`). So the fix is NOT naive sharing of the contended cache.

**Fix (bound total to managed memory WITHOUT the contention regression):**
1. **Global resident-shadow budget** (column_family.rs:54 TODO) — one slot/process-wide byte budget
   instead of 1 GiB × instance-count. Cuts ~16 GiB→~2 GiB. No sharing (each instance keeps its own
   shadow; only the TOTAL is capped) → no contention; correctness-safe (eviction → SST). Highest
   leverage, lowest risk — do this first.
2. **Slot-shared WriteBufferManager** sized from managed memory — WBM is just an atomic charge counter
   (negligible contention, unlike the block cache), so one shared WBM is safe and cuts ~8 GiB→~1.5 GiB.
3. **Wire `getSharedMemoryResourceForSlot`** in `createKeyedStateBackend` to size (1)+(2)+block-cache
   from the 4.6 GB managed budget and reference-count across the slot — finishing the documented
   `ForStRsSharedResourcesFactory` "future cycle". Needs an FFI change: accept a shared WBM/budget
   handle (`Arc<WriteBufferManager>`) on open rather than a per-open capacity.

Net: total forst-rs off-heap capped at the managed budget regardless of instance count → RSS ~12 GB →
no compression → the memory decay driver is gone. Shared by ALL heavy queries (q7/q9/q16/q20).

## 3b. Measured (clean machine, post-reboot, q4 100M, real JM wall-clock) + what's landed

LANDED + tests green this session: (1) **resident-bloom-skip** (Driver 1), (2) **global resident-shadow
budget** (`column_family.rs`: process-wide `GLOBAL_RESIDENT_SHADOW_USED` + `FRS_RESIDENT_SHADOW_TOTAL_MB`,
charge-on-enroll / release-on-evict+prune, FIFO-evict under global pressure; correctness-safe — eviction
falls back to SST; unit test `test_global_resident_shadow_budget_charge_release`).

| config | TM RSS | decay-zone rate | note |
|---|---|---|---|
| full shadow (no budget) | 34 GB | 506K→75K | heavy compression |
| 2 GB global budget + bloom-skip | 20 GB | 150K→68K | shadow-starved reads |
| **8 GB global budget + bloom-skip** | 30 GB | **658K→307K→~120K** | best early/sustain; WBM+block still compress |

The global budget bounds the shadow (RSS 34→20 GB at 2 GB) and is tunable; 8 GB is the throughput sweet
spot on this box (658K/307K through ~80 s **beats RocksDB's 237 K** before compression bites).

**But RSS stays ~30 GB even with the shadow capped at 8 GB** → the remaining ~12 GB native is the
**per-instance WriteBufferManager (~8 GB, 512 MiB×~16) + block cache (~4 GB, 256 MiB×~16)**, which the
shadow budget does NOT touch. On this 64 GB box (≈25–36 GB free) that still forces compression past
~90 s → the decay returns. So the shadow budget is necessary but NOT sufficient.

## 3c. Remaining work to finish "shared memory, managed by the same manager"

1. **Slot-shared WriteBufferManager** sized from managed memory (WBM = atomic charge counter, low
   contention) — one per slot, not 512 MiB × instance-count. (FFI: accept a shared `Arc<WriteBufferManager>`
   handle on `frs_db_open_*` instead of a per-open capacity.)
2. **Wire `getSharedMemoryResourceForSlot`** in `createKeyedStateBackend` (finish `ForStRsSharedResourcesFactory`)
   to size shadow-budget + WBM + block cache from the slot's managed-memory fraction and ref-count them.
3. Block cache: keep per-instance (a naive shared one regressed q9 via shard contention — db.rs:68) but
   size each = managed_fraction / instance_count so the total is bounded.

With all off-heap bounded to managed memory, RSS stays ≈ JVM budget → no compression → the 658K/307K
"uncompressed" rate sustains → q4 ≥ RocksDB parity. The early numbers already show forst-rs is FASTER
than RocksDB when not memory-bound; the gap is purely the missing memory-manager integration.

## 3d. CAPSTONE root cause (vmmap-proven): 137M per-entry index-key allocations

After landing the shadow + WBM budgets, RSS still sat at ~36 GB. `vmmap` on the live TM exposed why —
the dominant consumer is **`DefaultMallocZone` with 137 MILLION live allocations ≈ 32 GB**
(MALLOC_SMALL 18 GB + MALLOC_LARGE 13.5 GB). 137M small (~234-byte avg) objects is a **per-ENTRY
allocation storm**, not a sizing problem. Confirmed in code:

- `VectorizedMemTable` keys its sorted **BTreeMap** index AND its **HashMap** point index on
  `InternalKey { user_key: Arc<[u8]>, sequence }` — **a heap allocation per key entry**.
- The key bytes are **ALREADY packed** in `key_arena` (`key_spans` + `key_at(offset)`). So the index's
  `Arc<[u8]>` is a **DUPLICATE** of the arena copy — pure overhead, one malloc per entry.
- Across tens of millions of entries × (active + immutable + resident-shadow tiers) → ~137M live
  small allocations → 32 GB RSS + massive allocator fragmentation + the `_xzm` malloc-lock contention
  the CPU profile showed. This — not the shadow/WBM *size* — is the dominant RSS driver and why
  bounding shadow+WBM didn't drop RSS.

**This explains everything:** RocksDB's arena `InlineSkipList` stores each key INLINE in a bump arena
(≈ zero per-entry mallocs) → small RSS, no fragmentation, no malloc-lock contention → flat. forst-rs
allocates a heap key per entry → RSS + contention grow with entry count → decay. The burst is fast
(few entries → few allocs); it decays exactly as the allocation count climbs.

**THE FIX (the memtable memory-management refactor — next focused task, correctness-critical):** stop
the index from owning a per-entry heap key. Two options:
1. **Bounded (lower-risk):** change `InternalKey.user_key` from `Arc<[u8]>` to an **inline** small-key
   type (e.g. `SmallVec<[u8; 48]>` / a custom inline-or-heap enum). q4 keys are ~36 B → stored inline
   → **zero heap alloc per entry**; large keys spill to heap. Semantically transparent to Ord/Eq/Hash
   (they already deref to `&[u8]`). Eliminates ~all 137M allocs; B-tree node allocs (~11 keys/node)
   remain (~12M, ~11× fewer). Edit surface: `InternalKey` def + `new`/`range_start` + field — small,
   but it is THE core index, so TDD + full suite + a q4 RSS/throughput A/B gate it.
2. **Ideal (bigger):** replace the std `BTreeMap` index with an **arena `InlineSkipList`** (RocksDB
   model) whose nodes live in the key arena and whose comparator reads the inline key — the index then
   stores NO duplicate key at all (references the arena), eliminating both the per-entry alloc AND the
   duplicate storage. This is the documented "arena-skiplist memtable index" direction.

Expected: option 1 alone should cut RSS from ~36 GB toward ~JVM-budget, removing the compression that
caps q4 at ~120 K and letting the measured **uncompressed burst (579–758 K/s, >2× RocksDB) sustain**.
Verify with `vmmap` (allocation count should drop from 137M to ~10M) + a q4 100M run to completion.

## 4. Status vs the goal

- q4 is **correct and stable** (completes, no collapse post-reboot).
- Driver 1 fixed (resident-bloom-skip, landed, tests green, ~16–18% + 1.8× early).
- Driver 2 (the off-heap over-allocation) is the dominant remaining gap to RocksDB parity — open, localized
  to the Java off-heap path. This is shared by all heavy/stateful queries (q7/q9/q16/q20), so fixing it
  is high-leverage across the suite.

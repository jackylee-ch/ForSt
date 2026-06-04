# q4 "decay" root cause: memory SWAP (resident-shadow RAM × instance-count), not an engine algorithm defect

**Date:** 2026-06-03
**Status:** ROOT CAUSE PROVEN (6 instrumented runs + vm_stat + FRS_ITER_DIAG). Fix designed (global
shadow budget); a clean benchmark on this dev Mac requires a reboot (see §5).

---

## 1. The finding (overturns the read-amplification theory)

q4's throughput "decay" (bursts ~500K rec/s, slides to ~30–60K, never finishes 100M on this Mac) is
**memory swapping**, not SST read-amplification, not the memtable seek, not merge chains, not
compaction fan-out. The engine's read path is fine; it is **starved of RAM**.

`FRS_ITER_DIAG=1` on the live decayed run was decisive. Every slow `build_lazy_prefix` showed:
- `sst_sources=0`, `sst_considered=0` → **not SSTs / not read-amp / not compaction**.
- `resident_shadowed` flat at 4–5 → **not fan-out growth**.
- the time was 100% in `active_us`/`resident_us` (the in-RAM memtable + resident-shadow cursor build),
  and it spiked to **757,640 µs (757 ms)** for a build over 4 *immutable* in-RAM memtables + 1 SST.

757 ms to scan 4 immutable RAM structures for one key is impossible as compute or lock-wait. It is a
**page-in stall**: the structure had been swapped out and the probe blocked on swap-in. `vm_stat`
during the collapse confirmed it: `vm.swapusage used = 11.7 GB`, **`Pages free ≈ 15 MB`**,
`Swapouts = 18.6 M`, **TaskManager RSS = 21–31 GB**.

## 2. Why the RAM blows up — the resident-flushed shadow scales with instance count

forst-rs keeps just-flushed memtables in RAM in DECODED form (the "resident-flushed shadow",
`ColumnFamilyData::resident_flushed`) so a read avoids the SST decompress + Arrow-decode. The cap is
**per-CF = 1 GiB** (`DEFAULT_RESIDENT_FLUSHED_CAP_BYTES`). But a single NexMark query opens **~16
stateful DB instances** (interval-join ×p=4, plus the aggregation/rank operators × their CFs), and the
cap is per-CF — so the shadow scales to **~16 GiB of RAM**. Plus the 12 GiB JVM heap + block cache +
WBM + Arrow off-heap ⇒ TM RSS **31.7 GiB**. That exceeds usable RAM → the OS swaps the shadow out →
the next join probe stalls hundreds of ms on page-in → throughput collapses.

This is exactly the `column_family.rs:54` TODO: *"make this a GLOBAL cross-instance budget so it
cannot scale with instance count."*

## 3. The A/B matrix (6 runs, this Mac, 64 GiB, q4 100M, real JM wall-clock)

| Config | TM RSS | Early rate | Late behaviour |
|---|---|---|---|
| **Default** (1 GiB/CF shadow ≈ 16 GiB total, 12 GiB heap) | 31.7 GiB | **232 K/s = RocksDB parity** | collapses ~60 K after ~140 s (swap) |
| Minimal shadow (`FRS_RESIDENT_SHADOW_MB=64`) | 15.7 GiB | 307 K/s | decays to 33 K then 0 — reads spill to slow SST path; **regression** |
| Small shadow + 4 GiB block cache | 18 GiB | 540→160 K/s | **stable ~70–86 K/s, no collapse** (bounded RAM, but block-cache+SST is ~3× slower/probe than the decoded shadow) |
| 8 GiB heap + full shadow | 27 GiB | 524→399 K/s | collapses ~117 K after ~105 s (still crosses into swap) |

**Reading the matrix:** the resident shadow is what *delivers* RocksDB-parity (232–400 K/s, reads
from decoded RAM). Shrinking it bounds RAM but drops to the slower SST/block path (33–86 K/s). On this
machine **every** config eventually crosses into swap because of §4.

## 4. The dev-machine amplifier: 11.7 GiB of non-draining stale swap

This Mac had **11.7 GiB stuck in swap** (`vm.swapusage used = 11.7 GB`, `free = 1.5 GB`) accumulated
over a long multi-run session. macOS does **not** drain swap without a reboot, so effective RAM was
~64 − 11.7 − OS ≈ **34 GiB usable** instead of 64. A 27–31 GiB TM does not fit in 34 GiB alongside the
OS → swap → collapse. Freeing processes raised `Pages free` to ~33 GiB but the swap stayed at 11.7 GiB
(stale, won't reclaim without reboot). So on THIS machine, in THIS session, no config holds.

## 5. How to get a clean q4 number (and confirm the engine is at parity)

**Reboot the Mac** (drains the 11.7 GiB stale swap → ~60 GiB truly free), then run the DEFAULT config:
`QUERY=q4 CONFIG=forst-rs-ffm-local bash scripts/measure-sql.sh`. With 60 GiB free the 31 GiB TM fits
with no swap, the shadow stays RAM-resident, and q4 holds ~232 K/s = RocksDB parity through 100M.
(Evidence: the default run *did* hold 232 K/s until ~140 s — exactly when growing RSS crossed the
pre-existing swap line. Remove the swap line and it doesn't cross.)

## 6. The durable engine fix (next code step — designed, not yet landed)

**Process-global resident-shadow budget** (the `column_family.rs:54` TODO). Replace the per-CF
1 GiB cap with a process-wide byte budget (default ~4–6 GiB, env `FRS_RESIDENT_SHADOW_TOTAL_MB`)
shared across all CFs/DbImpl instances, so total shadow RAM cannot scale with instance count. FIFO-
evict oldest across the budget (keeping the hottest just-flushed tail). **Correctness-safe by design:**
the shadow is a pure read accelerator — every evicted entry falls back to the durable SST (Tier 3), so
mis-accounting can only slow reads, never lose data or crash.

Recommended implementation (clean + testable): inject an `Arc<ResidentShadowBudget>` into
`ColumnFamilyData` (production shares one process-global instance via `OnceLock`; tests inject a fresh
small budget). Enforce on `add_resident_flushed_with_bounds`; decrement on FIFO evict + `prune_resident_flushed`.
TDD: two CFs sharing one small budget, assert total resident bytes ≤ budget.

This bounds RSS (no swap → no collapse) regardless of instance count or machine. On a RAM-constrained
box the bounded shadow serves the hot tail from RAM and spills the cold remainder to the (fast, local,
FRS-LOCAL-DIRECT-READ + block-cached) SST path — steady throughput, no cliff. On a RAM-rich box, raise
the budget to keep the full working set resident (full parity). The deeper, longer lever to make the
spill cheap (so a small shadow suffices everywhere) is the documented faster SST/memtable read path
(arena/skiplist index + lock-free memtable).

## 7. What was tried and REVERTED (don't repeat)

- **Skip the resident shadow for local backends** (gate `add_resident_flushed` on `supports_atomic_rename`):
  REVERTED — the A/B proved removing the shadow regresses q4 to 33–86 K/s (the shadow is the parity
  lever, not the problem; its *unbounded scaling* is). The fix is to BOUND it globally, not remove it.
- block cache 2–4 GiB, 4 KB block size, 1-shard memtable, BTreeMap index, FRS-LOCAL-DIRECT-READ:
  all real improvements to per-probe cost, but none addresses the swap (the actual decay driver).

# Resident RAM-shadow sizing defeats the ckpt-ON heavy-join collapse

Date: 2026-05-31
Status: BREAKTHROUGH (measured on q4). Shadow lever defeats the collapse; a
follow-on resident-scan consolidation is the remaining lever toward the
ckpt-OFF-level heavy-query wins under ckpt-ON.

## The insight I had been missing

The heavy-join collapse under ckpt-ON (q4/q7/q9/q11/q15/q16/q18/q19/q20 capping
at the MAXSEC wall) was blamed on "working-set exceeds RAM → unavoidable
S3+Arrow-decode". That framing ignored **partitioning**: Flink keyed state is
sharded by key-group across the `parallelism` backend instances, so EACH
instance holds only ~1/parallelism of the total join state. A parallelism-4
heavy query → 4 instances → each holds ~1/4 of the working set.

The resident-flushed RAM shadow (the decoded, in-RAM copy of just-flushed
memtables, read via their BTree index with NO decompress/Arrow-decode) was
capped at **1 GiB/instance**. When each instance's *share* of the join state
exceeds 1 GiB, the shadow evicts and the overflow is read from S3-backed SSTs
(decompress + Arrow-IPC decode per block) — the collapse. ckpt-OFF never hit
this because state stayed in the active 6 GiB memtable (also RAM-resident,
decoded) — which is why ckpt-OFF gave the 22–35× heavy-query wins.

## The lever: size the shadow to one instance's share

Added `FRS_RESIDENT_SHADOW_MB` (engine env hook, `column_family.rs::
resident_flushed_cap_bytes`) overriding the 1 GiB default. Sized to hold one
instance's partitioned share, the just-flushed state stays RAM-resident under
ckpt-ON — reads hit the in-RAM BTree, never the S3 SST.

## Measured (q4, 100M, 8c/32g, ckpt-ON, FRS_RESIDENT_SHADOW_MB=2048)

| | 1 GiB shadow (default) | **2 GiB shadow** |
|---|---|---|
| behaviour at ~32M | **hard freeze** → 100–500/s | **no freeze** — sails through |
| 40M reached | never (frozen) | **362 s, ~49K/s** |
| 45M reached | never | **520 s (MAXSEC cap)** |
| RAM | — | 2 GiB × 4 inst = 8 GiB (+6 WBM +8 JVM ≈ 23 GiB, no OOM) |

The catastrophic collapse is **eliminated** — converted into a gradual rate
decline (185K/s at small state → ~25K/s at 45M). q4 still did not finish 100M
within the 520 s cap, but it is no longer collapsing.

## Why it still declines — and the remaining lever

The decline is NOT eviction (the shadow wasn't full at 45M) — it is the Tier-2
prefix-scan cost in `build_lazy_prefix_key_stream`: it builds a
`prefix_scan_cursor` for EVERY resident memtable whose [min,max] overlaps the
scan window, then k-way merges. Each 64 MiB flush adds a resident memtable, and
a heavy join's flushed memtables each cover a broad (random-sorted-subset) key
range → almost all overlap any given prefix → the per-probe cost grows
O(num_resident_memtables). ckpt-OFF had ONE big memtable (O(log)); the ckpt-ON
shadow has dozens (O(N·log)).

**So the path to the ckpt-OFF-level heavy-query wins under ckpt-ON is:
consolidate the resident-flushed memtables** (an in-RAM merge of several
immutable flushed memtables into one larger sorted structure — analogous to LSM
compaction but RAM-resident, preserving sequence-dedup + snapshot semantics), so
per-probe prefix scans are O(log) again. With state RAM-resident (shadow) AND
O(log) access (consolidation), forst-rs's vectorized batch path should regain
the per-record advantage that gave 22–35× on heavy joins at ckpt-OFF.

## Revised verdict

3× total is **not** the foregone-infeasible result the earlier capped sweep
suggested. The capped sweep (≤0.57×) measured the COLLAPSED state. The collapse
is now shown to be a *sizing/structure* problem, not a fundamental S3 ceiling:
- Shadow sizing (this doc) defeats the collapse — PROVEN.
- Resident-memtable consolidation (next) should restore O(log) resident access.
- The ~10% stateless-query overhead (q0–q2 at 0.90×, FFM+JDK25) remains, but is
  dwarfed by the heavy queries that dominate the total — exactly as at ckpt-OFF
  where the total hit 4.69× despite that overhead.

## Follow-up tests that narrowed the residual (measured)

- **Larger memtable (64→256 MiB, fewer resident memtables) — MARGINAL.** q4
  reached 48.3M@600s vs 64 MiB's 45.1M@520s, still declining 22K→15K/s. So the
  residual is **NOT** O(num_resident_memtables) — reducing the count barely
  helped. Reverted (also risks the v5x q3 write-cost regression).
- **Symbol profile** at the declining point (partial — caught during teardown)
  showed `prefix_scan_cursor → BTreeMap::range` + a per-probe
  `Version::live_sst_file_numbers` HashSet alloc, but no single dominant frame.

**Refined diagnosis of the residual:** it is most consistent with an
**intrinsic per-row cost**, not a structural memtable issue. q4's bottleneck
operator is the auction⋈bid **join**, whose MapState probe iterates all buffered
records for the join key. NEXMark auction popularity is Zipfian, so hot auctions
accumulate more buffered bids over time → the per-probe result grows → the rate
declines as state grows (matches the curve, and explains why memtable *size*
didn't help). Each iterated row pays the FFM-crossing + Arrow-decode tax — the
SAME overhead that shows as 0.90× on the stateless light queries (q0–q2), now
amplified by join row-volume. The Flink join operator's per-probe re-scan is
runtime behaviour (immutable, cannot change).

## Final, measured verdict

- The **shadow lever is a real, valuable win**: it converts the heavy-query
  *collapse* (NA / capped) into *finishing-but-slow* (gradual decline, no
  freeze) — proven on q4 (32M freeze → 48M decline). Heavy queries that would
  cap now make progress. KEEP it (env-tunable per parallelism).
- BUT it does **not** reach 3×. Even with no collapse, forst-rs/S3 is
  per-query slower than rocksdb-local because of the **FFM + Arrow per-row tax**
  (structural to the goal's own constraints: Panama FFI not JNI, Arrow not
  byte[]) on top of S3-backed reads. The stateless floor is already 0.90×; the
  join per-row tax makes heavy queries multiples slower.
- Resident-memtable **consolidation** would address the O(num_resident) term —
  but the 256 MiB test shows that term is NOT the dominant residual, so
  consolidation would likely also be marginal. It is therefore **deprioritised**
  (was the presumed next lever; measurement demoted it).

**Achievable target on 8c/32g + ckpt-ON + S3-primary vs local-disk RocksDB:
no collapse (shadow lever), all queries finish, total ~parity-to-slower — NOT
3× faster.** 3× requires either a config where state stays memtable-resident
without ckpt-ON flush pressure (the original 4.69× ckpt-OFF regime), much larger
RAM, or a baseline of RocksDB-on-S3 rather than RocksDB-on-local-disk.

## MEASURED full q0–q22 WITH the shadow lever (the gap, now closed)

Ran the full forst-rs sweep with `FRS_RESIDENT_SHADOW_MB=2048` (MAXSEC=700),
rocksdb baseline reused. Three-way:

| total | value |
|---|---|
| rocksdb (baseline) | 4238 s |
| forst-rs, no shadow | ≥ 7467 s (≤0.57×) |
| **forst-rs, +shadow** | **≥ 7614 s (≤0.56×)** |

The shadow total is **the same as no-shadow — marginally worse**. Why:
- Capped/collapse queries (q4/q7/q9/q11/q15/q16/q18/q19/q20) stay **NA** at the
  700 s cap — the shadow lets them reach further (q4 48M vs 32M) but at ~20K/s
  they still cannot finish 100M, so they cap either way → no total benefit.
- Finishing queries **regress**: q8 72→108 s, q17 353→461 s — the larger
  resident-memtable set raises the per-probe Tier-2 scan cost for queries that
  never needed the shadow.

So a **global** shadow is net-neutral-to-negative for the total. It is only
useful applied *selectively* to collapse-prone heavy queries — and even then the
total is unchanged because those queries remain capped (their ~20K/s steady rate
is ~20× below rocksdb's 383K/s on q4). The shadow's value is qualitative
(freeze → progress), not a total-time win on this envelope.

## Conclusive verdict (both full sweeps now measured)

3× is not achievable on 8c/32g + ckpt-ON + S3-primary vs local-disk RocksDB.
Measured both ways (no-shadow ≤0.57×, +shadow ≤0.56×). Root cause is the
intrinsic FFM + Arrow per-row tax (already 0.90× on stateless queries) plus
S3-backed reads, both structural to the goal's own constraints. The original
4.69× was ckpt-OFF (state memtable-resident). All changes correctness-clean
(578 unit tests green), uncommitted. Recommended config change if a win is
required: ckpt-OFF regime, larger RAM, or rebaseline vs RocksDB-on-S3.

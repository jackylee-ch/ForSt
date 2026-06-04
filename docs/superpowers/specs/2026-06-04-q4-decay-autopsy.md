# q4 decay autopsy — decompose the burst→floor decay; branch on the cause

**Date:** 2026-06-04
**Baseline:** commit 274bc0204 (C KV format + B1 + buffer-reuse + get_range_into, all test-green).
**Status:** CLOSED (see the falsification table + terminal synthesis below).

## ⇒ SUCCESSION — the q4 line's true heir (start HERE if revisiting the RocksDB gap)
q4 itself is closed: the residual decay is the active-memtable BTreeMap seek (~724 ns), which is
**irreducible** (arena-skiplist refuted by spike; memtable-size O(log N)-insensitive) AND is the same cost
class RocksDB's own memtable seek pays — so it is **not the gap**. The investigation proved (falsification
table below) that the **sole genuinely forst-specific source of the structural RocksDB gap is the Tier-2
RESIDENT-SHADOW** (the in-RAM cache of flushed SSTs that RocksDB has no analogue for). It was built for the
S3 regime; post-C, local SST reads are cheap, so the shadow's per-probe bloom+cursor (~1400 ns) may now
cost MORE than the SST read it avoids (~855 ns).
**HEIR = the post-C re-evaluation of Tier-2 (resident-shadow).** Entry points already written:
- the local-warm **bypass** analysis + its net recompute (~340 ns, bloom is a wash) + regime-gate +
  cross-tier MVCC correctness gate spec — §"VERIFICATION before building the resident-shadow bypass" below;
- the broader **"should Tier-2 be redesigned wholesale post-C"** question — PMC item in
  `2026-06-04-PMC-q4-arena-skiplist-DECISION.md`.
Anyone reopening the gap should start from Tier-2, NOT from the active-cursor/arena-skiplist (refuted) or
the 993 ms lock stalls (red herring). Tracked as a task so this entry point isn't lost when q4 archives.

## Method (what we instrument, and what each answers)
- **Per-level LSM shape** (`FRS_DECAY_DIAG`, per flush): `[DECAY_DIAG] flush#N ssts=… state=…MiB levels=[L0=n/MiB L1=…]`.
  → SSTs decomposed by level + the **state-size curve** (`state` = Σ live SST bytes).
- **Fan-out by source** (`FRS_ITER_DIAG` `sst_sources` per slow prefix-build) cross-referenced with the
  per-level counts → how many of the per-probe sources are L0 vs deeper.
- **Window expiry fires?** ⇐ the `state` curve: **monotonic growth ⇒ expiry NOT reclaiming** (the interval
  join isn't deleting aged state → root cause is the operator's state-lifecycle layer, not the engine);
  a **plateau ⇒ expiry fires** and the LSM reaches steady state.

## Config parity (item 5) — the rocksdb comparison is NOT fully apples-to-apples
Read from the live templates:
- **Checkpoint strategy DIFFERS:** rocksdb `state.backend.incremental: true` (incremental — only new SSTs);
  forst-rs `forst.rs.checkpoint.noflush=false` (flushing checkpoints). Different cost curves; part of the
  standing gap is checkpoint strategy, not engine read-path.
- **Block cache DIFFERS:** this forst-rs run forces 8 KiB blocks + 4 GiB decoded-block cache via env;
  rocksdb's block cache lives in Flink managed memory (default sizing). Not matched.
- **Matched:** both LOCAL state dir (`file://`), same query/scale, 30 s checkpoint interval. forst-rs timer
  = FORSTRS; rocksdb = heap timer.
⇒ When attributing the gap, separate the **engine read-amp** axis (this autopsy) from the **checkpoint +
cache config** axis. A like-for-like gap number needs matched checkpoint/cache config (future work).

## Pre-committed branch criteria (decide AFTER the data, no goalpost-moving)
1. **L0-driven** — decay tracks L0 SST count; fan-out dominated by L0 sources; deeper levels few.
   → Lever: compaction tuning (more aggressive L0→L1, subcompaction, lower L0 trigger). Engine-side, policy-clean.
2. **Self-asymptoting** — fan-out bounded by LSM depth; `state` plateaus; floor is a genuine steady state.
   → Action: confirm + accept the floor; document it as the LSM-depth read-amp tax (no further engine lever
   without a format/structure change).
3. **Unbounded state** — `state` grows monotonically without bound (beyond what the window size implies);
   expiry not firing. → Root cause is the **operator state-lifecycle layer** (Java backend / Flink interval-join
   retention), NOT the engine — and this would finally explain the standing gap to RocksDB (both engines would
   carry the same unbounded state, but forst-rs's per-probe read tax over that state is higher).

## Results

### KEY REFRAME (mid-run, decisive): q4's wall is the RESIDENT path, not SST read-amp
- **`DECAY_DIAG` fired 0×** → the write-buffer flush path (`run_flush`) essentially never triggers in q4:
  the 1024 MiB memtable rarely fills, so SSTs are **checkpoint-driven** (`noflush=false`, 30 s interval).
  ⇒ the LSM stays **shallow**; the SST-side per-level/state instrument is the wrong probe for this regime
  (q4 state lives in the RESIDENT memtable/shadow, not in SSTs). [Instrument bug: hook was on run_flush;
  to capture the real curve, instrument the resident/memtable size, not SST bytes.]
- **`FRS_ITER_DIAG` (51 slow builds >1ms over ~160 s):**
  - `sst_sources` histogram: **0×34, 1×2, 2×4, 3×5, 4×3, 5×3** → fan-out is **LOW (≤5)** and **34/51 slow
    builds touch ZERO SSTs**. Read-amp/fan-out is NOT the dominant q4 cost.
  - Those SST-free slow builds have **`resident_us` = 150–243 ms** (`sst_open_us=0`): the stall is entirely
    in the **resident prefix path** (in-RAM state walk + lock contention with background flush/compaction),
    not disk/SST.
- **Reconciliation with the read_at win:** the BULK of builds are fast, SST-warm reads (read_at ~99.5%
  warm, ~1.7 µs) — that's where C / get_range_into / B1 helped (197K→230K). But the q4 throughput **dips**
  are the rare 150–243 ms RESIDENT stalls. So q4 has TWO cost components: (a) modest SST-warm-read bulk
  [addressed], (b) resident-path stalls [the dominant remaining wall, engine state/locking layer].

### Branch verdict (vs the pre-committed criteria)
- **NOT branch 1 (L0-driven):** fan-out is low (≤5), LSM shallow (checkpoint-driven flush), SSTs cheap.
  Compaction tuning would not move q4.
- **Toward branch 3 (cost is in the resident/state layer, not the engine SST read path):** the dominant
  stalls are `resident_us` 150–243 ms with zero SST work — the in-RAM resident prefix scan + its lock
  contention during background churn. This is consistent with the standing "q4 is coordination/state-bound,
  not engine-read-fixable" finding, and explains why SST-read-path wins (C/B1/get_range_into) only partly
  move q4. **Confirm-needed:** the resident/memtable STATE-SIZE curve (is the resident set growing
  unbounded ⇒ window expiry not firing ⇒ operator state-lifecycle; or bounded ⇒ pure lock-granularity).
- **Corrected next instrument:** resident-shadow + active-memtable entry/byte count over time (NOT SST
  bytes), + whether the 150–243 ms stalls cluster at 30 s checkpoint boundaries (⇒ checkpoint-flush-induced
  resident/version lock contention).

### Full-run confirmation (300 s, q4 reached only 65M/100M — never finished; TPS 587K→110K, no asymptote)
- **Fan-out grew 0→9** over the run (`sst_sources`: 34×0, then 1–9 with 6–9 concentrated in the 2nd half) —
  modest SST read-amp growth as checkpoint flushes accumulate. Real but secondary.
- **SST-free resident stalls (34 builds): `resident_us` median 1.3 ms, max 993 ms**, and the **trend is the
  proof**: first-10 ≈ 0–1 µs → last-10 climb to 3.5 ms … **993 ms**. The resident-path cost **GROWS over the
  run**. `DECAY_DIAG`=0 (write-buffer flush never fires; SSTs are checkpoint-driven → LSM shallow).
- Throughput **never asymptotes** (declines through 300 s).

### FINAL VERDICT → Branch 3 (resident/state layer, NOT the engine SST read path)
- **NOT branch 1 (L0-driven):** LSM shallow, fan-out modest (≤9), SSTs warm/cheap — compaction tuning won't
  move q4.
- **NOT branch 2 (self-asymptoting):** monotonic decline through 300 s; no floor.
- **Branch 3:** the dominant, GROWING cost is `resident_us` (0 µs → hundreds of ms) on SST-FREE builds — the
  in-RAM resident-shadow prefix path. This is why SST-read-path wins (C/B1/get_range_into) only modestly
  moved q4 (they fixed the cheap bulk; the resident path is the wall), and it explains the standing RocksDB
  gap (RocksDB has no resident-shadow scan path).

**Open disambiguation (one targeted probe):** `resident_us` growing is consistent with EITHER
(3a) unbounded resident SET SIZE — interval-join window expiry not bounding state → operator state-lifecycle
layer; OR (3b) worsening resident/version LOCK CONTENTION during checkpoint-flush churn. Both are
state-layer, not SST read-amp, but imply different fixes. **Corrected instrument:** the resident-shadow +
active-memtable ENTRY-COUNT curve over time (not SST bytes), and check whether the big `resident_us` stalls
coincide with checkpoint-flush windows (⇒ 3b) or scale with cumulative state (⇒ 3a). `DECAY_DIAG` should be
re-pointed at the resident/memtable size, not `version_set` SST bytes.

## DISAMBIGUATION (2026-06-04) — discriminator = `resident_us ÷ entries`, + O(matched)-vs-O(total) by code
Added `resident_total / resident_examined / resident_seeks` to `FRS_ITER_DIAG` (the per-entry-cost
discriminator the masquerade demands: 3a and 3b mask each other because bigger state lengthens the flush
write-lock hold, so pure 3a auto-generates a 3b appearance — only per-entry cost separates "more work" from
"slower work"). Re-ran q4 300 s.
- **Measured (SST-free slow builds):** `resident_total` = **1–3 (NOT growing)**; slow builds show
  `resident_total=1, examined=1, seeks=1` yet `resident_us` = **1.66 ms** (up to **993 ms**); per-entry cost
  **rose 0 µs → 1.6 ms** over the run on **N≈1**.
- **Code (O(matched) vs O(total)):** inner `VectorizedMemTable::prefix_scan_keys` = sorted-BTreeMap **range**
  (O(log+matched)) + ≤4096-bounded unsorted filter ⇒ **O(matched), NOT O(total)**; selective 36-byte prefix
  → matched≈1 → microseconds. So the 1.6–993 ms is **not algorithmic**.
- **⇒ VERDICT: 3b (LOCK CONTENTION), not 3a, not O(total).** N small (1–3) ⇒ NOT 3a-masquerading; genuine
  "slower work" — the resident read path (`resident_flushed.read()` / per-shard `RwLock::read()`) blocks
  behind a flush/eviction WRITE-lock; sporadic stalls lengthen as flush work grows. Per-entry cost rises
  while N stays flat → discriminator points cleanly at contention. Scan algorithm is sound.
- **Fix direction (NOT touched — diagnosis only):** lock-free / snapshot the resident-read path (ArcSwap the
  `resident_flushed` list, as `sst_readers`/`version` already are) so probes never block behind flush
  write-locks. Policy-clean (ArcSwap, no unsafe). Kills the *dips* (993 ms stalls); the *steady* decline is
  partly SST fan-out (0→9), already partly addressed by C — weigh the Amdahl ceiling first.

## Amdahl ceiling — RocksDB q4 (300 s) + the 3b stall-budget
- **RocksDB: FINISHES 100M in 256 s, FLAT ~350–440K/s, NO decay.** forst-rs (KV+B1+get_range_into): decays
  587K→107K, reaches only 64.5M @300 s, never finishes. Caveat: rocksdb incremental ckpt + Flink-managed
  cache vs forst noflush + 4 GiB env cache (asymmetric) — BUT rocksdb's FLATNESS vs forst's DECAY is
  config-independent (both checkpoint at 30 s; the decay is a forst-specific read/state-path phenomenon).
- **forst-rs BURST (587K) BEATS rocksdb (440K)** — so the *entire* ~3.3× floor gap IS the decay. If the
  decay were removed, forst-rs reaches/beats rocksdb.
- **3b stall budget (the 993 ms lock-contention spikes):** Σ slow-build `resident_us` ≈ **2.1 s** over a
  300–1200 s thread-wall = **0.2–0.7%**. ⇒ **the 3b lock-contention fix recovers <1% of throughput — it is
  a TAIL-LATENCY fix, NOT the throughput lever.** (Vindicates "no fix until the discriminator is clear": the
  eye-catching 993 ms stalls are a red herring for throughput.)

## FINAL conclusion + corrected next lever
- **Disambiguation done:** the stalls are **3b (lock contention)**, not 3a (resident N=1–3, flat) and not
  O(total) (scan is O(matched)). **But 3b is ~0.5% of wall** → not worth fixing for throughput.
- **The real 3.3× floor gap is the BULK per-probe cost** (the ~99% of wall in fast <1 ms builds): SST
  fan-out (≤9 warm reads — where C/B1/get_range_into already operate, 197K→230K) + the resident-read FIXED
  overhead per probe (`resident_flushed.read()`+clone, the per-shard `prefix_scan_keys` O(matched + ≤4096
  unsorted filter) + the per-shard `keys.sort()`). RocksDB's flat curve = it has no such resident-shadow
  per-probe tax.
- **Corrected next instrument (NOT a fix):** lower the `FRS_ITER_DIAG` threshold (or sample) to capture the
  BULK build-cost composition — split resident-fixed-overhead vs SST-fan-out within the fast builds — to
  pick the bulk lever (reduce fan-out via compaction/cache, or cut the resident per-probe fixed cost). The
  3b lock-free/ArcSwap change is DEPRIORITIZED (tail-latency only).
- **Touched no resident-layer fix** (per directive). Diagnostic instruments (`FRS_DECAY_DIAG`,
  `resident_total/examined/seeks`) are uncommitted, gated, additive.

## ArcSwap ceiling — counterfactual (cheap, internal; preferred over the config-polluted RocksDB gap)
Subtract the resident-stall throughput losses from the q4 curve. The loss is NOT the stall duration — it is
the events the ONE stalled (key-partitioned) join thread would have processed during it, at the **decayed**
per-thread rate (~13–27K/s); the other 3 threads are unaffected (independent partitions).
- Observed stall budget ~1.2 s (mostly 1–3 ms + one 993 ms over 34 SST-free + ~37 with-source slow builds):
  **lost ≈ 62K events ≈ 0.10% of the run.**
- Generous upper bound (5× more 993 ms stalls than seen, ~5.3 s): **≈ 0.44%.**
- ⇒ **ArcSwap's throughput ceiling ≈ 0.1–0.5% — TAIL-LATENCY only.** The "993 ms-vs-1.7 µs gap lets a few
  dips dominate" intuition is REFUTED: ~34 dips over 300 s, dominated by a single 993 ms, cannot move a
  decayed-rate curve. **Decision: do NOT build ArcSwap for throughput.** (More reliable than the RocksDB
  gap, which §"Amdahl ceiling" flags as inflated by checkpoint/cache asymmetry.)

### ArcSwap concurrency-gate spec (PRESERVED for if it's ever pursued for tail latency — NOT built now)
Per directive, copying the `sst_readers` ArcSwap pattern is NOT assumed semantically equivalent. A #2-level
(get_range_into-grade) concurrency correctness gate would have to cover, beyond round-trip:
- **read-side stale-snapshot × flush/eviction interleaving:** a probe loads the resident snapshot, then a
  flush installs a new resident entry AND/OR an eviction drops one mid-probe — the probe must still return a
  correct, complete result (no missed key, no double-count) against the snapshot it holds.
- **resident→SST handoff atomicity:** the window where an entry transitions from resident-served to
  SST-served must not let a probe miss it in BOTH (the `resident_shadowed` set + Tier-3 SST skip must stay
  consistent under a stale snapshot).
- multi-thread stress: N probe threads × concurrent flush/evict, asserted against a single-threaded
  ground-truth reference (the pattern used for `get_range_into`).

## THE REAL LEVER (where the 3.3× gap lives) — next DIAGNOSTIC, not a fix
The ~99% of wall is the BULK fast-build per-probe cost: SST fan-out (≤9 warm reads — C/B1/get_range_into
already operate here, 197K→230K) + the resident-read FIXED overhead per probe (`resident_flushed.read()` +
O(N) clone, per-shard `prefix_scan_keys` O(matched + ≤4096 unsorted filter), per-shard `keys.sort()`).
Next step: lower `FRS_ITER_DIAG`'s 1 ms threshold (or sample 1/K builds) to capture the BULK build-cost
composition and split resident-fixed-overhead vs SST-fan-out — THAT picks the real lever. ArcSwap is parked.

## BULK breakdown (2026-06-04) — `FRS_BULK_SAMPLE=256`, 1/K sampled ns timing (no observer-effect)
Added a 1-in-256 sampled nanosecond breakdown of `build_lazy_prefix` (`bulk_sample_k/hit/record` in db.rs):
`rfve` (resident_flushed read-lock+O(N) clone) / `bloom` (per-resident `may_contain_range` prune) /
`cursor` (resident `prefix_scan_cursor` = scan+sort) / `sst_fanout` (Tier-3) / unaccounted (Tier-1 ACTIVE
memtable cursor + setup). Cumulative-avg trend over the run (burst→floor), q4 300 s:

| samples | total ns | rfve | bloom | cursor(resident) | sst_fanout | unaccounted(active+setup) |
|---|---|---|---|---|---|---|
| 8192 (burst) | 645 | 23 | 0 | 0 | 21 | ~600 |
| 49152 | 1591 | 38 | 309 | 401 | 40 | ~800 |
| 122880 | 2659 | 45 | 555 | 643 | 416 | ~1000 |
| 155648 (floor) | 3358 | 48 | 662 | 739 | 855 | ~1054 |

**Floor composition:** memtable-scan family (active-cursor ~1054 + resident `cursor` 739 + `bloom` 662) ≈
**~73%**; **`sst_fanout` 855 ≈ ~25%** (real, secondary; grew as fan-out 0→9, partly C-bounded); **`rfve`
48 ≈ ~1%** (clone/lock NEGLIGIBLE — re-confirms ArcSwap was rightly rejected). The per-probe cost RISES
0.6→3.4 µs over the run = the decay, driven by the memtable-scan family + late fan-out, NOT clone/lock.

## STRUCTURAL ROOT of the 3.3× gap + answer to "why a resident-shadow RocksDB doesn't have"
Per probe, forst-rs pays: Tier-1 active `prefix_scan_cursor` + **Tier-2 per-resident-memtable [`bloom` +
`prefix_scan_cursor`]** + Tier-3 SST. RocksDB pays: ONE O(log) memtable skiplist seek + a merged SST
iterator. Two structural costs:
1. **The resident-shadow tier (Tier-2) is now NET-NEGATIVE on local/warm storage.** It was built to AVOID
   expensive S3 SST reads — but post-C an SST read is **~855 ns (warm)** while the resident `bloom`+`cursor`
   it substitutes costs **~1400 ns**. So the shadow costs MORE than the SST it shadows, and its growth
   (accumulating resident memtables) is a decay component. **This is why forst-rs decays and RocksDB
   (no shadow) stays flat.** Fast-path bypass = on local/warm storage (cheap SSTs), **skip the resident
   shadow and read the SST** → saves ~1400 ns, replaced by ~855 ns, net ~550 ns/probe + removes the
   shadow-growth decay. ⚠️ REGIME-DEPENDENT: an earlier "resident-shadow-skip" was REVERTED because it
   REGRESSED in the S3/expensive-read regime (see [[project_q4_decay_is_memory_swap_2026-06-03]]); the
   calculus FLIPPED only because C made local SST reads cheap. Bypass must be gated on local/warm, and
   needs its own design + MVCC/shadowing correctness gate (it changes which tier serves a read).
2. **The active-memtable `prefix_scan_cursor` (~1054 ns)** vs RocksDB's ~O(log) skiplist seek — the
   vectorized-memtable read structure (per-shard `keys.sort()` + O(≤4096) unsorted-buffer linear filter).
   A per-probe cost on the always-present active memtable.

**Levers (diagnosis only — NOT built):** (1) resident-shadow bypass on local/warm — the clearest, biggest,
and it directly explains the RocksDB-flat-vs-forst-decay gap; (2) the active-memtable cursor structure
(unsorted-filter + per-shard sort). SST fan-out (~25%) is the residual where C/compaction/cache operate.
ArcSwap lock fix stays parked (≪1% + now re-confirmed rfve≈1%). Diagnostics uncommitted/gated; baseline
274bc0204 unchanged.

## VERIFICATION before building the resident-shadow bypass (it is a previously-reverted lever — treat as suspect)
The bypass is isomorphic to this session's opening SkipMap revert (a clean-looking change that regressed),
so verify the net benefit + gate reliability BEFORE building.

### (1) Net-benefit RECOMPUTE — the ~550 ns was OVERSTATED (bloom is a wash, not a saving)
Tier-3 ALSO calls `reader.may_contain_range` per non-shadowed SST (db.rs:5837). So **every overlapping SST
is bloomed exactly once** — currently split (Tier-2 for resident-shadowed SSTs, Tier-3 for the rest); after
bypass, ALL in Tier-3. **The 662 ns Tier-2 bloom is RELOCATED to Tier-3, not removed.** Likewise the ABSENT
case is a wash (bloom→skip, before and after). The ONLY thing that changes is the **PRESENT** resident
entries (`resident_seeks` ≈ 1/probe): currently served by the resident `cursor` (~739 ns); after bypass,
read from the SST in Tier-3. So:
  **net ≈ (resident cursor ~739 ns) − (warm read of that 1 SST source) − (nothing from bloom).**
The per-SST-source warm-read cost is NOT yet isolated (it's lumped in the 855 ns `sst_fanout` across all
sources). If it is ~300–400 ns, net ≈ ~340–440 ns/probe; if SST-read ≈ resident-cursor, net → ~0.
**⇒ the flip is NOT confirmed.** Required: isolate per-SST-source warm-read cost (cheap, same sampled
diag) and compare to the ~739 ns resident cursor. Do NOT build until net is confirmed clearly positive.

### (2) Regime-gate reliability — a wrong gate = relapse into the S3-revert
Bypass is only safe when the SST read is provably cheap. A cache MISS after bypass falls to the **remote
(S3) reader** (`LocalFirstSstFile` slow path) — the exact condition the original shadow-skip revert hit.
Conservative criterion: bypass ONLY when (a) `storage.uri` is local (`file://`) AND (b) the block is
guaranteed in the local write-through cache (not evicted). "Warm in OS page cache" is not knowable a priori
— so the gate must key on cache-RESIDENCY, not page-warmth, and fall back to the resident tier on any
uncertainty. This is NOT trivially definable; design it before building.

### (3) REPRIORITIZATION — the active-memtable cursor is the bigger AND safer lever
The Tier-1 ACTIVE-memtable `prefix_scan_cursor` (~1054 ns, the largest single component) is **always paid,
has NO regime dependence, and NO cross-tier MVCC-handoff risk** — vs the resident bypass (~340 ns net at
best, regime-risky, reverted-before). Its cost is the vectorized-memtable read structure (per-shard
`keys.sort()` + O(≤4096) unsorted-buffer linear filter) vs RocksDB's single O(log) skiplist seek.
**Pursue the active-cursor structure (lever #2) over the resident bypass (lever #1).** First confirm its
size with an active-tier timer (the ~1054 ns is currently inferred as "unaccounted", not directly measured).

### Cross-tier MVCC-visibility gate (PRESERVED for if the bypass is ever built — stricter than C/#2)
Bypass changes WHICH tier serves a read, so beyond round-trip + the get_range_into concurrency gate, it must cover:
- **flush-handoff window:** between a memtable being frozen and its SST becoming durable+visible+local-cached,
  a probe must not bypass to an SST that isn't readable yet (would miss the data). `resident_flushed` holds
  flushed-to-SST memtables (byte-identical, same seqs) — the bypass is only sound for entries whose SST is
  CONFIRMED live+local; the handoff edge is the silent-bug surface (class of seek_restart / ArcSwap handoff).
- **newest-version-still-resident-unflushed:** verify Tier-2 `resident_flushed` NEVER holds a version newer
  than the SST (it shouldn't — it's post-flush), else bypassing → serve a stale SST version (visibility bug).
  The active memtable (Tier-1, unflushed newest) is never bypassed — confirm that invariant holds.
- N-thread × concurrent-flush/evict/compaction, asserted vs a single-threaded ground-truth reference.

### PMC item
If the bypass's net is confirmed positive, the deeper question: **should Tier-2 (resident shadow) be
redesigned wholesale for the post-C world?** Built for the S3 regime (avoid remote reads); after C made
local SST reads cheap, a RAM shadow of flushed SSTs may be net-negative on local — possibly replace it with
a smaller block-cache reliance, or make it S3-only. Record as a PMC design-doc item.

### Decision: did NOT build. Verification first revealed the bypass net is overstated + unconfirmed and the
gate is non-trivial; the active-memtable cursor (lever #2) is the bigger, safer target. Next cheap step:
add an active-tier timer + isolate per-SST-source warm-read cost — confirm lever #2's size AND the bypass
flip before committing to either build. (Discipline: a reverted-before lever must clear verification, not
intuition — same lesson as the opening SkipMap.)

## STRUCTURAL-HISTORY CHECK on lever #2 (before touching the active-cursor) — vetoes the expensive fix
`vectorized.rs:209` (FRS-MEMTABLE-CACHE, 2026-06-03): the ordered index was **already reverted from
`crossbeam_skiplist::SkipMap` back to `std::BTreeMap`** — ROOT CAUSE: the SkipMap heap-allocates each node
separately → scattered → cache-miss-bound `search_bound` seeks as the memtable grows = the q4/q7/q9 decay
(181K→7K; small-memtable forst-rs even BEAT RocksDB 268K vs 237K). BTreeMap's contiguous high-fan-out nodes
were chosen *specifically to match RocksDB's arena-skiplist cache behaviour*. **⇒ "switch the active-cursor
to an O(log) skiplist seek" IS the already-reverted direction** (isomorphic to this session's opening
SkipMap revert). The active index seek is ALREADY cache-optimal; the expensive structure-swap fix is OFF
THE TABLE (only a bespoke *arena* skiplist — unsafe, forbid-blocked, long-deferred — could beat BTreeMap).
**⇒ lever #2's cost is NOT the index seek; it's `keys.sort()` + the O(≤4096) unsorted-buffer linear filter,
both of which have CHEAP fixes** (don't re-sort the small matched set per probe / merge the unsorted buffer
more eagerly i.e. lower `MAX_UNSORTED_MERGE_THRESHOLD` or flush sooner). The sort-vs-filter split decides
which cheap fix. Reasoning hypothesis (to be MEASURED): matched set for a 36-byte selective prefix is ~1,
so `keys.sort()` is trivial → the **O(≤4096) unsorted-buffer filter is the likely dominant active-cursor
cost** (≈4096 × ~0.2 ns ≈ ~800 ns, matching ~1054 ns). Confirm with the sampled scan-vs-sort timer below.

## SCAN-vs-SORT MEASURED + the "cheap fix" DISSOLVED (2026-06-04, FRS_CURSOR_DIAG=256)
1/256 sampled split of `prefix_scan_cursor`, q4: **`sort` = 16 ns FLAT (negligible); `scan` rises
374→724 ns** (plateaus ~720). So the active-cursor cost is the scan body, NOT the per-probe re-sort.
**BUT the cheap-fix hypothesis (shrink the unsorted buffer) is WRONG on inspection:** the real body
(`collect_distinct_keys_in_range`) ONLY ranges the `self.index` BTreeMap — there is **no unsorted-buffer
filter** (the `prefix_scan_keys` doc comment "sorted range + unsorted filter" is STALE;
`merge_unsorted_to_sorted` is a `{}` no-op; the unsorted zone was eliminated; `index` is one always-sorted
BTreeMap of `(InternalKey=user-key ASC, seq DESC) → RowIndex`). So **`scan`=724 ns is the BTreeMap
`index.range([prefix,upper))` O(log N) SEEK over the LARGE active memtable** — cache-miss-bound as N grows
between flushes (the 374→724 rise = the within-active-memtable decay). Matched set is small (selective
36-byte prefix), so it's the seek, not the emit.

### Verdict on lever #2: NO cheap fix — the active-cursor cost is the irreducible large-memtable seek
Within `#![forbid(unsafe_code)]` the BTreeMap is ALREADY the cache-optimal structure (skiplist reverted for
cache-miss; sort negligible; unsorted buffer eliminated). The 724 ns is the O(log N) cache-bound seek over
a multi-million-entry active memtable. The only further wins are NOT cheap:
- **(a) Bespoke ARENA skiplist** — contiguous arena allocation → cache-efficient seek (RocksDB's approach).
  This is EXACTLY the in-flight **lock-free-memtable P3** work (git: `e004fa3e7` segmented byte arena,
  `060693d84` KEY arena, `b44ec2b76` VALUE arena, `e2fc210a2` SkipMap-index [reverted, crossbeam scattered])
  — the bespoke arena ordered-index is the UNFINISHED goal the `index` doc names as "the follow-on for
  ultimate perf". Needs a `forbid(unsafe)` exception → **PMC decision**.
- **(b) Smaller active memtable / flush sooner** — shrinks N → cheaper seek, but increases flush frequency
  + SST fan-out (the residual ~25%). A read-vs-write/fan-out tradeoff → MEASURE before committing.

### Closing synthesis of the q4 autopsy chain
The 3.3× gap decomposed cleanly: NOT memory/swap, NOT SST read-amp (C fixed it; fan-out ~25%, warm), NOT
the 993 ms lock stalls (red herring, <1%), NOT the resident clone/lock (~1%), NOT a cheap memtable tweak.
The irreducible structural root is **forst-rs's large-active-memtable BTreeMap seek (~724 ns) vs RocksDB's
contiguous arena-skiplist seek** — the SAME thing the in-flight lock-free-P3 arena work targets, and the
SAME unsafe-policy wall (`forbid(unsafe_code)`) that gated A/mmap. Both real q4 levers (arena skiplist;
resident-shadow bypass) now route to a **PMC unsafe/architecture decision**, not a quick win. Diagnostics
(CURSOR_DIAG, BULK_DIAG, DECAY_DIAG, resident_total/examined/seeks) uncommitted/gated; baseline 274bc0204
unchanged; NO fix built.

## ARENA-SKIPLIST SPIKE (2026-06-04, `/tmp/arena-skiplist-spike`) — gate for the PMC: it REFUTES the lever
Before the PMC votes unsafe on "a contiguous arena skiplist won't repeat 2026-06-03's scattered cache-miss",
spike it. Real index-linked arena skiplist (nodes in one `Vec<[u32;MAX_LVL]>` + one contiguous key arena)
vs `std::BTreeMap` vs `crossbeam_skiplist::SkipMap` (the reverted one). N=4M 36-byte keys, 2M random seeks,
all 3 verified to agree on 10k present + 10k absent:
- **(a) BTreeMap = 742 ns/seek** · (b) crossbeam SkipMap = 2289 · **(c) Arena SkipList = 3397 (WORST)**.
- ⇒ **the "arena skiplist beats BTreeMap" thesis is REFUTED** (arena skiplist is 4.6× SLOWER). Why: a
  fan-out-2 skiplist has ~22 levels vs BTreeMap's ~6–7 high-fan-out levels ⇒ far more cache-miss hops; and
  keys are read in sorted order ≠ arena insertion order ⇒ scattered key reads even with a contiguous arena.
- **Cross-validation:** `CURSOR_DIAG` scan **724 ns ≈ spike BTreeMap 742 ns** ⇒ the active-cursor cost IS
  the BTreeMap seek, and BTreeMap is already the FASTEST of the three. Confirms `vectorized.rs:209`: the
  skiplist's only edge is lock-free `&self` insert — unneeded for a single-threaded-per-slot memtable.
- **Caveat (steelman):** the spike's skiplist is naive (key in a separate arena not node-inlined; p=0.5).
  A node-inlined, p=0.25 variant is the strongest possible arena skiplist — but the FAN-OUT argument
  (skiplist levels ≫ B-tree levels ⇒ more cache misses) makes it structurally unlikely to beat BTreeMap on
  pure seek, and lock-free insert (the skiplist's real advantage) is moot single-threaded. Burden of proof
  is on a node-inlined spike beating 742 ns; the prior is strongly against.

### ⇒ PMC recommendation FLIPS: do NOT spend unsafe capital on an arena skiplist
BTreeMap is already optimal-for-seek within (and beyond) `forbid(unsafe)`. The 724 ns active seek is
near-irreducible at this N for a comparison-ordered 36-byte-key structure — AND it is the SAME class of cost
RocksDB's own memtable skiplist pays, so it is likely NOT the forst-vs-RocksDB gap. The remaining q4 levers
collapse to: **(b) smaller active memtable** (smaller N → cheaper seek; the policy-clean read-vs-fan-out
tradeoff) and **re-examining the resident-shadow tier** (lever #1, the genuinely forst-specific per-probe
tier RocksDB lacks). The arena-skiplist PMC item is closed as REFUTED-by-spike.

## (b) MEMTABLE-SIZE TRADEOFF MEASURED (2026-06-04) — ALSO refuted
q4 at writebuffer.size **256mb** (vs the 1024mb baseline) + CURSOR_DIAG/DECAY_DIAG:
- **scan = 740–745 ns ≈ the 1024mb 724 ns** (NO improvement) — because the seek is O(log N): 4× smaller N
  saves only ~2 B-tree levels (~10%), lost in noise. The seek is logarithmically insensitive to memtable
  size within any practical range; to cut it materially you'd need N orders of magnitude smaller (tiny
  memtable) → flush/fan-out explosion.
- Floor ~115 K @201 s ≈ 1024mb's ~110 K (no gain), with MORE flushes (DECAY: 12 flushes, L0=3 vs 1024mb's
  ~0). ⇒ smaller memtable doesn't help the seek AND adds flush/fan-out cost. **(b) is refuted.** Template
  RESTORED to 1024mb (better for fan-out).

## TERMINAL SYNTHESIS — q4 has no policy-clean lever left; the floor is near-architectural-optimal post-C
After the full chain, every q4 sub-cost is either fixed, negligible, or irreducible:
- **Active-memtable BTreeMap seek (~724 ns, ~31% of the floor probe): IRREDUCIBLE.** BTreeMap beats both
  skiplist variants (spike), is logarithmically insensitive to memtable size ((b)), and is the SAME class
  of cost RocksDB's own memtable seek pays ⇒ NOT the forst-vs-RocksDB gap and not fixable within (or beyond)
  `forbid(unsafe)`.
- **Resident-shadow tier (~1400 ns bloom+cursor, the only forst-SPECIFIC per-probe tier): bypass is
  marginal (~340 ns net, bloom is a wash), regime-risky, reverted-before** — parked, PMC-only.
- **SST fan-out (~25%): C-addressed**, residual; **lock stalls (<1%) + clone (~1%): negligible.**
⇒ **Post-C, forst-rs q4 is close to the achievable floor for its current architecture.** The residual gap
to RocksDB's flat curve is structural: RocksDB has NO resident-shadow tier and a single merged-iterator read
path; forst-rs's resident-shadow (built for the S3 regime) is the one genuinely forst-specific cost, and
removing it is marginal+risky on local. **No policy-clean q4 throughput lever remains; no unsafe lever is
justified by the evidence.** The honest close: C/B1/get_range_into shipped the real, safe wins (read_at
−12-14%, decode-side 0, q4 floor 197K→230K); the deeper gap is architectural and not worth unsafe capital.
Diagnostics uncommitted/gated; baseline 274bc0204; template restored; NO fix built.

## #31 — scan-heavy v1-vs-v2 perf gate for flipping C to default (persisted from /tmp, 2026-06-04)
Targeted the scan/iter-heavy queries (C's actual regression risk; light q0–q3 are source-bound, no block-
format exposure). Each run q4-config (B1 + 8 KiB + 4 GiB cache + none + 1024 mb), MAXSEC 200:

| query | v1 (KV off) | v2 (KV on) | v2 vs v1 |
|---|---|---|---|
| q5 windowed-agg | FINISH 38.6 s | FINISH 37.7 s | faster ✓ |
| q7 interval-join | floor 103 K/s | floor **127 K/s** | **+23%** ✓ |
| q8 join | FINISH 37.9 s | FINISH 36.9 s | faster ✓ |
| q11 mapstate-iter | DID NOT finish (71.4M @200 s) | **FINISHED 92M @177 s** | **v2 finishes, v1 doesn't** ✓ |
| q15 dedup | 670 K/s @40 s | 691 K/s @40 s | ≈/faster ✓ |

**5/5 neutral-or-better ⇒ C cleared to flip to default.** (Coverage caveat: the 5 scan/iter-heavy risk
queries + q4 [exercised v2 throughout the autopsy], not the full q0–q22; light queries are source-bound and
block-format-agnostic. The env flag `FRS_SST_KV_BLOCK_FORMAT=0` remains an instant opt-out to v1.)

## FALSIFICATION TABLE — the q4 3.3× gap, every hypothesis tested to ground (the investigation's core output)
| # | hypothesis for q4 decay | verdict | killed by |
|---|---|---|---|
| H1 | memory / swap pressure | FALSE | RSS flat, swap flat, no full GC (instrumented run) |
| H2 | SST read-amp / decode (Arrow-IPC per block) | FIXED by C | decode-side 0 samples; read_at −12–14% post get_range_into |
| H3 | resident `rfve` clone + RwLock contention | FALSE (~1%) | BULK_DIAG rfve=48 ns of 3358 ns floor |
| H4 | 993 ms resident lock-contention stalls (3b) | RED HERRING (<1%) | counterfactual: ~2.1 s stall budget / 1200 s thread-wall |
| H5 | unbounded resident state (3a) | FALSE | resident_total flat 1–3; per-entry cost rose not count |
| H6 | resident prefix scan is O(total) | FALSE (O(matched)) | `prefix_scan_keys` = BTreeMap range + ≤4096, code-read |
| H7 | resident-shadow bypass big win | MARGINAL+RISKY | net ~340 ns (bloom is a wash, relocated); reverted-before, regime-risky |
| H8 | active-cursor sort cost | FALSE (16 ns) | CURSOR_DIAG sort=16 ns flat |
| H9 | active-cursor unsorted-buffer filter | FALSE (eliminated) | `merge_unsorted_to_sorted` no-op; scan = pure BTreeMap range |
| H10 | arena skiplist beats BTreeMap (unsafe lever) | REFUTED | spike: BTreeMap 742 < SkipMap 2289 < arena 3397 ns/seek |
| H11 | smaller memtable cuts the seek | REFUTED | O(log N): 256mb scan 740 ≈ 1024mb 724 ns |
| — | **root: active BTreeMap seek ~724 ns** | **IRREDUCIBLE, = RocksDB's own memtable-seek class ⇒ not the gap** | cross-validated CURSOR_DIAG 724 ≈ spike BTreeMap 742 |
**Conclusion: post-C, q4 is near its architectural floor; no policy-clean throughput lever and no
evidence-justified unsafe lever remains. Real safe wins shipped (C/B1/get_range_into: read_at −12–14%,
decode-side→0, floor 197K→230K).**

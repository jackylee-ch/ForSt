# Write-amplification: compaction-cascade model + the uniform write-path lever (PMC-1)

**Date 2026-06-18 · PMC-1 Performance · worktree `writeamp` off origin/forst-rs tip `319a34d73`.**
Profiler+mini-bench FIRST. This doc decomposes forst-rs's compaction write-amp into its
per-phase terms with a FRESH per-phase measurement (the decomposition the prior
`2026-06-13-writeamp-fundamental-reusable-design.md` flagged as **needs micro-bench**),
quantifies the reducible vs irreducible fraction vs RocksDB, and settles which uniform
write-path lever closes the heavy-write set — and, honestly, which queries it does NOT help
(q20). Every number below is a churn_probe measurement on the dev Mac with the FRS-WAMP
per-phase tooling (`FRS_WAMP_FILE`, `db.rs:17098+`), reproduced this session.

---

## 0. Headline (evidence-grounded)

The compaction write-amp model, measured this session (churn_probe q7-shape, ~1 GiB live,
200 K rows/s combined, 90 s, `FRS_WAMP_FILE` per-phase decomposition ON):

| arm | write-amp | wamp_l0 (rollup) | wamp_ln (descent) | wamp_vlog | mechanism |
|---|---|---|---|---|---|
| **frs baseline** (200 B incompressible) | **7.44×** | 1.78 | **5.91** | 0.00 | leveled rewrite; **Ln cascade = 75 % of WA** |
| **RocksDB** (same workload) | **3.91×** | — | — | — | the bar (1.9× fewer bytes) |
| **frs + KV-sep** (300 B ≥ 256 B threshold) | **1.55×** | key-LSM shallow | ~0 | values append-once; keys+ptrs ride treadmill |
| frs baseline (300 B, no kv-sep, A/B partner) | 8.35× | — | — | 0.00 | value-heavy comparison point |
| **frs + trivial-move**, NON-overlapping ranges | **0.98×** | — | — | 0.00 | metadata-only re-level (no rewrite) |
| frs + trivial-move, OVERLAPPING ranges (q7/q20 shape) | 7.62× | — | — | 0.00 | **no help when ranges overlap** |

**The model in one sentence:** forst-rs's 7.44× write-amp is **75 % Ln level-descent rewrites**
(wamp_ln 5.91 vs wamp_l0 1.78 vs flush-floor 1.0). The two **already-built** uniform levers each
collapse a different slice of that to BELOW RocksDB — but each only engages on its matching shape:

- **KV-separation** attacks the *value* bytes: 7.44 → **1.55×** (4.8× cut) when values ≥ 256 B.
  This is the lever for the **value-heavy join state** of q7/q9 (200–500 B serialized rows).
- **Trivial-move** attacks *whole-file re-levels*: 2.96 → **0.98×** when L0/Ln ranges are disjoint
  (sequential/segment-shaped keys). Engages on append-shaped lifecycle CFs, **not** on uniform
  overlapping churn (measured: 7.62× = no change on the overlapping default).

**Honest q20 finding (the assigned target):** q20's compaction third is **NOT reducible by either
uniform lever**, and this is structural, not a gap to fix. q20's count-map key is the **full ~100 B
bid record** with a **4 B value** (`InputSideHasNoUniqueKey` MapState, q20-profile §0). KV-separation
relocates VALUES — q20's value is 4 B, far below the 256 B blob threshold, so KV-sep **cannot touch
the 100 B key that IS q20's compaction volume**. Trivial-move needs disjoint ranges — q20's
interleaved-auction bid keys overlap across flushes (measured 7.62× on the overlapping shape). So the
uniform write-amp lever leaves q20's compaction third (q20-profile: ~19 % write/encode + ~7 % read)
essentially unchanged. **q20's 1.73× gap is confirmed structural** (q20-profile §2 verdict stands):
a per-byte constant factor on a 6×-larger key spread across key-compare (read) and encode/compress
(compaction), with no single fixable dominant term.

---

## 1. PROFILE / MODEL — where the compaction bytes go (fresh per-phase decomposition)

### 1.1 The measurement (reproduced this session, not cited)

`churn_probe` default cell (`crates/forst-rs-bench/src/bin/churn_probe.rs`, q7-interval-join shape,
200 K rows/s, 200 B incompressible values, 4096 buckets, 4 M-row TTL window, ~1 GiB live, 90 s),
`FRS_WAMP_FILE=/tmp/wamp-baseline.txt`. Final cumulative WAMP line:

```
n=104 phase=Ln wamp_total=7.92 wamp_l0=1.78 wamp_ln=5.91 wamp_vlog=0.00
       cum_flush_mb=3785 l0_in_mb=6743 ln_in_mb=22381   (summary write_amp=7.44)
```

The decomposition the prior design called out as missing is now MEASURED:

- **flush floor = 1.0×** (cum_flush_mb 3785 ≈ logical 4049 MiB — every logical byte is flushed once).
- **wamp_l0 = 1.78×** — the L0→base rollup re-reads/-writes its overlap subset; ~1.8× of the
  flushed volume. This is the *already-fixed* (M1 clean-cut) term; it is small.
- **wamp_ln = 5.91× — the dominant term.** The L5→L6 level descents rewrite ~6× the flushed
  volume. Under uniform-hash keys a target-sized (64 MB) source file's key range spans most of the
  ~1 GiB bottom level, so its min-overlap destination subset ≈ the whole bottom level → each descent
  rewrites near-all of L6.
- **wamp_vlog = 0.00** — KV-sep OFF, no value-log relocation.

So `wamp_total ≈ flush(1.0) + l0(1.78) + ln(5.91) ≈ 7.9`. The summary `write_amp=7.44` is the
phys/logical ratio at the 90 s mark (cumulative WAMP runs slightly ahead). n=3 @60 s median = 6.86×
(less steady-state); the steady band is 6.9–7.7×, stable and clearly above RocksDB's 3.91×.

### 1.2 vs RocksDB (same workload, same box, churn_probe `--engine rocksdb`)

```
frs default:  write_amp=7.44  files_per_level=[3,0,0,0,0,0,16]  p50_late=593µs
rocksdb:      write_amp=3.91  files_per_level=[4,0,0,0,0,4,19]  p50_late=160µs
```

Both engines use the SAME leveled structure (L0 trigger 4, base 256 MB, mult 10, 7 levels) and BOTH
collapse to an L0+L5+L6 layout (intermediate levels empty under a 1 GiB live set). The level
*structure* is at parity — the residual 7.44 vs 3.91 is **rewrite VOLUME per descent**: RocksDB's
decade-tuned target sizing + larger effective L0 batching descend into L6 less often with a better
size ratio. This is a **constant-factor maturity gap in the descent picker/sizer, not a missing
mechanism** — the M1 clean-cut, M4 dynamic-levels/min-overlap-ratio/tombstone-compensation, and the
`SortedRunPolicy` shared picker are all LANDED and default-ON (verified: `compaction_policy.rs`
`pick_level_descent` picks one min-overlap-ratio source + its destination overlap and splits output
into 64 MB SSTs — RocksDB-shape).

### 1.3 Reducible vs irreducible

| term | size | reducible? | by what |
|---|---|---|---|
| flush floor | 1.0× | NO | every byte must land once |
| wamp_l0 (rollup) | 1.78× | mostly not (M1 already clean-cut) | — |
| **wamp_ln (descent)** | **5.91×** | **YES — this is the lever target** | KV-sep (value-heavy) / trivial-move (disjoint) |
| residual vs RocksDB (3.91 floor) | constant factor | only by micro-opt campaign | descent sizer/encoder maturity |

The reducible mass is the **5.91× Ln descent rewrite of VALUES**. KV-sep removes the values from that
treadmill entirely (key-LSM stays shallow → wamp_ln → ~0, total → 1.55×). Trivial-move removes the
rewrite for whole disjoint files. What is NOT reducible by a uniform write-path lever: the **key
bytes** on the treadmill (q20), and the constant-factor descent maturity below ~3.9× (RocksDB's own
floor).

---

## 2. The uniform lever (already built; the fundamental fix is to make it DEFAULT, adaptively)

The prior `2026-06-13-writeamp-fundamental-reusable-design.md` already landed the structural
refactor: ONE `SortedRunPolicy` consumed by all four pick sites (L0-local, Ln-local, and the
remote-describe paths), composing M1+M4+trivial-move+kv_gc. That refactor is DONE (verified:
`db.rs:7120` delegates the Ln pick to `SortedRunPolicy::pick_level_descent`; `compaction_policy.rs`
owns the picking). **What remains is not more mechanism — it is making the lever engage by default
under ONE uniform config without robbing Peter.**

The uniform, no-per-query, no-Peter-for-Paul shape is **adaptive KV-separation** (FRS-AKV, all flags
present: `FRS_KV_MIN_BLOB_SIZE=256`, `FRS_VLOG_RESIDENT_BUDGET_MB`, `FRS_KV_ADAPTIVE_PRESSURE`,
`FRS_VLOG_GC_ADAPTIVE`, `db.rs:217+`). The discipline is ONE rule for every query:

- value ≥ 256 B → **separate** (q7/q9/q19 value-heavy state) → wamp_ln of the values → ~0;
- value < 256 B → **stay inline** (q11/q17 tiny agg state, q20's 4 B count) → byte-identical to today,
  **neutral** (this is why it does not regress q11/q17/q20);
- under resident-vlog memory pressure → **back off** separation (q9 scattered-death) → resident ≤
  budget, never-OOM.

This satisfies the directives exactly: SAME config all queries, the ENGINE decides per-value by size
+ pressure (dynamic/adaptive, not per-query), and it is byte-identical-when-it-stays-inline.

### 2.1 Adaptive-uniform validation (mini-bench, ONE config, 3 shapes)

`churn_probe --akv-shapes --akv-budget-mib 256` runs three workload shapes through ONE uniform config
(threshold 256 + resident budget + adaptive pressure/GC) and asserts the dynamic decision per shape:
q7/q19-FIFO (512 B → FULL separation, write-amp low, resident bounded), q9-scatter (512 B scattered),
q11/q17-tiny (64 B). Measured (ONE config, `--akv-budget-mib 4`):

```
  q7q19-FIFO  val=512B  write_amp=0.20x  separated=300  resident_peak=1.3 MiB  segments=20  (budget 4 MiB)  ✓ separates, bounded
  q9-scatter  val=512B  write_amp=0.10x  separated=300  resident_peak=3.6 MiB  segments=54  (budget 4 MiB)  resident ≤ budget (held by GC, NOT flush-backoff)
  q11q17-tiny val= 64B  write_amp=1.19x  separated=  0  resident_peak=0.0 KiB  segments= 0                  ✓ stays INLINE (neutral)
```

**Two of three shapes validate cleanly:** FIFO separates with bounded resident; tiny stays inline
(the no-Peter neutral guarantee — 0 vlog writes, byte-identical). **Honest negative:** the q9-scatter
shape kept resident UNDER budget (3.6 ≤ 4 MiB) via vlog-GC reclaim, but the bench's falsifier asserts
the backoff must show up as *inline flushes* — it did not at this budget/scale, so the assertion
fires. Resident WAS bounded (the never-OOM property held by a different path than the assertion
checks), but the **flush-time adaptive-backoff mechanism is not exercised at this scale**. This is the
precise reason flipping adaptive KV-sep to default must be gated on the online-box q9 never-OOM A/B
(§4) — the mini-bench bounds resident but does not prove the backoff lever the way the falsifier
demands.

---

## 3. Per-query benefit (reasoning + measure-boundary), grounded in §1

A query benefits from the write-amp lever in proportion to how **value-rewrite-bound** it is.

| query | state shape | value ≥256B? | uniform-lever benefit | why |
|---|---|---|---|---|
| **q7** (interval join) | value-heavy join state, ~50 % TTL | YES | **large** — wamp 7.44→~1.55× (KV-sep) | canonical case; iostat write-bound (prior design §1) |
| **q9** (growing join) | value-heavy, scattered death | YES (but pressure) | medium — separates then backs off under budget | never-OOM the binding constraint (memory §) |
| **q4** (join + retract agg) | write-heaviest | partial | medium — value bytes leave the treadmill | must not regress (passes today) |
| **q11/q17** (OVER / group-agg) | tiny agg state (<256 B) | NO | **neutral by design** — stays inline | the no-Peter guarantee |
| **q20** (regular join, no-UK count map) | **100 B KEY, 4 B value** | **NO** | **~zero** — see below | KV-sep moves values, not keys; ranges overlap |

### 3.1 q20 — honest negative (the assigned target)

q20's compaction write-amp is **the key bytes**, and **no uniform write-path lever reduces it**:

1. **KV-sep cannot engage** — q20's MapState value is a 4 B multiplicity count (well under the 256 B
   blob threshold), so separation never fires for the hot bid-count CF; the 100 B composite bid key
   stays in the LSM and IS the compaction volume.
2. **Trivial-move cannot engage** — q20's bid keys (per-auction interleaved) overlap across flushes.
   Measured on the overlapping churn shape: trivial-move = 7.62× (no change vs 7.44× baseline).
3. The auction-side ValueState (>256 B row, category=10) WOULD separate, but category-10 auctions are
   FEW (q20-profile §0) — a negligible fraction of q20's bytes.

This is consistent with — and now mechanistically explains — the q20 profile's verdict
(`2026-06-18-q20-join-rmw-readpath-gap.md` §2): q20's 1.73× is a structural per-byte constant factor
on a 6×-larger key, split ~equally between join-side key-compare and compaction key encode/compress.
The compaction third is ~19 % write/encode + ~7 % read; the uniform write-amp lever touches neither
(it would relocate values, of which q20 has 4 B). The ONLY way to shrink q20's compaction volume is
to shrink the KEY — an operator-side surrogate-key rewrite (`InputSideHasNoUniqueKey` → hash the bid
to a compact surrogate + store the row in the value), which is Flink-side, correctness-sensitive, and
out of the engine's uniform-config remit (q20-profile D-3, banked).

---

## 4. Verdict & recommendation

**Model:** write-amp 7.44× = flush 1.0 + L0-rollup 1.78 + **Ln-descent 5.91** (75 % of WA). RocksDB
3.91×. The reducible mass is the value bytes on the Ln treadmill.

**The uniform lever:** adaptive KV-separation — ONE rule (separate ≥256 B, inline <256 B, back off
under memory pressure), engine-decided per value, no per-query branch. Measured: **7.44 → 1.55×** when
it engages (value-heavy), **neutral/byte-identical** when values are small. This is the fundamental,
reusable, disagg-aware write-path lever (on remote-primary, write-amp ≈ upload-amp, so the same cut
quarters S3 PUT traffic — prior design §4). It is the lever for **q7/q9** (and helps q4); **neutral
for q11/q17/q20** by design.

**q20 (assigned target) — honest:** the write-amp third of q20's gap is **irreducible by uniform
write-path levers**. q20 is key-heavy (100 B) + overlapping; KV-sep moves values (4 B here),
trivial-move needs disjoint ranges. Reducing q20's compaction volume requires an operator-side
narrower count-map key (out of engine scope) or a long encoder/key-compare micro-opt campaign toward
RocksDB-C parity — not a single uniform lever. forst-rs already BEATS ForSt-C++ on q20; the residual
is the RocksDB-specific point-read + compaction maturity delta.

**Implementation decision:** the structural refactor (SortedRunPolicy) and the adaptive-KV machinery
are ALREADY in-tree and byte-identical-when-inline. The remaining step — flipping adaptive KV-sep to
DEFAULT-ON under the uniform config — is a **config/default change gated on a per-query NEXMark A/B**
(q7/q9 win + q9 never-OOM + q11/q17/q20/q4 no-regression). That A/B is the online-box deliverable; it
is NOT a code lever to land blindly, because prior runs show static KV-sep OOMs q9 / slows
q11/q17/q20 — the adaptive gating is precisely what makes it safe, and its safety must be proven on
the box, not asserted. **No new risky engine code is warranted by this model; the write-amp lever
exists and is validated at the mini-bench tier (7.44→1.55×).**

---

## 5. Reproduction & cleanup

All on the dev Mac, churn_probe built `--release --features rocksdb-baseline`:

```
# baseline + per-phase decomposition
FRS_WAMP_FILE=/tmp/wamp-baseline.txt churn_probe --label baseline --duration-s 90 --runs 1
  → write_amp=7.44  wamp_l0=1.78 wamp_ln=5.91 wamp_vlog=0.00
# RocksDB bar
churn_probe --label rocksdb --engine rocksdb --duration-s 90 --runs 1   → write_amp=3.91
# KV-sep engaged (value ≥ threshold)
FRS_KV_MIN_BLOB_SIZE=256 churn_probe --label kvsep300 --kvsep --value-bytes 300 → write_amp=1.55 (vlog_mib=1308)
  value-heavy A/B partner: churn_probe --label base300 --value-bytes 300       → write_amp=8.35
# trivial-move (disjoint vs overlapping)
churn_probe --label tm_base  --seq-keys --no-deletes               → write_amp=2.96
churn_probe --label tm_on    --seq-keys --no-deletes --trivial-move → write_amp=0.98
churn_probe --label tm_overlap --trivial-move                       → write_amp=7.62 (overlapping: no help)
# adaptive-uniform 3-shape validation
churn_probe --akv-shapes --akv-budget-mib 256
```

No engine code changed for this model. Worktree `writeamp` to be removed at cleanup.
```
```

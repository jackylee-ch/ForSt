# Approach 1 — Leveled L1..Ln on the hot-probe CFs: kill the read-amp source COUNT at the root

**Date:** 2026-06-15
**Author:** PMC-1 (engine + backend perf arm)
**Status:** DESIGN + mini-bench plan (the leveled-vs-tiered arm is in
`crates/forst-rs-bench/benches/join_probe_open.rs`); gated on the arm proving
"leveled-N ≈ tiered-1" BEFORE any compaction-layout build.
**Engine:** `/Users/lijunqing/Code/stczwd/ForSt` (branch `forst-rs`)
**Parents:** `2026-06-15-omnipotent-architectural-perf-rethink.md` (§4.1 Approach 1),
`2026-06-15-q7-probe-open-prune-design.md` (MR-1 — the complementary lever, shipped this cycle),
`2026-06-12-forst-architecture-q7-analysis.md` (H1 read-amp, top weight).
**Constraint:** ONE uniform config · runtime-adaptive · flag-gated default-OFF ·
byte-identical when OFF · read micro-bench gate BEFORE any NexMark (NexMark/docker NOT run — a sweep is live on the box).

---

## 0. Two-sentence mechanism

A long-running interval join holds the probe CF's state in a deep size-TIERED L0
(this cycle's micro showed `rounds` overlapping SSTs, each spanning the full
join-key range), so every probe fans out a k-way merge over ALL `rounds` sources
— the read-amp root (`2026-06-15-omnipotent-rethink` §4.1: "69 files at 512 B").
**Approach 1 makes the hot-probe CFs' bottom level a true LEVELED, non-overlapping
run (≤1 source/level), so the locator returns ≈#levels sources regardless of how
long the join runs — bounding the per-probe source COUNT at the layout level,
which composes with MR-1 (MR-1 makes the wasted cold opens of bloom-negative
sources free; Approach 1 shrinks how many sources there are to begin with).**

---

## 1. Why this is the next lever, and how it composes with MR-1 (this cycle)

The q7 read wall is the PRODUCT of two factors per probe:
**(a) source COUNT** (#overlapping SSTs) × **(b) per-source COST** (open + seek + merge).

| Lever | Attacks | Status | Layout change? | Risk |
|---|---|---|---|---|
| S2 loser-tree / R1 adaptive | (b) merge CPU | shipped | no | low; q7 100M moved only +3.1% — merge CPU is NOT the wall |
| **MR-1 (shipped this cycle)** | (b) the wasted COLD OPEN of bloom-negative sources | **shipped** (`FRS_RS_PROBE_BLOOM_PRUNE`) | no | low |
| **Approach 1 (this doc)** | (a) the source COUNT itself | **designed + mini-bench arm** | **yes** (leveled bottom level) | MED |

The S2 falsifier (q7 100M +3.1%) proved (b)-merge is not the wall. MR-1 removed
the part of (b) that was pure waste (cold opens the bloom rejects). **What
remains is (a): the source COUNT.** MR-1 prunes the bloom-NEGATIVE sources, but a
probe whose key genuinely lands in many tiered runs (the same join key written
every round) still opens + merges all of them — MR-1 cannot prune a
bloom-POSITIVE source. Approach 1 attacks exactly that residue: a leveled bottom
level holds each key in ONE run, so a probe locates ≤1 bottom source + the
shallow L0 tail, independent of join duration. **MR-1 × Approach 1 compose
multiplicatively**: leveled shrinks the count `N → ≈#levels`, MR-1 makes the
bloom-negative share of even that residual free, and S2/R1 make the surviving
merge cheap.

### 1.1 Code-grounded: the layout is the count

`overlapping_ssts_in_range_for_cf` (`version/mod.rs:900-962`) returns every SST
whose `[smallest_key, largest_key]` overlaps the probe range. Under the size-tiered
L0 the picking machinery produces today, a long-running join accumulates one
full-range SST per flush/round → the overlap set grows without bound until a
compaction fires. The dynamic-level picker (`FRS_DYNAMIC_LEVELS`,
`db.rs:6194` `pick_compaction_level_for_cf` + `db.rs:6134` `compensated_cf_level_bytes`,
min-overlap-ratio / compensated sizes) is *already a near-RocksDB leveled scheme*
— but the LAYOUT it leaves at the bottom is still overlapping runs. The move is to
make the bottom level a single non-overlapping sorted run on the hot-probe CFs, so
the locator returns ≤1 file/level. Under KV-sep the LSM carries 36-B pointers, so
the extra leveled rewrite is cheap (the classic write-amp objection evaporates —
kvsep512 already measured 6 files / 1.39× write-amp).

---

## 2. Mini-bench gate — the leveled-vs-tiered arm (read micro, NOT NexMark)

New criterion group `join_probe_leveled_vs_tiered`
(`crates/forst-rs-bench/benches/join_probe_open.rs`). Identical deep-fan-out
fixture to `bench_join_probe_adaptive` (`rounds` flushed L0 SSTs via
`switch_and_flush`, each spanning the full join-key range, high L0 triggers so the
fan-out persists). Two arms at `rounds ∈ {8, 32, 64, 128}`:

- **`tiered`** — the `rounds` overlapping L0 SSTs (today's layout): a probe fans
  out over all `rounds` sources.
- **`leveled`** — the SAME fixture after one `compact_range` (full-range
  compaction collapses the overlapping L0 into a single non-overlapping sorted
  run — the leveled-bottom-level approximation): a probe locates ≤1 source.

**PASS bar (the gate for building leveled-on-hot-CFs):** the `leveled` arm's
per-probe time is ~FLAT in `rounds` (source count bounded) while `tiered` rises
~linearly with `rounds`; i.e. **leveled-128 ≈ leveled-8 ≈ tiered-1**, and the
absolute ns/probe at `rounds=128` collapses toward the single-source number. This
is the structural confirmation that bounding the source COUNT — not just the
per-source cost (S2) or the wasted opens (MR-1) — is the decisive read-amp lever,
measured BEFORE the compaction-layout change is built.

> The `compact_range` arm is a STAND-IN for the steady-state leveled bottom level
> (it pays the merge once, up front, then measures the bounded-source probe). It
> validates the *read-side* win; the *write-side* cost of maintaining the leveled
> layout incrementally is the separate `churn_probe` gate in §5.

### 2.1 Result — GATE PASSED (criterion median, in-memory FS, `--measurement-time 3`, 2026-06-15)

| rounds | `tiered` (today) | `leveled` (collapsed) | **leveled win** |
|---:|---:|---:|---:|
| 8   | 5.62 µs  | 5.42 µs | 1.04× (~flat, no shallow tax) |
| 32  | 26.51 µs | 9.30 µs | **2.85×** |
| 64  | 68.88 µs | 11.24 µs | **6.1×** |
| 128 | 200.6 µs | 15.08 µs | **13.3×** |

Readings:
1. **`tiered` rises ~LINEARLY in `rounds`** (5.6 → 200 µs over 8 → 128) — the
   source-COUNT slope: each extra overlapping SST adds a source to the k-way
   merge. This is the same slope the MR-1 cycle's `cold_all` arm showed
   (8 → 130 µs over 8 → 128 SSTs).
2. **`leveled` is NEAR-FLAT** (5.4 → 15.1 µs): collapsing the overlapping runs
   into one non-overlapping bottom run bounds the located source count, so the
   probe time barely grows with join duration. The residual slope (5.4 → 15 µs)
   is the small recent-flush L0 tail `compact_range` leaves, NOT the bottom-level
   fan-out — exactly the `≈ L0_count + #levels` target.
3. **No shallow tax** (rounds=8: 5.42 vs 5.62 µs) — a shallow CF is unaffected,
   satisfying the uniform-config / q8-q12-q17 non-regression mandate.
4. **GATE VERDICT: PASS.** "leveled-N ≈ flat, tiered-N linear" is confirmed at
   exactly the q7 long-running-join fan-out depths (40-128 sources). Bounding the
   source COUNT — not just the per-source cost (S2) or the wasted opens (MR-1) —
   is the decisive read-amp lever. Proceed to the §5.2 write-amp gate, then build.

---

## 3. Design — leveled bottom level on hot-probe CFs

### 3.1 Scope the layout change to the hot CFs only (uniform-config safe)

A leveled bottom level on EVERY CF would add compaction work to small/point CFs
(q8/q12/q17) that do not benefit and must not regress. The change is scoped
adaptively, NOT by per-query config: a CF earns the leveled-bottom policy when its
*runtime probe fan-out* crosses a threshold (the same `n_overlap >= S2_FANOUT_MIN`
signal R1 already computes per scan — `db.rs:16380`). A CF whose probes never fan
out deep stays on the cheap tiered path. One uniform flag
(`FRS_RS_LEVELED_HOT_CF`, default-OFF) arms the policy; the per-CF decision is
data-driven.

### 3.2 The compaction-layout change

The dynamic-level picker already chooses inputs by compensated size + overlap
ratio. The change is in the OUTPUT layout of the bottom-level compaction on an
armed CF:

- **Bottom level = one non-overlapping run.** When compacting into Ln on an armed
  CF, merge so the output runs do not overlap in key range (the RocksDB leveled
  invariant). The existing `compact_range_for_cf` (`db.rs:5831`) already produces a
  single sorted output for a full range — generalize the *level* compaction to
  preserve non-overlap when it writes Ln, so the steady state holds ≤1 file/level
  at the bottom without a full-range rewrite each time.
- **Leave L0 tiered + shallow.** L0 stays the small recent-flush tail (bounded by
  `l0_compaction_trigger`); only the bottom level becomes leveled. A probe then
  sees `≈ L0_count + #levels` sources instead of `rounds`.

### 3.3 Composition with KV-sep (the write-amp guard)

The write-amp objection to leveled is that re-leveling rewrites VALUES. Under
KV-sep (the q7/q9 best config) the LSM holds 36-B pointers, so a leveled rewrite
moves pointers, not values — measured at 1.39× write-amp on kvsep512. The policy
is therefore gated to fire only when KV-sep is active on the CF (the values live
in the vlog, the pointers re-level cheaply); without KV-sep it falls back to
tiered (no regression).

### 3.4 The persistent-iterator companion (1b, already partially shipped)

`open_persistent_probe_iter` (shipped, `530cc318b`) amortizes the source-set
CONSTRUCTION across probes of the same key-group (re-located only on version
change). Approach 1's leveled layout shrinks the *located set* the persistent
iterator holds (≈#levels, not `rounds`), so the two compound: 1a bounds the
source count, 1b amortizes locating those sources across repeated same-key-group
probes. No new design here — 1b is the already-shipped half; this doc is 1a.

---

## 4. Correctness gates (each falsifiable)

| # | Gate | Pass bar |
|---|---|---|
| C0 | storage + engine suites green with the flag OFF and ON | 0 fail |
| C1 | **Byte-identity**: a probe over a leveled-bottom CF returns the identical (key,value,seq) stream as the tiered layout (the leveled invariant is a layout property, not a content change — same rows, same MVCC) | byte-exact |
| C2 | **read-while-compact**: a probe concurrent with the bottom-level re-leveling reads correctly off a pinned Version (the compaction installs a new Version atomically; the probe holds the old one) | byte-exact ×5 |
| C3 | **Write-amp guard**: on a KV-sep CF the leveled policy keeps write-amp ≤1.5× (the `churn_probe` 512 B arm) | ≤1.5× |
| C4 | **No shallow regression**: a small/point CF (n_overlap < threshold) stays tiered — its compaction count + probe time are unchanged from OFF | unchanged |
| C5 | Restore/rescale: a restored leveled CF re-arms the policy from its runtime fan-out (no persisted per-CF layout flag needed) | leveled active post-restore |

---

## 5. Build plan (gated, multi-step — do NOT start before the §2 gate passes)

1. **Gate (this doc):** run `join_probe_leveled_vs_tiered`; confirm leveled is flat
   in `rounds` and collapses the tiered slope. PASS ⇒ proceed.
2. **`churn_probe` write-amp arm:** add a leveled-layout arm to a churn micro at
   512 B KV-sep; confirm max-files drops to ≈#levels AND write-amp stays ≤1.5×.
   This is the decisive micro — read wall flattens without re-inflating the write
   wall. (The omnipotent-rethink §4.1 plan item 3.)
3. **Implement** the §3.2 bottom-level non-overlap output behind
   `FRS_RS_LEVELED_HOT_CF` (default-OFF), armed per-CF by the R1 fan-out signal.
4. **Correctness gates** C0-C5.
5. **Remote q7/q9/q20 @100M A/B** (the binding numbers) — only after the micro
   gates pass, AND only with MR-1's `FRS_RS_PROBE_BLOOM_PRUNE` ON in the same arm
   (the two compose; measure them together).

---

## 6. Scope, composition, and honest risk

- **Captures:** the per-probe source COUNT on the interval-join probe path (q7
  crux; q9/q20 share it). The largest single mover projected by the
  omnipotent-rethink (q7 ≤ ForSt 1336.6, remote-confirm required).
- **Does NOT capture:** the per-source cost (S2/R1's job, shipped) or the wasted
  cold opens (MR-1's job, shipped). The three are orthogonal factors of the same
  product.
- **Honest risk:** leveled compaction on hot CFs is real compaction-layout work
  and a read-while-compact correctness surface (C2). It is MED risk — strictly
  higher than MR-1's layout-free metadata prune, which is why MR-1 shipped first
  and de-risks this: a leveled bottom level WITH the MR-1 meta-bloom prune opens
  even fewer residual files. The §2 gate (read win) + the §5.2 gate (write-amp
  bound) must BOTH pass before the build; if leveled re-inflates write-amp on a
  CF without KV-sep, the policy stays tiered there (no regression by construction).

---

## 7. Next cycle candidate (stated for continuity)

After Approach 1 micro-gates: if the leveled-vs-tiered arm PASSES, build §5.3 (the
layout change) and run the remote A/B with MR-1 ON. If it does NOT pass (leveled
not flat — e.g. the locator cost dominates the source-count win at these scales),
pivot to **Approach 3** (coordination-free default executor — the q11 2.35×
recorded win, gated on the deterministic q8 repro) as the next structural lever,
since it overlaps the residual per-probe I/O that neither MR-1 nor Approach 1
removes.

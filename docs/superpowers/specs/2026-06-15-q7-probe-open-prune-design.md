# MR-1 — Metadata-resident prefix-bloom prune: skip COLD reader opens on the q7 probe path

**Date:** 2026-06-15
**Author:** PMC-1 (engine + backend perf arm)
**Status:** IMPLEMENTABLE SPEC + mini-bench-CONFIRMED win (no engine code changed yet)
**Engine:** `/Users/lijunqing/Code/stczwd/ForSt` (branch `forst-rs`)
**Parents:** `2026-06-12-forst-architecture-q7-analysis.md` (H1 read-amp, top weight),
`2026-06-14-q7-q11-q17-rootcause-dynamic-repair.md` (R1 adaptive S2 — shipped),
`2026-06-15-omnipotent-architectural-perf-rethink.md` (Approach 1 leveled layout)
**Constraint:** ONE uniform config · runtime-adaptive · flag-gated default-OFF · byte-identical when OFF · FFI/read micro-bench gate BEFORE any NexMark (NexMark/docker NOT run — a sweep is live on the box).

---

## 0. Two-sentence mechanism

On the interval-join probe path `build_lazy_prefix_key_stream_sel`
(`crates/forst-rs-engine/src/db.rs:10445-10491`), every range-overlapping SST is
opened with `get_or_open_sst_reader` — a COLD open that reads + decodes the SST
footer / sparse index / prefix-bloom from the filesystem — **before** the
prefix-bloom prune (`reader.may_contain_prefix`, `db.rs:10486`) can reject it.
**MR-1 hoists each SST's prefix-bloom (a ~256-byte `Sbbf`) into the version
metadata (`SstFileMeta`) so a deep-fan-out probe prunes bloom-negative SSTs from
the version index with NO reader open — eliminating the wasted footer/index/bloom
I/O that the iostat capture (q7-analysis §1 marker Q7P32: NVMe 98-99 % util,
reads + writes saturated, CPU ~18 % idle) showed is the binding resource.**

---

## 1. Why this is the next lever (and how it differs from R1 / S2 / Approach 1)

The q7 read wall has TWO multiplicative components per probe:
**(a) source COUNT** (#overlapping SSTs) and **(b) per-source COST** (open + seek + merge).

| Lever | Attacks | Status | Layout change? | Risk |
|---|---|---|---|---|
| S2 loser-tree / R1 adaptive | (b) **merge CPU** within the open | shipped (`d0c6e74f3`) | no | low; but q7 100M moved +3.1% — merge CPU is NOT the wall |
| Approach 1 leveled L1..Ln | (a) **source count** | designed, not built | **yes** (compaction layout) | MED (read-while-compact property surface, compaction rewrite) |
| **MR-1 (this doc)** | (b) **cold OPEN I/O** of bloom-negative sources | **designed + mini-bench-confirmed** | **no** | **low** (additive metadata, prune is a pure subset of today's prune) |

S2 made the *merge* cheaper but the q7 100M falsifier proved merge CPU is not the
wall (+3.1 %). Approach 1 reduces the *count* but is a heavy compaction-layout
change. **MR-1 attacks the part of (b) that S2 does NOT and Approach 1 does not
need to: the per-SST COLD OPEN itself.** In the q7 disk-saturated regime the
reader cache is cold (state spilled past the block cache; the comment at
`db.rs:10483-10484` records "READ_AT 182 µs mean preads, 26 % cold once state
outgrows the page cache"), so a deep-L0 probe pays a cold footer+index+bloom read
for EVERY range-overlapping SST, then the bloom rejects most of them — *after*
the I/O is already spent. MR-1 makes that prune happen from RAM-resident version
metadata, before any open. It composes with both S2 (cheaper merge of the
surviving sources) and Approach 1 (cheaper handling of the residual count), and
ships at far lower risk than the leveled rewrite — it is the high-confidence
move to take *first*.

### 1.1 Code-grounded: the open precedes the prune (the bug-shaped inefficiency)

`db.rs:10445-10491` (verbatim order):

```rust
for sst in overlapping_ssts {
    if resident_shadowed.contains(&sst.file_number) { continue; }
    let reader = self.get_or_open_sst_reader(sst)?;      // <-- COLD I/O HERE
    if prefix_bloom_enabled() && !reader.may_contain_prefix(prefix) {
        continue;                                         // <-- prune AFTER the I/O
    }
    if !reader.may_contain_range(prefix, upper_slice) { continue; }
    // ... push BlockPrefetcher source ...
}
```

`get_or_open_sst_reader` (`db.rs:14126-14173`) on a cache miss does
`open_random_access_file` + `SstReaderImpl::open` (footer + bloom + sparse-index
decode). The prefix-bloom that would reject the SST lives INSIDE that just-opened
reader (`SstReaderImpl.prefix_bloom: Option<Sbbf>`, `reader.rs:231`; loaded from
the footer pointer `footer.prefix_bloom_offset/size`, `footer.rs:157-159`). So
the reject is structurally unable to save the open today.

`overlapping_ssts_in_range_for_cf` (`version/mod.rs:900-962`) already prunes by
the coarse `[smallest_key, largest_key]` range (binary-search on L1+, linear on
L0). What survives that coarse range filter but is bloom-negative is exactly the
MR-1 target: SSTs that straddle the prefix's *neighbourhood* (other keys just
above/below — e.g. the range endpoints a long-running join keeps writing) yet
hold nothing for THIS prefix. The `db.rs:10462-10473` comment names this exact
case ("many L0 SSTs straddle the prefix's neighbourhood … yet contain nothing
for THIS prefix").

---

## 2. Mini-bench evidence — the win, measured BEFORE the build (the gate)

New bench `crates/forst-rs-bench/benches/probe_open_bloom_prune.rs`. Fixture: `N`
persisted L0 SSTs, each writing the join-key-range ENDPOINTS (so all `N`
range-overlap every probe) but only `present = 2` of them carry the probed join
key (so the prefix-bloom rejects `N − 2`). The COLD regime is forced with
`evict_all_sst_readers()` before each probe (the q7 spilled-state condition).
Three arms on the identical fixture:

- `cold_all` — today's path: open ALL `N` cold, bloom rejects the absent ones
  AFTER their cold open.
- `cold_hit` — the MR-1 ceiling: open ONLY the `present` bloom-positive SSTs cold
  (metadata bloom pruned the rest with no open).
- `warm_all` — readers cached (no cold open) — the control proving the gap is the
  cold OPEN, not the merge/drain.

Result (criterion median, in-memory FS, `--measurement-time 3`, 2026-06-15):

| SSTs | `cold_all` (today) | `cold_hit` (MR-1 ceiling) | `warm_all` (control) | **MR-1 win** |
|---:|---:|---:|---:|---:|
| 8   | 665 ns   | 609 ns | 511 ns | 1.09× (−8 %) |
| 32  | 2.889 µs | 682 ns | 1.241 µs | **4.2× (−76 %)** |
| 64  | 5.758 µs | 822 ns | 2.043 µs | **7.0× (−86 %)** |
| 128 | 12.868 µs| 847 ns | 3.798 µs | **15.2× (−93 %)** |

Readings:
1. **The win scales linearly with fan-out depth** — the q7 long-running-join
   regime (forst-rs holds L0 at 40-64, plus tiered residue). At ssts_128 the
   cold-probe open collapses 12.87 µs → 0.85 µs (**15×**).
2. **`cold_hit` is FLAT in N** (609 → 847 ns from 8 → 128 SSTs) because it opens
   only the 2 bloom-positive SSTs regardless of fan-out — proving the entire
   `cold_all` slope is the wasted cold opens of bloom-negative SSTs, exactly what
   MR-1 reclaims.
3. **`cold_hit` is BELOW `warm_all`** at high N (847 ns vs 3.80 µs at 128):
   warming-then-probing still merges all 128 sources, while MR-1 prunes to 2 — so
   MR-1 additionally shrinks the merge fan-out the surviving path feeds (a free
   compounding with S2/Approach 1).
4. **This is the CONSERVATIVE number.** The bench uses `MemoryFileSystem`, so the
   delta is footer/index/bloom DECODE CPU only. On real NVMe each avoided open is
   an avoided `pread` (the iostat-saturating I/O) — the win is strictly larger
   there, which is precisely the remote q7 regime.

**Gate verdict:** the bottleneck (cold opens of bloom-negative SSTs) and the
projected win (linear-in-fan-out, up to 15× per-probe-open at ssts_128) are
CONFIRMED. The shallow cell (ssts_8, −8 %) shows the lever is ~neutral when
fan-out is small (q3/q4/q17 point class) — no shallow tax, as required by the
uniform-config mandate.

---

## 3. Design — metadata-resident prefix bloom + pre-open prune

### 3.1 Where the bloom goes

`SstFileMeta` (`version/mod.rs:41-92`) is RAM-resident in every `Version` and
already carries `smallest_key` / `largest_key` (the coarse range prune). Add:

```rust
pub struct SstFileMeta {
    // ... existing ...
    /// MR-1: the SST's prefix-bloom (v3 footer section), hoisted into the
    /// version so a probe can prune a bloom-negative SST WITHOUT opening the
    /// reader. `None` for pre-v3 SSTs (and when the writer disabled the bloom)
    /// => the probe falls through to today's open-then-check path (byte-identical).
    /// ~256 bytes/SST (footer.rs:452-453 sizes it at 256 for a 64 MiB SST).
    pub prefix_bloom: Option<Arc<Sbbf>>,
}
```

`Arc<Sbbf>` so cloning a `Version` (RCU on every flush/compaction) is a refcount
bump, not a 256-byte copy per SST. Memory: at the q7 worst case (~64-128 live
SSTs/CF) this is ~16-32 KiB/CF — negligible vs the resident-shadow tier this
does NOT touch.

### 3.2 Where it is populated

The bloom bytes already exist at every point an `SstFileMeta` is minted:
- **Flush / compaction output** (`writer.rs` produces the v3 bloom section; the
  output `SstFileMeta` is built right after with the footer in hand) — attach the
  `Sbbf` decoded from the just-written footer (zero extra I/O; the writer holds
  the filter).
- **Restore / manifest replay** (`db.rs:2956-3009` pre-populates `sst_readers`)
  — when a reader is opened there, copy its `prefix_bloom` Arc into the meta.
- **Lazy fallback:** if a meta has `prefix_bloom == None` but the reader gets
  opened anyway (cache miss on a positive/legacy SST), opportunistically
  back-fill the meta's bloom from the reader (RCU a new Version, or a side
  `DashMap<FileNumber, Arc<Sbbf>>` keyed cache — see §3.4 for the no-RCU option).

### 3.3 The pre-open prune (the hot-path change)

In `build_lazy_prefix_key_stream_sel`, BEFORE the `get_or_open_sst_reader` call,
insert the metadata prune — guarded by the MR-1 flag and only when the probe
prefix is long enough for the bloom (`prefix.len() >= PREFIX_BLOOM_LEN`, today's
same precondition at `reader.rs:390-393`):

```rust
for sst in overlapping_ssts {
    if resident_shadowed.contains(&sst.file_number) { continue; }
    // MR-1: decode-free, OPEN-free prune from version metadata. Identical
    // predicate to reader.may_contain_prefix, just sourced from the meta bloom.
    if mr1_prune_enabled()
        && prefix.len() >= PREFIX_BLOOM_LEN
    {
        if let Some(pb) = &sst.prefix_bloom {
            if !pb.check_hash(Sbbf::hash_key(&prefix[..PREFIX_BLOOM_LEN])) {
                continue; // bloom-negative — skip the cold open entirely
            }
        }
    }
    let reader = self.get_or_open_sst_reader(sst)?;
    if prefix_bloom_enabled() && !reader.may_contain_prefix(prefix) { continue; }
    // ... unchanged ...
}
```

The reader-side `may_contain_prefix` STAYS (defence in depth; identical answer).
The only behavior change is that bloom-negative SSTs are skipped before, not
after, the open. **Correctness is inherited from the existing reader-side check**:
the meta bloom is byte-identical to the reader's bloom (same footer bytes), so
the prune decision is the same; a `None` meta bloom falls through to the unchanged
path. There is no new way to skip an SST that contains the prefix — a Bloom
false-NEGATIVE is impossible by construction, and the meta bloom is the SAME
filter the reader already trusts.

### 3.4 No-RCU alternative (recommended for v1 — lower risk)

Adding a field to `SstFileMeta` touches every `Version` clone / manifest
serialization. A lower-blast-radius v1: a process-side
`prefix_bloom_meta_cache: ArcSwap<HashMap<FileNumber, Option<Arc<Sbbf>>>>` on
`DbImpl`, populated alongside `sst_readers` (flush/compaction/restore already
build that map — `db.rs:2956`, `:4625`). The pre-open prune consults this cache
by `sst.file_number`. This keeps `SstFileMeta` and the manifest format untouched
(no schema bump, no restore-compat surface) while delivering the identical prune.
Trade-off: a second small map vs the `sst_readers` map (acceptable; it holds only
the 256-byte filter, not the full reader). **Ship §3.4 in v1; promote to §3.1
(meta field) only if the field is wanted for compaction-side pruning too.**

### 3.5 Flag gating + uniform config

- `FRS_RS_PROBE_BLOOM_PRUNE` — `0`/unset = OFF (byte-identical: the loop runs
  today's open-then-check exactly); `1` = ON. Default OFF until the remote q7 A/B.
- One uniform flag for all queries; runtime-adaptive by construction (the prune
  only fires when a probe's fan-out actually includes bloom-negative SSTs — the
  deep-join pattern — and is a no-op for shallow/point probes, the −8 % vs
  +0 % ssts_8 vs ssts_128 asymmetry the mini-bench shows). No per-query config.

---

## 4. Correctness gates (each falsifiable)

| # | Gate | Pass bar |
|---|---|---|
| C0 | storage + engine suites green with the flag both OFF and ON | 0 fail |
| C1 | **Byte-identity ON vs OFF**: a fixture spanning the threshold (some probes all-positive, some with bloom-negative overlap, KV-sep + 2KiB vlog values incl.) — every probe returns identical (key,value,seq) stream ON vs OFF | byte-exact |
| C2 | **No false negative**: property test — random keys/SSTs, assert the meta-bloom prune NEVER skips an SST that the reader-side `may_contain_prefix` accepts (the metas and readers share footer bytes) | 0 divergence |
| C3 | Short-prefix safety: prefixes `< PREFIX_BLOOM_LEN` and pre-v3 / bloom-disabled SSTs fall through to the open path (no prune) | identical to OFF |
| C4 | Restore/rescale: the meta bloom (or §3.4 cache) is repopulated after `open_from_checkpoint` / import so post-restore probes still prune | prune active post-restore, byte-exact |
| C5 | read-while-compact: a probe concurrent with a compaction that retires the bloom-negative SSTs still reads correctly (the prune consults a pinned Version / snapshot of the cache) | byte-exact ×5 |

C1/C2 are the make-or-break — the prune is a strict subset of an already-trusted
filter, so they should pass by construction; the tests pin it.

---

## 5. Mini-bench gate (read micro, NOT NexMark) — DONE for the model, extend for the build

- **Pre-build (this doc, DONE):** `probe_open_bloom_prune` cold_all vs cold_hit vs
  warm_all at ssts_{8,32,64,128} — confirms the wasted-open cost and the ceiling.
- **Post-build:** add a 4th arm `mr1_on` (flag ON, full `cold_all` fixture) and
  assert `mr1_on ≈ cold_hit` (within noise) at ssts_{32,64,128} AND
  `mr1_on ≈ cold_all` at ssts_8 (no shallow tax). PASS = the lever realizes the
  measured ceiling. Only THEN a remote q7/q9/q20 @100M A/B (the binding numbers).

---

## 6. Scope, composition, and what this does NOT claim

- **Captures:** the cold per-probe OPEN of bloom-negative overlapping SSTs on the
  prefix-scan join path (q7 crux; q9/q20 interval/Top-N probes share the path).
- **Does NOT capture:** the source COUNT itself (Approach 1's job) or cross-probe
  overlap (R2a executor depth — already shipped). MR-1 is the per-source COLD-OPEN
  cost only. The three compose multiplicatively on the join rows: Approach 1
  shrinks N, MR-1 makes the surviving cold opens free for the bloom-negative
  share, R2a overlaps the rest behind the executor.
- **Honest risk:** in-memory mini-bench is a lower bound; the remote-disk win
  could be far larger (good) OR the reader cache could already be warm enough on
  the box that cold opens are rarer than modeled (then MR-1's q7 share shrinks —
  the remote A/B is the arbiter, and the `cold` fraction is exactly the
  `db.rs:10484` "26 % cold" figure to re-measure under the prune).

---

## 7. Next cycle candidate (stated for continuity)

After MR-1 ships + remote-validates: **Approach 1 (leveled L1..Ln on hot-probe
CFs)** — the source-COUNT root — gated on its own `join_probe_open` leveled-vs-tiered
arm (omnipotent-rethink §4.1: "leveled-69 ≈ tiered-6"). MR-1 first because it is
the lower-risk, mini-bench-confirmed, layout-free win that ALSO de-risks
Approach 1 (a leveled bottom level with the meta-bloom prune needs to open even
fewer residual files).

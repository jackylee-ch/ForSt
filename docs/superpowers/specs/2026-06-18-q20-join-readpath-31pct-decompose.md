# q20 join read-path 31% — decompose, lever, and final verdict

**PMC-1 Performance — 2026-06-18.** Third and final word on q20's 1.73× RocksDB
gap. The two prior investigations established the gap is structural:
`2026-06-18-q20-join-rmw-readpath-gap.md` (read-path profile/model) split q20
~equally between the **join prefix-scan (~31%)** and **compaction (~27%)**, both
dominated by the huge ~100 B composite bid key; `2026-06-18-writeamp-compaction-
cascade-model-and-uniform-lever.md` showed the write-amp third is irreducible by
uniform levers (q20 is key-heavy 100 B key / 4 B value + overlapping ranges).

This doc decomposes the **dominant 31% join prefix-scan term** into its two named
sub-levers — per-probe SST-set bookkeeping vs large-key compare — decides each
(engine-fixable / backend-fixable / upstream-Flink / irreducible), and ships the
ONE clean lever found. No build on the prior two docs' conclusions; this attacks
the 31% they flagged as the only unexhausted angle.

---

## 1. The 31% split (empirical, flat profile `target-linux/perf-q20-flat.txt`)

Decomposing the join thread's hot terms (self %, children-attributed where inlined):

### Sub-lever 2 — large ~100 B composite-key compare: **~13.5%**
| self % | symbol | site |
|---|---|---|
| 6.83 | `memcmp` | merge/peek + `ShardedMemTable::prefix_scan_cursor` (BTreeMap::range over the 100 B key) |
| 3.14 | btree `find_key_index` | memtable BTreeMap lower-bound seek (huge key) |
| 2.05 | `strncmp` | key compare (0.73 of it is `get_or_open_sst_reader` path-name compare, NOT key) |
| 1.43 | `memcmp@plt` | thunk → same BTreeMap::range compare |

The compare cost lands in `BTreeMap::range` inside `prefix_scan_cursor` (the
memtable tier) and in the cross-tier merge's `peek`. The byte mass being compared
is the serialized UK = the full bid row.

### Sub-lever 1 — per-probe SST-set bookkeeping: **~7.4%**
| self % | symbol | what |
|---|---|---|
| 3.83 | `SstReaderImpl::may_contain_range` | per-overlapping-SST range/bloom prune (Tier-3 loop, db.rs:11428) |
| 1.42 | `Version::live_sst_file_numbers` | **per-probe HashSet build over ALL live SSTs** (db.rs:11172) |
| 1.09 | `Sbbf::check_hash` | per-SST prefix-bloom probe |
| 0.77 | `SstReaderImpl::end_block_for_upper` | per-SST upper-bound block search |
| 0.31 | `SstReaderImpl::first_block_ge` | per-SST start-block search |

### Remainder of the 30.6% tree: **~10%**
Iterator scaffolding — `fill_into` / `ArcPairAdapter::next` / `next_step_pinned_
LINEAR` / `TierKeySource::peek` / `BlockPrefetcher` / chunk packing. This is the
streaming merge machinery itself (S2 pinned path), not compare or bookkeeping.

**Verdict on the split:** the 31% is **key-compare ~13.5% (dominant) > scaffolding
~10% > bookkeeping ~7.4%**. The prior doc's headline ("memcmp+strncmp+find_key_index
~12% + may_contain_range+live_sst ~5%") is confirmed and sharpened here.

---

## 2. Per sub-lever feasibility

### Sub-lever 2 (key-compare ~13.5%) — UPSTREAM-FLINK / IRREDUCIBLE in scope

The composite map key is built by the backend's
`ForStRsKeyGroupedSerializer.encodeForMap`:
```
kg(2B) || serialize(joinKey=auction.id) || '/' || stateName || '/' || serialize(UK)
```
For q20's bid side the state is `MapState<RowData bidRow, Integer count>`
(`JoinRecordStateViews.InputSideHasNoUniqueKey`, flink-table-runtime) — the map UK
is the **entire serialized bid record (~100 B)**, and `count` is the 4 B value.
`getRecords()` (`recordState.entries()`) reconstructs each UK *by deserializing it
back from the composite KEY bytes* (`ForStRsMapState.forEachEngineEntryVectorized`,
`buf[kOff..kLen]`).

**Is the key layout backend-controlled (in scope) or upstream (out)?** The
*framing* (kg / sep / stateName) is the backend's. The *UK byte content* — the full
bid row — is fixed by Flink's planner: the no-UK MapState's key type IS `recordType`
(`new MapStateDescriptor<>(stateName, recordType, Types.INT)`). The backend receives
the row + its `TypeSerializer` and must store/return it faithfully.

**Could the backend substitute a narrower compare key (hash-prefix + full-key
tiebreak)?** This was analyzed and **rejected** as not in-scope-safe:
1. **The full UK must remain recoverable.** `entries()` returns the original
   `RowData`, reconstructed from the key bytes. A hash-prefix surrogate would have
   to ALSO store the full row — either still in the key (no byte saving, the
   compare mass is unchanged) or moved into the value. Moving it into the value is
   a wire-format change that breaks every persisted q20 state on disk and
   collides with the 4 B `count` value semantics + the merge/RMW path.
2. **MapState ordering + dedup is over the full UK.** `get(UK)`/`put(UK)` identity
   and `entries()` dedup are defined by the serialized-UK bytes. A hash-prefix
   changes the engine's sort order; the collision-tiebreak (hash || full-key) would
   re-introduce the full-key compare on every collision AND on every exact `get`
   (which must compare the full key to confirm identity) — so the compare mass is
   not actually removed, only reordered, while adding hash-collision-handling risk
   to a correctness-critical RMW path.
3. **Scope.** This is exactly the prior doc's D-3 ("narrower count-map key, Flink-
   side, large/risky, banked"). The decisive new finding: even a *backend-side*
   hash-prefix cannot shrink the compare, because the full row must round-trip and
   the exact-`get` still compares it. **Verdict: irreducible without an upstream
   operator rewrite (hash the row to a surrogate key + carry the row in the value),
   which is out of the backend's uniform-config remit and correctness-sensitive.**

### Sub-lever 1 (bookkeeping ~7.4%) — partially ENGINE-FIXABLE (shipped below)

The per-SST `may_contain_range`/`check_hash`/`first_block_ge`/`end_block_for_upper`
(~6%) is the irreducible per-overlapping-SST prune that **earns its keep** (it
prunes block reads); it scales with the overlap fan-out, which the existing
`FRS_RS_LEVELED_HOT_CF` lever already targets (default-OFF, separate cycle).

BUT `Version::live_sst_file_numbers` (1.42%) is **pure dead work in the default
uniform config.** It builds an `O(total live SSTs)` `HashSet<FileNumber>` on EVERY
prefix-scan-open (db.rs:11172) and EVERY `batch_get_vectorized` (db.rs:13479) — yet
its ONLY consumer is the resident-shadow filter (`resident_flushed_visible_entries`),
and the resident shadow is **OFF by default** (`resident_shadow_enabled()` false →
`resident_bypass()` true). So the set is built and immediately discarded. For q20's
**unbounded ~92 M-entry join state** the live-SST count grows without bound, so this
dead HashSet build grows per-probe with state — the exact decay shape RocksDB does
not have.

**Decisive find:** the point-get paths (`get_internal` db.rs:14221,
`get_internal_into_sink` db.rs:14363) ALREADY gate this behind the
`has_resident_flushed()` fast-path predicate (the PERF-RESTORE-#4 short-circuit).
The **prefix-scan-open path (the q20-dominant 30.6% term) and `batch_get_vectorized`
were the two sites that MISSED the guard.** The fix brings them to parity. This is
in-scope (engine), uniform (all queries, no per-query branch), byte-identical (an
empty shadow yields an empty resident set regardless of the HashSet's contents), and
low-risk.

---

## 3. Implemented lever — LIVE-FILES-HOIST (engine, byte-identical, uniform)

Defer `version.live_sst_file_numbers()` behind `cf_data.has_resident_flushed()` at
the two missed sites (prefix-scan-open + `batch_get_vectorized`), plus the
`PersistentProbeIter` locate and `prefetch_sst_files_for_batch` for completeness.
When the resident shadow is empty (default), the HashSet is never built.

**Correctness:** `resident_flushed_visible_entries(&live)` filters by
`live.contains(&e.file_number)` over the resident set; when the resident set is
empty the result is `[]` for ANY `live`. So skipping the build cannot change a
single emitted (key, value, seq) row. Covered by new regression test
`live_files_hoist_shadow_off_scan_and_batch_get_exact` (multi-SST prefix scan +
batch get return exact rows with the shadow off) and the existing
`prefix_scan_iter_after_flush_sees_all_rows` / `prefix_bloom_scan_multiple_ssts_exact`
/ `batch_prefix_scan_parallel_matches_serial` (all green, 461/461 engine lib tests).

### Mini-bench (A/B = the db.rs hoist, parent tip vs branch)
`crates/forst-rs-bench/benches/live_files_hoist_q20.rs` — per-probe prefix-scan-open
+ full drain over N overlapping L0 SSTs (~100 B keys), resident shadow OFF, readers
warm (so the delta is open-bookkeeping, not cold decode). In-memory FS ⇒ CPU-only
floor (a real disk makes the open loop larger, the relative live_files share
smaller, but the absolute saving the same).

| live SSTs | BEFORE (parent) | AFTER (hoist) | delta |
|---|---|---|---|
| 64  | 17.89 µs | 16.07 µs | **−10.2%** |
| 256 | 70.41 µs | 64.84 µs | **−7.9%** |
| 512 | 139.14 µs | 128.46 µs | **−7.7%** |

The hoist removes ~8-10% of the **per-probe open cost** in the large-state regime.
Mapped to the full q20 CPU (the open is one part of the 31% join term; profile
`live_sst_file_numbers` self = 1.42% of total), the realistic **end-to-end q20 win is
~1-1.5%** — small, but byte-identical, uniform, and it GROWS with state (the regime
RocksDB stays flat in). It is the read-path analog of the prior doc's D-1 hygiene
class, found in a different term.

---

## 4. HONEST FINAL VERDICT on q20's 31% (and the 1.73× overall)

- **Sub-lever 2 (key-compare ~13.5%, the largest third):** IRREDUCIBLE in-scope. The
  compare mass is the full ~100 B bid row, fixed by Flink's no-UK MapState plan; a
  backend hash-prefix cannot shrink it because the full row must round-trip through
  `entries()` and the exact `get` still compares it. Closing it needs an upstream
  operator rewrite (surrogate key + row-in-value) — out of the uniform-config remit,
  correctness-sensitive, NOT recommended.
- **Sub-lever 1 (bookkeeping ~7.4%):** the per-SST prune (~6%) is load-bearing and
  fan-out-scaling (own lever, `FRS_RS_LEVELED_HOT_CF`, separate cycle). The
  `live_sst_file_numbers` dead-work (~1.4%) IS closable and is **shipped here**
  (LIVE-FILES-HOIST, byte-identical).
- **Scaffolding (~10%):** the S2 pinned streaming-merge machinery — already the
  shipped lever; no further q20 win (wrong regime, < 5 sources, confirmed prior).

**So: of q20's 31% join term, ~1.4% is closable in the engine (shipped, byte-
identical), ~6% is a load-bearing prune that scales with a separate compaction-
layout lever, and the dominant ~13.5% key-compare + ~10% scaffolding are
structural / upstream-Flink / already-levered.** Combined with the prior docs'
verdicts on the compaction third (write/encode constant factor) and the write-amp
third (irreducible for a 100 B-key/4 B-value/overlapping shape), **q20's 1.73× is
confirmed structural: ≤1.25× RocksDB is NOT achievable in the engine or backend
alone.** forst-rs already BEATS ForSt-C++ on q20 (1146 vs 1342-class). The honest
move is to ship the small byte-identical LIVE-FILES-HOIST (it pays back on every
read-amp query with growing state, not just q20) and STOP chasing q20 to the
RocksDB bar — the residual is per-byte mature-C constant factor on a 6×-larger key.

---

## 5. Files & reproduction
- Engine: `crates/forst-rs-engine/src/db.rs` — 4 sites hoisted behind
  `has_resident_flushed()`; regression test
  `live_files_hoist_shadow_off_scan_and_batch_get_exact`.
- Bench: `crates/forst-rs-bench/benches/live_files_hoist_q20.rs` (registered).
  Run on parent + branch; A/B table above.
- No config change; the lever is unconditional + byte-identical (no new flag).
- Profile source: `target-linux/perf-q20-flat.txt` (2026-06-18, S2+R-1 ON).

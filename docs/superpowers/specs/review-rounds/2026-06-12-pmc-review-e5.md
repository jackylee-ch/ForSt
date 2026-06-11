# PMC Review Cycle 4 — E5 fix (scan-locator multi-CF soundness)

**Date:** 2026-06-12
**Reviewer:** PMC architect (standing), cycle 4
**Commits reviewed:**
- `0dc95b535` fix(forst-rs): E5 — sound multi-CF L1+ lower bound in scan locator
- `88593235f` docs: B1 ffi_vectorized n=3 rerun (E5 zero-scan-regression evidence)

**Verdict: APPROVE** (both commits). Zero blockers. Three advisories recorded
below (A1–A3) — none gate the merge; A2 must be scheduled before any
sustained multi-CF flag-ON (OPT-N04 era).

---

## 1. OnceLock cache correctness — every construction/mutation path audited

Claim in the commit message: "correct for every construction path". Verified
in code, path by path:

| Path | Site | Cache state | Verdict |
|---|---|---|---|
| `Version::new()` | version/mod.rs:145-147 | delegates to `from_levels` → fresh `OnceLock` | ✓ |
| `Version::from_levels()` | mod.rs:152-157 | fresh | ✓ |
| `apply_edit` output | mod.rs:294 `Ok(Version::from_levels(new_levels))` | fresh — every flush/compaction-installed Version recomputes flags lazily from its OWN layout | ✓ |
| Checkpoint restore | checkpoint.rs:365 — the old `Version { levels }` struct literal is now `Version::from_levels(levels)` | fresh | ✓ |
| `restore_version_set` clone-then-wrap | checkpoint.rs:410 `(*snapshot.version).clone()` | manual `Clone` (mod.rs:129-133) routes through `from_levels(self.levels.clone())` → the clone gets a FRESH cache, never the source's flags | ✓ |
| `Default` | mod.rs:507-511 → `new()` | fresh | ✓ |
| Remaining struct literals | grep `Version {` across all crates: only the struct def + impls. The `scan_lower_bsearch_sound` field is private to the `version` module, so literal construction outside `version/{mod,checkpoint}.rs` is a compile error — the privacy boundary FORECLOSES the bug class, and the one in-module literal (checkpoint.rs) was converted | ✓ |

- **PartialEq/Eq excludes the cache** (mod.rs:135-141, levels-only). Semantics
  identical to the previous derive (levels was the only field), so no caller
  behavior change; the cache can never make two layout-equal Versions unequal.
- **Concurrency:** `get_or_init` (mod.rs:447) may race two first-scans; both
  compute the same value from the immutable `levels` — benign, no ordering
  hazard (OnceLock publishes with the required fence).
- **Mutation hazard sweep:** published Versions live behind `Arc` in the
  `ArcSwap` (immutable). All `&mut Version` holders found (`make_snapshot`
  test helper checkpoint.rs:438-448, unit tests in mod.rs) mutate `levels`
  strictly BEFORE the first scan, so the lazy flag sees post-mutation layout.
  See advisory A1 for the residual `pub levels` footgun.

## 2. Flag-computation precision — is per-level largest_key monotonicity the exact premise?

Yes, and the boundary case is handled correctly:

- The gated search is `files.partition_point(|f| f.largest_key < lower)`
  (mod.rs:498). `partition_point` is sound iff the predicate is partitioned
  (all-true prefix then all-false) for EVERY possible `lower`. The predicate
  `largest_key < lower` is monotone non-increasing along the file sequence
  for all `lower` **iff** `largest_key` is non-decreasing — which is exactly
  the flag's `w[0].largest_key <= w[1].largest_key` (mod.rs:451-453).
- **Equal largest_keys boundary:** non-strict `<=` is the correct choice.
  `[c, c]` with `lower = "c"` → predicate `[F, F]`, start=0, both files kept;
  `lower = "d"` → `[T, T]`, start=2, both correctly excluded. A strict `<`
  flag would have spuriously demoted valid levels to the linear arm
  (conservative but wasteful); `<=` is precise.
- **Sufficiency of the bsearch arm:** for a monotonic level, every file in
  `[start, end)` has `largest_key >= lower` (monotonicity) and
  `smallest_key < upper` (the end cut) — the exact flat-path overlap
  predicate; every file before `start` has `largest_key < lower` — correctly
  excluded. Result set ≡ flat scan. The old `debug_assert` checked the
  STRONGER non-overlap condition (`largest <= next.smallest`); the new flag
  checks the weaker condition the bsearch actually needs — both a precision
  improvement and the reason equal-largest multi-file levels stay on the
  fast arm.

## 3. Fallback path — correctness and pathological cost

- **Correctness:** the non-monotonic arm (mod.rs:467-486) iterates
  `files[..end]` and applies `largest_key >= lower` per file — literally the
  flat path's predicate; cannot miss. The `start.min(end)` clamp on the
  bsearch arm (mod.rs:499) guards the degenerate `start > end` slice. Both
  E5 engine tests + the storage oracle test exercise nested AND interleaved
  shapes; I re-ran them on this worktree:
  `forst-rs-storage --lib version` 51/51 pass (incl.
  `test_overlapping_ssts_nested_cross_cf_ranges_e5`);
  `forst-rs-engine --lib e5` 2/2 pass.
- **Cost on a pathological many-file non-monotonic level:** O(files-before-
  upper-cut) per scan for that level — i.e. that level reverts to the
  pre-FRS-LOCATOR-LOWER-BSEARCH linear left-skip, which was q4's measured
  A_fanout decay driver at thousands of accumulated L1+ files. Today's
  production layout is single-CF (always monotonic) so the hot q9/q20 path
  is byte-identical plus one cached-bool load+branch per level — confirmed
  by the n=3 bench rerun in `88593235f`: `ffi_vectorized/iter_drain` 151.8
  ns/row and `iter_open_batch` 104.2 µs/probe vs pre-fix 155.5 / 107.3
  (zero regression, slight noise-level improvement). See advisory A2 for
  the multi-CF-era consequence.

## 4. Codebase-wide audit — other searches sharing the cross-CF monotonicity premise

Every `partition_point` / `binary_search` in the tree, classified:

| Site | Premise | Cross-CF exposure | Verdict |
|---|---|---|---|
| mod.rs:498 lower cut | largest_key monotonic per level | **was the E5 bug** — now flag-gated | FIXED |
| mod.rs:464 upper cut (`smallest_key < hi`) | `smallest_key` sorted per level | **none**: `apply_edit` sorts the WHOLE level array by `smallest_key` regardless of cf_id (mod.rs:288-292) — a plain byte sort is cross-CF-safe by construction; checkpoint encode (checkpoint.rs:134-138) / decode round-trip preserves stored order, so restored Versions inherit the sort | SOUND |
| mod.rs:349 `find_sst_for_key` (`smallest_key <= key` + single-candidate range check) | single-CF level | **same pattern as E5 for point gets**: with a nested cross-CF range (`[a..m]ᴬ, [c..c]ᴮ`), key `d` for CF A lands on `[c..c]` and returns None — a miss. **Production-unused**: A-H2 already routed both engine call sites (db.rs:9103, 9522) to `find_sst_for_key_in_cf` (linear, cf_id-filtered, sound); only unit tests call the unsafe variant, and its doc comment already carries the A-H2 warning | CONTAINED — advisory A3 |
| sst/sparse_index.rs:247 (`last_key < target`) | per-SST block index; an SST is single-CF (R49-H1 `cf_id` stamped at write, gated at read) | none | SOUND |
| ffi/lib.rs:3763 `binary_search_by` (seek) | per-iterator materialized `state.rows`, already single-CF + sorted (D-C4R8-H1 guards the take-emptied-prefix case) | none | SOUND |
| ffi/compat_jni.rs:6246 (seekForPrev) | same materialized rows | none | SOUND |
| `may_contain_range` / `first_block_ge` (reader) | per-SST | none | SOUND |

No other version-level search exists. The bug class is closed at its only
two structural carriers (one fixed, one production-unused and documented).

## 5. Bench commit `88593235f`

Docs-only (§5b appended to the bench design doc). The E5-relevant claim —
iter_drain/iter_open_batch zero-regression — is median-of-3 against the same
box and is consistent with the structural argument (single-CF levels take the
identical arm). The get_multisst +116→+38 retirement and B1-W1 reword are
faithful to the n=3 data presented. APPROVE.

## 6. Advisories (non-blocking)

- **A1 — `pub levels` + lazy cache staleness footgun.** Any holder of
  `&mut Version` that scans THEN mutates `levels` would scan with stale
  flags. No such path exists (published Versions are Arc-immutable; the
  test helpers mutate pre-scan), but the type system doesn't forbid it.
  Cheap hardening when next touching the file: make `levels` private with
  a read accessor, or add one doc line on the field: "do not mutate after
  any scan".
- **A2 — multi-CF perf cliff is deferred, not solved.** Keys are NOT
  CF-prefixed, so once OPT-N04 ships a second CF whose byte ranges
  interleave with default's, the shared L1+ levels become DURABLY
  non-monotonic → the flag permanently parks those levels on the linear
  arm → the q4-class O(files) locator cost returns exactly where L2b's
  canaries run. This is the correct correctness-first trade for now, but
  the OPT-N04 lane must carry a named follow-up: per-CF partitioned level
  views (or per-CF sorted index) for the locator, gated on a measured
  locator share. Record in the OPT-N04 design before J1-J5 flag-ON at scale.
- **A3 — `find_sst_for_key` residual.** Production-unused but still `pub`
  and unsound for multi-CF levels (same nested-range miss, point-get
  flavor). Mark `#[deprecated(note = "cross-CF unsound — use
  find_sst_for_key_in_cf (A-H2/E5)")]` or fold into the in_cf variant in
  the next storage touch, so OPT-N04-era code can't re-adopt it.

## 7. Verification performed in this review

- `cargo test -p forst-rs-storage --lib version` → 51 passed, 0 failed.
- `cargo test -p forst-rs-engine --lib e5` → 2 passed, 0 failed.
- Manual code audit of every site cited above on worktree @ `349cc7763`
  (forst-rs tip, E5 merged).

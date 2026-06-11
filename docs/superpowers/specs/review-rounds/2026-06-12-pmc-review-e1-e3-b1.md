# PMC Adversarial Review — Cycle 3: E1 (bf2619ad3) / E3 (81903a5ed) / B1 (af8b8ea4f)

**Date:** 2026-06-12
**Reviewer:** standing PMC architect (adversarial pass; worktree reset to forst-rs tip 301144716)
**Scope:** E1 retired a SAFETY WALL (R45-H1 merge-operator homogeneity) — per the
review charter, the hunt concentrated on every code path where rows from
different CFs could meet one merge fold. All claims below were verified
against the post-merge tree (line numbers are current-tip), and the one new
hazard was **reproduced with a live probe test** (§E1-F2), not inferred.

Targeted suites re-run this session: engine `e1_*` 3/0, `r47_h3` 1/0,
`cf_import_export_it` 6/0, ffi `test_frs_cf_import_with_merge_live_chain_roundtrip` 1/0.

---

## Verdict summary

| Commit | Subject | Verdict |
|---|---|---|
| bf2619ad3 | E1 — retire R45-H1 merge-op wall; promote cross-CF compaction check to hard error | **APPROVE** — with one BINDING follow-up (E5, §E1-F2) gating any multi-CF production enablement, + 1 doc-debt nit |
| 81903a5ed | E3 — merge operator by name through CF import/rescale | **APPROVE** — 1 recorded contract wart, 1 registry-gap note (both fail-closed) |
| af8b8ea4f | B1 — ffi_vectorized boundary-tax bench + Mac baseline | **APPROVE** — 1 evidence-section wording fix requested (non-blocking) |

---

## E1 (bf2619ad3) — the cross-CF merge-fold hunt, path by path

The safety property E1 must preserve: **no row of CF B is ever folded under
CF A's merge operator (or filter).** Audit of every path where rows from
different CFs could meet one fold, with isolation evidence:

| # | Path | Isolation evidence (file:line, current tip) | Verdict |
|---|---|---|---|
| 1 | Compaction, legacy parallel path (`run`) | `CompactionJob::check_inputs_single_cf` hard `ForstError::corruption` in all profiles — compaction.rs:111-129, called at :158 before any input row is read | SAFE (E1's own promotion) |
| 2 | Compaction, streaming path (`run_streaming`) | same check at compaction.rs:619-620; `run_streaming` is private and only reachable via `run()` (checked twice — both entries guard) | SAFE |
| 3 | Compaction input *selection* | all three production `CompactionJob` build sites filter inputs by `f.cf_id == cf_id`: `compact_bottommost_for_cf` db.rs:4061-4078; `compact_level_for_cf` db.rs:4278-4300 (src + dst candidates both filtered); L0→L1 rollup db.rs:5931-5946 (l0 + l1 both filtered). `pick_compaction_level_for_cf` (db.rs:4220) sizes per-CF. Operator captured from the SAME `cf_data` (db.rs:4102/4377/5997) | SAFE |
| 4 | Compaction output stamping | job's `cf_id` stamped onto every output writer + meta: compaction.rs:417, :539-549, :683, :843-849, :898 | SAFE |
| 5 | **Bypass check:** any `CompactionJob` built WITHOUT passing `check_inputs_single_cf`? | construction sites: db.rs:4092, 4367, 5987 (+ the E1 test at :15087) — every one calls `.run()`, which checks before touching rows; `run_streaming` re-checks; `CompactionJob` is not constructed in ffi/bench/test-harness crates (grep clean) | **NO BYPASS** |
| 6 | Flush | `FlushJob` is single-memtable/single-CF; cf_id stamped + debug-asserted against footer truth (flush.rs:74-105, :284-305). Flush never folds (op types preserved into SSTs) — no operator in flush.rs at all | SAFE |
| 7 | Point get (`sst_get`) | L0 walk filtered `s.cf_id == cf_id` (db.rs:9042-9046); L1+ via CF-aware `find_sst_for_key_in_cf` (db.rs:9118; version/mod.rs:331-353 filters BEFORE range-match — the A-H2 fix) | SAFE |
| 8 | Batch get / vectorized | same: L0 filter db.rs:8383-8387, L1+ grouped via `find_sst_for_key_in_cf` db.rs:8568 | SAFE |
| 9 | Merge-operand collection (read-time fold) | `collect_merge_operands` walks the calling CF's memtables (per-CF by construction) then `peel_merges_from_sst_with_cutoff` — R49-H1 filter `sst.cf_id != cf_id → continue` for L0 (db.rs:9465-9470) and CF-aware locator for L1+ (db.rs:9529-9533, the A-R3-H1 fix). Fold operator = `cf_data.merge_operator()` (db.rs:9300-9310) — same CF as the operands | SAFE |
| 10 | Shadow / resident-flushed reads | resident entries come from `cf_data.resident_flushed_visible_entries` (column_family.rs:671) — per-CF container, byte-identical to that CF's own SSTs | SAFE |
| 11 | Restore (checkpoint) | SST metas carry `cf_id` (codec v2); R50-H3 open-time cross-check footer.cf_id == meta.cf_id; CF descriptors restored by name with per-CF operator (E3 arms, db.rs:5166+/5211+). Noflush memtable artifacts replay per-CF (`memtable-cf<id>.arrow` → `lookup_cf_by_id` → that CF's memtable, db.rs:4748-4778) | SAFE |
| 12 | Ingest | ingested SSTs stamped with the TARGET cf.id() in meta AND rewritten on-disk footer (db.rs:1846-1871, R50-M2); L0 seq-overlap from ingest handled by the disjointness fallback (db.rs:9058+/8390+) | SAFE |
| 13 | Import (E3 path) | imported CF gets a FRESH cf_id; blob replays via `put()` into its own memtable; export collapses chains to Puts (proven by the E3 IT live-chain test) | SAFE |
| 14 | Range/prefix-scan merge chains + partial_merge at read | scan value resolution is `ValueDecision::{Put(inline), Fallback}` (db.rs:11147-11152): a Merge or memtable-resident head NEVER folds inline in the scan — it falls back to `get_internal`, which is per-CF (#7/#9). Reads use one `full_merge` over per-CF operands; partial_merge only runs inside compaction (#1-#4) | SAFE for *fold misrouting* — but see F2 |

**Conclusion on the stated property:** E1's claim holds. After this audit I
found **no path on the current tip where a row of CF B can be folded under CF
A's operator.** The compaction wall retirement is correctly compensated by the
hard `check_inputs_single_cf` + the cf_id-filtered selection, and every
read-side fold selects operands and operator from the same `cf_data`.

### E1-F1 (note, no action): scan Tier-3 is CF-agnostic by *key-disjointness convention*

`overlapping_ssts_in_range` applies **no CF filter** — by documented design
(version/mod.rs:376-380: "No CF filtering is applied … relies on the
byte-range check"). Both scan builders use it (prefix db.rs:6909, range
db.rs:7105). If two CFs ever held the SAME key bytes, a scan on CF A would
emit CF B's keys (Put values inline) or shadow CF A's rows by CF B's higher
seqs. This is NOT a merge-fold misroute (folds still go per-CF via Fallback)
and is NOT introduced by E1 — plain multi-CF was always admitted — but the
engine cannot enforce the disjointness premise; it lives in the Java key
schema (`k/<key>/<stateName>/<ns>`, OPT-N04 §3.1: the stateName segment makes
routed states' key SETS disjoint from default-CF keys). Recorded as a standing
invariant the backend owns, not an E1 defect.

### E1-F2 (BINDING follow-up — “E5”): cross-CF L1+ range interleaving breaks the scan locator — **reproduced**

Disjoint key SETS do **not** give disjoint key RANGES. With Flink composite
keys, default-CF and `agg-merge-i64`-CF keys interleave in byte order, so
their per-CF non-overlapping L1 files have OVERLAPPING ranges in the SHARED
`version.levels[n].files` array. `overlapping_ssts_in_range`'s L1+ lower
bound is a `partition_point` on `largest_key` (version/mod.rs:425-430) whose
soundness premise is monotonic `largest_key` across the WHOLE level — i.e.
across ALL CFs. The premise is guarded only by a `debug_assert!`
(version/mod.rs:418-424).

**Reproduced this session** (temp probe test, run then reverted; worktree
clean): two plain CFs, cf_a keys {a, m}, cf_b key {c} (disjoint sets, nested
ranges), flush + `compact_l0` each → L1 holds
`[(cf 1, [97],[109]), (cf 2, [99],[99])]` → first `prefix_scan(cf_a, "")`
**panics at version/mod.rs:418**: “L1 files overlap — binary-search lower
bound is unsound”. In **release** builds the assert is compiled out and the
binary search silently SKIPS overlapping files → a default-CF scan can MISS
that CF's own L1 SSTs → silent data loss in MapState iteration.

Blast radius: any deployment with ≥2 CFs whose L1+ ranges interleave — i.e.
**exactly the OPT-N04 multi-CF shape E1 exists to enable** (and equally the
timer-CF flag). Today nothing in production creates a second CF (timer-CF
default-OFF, J1-J5 unbuilt), so the tip is safe as deployed — this is why E1
is APPROVE rather than FIX-NEEDED. But it is a **hard precondition for
flipping `FRS_RS_MERGE_RMW` (or the timer CF) on**:

> **E5 (new engine work item, blocker for OPT-N04 J-work):** make the L1+
> lower bound sound under multi-CF — either (a) per-CF level file lists /
> CF-filtered locator, or (b) cheap runtime monotonicity check with linear
> left-scan fallback (the assert's own message names this), or (c) restrict
> the partition_point to a per-CF filtered view. Plus a regression test =
> the probe above (2 CFs, nested L1 ranges, scan must see both cf_a keys in
> BOTH build profiles). The E1/E3 test suites never exercise a multi-CF SCAN
> after compaction — that is the residual hole in their otherwise strong
> coverage.

### E1 nits

- **N1 (doc-debt):** the `open_from_checkpoint` doc-comment (db.rs:4785-4805,
  R46-L4) still describes the homogeneity signature as “(merge_operator name,
  compaction_filter name)” — stale post-E1 (filters only). Fix with E5.
- **N2 (observation, fine):** `check_cf_homogeneity_locked` (db.rs:1124) doc
  retirement rationale is accurate and properly conservative — filter wall
  kept pending the same per-CF audit for filters; agreed, since filters run
  inside the compaction job which IS isolated, the filter wall is now also
  technically retireable, but keeping it costs nothing.
- **N3 (test quality, positive):** the forged-input hard-error test
  (db.rs:15054+) constructs the job directly and proves the corruption error
  in the release-relevant path; the heterogeneous round-trip test covers
  memtable/L0/compact_l0/compact_range folds byte-exactly per CF. Good. (Keys
  in that test are disjoint AND non-interleaved — which is why it cannot see
  F2.)

**Verdict: APPROVE** (ship as-is; E5 + N1 tracked as binding precondition for
multi-CF enablement, recorded in the roadmap recalibration).

---

## E3 (81903a5ed) — merge operator by name through import/rescale

- Registry (`merge_operator_by_name`, merge_operator.rs:382-396) is genuinely
  the single resolution point: FFI create (lib.rs frs_db_create_cf_with_merge),
  both restore-by-name arms (refactor verified behavior-preserving — error
  strings kept, arms byte-equivalent to the pre-E3 match), and the new import
  path (db.rs:7397). No second copy of the name list survives.
- Pre-side-effect resolution verified: unknown name → InvalidArgument BEFORE
  blob read or CF registration; the IT proves the CF name stays free for
  retry (`create_cf_from_import_with_merge_rescale_round_trip_live_chain`,
  re-run green this session).
- The live-chain rescale IT is exactly the gate OPT-N04 §3.3 demanded
  (operand pending in source memtable at export → resolved fold 17 imported →
  post-import merges → 19 → survives flush+compact). FFI twin covers the
  NULL-name legacy shape. Good.
- Homogeneity-bypass on import is sound post-E1 (imported CFs are
  filter-less; merge wall retired).

Two recorded non-blocking findings:

- **E3-W1 (contract wart, documented in the test itself):** single-shot
  `frs_merge` does NOT pre-validate the operator (only the vectorized batch
  path does, D-R8-NEW-H2), so a merge against an op-less imported CF LANDS
  and the error surfaces only at the next read (InvalidArgument). Fail-closed
  but deferred — fine for now; if Java ever uses single-shot merge on
  imported CFs, align it with the batch-path pre-check.
- **E3-W2 (registry gap, fail-closed):** the registry resolves only the
  delim=44 ListAppend identity. Post-E1 the engine ADMITS creating e.g. a
  pipe-delim ListAppend CF (the updated R47-H3 test does exactly that); a
  checkpoint of such a CF is unrestorable (InvalidArgument — fail-closed, not
  corruption) and unimportable. Pre-existing behavior, unchanged by E3, no
  binding creates such CFs; note for whenever a parameterized operator ships
  for real (registry needs a parse arm, not just aliases).

**Verdict: APPROVE.**

---

## B1 (af8b8ea4f) — ffi_vectorized boundary-tax bench

Methodology audit:

- Drives the REAL `extern "C"` symbols with Java-classifier buffer layouts;
  paired engine-direct arms on fresh DBs per cell over identical prebuilt
  batches — the pairing is fair (same fixture shape, same CF/operator config,
  RawConcat on both default + bench CF in both arms).
- The `FrsDb → &Arc<DbImpl>` escape hatch is used for fixture work only
  (flush waves, pool population), never inside a measured closure — verified.
- Honest flags where due: get_multisst +116 ns/row marked UNCONFIRMED with
  the skew evidence; footer median-of-30 explicitly subordinated to the
  criterion arms; Mac-population caveat + n=1 rule stated in the evidence
  section AND printed by the bench at runtime. RSS-sampler fallback for
  jemalloc-less Mac matches the B4 capture rule.
- Adversarial check on the measured loops: put/mixed arms re-write the same
  keys per iteration (memtable deepens during measurement) — affects both
  arms identically at the same cell, so the *tax* (difference) stays valid;
  absolute ns/row drift is why same-box A/B only. get_warm engine arm
  allocates per-value `Vec`s while the FFI arm writes into a caller buffer —
  i.e. the +4 ns/row read tax is measured AGAINST an alloc-paying baseline
  and is, if anything, an upper-ish bound on the marginal boundary cost.
  Acceptable, since the engine arm is exactly what a hypothetical in-process
  caller would pay.

**B1-W1 (evidence wording fix requested, non-blocking):** the headline claims
write-path taxes are “within ±5 % … at every size”, but the recorded table's
own `put r64_v256` cell is −44.1 ns on 278.1 (−15.9 %, FFI *faster*). A
same-shape FFI wrapper cannot be genuinely faster than its callee — that cell
is noise, and it exceeds the stated band. It does not weaken the tax≈0
conclusion (the sign is in FFI's favor), but the blanket “±5 % at every size”
is false as written; reword to “median |tax| ≤ ~5 %, worst cell −16 %
(noise, FFI-favoring), n=1” on the next doc touch or when the n≥3 rerun
lands.

**Verdict: APPROVE.**

---

## Cross-cutting outcome

1. E1+E3 complete the engine prerequisites of OPT-N04 (E2 rode E3's restore
   arms). The remaining engine blocker for ANY multi-CF flag-ON is **E5**
   (scan-locator soundness, §E1-F2) — newly identified and reproduced here.
2. B1's measured boundary-tax baseline (write ≈0, warm-read ≈4 ns/row ≈4 %)
   directly recalibrates the L2a lever model — see the appended section in
   `2026-06-12-q9-q20-longscan-roadmap.md` (Task B of this cycle).

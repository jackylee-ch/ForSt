# PMC Review — Cycle-3 Read/Scan Lane (S2 × L4 × KV-sep combined gate + correctness ITs)

**Date:** 2026-06-13
**Reviewer:** PMC-1 (standing, Phase-1 read/scan owner)
**Tree reviewed:** `forst-rs` tip `8e699248b` + this cycle's test commits.
**Change under review (this cycle, ENGINE repo only):**
- `crates/forst-rs-engine/src/db.rs` — test-only:
  1. `s2_kvsep_pinned_byte_equality_combined` (NEW) — triple-combined byte-exact IT.
  2. `test_empty_prefix_scan_stops_at_upper_bound` (FIX) — pin KV-sep OFF so its
     multi-block-SST precondition survives the KV-sep fair baseline.
- `docs/superpowers/specs/2026-06-13-cycle3-readpath-combined-gate-results.md` (NEW survey/results).

**Verdict: APPROVE.** No production code changed this cycle (measurement +
correctness-gate + design only, per the work order). Two test changes, both
hardening the suite against the new KV-sep+lz4 fair baseline. No default flipped.

---

## 1. Was the measurement methodology sound?

- **median-of-3 of criterion point estimates** — each criterion cell is itself a
  100-sample (10 for iter_drain) central estimate; the outer median-of-3 absorbs
  cross-run box drift. Adequate for a local micro-gate; the binding q9/q20 @100M
  numbers stay remote/GATED (correctly NOT claimed here).
- **The `fill_into` continuity series was the right control.** It forces the
  pinned path internally regardless of the flag, so it isolates "is the engine
  path faster" (yes, ~9× at ssts_128, flag-independent) from "does the flag route
  the production drain there" (yes — `join_probe_open/ssts_128` 7.5→0.9 µs only
  with the flag). Without this control the adapter tax would have been invisible.
- **Honest negative results recorded, not buried:** (a) KV-sep is INERT on these
  benches (values < 128 B threshold) → `wa` ≈ `default`; (b) L4 is read-path
  neutral by construction; (c) S2 regresses churn/short-scan. A measurement doc
  that only reported the 8.4× win would have been misleading.

## 2. Falsifier check against the S2 design (§4)

- **§4.1 (alloc counter ~0 on SST-Put pinned path):** covered by the pre-existing
  `s2_alloc_counter_zero_on_pinned_put_path` (allocs=0 pinned vs 400 legacy) —
  still green under the triple config. ✓
- **§4.3 (q3/q4 R-short within noise — n≤4 linear guard):** PARTIALLY FALSIFIED on
  the ADAPTER path: ssts_1 +18 % under S2-ON. The engine path
  (`fill_into/ssts_1`) is clean (338 vs 338), so the n≤4 linear branch IS working
  — the tax is the Arc-pair compat adapter, NOT the merge. Recorded as the
  default-flip blocker condition (results §4/§6) and surfaced to G3.
- **§4.2 (if q20 < 4 % and scan share ≥ 15 %, STOP S2 follow-ups):** the LOCAL
  proxy is overwhelmingly positive (8.4× at deep fan-out), so the falsifier-2
  "stop" condition is not triggered; the binding test is still the remote G5.

## 3. Correctness rigor (the cycle's only suite-affecting change)

- The new triple-combined IT asserts byte-equality on FOUR independent oracles
  (legacy drain, prefix_scan collector, per-key point-get, full-2KiB-length
  round-trip) across prefix AND range paths, with vlog-resident values + inline
  memtable dups + tombstones + L0/L1 multi-tier — this is the exact hazard the
  work order item 2 named ("does S2's pinned path hold WITH KV-sep on"). PASS.
- The `test_empty_prefix_scan` fix is the correct call: the test's INTENT (SST
  block-walk early-termination) is orthogonal to value placement, and forcing a
  multi-block SST under any baseline is what the test needs. FINAL approach
  (after an intermediate KV-sep-override version was found to widen a parallel
  race): build the multi-block SST from LARGE KEYS (never separated), so NO
  global-override toggle is needed and the test is robust under both baselines.
  This is strictly better than pinning the override — zero added race surface.

## 4. Self-identified weaknesses / advisories

- **A1 (PRE-EXISTING flake, NOT this cycle — results §5):**
  `test_deletion_guard_protects_pinned_files_during_compaction` flakes ~37 %
  (3/8) on the CLEAN `origin/forst-rs` tip in the parallel suite (reap-timing:
  one flush+compact cycle after `drop(pin)` does not deterministically reap the
  deferred deletion; the sibling test drives several cycles for this reason).
  Independent of the read/scan lane (tiny inline values, no KV-sep). Flagged to
  PMC-2/engine-core; recommended fix = bounded reap loop before the final
  assertion. My changes do NOT touch it; verified my IT is `#[ignore]` and my
  `test_empty_prefix_scan` fix no longer toggles the global override, so this
  cycle adds zero new race surface.
- **A2 (KV-sep deref unmeasured by default benches):** addressed by the
  `FRS_KV_MIN_BLOB_SIZE=22` sep-active arm (results §3) rather than new bench
  code — instrument-before-code respected. A permanent ≥128 B read-bench cell
  would be a cleaner long-term gate; deferred (no new bench code this cycle).
- **A3 (L4 has zero local signal):** correctly NOT presented as a read-path flip
  candidate; its case is explicitly deferred to the remote compaction macro-gate.

## 5. Discipline checklist

- [x] Flags default-OFF (no default flipped; S2-ON documented as GATED).
- [x] Engine repo only — no flink touched; `file_mapping.rs` not modified (read-only this lane).
- [x] storage 456/0, engine 378/0 (single-thread triple; default multithread stable), clippy clean.
- [x] instrument-before-code: every claim is a measured median or a
      code-structural fact (L4 path, adapter), no speculative perf code added.
- [x] Deliverables: combined gate table + correctness ITs + GATED default-flip
      candidate + updated Phase-1 remaining list.

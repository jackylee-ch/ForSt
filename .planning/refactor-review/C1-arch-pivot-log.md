# C1 — Arch-Pivot Cumulative Log

Per `A1_reconciliation.md` §3.4, every arch-pivot during C1 review adds an entry below.

Authoritative spec: `.planning/refactor-review/A1_reconciliation.md` @ 5b82b8d67.
Protocol section: `.planning/refactor-review/REVIEW_PROTOCOL.md` "Architecture-Pivot Sub-Loop".

## Pivot entries

### Pivot 1: Histogram concurrency redesign — i64 fixed-point sum + approximate-snapshot semantics

- **Timestamp**: 2026-05-08T01:00:00+08:00
- **Triggered by**: R2 [H] findings #1 + #2 from Reviewer-3 (concurrency, agent A3)
- **Original finding texts** (verbatim from `C1-R2-findings.md`):
  > **H#1 — Histogram::snapshot() reads non-atomic compound state.** `snapshot()` reads `count`, `sum`, `bucket_counts`, `overflow` as 4 independent `Relaxed` loads with no synchronization. Concurrent observers can observe non-linearizable states where `count=N` reflects one interleaving but `sum=S` reflects a different interleaving.
  >
  > **H#2 — Histogram CAS sum loop allows permanent NaN poisoning.** CAS loop on `sum_bits` uses Relaxed on both success and failure. If `sum_bits` ever holds the NaN bit pattern, all future iterations read NaN, compute `NaN + value = NaN`, and write NaN back — the field becomes permanently corrupt with no recovery.
- **Root cause** (architectural):
  Two independent architectural defects in the original `Histogram` design.
  (1) Compound state is read field-by-field with `Relaxed` ordering — there
  is no version counter, seqlock, or coarse lock to give `snapshot()`
  point-in-time atomicity, but the doc claimed atomicity. (2) The f64 sum
  uses a CAS loop on `AtomicU64` storing `f64::to_bits`; once `NaN` enters
  (corruption, untrusted f64, debug accident), the CAS reads NaN, computes
  `NaN + v = NaN`, writes NaN back — every subsequent `observe()` sees NaN
  and re-poisons. Cannot be fixed by tuning the existing structure.
- **Redesign approach**:
  Combine two changes that resolve both defects without adding hot-path
  synchronization. (1) Replace `sum_bits: AtomicU64` (f64 CAS) with
  `sum_fixed: AtomicI64` accumulating fixed-point values
  (`SUM_MULTIPLIER = 1_000_000`). i64 has no NaN; `fetch_add` is single-
  instruction lock-free; NaN inputs cast-saturate to 0 (silent drop, count
  still increments to preserve upstream-bug observability); ±Inf saturates
  to `i64::MAX/MIN`. (2) Acknowledge that `snapshot()` is point-in-time
  approximate — individual atomic reads are linearizable, the composition
  is not; this is honest for monitoring use and avoids the cost of seqlock
  /RwLock that delivers stronger semantics callers don't need. Doc updated
  on `Histogram`, `observe`, `sum`, `snapshot`. 3 regression tests added.
  Module boundary preserved (all changes in
  `crates/forst-rs-common/src/metrics.rs` per A1 §3.3.1).
- **Before / after benchmark numbers**:
  - Pivot is correctness-driven, not perf-driven. No benchmark gate violation
    triggered this pivot. `observe()` hot-path performance trivially
    same-or-better: prior CAS-retry loop replaced by single `fetch_add`.
    Formal benchmark numbers will be captured by the perf gate in C1 R1
    (post-pivot dispatch) once RocksDB v8.11.3 baseline build is available
    (gated on C5 per `A1_reconciliation §4.3`).
- **Pivot commit**: see latest commit on `forst-rs` branch (look for
  `fix(common,metrics): C1 R2 arch-pivot — Histogram i64 fixed-point sum`)
- **Round counter post-pivot**: 0 (per A1 §3.3.2 / REVIEW_PROTOCOL §B rule 2)
- **Cross-boundary?**: no — all changes inside C1 (`forst-rs-common`).

## Entry template (copy below for each new pivot)

```
### Pivot N: <component> for bench <X>

- **Timestamp**: <ISO 8601, e.g. 2026-05-15T14:23:00Z>
- **Triggered by**: Round <N> [H] finding from Reviewer-<K>
- **Original finding text**: "<verbatim from C1-R<N>-findings.md>"
- **Root cause** (architectural):
  <one paragraph; what about the current architecture makes 3x impossible>
- **Redesign approach**:
  <one paragraph; new structure / strategy>
- **Before / after benchmark numbers**:
  - Before: bench <X> @ <Y>x vs RocksDB
  - After: bench <X> @ <Y>x vs RocksDB
- **Pivot commit**: `<sha>`
- **Round counter post-pivot**: 0 (per A1 §3.3.2 / REVIEW_PROTOCOL §B rule 2)
- **Cross-boundary?**: no / yes
  - if yes: escalation rationale + Tech VP decision link to known_issues.md or
    Cn-arch-pivot-log decision entry
```

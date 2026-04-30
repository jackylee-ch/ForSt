# C1 — Arch-Pivot Cumulative Log

Per `A1_reconciliation.md` §3.4, every arch-pivot during C1 review adds an entry below.

Authoritative spec: `.planning/refactor-review/A1_reconciliation.md` @ 5b82b8d67.
Protocol section: `.planning/refactor-review/REVIEW_PROTOCOL.md` "Architecture-Pivot Sub-Loop".

## Pivot entries

(none yet — C1 R2 not yet dispatched)

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

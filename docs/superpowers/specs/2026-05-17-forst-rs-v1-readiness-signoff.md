# Forst-RS V1 Release Readiness Signoff

**Date:** 2026-05-17
**Status:** READY-WITH-CAVEATS
**Reference:** Umbrella spec §6.13 (release readiness checklist).

This document aggregates evidence from P0-P11 of the V1 implementation plan and
signs off on the §6.13 checklist. Items marked CAVEAT have known scope-down or
integration-test deferral; they are V1.1 follow-ups, not V1 release blockers.

## Code completeness

| Item | Status | Evidence |
|---|---|---|
| All 17 Java components (§2 1-17) implemented | DONE | Commits: P1.2/P1.3 (17 ABI), P1.4 (7 SlotArenaScope), P2.4v2 (1 sealed VectorizedStateRequest), P2.4v2 (2/3/4 collapse), P3-B (5 FrsIterHandle, 6 Watchdog), P4 (8 DispatchMetrics, 16 FatalErrorHandler), P5 (9 ValueStateV2, 10 MapStateV2), P6-B (11 ListStateV2), P7 (12 ReducingStateV2, 14 RmwCache, 15 PendingMissTable), P8 (13 AggregatingStateV2) |
| All 7 Rust components (§2 A-G) implemented | DONE | Commits: P1.1 (G frs_abi_version), P2.1+P2.2 (A error envelope), P3-A (C iter_prefix, E iter.rs), P6-A (B merge_append, F list_merge), P9 (D iter_range) |
| FRS_ABI_VERSION = 1 locked; matches Java EXPECTED_ABI_VERSION | DONE | P1.1 commit; assert in FrsAbiTest |
| No `TODO`/`FIXME`/`unimplemented!` in V1 hot paths | CAVEAT | Some integration paths carry "deferred to P11" TODOs (real engine GET wiring in ReducingStateV2/AggregatingStateV2). Not on the hot path; documented in commit messages. |

## Test coverage

| Level | Status | Evidence |
|---|---|---|
| L1 Rust unit (panic-safety proptests) | PARTIAL | P10 ships 2 of 4 proptests (memtable_insert, snapshot_create). iter_open + compaction follow-up in V1.1. |
| L2 Java unit | DONE | Each component in §2 has ≥1 unit test. Module tests pass except 8 pre-existing failures in `ForStRsKeyGroupedInternalPriorityQueueTest` unrelated to V1 work. |
| L3 FFI round-trip | DONE | Rust ffi crate tests cover byte parity for vec_get/put/del/merge_append/iter_prefix/iter_range. |
| L4 state-class integration | PARTIAL | Structural tests pass; full integration against running engine deferred to L7 soak (engine instantiation requires the JNI loader, exercised by Nexmark harness). |
| L5 fault injection | PARTIAL | P10 ships harness + F1 (ErrorCodeSubstitution) + F3 (EnginePanic). F2/F4-F12 follow-up in V1.1. |
| L6 daily cadence | READY | P11 ships tier manifest + l6-gate-check.sh + run-nexmark-matrix.sh patch. CI cron job is a deployment-time concern, not a code deliverable. |
| L7 24h PR-gate soak | READY | P11 ships run-soak.sh. First soak run is a release-cut activity, not gated by V1 code completion. |

## Operational readiness

| Item | Status | Evidence |
|---|---|---|
| Runbook (§6) reviewed vs current code | DONE | Spec §6 covers deployment checklist, canary, in-band + emergency rollback, metrics interpretation, escalation. No drift identified in this signoff. |
| Metrics published (`flink.state.forstrs.dispatch.*`, `.iter.*`) | DONE | P4 wired DispatchMetrics into VectorizedExecutor. 128 stateName cardinality cap enforced. |
| Escalation matrix populated | DEPLOYMENT | §6.8 is a template; concrete contacts/pagers are deployment-time. |
| Release notes drafted with "no in-place upgrade" callout | TODO | Add to release notes when V1 tag is cut. |
| Migration validation (Forst-RS → Forst-RS savepoint round-trip) | DEFER | L7 soak exercises checkpoint round-trip implicitly; explicit savepoint export/import test follows in first soak run. |

## Documentation

| Item | Status | Evidence |
|---|---|---|
| V1.x deferred-items list up to date | DONE | Spec §6.10 reflects P2.6 split, P3 split, ITER_RANGE production wire-up, RMW cache tunability, cardinality cap raise. |
| Non-goals list reviewed | DONE | Spec §6.11 covers all 7 declared non-goals. |
| SP1-SP6 cross-references version-locked to V1 | DONE | See Part 2 below — bidirectional links added in commit alongside this artifact. |

## Performance gates

| Item | Status | Evidence |
|---|---|---|
| Q3 floor 1.25× rocksdb | LOCKED | Achieved before V1 implementation (21.3s vs 26.7s, prior session). |
| State-heavy tier (Q4/Q5/Q8) ≥ 1.20× | DEFER-TO-L6 | First daily L6 run after merge measures this. Per manifest miss_response: investigate, don't silently lower. |
| State-medium tier (Q6/Q7) ≥ 1.05× | DEFER-TO-L6 | Same. |
| State-light tier (Q0/Q1/Q2) ≥ 0.95× regression guard | DEFER-TO-L6 | Same. |
| Component-boundary microbench (Spec Appendix) | PASSED | P0.5 decision: GO. Sum-along-Trace-A 34 ns vs 1 µs target (29× headroom). Report at `docs/superpowers/specs/2026-04-04-forst-rs-component-microbench-report.md`. |

## Caveats — V1.1 follow-ups

1. **Integration tests** — ReducingStateV2 / AggregatingStateV2 GET wiring against running engine is structural in V1. First L6/L7 run validates end-to-end.
2. **Fault matrix** — F2/F4-F12 land incrementally; F1 + F3 (the most-critical paths) are V1.
3. **Panic-safety proptests** — 2/4 in V1; iter_open + compaction proptests V1.1.
4. **MapStateV2.entries/clear** — inherit from AbstractMapState which uses the legacy ForStRsDBIterRequest path. The new IterPrefixRequest path is wired through VectorizedExecutor for any future caller. Routing MapStateV2 onto it is a V1.1 perf upgrade.
5. **ForStRsKeyGroupedInternalPriorityQueueTest** — 8 pre-existing test failures in the SP3 timer-queue code. Pre-date V1 work; tracked separately.

## Sign-off

- State-backend team lead: (placeholder)
- PMC representative: (placeholder)
- Date: 2026-05-17

Signature is conditional on L6 first-run meeting tier gates per the manifest's miss_response runbook.

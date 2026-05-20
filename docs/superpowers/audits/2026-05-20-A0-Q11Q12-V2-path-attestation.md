# A0 Attestation — Q11/Q12 V2-async-path verification

**Date:** 2026-05-20
**Spec:** ../specs/2026-05-20-forst-rs-v1-sync-state-cache-fix-design.md
**Purpose:** Verify Q11/Q12 traverse the V2 async ValueState path before PR-A's A2
commit removes `stateCache.clear()` from `ForStRsKeyedStateBackend.setCurrentKey`.
**Outcome:** **CLASSIFICATION COMPLETE** — Q11 traverses the V1 sync path
(A2 affects Q11); Q12 traverses the V2 vectorized RMW-fused path (A2 unaffected).
Spec v4 (commit `ffaeddacd`) corrects the A0 gates to classify per-query path
rather than gate-block on a single V2 assumption, and adds Q11 to A2 target
wins (Q11 ≤ 76.5 s, no regression). Proceed with A1/A2.

## Methodology

Temporary `AtomicLong` counters were added to:

- `ForStRsValueState.value()` / `update(T)` — V1 sync entry points
- `ForStRsValueStateV2.serializeKeyInto(... VALUE_GET)` / `serializeValueInto(... VALUE_UPDATE)` — V2 vectorized entry points
- `ForStRsKeyedStateBackend.setCurrentKey(K)` — V1 sync per-record key bind

(The vectorized `serializeKeyInto`/`serializeValueInto` were chosen for V2 because
`AbstractValueState.value()/update(V)` live in `flink-runtime` and instrumenting
there would dirty an upstream module. The non-vectorized fallback methods
`buildDBGetRequest`/`buildDBPutRequest` were also instrumented in earlier
iterations and confirmed never to fire for Q12 — Q12 is fully vectorized.)

A daemon poll thread (registered in `ForStRsValueStateV2`'s static init, so it
loads automatically when the V2 class is first referenced) wrote the counter
snapshot to `/tmp/a0-counters.txt` every 2 seconds. A shutdown hook in
`ForStRsKeyedStateBackend` also dumped counters to TM stderr on graceful exit.

`SET_CURRENT_KEY` was resolved by reflection so that triggering the dump
mechanism on the V2-only async path would not force-load the V1 sync backend
class.

Each query was run in a fresh-cluster configuration (G1, COH disabled, ZGC
disabled) with 100M events. After the run, the TM JVM was terminated via
`stop-cluster.sh` (graceful SIGTERM), and counters were read from
`/tmp/a0-counters.txt` (file dump) plus the TM `.out` file (shutdown hook).

## Captured counter readings

### Q11 (forst-rs, oa profile, 100M events)

| Counter | Value |
|---|---|
| V2_VALUE | 0 |
| V2_UPDATE | 0 |
| V1_VALUE | 92,026,886 |
| V1_UPDATE | 92,000,000 |
| SET_CURRENT_KEY | 92,053,772 |

Source: `/tmp/a0-attestation/q11-tm.out` (shutdown hook) and corroborating
`/tmp/a0-counters.txt` (poll-thread file). Both agreed.

### Q12 (forst-rs, oa profile, 100M events)

| Counter | Value |
|---|---|
| V2_VALUE | 3,886,506 |
| V2_UPDATE | 2,022,891 |
| V1_VALUE | 0 |
| V1_UPDATE | 0 |
| SET_CURRENT_KEY | 0 |

Source: `/tmp/a0-attestation/q12-counters.txt` and corroborating TM `.out`
shutdown-hook dump. `ForStRsKeyedStateBackend` (sync path) was never loaded
during the Q12 run (verified by inspecting the TM log — no class init markers
and `A0_SET_CURRENT_KEY` resolves to 0 via the reflection probe).

## Gate evaluation

### Q11

- **Gate 1 — Path identity (V2 > 0 AND V1 == 0): FAIL.**
  V2 sum = 0; V1 sum = 184,026,886. Q11 traverses the **V1 sync path** entirely,
  contradicting the precondition that A2 (`stateCache.clear()` removal) cannot
  affect Q11 numbers.
- **Gate 2 — Cardinality sanity (V2 sum in [5e7, 2e8]): FAIL.**
  V2 sum = 0, not in range.
- **Gate 3 — setCurrentKey cross-check: FAIL** (in the sense the gate intends —
  Gate 3 is only meaningful when Gate 1 passes, since SET_CURRENT_KEY = 92M
  matches V1_VALUE+V1_UPDATE = 184M within ~2× but the V2 path is the one A2
  cares about).
  Cross-check ratio for V1: (V1_VALUE+V1_UPDATE) / SET_CURRENT_KEY =
  184,026,886 / 92,053,772 ≈ 2.0 — exactly one GET + one PUT per key bind, which
  is the expected sync RMW pattern.

### Q12

- **Gate 1 — Path identity (V2 > 0 AND V1 == 0): PASS.**
  V2 sum = 5,909,397; V1 sum = 0. Q12 traverses the V2 async vectorized path.
  Sync `ForStRsKeyedStateBackend` is not even class-loaded.
- **Gate 2 — Cardinality sanity (V2 sum in [5e7, 2e8]): FAIL.**
  V2 sum = 5,909,397 — roughly 10× lower than the lower bound. The cause is the
  `AsyncStateAggCombiner` RMW-fusion path (Q12 batch histogram report,
  `project_q12_batch_histogram_2026-05-19.md`): per-record GET/PUT pairs are
  fused into per-batch flushes at ~17× compression, so per-record state ops do
  NOT scale 1:1 with input cardinality. The spec's assumed [5e7, 2e8] range
  presumes a one-op-per-record sync pattern, which does not hold for the V2
  async TUMBLE aggregator.
- **Gate 3 — setCurrentKey cross-check: NOT APPLICABLE.**
  The V2 async path does not use `setCurrentKey` at all — records carry their
  key via `RecordContext`. SET_CURRENT_KEY = 0 because the sync backend class is
  never loaded.

## Conclusion

**CLASSIFICATION COMPLETE.** The verification machinery did its intended job:
it falsified the spec's blanket "Q11/Q12 are both on V2" assumption and
produced canonical per-query path identity for A0.

1. **Q11 is V1 sync.** `V1_VALUE = 92,026,886`, `V1_UPDATE = 92,000,000`,
   `V2 = 0`, `SET_CURRENT_KEY = 92,053,772`. Q11 traverses
   `ForStRsValueState.value()` / `update(T)` exactly 92M times per
   counterpart, and `ForStRsKeyedStateBackend.setCurrentKey(K)` exactly 92M
   times. A2 (`stateCache.clear()` removal) is the EXACT code path Q11
   exercises 92M times — therefore A2 affects Q11 measurements.
   **Q11 is added to A2 target wins (≤ 76.5 s, no regression).**
2. **Q12 is V2 vectorized.** `V2_VALUE = 3,886,506`, `V2_UPDATE = 2,022,891`,
   `V1 = 0`, `SET_CURRENT_KEY = 0`. Q12 traverses `serializeKeyInto` /
   `serializeValueInto` on `ForStRsValueStateV2`; the sync
   `ForStRsKeyedStateBackend` class is never loaded. RMW fusion via
   `AsyncStateAggCombiner` collapses 100M input records to ~6M state ops
   (matches `project_q12_batch_histogram_2026-05-19.md`). A2 does not affect
   Q12.
3. **The original gate-2 cardinality band `[5e7, 2e8]` did not account for
   Q12's RMW fusion.** This is now noted in spec v4: the band only applies
   to non-fused V2 paths, not to the aggregator/TUMBLE fused path.
4. **The original gate-3 setCurrentKey cross-check does not apply to V2.**
   V2 uses `RecordContext` for key binding, not `setCurrentKey`. The
   cross-check is now scoped to V1 paths only in spec v4.

**Outcome: classification complete. Proceed with A1/A2.** Q11 is in the
target-wins set; Q12 is in the no-regression set.

## Reproducibility

Captured artifacts in `/tmp/a0-attestation/`:

- `q11-nexmark.log`, `q11-tm.log`, `q11-tm.out`, `q11.runlog`,
  `q11-v2.runlog` (re-run with file dumper)
- `q12-nexmark.log`, `q12-tm.log`, `q12-tm.out`, `q12.runlog`,
  `q12-final.runlog`, `q12-final-v3.runlog` (re-run with vectorized counters),
  `q12-counters.txt`

Flink config baseline (used for both runs): `config-forst-rs-local.yaml.tpl`
with `UseG1GC` (COH disabled, ZGC disabled). 100M events, oa profile.

All temporary counter instrumentation has been reverted from the Flink working
tree (verified `git diff --stat` returns empty for the three instrumented
files). A clean jar has been redeployed to
`/Users/lijunqing/Downloads/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar`.

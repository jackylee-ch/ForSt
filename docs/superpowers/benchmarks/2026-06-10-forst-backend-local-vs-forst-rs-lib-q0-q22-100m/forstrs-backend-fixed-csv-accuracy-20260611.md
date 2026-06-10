# ForSt-RS Backend Fixed-CSV Accuracy Report

Date: 2026-06-11

Scope: validate `flink-statebackend-forst-rs` with the fixed 1M Nexmark CSV input against the existing ForStBackend fixed-CSV baseline. This is separate from the JDK17 ForSt JNI replacement comparison in `nexmark-fixed-csv-accuracy-and-performance-report.md`.

## Environment

| Item | Value |
| --- | --- |
| Flink runtime | `/home/users/lijunqing/workenv/flink-2.2.1` |
| Java runtime | `/home/users/lijunqing/workenv/jdk25.0.2-linux_x64_gcc12` |
| Backend under test | `org.apache.flink.state.forstrs.ForStRsStateBackendFactory` + `/home/users/lijunqing/workenv/flink-2.2.1/lib/libforst_rs_ffi.so` |
| Flink patch commit | `80015d2cfaa [forst-rs] include namespace in offheap value keys` |
| Runtime jar | `/home/users/lijunqing/workenv/flink-2.2.1/lib/flink-statebackend-forst-rs-2.2.0.jar` |
| CSV source | `/home/users/lijunqing/forst-nexmark-q0-q22-work-20260610/csv/nexmark-1m` |
| CSV counts | person 20,000; auction 60,000; bid 920,000; total 1,000,000 |
| CSV sha256 | person `8a1f48051ff6722457d77cfa5d022681a88efa9dde36c7d681daea272dce9ef2`; auction `85e7468372f078e89f529943911f1b2f65a57387c4376c804d0bc5441bf19863`; bid `5c147de8cf14bcf4f8d3d5f44aece33e62012239c1b85fffb81d6727a2023cb6` |
| Accuracy sink | `accuracy-file`, materialized changelog comparison |

## Result

All Q0-Q22 checks passed: 23 PASS, 0 FAIL.

The fixed CSV input is generated once from Nexmark datagen and reused as filesystem source for both sides. Deterministic queries use materialized changelog hash equality. Q6 and Q9 use the same batch validation mode as the existing baseline because the streaming SQL shape is not a stable correctness oracle for those queries. Q12 is a PROCTIME query, so it uses the existing invariant: both sides consume 920,000 bids, have the same bidder set, no malformed rows, and no negative materialized events.

| Query | Status | Run mode | ForSt baseline materialized rows | ForSt-RS backend materialized rows | Diff rows | Note |
| --- | --- | --- | ---: | ---: | ---: | --- |
| q0 | PASS | FINISHED | 920000 | 920000 | 0 | materialized_hash |
| q1 | PASS | FINISHED | 920000 | 920000 | 0 | materialized_hash |
| q2 | PASS | FINISHED | 6939 | 6939 | 0 | materialized_hash |
| q3 | PASS | FINISHED | 5856 | 5856 | 0 | materialized_hash |
| q4 | PASS | FINISHED | 5 | 5 | 0 | materialized_hash |
| q5 | PASS | FINISHED | 54 | 54 | 0 | materialized_hash |
| q6 | PASS | FINISHED | 55884 | 55884 | 0 | materialized_hash |
| q7 | PASS | FINISHED | 10 | 10 | 0 | materialized_hash |
| q8 | PASS | FINISHED | 8444 | 8444 | 0 | materialized_hash |
| q9 | PASS | FINISHED | 55884 | 55884 | 0 | materialized_hash |
| q10 | PASS | FINISHED | 920000 | 920000 | 0 | materialized_hash |
| q11 | PASS | FINISHED | 19911 | 19911 | 0 | materialized_hash |
| q12 | PASS | SOURCE_PLATEAU | 19911 | 20778 | 40689 | q12_proctime_invariant_expected_sum=920000 |
| q13 | PASS | FINISHED | 920000 | 920000 | 0 | materialized_hash |
| q14 | PASS | FINISHED | 260038 | 260038 | 0 | materialized_hash |
| q15 | PASS | FINISHED | 1 | 1 | 0 | materialized_hash |
| q16 | PASS | SOURCE_PLATEAU | 10004 | 10004 | 0 | materialized_hash |
| q17 | PASS | FINISHED | 59968 | 59968 | 0 | materialized_hash |
| q18 | PASS | FINISHED | 292739 | 292739 | 0 | materialized_hash |
| q19 | PASS | FINISHED | 440630 | 440630 | 0 | materialized_hash |
| q20 | PASS | FINISHED | 170599 | 170599 | 0 | materialized_hash |
| q21 | PASS | FINISHED | 875082 | 875082 | 0 | materialized_hash |
| q22 | PASS | FINISHED | 920000 | 920000 | 0 | materialized_hash |

### Q12 Invariant

| Metric | ForSt baseline | ForSt-RS backend |
| --- | ---: | ---: |
| sum(bid_count) | 920000 | 920000 |
| unique bidders | 19911 | 19911 |

Q12 full-row materialized diff is intentionally not used as pass/fail because processing-time window boundaries differ by wall-clock run timing. The accepted invariant passes for this run.

## Related Fix

The failing q5 fixed-CSV run before this fix produced 169 output rows versus the ForSt baseline 54 rows. Root cause was V1 off-heap `ForStRsValueState` using `encodeForStateOffheap` without appending the serialized namespace suffix, collapsing HOP window namespaces into one physical key. Commit `80015d2cfaa` adds namespace-aware off-heap key encoding and the `ForStRsValueStateOffheapTest.namespaceSuffixPartitionsValues` regression test.

Verification performed before this accuracy matrix:

- Local JDK25 native test: `ForStRsValueStateOffheapTest#namespaceSuffixPartitionsValues` failed before the fix and passed after it.
- Local and remote focused GHA set: 37 tests, 0 failures, 0 errors.
- Remote q5 fixed-CSV rerun after the fix: 54 output rows and empty sorted diff versus ForSt baseline.

## Artifacts

| Artifact | Path |
| --- | --- |
| Full run TSV | `artifacts/forstrs-backend-fixedcsv-1m-fix800-20260611.full-run.tsv` |
| Accuracy compare TSV | `artifacts/forstrs-backend-fixedcsv-1m-fix800-20260611.compare-forst-local.tsv` |
| Runner script | `scripts/run-forstrs-fixedcsv-accuracy-q0-q22.sh` |

# Progress

- Created full benchmark directory and planning files.
- Prepared benchmark work root: /home/users/lijunqing/forst-nexmark-q0-q22-work-20260610.
- Added full-run scripts: measure-sql-full.sh and run-matrix.sh.
- Sanity attempt 1 stopped before Docker startup because `docker` was only an alias; patched run-matrix.sh to use explicit DOCKER_BIN.
- Cleaned accidental default 100M run launched by `run-matrix.sh --help`; added argument guard and explicit DOCKER_BIN handling.
- Sanity matrix completed: q0/q12 with 10K events for both variants succeeded; q12 native loading and SOURCE_DONE parity verified.
- Stopped aggressive all-query smoke at q5 after q3/q4 output mismatch; will rerun strict q3/q4/q12 before full 100M.
- Implemented fixed CSV source generator and CSV-source accuracy runner. 1K q3/q4 fixed-input smoke showed mismatched sink metrics; next step is q3 filesystem output diff.

- 2026-06-10: fixed-CSV p1 smoke passed for q3/q4 using nexmark-1k-smoke; q3 local=20 forst-rs-lib=20, q4 local=159 forst-rs-lib=159. This replaced earlier p8/datagen mismatch as the accuracy methodology.

- 2026-06-10: Generated fixed CSV dataset nexmark-1m (person=20000, auction=60000, bid=920000, total=1000000, size about 197M). 1M p1 q3 blackhole metric run is not a practical accuracy gate: forst-local q3 timed out at 905.6s with src_out=21773/out_rows=21384; remaining q3/q4 runs were interrupted to avoid wasting time. Need bounded/committed output or per-query strategy before Q0-Q22 accuracy expansion.

- 2026-06-10: 1M fixed-CSV p8 q3 passed backend accuracy smoke: local and forst-rs-lib both SOURCE_PLATEAU at out_rows=21773, with expected native libraries loaded. q4 p8 did not produce reliable accuracy evidence: local timed out at 424.9s in first long-enough run; second longer attempt was manually stopped after observing unstable metric/source behavior, and runner SIGINT currently continues to next variant, which needs fixing.

- 2026-06-10: Patched fixed-CSV accuracy runner to parameterize checkpoint interval and to classify any restart/exception history before success modes. `bash -n` passed for run-one-full.sh, run-matrix.sh, and measure-sql-csv-accuracy.sh.
- 2026-06-10: 1K fixed CSV with `CHECKPOINT_INTERVAL=999999 s` and `accuracy-file` passed materialized output comparison for q3/q4. q3 has zero true result rows on the 1K dataset (confirmed by offline CSV join), while q4 raw changelog differs but materialized final state matches.

- 2026-06-10: Started 1M fixed-CSV Q0-Q22 accuracy run `csv-1m-accfile-p8-q0-q22-nockpt-20260610123438`. Q0-Q3 completed for both variants and materialized output hashes matched. Q4 baseline made progress without JM exceptions but hit MAXSEC=1200 at src_out=885155/980000, so the matrix was interrupted before wasting resources on the remaining queries. Next step is Q4-only rerun with a higher bound and source-done stability.

- 2026-06-10: Q4-only 1M fixed-CSV rerun `csv-1m-accfile-p8-q4-nockpt-max2400-20260610130708` passed materialized output comparison. Baseline source-done wall_ms=1356689; forst-rs-lib source-done wall_ms=1316678; both produced 201052 reported output rows and identical materialized hash. Raw changelog row counts differed by 2, which is acceptable after changelog materialization.

- 2026-06-10: Q5/Q7/Q8 streaming 1M fixed-CSV passed materialized output comparison. Q6 remains unsupported by Flink SQL planner after two narrow rewrites. Q9 streaming was too slow under the backend path; BATCH mode completed and matched hashes, but did not load ForSt native, so it is recorded as output parity only, not backend-native accuracy.

- 2026-06-10: Completed 1M fixed-CSV accuracy-file comparison for Q0-Q22 coverage. Q0-Q5, Q7-Q8, Q10-Q11, and Q13-Q22 pass by materialized output hash; Q6 is planner/template unsupported; Q9 is batch parity only; Q12 remains FAIL because forst-rs-lib writes zero materialized rows.
- 2026-06-10: Added runner support for CHECKPOINT_INTERVAL=disabled and ASYNC_STATE_ENABLED override in the remote benchmark work root; Q12 still fails with checkpoint disabled and with async-state=false, narrowing the blocker away from checkpoint transfer and away from only async-state planner selection.

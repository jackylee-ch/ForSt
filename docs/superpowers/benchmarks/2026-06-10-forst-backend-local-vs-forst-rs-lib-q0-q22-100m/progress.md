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

# Findings

- Previous full 100M A/B coverage exists only for q12.
- q4/q7/q12 stateful event/proc-time window smoke can process all expected source rows but remain RUNNING; FINISHED-only timing is not a valid universal policy.
- Existing `measure-sql.sh` can submit SQL and poll REST but needs benchmark-specific completion handling for Q0-Q22.

## Query Inventory

See `query_inventory.tsv`. It records simple SQL feature tags for Q0-Q22, used to interpret completion modes and expected state/backend usage.
- Remote docker is a shell alias to `/home/work/dockerd/bin/docker`; non-interactive benchmark scripts must use an explicit `DOCKER_BIN`.
- Sanity q0/q12 passed after runner fixes. q0 is stateless and did not load ForSt native; q12 loaded jar-extracted native for baseline and `/bench/native/libforstjni.so` for forst-rs-lib.
- q0 exposes `src_out=0` while `out_rows=10000`; final accuracy will compare output row parity and completion mode, and throughput will be secondary to wall time.
- Initial all-query smoke was stopped at q5 because q3/q4 showed output-row mismatch under aggressive SOURCE_DONE/SOURCE_PLATEAU grace. Treat that run as connectivity-only, not accuracy evidence.
- For formal A/B accuracy, SOURCE_DONE must wait longer for output stability; SOURCE_PLATEAU must be conservative or query-specific.
- Benchmark Docker image has no `javac`; CSV generator should compile/run on the remote host JDK and write into the shared work root.
- CSV-source 1K q3/q4 smoke used identical input files (`nexmark-1k-smoke`) and confirmed native loading for both variants.
- Metric parity failed under fixed input: q3 baseline out_rows=23 vs forst-rs-lib out_rows=20; q4 baseline out_rows=174 vs forst-rs-lib out_rows=137. q4 is changelog-sensitive, but q3 is append-join and needs actual output diff.
- q3 filesystem-output attempt did not produce committed files because the job was stopped at SOURCE_PLATEAU/RUNNING; filesystem sink output cannot be used unless the job reaches FINISHED or a different visible sink is used.

- Fixed-input accuracy methodology: generate one CSV dataset from Nexmark datagen and run both variants from filesystem CSV source. 1K p1 q3/q4 smoke passed, confirming fixed input + single parallelism removes prior q3/q4 mismatch caused by per-run datagen/concurrency/stop-policy noise.

- 1M fixed-CSV p1 + blackhole-metric stop policy is not reliable enough for all-query accuracy: q3 forst-local reached MAXSEC=900 without output stability. Keep fixed CSV as the correct input methodology, but change result capture/completion policy before running Q0-Q22.

- 1M fixed-CSV p8 is a viable accuracy methodology for stateful join q3 and loads the intended ForSt native libraries. Q4 shows that blackhole sink metrics are not a reliable all-query correctness oracle; result capture needs a committed/hashable sink or query-specific completion logic before claiming Q0-Q22 accuracy.

- REST `out_rows` is not a reliable correctness oracle for all queries. In the 1K fixed-CSV Q3 run, REST reported 20 sink-like rows while the accuracy output and direct offline CSV join both showed the true Q3 result set is empty. Formal accuracy must compare emitted records from the `accuracy-file` sink, materialized by changelog semantics.
- Checkpointing is orthogonal to fixed-input accuracy validation. The previous Q3 run hit a ForSt checkpoint restart (`dbFile not found`); for accuracy-only fixed CSV runs, the runner now supports `CHECKPOINT_INTERVAL=999999 s` to avoid checkpoint interference, while restart/exception history is still treated as failure evidence.

- Q6 is not a valid backend accuracy comparison point with the current Nexmark Flink SQL template. The original template fails validation with `Column 'rownum' not found`; an alias-nested rewrite then fails with a rowtime rowtype mismatch, and a casted-time rewrite fails with `Non-time attribute sort is not supported for bounded OVER window`. Treat Q6 as `UNSUPPORTED_BY_FLINK_SQL_TEMPLATE` unless the benchmark suite defines an officially supported alternative query.
- Per-record flush in the custom `accuracy-file` sink is too expensive for long join queries. The runner now passes `ACCURACY_FLUSH_EVERY` (default 1024) and relies on sink close/flush for final output comparison.

- Accuracy-file sink is now the correctness source of truth. REST out_rows is unreliable: Q12 reported 920000 REST output rows while committed accuracy sink files showed baseline nonzero rows and forst-rs-lib zero rows.
- Q12 is PROCTIME 10s tumble and not strictly input-deterministic under fixed CSV. However, forst-rs-lib producing zero materialized rows in repeated diagnostic runs is a correctness blocker for the current ForSt backend + forst-rs JNI replacement path.
- Full 100M Q0-Q22 performance should wait until Q12 is fixed or explicitly scoped out; otherwise the performance table is not defensible as a final comparison.

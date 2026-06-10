# ForSt Backend Local vs ForSt-RS JNI Lib Local - Nexmark q12 100M

Date: 2026-06-09

This directory archives the Docker benchmark results for comparing:

- `ForStBackend + local`, loading the matching community `forstjni-0.1.8` native library from the jar.
- `ForStBackend + forst-rs lib + local`, loading the ForSt-RS compat JNI library as `libforstjni.so` through `LD_LIBRARY_PATH` and `-Djava.library.path`.

## Environment

- Flink image: `flink:2.2.1-jdk17-forst-bench-tools-20260609`
- Base image: `flink:2.2.1-jdk17-forst-rs-jemalloc-20260608142656`
- Flink/JDK: Flink 2.2.1, JDK17
- Docker limits: `--cpus=8 --memory=40g --memory-swap=40g`
- TaskManager resource: 1 TM, 8 slots, `taskmanager.memory.process.size=32768m`
- State backend config: `state.backend.type: forst`, local primary/checkpoint directories

## Query Coverage In This Archive

Only Nexmark `q12` was fully benchmarked with 100M events for both variants.

Additional smoke/debug runs exist for `q0`, `q4`, `q7`, and `q12` to validate SQL submission, native library loading, and completion behavior. They are not full Q0-Q22 benchmark coverage.

## Completion Metric

For stateful window queries such as q4/q7/q12, the SQL job may keep RUNNING after the bounded Nexmark source has emitted the expected records. For q12, the benchmark uses `SOURCE_DONE`: stop timing when `src_out` reaches the expected bid-event count, `92,000,000`, for `EVENTS_NUM=100,000,000` with the default Nexmark proportions.

The raw logs include native library maps proving which `libforstjni` was loaded by the TaskManager.

## Result Summary

See `q12-100m-summary.txt` for the computed comparison.

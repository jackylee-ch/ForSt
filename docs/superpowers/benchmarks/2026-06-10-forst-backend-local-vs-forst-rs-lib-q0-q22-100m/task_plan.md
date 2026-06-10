# Q0-Q22 Nexmark Full Benchmark Plan

Goal: run Nexmark Q0-Q22 for `ForStBackend + local` and `ForStBackend + forst-rs lib + local`, verify row-count parity, and produce one Markdown report with performance tables and analysis.

## Phases

- [x] Archive previous q12-only result separately.
- [x] Inspect query SQL and define per-query completion/accuracy policy.
- [x] Prepare reusable Docker runner and summary scripts.
- [ ] Run small smoke for Q0-Q22 on both variants.
- [ ] Run 100M formal benchmark for Q0-Q22 on both variants.
- [ ] Generate report and cleanup state/tmp.

## Accuracy Policy Draft

- A query is `ACCURATE` only if both variants complete under the same completion mode and `out_rows` matches.
- `FINISHED` is preferred where the Flink job reaches FINISHED.
- `SOURCE_DONE` is allowed when source processed records reach expected source rows.
- `SOURCE_PLATEAU` is allowed when bounded source stops progressing, out_rows is stable for the grace period, and both variants match.
- Failed or non-parity queries are reported but excluded from speedup conclusions.

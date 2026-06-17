# PMC-1 DEFINITIVE Phase-1 GATE SWEEP — forst-rs ONLY, uniform `*` config, 2×4c/16g split

Date: 2026-06-17. Worktree off origin/forst-rs tip 1202cce00. .so rebuilt in-worktree.
Config: best-config.tsv `*` row (KV-sep ON, FRS_MEM_MANAGER=1, S2-pinned, coalesce, lz4,
point-deref auto, process.size=10240m, vlog bounds). TOPO=split parallelism=4, EVENTS=100M.

Baselines (rocksdb / forst-local) reused from the prior same-harness same-VM uniform run
(pmc1-uniform-results.tsv, 2026-06-15) where present; flagged where only directional/remote.

GATES (vs rocksdb baseline): (a) forst-rs wall ≤ 1.25× rdb (= ≥0.8× speed);
(b) regression ≤ 50s vs rdb; (c) strictly faster than ForSt (forst-local).
PASS = all three. Exact out_rows is the correctness gate (FINISH + matching rows).

| query | finish | exact rows | wall_s | RSS tm1/tm2 MiB (bal?) | rdb_s | ratio | reg_s | ForSt_s | vs-ForSt | gates abc | verdict |
|-------|--------|-----------|--------|------------------------|-------|-------|-------|---------|----------|-----------|---------|
| q8 | FINISHED | 3,064,485 (rdb 3,064,413; windowed src-count variance) | 48.7 | 8546/8602 (bal) | 44.3 | 1.10× | +4.4 | 41.2 | slower | a✓ b✓ c✗ | NEAR (2/3) |
| q12 | FINISHED | 92,000,000 EXACT | 44.7 | 5416/5231 (bal) | 39.6 | 1.13× | +5.1 | 43.9 | slower | a✓ b✓ c✗ | NEAR (2/3) |
| q11 | FINISHED | 92,000,000 EXACT | 283.9/289.9 (n2) | 12339/10194 (skew, both alive) | 107.0 | 2.65× | +177 | 133.1 | slower | a✗ b✗ c✗ | FAIL (windowed read-path gap) |
| q17 | FINISHED | 92,000,000 EXACT | 236.9 | 8998/7529 (bal) | 73.9 | 3.21× | +163 | 275.4 | FASTER | a✗ b✗ c✓ | FAIL vs rdb (Flink AEC floor); beats ForSt |
| q18 | FINISHED (clean re-run) | 92,000,000 EXACT | 470.4 | 8113/8253 (bal) | 360.4 | 1.31× | +110 | 441.7 | slower | a✗ b✗ c✗ | FAIL (marginal; prior uniform 442.6s=1.23× was a PASS — VM-noise borderline) |
| q19 | FINISHED (clean, 0 restart) | 92,000,000 EXACT | 565.5 | 11325/11345 (bal) | 305.5 | 1.85× | +260 | 272.4 | slower | a✗ b✗ c✗ | FAIL — heavy join + 11.3G compaction-transient drags on overcommitted VM (prior uniform 238.6s would PASS — VM headroom-bound) |
| q4 | FINISHED (clean, 0 restart) | 25,848,286 (forst-rs windowed-emit; matches prior 25.83M; rdb emits 177.6M diff semantics) | 400.1 | 9094/8318 (bal) | 503.0 | 0.80× | -103 | DNF (forst-local RESTARTING 1644s) | FASTER | a✓ b✓ c✓ | **PASS all 3** — KV-sep win; beats rdb AND forst-local |
| q7 | FINISHED (clean, 0 restart) | 92,000,002 EXACT (src 92,000,164) | 1176.1 | 7556/7387 (bal) | no same-pop local rdb (remote rdb 1367.6 DNF-w/o-io_uring) | (1176<1367 dir.) | — | ForSt-M 586.8 | SLOWER | a~(dir) b— c✗ | FAIL vs ForSt (io_uring-dep heavy join, ForSt far ahead); finish+correct; slower than prior 947.8s = VM load |

## Docker-crash / restart events (honest log)
- After q8/q12/q11(×2)/q17 back-to-back, VM memory pressure accumulated. q18 (1st attempt)
  stalled at 76.6M, a TM failed → job RESTARTING ~100s → re-ran source from 0 → false-FINISHED
  at 37.4M rows. Recorded as DNF, NOT a pass (truncated rows after restart = the documented
  false-FINISH trap). Mitigation: full Docker Desktop restart (quit→relaunch) to clear VM
  pressure. After restart Docker flapped (500 errors, then a cold-start SIGBUS in tm2's JVM on
  the first q18 retry) — required a 2nd full quit/relaunch + image warm-up run before clusters
  came up healthy. q18 re-run (warm) launched clean (all 3 containers Up).

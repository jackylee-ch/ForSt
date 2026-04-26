# Review Protocol — 120-round loop per commit

## Termination condition (strict)

```
STOP when:
  (consecutive_clean_rounds >= 10)   # H=0 AND M=0 for 10 in a row
  OR round_count >= 120
```

## Per-round procedure

1. **Launch 10 parallel sub-agents**, one per dimension. Each receives:
   - Target commit SHA
   - Worktree path: `~/code/github/ForSt-review/`
   - Output format (strict — used by extractor script)
2. **Aggregate** H/M/L counts per dimension.
3. If H+M = 0: increment `consecutive_clean_rounds`; else reset to 0.
4. Fix all H and M findings. Commit fix as `fix(Cn): round N review H/M — <brief>`.
5. Run `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && cargo bench --bench <Cn>_bench`.
6. If any check fails → revert fix, re-open finding. Else go to round N+1.

## Agent dimensions (10 fixed)

| # | Dimension | Focus |
|---|-----------|-------|
| 1 | Memory safety | unsafe blocks, aliasing, lifetimes, raw pointers |
| 2 | Correctness | algorithmic correctness, edge cases, data consistency |
| 3 | Concurrency | locks, atomics, Send/Sync, race conditions, ArcSwap |
| 4 | Test coverage | branch coverage, missing edge-case tests, false positives |
| 5 | Error handling | Result propagation, panic safety, FFI boundary |
| 6 | **Performance** | vs RocksDB/ForSt C++ baseline benchmark results. ANY perf regression or <3x on claimed dim → M/H |
| 7 | Documentation | doc accuracy, API contract clarity, examples |
| 8 | Idiomatic Rust | style, clippy, patterns, API ergonomics |
| 9 | Security | DoS, OOM, input validation, resource leaks |
| 10 | Integration | cross-module interaction, downstream consumer impact |

## Agent prompt template

```
Round N Agent K reviewing commit <SHA> at ~/code/github/ForSt-review/.
Dimension: <name>.

Read: git show <SHA>. Focus files: <list>.
Benchmarks produced by Cn live in forst-rs-bench/benches/<Cn>/.

OUTPUT (strict):
## Agent K — <dim> — Round N — Cn
### Findings
- [H/M/L] title (file:line): desc; reproduction if applicable
### Summary
H: n, M: n, L: n
```

## Aggregation script (bash, simplified)

```bash
for id in "$@"; do
  out=$(python3 -c "..."  # extract last assistant text)
  h=$(echo "$out" | grep -oP 'H:\s*\K\d+' | tail -1)
  m=$(echo "$out" | grep -oP 'M:\s*\K\d+' | tail -1)
  echo "$id H=$h M=$m"
done | awk '{h+=$2;m+=$3} END {print "TOTAL H="h" M="m}'
```

## Perf gate (Dimension 6 binding)

Each benchmark in `forst-rs-bench/benches/Cn/` asserts against `baseline.json`:

```rust
#[bench_target(speedup = 3.0, vs = "rocksdb_cxx")]
fn bench_sst_seq_read(b: &mut Bencher) { ... }
```

If measured throughput < 3x baseline → harness emits `PERF FAIL: bench=X expected=3x actual=2.1x` and exits 1.
Agent 6 treats any such FAIL as **[H]** in the round.

## Fix-commit discipline

- Each fix commit: `fix(Cn): round N review — <summary>`
- Must update benchmarks if perf regression
- Must add regression test for correctness bugs
- Round N+1 reviews fix commit (on top of Cn) — not Cn directly

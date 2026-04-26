# Session Handoff — State Machine

## Current state (as of 2026-04-25, session A finished)

```
phase: A (infrastructure done)
worktree: NOT YET CREATED
current_commit: none
current_round: 0
consecutive_clean: 0
baselines_built: false
```

## Next session (B) should execute

### Immediate actions (first 30 min)

```bash
# 1. Create worktree
cd ~/code/github/ForSt
git worktree add ../ForSt-review -b review-loop forst-rs

# 2. Verify
cd ~/code/github/ForSt-review
git log --oneline -1  # should show 4b0099c59

# 3. Reset to pre-refactor base for clean replay
# (discuss with user: reset to main branch? keep forst-rs tip?)
# Default: keep forst-rs tip; Cn review runs on-top.

# 4. Build baselines
mkdir -p ~/code/baselines
cd ~/code/baselines
git clone --depth 1 --branch v8.11.3 https://github.com/facebook/rocksdb
cd rocksdb && make static_lib -j$(nproc)
```

### Per-commit loop (semi-automated)

```
for commit_id in C1 C2 C3 C4 C5 C6 C7 C8 C9:
    run session with:
        "Continue refactor-review for <commit_id>.
         Read .planning/refactor-review/*.md.
         Current state in this file below.
         Execute: ensure Cn committed → add benches → round N review.
         Stop when consecutive_clean=10 or round=120.
         At session end, update STATE below."
```

## Session B-N: per-session work item

Each session should:
1. Read this STATE block → resume where left off
2. Launch 10 parallel agents (ref REVIEW_PROTOCOL.md)
3. Extract findings → fix H/M → commit → verify
4. Advance round count
5. Update STATE block before context runs out
6. Emit continuation prompt for next session

## STATE (update this at end of every session)

```yaml
# Copy entire block before modifying; maintain history below
last_updated: 2026-04-25T02:30:00Z
last_session_id: A
phase: A_DONE
current_commit: none  # will be C1 in session B
current_round: 0
consecutive_clean: 0
rounds_log: []  # will accumulate {commit, round, H, M, L, fix_sha}
baselines_built: false
next_action: "Session B: create worktree, build RocksDB baseline, cherry-pick C1, start C1 Round 1"
```

## Continuation prompt (paste into next session)

```
继续 ForSt-RS refactor-review 任务。

工作目录: ~/code/github/ForSt
Worktree (to create): ~/code/github/ForSt-review (branch: review-loop)
语言: 中文
IMPORTANT: Before ANY cargo command:
  export PATH="/home/users/lijunqing/.cargo/bin:/usr/bin:/bin:/usr/local/bin:$PATH"

1. 读 .planning/refactor-review/SESSION_HANDOFF.md 的 STATE 块
2. 读 COMMIT_MANIFEST.md + REVIEW_PROTOCOL.md + BENCHMARK_BASELINE.md
3. 按 next_action 执行；每完成一个里程碑更新 STATE
4. session 即将耗尽上下文时，更新 STATE + 写出下一个 continuation prompt
5. 硬性要求: 每 commit 120 轮 10-agent review，终止条件 10 consecutive clean rounds
6. 性能硬性 3-5x vs RocksDB — benchmark 失败即 H issue
```

## Session history

| Session | Date | Phase | Commit | Rounds | Status |
|---------|------|-------|--------|--------|--------|
| A | 2026-04-25 | Phase A | infra | N/A | Infrastructure docs written |
| B | TBD | Phase B | C1 common | 0→? | Pending |
| ... | | | | | |

## Risk register

1. **3-5x perf KPI not achievable for all benchmarks**: per user directive 2026-04-25 conv., hard target stands. Expect some Cn to stall indefinitely at 120 rounds with M-level perf issues. Accept 120-round cap as fallback exit.
2. **Baseline build broken**: pin RocksDB v8.11.3; if build fails on target host, fall back to Docker image `facebookresearch/rocksdb:v8.11.3`.
3. **Context exhaustion mid-commit**: STATE block must be updated BEFORE running risky long tool calls. Every 5 rounds → snapshot STATE.
4. **Agent output parse failures**: REVIEW_PROTOCOL.md mandates strict "H: n, M: n, L: n" final line. Enforcer: any agent output lacking this line → rerun that agent (up to 2 retries).

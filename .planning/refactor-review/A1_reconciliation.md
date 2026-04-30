# A1 Reconciliation — ForSt-RS Refactor-Review v2.1

**Status:** Approved by user via interactive brainstorming session, 2026-04-30
**Authority:** This document supersedes the user-supplied "Master Prompt v2" where they conflict; v2 is informing-only.
**Resume point:** `.planning/refactor-review/SESSION_HANDOFF.md` STATE block (to be updated by writing-plans next-step)

---

## 0. Purpose

Reconcile the user-supplied "Master Prompt v2" (English) with the in-flight refactor-review work on the `forst-rs` branch. As of 2026-04-30 this branch carries:

- 28 design / research docs (`docs/understanding/1.1–1.17`, `docs/design/2.1–2.13`)
- 9-commit refactor manifest C1–C9 (`.planning/refactor-review/COMMIT_MANIFEST.md`)
- Round 1 of C1 review complete with 10/10 agents: H=26, M=52, L=55
- Open blocker: 7 of 13 C1 benchmarks proved architecturally unable to reach the 3× RocksDB bar under the original C1 design

This document produces a single authoritative framework for completing the rewrite to user-acceptance criteria. It is the spec output of the `superpowers:brainstorming` flow and the input to `superpowers:writing-plans`.

---

## 1. Hard Constraints (non-negotiable)

1. **Per-component perf gate:** ≥3× vs RocksDB C++ on every benchmark in `forst-rs-bench/benches/Cn/`. The harness must exit non-zero on miss. No tiering, no per-bench downgrades, no "accept and document" bypass for perf reasons. Misses are resolved through §3 arch-pivots.
2. **End-to-end perf gate (new):** ≥30–40% speedup on Flink Nexmark vs the original ForSt + Flink stack. Verified at Phase H against the §4 baseline harness.
3. **Termination predicate per Cn:** `(consecutive_clean_rounds ≥ 10 with H=0 ∧ M=0) OR (round_count = 120)`. Both H *and* M must be zero in every clean round — Medium issues are convergence-blocking.
4. **Object-store I/O:** Apache OpenDAL only. No callbacks into Flink IO interfaces. Credentials only from user config or environment (`forst-rs.remote.access-key`, `FORST_RS_ACCESS_KEY`, etc.).
5. **No vocabulary churn:** keep C1–C9 commit manifest, the 7 actual workspace crates, and `.planning/refactor-review/` paths.

---

## 2. Framework Spine (§A)

### 2.1 Stays from existing protocol

| Item | Value | Source |
|---|---|---|
| Commit shape | 9 commits **C1–C9** | `.planning/refactor-review/COMMIT_MANIFEST.md` |
| Crates | `forst-rs-{common, engine, storage, io, ffi, bench, test-utils}` | `Cargo.toml:17–24` |
| Reviewer count per round | **10 fixed dimensions** | `.planning/refactor-review/REVIEW_PROTOCOL.md:24–36` |
| Termination | 120 rounds **OR** 10× `H=0 ∧ M=0` | `REVIEW_PROTOCOL.md:1–10` |
| Per-bench perf gate | **Hard 3× vs RocksDB C++** (harness exits non-zero on miss) | `REVIEW_PROTOCOL.md:66–75` |
| Reports path | `.planning/refactor-review/Cn-Rk-findings.md` | (existing convention) |
| Resume mechanism | `SESSION_HANDOFF.md` STATE block | (existing convention) |

### 2.2 Added from v2 (absorbed)

| Item | Phase | Why |
|---|---|---|
| Nexmark 30–40% E2E acceptance | H | User's stated end goal (2026-04-30); closes the gap between per-component 3× and user-visible win |
| GHA rebuild — 6 workflows | F | v2 §12.3 list; existing `.github/workflows/` is partial |
| Flink P0/P1/P2 scenario tiers | G | v2 §13 test matrix; ensures KeyedState/CP/Rescaling/TTL/LocalRecovery/Timer pass E2E |
| OpenDAL-only constraint (reaffirmed) | All | Prevent accidental Flink-IO callbacks |
| Coverage gates: Rust ≥ 90%, Java ≥ 80% | G | v2 §13.1 |
| Nexmark Gate-Zero baseline track | F (parallel) | Required to verify §1 constraint 2 at Phase H |

### 2.3 Dropped from v2 (rejected)

| Item | Why rejected |
|---|---|
| PR-01 .. PR-18 vocabulary | Pure rename of C1–C9; zero value, all-files churn |
| 12-tiny-crate split (`forst-core`, `forst-arrow`, …) | Conflicts with the 7 already-shipped crates; rename forces touch on every file |
| `reports/A1_*` / `reports/E_pr*_round*` paths | Existing `.planning/refactor-review/` paths work and are referenced by infra |
| Softer perf bar ("≥2× vs original ForSt") | User confirmed 3× vs RocksDB is the hard requirement |
| 100-round / H-only termination | v2's gate is *strictly weaker* than existing `H=0 ∧ M=0`; existing wins |
| Crate renames mid-flight | All-files churn for no perf or correctness benefit |

---

## 3. Architecture-Pivot Sub-Loop (§B) — The Only Protocol Modification

This is the only delta to `REVIEW_PROTOCOL.md`. Resolves the C1 R1 blocker (7 architecturally-stuck benchmarks).

### 3.1 Trigger

A Round-N reviewer raises an `[H]` finding of the form: **"benchmark X cannot reach 3× under current architecture."**

### 3.2 Triage (Tech VP role)

| Class | Definition | Path |
|---|---|---|
| **Tuning** | Adjustable params, alignment, prefetch, cache size, batch granularity | Standard fix-in-place; regression test added; advance round normally |
| **Architectural** | Data structure choice, allocation strategy, FFI shape, locking model, layout | Arch-pivot sub-task per §3.3 |

### 3.3 Arch-pivot rules

1. **Module boundary (§B.1 = a):** Pivot must stay within the current Cn's module boundary.
   - During C1 (`forst-rs-common`) review, may rewrite Arena, varint, checksum, metrics, config — but **may NOT** touch `forst-rs-storage` SST format (that's C3 territory) or `forst-rs-engine` (C7).
   - Cn module boundaries are the column "Scope" in `COMMIT_MANIFEST.md`.

2. **Round counter (§B.2 = b):** Round counter **resets to 0** on every arch-pivot.
   - Pivoted code is treated as "new code"; needs a fresh 10-clean streak from scratch.
   - Rationale: every architectural change gets full multi-round scrutiny under all 10 dimensions; hidden regressions from a pivot cannot sneak through on a partially-clean prior streak.
   - **Schedule cost (acknowledged):** each pivot adds ~10–15 round-days to that Cn's calendar. C1 with 7 architecturally-stuck benches likely requires **~70–90 rounds total**, ~11–15 calendar weeks. Total project calendar may extend to ~12+ months if Cn's beyond C1 also surface arch-pivot-class findings.

3. **Cross-boundary escalation (§B.3 = a):** If a pivot truly cannot stay inside Cn (e.g., C1 perf gap requires SST-format change), Tech VP picks per-case:
   - **(i)** Defer the affected bench to Cn+k where the dependency lives. Recorded in `known_issues.md` with rationale + Nexmark-impact estimate. The deferred bench *must* be solved in Cn+k's review or escalated again.
   - **(ii)** Re-order the C1–C9 sequence to put the dependency commit first. Recorded as a Tech VP decision in `.planning/refactor-review/Cn-arch-pivot-log.md` with new dependency DAG.
   - Either decision is a STOP-and-Confirm checkpoint with the user — Tech VP proposes, user approves.

### 3.4 Output artifacts

- Pivot commit format: `arch(Cn): pivot <component> for bench <X> — <one-line reason>`
- Per-Cn cumulative log: `.planning/refactor-review/Cn-arch-pivot-log.md`
  - Entry per pivot: timestamp, original H finding, root cause, redesign approach, before/after benchmark numbers, post-pivot round count

---

## 4. Nexmark Gate-Zero (§C)

### 4.1 Goal

Establish a Nexmark baseline (Flink + original ForSt) so the ≥30–40% E2E target in §1 is verifiable in Phase H and traceable from per-component 3× wins.

### 4.2 Plan

1. **Research** (Performance Engineer + QA): investigate `https://github.com/apache/flink/tree/release-2.2.0/.github/workflows/` for any Nexmark CI workflow. Cross-check `https://github.com/nexmark/nexmark` (community repo) as fallback.
   - **Open feasibility risk:** Flink GHA Nexmark infra may not exist as a public reusable workflow. Outcome of research determines whether (a) we mirror Flink GHA, (b) we adopt the community `nexmark/nexmark` harness, or (c) we build from scratch.
2. **Mirror or build**: based on research outcome, instantiate `.github/workflows/nexmark-baseline.yml` and `nexmark/` directory containing `docker-compose.yml` (Flink mini-cluster + original ForSt + queries q0..q22).
3. **Establish baseline**: 3× warmup runs + 5× measurement runs; per query record p50, p95, p99 latency and throughput. Commit `.planning/nexmark/baseline.json` with the numbers + reproducibility notes.
4. **Sign-off**: user reviews `baseline.json` and signs off via commit comment in `.planning/nexmark/baseline-signoff.md` referencing the SHA of the baseline commit.

### 4.3 Owner & gates

- **Owner**: Performance Engineer (lead) + QA
- **Effort estimate**: ~2 weeks elapsed (parallel with C1–C4 review)
- **Gating**: C5 (`forst-rs-storage` cache) review **may not start** until `baseline.json` is signed off. C1–C4 advance in parallel. This balances early E2E feedback against blocking the in-flight C1 work.
- **Tracking**: per-Cn perf summary appends an "Estimated Nexmark contribution: q-X +Y%" column to `COMMIT_MANIFEST.md` so we always know how per-component 3× wins compose toward the 30–40% E2E target.

---

## 5. Phase Mapping (v2 A→H ↔ reality) — §D

| v2 Phase | Status today | Reconciled action |
|---|---|---|
| **A** Status assessment | de facto done via `docs/understanding/1.*` (17 files), `docs/design/2.*` (13 files), C1 R1 findings | This document (`A1_reconciliation.md`) is the bridge / authoritative spec. No separate v2 §7 assessment needed. |
| **B** PR split plan | done as `COMMIT_MANIFEST.md` (9 commits) | **writing-plans next-step:** annotate `COMMIT_MANIFEST.md` with explicit Done Criteria + estimated Nexmark contribution column per Cn. |
| **C** Branch baseline | `forst-rs-bak` exists locally and on origin | No-op. |
| **D** Per-Cn refactor loop | C1 R1 done; blocker open | **writing-plans next-step:** update `REVIEW_PROTOCOL.md` to include §3 arch-pivot sub-loop, then resume **C1 R2** with arch-pivot authority active. |
| **E** Reviewer loop | 10 reviewers × 120 rounds × `H=0 ∧ M=0` | Keep. v2's 5/10 escalation rule is *less strict* and rejected. |
| **F** GHA rebuild | partial (`.github/workflows/` exists; needs audit) | Adopt v2 §12.3 list adapted to actual crates: `ci-rust.yml`, `ci-java.yml`, `ci-e2e.yml`, `ci-bench.yml`, `ci-security.yml`, `ci-release.yml`. Add `nexmark-baseline.yml` per §4. |
| **G** Coverage + scenarios | not started | Adopt v2 §13 test matrix + Flink P0/P1 scenarios E2E. Add Nexmark E2E run as a Phase-G acceptance step. |
| **H** Final acceptance | not started | Adopt v2 §14 checklist with hard Nexmark 30–40% line added (see §6 below). |

---

## 6. Definition of Done — §E

The Tech VP signs off only if **all** of the following hold:

1. **C1–C9 each terminated**: 120 rounds OR 10× consecutive `H=0 ∧ M=0`.
2. **Per-bench perf gate met**: ≥3× vs RocksDB C++ on every benchmark across C1–C9. Architectural misses resolved by §3 arch-pivots; no tiering, no caps, no "accept and document" downgrades.
3. **Nexmark E2E gate met**: ≥30–40% speedup vs original ForSt + Flink, measured on §4 harness, signed off by user.
4. **GHA**: all 6 workflows + `nexmark-baseline.yml` green; release dry-run executed once.
5. **Coverage**: Rust ≥ 90%, Java ≥ 80%.
6. **Flink scenarios**: P0 (KeyedState / Checkpoint / Rescaling / TTL / Local Recovery / Timer) + P1 (Schema Evolution / Savepoint Native+Canonical / State Processor / Fast Recovery / Changelog) all pass E2E. P2 Roadmap doc complete with prereq / effort / risk per entry.
7. **Soak test**: 72-hour continuous workload (StateBackend + Rescaling + TTL + Schema Evolution concurrent), zero crashes / leaks / Checkpoint failures.
8. **Migration Guide**: original ForSt → ForSt-RS *and* RocksDB → ForSt-RS, both reproducible by an outside reader.
9. **Deployment guide**: drop `libforst_rs.so` + `forst-rs-flink-<v>.jar` into Flink `lib/`, reproducible by outside reader.
10. **`known_issues.md`**: lists every residual L-level / non-blocking issue with priority and Nexmark-impact estimate.

---

## 7. Open Items / Dependencies / Verifiable Risks

These are *known* and tracked, not blockers to spec approval:

1. **§4 Flink GHA Nexmark feasibility unverified.** Research outcome determines whether mirror / community-fork / build-from-scratch path is taken. Will surface to user before committing to a path.
2. **§3.3.2 schedule cost is a real risk.** With `(b) counter resets`, C1 alone may require 11–15 calendar weeks if all 7 stuck benches need arch-pivots. Total project calendar may extend to 12+ months. User has acknowledged this trade-off (correctness over schedule) on 2026-04-30.
3. **§3.3.3 cross-boundary escalation** is the riskiest path. If a Cn requires cross-module arch-pivot, project schedule and Cn ordering may shift. Each occurrence is a STOP-and-Confirm with user.
4. **Coverage tooling**: `cargo llvm-cov` is the chosen Rust tool (per v2 §13.1). Needs to be wired into `ci-rust.yml` with a fail-build threshold of 90%.

---

## 8. Constraints Inherited Verbatim from v2 §18

These remain hard constraints throughout the project:

- ❌ Fabricating APIs / class names / methods / line numbers
- ❌ Writing AK/SK into any file, log, comment, or commit message
- ❌ Pushing directly to a main branch (worktree + feature branch only)
- ❌ Skipping STOP nodes
- ❌ A single commit doing multiple things ("chore + feat + fix" must be split)
- ❌ Calling back into Flink interfaces for object-store IO (must go through OpenDAL)
- ❌ Building Table Format / Catalog / user-semantic metadata
- ❌ Skipping Review-Fix and merging directly
- ❌ Fixing a High issue without adding a regression test
- ❌ Treating P2 as "won't do" (Roadmap is mandatory)
- ❌ Vague language ("probably / should / maybe") for performance and correctness conclusions

---

## 9. Next Step (after user approves this spec)

1. Invoke `superpowers:writing-plans` to produce an implementation plan covering at minimum:
   - **a.** Update `REVIEW_PROTOCOL.md` to add §3 arch-pivot sub-loop language
   - **b.** Update `SESSION_HANDOFF.md` STATE block: clear `CRITICAL_BLOCKER`, set `phase: B_R2_READY`, set `next_action` to "Resume C1 R2 with arch-pivot authority"
   - **c.** Annotate `COMMIT_MANIFEST.md` with Done Criteria column + estimated Nexmark contribution column
   - **d.** Kick off Nexmark Gate-Zero research task (Phase F, parallel)
   - **e.** Plan Phase F GHA rebuild work (6 workflows)
   - **f.** Schedule resume of C1 R2 (after a, b, c land)
2. Each subsequent Cn loop runs through the modified protocol.
3. STOP-and-Confirm checkpoints respected at: end of each Cn, every cross-boundary escalation, Nexmark baseline sign-off, Phase F→G→H transitions.

---

## 10. Source References

| Reference | Purpose |
|---|---|
| User-supplied "Master Prompt v2" (English) — this session's args | Source of v2 phases, deliverables index, hard constraints, scenario tiers, GHA workflow list |
| `.planning/refactor-review/COMMIT_MANIFEST.md` | 9-commit C1–C9 plan, per-Cn scope and perf target |
| `.planning/refactor-review/REVIEW_PROTOCOL.md` | 10-agent / 120-round / `H=0 ∧ M=0` termination, per-round procedure |
| `.planning/refactor-review/SESSION_HANDOFF.md` | State machine, current blocker, agent IDs |
| `.planning/refactor-review/C1-R1-findings.md` | C1 R1 specific findings (informs which 7 benches need arch-pivot) |
| `Cargo.toml:17–24` | Actual workspace members |
| `docs/understanding/1.1–1.17` | Source-research notes (Flink, ForSt, Fluss, OpenDAL) |
| `docs/design/2.1–2.13` | Existing design docs (Arrow SST, MemTable, BlockCache, FFM, Compaction, Checkpoint, Flink adapter) |
| v2 §12.3 | GHA workflow list adopted in §5 row F |
| v2 §13 | Test matrix adopted in §5 row G |
| v2 §14 | Acceptance checklist adopted in §6 (with Nexmark addition) |
| v2 §18 | Hard constraints inherited verbatim in §8 |

---

## 11. Approval

| Section | User decision | Decided |
|---|---|---|
| §A Framework spine partition | ✅ Agreed (incl. strict `H=0 ∧ M=0`) | 2026-04-30 |
| §B.1 Module boundary | (a) pivot stays within Cn | 2026-04-30 |
| §B.2 Counter on pivot | (b) counter resets on every arch-pivot | 2026-04-30 |
| §B.3 Cross-boundary escalation | (a) Tech VP per-case | 2026-04-30 |
| §C Nexmark Gate-Zero timing | (b) before C5 review starts | 2026-04-30 |
| §D Phase mapping | confirmed (no objections raised) | 2026-04-30 |
| §E Definition of Done | confirmed (no objections raised) | 2026-04-30 |
| Spec output location | `.planning/refactor-review/A1_reconciliation.md` (existing infra path) | 2026-04-30 |

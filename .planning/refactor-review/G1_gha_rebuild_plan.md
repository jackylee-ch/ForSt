# G1 — GHA Rebuild Design + Plan

> **For agentic workers:** This combines a brainstorm-level design audit with a per-workflow implementation plan. The shape is largely fixed by A1 spec §5 row F + v2 §12.3; the design work here is filling in commands, gates, matrices, and migration order. Tasks use checkbox (`- [ ]`) syntax.

**Goal:** Bring this repo's GHA in line with A1 spec's expected workflow set: `ci-rust.yml`, `ci-java.yml`, `ci-e2e.yml`, `ci-bench.yml`, `ci-security.yml`, `ci-release.yml` — plus the already-landed `nexmark-baseline.yml`.

**Architecture:** Migrate the existing single `ci.yml` (Rust fmt/clippy/test/build/MSRV/audit) into the named `ci-rust.yml` superset that adds an `llvm-cov` coverage gate ≥90%. Add the other 5 workflows as new files. Stub out workflows whose dependencies aren't built yet (Java cdylib JAR, E2E containers) so they exist but no-op until the real pieces land.

**Tech Stack:** GitHub Actions YAML, `dtolnay/rust-toolchain`, `actions/cache`, `actions-rs/cargo-llvm-cov`, `cargo-deny`, `actions/setup-java` (for ci-java/ci-e2e), `docker/build-push-action` (for ci-e2e + ci-release multi-platform).

**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` §5 row F @ commit `107bc0ce2`.
**v2 reference:** v2 §12.3 minimum workflow list.

---

## 0. Existing State Audit

| Workflow | Path | Status | Coverage |
|---|---|---|---|
| `ci.yml` (Rust) | `.github/workflows/ci.yml` | EXISTS | fmt-check, clippy, test, build-release, msrv, audit. Missing: `cargo-llvm-cov` coverage gate. |
| `nexmark-baseline.yml` | `.github/workflows/nexmark-baseline.yml` | EXISTS (landed in N2 Lane A Task 5) | Smoke-build only; gracefully no-ops until nexmark/ submodule lands |
| `ci-rust.yml` (renamed/superset) | not yet | TARGET | superset of ci.yml + coverage gate |
| `ci-java.yml` | not yet | TARGET (currently no Java code in repo — stub workflow) |
| `ci-e2e.yml` | not yet | TARGET (currently no cdylib JAR — stub workflow) |
| `ci-bench.yml` | not yet | TARGET (criterion benches exist in `crates/forst-rs-bench/`) |
| `ci-security.yml` | not yet | TARGET (audit currently inside `ci.yml` `audit` job; expand to dedicated workflow) |
| `ci-release.yml` | not yet | TARGET (template for tag-based release; activated only on tag push) |

---

## 1. Per-Workflow Design

### 1.1 `ci-rust.yml` — primary Rust CI

**Trigger:** push / PR to `forst-rs` (later: also `main` once integration starts).

**Jobs:**
1. **fmt-check** — `cargo fmt --all --check` (existing)
2. **clippy** — `cargo clippy --workspace --all-targets -- -D warnings` (existing)
3. **test** — `cargo test --workspace` (existing)
4. **build-release** — `cargo build --release --workspace` (existing)
5. **msrv** — `cargo check` against MSRV `1.75` (existing)
6. **coverage** *(NEW)* — `cargo llvm-cov --workspace --lcov --output-path lcov.info --fail-under-lines 90` per A1 spec §6 item 5. Upload to Codecov.
7. **doc** *(NEW)* — `cargo doc --no-deps --workspace --document-private-items` to surface rustdoc errors early.

**Cache strategy:** Per-job cache keyed on `Cargo.lock` hash (existing pattern). Coverage adds `~/.cargo/llvm-cov-target` to cache key.

**Gate:** All jobs must pass for PR merge. Coverage gate `--fail-under-lines 90` enforces the spec hard requirement.

**Migration plan:** create new `ci-rust.yml` with all 7 jobs; delete `ci.yml` in the same commit. The `audit` job moves to `ci-security.yml` (see 1.5).

### 1.2 `ci-java.yml` — Java side CI

**Trigger:** push / PR to `forst-rs` (Java-paths only, e.g. `**/*.java`, `**/pom.xml`).

**Jobs:**
1. **build** — `mvn -B verify -DskipTests=false` (placeholder until Java module lands; until then, no-op via path-filter that fails to match)
2. **spotbugs** — `mvn -B spotbugs:check` (placeholder)
3. **shaded-jar** — `mvn -B package -DskipTests` and verify shaded JAR exists (placeholder)

**Cache strategy:** Cache `~/.m2/repository` keyed on `**/pom.xml` hash.

**Gate:** All Java jobs must pass for PR merge once Java module lands. Until then, workflow has no-op trigger.

**Stub policy:** workflow YAML lands now with `if: hashFiles('**/pom.xml') != ''` guards on each job — when pom.xml exists, jobs run; when it doesn't, they skip cleanly. No false-positive breakages.

### 1.3 `ci-e2e.yml` — Cross-language end-to-end

**Trigger:** push to `forst-rs` (NOT pull_request — too expensive); manual `workflow_dispatch`; nightly schedule (`cron: '0 6 * * *'`).

**Jobs:**
1. **integration-suite** — Docker compose with:
   - MinIO (S3-compatible object store for OpenDAL backend)
   - Built `libforst_rs.so` (Rust cdylib)
   - Built `forst-rs-flink-<v>.jar` (Java FFM bridge)
   - Flink mini-cluster (single-JM + single-TM containerized)
   - The existing `flink-state-backend-common-test` test class

   Run `mvn -B integration-test` against the compose stack.

**Stub policy:** workflow lands now; jobs use `if: hashFiles('libforst_rs.so', 'forst-rs-flink-*.jar') != ''`-style guards. No-op until artifacts exist.

**Gate:** Required for `main` merges (post-integration); not required for `forst-rs` PRs until Phase G.

### 1.4 `ci-bench.yml` — Performance regression CI

**Trigger:** nightly schedule (`cron: '0 4 * * *'`); manual; on tag push.

**Jobs:**
1. **criterion** — `cargo bench --workspace` (forst-rs-bench crate)
2. **upload-perf-trend** — push criterion HTML reports to GH Pages branch `gh-pages` for trend dashboards

**Cache strategy:** Cache `target/criterion` keyed on bench source hash.

**Gate:** Non-blocking (informational). Per `REVIEW_PROTOCOL.md` §"Perf gate", the actual ≥3× perf gate runs in `forst-rs-bench` per-Cn benches against `baseline.json` and exits non-zero on miss — this CI workflow runs the criterion suite for trend tracking.

**NOT in scope of this CI**: the `perf_gate` exit-code gate on per-Cn benches (`forst-rs-bench/benches/Cn/`) — that runs as part of `cargo test` in `ci-rust.yml`. ci-bench is for criterion+trend.

### 1.5 `ci-security.yml` — Dedicated security audit

**Trigger:** push / PR / weekly schedule (`cron: '0 8 * * 1'`).

**Jobs:**
1. **cargo-audit** — `cargo audit --deny warnings` (move from existing ci.yml `audit` job)
2. **cargo-deny** — `cargo deny check` against `deny.toml` (license + advisory + bans)
3. **dependabot-config** — verify `.github/dependabot.yml` is well-formed (yamllint)

**Cache strategy:** Cache `~/.cargo/advisory-db`.

**Gate:** All 3 must pass for PR merge.

**Companion file needed:** `deny.toml` (currently missing). Default content: deny non-Apache-2.0-compatible licenses, deny known-vulnerable advisories, deny duplicate transitive deps.

### 1.6 `ci-release.yml` — Tag-driven release

**Trigger:** push of tag matching `v*`.

**Jobs:**
1. **build-cdylib** — matrix over `[ubuntu-22.04 (x86_64), ubuntu-22.04-arm (aarch64), macos-14 (x86_64), macos-14 (arm64)]`. Build `target/release/libforst_rs.{so,dylib}` for each.
2. **build-jar** — `mvn -B package` (shaded JAR) on ubuntu-22.04
3. **upload-release** — `softprops/action-gh-release@v2` attaches all cdylibs + the JAR to the GitHub release
4. **release-notes** — generate from `git log <prev-tag>..HEAD` with conventional-commit grouping

**Stub policy:** workflow lands now; gated on tag push only. No tags expected until Phase H acceptance.

**Gate:** All 4 jobs must succeed for the release to be published.

---

## 2. Migration Order

Tasks below produce 6 commits — one per workflow file (plus one for `ci.yml` removal as part of `ci-rust.yml` migration). Commits 1, 5, 6 are zero-risk (new files, immediately useful). Commits 2, 3, 4 are stub-gated (workflow files exist but jobs no-op until dependencies land).

### Task 1: Create `ci-rust.yml` superset; delete `ci.yml`

**Files:**
- Create: `.github/workflows/ci-rust.yml`
- Delete: `.github/workflows/ci.yml`

- [ ] **Step 1.1**: Read existing `.github/workflows/ci.yml` to baseline
- [ ] **Step 1.2**: Write `ci-rust.yml` containing all 7 jobs (existing 5 + coverage + doc)
- [ ] **Step 1.3**: Verify locally `actionlint .github/workflows/ci-rust.yml` passes (or gh action validation)
- [ ] **Step 1.4**: Delete `ci.yml`
- [ ] **Step 1.5**: Commit `feat(ci): replace ci.yml with ci-rust.yml + coverage gate ≥90%`

**Risk**: removing `ci.yml` disables current CI temporarily during the commit window. Acceptable since `ci-rust.yml` lands in the same commit.

### Task 2: Create `ci-java.yml` stub

**Files:**
- Create: `.github/workflows/ci-java.yml`

- [ ] **Step 2.1**: Write workflow with 3 jobs (build, spotbugs, shaded-jar), each guarded by `hashFiles('**/pom.xml') != ''`
- [ ] **Step 2.2**: Add path-filter so workflow only runs when Java files change
- [ ] **Step 2.3**: Commit `feat(ci): add ci-java.yml stub (no-op until Java module lands)`

### Task 3: Create `ci-e2e.yml` stub

**Files:**
- Create: `.github/workflows/ci-e2e.yml`

- [ ] **Step 3.1**: Write workflow with `integration-suite` job using `services:` block (MinIO container) + path-filter guards
- [ ] **Step 3.2**: Document docker-compose location placeholder (`tests/e2e/docker-compose.yml` to be created in Phase G)
- [ ] **Step 3.3**: Commit `feat(ci): add ci-e2e.yml stub (no-op until cdylib + JAR built)`

### Task 4: Create `ci-bench.yml`

**Files:**
- Create: `.github/workflows/ci-bench.yml`

- [ ] **Step 4.1**: Write workflow with `criterion` and `upload-perf-trend` jobs
- [ ] **Step 4.2**: Verify `forst-rs-bench` workspace member exists (it does, per `Cargo.toml`)
- [ ] **Step 4.3**: Configure GH Pages destination (branch `gh-pages`, path `criterion-reports/`)
- [ ] **Step 4.4**: Commit `feat(ci): add ci-bench.yml for nightly criterion + perf-trend dashboard`

### Task 5: Create `ci-security.yml` + `deny.toml`

**Files:**
- Create: `.github/workflows/ci-security.yml`
- Create: `deny.toml` (workspace root)
- Create: `.github/dependabot.yml`

- [ ] **Step 5.1**: Write `deny.toml`:
  - `[bans]`: deny multiple-versions for `arrow`, `tokio`
  - `[licenses]`: allow `Apache-2.0`, `MIT`, `BSD-3-Clause`, `Unicode-3.0` (plus Apache-2.0 with LLVM exception); deny `GPL-*`, `AGPL-*`
  - `[advisories]`: `unmaintained = "deny"`, `yanked = "deny"`
  - `[sources]`: `unknown-registry = "deny"`, `unknown-git = "deny"`
- [ ] **Step 5.2**: Write `.github/dependabot.yml` for cargo + github-actions ecosystems
- [ ] **Step 5.3**: Write `ci-security.yml` with 3 jobs (cargo-audit, cargo-deny, dependabot-config)
- [ ] **Step 5.4**: Remove `audit` job from `ci-rust.yml` (was migrated from old ci.yml; now lives in ci-security.yml — avoid duplication)
- [ ] **Step 5.5**: Commit `feat(ci): add ci-security.yml + deny.toml + dependabot.yml`

### Task 6: Create `ci-release.yml`

**Files:**
- Create: `.github/workflows/ci-release.yml`

- [ ] **Step 6.1**: Write workflow with 4 jobs (build-cdylib matrix, build-jar, upload-release, release-notes)
- [ ] **Step 6.2**: Add `permissions: contents: write` for the upload-release job
- [ ] **Step 6.3**: Verify trigger restricted to `tags: ['v*']` (no push on branches)
- [ ] **Step 6.4**: Commit `feat(ci): add ci-release.yml (tag-driven multi-platform release)`

### Task 7 (optional): Final acceptance test

After all 6 workflows land:

- [ ] **Step 7.1**: Open a test PR with a no-op change to verify all push/PR workflows trigger correctly
- [ ] **Step 7.2**: Manually `workflow_dispatch` the nightly workflows once to verify they run
- [ ] **Step 7.3**: Manually create a `v0.0.0-rc1` tag and verify ci-release.yml runs (delete tag after)
- [ ] **Step 7.4**: Document outcomes in `reports/F1_gha_setup.md` (per A1 §5 row F deliverable)

---

## 3. Resolved Decisions (user 2026-04-30)

1. **Coverage gate**: hard floor at **80%** (`cargo llvm-cov --fail-under-lines 80`). "Larger is better" — gate ratchets up over time, never down. A1 §6 item 5 stretch target Rust ≥ 90% remains as Phase H acceptance gate (separate enforcement). Implementation: `ci-rust.yml` coverage job uses `--fail-under-lines 80`; gate may be raised in subsequent commits as coverage grows. Document trajectory in `reports/F1_gha_setup.md`.

2. **GH Pages**: ✅ enabled. Wire `ci-bench.yml` upload-perf-trend job to push criterion HTML to `gh-pages` branch under `criterion-reports/`. User confirms repo has GH Pages enabled.

3. **macOS arm64 runner availability for `ci-release.yml`**: GitHub `macos-14` arm64 runners verified available; proceed.

4. **JDK version**: ✅ **JDK 25.0.3** (specific patch version). Both `ci-java.yml` and `ci-e2e.yml` pin `actions/setup-java@v4` with `java-version: '25.0.3'`. JDK 25.0.3 is GA-released and stable; no `--enable-preview` flag required for FFM (FFM is finalized in JDK 22+).

5. **Codecov vs Coveralls**: Codecov only. Single uploader is enough; redundancy adds complexity.

---

## 4. Out of Scope (separate work)

- **Per-workflow YAML implementation**: this G1 is the design + plan. Actual `.yml` files for Tasks 1-6 are deferred to a follow-up session (each task is small but together they span ~500 lines of YAML and benefit from being applied in a clean execution session).
- **`reports/F1_gha_setup.md`**: per A1 §5 row F deliverables, this is produced after Task 7 acceptance — outcome doc, not part of this plan.
- **Wiring perf-trend dashboard onto GH Pages**: needs Task 4 + GH Pages enable; defer.
- **Java module structure (Maven layout)**: until Phase D commits C9 (FFI+Arrow+bench infra) lands the actual Java module, ci-java.yml stays a stub. The Java structure decision lives elsewhere.

---

## 5. Self-Review

**Spec coverage check (against A1 §5 row F + v2 §12.3):**

| v2 §12.3 workflow | G1 task |
|---|---|
| ci-rust.yml | Task 1 ✓ |
| ci-java.yml | Task 2 ✓ (stub) |
| ci-e2e.yml | Task 3 ✓ (stub) |
| ci-bench.yml | Task 4 ✓ |
| ci-security.yml | Task 5 ✓ |
| ci-release.yml | Task 6 ✓ |
| nexmark-baseline.yml | already landed in N2 Lane A Task 5 |

A1 §5 row F's "release dry-run executed once" → Task 7.3 (Lane B-style — needs user to push test tag).

**Gate alignment with A1 §6 (Definition of Done):**
- §6.4 "all 6 workflows green" → Tasks 1-6 land workflows; Task 7 verifies green
- §6.5 "Rust ≥ 90%" → Task 1 step 5.6 coverage gate (subject to ratcheting decision in §3.1)
- §6.5 "Java ≥ 80%" → ci-java.yml jacoco coverage (deferred to when Java module lands)

**Placeholder scan**: No "TBD"/"TODO" inside task steps. Open Decisions §3 are *flagged questions for user* before specific tasks execute, not placeholders.

**Plan size**: 6 implementation tasks (+1 acceptance) with ~25 steps total. Each step is 5-15 min (workflow YAML is verbose). Total: ~3 hours one-session execution OR ~1 hour with subagent parallelism (tasks 1-6 can largely run in parallel since they touch different files; Task 5 needs Task 1 finished first because it removes the `audit` job from `ci-rust.yml`).

Self-review pass: ✅.

---

## 6. Source References

- A1 spec §5 row F + §6 item 4-5 (DoD GHA + coverage)
- v2 §12.3 (workflow list)
- Existing `.github/workflows/ci.yml` (Rust baseline to extend)
- N2 Lane A Task 5 — `.github/workflows/nexmark-baseline.yml` (already landed, no overlap)
- `Cargo.toml:17–24` — workspace members (informs ci-rust matrix coverage)

---

## 7. Execution Note

Per writing-plans skill scope check: this plan covers 6 independent workflow files. Recommend execution mode: **subagent-driven, parallel where possible**:
- Task 1 first (ci-rust.yml; blocks Task 5 step 4 which removes the audit job)
- Tasks 2, 3, 4, 6 in parallel after Task 1 (different files)
- Task 5 last (depends on Task 1 having moved/created audit logic)
- Task 7 (acceptance) sequential after all 6 land

Open Decisions §3 must be resolved before Task 1 step 6 (coverage threshold) and Task 4 step 3 (GH Pages).

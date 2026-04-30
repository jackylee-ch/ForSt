# N1 — Nexmark Gate-Zero Research Report

**Status:** Research complete; path decision data-driven (option b: fork-community).
**Authority:** This research informs the Nexmark Gate-Zero plan (separate document).
**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` §4 @ 5b82b8d67.

---

## 0. Question Investigated

The A1 spec §4.2 prescribed three candidate paths for establishing a Nexmark baseline:
- **(a)** Mirror Flink's GHA Nexmark workflow into our repo
- **(b)** Fork the `nexmark/nexmark` community harness
- **(c)** Build from scratch

Research goal: determine which path is viable, by direct inspection of:
- `apache/flink` `.github/workflows/` (default + release-2.2 branches)
- `nexmark/nexmark` (canonical community harness)
- Other community Nexmark repos for streaming systems

---

## 1. Findings

### 1.1 Apache Flink GHA — no Nexmark workflow exists

`gh api repos/apache/flink/contents/.github/workflows --jq '.[].name'` returned 11 files:

```
ci.yml
community-review.sh / community-review.yml
docs-legacy.yml / docs.sh / docs.yml
nightly-trigger.yml / nightly.yml
stale.yml
template.flink-ci.yml / template.pre-compile-checks.yml
```

**None of these run Nexmark.** Code search `gh search code --repo apache/flink 'nexmark'` finds only:
- `flink-table/flink-sql-client/src/test/resources/nexmark.sql` — Nexmark queries used as a SQL test fixture (not a benchmark CI)
- `flink-table/flink-sql-client/src/test/java/.../SqlClientTest.java#testExecuteNexmark` — unit test that runs `nexmark.sql` through the SQL client (correctness check, not perf)
- `flink-table/flink-table-planner/.../MultiJoinTestPrograms.java` — comment referencing "a query from the nexmark benchmark" in a planner test

**Conclusion:** Flink upstream does NOT operate a Nexmark CI. Path (a) "mirror Flink GHA" is not viable — there is nothing to mirror.

Verified branches that DO exist on `apache/flink`:
- `release-2.0`, `release-2.1`, `release-2.2`, `release-2.2.1-rc1/rc2`, `master`
- (Note: `release-2.2.0` referenced in the original master prompt does not exist; the branch is `release-2.2`. Minor nomenclature correction.)

### 1.2 `nexmark/nexmark` — canonical community harness, actively maintained

`https://github.com/nexmark/nexmark` is the canonical Nexmark suite. Top-level structure:

```
GUIDE_V1.md / GUIDE_V2.md     — runner guides (V2 is the current runner, two-phase mode)
LICENSE / README.md
nexmark-flink/                — Flink-specific harness (Maven project)
nexmark-spark/                — Spark-specific harness
pom.xml                       — Maven aggregator
```

Recent commits on `master` (most recent first):

| Date | SHA | Message |
|---|---|---|
| 2025-12-26 | 6b3646c | [Flink] Fix unexpected job cancellation (#68) |
| 2025-12-25 | 8063de6 | [Flink] Add q23 for multi-way join query (#70) |
| 2025-03-02 | e115ef9 | [Flink] Guidelines and config template for new runner |
| 2025-03-02 | a0b99a8 | [Flink] Introduce maxEmitSpeed option in source |
| 2025-03-02 | bb5a991 | [Flink] Runner V2 with two-phase |

**Activity signal:** ~2 commits per month, all `[Flink]`-tagged, primarily Flink-runner improvements. Last commit 2025-12-26; 4 months stale as of 2026-04-30 but not abandoned.

**Query coverage:** q0–q22 (canonical 23 queries) per `README.md`, plus q23 (multi-way join) added Dec 2025. All queries marked Flink-supported except q6 (blocked on FLINK-19059).

### 1.3 `nexmark-flink` harness structure

```
nexmark-flink/build.sh       — single-line `mvn clean package -DskipTests` then tar
nexmark-flink/pom.xml        — Maven build descriptor
nexmark-flink/src/main/      — runner code (Java)
nexmark-flink/src/test/      — runner unit tests
```

`build.sh` produces `nexmark-flink.tgz` artifact for cluster deployment.

`GUIDE_V2.md` describes the deployment model (extracted relevant excerpts):

- **Requires Flink 2.0.0 or above** — our target (Flink 2.2) qualifies
- **Cluster topology**: 1 master + N workers (Linux, JDK 11+, passwordless SSH between nodes)
- **Workload script**: `nexmark/bin/run_query.sh all` runs all queries; `run_query.sh q1,q2` for specific
- **Output metric**: `Cores × Time(s)` per query — single composite metric (NOT separate p50/p99/throughput)
- **Min hardware**: 4 worker nodes × 16 cores × 32GB RAM × 100GB SSD + HDFS data nodes on 1Gbps LAN
- **Default config**: 8 TaskManagers per worker, each 1 slot

**Most importantly for ForSt-RS:**

> "Replace `flink/conf/config.yaml` by `nexmark/conf/config.yaml`. Remember to update [...] **Set a proper state backend via `state.backend.type`, e.g. `forst` or `rocksdb`.**"

The harness **explicitly supports `state.backend.type: forst`** (the original ForSt). This means our integration path is:
1. Establish baseline with `state.backend.type: forst` (original ForSt) → records baseline numbers
2. After ForSt-RS lands, run with `state.backend.type: forst-rs` (or however we surface it) → records candidate numbers
3. Compute speedup ratio; verify ≥30–40% E2E

### 1.4 Other Nexmark forks (rejected)

For completeness, surveyed alternative repos:
- `wuchong/flink-nexmark-benchmark` — Flink-targeted, last commit 2024-12-19. Stale relative to canonical. Reject.
- `risingwavelabs/nexmark-rs`, `nexmark-bench`, `nexmark-risingwave-1.0` — RisingWave-focused (Rust + Kafka). Different stack. Reject.
- `timeplus-io/nexmark`, `matthewbrookes/nexmark_timely_faster`, `ABDOLI-Hossein/NXM` — niche, low signal. Reject.

---

## 2. Decision

**Path (b) fork-community is the only viable path.** Specifically:

1. Vendor `nexmark/nexmark` at a pinned commit (recommended: `6b3646c` — 2025-12-26, latest stable) into this repo as either:
   - **Submodule** at `nexmark/` (lightweight; clone-time fetch)
   - **Vendored copy** under `nexmark/` (heavier; full source in our repo)

2. Build via `nexmark-flink/build.sh` to produce `nexmark-flink.tgz`.

3. Deploy onto **real cluster hardware** (4 worker minimum) with:
   - Flink 2.2.x (matches our target integration version)
   - Original ForSt as `state.backend.type: forst`
   - HDFS for checkpoint state (or S3 / OpenDAL via Flink's filesystem adapter)

4. Run `nexmark/bin/run_query.sh all` — produces `Cores × Time(s)` baseline per query.

5. Commit baseline to `.planning/nexmark/baseline.json` with provenance metadata (Flink version, ForSt version, hardware spec, run timestamps).

6. Sign-off via `.planning/nexmark/baseline-signoff.md` (user countersigns the numbers).

---

## 3. Critical Constraints Surfaced by Research

### 3.1 Hardware requirement is REAL, not docker-compose

The A1 spec §4.2 step 2 mentioned `docker-compose.yml`. The actual harness is **Maven + cluster**. There is no docker path in `nexmark/nexmark`. Two options:

- **(α)** Use the canonical cluster path (4 real or VM workers, HDFS). Slower setup; higher cost; produces canonical numbers.
- **(β)** Author a `docker-compose.yml` that brings up Flink mini-cluster + HDFS in containers. Lower cost; easier reproducibility; **risk: numbers may not match canonical Nexmark conventions**, which limits comparability with published Flink Nexmark results.

Recommendation: **(α) for the baseline pinned in `baseline.json`** (canonical comparability); **(β) for CI smoke runs** (fast feedback, won't lock the perf gate).

### 3.2 Single-metric output format

Nexmark records `Cores × Time(s)` — not p50/p99/throughput. The A1 spec §4.2 step 3 expects "p50/p99/throughput per query" — which the canonical Nexmark harness does NOT produce out of the box.

Two options:
- **(γ)** Adopt the canonical Cores×Time metric as-is. Simpler. Aligns with published Flink Nexmark comparisons.
- **(δ)** Patch the runner to emit p50/p99/throughput in addition to Cores×Time. Custom maintenance burden; risk of drift from upstream.

Recommendation: **(γ) Cores×Time for baseline**. Update A1 §4 metric language accordingly. The 30–40% speedup target is robust to either metric (a 30% Cores×Time reduction == a 30% throughput improvement at constant CPU spend).

### 3.3 Hardware-bound work is out of scope for this session

Establishing actual baseline numbers requires:
- Real or cloud cluster (4 workers minimum)
- HDFS or equivalent
- ~50 minutes per benchmark run × 5 runs minimum × multiple state-backend configurations
- User-side run (Claude has no hardware access)

**This is an explicit hand-off**: the research, vendoring, build automation, and harness configuration can land in this session; the actual baseline numbers must be produced by the user (or a CI agent with hardware) in a follow-up session.

---

## 4. Open Items / Risks

1. **Cluster availability**: User has not confirmed they have access to a 4-worker Linux cluster + HDFS. If not, options: provision cloud (AWS EMR / GCP Dataproc), OR adopt option (β) docker-compose harness, OR rent benchmark hardware. Decision needed before plan starts execution.

2. **State backend identifier for ForSt-RS**: After ForSt-RS lands, what string does Flink use for `state.backend.type`? Plan should specify this — currently undefined. Likely candidates: `forst-rs`, `forst_rs`, or extending the existing `forst` factory.

3. **Snapshot recency**: `nexmark/nexmark` last commit is 2025-12-26 (4 months stale). If a critical Flink 2.2 incompatibility emerges, may need to upstream a fix or fork-and-patch.

4. **q23 inclusion**: The recently-added q23 (multi-way join) is not in the canonical "q0–q22" set. Decision: include in baseline (gives more coverage) or pin to q0–q22 (matches historical comparisons). Recommendation: include.

---

## 5. Source References

- `apache/flink/.github/workflows/` (master) — verified empty of Nexmark CI
- `apache/flink` branch list — verified `release-2.2` exists; `release-2.2.0` does NOT (cosmetic correction to spec language)
- `nexmark/nexmark` — verified canonical, actively maintained (last 2025-12-26), supports `state.backend.type: forst`
- `nexmark/nexmark/nexmark-flink/build.sh` — verified Maven-only build, no docker
- `nexmark/nexmark/GUIDE_V2.md` — extracted hardware requirements + run commands
- `nexmark/nexmark/README.md` — extracted query catalog (q0–q22, q23 added)

---

## 6. Output

This research informs:
- **A1 spec §4** — three small corrections needed: (i) replace `release-2.2.0` with `release-2.2`; (ii) drop "docker-compose" assumption from §4.2 step 2 (real-cluster harness); (iii) replace "p50/p99/throughput" metric assumption in §4.2 step 3 with "Cores × Time per query"
- **N2_nexmark_plan.md** — implementation plan derived from this research (separate doc)

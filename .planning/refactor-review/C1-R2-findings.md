# C1 Round 2 Review — Aggregated Findings

**Round:** 2 / 120
**Reviewed against:** `crates/forst-rs-common/` at HEAD `c9805098a` (the C1 logical scope per `COMMIT_MANIFEST.md`)
**Date:** 2026-05-05
**Authoritative spec:** `.planning/refactor-review/A1_reconciliation.md` @ `33f85b1c5`
**Protocol:** `.planning/refactor-review/REVIEW_PROTOCOL.md` (with §B arch-pivot sub-loop)

## Protocol adaptation

The original protocol expected commit `68c0bd464` in worktree `~/code/github/ForSt-review/`; neither is present on this machine. R2 reviews `crates/forst-rs-common/` directly at HEAD. **Dimension 6 (Performance) is deferred** — C1 microbenches (varint/crc32c/arena) and the RocksDB v8.11.3 baseline are not local; perf-dimension review will run when those land. 9 of 10 dimensions executed.

## Severity tally (9/10 agents — A6 deferred)

| Agent | Dimension | H | M | L |
|-------|-----------|---|---|---|
| A1 | Memory safety | 0 | 0 | 0 |
| A2 | Correctness | 0 | 0 | 0 |
| A3 | Concurrency | **2** | 2 | 1 |
| A4 | Test coverage | **2** | 3 | 1 |
| A5 | Error handling | **2** | 1 | 1 |
| A6 | Performance | DEFERRED | DEFERRED | DEFERRED |
| A7 | Documentation | 0 | 0 | 0 |
| A8 | Idiomatic Rust | **1** | 5 | 2 |
| A9 | Security | 0 | 2 | 1 |
| A10 | Integration | 0 | 1 | 0 |
| **Raw totals** |  | **7** | 14 | 6 |

After Tech VP dedup (consolidated below): **H=4, M=11, L=5**.

R1 totals were 26/52/55 → R2 is ~4× lower across the board, expected since R1 caught the structural bulk.

---

## Tech VP consolidated H findings (4) with §B triage

### H#1 — Histogram::snapshot() reads non-atomic compound state
**Source:** A3-H1 (concurrency)
**Location:** `metrics.rs:260-268`
**Description:** `snapshot()` reads `count`, `sum`, `bucket_counts`, `overflow` as 4 independent `Relaxed` loads with no synchronization. Concurrent observers can observe non-linearizable states where `count=N` reflects one interleaving but `sum=S` reflects a different interleaving. The doc claim "updated atomically" is false at the snapshot level.

**§B triage:** **Architectural** — fixing snapshot consistency requires either (a) a seqlock / version-counter pattern around the four fields, (b) a coarse RwLock guarding the snapshot read, or (c) per-thread accumulators with periodic merge. None are tunable parameter changes; all are structural redesigns.

**Disposition this round:** **Defer** — design the pivot in a focused next-session iteration; per A1 §3.3.2 the round counter resets to 0 on every arch-pivot, so we want the pivot done deliberately, not jammed in alongside lower-cost fixes.

### H#2 — Histogram CAS sum loop allows permanent NaN poisoning
**Source:** A3-H2 (concurrency)
**Location:** `metrics.rs:199-211`
**Description:** CAS loop on `sum_bits` uses Relaxed on both success and failure. If `sum_bits` ever holds the NaN bit pattern (from corruption or untrusted f64 input), all future iterations read NaN, compute `NaN + value = NaN`, and write NaN back — the field becomes permanently corrupt with no recovery.

**§B triage:** **Architectural** — fixing requires either (a) input-validation reject NaN/Inf at `observe()` (semantic change to API), (b) replace f64 sum with i64 fixed-point (precision loss; semantic change), or (c) use a different reduction structure entirely. All are structural.

**Disposition this round:** **Defer** — coupled to H#1; both Histogram H findings should be solved together in one arch-pivot.

### H#3 — Arena `allocate` OOM boundary is untested
**Source:** A4-H1 (test coverage)
**Location:** `arena.rs:80, 161` (test gap)
**Description:** All Arena tests use small block sizes; no test exercises the OOM panic path of the underlying `vec![0u8; size]`. The existing R1 finding A5-H1 noted Arena lacks a `try_allocate` fallible variant; this finding is the test-side complement.

**§B triage:** **Tuning** — write the test; no architecture change. The fallible-variant addition itself (R1 A5-H1) is a separate API change deferred to the same session that addresses §B.2 NaN poisoning.

**Disposition this round:** **Fix** in this commit — add `#[should_panic]` test for OOM.

### H#4 — Arena `unwrap()` calls in `allocate` / `allocate_aligned`
**Source:** A5-H1 (error handling)
**Location:** `arena.rs:91, 110, 136`
**Description:** `if let Some(last) = self.blocks.last() { ... self.blocks.last_mut().unwrap() ... }` pattern. The unwrap is provably safe given the surrounding invariant, but bare `unwrap()` violates the "handle errors explicitly" rule and is fragile to refactor.

**§B triage:** **Tuning** — replace `unwrap()` with `expect("documented invariant")` at the 3 sites. The invariant remains identical; the change documents WHY it's safe.

**Disposition this round:** **Fix** in this commit.

---

## Tech VP consolidated M findings (11)

| # | Source | Location | Brief | Disposition |
|---|---|---|---|---|
| M#1 | A3-M1 | `metrics.rs:23, 152` | Snapshot atomicity claim in docs is misleading | **Fix-with-arch-pivot** (couples to H#1/H#2) |
| M#2 | A3-M2 + A4-H2 (merged) | `metrics.rs` tests | No concurrent observe()+snapshot() consistency test; NaN/Inf inputs to observe() not tested | **Fix-with-arch-pivot** |
| M#3 | A4-M1 | `coding.rs:121, 155` (test side) | Tests check `is_err()` but don't discriminate error variants (truncated vs overlong vs unterminated) | Defer (low risk; doesn't block convergence) |
| M#4 | A4-M2 | `config.rs:310` (test) | `CfOptions::effective_*` methods only tested in combined override scenario; missing fallback-only tests | Defer |
| M#5 | A4-M3 | `config.rs:356` (test) | `ReadOptions` field combinations untested (only default state tested) | Defer |
| M#6 | A8-H1 + A8-L1 (merged; downgraded to M) | `metrics.rs:492` (test) | Test accesses private `bounds` field; should expose getter or use public API | **Fix** in this commit (one-line test rewrite + add `bounds_len()` getter if needed) |
| M#7 | A8-M1 | `metrics.rs:551, 552` | clippy `const_is_empty` violations on static string assertions | **Fix** in this commit |
| M#8 | A8-M2 | `metrics.rs:263` | Unnecessary `Vec::clone()` in `Histogram::snapshot()` | Defer (minor; couples to arch-pivot anyway) |
| M#9 | A8-M5 | `config.rs:188` | Builder `db_path(impl Into<String>)` accepts `Into` but other setters take concrete types — inconsistent ergonomics | Defer |
| M#10 | A9-M1 | `arena.rs:129` | `let total = size + padding;` — unchecked addition; should use `checked_add` for defense-in-depth | **Fix** in this commit |
| M#11 | A10-M1 | `forst-rs-{io,storage,engine}` consumers | Non-canonical import paths (`use forst_rs_common::error::{...}` vs canonical `use forst_rs_common::{...}`) — defeats re-export design | Defer to consumers' Cn rounds |

**Downgrades from raw agent ratings:** A8-H1 (private-field test access) → M (test-only style; no production correctness impact). All others kept at agent's classification.

## Tech VP consolidated L findings (5)

- A3-L1 — Counter/Gauge Relaxed semantics OK, no action
- A4-L1 — `KeyRange::contains` doesn't test empty-slice boundary
- A5-L1 — `get_fixed` byte-by-byte access vs slice pattern
- A8-L2 — `ReadTier::ReadBoth` naming is awkward
- A9-L1 — Config types' `Debug` derive could leak future credential fields

## Inherited / dropped duplicates of R1

- A5-H2 (Arena `vec![]` OOM panic) — **duplicate of R1 A5-H1**; not double-counted.
- A5-M1 (coding error messages omit context) — **duplicate of R1 A5-M-4**; not double-counted.

---

## This round's actions

**Fixed in this round (4 H/M items):**
- H#3 — Arena OOM boundary test added
- H#4 — `unwrap()` → `expect("…")` at 3 Arena sites
- M#6 — Histogram test no longer accesses private `bounds`
- M#7 — clippy `const_is_empty` resolved
- M#10 — `arena.rs:129` `size + padding` uses `checked_add`

**Deferred to next-session arch-pivot (2 architectural H + 2 coupled M):**
- H#1 + H#2 + M#1 + M#2 — Histogram concurrency + observability + tests, all redesigned together as one arch-pivot per A1 §3.3.2

**Deferred to later Cn rounds (5 M + 5 L):**
- Test coverage gaps (M#3-M#5)
- Style/API ergonomics (M#8-M#9)
- Cross-crate import path cleanup (M#11) — best done during each consumer Cn
- All L items — gather over next several rounds

## Decision: continue / converge / escalate

- **Round count:** 2 / 120
- **Consecutive clean (H=0 ∧ M=0):** 0 / 10 needed to converge
- **Recommendation:** **continue** — apply the 4 fixes above (which produce a fix commit, not a counter reset); architectural pivots for Histogram remain queued for a focused pivot session

## Reviewer agent IDs (for traceability)

| Agent | ID |
|-------|---|
| A1 Memory safety | a6526bca3d5e7712f |
| A2 Correctness | a8b59dd142d4699a1 |
| A3 Concurrency | a3e7ca53bf0e8ad6e |
| A4 Test coverage | a6fc17ced24fdab8c |
| A5 Error handling | a1cde272063a56195 |
| A6 Performance | DEFERRED |
| A7 Documentation | aae9b5198f35c569b |
| A8 Idiomatic Rust | a78246f26fd486fc7 |
| A9 Security | a38e6a4390d2db7a9 |
| A10 Integration | ac940f1c364f1a891 |

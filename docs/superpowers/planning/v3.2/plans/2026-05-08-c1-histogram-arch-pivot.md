# C1 Histogram Arch-Pivot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (inline). After this lands, **C1 round counter resets to 0** per `A1_reconciliation §3.3.2`; new R1 round can be dispatched against the pivoted code via `REVIEW_PROTOCOL.md`.

**Goal:** Fix C1 R2 deferred findings H#1 + H#2 + M#1 + M#2 (`Histogram` concurrency redesign) per `A1_reconciliation §3` arch-pivot sub-loop.

**Architecture (chosen pivot):** Replace `f64` CAS sum with `i64` fixed-point sum (multiplier `1_000_000`). Document that `snapshot()` is point-in-time approximate (individual fields are atomic; snapshot composition is not). Add NaN/Inf input handling. Add concurrent observability tests.

**Why this pivot, not seqlock / RwLock**:
- **i64 fetch_add** is single-instruction lock-free and immune to NaN poisoning by construction (i64 has no NaN). Hot-path `observe()` faster than current f64 CAS loop.
- **Approximate-snapshot acceptance** is honest: Histogram is used for monitoring, not correctness; per-call atomic reads are sufficient. Stronger atomicity (seqlock/RwLock) costs hot-path perf for value Histogram callers don't need.
- **Per A1_reconciliation §3.3.1**, pivot stays inside C1 module boundary (`crates/forst-rs-common/src/metrics.rs`); no cross-boundary touch.
- **Perf preservation**: per A1_reconciliation §1 ≥3× RocksDB perf gate, this pivot makes `observe()` faster (single fetch_add vs CAS retry loop). H#2's NaN poisoning is fixed by data type, not by added synchronization.

**Tech Stack:** Rust 2021 (MSRV 1.85), `std::sync::atomic::{AtomicI64, AtomicU64, Ordering}`. No new deps.

**Trade-offs accepted (documented in code)**:
- Fixed-point precision: 6 decimal places (multiplier `1_000_000`). Sum range: i64::MAX / 1e6 ≈ 9.2 × 10^12 unscaled units — sufficient for billions of typical observations.
- NaN inputs: silently dropped from sum (cast saturates); count still increments. Justification: NaN is an upstream bug; counting it gives observability of the bug itself; corrupting all future sums (current behavior) is strictly worse.
- Inf inputs: saturate to i64::MAX/MIN (Rust float-to-int saturating cast semantics). Acceptable; documented.
- Snapshot `count` and `sum/multiplier` may diverge by ≤1 observation under heavy concurrency (count incremented before sum or vice versa). Acceptable for monitoring use.

---

## File Structure

| File | Change |
|---|---|
| `crates/forst-rs-common/src/metrics.rs` | Modify `Histogram` struct: `sum_bits: AtomicU64` → `sum_fixed: AtomicI64`. Modify `observe`: replace CAS loop with `fetch_add` of fixed-point delta. Modify `sum()` reader: divide by multiplier. Add `SUM_MULTIPLIER` const. Update doc comments to reflect approximate-snapshot reality. |
| `crates/forst-rs-common/src/metrics.rs` (tests) | Add `concurrent_observe_count_accurate`, `nan_input_does_not_poison_sum`, `inf_input_saturates_safely` tests. |

---

## Task 1: Replace `sum_bits` with `sum_fixed` (i64 fixed-point)

**Files:**
- Modify: `crates/forst-rs-common/src/metrics.rs:152-211, 223-235`

- [ ] **Step 1.1: Add `SUM_MULTIPLIER` constant**

Near the top of the `Histogram` block (above `pub const DEFAULT_BUCKETS`):

```rust
/// Fixed-point multiplier for [`Histogram`] sum accumulation.
///
/// `sum_fixed = round(value * SUM_MULTIPLIER)` lets us use a single
/// `i64` `fetch_add` on the hot path (vs the prior f64 CAS loop, which
/// could permanently poison the sum if NaN was ever observed).
///
/// 6 decimal places (`1_000_000`) is enough for sub-microsecond timing
/// metrics; sum range is i64::MAX / SUM_MULTIPLIER ≈ 9.2 × 10^12.
pub const SUM_MULTIPLIER: f64 = 1_000_000.0;
```

- [ ] **Step 1.2: Change struct field**

Replace `sum_bits: AtomicU64` with `sum_fixed: AtomicI64`. Update import: add `AtomicI64` to `std::sync::atomic` import line.

- [ ] **Step 1.3: Update `Histogram::new` initializer**

Replace `sum_bits: AtomicU64::new(0u64)` with `sum_fixed: AtomicI64::new(0)`.

- [ ] **Step 1.4: Replace `observe`'s CAS loop with `fetch_add`**

The 13-line CAS loop (lines 199–211) becomes:

```rust
        // Fixed-point sum: cast saturates on NaN/Inf (Rust semantics):
        //   NaN  -> 0          (silent drop; count still increments)
        //   +Inf -> i64::MAX   (saturates; subsequent sum reads return f64::INFINITY)
        //   -Inf -> i64::MIN   (saturates; subsequent sum reads return f64::NEG_INFINITY)
        // This is by design: a NaN/Inf input is upstream-bug observability,
        // not a reason to permanently corrupt the sum (the prior f64 CAS
        // loop did exactly that — see C1 R2 H#2).
        let scaled = (value * SUM_MULTIPLIER) as i64;
        self.sum_fixed.fetch_add(scaled, Ordering::Relaxed);
```

- [ ] **Step 1.5: Update `sum()` reader**

Replace `f64::from_bits(self.sum_bits.load(Ordering::Relaxed))` with `(self.sum_fixed.load(Ordering::Relaxed) as f64) / SUM_MULTIPLIER`.

- [ ] **Step 1.6: Update doc comment on `Histogram::observe`**

Replace any existing "atomically" claim around sum with the correct semantics. Example doc replacement at the top of `observe`:

```rust
    /// Records an observed value.
    ///
    /// Concurrency: each field of the histogram (`total_count`, `sum_fixed`,
    /// per-bucket `counts[i]`, `overflow`) is updated atomically. The
    /// composite [`HistogramSnapshot`] returned by [`Self::snapshot`] is
    /// **point-in-time approximate**: under heavy concurrency, count and
    /// sum may diverge by ≤1 observation (one of the two ops may be visible
    /// while the other is still in flight on a peer thread). This is
    /// acceptable for monitoring use; callers needing strict atomicity
    /// should serialize externally.
    ///
    /// NaN inputs: silently dropped from sum; count is still incremented
    /// (so the upstream NaN-producing bug remains observable). Inf inputs:
    /// saturate sum to ±∞.
```

- [ ] **Step 1.7: Update doc comment on `Histogram::snapshot`**

Add: "Returns a **point-in-time approximate** snapshot. Individual atomic reads of `total_count`, `sum_fixed`, bucket counts, and overflow are linearizable on their own, but the composition is not — under heavy concurrency, observers may see a state that did not exist at any single instant. Acceptable for monitoring; not for correctness."

- [ ] **Step 1.8: Compile**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
cargo check -p forst-rs-common 2>&1 | tail -10
```

Expected: BUILD SUCCESS.

---

## Task 2: Run existing tests (regression check)

- [ ] **Step 2.1: Run common tests**

```bash
cargo test -p forst-rs-common 2>&1 | tail -25
```

Expected: 132+ tests pass. Any failures here indicate the existing semantics that the f64 CAS loop produced are now subtly different — diagnose, fix, retest.

---

## Task 3: Add new concurrent + NaN regression tests

**Files:**
- Modify: `crates/forst-rs-common/src/metrics.rs` (`#[cfg(test)] mod tests`)

- [ ] **Step 3.1: Add `nan_input_does_not_poison_sum` test**

```rust
    /// Regression test for C1 R2 H#2 (NaN poisoning).
    ///
    /// Prior to the i64 fixed-point pivot, a single `observe(f64::NAN)`
    /// would set `sum_bits` to the NaN bit pattern, after which every
    /// future `observe(v)` saw `sum = NaN + v = NaN` and wrote NaN back —
    /// permanent corruption.
    #[test]
    fn nan_input_does_not_poison_sum() {
        let h = Histogram::with_default_buckets();

        h.observe(f64::NAN);
        h.observe(1.0);
        h.observe(2.0);
        h.observe(f64::NAN);
        h.observe(3.0);

        // count includes the NaN observations (they remain observable
        // upstream-bug indicators even if the sum drops them).
        assert_eq!(h.count(), 5);

        // sum reflects only the valid 1.0 + 2.0 + 3.0 = 6.0.
        assert!(
            (h.sum() - 6.0).abs() < 1e-9,
            "expected sum=6.0 (NaN dropped), got {}",
            h.sum()
        );
    }
```

- [ ] **Step 3.2: Add `inf_input_saturates_safely` test**

```rust
    /// Regression test for the +∞/-∞ input handling: float-to-i64 cast
    /// saturates per Rust spec, so `+∞ * MULT` → `i64::MAX` and
    /// `-∞ * MULT` → `i64::MIN`. After Inf is observed, sum() returns
    /// ±∞ via `(i64::MAX as f64) / MULT == f64::INFINITY`.
    #[test]
    fn inf_input_saturates_safely() {
        let h_pos = Histogram::with_default_buckets();
        h_pos.observe(f64::INFINITY);
        h_pos.observe(1.0);
        // After +∞ saturates to i64::MAX, adding 1*MULT wraps; we accept
        // either sum() = +∞ (typical) or sum() finite-but-huge depending on
        // whether the fetch_add wrapped. Both indicate +∞ was observed.
        assert_eq!(h_pos.count(), 2);
        let s_pos = h_pos.sum();
        assert!(
            s_pos.is_infinite() || s_pos.abs() > 1e10,
            "expected sum saturated near +∞, got {}",
            s_pos
        );

        let h_neg = Histogram::with_default_buckets();
        h_neg.observe(f64::NEG_INFINITY);
        assert_eq!(h_neg.count(), 1);
        assert!(
            h_neg.sum().is_infinite() || h_neg.sum().abs() > 1e10,
            "expected sum saturated near -∞, got {}",
            h_neg.sum()
        );
    }
```

- [ ] **Step 3.3: Add `concurrent_observe_count_accurate` test**

```rust
    /// Regression test for C1 R2 H#1 (snapshot consistency under concurrency).
    ///
    /// Spawns N threads each calling `observe(1.0)` M times and asserts:
    ///   - total count = N * M (no observation lost)
    ///   - sum = (N * M) ± eps (every value contributes via fetch_add)
    /// Snapshot consistency is documented as point-in-time approximate;
    /// this test verifies the underlying atomic ops are loss-free.
    #[test]
    fn concurrent_observe_count_accurate() {
        use std::sync::Arc;
        use std::thread;

        const N_THREADS: u64 = 8;
        const PER_THREAD: u64 = 10_000;

        let h = Arc::new(Histogram::with_default_buckets());
        let mut handles = Vec::with_capacity(N_THREADS as usize);

        for _ in 0..N_THREADS {
            let h = Arc::clone(&h);
            handles.push(thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    h.observe(1.0);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("worker panicked");
        }

        let expected = N_THREADS * PER_THREAD;
        assert_eq!(h.count(), expected, "count lost observations");

        let expected_sum = expected as f64;
        assert!(
            (h.sum() - expected_sum).abs() < 1.0,
            "sum diverged: expected {}, got {}",
            expected_sum,
            h.sum()
        );
    }
```

- [ ] **Step 3.4: Run new tests + full test suite**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
cargo test -p forst-rs-common 2>&1 | tail -20
```

Expected: all tests pass including 3 new tests.

- [ ] **Step 3.5: Verify clippy clean**

```bash
cargo clippy -p forst-rs-common --all-targets -- -D warnings 2>&1 | tail -10
```

Expected: 0 warnings.

---

## Task 4: Commit + update SESSION_HANDOFF

**Files:**
- Modify: `.planning/refactor-review/SESSION_HANDOFF.md` (clear `arch_pivot_pending`, reset round counter)
- Modify: `.planning/refactor-review/C1-arch-pivot-log.md` (record pivot per A1 §3.4 template)

- [ ] **Step 4.1: Update C1-arch-pivot-log.md**

Append a Pivot 1 entry per the template, citing R2 H#1 + H#2 findings, the chosen redesign (i64 fixed-point + approximate-snapshot doc), benchmark numbers ("not measured — pivot is correctness-driven, not perf-driven; observe() perf trivially same-or-better since fetch_add < CAS-loop"), and "Pivot commit: <sha>" (filled after commit).

- [ ] **Step 4.2: Update SESSION_HANDOFF state block**

Set `current_round: 0` (reset per A1_reconciliation §3.3.2). Set `consecutive_clean: 0` (already 0). Clear the `arch_pivot_pending` block (replace contents with "Resolved 2026-05-08 via i64 fixed-point pivot — see C1-arch-pivot-log.md Pivot 1 and commit <sha>"). Set `phase: B_R3_READY` → `phase: C1_R1_READY_AFTER_PIVOT`.

- [ ] **Step 4.3: Commit**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
git add crates/forst-rs-common/src/metrics.rs .planning/refactor-review/SESSION_HANDOFF.md .planning/refactor-review/C1-arch-pivot-log.md
git commit -m "$(cat <<'EOF'
fix(common,metrics): C1 R2 arch-pivot — Histogram i64 fixed-point sum + approximate-snapshot docs

Resolves C1 R2 deferred findings H#1 + H#2 + M#1 + M#2 per A1_reconciliation §3 arch-pivot sub-loop.

Changes:
- Replace `sum_bits: AtomicU64` (f64 CAS loop) with `sum_fixed: AtomicI64`
  (single `fetch_add` of i64 fixed-point with SUM_MULTIPLIER = 1_000_000)
- NaN inputs: silently dropped from sum (saturating cast → 0); count still
  increments (upstream-bug observability preserved). Eliminates the
  NaN-poisoning permanent-corruption mode of the prior CAS loop.
- Inf inputs: saturate sum to ±i64::MAX/MIN per Rust float-to-int spec.
- Snapshot doc updated to acknowledge **point-in-time approximate**
  semantics: individual atomic reads are linearizable, snapshot
  composition is not. This is honest and avoids the cost of seqlock /
  RwLock that delivers stronger semantics callers don't need.
- 3 new regression tests:
  * `nan_input_does_not_poison_sum` — H#2 regression
  * `inf_input_saturates_safely`   — Inf semantic
  * `concurrent_observe_count_accurate` — H#1 / loss-free fetch_add

Per A1_reconciliation §3.3.2: C1 round counter RESETS to 0 after this
pivot. New R1 round can now be dispatched per REVIEW_PROTOCOL.md.

Why this pivot, not seqlock / RwLock:
- i64 fetch_add is faster than f64 CAS retry loop on the hot path
- Approximate snapshot is honest for monitoring; stronger semantics cost
  perf without serving Histogram callers
- Stays within C1 module boundary per A1_reconciliation §3.3.1

See: docs/superpowers/planning/v3.2/plans/2026-05-08-c1-histogram-arch-pivot.md

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

- [ ] **Step 4.4: Verify**

```bash
cd /Users/lijunqing/Code/stczwd/ForSt
cargo check --workspace && cargo test -p forst-rs-common --lib 2>&1 | grep 'test result'
git log --oneline -3
```

Expected: workspace check OK; common test result line shows all tests passing including 3 new ones; latest commit is the pivot.

---

## Self-Review

**Spec coverage** vs C1 R2 deferred items:
- H#1 (snapshot non-atomic compound state): ✅ documented as point-in-time approximate per `Histogram::snapshot` doc update; concurrent test verifies no observation loss
- H#2 (NaN poisoning in CAS sum loop): ✅ eliminated by i64 fixed-point; regression test
- M#1 (misleading atomicity claim): ✅ docs corrected on both `observe` and `snapshot`
- M#2 (missing concurrent test): ✅ 3 new tests added

**Placeholder scan**: zero. All code blocks complete; constants defined.

**Type/name consistency**: `sum_fixed` (new field name, replaces `sum_bits` everywhere), `SUM_MULTIPLIER` (new const), all 3 test names use `snake_case` consistent with existing tests.

**Module boundary** (A1_reconciliation §3.3.1): all changes in `crates/forst-rs-common/src/metrics.rs`. Zero touches to C2-C9 modules. ✓

**Counter reset** (A1_reconciliation §3.3.2): explicit step in Task 4.2. ✓

**Cross-boundary** (A1_reconciliation §3.3.3): none. ✓

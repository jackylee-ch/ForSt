// Copyright 2026 The ForSt-RS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! MVCC Snapshot type + SnapshotRegistry.
//!
//! Per spec §6a.2: a [`Snapshot`] captures `(seq, db_id, captured_at)` plus
//! an `Arc` to the issuing [`SnapshotRegistry`]. Drop releases the ref-count
//! back to the registry so compaction's `min_active` query advances.
//!
//! The registry stores active sequences in a `Mutex<BTreeMap<u64,
//! RegistryEntry>>` (BTreeMap because compaction needs the *minimum* live
//! seq, which is the first key). For the hot-path `min_active` read taken
//! by every compaction iteration, a `cached_min: AtomicU64` mirrors the
//! BTreeMap's first key without taking the lock — see `recompute_cached_min`
//! for the invariant. Stale reads are SAFER than truth (older seq → retain
//! more versions → never drop too aggressively); see spec §6a.2 inline
//! comment on `cached_min`.
//!
//! `oldest_age_ms` walks the BTreeMap and returns the maximum
//! `captured_at.elapsed().as_millis()` (B-Prod-P0 Task 0.5; spec §6a.3).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use forst_rs_common::types::SequenceNumber;

/// Default warn-line for long-lived snapshots, in milliseconds. Per spec
/// §6a.3 — 5 minutes balances "alert before the operator falls asleep"
/// against "don't spam logs for a normal-length checkpoint".
pub const DEFAULT_MAX_AGE_MS: u64 = 5 * 60 * 1_000;

/// Operator runbook hint embedded in [`SnapshotAgeWarning`]'s `Display`
/// impl so a log line is self-contained. Centralized as a constant so
/// any docs that reference the hint string can stay in lock-step.
pub const SNAPSHOT_AGE_HINT: &str = "long-lived snapshot is pinning versions \
    — check the operator runbook (spec §6a.3) for safe-release procedure; \
    auto-release is intentionally disabled because dropping a live snapshot \
    silently breaks the reader's correctness contract";

/// Diagnostic returned by [`SnapshotRegistry::check_long_lived`] when any
/// pinned snapshot has been alive longer than the configured `max_age_ms`.
///
/// Carrying the seq + DB id makes the warning actionable — an operator
/// reading the log line can grep their own audit trail for the same `seq`
/// to find the call site that captured the snapshot but never released
/// it. The `Display` impl produces the exact text emitted by the engine's
/// periodic check; consumers SHOULD NOT format it themselves so the wire
/// format stays stable across releases.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotAgeWarning {
    /// Sequence number of the offending snapshot.
    pub seq: SequenceNumber,
    /// DB id the snapshot was captured against (spec §15 "Same-DB").
    pub db_id: DbId,
    /// Wall-clock milliseconds since the snapshot was captured. Always
    /// `> max_age_ms` when this struct is constructed; we surface it
    /// rather than letting callers re-derive it so the warn line reports
    /// the same value the threshold check used.
    pub age_ms: u64,
    /// The threshold that was exceeded. Included so a log aggregator
    /// can correlate the warn against any subsequent `set_max_age_ms`
    /// reconfiguration without having to query the engine.
    pub max_age_ms: u64,
    /// Static hint pointing operators at the runbook. Stored as `&'static
    /// str` because [`SNAPSHOT_AGE_HINT`] is the only producer today and
    /// callers should never customize it (the message is a stable docs
    /// landmark).
    pub hint: &'static str,
}

impl fmt::Display for SnapshotAgeWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "snapshot age exceeds max_age_ms: seq={}, db_id={}, \
             age_ms={}, max_age_ms={}; hint: {}",
            self.seq.value(),
            self.db_id.0,
            self.age_ms,
            self.max_age_ms,
            self.hint
        )
    }
}

/// Opaque DB instance identifier.
///
/// Bound into every [`Snapshot`] at capture time so the FFI release path
/// can reject cross-DB releases (spec §15 "Same-DB" invariant). The engine
/// chooses how to mint these — typically a process-monotonic `AtomicU64`
/// owned by the `DbImpl`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DbId(pub u64);

/// Per-sequence state stored in the registry's `BTreeMap`.
struct RegistryEntry {
    /// Number of live `Snapshot` handles pinned at this `seq`. Multiple
    /// captures at the same seq (e.g. concurrent readers between two
    /// writes) share an entry; see `duplicate_seq_refcounted` test.
    ref_count: AtomicUsize,
    /// Wall-clock time of the *first* capture at this seq. Subsequent
    /// captures at the same seq do NOT refresh this — the oldest pin
    /// is what matters for `oldest_age_ms` accounting.
    captured_at: Instant,
    /// DB id of the *first* capture at this seq. In practice every
    /// capture against a given registry uses the same `DbId` (the
    /// registry is owned by one [`crate::DbImpl`]), so this is always
    /// the issuing DB's id; storing it here lets
    /// [`SnapshotRegistry::check_long_lived`] surface it without
    /// needing a backref from the entry to the caller.
    db_id: DbId,
}

/// RAII snapshot handle. `Drop` releases the ref-count back to the issuing
/// registry. Per spec §6a.2.
///
/// Not `Clone` by design: each snapshot is a distinct ref-count slot, so
/// duplicating one would underflow the count when the duplicate dropped.
/// To share, wrap in `Arc<Snapshot>`.
pub struct Snapshot {
    seq: SequenceNumber,
    db_id: DbId,
    captured_at: Instant,
    registry: Arc<SnapshotRegistry>,
}

impl Snapshot {
    /// Returns the snapshot sequence number.
    #[inline]
    pub fn seq(&self) -> SequenceNumber {
        self.seq
    }

    /// Returns the DB instance this snapshot was captured against. The FFI
    /// release path checks this against the calling `DbImpl` to enforce
    /// the spec §15 "Same-DB" invariant.
    #[inline]
    pub fn db_id(&self) -> DbId {
        self.db_id
    }

    /// Returns wall-clock milliseconds since this snapshot was captured.
    /// Used by the operator-facing `forst.snapshot.oldest_age_ms` gauge
    /// (spec §6a.3) and any per-snapshot age log lines.
    #[inline]
    pub fn age_ms(&self) -> u64 {
        // `as u64` saturates at u64::MAX, but elapsed() in millis would
        // need ~584M years to overflow — not a real concern.
        self.captured_at.elapsed().as_millis() as u64
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.registry.release_internal(self.seq);
    }
}

/// Tracks the set of live snapshot sequence numbers; see module docs.
pub struct SnapshotRegistry {
    /// Per-seq ref-count map. BTreeMap so the first key is the live
    /// minimum (compaction's `min_active_snapshot`).
    active: Mutex<BTreeMap<SequenceNumber, RegistryEntry>>,
    /// Lock-free mirror of `active.keys().next()`, or `u64::MAX` when
    /// empty. Read by every compaction iteration; updated under the
    /// `active` lock on every capture/release transition that changes
    /// the minimum. Stale reads are SAFER than truth — see module docs.
    cached_min: AtomicU64,
    /// Warn-line threshold (ms) consulted by [`Self::check_long_lived`].
    /// Defaults to [`DEFAULT_MAX_AGE_MS`] (5 minutes per spec §6a.3).
    /// `Relaxed` ordering is fine — the threshold is read on a slow
    /// operator-facing path; transient staleness after a `set_max_age_ms`
    /// call is acceptable (the next periodic tick picks up the new
    /// value).
    max_age_ms: AtomicU64,
}

impl SnapshotRegistry {
    /// Constructs an empty registry. Returned as `Arc<Self>` because every
    /// `Snapshot` carries an `Arc` back to its issuer for the `Drop`-time
    /// release.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(BTreeMap::new()),
            cached_min: AtomicU64::new(u64::MAX),
            max_age_ms: AtomicU64::new(DEFAULT_MAX_AGE_MS),
        })
    }

    /// Captures a snapshot at `current_seq` against `db_id`. The returned
    /// [`Snapshot`] holds an `Arc` to this registry; dropping it releases
    /// the ref-count.
    ///
    /// Per spec §13 ("dbSnapshot() < 100 µs P99"): hot path is one
    /// `Mutex` acquire + one BTreeMap lookup; no allocation on the
    /// duplicate-seq path.
    pub fn capture(self: &Arc<Self>, db_id: DbId, current_seq: SequenceNumber) -> Snapshot {
        let now = Instant::now();
        {
            let mut guard = self.active.lock().expect("SnapshotRegistry mutex poisoned");
            match guard.get(&current_seq) {
                Some(entry) => {
                    // Existing pin at this seq — bump ref-count, leave
                    // the original captured_at alone (oldest pin wins).
                    entry.ref_count.fetch_add(1, Ordering::Relaxed);
                }
                None => {
                    guard.insert(
                        current_seq,
                        RegistryEntry {
                            ref_count: AtomicUsize::new(1),
                            captured_at: now,
                            db_id,
                        },
                    );
                }
            }
            // Recompute under the same lock so cached_min is consistent
            // with the BTreeMap state any concurrent compaction could
            // observe via min_active().
            self.recompute_cached_min(&guard);
        }

        Snapshot {
            seq: current_seq,
            db_id,
            captured_at: now,
            registry: Arc::clone(self),
        }
    }

    /// Releases one ref-count at `seq`. Intended to be called from
    /// [`Snapshot::drop`]; see the inline `Drop` impl. Public so FFI /
    /// engine integration tests that don't go through `Snapshot` (e.g.
    /// the C-shim same-DB validation path in spec §15) can drive
    /// release explicitly.
    ///
    /// No-op if `seq` is not in the registry — e.g. because the entry
    /// was already removed by another concurrent release of the last
    /// pin. Underflowing the ref-count is treated as a programmer error
    /// and panics in debug builds.
    pub fn release_internal(&self, seq: SequenceNumber) {
        let mut guard = self.active.lock().expect("SnapshotRegistry mutex poisoned");
        let remove = match guard.get(&seq) {
            Some(entry) => {
                let prev = entry.ref_count.fetch_sub(1, Ordering::Relaxed);
                debug_assert!(
                    prev > 0,
                    "SnapshotRegistry::release_internal underflow at seq={}",
                    seq
                );
                prev == 1
            }
            None => {
                debug_assert!(
                    false,
                    "SnapshotRegistry::release_internal called for unknown seq={}",
                    seq
                );
                false
            }
        };
        if remove {
            guard.remove(&seq);
            self.recompute_cached_min(&guard);
        }
        // If we did NOT remove (still pinned), the minimum cannot have
        // changed — no recompute needed.
    }

    /// Returns the minimum live snapshot sequence number, or
    /// `SequenceNumber(u64::MAX)` when the registry is empty.
    ///
    /// Hot path for compaction: a single relaxed atomic load. No lock.
    /// May be stale on the safer side (older than truth → compaction
    /// retains *more* versions than strictly needed); see module docs.
    pub fn min_active(&self) -> SequenceNumber {
        SequenceNumber(self.cached_min.load(Ordering::Acquire))
    }

    /// Returns the number of *distinct seqs* currently pinned. Multiple
    /// snapshots at the same seq count once (they share a BTreeMap
    /// entry). For total live-handle count, sum the ref-counts; not
    /// exposed here because no caller in the spec needs it.
    pub fn active_count(&self) -> usize {
        self.active
            .lock()
            .expect("SnapshotRegistry mutex poisoned")
            .len()
    }

    /// Maximum `captured_at.elapsed().as_millis()` across all live
    /// snapshots, or 0 when empty.
    ///
    /// Per spec §6a.3 `forst.snapshot.oldest_age_ms` gauge. Walks
    /// `active` under the lock; cost is O(n) in live distinct seqs.
    /// Operator-facing metric — not on the compaction hot path.
    pub fn oldest_age_ms(&self) -> u64 {
        let g = self.active.lock().expect("SnapshotRegistry mutex poisoned");
        g.values()
            .map(|e| e.captured_at.elapsed().as_millis())
            .max()
            .map(|m| u64::try_from(m).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }

    /// Returns an estimate of bytes retained because of active snapshots.
    /// The closure is invoked with the current min_active seq and is expected
    /// to return the bytes pinned by versions with seq >= min_active that
    /// have a newer version superseding them. Returns 0 when no snapshots.
    ///
    /// Implementation note: the registry doesn't know the data layout, so
    /// the actual byte estimation is delegated to the caller (memtable +
    /// SST scan). Per spec §6a.3, this is the metric `forst.snapshot.pinned_bytes`.
    pub fn pinned_bytes_estimate<F: FnOnce(SequenceNumber) -> u64>(&self, walker: F) -> u64 {
        let min = self.min_active();
        if min.0 == u64::MAX {
            return 0;
        }
        walker(min)
    }

    /// Sets the warn-line threshold (ms) consulted by
    /// [`Self::check_long_lived`]. Pass `0` to effectively disable the
    /// warn-line (every check returns `Some(...)` immediately because
    /// any positive age exceeds `0`); callers that want to disable the
    /// check should simply stop calling `check_long_lived` instead.
    ///
    /// Threshold updates are not retroactive: a snapshot that has
    /// already aged past the OLD threshold and been warned once will
    /// only re-warn after a subsequent check at the new threshold;
    /// emission cadence is owned by the caller (the registry stays
    /// stateless wrt. warn-rate-limiting).
    pub fn set_max_age_ms(&self, ms: u64) {
        // `Relaxed`: see field-level doc on `max_age_ms`.
        self.max_age_ms.store(ms, Ordering::Relaxed);
    }

    /// Returns the current warn-line threshold in milliseconds.
    pub fn max_age_ms(&self) -> u64 {
        self.max_age_ms.load(Ordering::Relaxed)
    }

    /// Returns `Some(warning)` when at least one pinned snapshot has been
    /// alive longer than the configured `max_age_ms`. The returned
    /// warning references the OLDEST offending snapshot (BTreeMap order
    /// is by seq, which is the order snapshots were captured under
    /// monotonic-seq writes; the first hit at the lowest seq is the
    /// most-aged in practice). Returns `None` when the registry is
    /// empty OR no snapshot has exceeded the threshold.
    ///
    /// Behavior: WARN-ONLY. Per spec §6a.3 the registry NEVER auto-
    /// releases a snapshot — doing so would silently break the reader's
    /// correctness contract (a `Snapshot` pinned at seq S guarantees
    /// every version visible at S remains readable for the snapshot's
    /// lifetime; auto-releasing would let compaction reclaim those
    /// versions while a Java-side reader still holds the handle). The
    /// fix is operator action (find the leaker, release it) — this
    /// method just surfaces enough context to drive that action.
    ///
    /// Cost: O(n) walk over live distinct seqs under the `active` lock.
    /// Operator-facing path — not on the compaction hot path. Callers
    /// should rate-limit invocations (e.g. once per second on a
    /// background tick), NOT call this from every read or write.
    pub fn check_long_lived(&self) -> Option<SnapshotAgeWarning> {
        let max = self.max_age_ms.load(Ordering::Relaxed);
        let guard = self.active.lock().expect("SnapshotRegistry mutex poisoned");
        // Walk in BTreeMap (seq-ascending) order so we return the first
        // (oldest) hit deterministically.
        for (seq, entry) in guard.iter() {
            let age = u64::try_from(entry.captured_at.elapsed().as_millis()).unwrap_or(u64::MAX);
            if age > max {
                return Some(SnapshotAgeWarning {
                    seq: *seq,
                    db_id: entry.db_id,
                    age_ms: age,
                    max_age_ms: max,
                    hint: SNAPSHOT_AGE_HINT,
                });
            }
        }
        None
    }

    /// Recomputes `cached_min` from the current `active` map. Called
    /// under the `active` lock so the cached value is consistent with
    /// any state a concurrent reader might observe via `min_active`.
    fn recompute_cached_min(&self, guard: &BTreeMap<SequenceNumber, RegistryEntry>) {
        let next = guard
            .keys()
            .next()
            .copied()
            .map(|s| s.value())
            .unwrap_or(u64::MAX);
        // Release ordering pairs with the Acquire load in min_active().
        self.cached_min.store(next, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn seq(v: u64) -> SequenceNumber {
        SequenceNumber::new(v)
    }

    fn db_id() -> DbId {
        DbId(1)
    }

    /// Test 1: capture two snapshots → active_count = 2; drop one → 1.
    #[test]
    fn capture_increments_count() {
        let reg = SnapshotRegistry::new();
        let s1 = reg.capture(DbId(0), seq(10));
        let s2 = reg.capture(DbId(0), seq(20));
        assert_eq!(reg.active_count(), 2);
        drop(s1);
        assert_eq!(reg.active_count(), 1);
        drop(s2);
        assert_eq!(reg.active_count(), 0);
    }

    /// Test 2: empty → u64::MAX; new lower seq lowers the min; releases
    /// re-raise it; final release returns to u64::MAX.
    #[test]
    fn min_active_tracks_lowest() {
        let reg = SnapshotRegistry::new();
        assert_eq!(reg.min_active(), SequenceNumber(u64::MAX));
        let s50 = reg.capture(DbId(0), seq(50));
        assert_eq!(reg.min_active(), seq(50));
        let s20 = reg.capture(DbId(0), seq(20));
        assert_eq!(reg.min_active(), seq(20));
        drop(s20);
        assert_eq!(reg.min_active(), seq(50));
        drop(s50);
        assert_eq!(reg.min_active(), SequenceNumber(u64::MAX));
    }

    /// Test 3: db_id round-trips through Snapshot.
    #[test]
    fn snapshot_carries_db_id() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(42), seq(7));
        assert_eq!(snap.db_id(), DbId(42));
        assert_eq!(snap.seq(), seq(7));
    }

    /// Test 4: age_ms advances with wall-clock. 20ms is a generous floor
    /// — even on a loaded CI runner the sleep + measurement should clear
    /// 20ms comfortably.
    #[test]
    fn snapshot_age_advances() {
        let reg = SnapshotRegistry::new();
        let snap = reg.capture(DbId(0), seq(1));
        thread::sleep(Duration::from_millis(20));
        assert!(
            snap.age_ms() >= 20,
            "snapshot age should be >= 20ms after sleep, got {}",
            snap.age_ms()
        );
    }

    #[test]
    fn oldest_age_ms_tracks_oldest() {
        let reg = SnapshotRegistry::new();
        assert_eq!(reg.oldest_age_ms(), 0);
        let s1 = reg.capture(db_id(), SequenceNumber::new(1));
        std::thread::sleep(std::time::Duration::from_millis(30));
        let s2 = reg.capture(db_id(), SequenceNumber::new(2));
        let age = reg.oldest_age_ms();
        assert!(age >= 30, "oldest_age_ms = {}", age);
        drop(s1);
        let age2 = reg.oldest_age_ms();
        assert!(
            age2 < age,
            "after dropping s1, oldest should be the younger s2 (age2={}, age={})",
            age2,
            age
        );
        drop(s2);
        assert_eq!(reg.oldest_age_ms(), 0);
    }

    /// Test 5: two captures at the same seq share a ref-count entry;
    /// dropping one keeps the seq pinned; dropping the other empties.
    #[test]
    fn duplicate_seq_refcounted() {
        let reg = SnapshotRegistry::new();
        let s_a = reg.capture(DbId(0), seq(7));
        let s_b = reg.capture(DbId(0), seq(7));
        // Distinct snapshots, same seq → one BTreeMap entry.
        assert_eq!(reg.active_count(), 1);
        assert_eq!(reg.min_active(), seq(7));
        drop(s_a);
        // Still pinned at seq=7 by s_b.
        assert_eq!(reg.active_count(), 1);
        assert_eq!(reg.min_active(), seq(7));
        drop(s_b);
        assert_eq!(reg.active_count(), 0);
        assert_eq!(reg.min_active(), SequenceNumber(u64::MAX));
    }

    #[test]
    fn pinned_bytes_estimate_returns_zero_when_no_snapshots() {
        let reg = SnapshotRegistry::new();
        let estimate = reg.pinned_bytes_estimate(|_min_seq| 0);
        assert_eq!(estimate, 0);
    }

    /// Followup 1 (spec §6a.3): under-threshold returns None.
    #[test]
    fn check_long_lived_under_threshold_returns_none() {
        let reg = SnapshotRegistry::new();
        // Default warn-line is 5 minutes; a freshly-captured snapshot
        // is well under that — even a slow CI runner won't burn 5
        // minutes between the capture and this call.
        let _snap = reg.capture(DbId(7), seq(1));
        assert_eq!(reg.check_long_lived(), None);
    }

    /// Followup 1 (spec §6a.3): over-threshold returns Some with the
    /// right fields, and Display contains the operator-runbook hint.
    #[test]
    fn check_long_lived_over_threshold_returns_warning() {
        let reg = SnapshotRegistry::new();
        // Set a small threshold so the test doesn't have to sleep
        // minutes — 5 ms is comfortably above timer noise on every
        // supported platform.
        reg.set_max_age_ms(5);
        let _snap = reg.capture(DbId(42), seq(99));
        thread::sleep(Duration::from_millis(20));
        let warn = reg.check_long_lived().expect("should warn after sleep");
        assert_eq!(warn.seq, seq(99));
        assert_eq!(warn.db_id, DbId(42));
        assert!(
            warn.age_ms >= 20,
            "age_ms should be at least the sleep duration, got {}",
            warn.age_ms
        );
        assert_eq!(warn.max_age_ms, 5);
        assert_eq!(warn.hint, SNAPSHOT_AGE_HINT);
        // Sanity-check the Display impl includes the seq, db_id, and
        // hint so a log scraper finds the expected landmarks.
        let rendered = warn.to_string();
        assert!(
            rendered.contains("seq=99"),
            "Display missing seq: {}",
            rendered
        );
        assert!(
            rendered.contains("db_id=42"),
            "Display missing db_id: {}",
            rendered
        );
        assert!(
            rendered.contains("hint:"),
            "Display missing hint label: {}",
            rendered
        );
    }

    /// Followup 1 (spec §6a.3): the setter changes the threshold
    /// dynamically and subsequent checks honor the new value.
    #[test]
    fn check_long_lived_threshold_is_dynamic() {
        let reg = SnapshotRegistry::new();
        // Start with the warn-line WAY above the test's sleep budget;
        // a 1-hour threshold means check_long_lived returns None even
        // after a substantial sleep.
        reg.set_max_age_ms(60 * 60 * 1_000);
        assert_eq!(reg.max_age_ms(), 60 * 60 * 1_000);
        let _snap = reg.capture(DbId(0), seq(123));
        thread::sleep(Duration::from_millis(10));
        assert_eq!(
            reg.check_long_lived(),
            None,
            "should NOT warn under 1h threshold"
        );

        // Dial the threshold down — the same snapshot is now over
        // the limit.
        reg.set_max_age_ms(1);
        assert_eq!(reg.max_age_ms(), 1);
        let warn = reg
            .check_long_lived()
            .expect("should warn after lowering threshold");
        assert_eq!(warn.seq, seq(123));
        assert_eq!(warn.max_age_ms, 1);
    }

    #[test]
    fn pinned_bytes_estimate_invokes_callback_with_min() {
        let reg = SnapshotRegistry::new();
        let _s = reg.capture(db_id(), SequenceNumber::new(42));
        let received = std::sync::Mutex::new(SequenceNumber::new(0));
        let _ = reg.pinned_bytes_estimate(|min| {
            *received.lock().unwrap() = min;
            12345
        });
        assert_eq!(received.lock().unwrap().0, 42);
    }
}

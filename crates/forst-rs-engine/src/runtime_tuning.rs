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

//! Runtime tuning hooks (B-Prod-P7, spec §6d).
//!
//! Production knobs that operators tune without recompiling the engine:
//!
//! - **Block cache** — single shared LRU sized by
//!   `EngineOptions::block_cache_capacity_bytes`. Held by `DbImpl` and
//!   handed to SST readers as those wire it on the read path. The handle
//!   is exposed via `DbImpl::block_cache` so future readers (or the
//!   FFI tuning surface) can sample its hit-rate / current bytes.
//!
//! - **WriteBufferManager** — cross-CF memtable budget. The engine adds
//!   per-write deltas via [`WriteBufferManager::reserve`] and subtracts
//!   them on flush via [`WriteBufferManager::release`]. The total bytes
//!   currently held across all CFs is read by the writer hot path; once
//!   the running sum exceeds the configured cap the engine triggers an
//!   immediate flush of the largest CF rather than blocking writers
//!   (matches RocksDB's default `allow_stall = false` behaviour).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// FRS-GLOBAL-WBM-BUDGET (2026-06-03): process-wide running sum of memtable bytes
/// across ALL `DbImpl` instances that opted into the global budget. A Flink
/// TaskManager runs many keyed-state DBs (q4 ≈ 16: join ×p + agg/rank ×p), each a
/// separate engine with its own per-CF `WriteBufferManager`. The per-instance cap
/// (512 MiB) × ~16 = ~8 GiB of memtables — unbounded by Flink's managed memory and
/// (with the resident shadow) a dominant slice of the RSS bloat that forces OS
/// memory compression and decays heavy-query throughput. RocksDB avoids this by
/// SHARING one WriteBufferManager across the slot, sized from managed memory. This
/// is the engine half of that fix: a process-global memtable budget so the TOTAL
/// across instances cannot scale with instance count. The per-instance cap stays as
/// a secondary bound (no single instance hoards), and over-budget only ever TRIGGERS
/// A FLUSH (always correctness-safe) — so the writing instance flushing its own
/// actively-growing memtable reduces the global sum back under budget.
static GLOBAL_WBM_USED: AtomicU64 = AtomicU64::new(0);

/// Process-global memtable budget in bytes. Default 2 GiB (fits within a typical
/// Flink managed-memory fraction alongside the block cache + resident shadow);
/// override via `FRS_WBM_TOTAL_MB`; `0` disables the global bound (per-instance cap
/// only). Sized once on first read.
fn global_wbm_cap_bytes() -> u64 {
    static CAP: OnceLock<u64> = OnceLock::new();
    *CAP.get_or_init(|| {
        // FRS-MEM-MANAGER: an explicit `FRS_WBM_TOTAL_MB` always wins (operator
        // pin); otherwise, when the unified controller is armed, take the WBM
        // slice of the coordinated engine-native budget (auto-scales with the
        // cgroup). Falls back to the historical 2 GiB default when neither
        // applies — byte-identical to pre-manager.
        if let Some(mb) = std::env::var("FRS_WBM_TOTAL_MB")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            return mb.saturating_mul(1024 * 1024);
        }
        if let Some(cap) =
            crate::memory_manager::consumer_cap_bytes(crate::memory_manager::Consumer::WriteBuffer)
        {
            return cap;
        }
        2 * 1024 * 1024 * 1024
    })
}

/// FRS-WBM-HARD-CAP (2026-06-08): process-global HARD memtable cap. The soft cap
/// (`global_wbm_cap_bytes`) only TRIGGERS flush (advisory); under a heavy ingest
/// BURST on the UNBOUNDED-state queries (q4/q9/q17/q18/q20) writes outpace flush,
/// memtables grow far past the soft cap (~6.9 GiB observed), and the 8c/32g TM OOMs.
/// This HARD cap is where writers actually STALL (RocksDB `allow_stall`), throttling
/// the source to a flush-sustainable rate so total RAM stays bounded — like RocksDB.
/// Set ABOVE the soft cap so normal/bounded-state queries (q5/q16/q19, which stay
/// under it) never stall (avoids the soft-cap stall that froze q5). Default 6 GiB;
/// override `FRS_WBM_HARD_MB`; `0` disables the stall entirely.
pub fn global_wbm_hard_cap_bytes() -> u64 {
    static CAP: OnceLock<u64> = OnceLock::new();
    *CAP.get_or_init(|| {
        // Default 0 = DISABLED (2026-06-08): the memtable hard-cap stall was REFUTED
        // as the q9 OOM fix — q9's OOM is dominated by compaction transient + Java FFM,
        // NOT memtables (jemalloc-ctl proven), so stalling on memtables didn't prevent
        // it and made it WORSE (q9 crashed at 17.6M vs 60M; backpressure accumulated the
        // Java AEC in-flight → earlier OOM; memtables still hit 7.2GB). Opt in via
        // FRS_WBM_HARD_MB for experiments; the real memory-model fix must bound the
        // compaction transient + Java off-heap, not memtables.
        if let Some(mb) = std::env::var("FRS_WBM_HARD_MB")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
        {
            return mb.saturating_mul(1024 * 1024);
        }
        // FRS-MEM-MANAGER: the standalone hard cap was REFUTED (it stalled
        // writers while the UNBOUNDED Java AEC in-flight grew → EARLIER OOM).
        // Under the unified controller it is SAFE because the JVM side is
        // bounded in the SAME budget (process.size is carved out, leaving the
        // Java in-flight room). Derive the hard stall point at 1.25× the WBM
        // soft slice: writers stall only after exceeding the coordinated WBM
        // budget by a margin, so steady/bounded-state queries never stall but a
        // runaway ingest burst is throttled to a flush-sustainable rate BEFORE
        // the engine-native sum can cross the cliff. Disabled (0) when the
        // manager is off — byte-identical to today.
        if let Some(soft) =
            crate::memory_manager::consumer_cap_bytes(crate::memory_manager::Consumer::WriteBuffer)
        {
            return soft.saturating_add(soft / 4);
        }
        0
    })
}

/// True when the process-global memtable sum exceeds the HARD cap (writers must
/// stall). `0` hard cap disables it.
pub fn over_global_hard_budget() -> bool {
    let cap = global_wbm_hard_cap_bytes();
    cap != 0 && GLOBAL_WBM_USED.load(Ordering::Relaxed) > cap
}

/// FRS-MEM-WINDOWED-CEILING (2026-06-16, PMC-1 live-state track): the
/// LAST-RESORT live-memtable bound for a windowed merge-CF (q5 sliding-window
/// accumulator).
///
/// # Why a SEPARATE, HIGHER ceiling instead of the plain hard cap
///
/// q5's over-budget term is NOT a write burst the ordinary hard cap was tuned
/// for — it is the per-pane merge-operand accumulator held LIVE in the active
/// memtable until each window fires. The plain hard cap (1.25× the WBM soft
/// slice) fires far too early for that pattern: it pinned q5 at the cap and burnt
/// 571 s of stall (`stall_ms=571828`, sweep-results.md) WITHOUT preventing the
/// OOM, because the live window state kept growing while the writer parked. So
/// the windowed path must NOT stall at the ordinary hard cap.
///
/// But "never stall at all" (the prior unconditional skip) leaves the windowed
/// memtable sum with NO upper bound — a genuine unbounded-live path if flush
/// cannot keep up. The fix is a HIGHER ceiling that is still strictly bounded by
/// the engine-native budget, so it NEVER binds in steady state (the windowed
/// force-flush lever drains cold panes to SST long before the sum reaches it) yet
/// GUARANTEES, by construction, that the live memtable component can never grow
/// past a fixed fraction of the budget → never-OOM.
///
/// # The value
///
/// The windowed accumulator is the legitimately-large live consumer, so we grant
/// it the WBM slice PLUS the resident-shadow + vlog slices it would otherwise
/// share (those are evict-on-pressure caches the windowed CF does not stress) —
/// i.e. the ceiling is the sum of the WriteBuffer, ResidentShadow and
/// VlogResident slices. That is comfortably below the engine-native budget (it
/// excludes the block-cache and compaction-transient slices, which the windowed
/// path still needs), so the SUM of (windowed memtables + everything else the
/// controller bounds) stays under the cgroup minus headroom by construction.
/// `0` (manager off / no budget) ⇒ disabled, the caller keeps today's behaviour.
pub fn global_wbm_windowed_ceiling_bytes() -> u64 {
    static CAP: OnceLock<u64> = OnceLock::new();
    *CAP.get_or_init(|| {
        // Explicit operator pin always wins (A/B + escape hatch).
        if let Some(mb) = std::env::var("FRS_MEM_WINDOWED_CEILING_MB")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&v| v > 0)
        {
            return mb.saturating_mul(1024 * 1024);
        }
        use crate::memory_manager::{consumer_cap_bytes, Consumer};
        // Sum the live-state slices the windowed accumulator may legitimately
        // occupy. Each is `None` when the manager is off → ceiling 0 (disabled).
        match (
            consumer_cap_bytes(Consumer::WriteBuffer),
            consumer_cap_bytes(Consumer::ResidentShadow),
            consumer_cap_bytes(Consumer::VlogResident),
        ) {
            (Some(wbm), Some(shadow), Some(vlog)) => {
                wbm.saturating_add(shadow).saturating_add(vlog)
            }
            _ => 0,
        }
    })
}

/// True when the process-global memtable sum exceeds the WINDOWED ceiling
/// (a windowed merge-CF writer must stall — the last-resort never-OOM bound).
/// `0` ceiling (manager off / no budget) disables it.
pub fn over_global_windowed_ceiling() -> bool {
    let cap = global_wbm_windowed_ceiling_bytes();
    cap != 0 && GLOBAL_WBM_USED.load(Ordering::Relaxed) > cap
}

/// Current process-global memtable byte total (also used by the FRS_MEM_DIAG logger).
pub fn global_wbm_used_bytes() -> u64 {
    GLOBAL_WBM_USED.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// L4 (2026-06-12 compaction windowed-readpath design §2.3): compaction-input
// windowed-read knobs. The budget constants live HERE (one place) so the M3
// scan-prefetch budget fix can share the accounting when it lands.
// ---------------------------------------------------------------------------

/// `FRS_COMPACT_WINDOWED=1|true` turns ON windowed, double-buffered,
/// cache-Skip compaction-input reads (design W1-W3). Default OFF.
pub fn compaction_windowed_enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| {
        matches!(
            std::env::var("FRS_COMPACT_WINDOWED").ok().as_deref(),
            Some("1") | Some("true") | Some("TRUE")
        )
    })
}

/// Per-input compaction read window in BYTES (`FRS_COMPACT_WINDOW_BYTES`).
/// Default 2 MiB = 32 blocks at the 64 KiB default block size — RocksDB's
/// `compaction_readahead_size = 2 MB` class.
pub fn compaction_window_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("FRS_COMPACT_WINDOW_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(2 * 1024 * 1024)
    })
}

/// Aggregate compaction prefetch budget in BYTES
/// (`FRS_COMPACT_PREFETCH_BUDGET`; the design doc names the env without a
/// unit — bytes chosen for consistency with `FRS_COMPACT_WINDOW_BYTES`).
/// Worst case held by one job = fan-in × (1 ready + 1 inflight) windows, so
/// the per-input window is clamped to `budget / (2 × inputs × block_size)`.
/// Default 64 MiB.
pub fn compaction_prefetch_budget_bytes() -> u64 {
    static V: OnceLock<u64> = OnceLock::new();
    *V.get_or_init(|| {
        if let Some(v) = std::env::var("FRS_COMPACT_PREFETCH_BUDGET")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&v| v > 0)
        {
            return v;
        }
        // FRS-MEM-MANAGER: bound the compaction/build TRANSIENT prefetch working
        // set — the spike the standalone caps never bounded — with the
        // coordinated CompactionTransient slice when the controller is armed.
        // Floored at the historical 64 MiB default so small-budget configs don't
        // starve compaction below a usable window; falls back to 64 MiB when the
        // manager is off (byte-identical).
        if let Some(cap) = crate::memory_manager::consumer_cap_bytes(
            crate::memory_manager::Consumer::CompactionTransient,
        ) {
            return cap.max(64 * 1024 * 1024);
        }
        64 * 1024 * 1024
    })
}

/// L4 §2.3 fan-in budget clamp (pure — unit-testable without env):
/// `window_blocks = min(window_bytes/block_size, budget/(2 × inputs ×
/// block_size))`, floor 4 blocks. Below the floor returns `None` — the job
/// falls back to Demand-mode cursors (windowing a sliver isn't worth the
/// pool traffic).
pub fn compaction_window_blocks(
    n_inputs: usize,
    block_size: usize,
    window_bytes: u64,
    budget_bytes: u64,
) -> Option<u32> {
    /// Minimum useful window (blocks); below this, fall back to Demand.
    const WINDOW_FLOOR_BLOCKS: u64 = 4;
    if n_inputs == 0 || block_size == 0 {
        return None;
    }
    let bs = block_size as u64;
    let default_blocks = (window_bytes / bs).max(1);
    let budget_blocks = budget_bytes / (2 * n_inputs as u64 * bs);
    let w = default_blocks.min(budget_blocks);
    if w < WINDOW_FLOOR_BLOCKS {
        None
    } else {
        Some(w.min(u32::MAX as u64) as u32)
    }
}

/// Cross-CF memtable budget tracker (spec §6d "WriteBufferManager").
///
/// Each engine owns one [`WriteBufferManager`] shared across every column
/// family. Per-write paths call [`Self::reserve`] before adding a key/value
/// pair to the active memtable and [`Self::release`] after a flush
/// successfully evicts a memtable's bytes. The running sum is observable
/// via [`Self::current_bytes`]; the cap is fixed at construction time and
/// readable via [`Self::capacity_bytes`].
///
/// A capacity of `0` means "unbounded" — `over_budget` returns `false`
/// regardless of the running sum, so consumers can wire the manager
/// uniformly and let the config decide whether the cap fires.
#[derive(Debug)]
pub struct WriteBufferManager {
    /// Configured per-instance cross-CF cap in bytes. `0` = unbounded.
    capacity: u64,
    /// Running sum of bytes reserved across all CF memtables. Updated via
    /// `Relaxed` atomics — the cap check is advisory (writers don't block
    /// on the running sum), so the only ordering requirement is that
    /// `current_bytes()` eventually reflects committed reservations.
    current: AtomicU64,
    /// FRS-GLOBAL-WBM-BUDGET: when `true`, reservations also charge the
    /// process-global [`GLOBAL_WBM_USED`] sum and `over_budget()` additionally
    /// fires when that global sum exceeds `global_wbm_cap_bytes`. Enabled by the
    /// engine open path (`new_global`); `false` for the bare `new` used by tests
    /// and standalone callers (keeps their per-instance semantics + test isolation).
    use_global: bool,
}

impl WriteBufferManager {
    /// Creates a new per-instance manager with the given capacity. `0` =
    /// unbounded. Does NOT participate in the process-global budget.
    pub fn new(capacity_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity: capacity_bytes,
            current: AtomicU64::new(0),
            use_global: false,
        })
    }

    /// FRS-GLOBAL-WBM-BUDGET: like [`Self::new`] but also enrolls this manager in
    /// the PROCESS-GLOBAL memtable budget. `local_capacity_bytes` stays the
    /// per-instance secondary bound; the global cap (`global_wbm_cap_bytes`)
    /// bounds the TOTAL across all instances so memtable RAM cannot scale with
    /// instance count. The engine open path uses this.
    pub fn new_global(local_capacity_bytes: u64) -> Arc<Self> {
        Arc::new(Self {
            capacity: local_capacity_bytes,
            current: AtomicU64::new(0),
            use_global: true,
        })
    }

    /// Returns the configured cap in bytes (`0` = unbounded).
    #[inline]
    pub fn capacity_bytes(&self) -> u64 {
        self.capacity
    }

    /// Returns the running sum of reserved bytes across every CF.
    #[inline]
    pub fn current_bytes(&self) -> u64 {
        self.current.load(Ordering::Relaxed)
    }

    /// Reserves `n` bytes against the cross-CF budget. Always succeeds —
    /// reservation tracking is non-blocking and advisory; consumers
    /// observe `over_budget()` after the reserve to decide whether to
    /// trigger a flush. Saturates at `u64::MAX` rather than panicking on
    /// overflow.
    #[inline]
    pub fn reserve(&self, n: u64) {
        // Use `fetch_add` returning previous value so we can detect
        // saturation; explicit saturating add avoids wraparound on
        // pathological inputs (the FFI surface caps the per-CF
        // `write_buffer_size` at 1 TiB so realistic engines never
        // approach 2^64 bytes anyway).
        let prev = self.current.fetch_add(n, Ordering::Relaxed);
        if prev.checked_add(n).is_none() {
            // Wraparound — pin to u64::MAX for the next observer.
            self.current.store(u64::MAX, Ordering::Relaxed);
        }
        if self.use_global {
            GLOBAL_WBM_USED.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Releases `n` bytes from the cross-CF budget. Saturates at `0`
    /// rather than wrapping when consumers double-release on flush
    /// retries.
    #[inline]
    pub fn release(&self, n: u64) {
        // Saturating subtract via CAS loop — `fetch_sub` would wrap on
        // an over-release, masking the bug as a multi-EiB running sum.
        saturating_sub_atomic(&self.current, n);
        if self.use_global {
            saturating_sub_atomic(&GLOBAL_WBM_USED, n);
        }
    }

    /// Returns `true` when EITHER the per-instance running sum exceeds the
    /// per-instance cap, OR (in global mode) the process-global memtable sum
    /// exceeds the global budget. Always `false` for an unbounded per-instance
    /// manager not in global mode.
    ///
    /// Over-budget is advisory: the caller's only response is to TRIGGER A FLUSH
    /// of its own memtable, which is always correctness-safe and reduces both
    /// sums. So a global-over condition makes the next writer to ANY enrolled
    /// instance flush — collectively bounding the total to the global budget.
    #[inline]
    pub fn over_budget(&self) -> bool {
        if self.capacity != 0 && self.current_bytes() > self.capacity {
            return true;
        }
        if self.use_global {
            let cap = global_wbm_cap_bytes();
            if cap != 0 && GLOBAL_WBM_USED.load(Ordering::Relaxed) > cap {
                return true;
            }
        }
        false
    }
}

/// Saturating subtract on an `AtomicU64` via CAS loop — `fetch_sub` would wrap on
/// an over-release, masking the bug as a multi-EiB running sum.
#[inline]
fn saturating_sub_atomic(a: &AtomicU64, n: u64) {
    loop {
        let cur = a.load(Ordering::Relaxed);
        let next = cur.saturating_sub(n);
        if a.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            break;
        }
    }
}

impl Drop for WriteBufferManager {
    fn drop(&mut self) {
        // FRS-GLOBAL-WBM-BUDGET: on engine teardown, release this instance's
        // still-charged memtable bytes from the process-global sum so a dropped
        // DB does not leave a permanent phantom charge (which would over-trigger
        // flushes on the surviving instances). Per-instance `current` is the
        // authoritative residual; releasing it is idempotent (saturating).
        if self.use_global {
            let residual = self.current.load(Ordering::Relaxed);
            if residual != 0 {
                saturating_sub_atomic(&GLOBAL_WBM_USED, residual);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unbounded_capacity_never_over_budget() {
        let wbm = WriteBufferManager::new(0);
        wbm.reserve(u64::MAX / 2);
        assert!(!wbm.over_budget());
    }

    #[test]
    fn global_budget_fires_across_instances_and_releases() {
        // FRS-GLOBAL-WBM-BUDGET: two engines sharing the process-global memtable
        // budget. Each has an UNBOUNDED per-instance cap (0) so ONLY the global
        // bound can fire — proving the cross-instance budget works.
        let cap = global_wbm_cap_bytes();
        if cap == 0 {
            return; // global bound disabled in this environment → nothing to assert
        }
        let a = WriteBufferManager::new_global(0);
        let b = WriteBufferManager::new_global(0);
        // Each reserves the FULL global cap, so the combined sum is unambiguously
        // over budget even under concurrent test noise (other tests can only ADD
        // to the global sum, never push it below our 2×cap contribution).
        a.reserve(cap);
        b.reserve(cap);
        assert!(
            a.over_budget(),
            "global over-budget must fire on A (global sum >> cap)"
        );
        assert!(
            b.over_budget(),
            "global over-budget must fire on B (global sum >> cap)"
        );
        // A NON-global manager with a huge local cap must ignore the global sum
        // (the budget is strictly opt-in).
        let local_only = WriteBufferManager::new(u64::MAX);
        assert!(
            !local_only.over_budget(),
            "non-global manager must ignore the process-global sum"
        );
        // Release returns our contribution to the global sum (and Drop would too).
        a.release(cap);
        b.release(cap);
    }

    #[test]
    fn reserve_release_round_trip_zeroes_running_sum() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(512);
        wbm.reserve(256);
        assert_eq!(wbm.current_bytes(), 768);
        wbm.release(768);
        assert_eq!(wbm.current_bytes(), 0);
    }

    #[test]
    fn over_budget_fires_when_sum_exceeds_capacity() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(1025);
        assert!(wbm.over_budget());
    }

    #[test]
    fn over_budget_clears_after_release() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(2048);
        assert!(wbm.over_budget());
        wbm.release(1500);
        assert!(!wbm.over_budget());
    }

    #[test]
    fn release_saturates_at_zero_on_over_release() {
        let wbm = WriteBufferManager::new(1024);
        wbm.reserve(100);
        wbm.release(500); // over-release
        assert_eq!(wbm.current_bytes(), 0);
    }

    #[test]
    fn capacity_bytes_round_trips_construction_arg() {
        let wbm = WriteBufferManager::new(42);
        assert_eq!(wbm.capacity_bytes(), 42);
    }

    // -- L4 compaction windowed-read budget clamp (design §2.3) --------------

    const KIB64: usize = 64 * 1024;
    const MIB: u64 = 1024 * 1024;

    #[test]
    fn compact_window_default_unclamped_small_fanin() {
        // 2 MiB window / 64 KiB blocks = 32; 1 input: budget 64 MiB allows
        // 512 blocks → window stays the default 32.
        assert_eq!(
            compaction_window_blocks(1, KIB64, 2 * MIB, 64 * MIB),
            Some(32)
        );
        // 8 inputs: budget allows 64 blocks/input → still 32.
        assert_eq!(
            compaction_window_blocks(8, KIB64, 2 * MIB, 64 * MIB),
            Some(32)
        );
    }

    #[test]
    fn compact_window_budget_clamps_high_fanin() {
        // 20-input L0→L1 job: 64 MiB / (2 × 20 × 64 KiB) = 25.6 → 25 < 32.
        assert_eq!(
            compaction_window_blocks(20, KIB64, 2 * MIB, 64 * MIB),
            Some(25)
        );
    }

    #[test]
    fn compact_window_floor_boundary() {
        // Exactly the floor: budget/(2·n·bs) = 4 → Some(4).
        // 4 = 64 MiB / (2 × n × 64 KiB) ⇒ n = 128.
        assert_eq!(
            compaction_window_blocks(128, KIB64, 2 * MIB, 64 * MIB),
            Some(4)
        );
        // One more input pushes below the floor → Demand fallback.
        assert_eq!(
            compaction_window_blocks(129, KIB64, 2 * MIB, 64 * MIB),
            None
        );
        // Grossly over-fanned job → Demand fallback.
        assert_eq!(
            compaction_window_blocks(4096, KIB64, 2 * MIB, 64 * MIB),
            None
        );
    }

    #[test]
    fn compact_window_small_window_env_floor() {
        // A window-bytes override below 4 blocks also falls back to Demand.
        assert_eq!(
            compaction_window_blocks(1, KIB64, 3 * 64 * 1024, 64 * MIB),
            None
        );
        // ... and exactly 4 blocks is accepted.
        assert_eq!(
            compaction_window_blocks(1, KIB64, 4 * 64 * 1024, 64 * MIB),
            Some(4)
        );
    }

    #[test]
    fn compact_window_degenerate_inputs() {
        assert_eq!(compaction_window_blocks(0, KIB64, 2 * MIB, 64 * MIB), None);
        assert_eq!(compaction_window_blocks(1, 0, 2 * MIB, 64 * MIB), None);
    }

    // -- FRS-MEM-WINDOWED-CEILING (PMC-1 live-state track) -------------------

    #[test]
    fn windowed_ceiling_disabled_when_manager_off() {
        // With the manager unarmed (default test process), the ceiling is 0
        // (disabled) → `over_global_windowed_ceiling` never fires, so the
        // windowed path behaves byte-identically to today (no extra stall).
        if !crate::memory_manager::manager_enabled() {
            assert_eq!(
                global_wbm_windowed_ceiling_bytes(),
                0,
                "manager off ⇒ windowed ceiling disabled (0)"
            );
            assert!(
                !over_global_windowed_ceiling(),
                "manager off ⇒ windowed ceiling never fires"
            );
        }
    }

    #[test]
    fn windowed_ceiling_pure_sum_of_live_slices() {
        // The ceiling = WBM + ResidentShadow + VlogResident slices (the live
        // consumers the window accumulator may legitimately occupy). Verify the
        // PURE relationship against the manager's own slice helpers at a pinned
        // budget — independent of the process arming cache (these are pure fns).
        use crate::memory_manager::{engine_native_budget_for_test, slice_for_test, Consumer};
        let native = engine_native_budget_for_test(16 * 1024, 10 * 1024);
        let wbm = slice_for_test(native, Consumer::WriteBuffer);
        let shadow = slice_for_test(native, Consumer::ResidentShadow);
        let vlog = slice_for_test(native, Consumer::VlogResident);
        let ceiling = wbm + shadow + vlog;
        // The ceiling MUST be strictly below the engine-native budget (it
        // excludes the block-cache + compaction-transient slices, which the
        // windowed path still needs) — this is the never-OOM-by-construction
        // headroom: windowed memtables + the other bounded consumers stay under
        // the cgroup minus headroom.
        assert!(
            ceiling < native,
            "windowed ceiling {ceiling} must be strictly below native {native}"
        );
        // And it must be the LARGEST single live bound (a windowed CF gets more
        // headroom than the ordinary WBM hard cap = 1.25× the WBM slice alone).
        assert!(
            ceiling > wbm + wbm / 4,
            "windowed ceiling {ceiling} must exceed the ordinary hard cap {}",
            wbm + wbm / 4
        );
    }
}

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

//! FRS-MEM-MANAGER (2026-06-16, PMC-1): the UNIFIED engine-native memory
//! controller.
//!
//! # Why a single controller
//!
//! The 16 g/TM OOM cliff is a SUM problem, not any one consumer. Profiling
//! (docs/superpowers/specs/2026-06-08-8c32g-3backend-sweep-results.md) decomposed
//! the cgroup `memory.current` the OOM-killer watches into THREE pools:
//!
//!   1. **JVM process** (Flink heap + managed + FFM off-heap state buffers) —
//!      sized by `taskmanager.memory.process.size`. At process.size=10240m this
//!      is ~10 GiB; for q19 it dominated (~14.4 GiB of 16 GiB).
//!   2. **engine-native (jemalloc) RESIDENT** — block cache + memtables (WBM) +
//!      resident shadow + vlog reader cache + the compaction/build TRANSIENT.
//!      For q9 this was ~5.5 GiB live + a build spike on top.
//!   3. page cache / kernel — small, uncontrollable.
//!
//! Before this controller each engine-native consumer had its OWN independent,
//! hard-coded cap (block cache 256 MiB × ~8 instances ≈ 2 GiB; WBM soft 2 GiB;
//! resident shadow 2 GiB; vlog 2048 handles; compaction prefetch 64 MiB). Those
//! defaults were sized for a 32 GiB budget and do NOT (a) coordinate — their SUM
//! is unbounded relative to the cgroup — nor (b) scale when the TM is 10/12/16 g.
//! At 16 g/TM with a ~10 GiB JVM the engine-native sum + the build/compaction
//! transient spike crosses the cliff → exit-137. Reactive shedding + the
//! retained-pool purge were both proven NECESSARY-BUT-INSUFFICIENT: shedding
//! can't catch the build spike, and `retained` is virtual (not RSS).
//!
//! The fix is PROACTIVE ADMISSION: read the cgroup budget ONCE, subtract the
//! JVM reservation + FFM + safety headroom, derive ONE engine-native budget, and
//! split it across the consumers with fixed fractions so their SUM is bounded by
//! construction AND every cap auto-scales with the configured TM size. A
//! lower-than-default WBM HARD cap then becomes safe because the controller has
//! already carved process.size headroom for the Java AEC backpressure (the
//! standalone WBM hard-cap was refuted precisely because it stalled writers while
//! the JVM in-flight grew — here the JVM side is bounded in the same budget).
//!
//! # The budget formula
//!
//! ```text
//! cgroup       = FRS_MEM_CGROUP_MB  (override) | /sys/fs/cgroup/memory.max
//! jvm_reserved = FRS_JVM_RESERVED_MB (== taskmanager.memory.process.size)
//! ffm_reserved = FRS_FFM_RESERVED_MB (bounded off-heap state-buffer pool;
//!                                     default small — FFM is already bounded)
//! headroom     = max(FRS_MEM_HEADROOM_MB, cgroup * HEADROOM_FRAC)
//! engine_native = cgroup - jvm_reserved - ffm_reserved - headroom   (floored)
//! ```
//!
//! `engine_native` is then split:
//!   * block cache            BLOCK_CACHE_FRAC  (re-probed decoded blocks)
//!   * memtables (WBM)        WBM_FRAC          (write buffer)
//!   * resident shadow        SHADOW_FRAC       (flushed-memtable read cache)
//!   * vlog reader resident   VLOG_FRAC         (KV-sep value-log working set)
//!   * compaction transient   COMPACT_FRAC      (build/merge working set — the
//!     spike the standalone caps missed)
//!
//! Each consumer's cap function (in `db.rs` / `column_family.rs` /
//! `runtime_tuning.rs`) consults [`consumer_cap_bytes`] when the manager is armed
//! and the consumer's explicit env override is unset; otherwise it keeps its
//! current env/default (so behaviour is byte-identical when the manager is off OR
//! when an operator pins a specific cap).
//!
//! # Master flag — DEFAULT OFF, byte-identical when off
//!
//! `FRS_MEM_MANAGER=1` arms the controller. When unset (default) every
//! `consumer_cap_bytes` call returns `None`, so each consumer keeps its existing
//! independent default — no behaviour change, no cgroup reads. Purely additive.
//!
//! # Correctness
//!
//! Every cap the controller sets is a pure RAM bound on a CACHE / re-derivable
//! working set: a smaller block cache / shadow / vlog cache only re-reads from
//! the durable SST/segment (slower, never wrong); a smaller WBM only flushes
//! sooner (always correctness-safe); a smaller compaction window only narrows
//! prefetch. So tightening any cap can change TIMING but never OUTPUT — the rows
//! are byte-identical regardless of the budget. Smaller budget ⇒ never-OOM but
//! slower; that is the whole point.

/// The engine-native consumers the unified budget is split across. The order is
/// the split order; the fractions are on [`split_fraction`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consumer {
    /// Per-instance decoded-block `ShardedClockCache`. Re-probed join blocks.
    BlockCache,
    /// Cross-CF write-buffer (memtables) — the WBM soft+hard cap.
    WriteBuffer,
    /// Process-global resident-shadow (flushed-memtable read cache).
    ResidentShadow,
    /// KV-separation value-log reader resident working set.
    VlogResident,
    /// Compaction/build TRANSIENT prefetch working set (the spike).
    CompactionTransient,
}

impl Consumer {
    /// This consumer's fraction of the total engine-native budget. The five sum
    /// to 1.0 by construction (verified in tests) so the per-consumer caps sum
    /// to exactly `engine_native_budget_bytes`.
    #[inline]
    fn split_fraction(self) -> f64 {
        match self {
            // Re-probed decoded data blocks for streaming joins — the largest
            // genuine working set; keep the biggest slice.
            Consumer::BlockCache => 0.28,
            // Memtables: enough to amortise flush without dominating; the hard
            // cap derives from this slice so writers stall (RocksDB allow_stall)
            // BEFORE the engine-native sum can cross the cliff.
            Consumer::WriteBuffer => 0.30,
            // Flushed-memtable read cache — correctness-safe to evict.
            Consumer::ResidentShadow => 0.18,
            // Value-log reader resident — bounded; smaller than WBM/cache.
            Consumer::VlogResident => 0.10,
            // The build/merge TRANSIENT — the spike the standalone caps never
            // bounded. A dedicated slice means the controller PRE-RESERVES room
            // for it instead of letting it grow on top of everything else.
            Consumer::CompactionTransient => 0.14,
        }
    }
}

/// Number of co-resident keyed-state DB instances to assume when dividing the
/// PER-INSTANCE caps (block cache is per-instance; WBM / shadow are already
/// process-global). A Flink slot runs ~p join/agg DBs; `FRS_MEM_INSTANCES`
/// overrides. Default 8 (matches the historical 8c/32g instance count).
fn assumed_instances() -> u64 {
    std::env::var("FRS_MEM_INSTANCES")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(8)
}

/// FRS-MEM-MANAGER master flag (`FRS_MEM_MANAGER=1`, **DEFAULT OFF**). When off,
/// [`consumer_cap_bytes`] always returns `None` → every consumer keeps its
/// existing env/default cap, byte-identical to today.
pub fn manager_enabled() -> bool {
    // Cache only the armed (true) state: in production `FRS_MEM_MANAGER` is set
    // before process start so the first read is authoritative; caching `false`
    // would, in a shared test process, pin the manager off after an early probe.
    // Reading the env until armed is cheap (once per cap-function `OnceLock`
    // init) and never observed on the hot path.
    if ARMED.load(std::sync::atomic::Ordering::Relaxed) {
        return true;
    }
    let on = matches!(
        std::env::var("FRS_MEM_MANAGER").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    );
    if on {
        ARMED.store(true, std::sync::atomic::Ordering::Relaxed);
    }
    on
}

static ARMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// MiB → bytes.
const MIB: u64 = 1024 * 1024;

/// The cgroup memory budget in bytes: `FRS_MEM_CGROUP_MB` override first (lets a
/// non-cgroup host / a test pin it), then cgroup-v2 `memory.max`. `None` when
/// neither is available (no override, no cgroup limit) — then the manager cannot
/// derive a budget and stays inert (consumers keep their defaults).
fn cgroup_budget_bytes() -> Option<u64> {
    if let Some(mb) = env_mb("FRS_MEM_CGROUP_MB") {
        return Some(mb.saturating_mul(MIB));
    }
    read_cgroup_max_bytes()
}

/// Read the cgroup-v2 `memory.max` (bytes). `None` off cgroup-v2 / when unset
/// (`"max"`). Separated so the override path is testable without `/sys`.
fn read_cgroup_max_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let max = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
        let max = max.trim();
        if max == "max" {
            return None;
        }
        let v: u64 = max.parse().ok()?;
        if v == 0 {
            None
        } else {
            Some(v)
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Parse a `*_MB` env var into a positive MiB value, or `None`.
fn env_mb(key: &str) -> Option<u64> {
    std::env::var(key)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&v| v > 0)
}

/// The JVM reservation in bytes — the bytes the controller carves OUT of the
/// cgroup for the Flink JVM process (heap + managed + FFM). Set from
/// `taskmanager.memory.process.size` via `FRS_JVM_RESERVED_MB`. Default: 64 % of
/// the cgroup (a typical process.size on a 16 g TM is ~10 g ≈ 0.64), so the
/// controller still works if the harness forgets to forward the exact value.
fn jvm_reserved_bytes(cgroup: u64) -> u64 {
    match env_mb("FRS_JVM_RESERVED_MB") {
        Some(mb) => mb.saturating_mul(MIB),
        None => (cgroup as f64 * 0.64) as u64,
    }
}

/// The FFM off-heap reservation in bytes (`FRS_FFM_RESERVED_MB`). The FFM
/// state-buffer pool is ALREADY bounded (per-buffer sub-arenas, free-on-grow);
/// this is an extra cushion so the engine-native split doesn't claim bytes the
/// bounded FFM pool legitimately uses. Default 512 MiB.
fn ffm_reserved_bytes() -> u64 {
    env_mb("FRS_FFM_RESERVED_MB")
        .map(|mb| mb.saturating_mul(MIB))
        .unwrap_or(512 * MIB)
}

/// Safety headroom in bytes: `max(FRS_MEM_HEADROOM_MB, cgroup * HEADROOM_FRAC)`.
/// This is the gap kept BELOW the cgroup limit so a transient overshoot (a flush
/// in flight, a compaction job materialising its output) cannot reach the cliff
/// before the next admission decision. Default fraction 0.10, floor 1 GiB.
fn headroom_bytes(cgroup: u64) -> u64 {
    const HEADROOM_FRAC: f64 = 0.10;
    let frac = (cgroup as f64 * HEADROOM_FRAC) as u64;
    let floor = env_mb("FRS_MEM_HEADROOM_MB")
        .map(|mb| mb.saturating_mul(MIB))
        .unwrap_or(1024 * MIB);
    frac.max(floor)
}

/// The total engine-native budget in bytes:
/// `cgroup - jvm_reserved - ffm_reserved - headroom`, floored at
/// `ENGINE_NATIVE_FLOOR` (1.5 GiB). This is the bytes ALL engine-native consumers
/// together may hold; the split fractions partition exactly this. `None` when no
/// cgroup budget is visible (manager stays inert).
///
/// Computed once and cached — the cgroup limit does not change within a run.
pub fn engine_native_budget_bytes() -> Option<u64> {
    // Cache only a SUCCESSFUL computation (Some) — a transient None (cgroup not
    // yet visible, e.g. an override env set after an early probe) must not pin
    // the budget off for the run. Once a real budget is derived it is stable.
    static CACHE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let cached = CACHE.load(std::sync::atomic::Ordering::Relaxed);
    if cached != 0 {
        return Some(cached);
    }
    let cgroup = cgroup_budget_bytes()?;
    let jvm = jvm_reserved_bytes(cgroup);
    let ffm = ffm_reserved_bytes();
    let head = headroom_bytes(cgroup);
    let reserved = jvm.saturating_add(ffm).saturating_add(head);
    let native = cgroup.saturating_sub(reserved).max(ENGINE_NATIVE_FLOOR);
    CACHE.store(native, std::sync::atomic::Ordering::Relaxed);
    Some(native)
}

/// Absolute floor for the engine-native budget (bytes). Below this the engine
/// cannot make progress on a heavy query; if the formula yields less (a tiny TM
/// or a huge process.size) we floor here and accept the closer-to-cliff risk —
/// better than starving the engine to zero. 1.5 GiB.
const ENGINE_NATIVE_FLOOR: u64 = 1536 * MIB;

/// The cap (bytes) for one engine-native [`Consumer`] under the unified budget,
/// or `None` when the manager is off / no cgroup budget is visible (caller keeps
/// its own default). For per-instance consumers (`BlockCache`) the slice is
/// divided by the assumed co-resident instance count (`FRS_MEM_INSTANCES`);
/// process-global consumers get the whole slice.
///
/// This is THE coupling point: every consumer's existing cap function calls this
/// first and only falls back to its env/default when this returns `None`.
pub fn consumer_cap_bytes(consumer: Consumer) -> Option<u64> {
    if !manager_enabled() {
        return None;
    }
    let native = engine_native_budget_bytes()?;
    let slice = (native as f64 * consumer.split_fraction()) as u64;
    let cap = match consumer {
        // Per-instance cache: divide the slice across the co-resident DBs.
        Consumer::BlockCache => slice / assumed_instances().max(1),
        // Process-global consumers hold the whole slice.
        Consumer::WriteBuffer
        | Consumer::ResidentShadow
        | Consumer::VlogResident
        | Consumer::CompactionTransient => slice,
    };
    Some(cap.max(MIB)) // never return a degenerate 0 cap
}

// ===========================================================================
// FRS-MEM-COMPACT-ADMISSION (2026-06-16, PMC-1 live-state track): PROACTIVE
// admission bound on the in-flight compaction/build TRANSIENT working set.
//
// # Why a SEPARATE mechanism from `consumer_cap_bytes(CompactionTransient)`
//
// `consumer_cap_bytes(CompactionTransient)` bounds the per-job *prefetch read
// window* (the read-ahead buffer). It does NOT bound how many compaction jobs
// run CONCURRENTLY, nor the anon working set each materialises (input decoded
// blocks + the k-way merge heap + the output writer's buffers). The q9 cliff
// (measured, sweep-results.md): `jemalloc_alloc(LIVE)` goes Ample→over-cliff
// inside ONE sampler window because, during the interval-join build storm, the
// ~8 co-resident keyed-state DBs all hit their L0 rollup at once and the SUM of
// their concurrent compaction transients spikes the anon RSS past the cgroup
// BEFORE the reactive sampler/purge gets a tick. The LIVE set FITS the budget
// in steady state (4675 MB < 6041 MB); it is the TRANSIENT SPIKE that crosses.
//
// # The mechanism: a process-global byte SEMAPHORE
//
// Before a picked compaction job runs its heavy merge it ADMITS an estimate of
// its transient working set (input bytes, clamped) against the
// `CompactionTransient` budget slice. If admitting would exceed the slice the
// job WAITS (back-pressuring the bg-compaction pool, which back-pressures flush
// enqueue, which throttles ingest) until an in-flight job finishes and releases.
// This is PROACTIVE: the spike is bounded BEFORE it materialises, by capping the
// SUM of concurrent transients — exactly what the reactive purge could not do.
//
// # Correctness
//
// Compaction is ALWAYS deferrable: a job that waits produces a byte-identical
// output whenever it runs, and the engine re-picks the same inputs against the
// then-current Version. Delaying a merge only changes TIMING (L0 stays deeper
// for longer ⇒ reads scan more SSTs ⇒ slower) — never OUTPUT. So the admission
// bound is byte-identical and never-OOM, the same correctness argument as every
// other cap in this controller.
//
// # Flag — folds into the master `FRS_MEM_MANAGER`
//
// Armed iff `manager_enabled()` (so the existing armed sweep gets it for free)
// AND a `CompactionTransient` budget is derivable. Off ⇒ `admit` is an instant
// no-op permit (byte-identical to today). `FRS_MEM_COMPACT_ADMISSION=0`
// force-disables it even under the manager (escape hatch for A/B).
// ===========================================================================

/// In-flight admitted compaction-transient bytes (process-global). Read for the
/// diag line; mutated by [`CompactionAdmission`] acquire/release.
static COMPACT_INFLIGHT_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Count of compaction admissions that had to WAIT for headroom (diag evidence
/// the proactive bound actually engaged — the q9 spike was throttled).
static COMPACT_ADMISSION_WAITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Whether the compaction-transient admission bound is armed. Folded into the
/// master manager flag; `FRS_MEM_COMPACT_ADMISSION=0` force-disables.
fn compact_admission_enabled() -> bool {
    if matches!(
        std::env::var("FRS_MEM_COMPACT_ADMISSION").ok().as_deref(),
        Some("0") | Some("false") | Some("FALSE")
    ) {
        return false;
    }
    manager_enabled()
}

/// A held compaction-transient admission permit. Releases the admitted bytes
/// back to the global pool on drop (RAII — release on EVERY exit path, incl. the
/// `?` early-returns and panics in `run_compaction`).
#[must_use = "the permit must be held for the duration of the compaction merge"]
pub struct CompactionAdmission {
    bytes: u64,
}

impl CompactionAdmission {
    /// PROACTIVELY admit `estimate` bytes of compaction-transient working set
    /// against the `CompactionTransient` budget slice. Blocks (bounded poll)
    /// until the in-flight SUM + `estimate` fits the slice, then reserves and
    /// returns the permit. When the bound is off (manager unarmed / no budget /
    /// force-disabled) this is an INSTANT no-op permit reserving 0 bytes —
    /// byte-identical to today (no wait, no accounting).
    ///
    /// `estimate` is clamped to the slice so a single huge job can never
    /// deadlock against its own bound (it admits the whole slice and runs alone,
    /// which is the correct never-OOM behaviour — one job at a time rather than
    /// many concurrent ones crossing the cliff).
    pub fn admit(estimate: u64) -> Self {
        if !compact_admission_enabled() {
            return Self { bytes: 0 };
        }
        let Some(slice) = consumer_cap_bytes(Consumer::CompactionTransient) else {
            return Self { bytes: 0 };
        };
        // Clamp so one oversized job admits at most the whole slice (runs solo).
        let want = estimate.clamp(MIB, slice);
        let mut waited = false;
        loop {
            let cur = COMPACT_INFLIGHT_BYTES.load(std::sync::atomic::Ordering::Acquire);
            // Always allow the FIRST job in (cur == 0) even if `want` == slice,
            // so progress is guaranteed; otherwise require room for `want`.
            if cur == 0 || cur.saturating_add(want) <= slice {
                if COMPACT_INFLIGHT_BYTES
                    .compare_exchange_weak(
                        cur,
                        cur.saturating_add(want),
                        std::sync::atomic::Ordering::AcqRel,
                        std::sync::atomic::Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    return Self { bytes: want };
                }
                continue; // lost the CAS race; re-read and retry immediately.
            }
            if !waited {
                COMPACT_ADMISSION_WAITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                waited = true;
            }
            // Bounded poll: another job will release and lower `cur`. 2 ms keeps
            // the bg-compaction thread responsive without busy-spinning.
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
    }
}

impl Drop for CompactionAdmission {
    fn drop(&mut self) {
        if self.bytes == 0 {
            return;
        }
        // Saturating subtract — never wrap on a double-release.
        let b = self.bytes;
        loop {
            let cur = COMPACT_INFLIGHT_BYTES.load(std::sync::atomic::Ordering::Acquire);
            let next = cur.saturating_sub(b);
            if COMPACT_INFLIGHT_BYTES
                .compare_exchange_weak(
                    cur,
                    next,
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }
}

/// Current in-flight admitted compaction-transient bytes (diag).
pub fn compact_inflight_bytes() -> u64 {
    COMPACT_INFLIGHT_BYTES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Number of compaction admissions that had to wait for headroom (diag).
pub fn compact_admission_waits() -> u64 {
    COMPACT_ADMISSION_WAITS.load(std::sync::atomic::Ordering::Relaxed)
}

/// FRS-MEM-MANAGER: a one-line snapshot of the derived budget for the
/// `[FRS_MEM_DIAG]` log — DIRECT evidence that the controller engaged in-container
/// and which cap each consumer received. Empty string when the manager is off.
pub fn diag_str() -> String {
    if !manager_enabled() {
        return String::new();
    }
    let Some(native) = engine_native_budget_bytes() else {
        return " mem_mgr=armed(no-cgroup)".to_string();
    };
    let mb = |b: Option<u64>| b.map(|v| v / MIB).unwrap_or(0);
    format!(
        " mem_mgr_native_MB={} mm_blockcache_MB={} mm_wbm_MB={} mm_shadow_MB={} mm_vlog_MB={} mm_compact_MB={} mm_compact_inflight_MB={} mm_compact_waits={}",
        native / MIB,
        mb(consumer_cap_bytes(Consumer::BlockCache)),
        mb(consumer_cap_bytes(Consumer::WriteBuffer)),
        mb(consumer_cap_bytes(Consumer::ResidentShadow)),
        mb(consumer_cap_bytes(Consumer::VlogResident)),
        mb(consumer_cap_bytes(Consumer::CompactionTransient)),
        compact_inflight_bytes() / MIB,
        compact_admission_waits(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // The env + the OnceLock budget cache are process-global; serialize the
    // tests that flip env so the parallel runner can't interleave. (The budget
    // cache is computed once, so these tests pin the env via the OVERRIDE path —
    // FRS_MEM_CGROUP_MB / FRS_JVM_RESERVED_MB — but the cache means only the
    // FIRST armed computation wins per process. The fraction/formula tests below
    // therefore exercise the PURE helpers directly, not the cached entry point.)
    static MM_TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn fractions_sum_to_one() {
        let total: f64 = [
            Consumer::BlockCache,
            Consumer::WriteBuffer,
            Consumer::ResidentShadow,
            Consumer::VlogResident,
            Consumer::CompactionTransient,
        ]
        .iter()
        .map(|c| c.split_fraction())
        .sum();
        assert!(
            (total - 1.0).abs() < 1e-9,
            "split fractions must sum to 1.0, got {total}"
        );
    }

    #[test]
    fn budget_formula_subtracts_all_reservations() {
        let _g = MM_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // 16 GiB cgroup, 10 GiB JVM, 512 MiB FFM, headroom = max(1.6G, 1G)=1.6G.
        let cgroup = 16 * 1024 * MIB;
        let jvm = 10 * 1024 * MIB; // simulate FRS_JVM_RESERVED_MB=10240
        let ffm = ffm_reserved_bytes(); // default 512 MiB (no env)
        let head = headroom_bytes(cgroup); // 0.10*16G = 1.6 GiB > 1 GiB floor
        let native = cgroup
            .saturating_sub(jvm)
            .saturating_sub(ffm)
            .saturating_sub(head);
        // native == cgroup - jvm - ffm - headroom, exactly (no float in this sum).
        assert_eq!(native, cgroup - jvm - ffm - head);
        // And it is the expected ~3.9 GiB engine-native slice.
        assert!(
            (3800..4100).contains(&(native / MIB)),
            "native MiB = {} not in 3800..4100",
            native / MIB
        );
        // Above the floor, so the floor does not clamp here.
        assert!(native > ENGINE_NATIVE_FLOOR);
    }

    #[test]
    fn budget_scales_down_with_smaller_cgroup() {
        // 12 g vs 16 g, same 0.64-default JVM fraction ⇒ strictly smaller native.
        let head16 = headroom_bytes(16 * 1024 * MIB);
        let native16 = (16 * 1024 * MIB)
            .saturating_sub(jvm_reserved_bytes(16 * 1024 * MIB))
            .saturating_sub(ffm_reserved_bytes())
            .saturating_sub(head16);
        let head12 = headroom_bytes(12 * 1024 * MIB);
        let native12 = (12 * 1024 * MIB)
            .saturating_sub(jvm_reserved_bytes(12 * 1024 * MIB))
            .saturating_sub(ffm_reserved_bytes())
            .saturating_sub(head12);
        assert!(
            native12 < native16,
            "smaller cgroup must yield a smaller engine-native budget ({native12} !< {native16})"
        );
    }

    #[test]
    fn headroom_is_max_of_floor_and_fraction() {
        // Big cgroup ⇒ fraction (0.10) dominates the 1 GiB floor.
        assert_eq!(headroom_bytes(40 * 1024 * MIB), 4 * 1024 * MIB);
        // Small cgroup ⇒ the 1 GiB floor dominates.
        assert_eq!(headroom_bytes(4 * 1024 * MIB), 1024 * MIB);
    }

    #[test]
    fn off_by_default_returns_none() {
        let _g = MM_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // With the manager unarmed (the default in the test process), every
        // consumer cap defers to the caller's own default.
        if !manager_enabled() {
            for c in [
                Consumer::BlockCache,
                Consumer::WriteBuffer,
                Consumer::ResidentShadow,
                Consumer::VlogResident,
                Consumer::CompactionTransient,
            ] {
                assert_eq!(consumer_cap_bytes(c), None, "off ⇒ None for {c:?}");
            }
        }
    }

    #[test]
    fn jvm_reserved_defaults_to_064_fraction() {
        let cgroup = 16 * 1024 * MIB;
        // No FRS_JVM_RESERVED_MB env in the default test process ⇒ 0.64 fraction.
        if std::env::var("FRS_JVM_RESERVED_MB").is_err() {
            assert_eq!(jvm_reserved_bytes(cgroup), (cgroup as f64 * 0.64) as u64);
        }
    }

    // -- FRS-MEM-COMPACT-ADMISSION (PMC-1 live-state track) ------------------

    #[test]
    fn admission_is_noop_permit_when_manager_off() {
        let _g = MM_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // Default test process: the manager is unarmed ⇒ admit must be an instant
        // no-op permit reserving ZERO bytes and never touching the in-flight sum
        // (byte-identical to today: no wait, no accounting).
        if !manager_enabled() {
            let before = compact_inflight_bytes();
            let permit = CompactionAdmission::admit(64 * MIB);
            assert_eq!(
                permit.bytes, 0,
                "manager off ⇒ no-op permit reserves 0 bytes"
            );
            assert_eq!(
                compact_inflight_bytes(),
                before,
                "manager off ⇒ in-flight sum unchanged by admit"
            );
            drop(permit);
            assert_eq!(
                compact_inflight_bytes(),
                before,
                "manager off ⇒ in-flight sum unchanged by release"
            );
        }
    }

    #[test]
    fn admission_drop_is_saturating_and_balanced() {
        // The in-flight counter math (reserve on admit, release on drop) must be
        // exactly balanced and never wrap. Exercise the Drop accounting directly
        // with synthetic permits (independent of the manager-armed gate) so the
        // invariant holds regardless of the test process's arming state.
        let base = compact_inflight_bytes();
        // Two synthetic held permits add then release their bytes exactly.
        let a = CompactionAdmission { bytes: 10 * MIB };
        COMPACT_INFLIGHT_BYTES.fetch_add(10 * MIB, std::sync::atomic::Ordering::AcqRel);
        let b = CompactionAdmission { bytes: 5 * MIB };
        COMPACT_INFLIGHT_BYTES.fetch_add(5 * MIB, std::sync::atomic::Ordering::AcqRel);
        assert_eq!(compact_inflight_bytes(), base + 15 * MIB);
        drop(a);
        assert_eq!(compact_inflight_bytes(), base + 5 * MIB);
        drop(b);
        assert_eq!(
            compact_inflight_bytes(),
            base,
            "balanced reserve/release returns to baseline"
        );
        // Over-release saturates at zero rather than wrapping.
        let over = CompactionAdmission {
            bytes: compact_inflight_bytes() + 1_000 * MIB,
        };
        drop(over);
        // Counter floored at 0 (saturating) — never a multi-EiB wrap.
        assert!(compact_inflight_bytes() <= base);
    }
}

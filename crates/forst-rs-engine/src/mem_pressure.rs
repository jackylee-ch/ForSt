//! FRS-DYN-SHED (2026-06-16): dynamic, memory-pressure-aware lever shedding.
//!
//! # Why
//!
//! The "full-stack-ON" uniform config (KV-separation + persistent-probe-iter +
//! coalesce + S2-pinned + leveled-hot-CF + a generous vlog resident cache)
//! makes the per-query wins (q7 etc.) but, enabled UNCONDITIONALLY, robs the
//! big scatter-joins / sliding windows (q9/q19/q5) of headroom at the uniform
//! 16 g/TM split: their resident working set plus every lever's buffers pushed
//! RSS over the cgroup and got the TM OOM-killed (exit-137, NOT a correctness
//! fault). The prior LEANER config FIT those queries.
//!
//! The directive is ONE uniform config (all levers "enabled", KV-sep format
//! uniform — no hybrid layout), but the engine must DYNAMICALLY shed the
//! memory-hungry levers under cgroup pressure — bounded, NEVER-OOM — and keep
//! them when memory is ample (so the queries that benefit still win). This is
//! the "same-config dynamic-only" model: the engine decides at runtime, NOT
//! per query.
//!
//! # Mechanism
//!
//! A process-global memory watermark is sampled from the most authoritative
//! source available:
//!   1. an explicit test/bench override (deterministic),
//!   2. the cgroup-v2 controller (`memory.current` / `memory.max`) — the exact
//!      bytes the OOM-killer watches, so shedding tracks the real budget,
//!   3. the process RSS from `/proc/self/statm` against a configured budget
//!      (`FRS_MEM_BUDGET_MB`) as a portable fallback.
//!
//! The sampled fraction `used / budget` is bucketed into a [`PressureLevel`].
//! Each memory-hungry lever is assigned a [`ShedPriority`] (the order in which
//! it is shed as pressure rises — least-correctness-sensitive / most
//! memory-hungry first). A lever's `*_enabled()` gate AND-folds
//! [`should_shed`]: under pressure at/above the lever's shed threshold the
//! lever transparently disengages, dropping its resident footprint, so total
//! resident stays under a safe fraction of the cgroup.
//!
//! # Safety / correctness
//!
//! Shedding only ever turns a lever OFF — i.e. it falls back to the
//! byte-identical legacy path that lever has an A/B equivalence test for. It
//! NEVER changes the on-disk format: KV-separation stays ON (uniform format);
//! shedding only declines to SEPARATE *new* flushes for the value-log readers'
//! resident cache budget — the existing separated data is read exactly as
//! before. So output is byte-identical regardless of whether a lever is shed
//! mid-run.
//!
//! # Master flag — DEFAULT OFF, byte-identical when off
//!
//! `FRS_DYNAMIC_SHED=1` arms the whole mechanism. When unset (default),
//! [`should_shed`] always returns `false` and every lever behaves EXACTLY as
//! today (no sampler thread starts, no cgroup reads). The mechanism is purely
//! additive and observable only when explicitly armed.

use std::sync::atomic::{AtomicU8, Ordering};

/// Coarse memory-pressure buckets. Higher = closer to the cgroup limit.
///
/// The thresholds (as a fraction of the budget) are deliberately conservative
/// so shedding engages BEFORE the OOM-killer would fire, not at the cliff:
///   * `Ample`    `< 0.75` — keep every lever (full-stack-ON wins).
///   * `Elevated` `0.75..0.85` — shed the heaviest, least-sensitive levers.
///   * `High`     `0.85..0.92` — shed the mid tier too.
///   * `Critical` `>= 0.92` — shed everything sheddable; only the uniform
///     format (KV-sep) and correctness-load-bearing state remain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PressureLevel {
    /// Memory is ample — no shedding.
    Ample = 0,
    /// Approaching the budget — shed the highest-priority (heaviest) levers.
    Elevated = 1,
    /// Close to the budget — shed the mid tier as well.
    High = 2,
    /// At the cliff — shed everything sheddable.
    Critical = 3,
}

/// Shed priority for a memory-hungry lever: the [`PressureLevel`] at (and
/// above) which the lever is shed. A lever with a LOWER variant sheds FIRST
/// (i.e. at a lower pressure) — it is the least-correctness-sensitive / most
/// memory-hungry, so it is the first to give back headroom and the last we
/// want to rely on under pressure.
///
/// The ladder (shed-first → shed-last), per the work-order priority order:
///   1. [`ShedPriority::PersistentProbeIter`] — held iterators pin an
///      `Arc<Version>` + the overlapping SST readers for the whole probe
///      window; dropping them lets the version + readers be reclaimed. Highest
///      resident cost, trivially reconstructable (per-probe rebuild path).
///   2. [`ShedPriority::CoalesceBuffers`] — the deferred-deref side lists +
///      grouped per-segment chunk buffers; falling back to inline per-key
///      deref frees them. Pure throughput lever.
///   3. [`ShedPriority::S2Pinned`] — pinned data blocks held across a scan;
///      the legacy streaming path re-reads on demand instead of pinning.
///   4. [`ShedPriority::LeveledHotCf`] — the lowered L0 rollup trigger keeps a
///      hot CF more leveled (more resident index/filter blocks); relaxing it
///      back to the base trigger trims that residency.
///   5. [`ShedPriority::VlogResident`] — the value-log reader cache budget;
///      tightened LAST because it is the most directly tied to KV-sep read
///      correctness latency and is already byte-bounded by its own budget.
///
/// `KV-separation` itself is NOT on this ladder — the format stays uniform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShedPriority {
    /// Shed first (at `Elevated`): persistent probe iterators.
    PersistentProbeIter,
    /// Shed at `Elevated`: coalesce side-buffers.
    CoalesceBuffers,
    /// Shed at `High`: S2 pinned blocks.
    S2Pinned,
    /// Shed at `High`: the leveled-hot-CF residency boost.
    LeveledHotCf,
    /// Shed last (at `Critical`): tighten the vlog reader resident budget.
    VlogResident,
}

impl ShedPriority {
    /// The lowest [`PressureLevel`] at (and above) which this lever is shed.
    #[inline]
    fn shed_at(self) -> PressureLevel {
        match self {
            ShedPriority::PersistentProbeIter | ShedPriority::CoalesceBuffers => {
                PressureLevel::Elevated
            }
            ShedPriority::S2Pinned | ShedPriority::LeveledHotCf => PressureLevel::High,
            ShedPriority::VlogResident => PressureLevel::Critical,
        }
    }
}

/// Test/bench override for the sampled pressure level. `u8::MAX` = unset
/// (sample the real source); otherwise the [`PressureLevel`] discriminant.
/// Programmatic (not env) so in-process fixtures pick a level deterministically
/// without a sampler thread or cgroup files.
static PRESSURE_OVERRIDE: AtomicU8 = AtomicU8::new(u8::MAX);

/// The most-recently-sampled pressure level (discriminant). Updated by the
/// sampler thread (when armed) every few seconds; read lock-free on the hot
/// gate path. Starts at `Ample` (0) so an un-sampled engine never sheds.
static SAMPLED_PRESSURE: AtomicU8 = AtomicU8::new(PressureLevel::Ample as u8);

/// Cached arming decision: 0 = unknown, 1 = off, 2 = on. Avoids re-reading the
/// env on every hot-path gate call.
static ARMED: AtomicU8 = AtomicU8::new(0);

/// FRS-DYN-SHED: master flag (`FRS_DYNAMIC_SHED=1`, **DEFAULT OFF**). When off,
/// [`should_shed`] is always `false` and no lever ever sheds — byte-identical
/// to today. Cached after the first read; a test override forces it.
pub fn dynamic_shed_enabled() -> bool {
    match ARMED.load(Ordering::Relaxed) {
        1 => return false,
        2 => return true,
        _ => {}
    }
    let on = matches!(
        std::env::var("FRS_DYNAMIC_SHED").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    );
    ARMED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

/// FRS-DYN-SHED: force the master flag on/off for tests/benches (`None`
/// restores the `FRS_DYNAMIC_SHED` env behaviour).
pub fn set_dynamic_shed_override(v: Option<bool>) {
    ARMED.store(
        match v {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        },
        Ordering::Relaxed,
    );
    // The purge valve's arming folds in `dynamic_shed_enabled()`, so its cache
    // must be re-resolved whenever the master flag override changes.
    PURGE_ARMED.store(0, Ordering::Relaxed);
}

/// FRS-MEM-PRESSURE-PURGE: force the purge-valve arming on/off for tests/benches
/// (`None` restores the env-derived behaviour).
pub fn set_purge_valve_override(v: Option<bool>) {
    PURGE_ARMED.store(
        match v {
            None => 0,
            Some(false) => 1,
            Some(true) => 2,
        },
        Ordering::Relaxed,
    );
}

/// FRS-DYN-SHED: force the sampled pressure level for tests/benches (`None`
/// restores real sampling). Lets an in-process A/B drive a lever's shed
/// behaviour deterministically.
pub fn set_pressure_override(level: Option<PressureLevel>) {
    PRESSURE_OVERRIDE.store(level.map_or(u8::MAX, |l| l as u8), Ordering::Relaxed);
}

/// Decode a stored discriminant back into a [`PressureLevel`] (saturating).
#[inline]
fn level_from_u8(v: u8) -> PressureLevel {
    match v {
        0 => PressureLevel::Ample,
        1 => PressureLevel::Elevated,
        2 => PressureLevel::High,
        _ => PressureLevel::Critical,
    }
}

/// The current memory-pressure level: the override if set, else the most
/// recent sample. Cheap (one relaxed load on the common path).
#[inline]
pub fn current_pressure() -> PressureLevel {
    let ov = PRESSURE_OVERRIDE.load(Ordering::Relaxed);
    if ov != u8::MAX {
        return level_from_u8(ov);
    }
    level_from_u8(SAMPLED_PRESSURE.load(Ordering::Relaxed))
}

/// THE gate every memory-hungry lever AND-folds. Returns `true` (shed this
/// lever now) iff the master flag is armed AND the current pressure is at/above
/// the lever's shed threshold. When the master flag is OFF this is ALWAYS
/// `false` → byte-identical to today.
///
/// `dynamic_shed_enabled()` is checked first so the un-armed hot path is a
/// single relaxed atomic load and never touches the pressure level.
#[inline]
pub fn should_shed(lever: ShedPriority) -> bool {
    if !dynamic_shed_enabled() {
        return false;
    }
    current_pressure() >= lever.shed_at()
}

/// Map a `used / budget` fraction (in basis points, `used * 10_000 / budget`)
/// to a [`PressureLevel`] using the conservative thresholds documented on
/// [`PressureLevel`]. Used by the Linux sampler and the threshold test; on a
/// non-Linux dev host the sampler is a no-op, so this is only referenced by the
/// `#[cfg(test)]` threshold check.
#[cfg_attr(not(any(target_os = "linux", test)), allow(dead_code))]
#[inline]
fn level_from_bp(bp: u64) -> PressureLevel {
    if bp >= 9_200 {
        PressureLevel::Critical
    } else if bp >= 8_500 {
        PressureLevel::High
    } else if bp >= 7_500 {
        PressureLevel::Elevated
    } else {
        PressureLevel::Ample
    }
}

/// Read the cgroup-v2 memory budget as `(current_bytes, max_bytes)`. Returns
/// `None` when not on cgroup-v2 or when the limit is unset (`"max"`). This is
/// the exact pair the OOM-killer watches, so shedding tracks the real budget.
#[cfg(target_os = "linux")]
fn read_cgroup_v2() -> Option<(u64, u64)> {
    let cur = std::fs::read_to_string("/sys/fs/cgroup/memory.current").ok()?;
    let max = std::fs::read_to_string("/sys/fs/cgroup/memory.max").ok()?;
    let cur: u64 = cur.trim().parse().ok()?;
    let max = max.trim();
    if max == "max" {
        return None; // no limit set on this cgroup
    }
    let max: u64 = max.parse().ok()?;
    if max == 0 {
        return None;
    }
    Some((cur, max))
}

/// Read process RSS in bytes from `/proc/self/statm` (page 2 = resident pages).
#[cfg(target_os = "linux")]
fn read_rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
    Some(pages.saturating_mul(4096))
}

/// `FRS_MEM_BUDGET_MB`: the RSS budget (MiB) for the portable fallback when no
/// cgroup-v2 limit is visible. `0`/unset disables the RSS fallback (then only
/// the cgroup source can drive pressure). Linux-only — the fallback needs
/// `/proc/self/statm`.
#[cfg(target_os = "linux")]
fn rss_budget_bytes() -> Option<u64> {
    std::env::var("FRS_MEM_BUDGET_MB")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&mb| mb > 0)
        .map(|mb| mb.saturating_mul(1024 * 1024))
}

/// Sample the current pressure once: cgroup-v2 first (authoritative), then the
/// RSS-vs-budget fallback. Returns `None` when no source is available (no
/// cgroup limit and no `FRS_MEM_BUDGET_MB`), in which case the sampler leaves
/// the level untouched (stays `Ample` → no shedding).
fn sample_pressure_once() -> Option<PressureLevel> {
    #[cfg(target_os = "linux")]
    {
        if let Some((cur, max)) = read_cgroup_v2() {
            let bp = cur.saturating_mul(10_000) / max;
            return Some(level_from_bp(bp));
        }
        if let (Some(rss), Some(budget)) = (read_rss_bytes(), rss_budget_bytes()) {
            let bp = rss.saturating_mul(10_000) / budget;
            return Some(level_from_bp(bp));
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        // No cgroup / /proc on non-Linux (the dev Mac). The portable budget
        // path needs an RSS reading we don't have here, so the sampler is a
        // no-op; tests drive the level via `set_pressure_override`.
        None
    }
}

/// FRS-DYN-SHED: start the process-global pressure sampler (idempotent). A
/// no-op unless the master flag is armed. Samples every
/// `FRS_DYN_SHED_INTERVAL_MS` (default 1000 ms) and publishes the level for the
/// lock-free [`should_shed`] gate path.
pub fn maybe_start_sampler() {
    use std::sync::OnceLock;
    static STARTED: OnceLock<()> = OnceLock::new();
    // Start the sampler if EITHER the shed master flag OR the purge valve is
    // armed — the purge valve needs the sampled level even when shedding is off.
    if !dynamic_shed_enabled() && !purge_valve_enabled() {
        return;
    }
    STARTED.get_or_init(|| {
        let interval_ms = std::env::var("FRS_DYN_SHED_INTERVAL_MS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&ms| ms > 0)
            .unwrap_or(1000);
        let _ = std::thread::Builder::new()
            .name("frs-mem-pressure".to_string())
            .spawn(move || loop {
                if let Some(level) = sample_pressure_once() {
                    SAMPLED_PRESSURE.store(level as u8, Ordering::Relaxed);
                    // FRS-MEM-PRESSURE-PURGE: under High/Critical, return
                    // jemalloc's retained (already-freed) pages to the OS so
                    // RSS tracks LIVE — the never-OOM valve. No-op when the
                    // valve is unarmed or pressure is below High.
                    maybe_purge(level);
                }
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
            });
    });
}

/// The most-recent sampled level (for diagnostics; ignores the test override).
pub fn sampled_pressure_for_diag() -> PressureLevel {
    level_from_u8(SAMPLED_PRESSURE.load(Ordering::Relaxed))
}

// ===========================================================================
// FRS-MEM-PRESSURE-PURGE (2026-06-16): proactive jemalloc page-reclaim valve.
//
// Root cause (measured, commit 23420c2c0): at the q9/q19/q5 16 g/TM cliff the
// LIVE allocation is tiny (q19 ~1.4 GiB) but jemalloc holds ~6-8 GiB of
// freed-but-unpurged dirty/muzzy pages, so RSS — what the cgroup OOM-killer
// watches — hits the limit and the TM is exit-137 killed. This is ORTHOGONAL
// to the shed levers (leaner config + KV-sep OFF still OOM): the bytes are
// already FREED, jemalloc just hasn't returned them to the OS yet.
//
// The valve: when the sampler observes High/Critical pressure it forces
// jemalloc to release ALL retained dirty+muzzy pages immediately via the void
// `arena.<MALLCTL_ARENAS_ALL>.purge` mallctl. Purge only ever returns
// already-freed memory to the OS — it touches NO live allocation — so it is
// byte-identical / zero correctness impact. The compiled 10 s decay
// (MALLOC_CONF) is left as-is for q4 steady state; this only fires under
// pressure.
//
// Flag: `FRS_MEM_PRESSURE_PURGE=1` (DEFAULT OFF) OR the master `FRS_DYNAMIC_SHED`
// (so the existing armed sweep gets the valve for free). Off ⇒ no purge, no
// extra sampling work — byte-identical to today.
// ===========================================================================

/// Count of proactive purge calls fired so far (FRS_MEM_DIAG evidence). Read
/// lock-free for the diag line; incremented by the sampler thread.
static PURGE_COUNT: AtomicU64 = AtomicU64::new(0);

use std::sync::atomic::AtomicU64;

/// Cached purge-valve arming: 0 = unknown, 1 = off, 2 = on. Armed iff
/// `FRS_MEM_PRESSURE_PURGE` is truthy OR the master `FRS_DYNAMIC_SHED` is on.
static PURGE_ARMED: AtomicU8 = AtomicU8::new(0);

/// FRS-MEM-PRESSURE-PURGE: is the proactive purge valve armed? Armed by its own
/// sub-flag `FRS_MEM_PRESSURE_PURGE=1` OR by the master `FRS_DYNAMIC_SHED=1`
/// (default OFF for both ⇒ no purge). Cached after first read.
pub fn purge_valve_enabled() -> bool {
    match PURGE_ARMED.load(Ordering::Relaxed) {
        1 => return false,
        2 => return true,
        _ => {}
    }
    let on = matches!(
        std::env::var("FRS_MEM_PRESSURE_PURGE").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    ) || dynamic_shed_enabled();
    PURGE_ARMED.store(if on { 2 } else { 1 }, Ordering::Relaxed);
    on
}

/// FRS-MEM-PRESSURE-PURGE: number of proactive purges fired (FRS_MEM_DIAG).
pub fn purge_count() -> u64 {
    PURGE_COUNT.load(Ordering::Relaxed)
}

/// The installed jemalloc-purge hook, or `None` until one is registered.
///
/// The actual mallctl call needs `unsafe` (FFI into jemalloc), but this engine
/// crate is `#![forbid(unsafe_code)]`. So the unsafe purge lives in the
/// `forst-rs-ffi` crate (which links the jemalloc global allocator and permits
/// `unsafe`), and is registered here at startup via [`register_purge_hook`].
/// The hook returns `true` when the purge succeeded. Off Linux / when no hook
/// is installed, the valve is a no-op.
static PURGE_HOOK: std::sync::OnceLock<fn() -> bool> = std::sync::OnceLock::new();

/// FRS-MEM-PRESSURE-PURGE: install the jemalloc page-reclaim hook (idempotent;
/// the first registration wins). Called once by `forst-rs-ffi` at load, since
/// the void `arena.<MALLCTL_ARENAS_ALL>.purge` mallctl requires `unsafe` which
/// this `#![forbid(unsafe_code)]` crate cannot host. The hook must return `true`
/// iff the purge succeeded.
pub fn register_purge_hook(hook: fn() -> bool) {
    let _ = PURGE_HOOK.set(hook);
}

/// Invoke the registered purge hook, if any. `false` when no hook is installed
/// (off Linux, or `forst-rs-ffi` not linked — e.g. an in-process engine test).
fn jemalloc_purge_all() -> bool {
    match PURGE_HOOK.get() {
        Some(hook) => hook(),
        None => false,
    }
}

/// FRS-MEM-PRESSURE-PURGE: if the valve is armed AND pressure is at/above
/// [`PressureLevel::High`], force-purge jemalloc's retained pages and bump the
/// purge counter. Called by the sampler each tick after publishing the level.
/// A no-op when unarmed or below High → byte-identical to today.
fn maybe_purge(level: PressureLevel) {
    if !purge_valve_enabled() {
        return;
    }
    if level < PressureLevel::High {
        return;
    }
    if jemalloc_purge_all() {
        PURGE_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

/// Test-only: directly publish a sampled level (simulates the sampler thread)
/// so the sampler→gate wiring can be exercised without a cgroup. Distinct from
/// [`set_pressure_override`], which bypasses the sampled value entirely.
#[doc(hidden)]
pub fn __test_publish_sampled(level: PressureLevel) {
    SAMPLED_PRESSURE.store(level as u8, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // The override statics are process-global; serialize the tests that flip
    // them so the parallel test runner can't interleave (mirrors db.rs's
    // `WA_V1_TEST_LOCK`).
    static SHED_TEST_LOCK: Mutex<()> = Mutex::new(());

    // Each test fully resets the static overrides it touches so the suite is
    // order-independent (the statics are process-global).
    fn reset() {
        set_dynamic_shed_override(None);
        set_purge_valve_override(None);
        set_pressure_override(None);
        __test_publish_sampled(PressureLevel::Ample);
    }

    #[test]
    fn off_by_default_never_sheds() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        set_dynamic_shed_override(Some(false));
        // Even at Critical pressure, the OFF master flag means no lever sheds.
        set_pressure_override(Some(PressureLevel::Critical));
        for lever in [
            ShedPriority::PersistentProbeIter,
            ShedPriority::CoalesceBuffers,
            ShedPriority::S2Pinned,
            ShedPriority::LeveledHotCf,
            ShedPriority::VlogResident,
        ] {
            assert!(!should_shed(lever), "OFF must never shed: {lever:?}");
        }
        reset();
    }

    #[test]
    fn priority_ladder_sheds_in_order() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        set_dynamic_shed_override(Some(true));

        // Ample: nothing sheds.
        set_pressure_override(Some(PressureLevel::Ample));
        assert!(!should_shed(ShedPriority::PersistentProbeIter));
        assert!(!should_shed(ShedPriority::VlogResident));

        // Elevated: the two heaviest shed; the rest stay.
        set_pressure_override(Some(PressureLevel::Elevated));
        assert!(should_shed(ShedPriority::PersistentProbeIter));
        assert!(should_shed(ShedPriority::CoalesceBuffers));
        assert!(!should_shed(ShedPriority::S2Pinned));
        assert!(!should_shed(ShedPriority::LeveledHotCf));
        assert!(!should_shed(ShedPriority::VlogResident));

        // High: the mid tier joins.
        set_pressure_override(Some(PressureLevel::High));
        assert!(should_shed(ShedPriority::PersistentProbeIter));
        assert!(should_shed(ShedPriority::S2Pinned));
        assert!(should_shed(ShedPriority::LeveledHotCf));
        assert!(!should_shed(ShedPriority::VlogResident));

        // Critical: everything sheddable sheds.
        set_pressure_override(Some(PressureLevel::Critical));
        assert!(should_shed(ShedPriority::VlogResident));
        assert!(should_shed(ShedPriority::LeveledHotCf));

        reset();
    }

    #[test]
    fn sampler_published_level_drives_gate() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        set_dynamic_shed_override(Some(true));
        set_pressure_override(None); // use the sampled value, not the override
        __test_publish_sampled(PressureLevel::High);
        assert_eq!(current_pressure(), PressureLevel::High);
        assert!(should_shed(ShedPriority::S2Pinned));
        assert!(!should_shed(ShedPriority::VlogResident));
        reset();
    }

    #[test]
    fn purge_valve_off_by_default() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        // Neither flag armed ⇒ valve OFF.
        set_dynamic_shed_override(Some(false));
        set_purge_valve_override(Some(false));
        assert!(!purge_valve_enabled());
        reset();
    }

    #[test]
    fn purge_valve_armed_by_sub_flag_or_master() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        // Sub-flag alone arms the valve.
        set_dynamic_shed_override(Some(false));
        set_purge_valve_override(Some(true));
        assert!(purge_valve_enabled());

        // Master shed flag alone also arms the valve (the armed sweep gets the
        // valve for free); its override resets the purge cache so it re-folds.
        set_purge_valve_override(None);
        set_dynamic_shed_override(Some(true));
        assert!(purge_valve_enabled());
        reset();
    }

    #[test]
    fn maybe_purge_only_fires_at_high_or_above() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        set_purge_valve_override(Some(true));

        // Below High: no purge attempt (counter unchanged regardless of OS).
        let before = purge_count();
        maybe_purge(PressureLevel::Ample);
        maybe_purge(PressureLevel::Elevated);
        assert_eq!(purge_count(), before, "must not purge below High");

        // Unarmed: never purges even at Critical.
        set_purge_valve_override(Some(false));
        set_dynamic_shed_override(Some(false));
        let before = purge_count();
        maybe_purge(PressureLevel::Critical);
        assert_eq!(purge_count(), before, "unarmed must never purge");
        reset();
    }

    #[test]
    fn registered_hook_fires_at_high_not_below() {
        let _g = SHED_TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        reset();
        // A registered hook makes the purge "succeed" so the counter advances —
        // proves the sampler→hook wiring fires at High/Critical and NOT below.
        // (The OnceLock hook persists for the process; once installed, later
        // tests still see `false`-returning behaviour only when unarmed/low.)
        register_purge_hook(|| true);
        set_purge_valve_override(Some(true));

        let base = purge_count();
        maybe_purge(PressureLevel::Ample);
        maybe_purge(PressureLevel::Elevated);
        assert_eq!(purge_count(), base, "no purge below High");

        maybe_purge(PressureLevel::High);
        assert_eq!(purge_count(), base + 1, "High must purge once");
        maybe_purge(PressureLevel::Critical);
        assert_eq!(purge_count(), base + 2, "Critical must purge once");
        reset();
    }

    #[test]
    fn level_from_bp_thresholds() {
        assert_eq!(level_from_bp(0), PressureLevel::Ample);
        assert_eq!(level_from_bp(7_499), PressureLevel::Ample);
        assert_eq!(level_from_bp(7_500), PressureLevel::Elevated);
        assert_eq!(level_from_bp(8_499), PressureLevel::Elevated);
        assert_eq!(level_from_bp(8_500), PressureLevel::High);
        assert_eq!(level_from_bp(9_199), PressureLevel::High);
        assert_eq!(level_from_bp(9_200), PressureLevel::Critical);
        assert_eq!(level_from_bp(10_000), PressureLevel::Critical);
    }
}

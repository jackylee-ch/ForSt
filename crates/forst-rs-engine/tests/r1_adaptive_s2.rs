// R1 (2026-06-14, repair design §3-R1): per-scan fan-out-ADAPTIVE S2 selection.
//
// These falsifiers assert the lever is OUTPUT-byte-identical: the adaptive
// arm only changes which proven-equal merge mechanism runs per scan (pinned
// loser-tree vs legacy), never the emitted (key, value) data.
//
// Fixture spans the threshold deliberately: DEEP prefixes overlap many SSTs
// (>= threshold → pinned merge engages) while SHALLOW prefixes overlap one
// (< threshold → legacy path). Both must yield byte-identical rows ON vs OFF.

// The probe helper returns the recorded (key, value) rows + the source count;
// the engine crate allows this at crate level, mirror it for the test target.
#![allow(clippy::type_complexity)]

use std::sync::{Arc, Mutex};

use forst_rs_common::EngineOptions;
use forst_rs_engine::{
    set_s2_adaptive_fanout_min_override, set_s2_pinned_override, DbImpl, FillOutcome, RowSink,
};
use forst_rs_io::{FileSystem, MemoryFileSystem};

/// 4-byte join-key prefix (NOT 8 — a full-length prefix would match only the
/// single round-0 key, defeating the deep-fan-out fixture).
fn ns_prefix(jk: u32) -> [u8; 4] {
    jk.to_be_bytes()
}
fn state_key(jk: u32, entry: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[..4].copy_from_slice(&jk.to_be_bytes());
    k[4..].copy_from_slice(&entry.to_be_bytes());
    k
}

/// Sink that records every (key, value) so we can byte-compare two runs.
struct Recorder(Vec<(Vec<u8>, Vec<u8>)>);
impl RowSink for Recorder {
    fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.0.push((key.to_vec(), value.to_vec()));
        true
    }
}

/// Build a DB where join state for keys `[0, deep_keys)` is spread across
/// `num_ssts` overlapping L0 SSTs (each spanning the full key range, so the
/// coarse range-skip cannot prune → a deep probe sees ~num_ssts sources), and
/// keys `[deep_keys, deep_keys + shallow_keys)` live in exactly ONE SST each
/// (shallow probes). Returns the DB.
///
/// IMPORTANT: a tiny write buffer (so every round flushes a real SST) + a very
/// high L0 compaction trigger (so background compaction does NOT collapse the
/// overlapping SSTs) are BOTH required — otherwise the in-memory harness serves
/// the whole result from one memtable/one compacted SST (1 source) and the deep
/// probe never crosses the adaptive threshold. (Diagnosed empirically: 64 MiB
/// buffer → live=0/sources=1; 4 KiB buffer + trigger 100000 → sources≈num_ssts.)
fn build_mixed_db(deep_keys: u32, shallow_keys: u32, num_ssts: u32) -> Arc<DbImpl> {
    // SAFETY: tests in this binary run the R1 suite single-threaded (one #[test]
    // entry sequencing the checks); env is read fresh per DB open (env_u32).
    std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    std::env::set_var("FRS_L0_STOP_TRIGGER", "100000");
    std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "100000");
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 4096,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    let cf = db.default_cf();

    // Deep keys: written every round → present in every SST.
    for round in 0..num_ssts {
        for jk in 0..deep_keys {
            let v = format!("deep-v{:04}-r{:04}", jk, round);
            db.put(&cf, &state_key(jk, round), v.as_bytes()).unwrap();
        }
        db.flush_cf(&cf).unwrap();
    }
    // Shallow keys: each written once into a single trailing SST.
    for jk in deep_keys..(deep_keys + shallow_keys) {
        let v = format!("shallow-v{:04}", jk);
        db.put(&cf, &state_key(jk, 0), v.as_bytes()).unwrap();
    }
    db.flush_cf(&cf).unwrap();
    db
}

/// Drain one prefix probe through the push-style stream into a Recorder.
/// Returns (rows, source_count) — the source count proves whether the deep
/// probe actually crossed the adaptive fan-out threshold.
fn probe_stream(db: &Arc<DbImpl>, prefix: &[u8]) -> (Vec<(Vec<u8>, Vec<u8>)>, usize) {
    let cf = db.default_cf();
    let mut s = db
        .prefix_scan_stream_with_error_slot(&cf, prefix, Arc::new(Mutex::new(None)))
        .unwrap();
    let mut sink = Recorder(Vec::new());
    assert_eq!(s.fill_into(&mut sink).unwrap(), FillOutcome::Exhausted);
    let sources = s.debug_source_count();
    (sink.0, sources)
}

/// Drain one prefix probe through the legacy owned-arc iterator.
fn probe_iter(db: &Arc<DbImpl>, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let cf = db.default_cf();
    let it = db.prefix_scan_iter_owned_arc(&cf, prefix).unwrap();
    it.map(|r| {
        let (k, v) = r.unwrap();
        (k.to_vec(), v.to_vec())
    })
    .collect()
}

/// All R1 checks run in ONE test: the lever toggles process-global overrides
/// (`set_s2_*_override`), so parallel test functions would race the shared
/// state. Sequencing them in a single binary entry keeps the falsifier
/// deterministic.
#[test]
fn r1_adaptive_s2_suite() {
    r1_adaptive_byte_identical_deep_and_shallow();
    r1_adaptive_active_flag_tracks_threshold();
    r1_boundary_fanout_byte_identical();
}

/// R1 core falsifier: for BOTH a deep probe (crosses the threshold → pinned)
/// and a shallow probe (below → legacy), the adaptive-ON output is byte-
/// identical to the adaptive-OFF (legacy) output.
fn r1_adaptive_byte_identical_deep_and_shallow() {
    let deep_keys = 64u32;
    let shallow_keys = 64u32;
    let num_ssts = 32u32; // deep probe overlaps ~32 SSTs
    let db = build_mixed_db(deep_keys, shallow_keys, num_ssts);

    let deep_prefix = ns_prefix(7); // present in every SST → deep fan-out
    let shallow_prefix = ns_prefix(deep_keys + 5); // one SST → shallow

    // --- Reference (adaptive OFF, threshold = MAX → legacy everywhere) ---
    set_s2_pinned_override(None);
    set_s2_adaptive_fanout_min_override(None); // env unset in tests → MAX
    let deep_off = probe_iter(&db, &deep_prefix);
    let shallow_off = probe_iter(&db, &shallow_prefix);
    assert!(!deep_off.is_empty(), "deep probe must return rows");
    assert_eq!(
        deep_off.len(),
        num_ssts as usize,
        "deep probe should find one entry per SST"
    );
    assert_eq!(shallow_off.len(), 1, "shallow probe should find one entry");

    // --- Adaptive ON, threshold = 8: deep (>=8 sources) crosses → pinned
    //     loser tree; shallow (1 source) stays legacy. Output must be
    //     byte-identical to OFF. ---
    set_s2_adaptive_fanout_min_override(Some(8));
    let (deep_on, deep_sources) = probe_stream(&db, &deep_prefix);
    let (shallow_on, shallow_sources) = probe_stream(&db, &shallow_prefix);

    // Prove the fixture actually exercises BOTH arms (else byte-equality is
    // vacuous): the deep probe must genuinely cross the threshold (>=8 sources
    // → pinned engaged) and the shallow probe must stay below it.
    assert!(
        deep_sources >= 8,
        "deep probe must cross the adaptive threshold (sources={} < 8) — \
         fixture failed to sustain fan-out",
        deep_sources
    );
    assert!(
        shallow_sources < 8,
        "shallow probe must stay below the threshold (sources={} >= 8)",
        shallow_sources
    );

    assert_eq!(
        deep_off, deep_on,
        "R1: deep probe output must be byte-identical adaptive ON (pinned) vs OFF (legacy)"
    );
    assert_eq!(
        shallow_off, shallow_on,
        "R1: shallow probe output must be byte-identical adaptive ON vs OFF"
    );

    // Restore default global state for any subsequent test in the binary.
    set_s2_adaptive_fanout_min_override(None);
}

/// The selection logic itself: confirm the threshold actually discriminates —
/// adaptive resolves to pinned for the deep probe and legacy for the shallow
/// probe (proven indirectly: both still byte-identical, but we also assert the
/// stream path is engaged adaptively, i.e. `s2_adaptive_active()` flips).
fn r1_adaptive_active_flag_tracks_threshold() {
    use forst_rs_engine::s2_adaptive_active;
    set_s2_pinned_override(None);

    set_s2_adaptive_fanout_min_override(None);
    assert!(
        !s2_adaptive_active(),
        "default (threshold MAX) => adaptive inactive => FFI keeps legacy path"
    );

    set_s2_adaptive_fanout_min_override(Some(8));
    assert!(
        s2_adaptive_active(),
        "threshold set => adaptive active => FFI takes the per-scan stream path"
    );

    // Static force ON must NOT report as adaptive (it's Force(true)).
    set_s2_pinned_override(Some(true));
    assert!(
        !s2_adaptive_active(),
        "static force ON is Force(_), not Adaptive"
    );

    // Restore.
    set_s2_pinned_override(None);
    set_s2_adaptive_fanout_min_override(None);
}

/// Cross-check that the AT-boundary and JUST-BELOW-boundary selections both
/// produce byte-identical output. Data-driven: observe the probe's actual
/// runtime fan-out (the in-memory harness's flush/compaction makes the exact
/// SST count non-deterministic), then exercise BOTH arms relative to it.
fn r1_boundary_fanout_byte_identical() {
    let deep_keys = 16u32;
    let num_ssts = 32u32;
    let db = build_mixed_db(deep_keys, 0, num_ssts);
    let prefix = ns_prefix(3);

    set_s2_pinned_override(None);

    // Reference (legacy) + the observed runtime source count.
    set_s2_adaptive_fanout_min_override(None);
    let off = probe_iter(&db, &prefix);
    let (_, sources) = probe_stream(&db, &prefix);
    assert!(
        sources >= 2,
        "boundary fixture needs multi-source fan-out (sources={})",
        sources
    );

    // AT boundary (threshold == sources): pinned arm engages.
    set_s2_adaptive_fanout_min_override(Some(sources));
    let (at, _) = probe_stream(&db, &prefix);
    assert_eq!(off, at, "R1: at-boundary probe byte-identical ON vs OFF");

    // JUST ABOVE the fan-out (threshold == sources + 1): legacy arm.
    set_s2_adaptive_fanout_min_override(Some(sources + 1));
    let (below, _) = probe_stream(&db, &prefix);
    assert_eq!(
        off, below,
        "R1: just-below-threshold probe byte-identical ON vs OFF"
    );

    set_s2_adaptive_fanout_min_override(None);
}

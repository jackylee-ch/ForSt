// Approach 1 (2026-06-15-q7-approach1-leveled-hot-cf-design.md): leveled-bottom
// discipline on hot interval-join probe CFs (FRS_RS_LEVELED_HOT_CF).
//
// These falsifiers assert the lever is a pure LAYOUT/SCHEDULING change:
//   C1 byte-identity — a probe over a CF returns the IDENTICAL (key, value)
//      stream whether the leveled-hot-CF gate is OFF or ON (compaction never
//      changes the visible rows, only how many SSTs they live across);
//   arming — a CF whose runtime probe fan-out crosses the threshold ARMS
//      (its effective L0 rollup trigger tightens below the base), so the
//      background compaction folds its overlapping L0 tail into the leveled
//      bottom level sooner — bounding the per-probe source COUNT;
//   C4 no-shallow-regression — a CF whose probes never fan out deep NEVER arms
//      (it keeps the base trigger / tiered path).
//
// The arming is data-driven (the CF's observed `n_overlap`, the SAME signal the
// R1 S2 selector samples), gated by the flag (default-OFF ⇒ byte/behavior-
// identical). The deep-fan-out fixture mirrors r1_adaptive_s2.rs.

#![allow(clippy::type_complexity)]

use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{set_leveled_hot_cf_override, DbImpl};
use forst_rs_io::{FileSystem, MemoryFileSystem};

/// 4-byte join-key prefix (a deep probe overlaps every round's SST).
fn ns_prefix(jk: u32) -> [u8; 4] {
    jk.to_be_bytes()
}
fn state_key(jk: u32, entry: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[..4].copy_from_slice(&jk.to_be_bytes());
    k[4..].copy_from_slice(&entry.to_be_bytes());
    k
}

/// Build a DB whose join state for keys `[0, deep_keys)` is spread across
/// `num_ssts` overlapping L0 SSTs (each spanning the full key range, so a deep
/// probe sees ~num_ssts sources). The very high L0 triggers keep background
/// compaction from collapsing the fan-out so the probe genuinely fans out deep
/// (mirrors r1_adaptive_s2.rs's fixture rationale).
fn build_deep_db(deep_keys: u32, num_ssts: u32) -> Arc<DbImpl> {
    // SAFETY: this binary runs its checks single-threaded (one #[test] entry).
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
    for round in 0..num_ssts {
        for jk in 0..deep_keys {
            let v = format!("deep-v{:04}-r{:04}", jk, round);
            db.put(&cf, &state_key(jk, round), v.as_bytes()).unwrap();
        }
        db.flush_cf(&cf).unwrap();
    }
    db
}

/// Drain one prefix probe through the owned-arc iterator → recorded rows.
fn probe(db: &Arc<DbImpl>, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let cf = db.default_cf();
    let it = db.prefix_scan_iter_owned_arc(&cf, prefix).unwrap();
    it.map(|r| {
        let (k, v) = r.unwrap();
        (k.to_vec(), v.to_vec())
    })
    .collect()
}

/// All checks in ONE test: the lever toggles a process-global override, so
/// parallel test functions would race the shared state. Sequencing them in a
/// single binary entry keeps the falsifier deterministic.
#[test]
fn leveled_hot_cf_suite() {
    leveled_hot_cf_byte_identical_and_arms();
    leveled_hot_cf_shallow_never_arms();
    leveled_hot_cf_off_never_arms();
}

/// C1 + arming: a deep-fan-out probe returns the byte-identical stream ON vs
/// OFF, AND with the gate ON the CF ARMS (peak fan-out crosses the threshold →
/// effective L0 trigger tightens below the base).
fn leveled_hot_cf_byte_identical_and_arms() {
    let deep_keys = 64u32;
    let num_ssts = 32u32;
    let prefix = ns_prefix(7);

    // --- Reference (gate OFF): the legacy tiered path. ---
    set_leveled_hot_cf_override(Some(false));
    let db_off = build_deep_db(deep_keys, num_ssts);
    let off = probe(&db_off, &prefix);
    assert_eq!(
        off.len(),
        num_ssts as usize,
        "deep probe must find one entry per overlapping SST"
    );
    assert!(
        !db_off.leveled_hot_cf_armed_by_name("default"),
        "gate OFF ⇒ CF must NEVER arm (byte/behavior-identical to legacy)"
    );

    // --- Gate ON: probe arms the CF; output byte-identical to OFF. ---
    set_leveled_hot_cf_override(Some(true));
    std::env::set_var("FRS_RS_LEVELED_HOT_CF_FANOUT_MIN", "8");
    let db_on = build_deep_db(deep_keys, num_ssts);
    let on = probe(&db_on, &prefix);

    assert_eq!(
        off, on,
        "C1: deep probe output must be byte-identical leveled-hot-CF ON vs OFF \
         (a layout/scheduling change never alters the visible rows)"
    );
    // The deep probe drove `note_probe_fanout` past the threshold → armed.
    assert!(
        db_on.leveled_hot_cf_armed_by_name("default"),
        "arming: a deep-fan-out CF (sources ≥ 8) must arm for the leveled-bottom \
         policy when the gate is ON"
    );

    set_leveled_hot_cf_override(None);
    std::env::remove_var("FRS_RS_LEVELED_HOT_CF_FANOUT_MIN");
}

/// C4: a shallow CF (probes overlap < threshold sources) must NEVER arm even
/// with the gate ON — no shallow regression (q8/q12/q17 stay tiered).
fn leveled_hot_cf_shallow_never_arms() {
    // One round → one SST → a probe overlaps exactly ONE source (< threshold 8).
    let db = build_deep_db(64, 1);
    set_leveled_hot_cf_override(Some(true));
    std::env::set_var("FRS_RS_LEVELED_HOT_CF_FANOUT_MIN", "8");

    let prefix = ns_prefix(7);
    let rows = probe(&db, &prefix);
    assert_eq!(rows.len(), 1, "shallow fixture: probe overlaps one SST");
    assert!(
        !db.leveled_hot_cf_armed_by_name("default"),
        "C4: a shallow CF (fan-out < threshold) must NOT arm — no shallow regression"
    );

    set_leveled_hot_cf_override(None);
    std::env::remove_var("FRS_RS_LEVELED_HOT_CF_FANOUT_MIN");
}

/// With the gate OFF, even a deep-fan-out probe must leave the CF unarmed — the
/// arming path is never taken (one env read, no CF lookup, no atomic), so a build
/// with the flag OFF is behavior-identical to the legacy engine.
fn leveled_hot_cf_off_never_arms() {
    set_leveled_hot_cf_override(Some(false));
    let db = build_deep_db(64, 32);
    let _ = probe(&db, &ns_prefix(7)); // deep probe, but gate is OFF
    assert!(
        !db.leveled_hot_cf_armed_by_name("default"),
        "gate OFF ⇒ deep probe must not arm the CF"
    );
    set_leveled_hot_cf_override(None);
}

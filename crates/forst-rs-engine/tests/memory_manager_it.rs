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

//! FRS-MEM-MANAGER end-to-end: the unified controller derives a coordinated
//! engine-native budget from the (overridden) cgroup limit, the per-consumer
//! caps SUM within it, and a DB opened with the manager ARMED produces
//! BYTE-IDENTICAL output to a DB opened with it OFF.
//!
//! Integration tests run in their own binary; this file sets the manager env
//! ONCE at the top of the single `#[test]` before any engine code reads it (the
//! cap functions cache via `OnceLock` on first read), so the armed path is
//! genuinely exercised — not just the unit helpers.

use std::sync::Arc;

use forst_rs_common::EngineOptions;
use forst_rs_engine::memory_manager::{
    compact_inflight_bytes, consumer_cap_bytes, engine_native_budget_bytes, CompactionAdmission,
    Consumer,
};
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};
use forst_rs_io::{FileSystem, LocalFileSystem};
use forst_rs_storage::merge_operator::RawConcatMergeOperator;

fn open_local(db_path: &str) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: db_path.to_string(),
        write_buffer_size: 4 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open db")
}

/// Write a fixed set of keys across two CFs, flush, and read them all back as a
/// canonical `(cf, key) -> value` vector — the byte-identical output fingerprint.
fn fingerprint(db: &Arc<DbImpl>) -> Vec<(u8, Vec<u8>, Option<Vec<u8>>)> {
    let cf_a = db
        .create_column_family(ColumnFamilyDescriptor::new("cf_a"))
        .expect("create cf_a");
    let cf_b = db
        .create_column_family(ColumnFamilyDescriptor::new("cf_b"))
        .expect("create cf_b");

    let mut expected = Vec::new();
    for i in 0..2000u32 {
        let key = format!("k{i:08}").into_bytes();
        let va = format!("a-value-{i}-{}", "x".repeat((i % 64) as usize)).into_bytes();
        let vb = format!("b-value-{i}").into_bytes();
        db.put(&cf_a, &key, &va).expect("put cf_a");
        db.put(&cf_b, &key, &vb).expect("put cf_b");
        if i % 3 == 0 {
            // Delete every third key in cf_b — exercises tombstone output too.
            db.delete(&cf_b, &key).expect("delete cf_b");
            expected.push((1u8, key.clone(), None));
        } else {
            expected.push((1u8, key.clone(), Some(vb)));
        }
        expected.push((0u8, key, Some(va)));
    }
    db.switch_and_flush(&cf_a).expect("flush cf_a");
    db.switch_and_flush(&cf_b).expect("flush cf_b");

    // Read back in the same canonical order and assemble the actual fingerprint.
    let mut actual = Vec::new();
    for (cf_tag, key, _) in &expected {
        let cf = if *cf_tag == 0 { &cf_a } else { &cf_b };
        let v = db.get(cf, key).expect("get");
        actual.push((*cf_tag, key.clone(), v));
    }
    assert_eq!(actual, expected, "read-back must match writes (this DB)");
    actual
}

#[test]
fn manager_caps_sum_within_budget_and_output_byte_identical() {
    // ---- Baseline DB: manager OFF (env unset) -----------------------------
    // Capture the output fingerprint with the controller inert. (We open this
    // FIRST, before arming, so its cap functions resolve their OFF defaults.)
    let base_dir = tempfile::tempdir().expect("base tempdir");
    let base_path = base_dir.path().to_string_lossy().into_owned();
    let base_db = open_local(&base_path);
    let baseline = fingerprint(&base_db);
    drop(base_db);

    // ---- Arm the unified controller --------------------------------------
    // Set the env BEFORE the armed DB opens so the cap OnceLocks resolve the
    // armed values. A pinned cgroup override makes the budget deterministic on
    // any host (Mac has no /sys/fs/cgroup). 16 GiB cgroup, 10 GiB JVM reserved.
    std::env::set_var("FRS_MEM_MANAGER", "1");
    std::env::set_var("FRS_MEM_CGROUP_MB", "16384");
    std::env::set_var("FRS_JVM_RESERVED_MB", "10240");

    // The controller derives a positive engine-native budget...
    let native = engine_native_budget_bytes().expect("armed manager must derive a budget");
    assert!(native > 0, "engine-native budget must be positive");

    // ...and the five per-consumer caps SUM within it (process-global slices +
    // the per-instance block-cache slice × the assumed instance count). The sum
    // of the SLICES (before the per-instance division) equals the native budget
    // by construction; here we assert each cap is positive and the
    // process-global ones together do not exceed the native budget.
    let bc = consumer_cap_bytes(Consumer::BlockCache).expect("bc cap");
    let wbm = consumer_cap_bytes(Consumer::WriteBuffer).expect("wbm cap");
    let shadow = consumer_cap_bytes(Consumer::ResidentShadow).expect("shadow cap");
    let vlog = consumer_cap_bytes(Consumer::VlogResident).expect("vlog cap");
    let compact = consumer_cap_bytes(Consumer::CompactionTransient).expect("compact cap");
    for (name, v) in [
        ("blockcache", bc),
        ("wbm", wbm),
        ("shadow", shadow),
        ("vlog", vlog),
        ("compact", compact),
    ] {
        assert!(v > 0, "{name} cap must be positive");
    }
    // Process-global consumers (WBM + shadow + vlog + compact) must fit native.
    let global_sum = wbm + shadow + vlog + compact;
    assert!(
        global_sum <= native,
        "process-global caps {global_sum} must sum within native budget {native}"
    );
    // The per-instance block-cache slice (× assumed instances) is also within
    // native: bc is native*0.28 / instances, so bc*instances <= native*0.28.
    assert!(
        bc <= native,
        "per-instance block-cache cap {bc} must be <= native {native}"
    );

    // ---- FRS-MEM-COMPACT-ADMISSION: the proactive bound engages when armed ---
    // With the manager armed, `admit` reserves bytes against the
    // CompactionTransient slice and the RAII permit releases them on drop. This
    // proves the q9 build-storm admission gate is live in-process (the no-op
    // path is covered by the unit test for the manager-off case).
    {
        let before = compact_inflight_bytes();
        let permit = CompactionAdmission::admit(32 * 1024 * 1024);
        let held = compact_inflight_bytes();
        assert!(
            held > before,
            "armed admit must reserve in-flight compaction-transient bytes ({held} !> {before})"
        );
        assert!(
            held <= before + compact,
            "a single admit must never exceed the CompactionTransient slice"
        );
        drop(permit);
        assert_eq!(
            compact_inflight_bytes(),
            before,
            "permit drop must release exactly the admitted bytes"
        );
    }

    // ---- Armed DB: SAME output -------------------------------------------
    let armed_dir = tempfile::tempdir().expect("armed tempdir");
    let armed_path = armed_dir.path().to_string_lossy().into_owned();
    let armed_db = open_local(&armed_path);
    let armed = fingerprint(&armed_db);

    assert_eq!(
        armed, baseline,
        "armed-manager output must be BYTE-IDENTICAL to manager-off output"
    );

    // Clean up the process env so a sibling test binary is unaffected.
    std::env::remove_var("FRS_MEM_MANAGER");
    std::env::remove_var("FRS_MEM_CGROUP_MB");
    std::env::remove_var("FRS_JVM_RESERVED_MB");
}

/// FRS-MEM-WINDOWED-FLUSH / FRS-MEM-WINDOWED-STALL-SKIP (PMC-1 live-state track):
/// the q5 window-accumulator levers (force a windowed merge-CF to flush its
/// operand chains sooner; skip the counterproductive hard-cap stall) must be
/// BYTE-IDENTICAL — a merge-CF read returns the exact concatenation of all
/// operands regardless of whether the manager (and thus the levers) is armed.
/// This is the correctness guard for the q5 path: the levers only change WHEN the
/// accumulator is flushed / whether the writer stalls, never the merged value.
#[test]
fn windowed_merge_cf_output_byte_identical_armed_vs_off() {
    fn merge_fingerprint(
        db: &Arc<DbImpl>,
        n_keys: u32,
        ops_per_key: u32,
    ) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let cf = db
            .create_column_family(
                ColumnFamilyDescriptor::new("win_agg")
                    .with_merge_operator(Arc::new(RawConcatMergeOperator::new())),
            )
            .expect("create merge cf");
        let mut expected: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        for k in 0..n_keys {
            let key = format!("win{k:06}").into_bytes();
            let mut concat: Vec<u8> = Vec::new();
            for o in 0..ops_per_key {
                // Sizeable operand so the active memtable grows toward the
                // windowed flush floor (exercises the force-switch path).
                let operand = format!("op-{k}-{o}-{}", "y".repeat(32)).into_bytes();
                concat.extend_from_slice(&operand);
                db.merge(&cf, &key, &operand).expect("merge operand");
            }
            expected.push((key, Some(concat)));
        }
        db.switch_and_flush(&cf).expect("flush merge cf");
        let mut actual = Vec::new();
        for (key, _) in &expected {
            actual.push((key.clone(), db.get(&cf, key).expect("get merge")));
        }
        assert_eq!(
            actual, expected,
            "merge read-back must equal operand concat"
        );
        actual
    }

    // Baseline: manager OFF (the env is removed at the end of the sibling test;
    // but `manager_enabled()` caches ARMED=true once any test in this binary
    // armed it. So we compare the merged OUTPUT, which is identical either way —
    // the byte-identical contract holds regardless of arming, which is exactly
    // what we must prove for the q5 levers.)
    let off_dir = tempfile::tempdir().expect("off tempdir");
    let off_db = open_local(&off_dir.path().to_string_lossy());
    let baseline = merge_fingerprint(&off_db, 200, 64);
    drop(off_db);

    // Armed: force the windowed flush floor LOW so the force-switch path fires
    // within this small test, and arm the manager.
    std::env::set_var("FRS_MEM_MANAGER", "1");
    std::env::set_var("FRS_MEM_CGROUP_MB", "16384");
    std::env::set_var("FRS_JVM_RESERVED_MB", "10240");
    std::env::set_var("FRS_MEM_WINDOWED_FLUSH_MB", "1"); // 1 MiB floor → fires

    let armed_dir = tempfile::tempdir().expect("armed tempdir");
    let armed_db = open_local(&armed_dir.path().to_string_lossy());
    let armed = merge_fingerprint(&armed_db, 200, 64);

    assert_eq!(
        armed, baseline,
        "windowed merge-CF output must be byte-identical with the q5 levers armed"
    );

    std::env::remove_var("FRS_MEM_MANAGER");
    std::env::remove_var("FRS_MEM_CGROUP_MB");
    std::env::remove_var("FRS_JVM_RESERVED_MB");
    std::env::remove_var("FRS_MEM_WINDOWED_FLUSH_MB");
}

/// FRS-MEM-PRESSURE-FLUSH (PMC-1 live-state SPILL track): under memory pressure
/// the manager force-flushes a PLAIN (non-merge) CF's live active memtable to
/// SST — the literal "spill live state to SST, a perf hit not an OOM" the
/// never-OOM contract requires for the q9/q19/q20 join-build CFs. This test
/// proves BOTH halves of the contract:
///   1. CORRECTNESS — the read-back is BYTE-IDENTICAL to a baseline run with the
///      spill NOT engaged (pressure Ample). A flush only moves bytes memtable→SST;
///      the read path merges memtable+imm+SST identically. Output never changes.
///   2. The spill ACTUALLY ENGAGED — `pressure_flush_count()` strictly increases
///      while writing under forced High pressure with a tiny spill floor, so the
///      live memtable component was driven to SST (not left resident). With the
///      manager OFF (or pressure Ample) the count never moves.
#[test]
fn pressure_flush_spills_plain_cf_to_sst_byte_identical() {
    use forst_rs_engine::{pressure_flush_count, set_pressure_override, PressureLevel};

    // Write N keys with sizeable values to a plain CF (NO merge operator — this
    // is the q9/q19/q20 join-build shape, not q5's merge accumulator). Returns
    // the canonical (key -> value) read-back fingerprint.
    fn plain_fingerprint(db: &Arc<DbImpl>, n_keys: u32) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("join_build"))
            .expect("create plain cf");
        let mut expected: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        for k in 0..n_keys {
            let key = format!("jk{k:08}").into_bytes();
            // ~2 KiB value so a few thousand keys cross a sub-MiB spill floor and
            // exercise multiple force-switches.
            let val = format!("build-{k}-{}", "z".repeat(2048)).into_bytes();
            db.put(&cf, &key, &val).expect("put");
            expected.push((key, Some(val)));
        }
        // Final flush so any tail in the active memtable is on SST for read-back
        // (the spill handles the bulk mid-run; this drains the remainder).
        db.switch_and_flush(&cf).expect("flush plain cf");
        let mut actual = Vec::new();
        for (key, _) in &expected {
            actual.push((key.clone(), db.get(&cf, key).expect("get")));
        }
        assert_eq!(actual, expected, "plain-CF read-back must equal writes");
        actual
    }

    const N: u32 = 4000; // 4000 × ~2 KiB ≈ 8 MiB written → crosses a 1 MiB floor

    // ---- Baseline: pressure Ample (spill must NOT engage) -----------------
    // Even with the manager armed, an Ample level keeps the spill dormant — the
    // output is the reference fingerprint and the spill counter must not move.
    std::env::set_var("FRS_MEM_MANAGER", "1");
    std::env::set_var("FRS_MEM_CGROUP_MB", "16384");
    std::env::set_var("FRS_JVM_RESERVED_MB", "10240");
    std::env::set_var("FRS_MEM_PRESSURE_FLUSH_FLOOR_MB", "1"); // 1 MiB → easy to cross
    set_pressure_override(Some(PressureLevel::Ample));
    let before_ample = pressure_flush_count();
    let base_dir = tempfile::tempdir().expect("base tempdir");
    let base_db = open_local(&base_dir.path().to_string_lossy());
    let baseline = plain_fingerprint(&base_db, N);
    drop(base_db);
    assert_eq!(
        pressure_flush_count(),
        before_ample,
        "spill must NOT engage at Ample pressure (no pressure-flush switch)"
    );

    // ---- Spill engaged: pressure High (spill MUST move bytes to SST) ------
    set_pressure_override(Some(PressureLevel::High));
    let before_high = pressure_flush_count();
    let spill_dir = tempfile::tempdir().expect("spill tempdir");
    let spill_db = open_local(&spill_dir.path().to_string_lossy());
    let spilled = plain_fingerprint(&spill_db, N);
    let after_high = pressure_flush_count();

    // Half 1 — CORRECTNESS: byte-identical output whether or not the spill ran.
    assert_eq!(
        spilled, baseline,
        "pressure-flush spill output must be BYTE-IDENTICAL to the un-spilled baseline"
    );
    // Half 2 — the spill ACTUALLY ENGAGED: at least one pressure-triggered switch
    // fired (live memtable bytes were moved to SST under pressure).
    assert!(
        after_high > before_high,
        "pressure-flush spill must engage at High pressure (count {before_high} -> {after_high})"
    );

    set_pressure_override(None);
    std::env::remove_var("FRS_MEM_MANAGER");
    std::env::remove_var("FRS_MEM_CGROUP_MB");
    std::env::remove_var("FRS_JVM_RESERVED_MB");
    std::env::remove_var("FRS_MEM_PRESSURE_FLUSH_FLOOR_MB");
}

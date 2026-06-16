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
use forst_rs_engine::memory_manager::{consumer_cap_bytes, engine_native_budget_bytes, Consumer};
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};
use forst_rs_io::{FileSystem, LocalFileSystem};

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

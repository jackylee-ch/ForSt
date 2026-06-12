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

//! Phase-2 disagg Stage-5 gate (partial): cache-pressure soak on the REAL
//! engine read path with a local file-cache budget deliberately ≪ state —
//! the 2026-05-31 q9 failure mode (state past the cache budget living
//! remote-only) as a regression test, plus the FRS-CACHE-ADMISSION /
//! FRS-CACHE-BG-EXEMPT machinery exercised end-to-end (engine →
//! CachedFileSystem → LocalCache) instead of only at the storage unit level.
//!
//! Bar (per the Phase-2 design §4.2): functional correctness + no-collapse
//! under cache pressure — byte-exact reads in BOTH policy cells, churn
//! bounded, and the default cell's admission machinery fully inert. S3
//! latency wins are Phase 3 (this runs on the opendal `memory://`
//! fs-emulation substrate per the standing dev-box rule).

use std::path::Path;
use std::sync::Arc;
use std::sync::Once;

use forst_rs_common::error::ForstResult;
use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, OpendalFileSystem};
use forst_rs_storage::cached_fs::CachedFileSystem;
use forst_rs_storage::local_cache::{AdmissionParams, CachePolicy, LocalCache};

const N_KEYS: usize = 1024;
const VAL_LEN: usize = 4096; // ~4 MiB total state
const KEYS_PER_SST: usize = 32; // → 32 SSTs of ~128 KiB
const CACHE_BUDGET: u64 = 256 * 1024; // holds ~2 of 32 SSTs: state ≫ budget

/// The engine keeps just-flushed memtables RAM-resident (the 1 GiB/CF
/// "resident shadow"), which would serve every read from RAM and bypass the
/// file cache entirely at this state size. Clamp it to the 1 MiB minimum so
/// reads actually reach the SST/cache stack (the regime under test). Set
/// once, before any engine is opened, for every test in this binary.
fn clamp_resident_shadow() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        std::env::set_var("FRS_RESIDENT_SHADOW_MB", "1");
    });
}

fn key_for(i: usize) -> Vec<u8> {
    format!("pressure_key_{i:05}").into_bytes()
}

fn val_for(i: usize) -> Vec<u8> {
    let mut v = format!("pressure_val_{i:05}_").into_bytes();
    // Deterministic xorshift stream per key: INCOMPRESSIBLE filler, so SSTs
    // stay ~full-size on disk and the tiny cache budget is real pressure
    // (regular patterns compress to a few KiB and defeat the test premise).
    let mut s = (i as u32).wrapping_mul(0x9E37_79B9) | 1;
    while v.len() < VAL_LEN {
        s ^= s << 13;
        s ^= s >> 17;
        s ^= s << 5;
        v.extend_from_slice(&s.to_le_bytes());
    }
    v.truncate(VAL_LEN);
    v
}

/// Stands up an engine over a `memory://` remote fronted by a tiny
/// LocalCache with the given policy (the open_remote composition, with an
/// explicit policy instead of env flags so cells can run in one process),
/// writes the full keyspace across many SSTs, and returns the handles.
/// The returned TempDir keeps the cache directory alive.
type PressuredCell = (
    Arc<DbImpl>,
    Arc<LocalCache>,
    Arc<dyn FileSystem>,
    tempfile::TempDir,
);

fn build_pressured_db(policy: CachePolicy) -> ForstResult<PressuredCell> {
    clamp_resident_shadow();
    let cache_dir = tempfile::TempDir::new().expect("cache tempdir");
    let remote: Arc<dyn FileSystem> = Arc::new(OpendalFileSystem::memory()?);
    remote.create_dir_all(Path::new("/db-pressure"))?;
    let cache = Arc::new(
        LocalCache::open_with_policy(cache_dir.path(), CACHE_BUDGET, policy)
            .expect("open local cache"),
    );
    let fs: Arc<dyn FileSystem> = Arc::new(CachedFileSystem::new(
        Arc::clone(&remote),
        Arc::clone(&cache),
    ));

    let opts = EngineOptions {
        db_path: "/db-pressure".to_string(),
        // Larger than one explicit-flush chunk so SST boundaries are the
        // explicit switch_and_flush calls below (deterministic SST count).
        write_buffer_size: 1024 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open_with_fs(opts, fs)?;
    let cf = db.default_cf();

    // Write the keyspace, flushing every KEYS_PER_SST keys so state lands in
    // many SSTs (each flush write-throughs its SST into the tiny cache,
    // churning earlier ones out — exactly the budget ≪ state regime).
    for i in 0..N_KEYS {
        db.put(&cf, &key_for(i), &val_for(i))?;
        if (i + 1) % KEYS_PER_SST == 0 {
            db.switch_and_flush(&cf)?; // synchronous flush
        }
    }
    Ok((db, cache, remote, cache_dir))
}

/// Reads every key 3× via `get` and once via `batch_get`; asserts byte-exact
/// values throughout (failures panic with cell context).
fn read_soak_byte_exact(db: &Arc<DbImpl>, cell: &str) {
    let cf = db.default_cf();
    for pass in 0..3 {
        for i in 0..N_KEYS {
            let got = db
                .get(&cf, &key_for(i))
                .unwrap_or_else(|e| panic!("[{cell}] pass {pass} get key {i}: {e}"));
            assert_eq!(
                got.as_deref(),
                Some(val_for(i).as_slice()),
                "[{cell}] pass {pass} key {i}: wrong/missing value under cache pressure"
            );
        }
    }
    // Batch path (exercises prefetch_sst_files_for_batch → the gated
    // concurrent warm) — byte-exact for a stride covering every SST.
    let keys: Vec<Vec<u8>> = (0..N_KEYS).step_by(7).map(key_for).collect();
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let got = db.batch_get(&cf, &key_refs).expect("batch_get");
    for (j, i) in (0..N_KEYS).step_by(7).enumerate() {
        assert_eq!(
            got[j].as_deref(),
            Some(val_for(i).as_slice()),
            "[{cell}] batch_get key {i}: wrong/missing value"
        );
    }
}

#[test]
fn legacy_default_policy_byte_exact_under_cache_pressure() {
    // Default-OFF cell: the q9-2026-05-31 regression shape — state ≫ cache,
    // every read must still be byte-exact (pass-through + re-fetch), and the
    // cache must respect its budget while churning.
    let (db, cache, _remote, _cache_dir) =
        build_pressured_db(CachePolicy::default()).expect("build legacy cell");
    read_soak_byte_exact(&db, "legacy");
    assert!(
        cache.current_bytes() <= cache.capacity_bytes(),
        "cache exceeded its budget: {} > {}",
        cache.current_bytes(),
        cache.capacity_bytes()
    );
    // Default policy: the admission machinery must be fully inert.
    assert_eq!(cache.admission_stats(), (0, 0));
    assert_eq!(cache.bg_stats(), (0, 0));
}

/// Lists the engine's SST paths from the remote, normalized to the
/// leading-slash form the engine (and thus the cache keys) uses — opendal
/// `list_dir` returns relative entry paths.
fn sst_paths(remote: &Arc<dyn FileSystem>) -> Vec<std::path::PathBuf> {
    let metas = remote.list_dir(Path::new("/db-pressure")).expect("list");
    let mut out = Vec::new();
    for m in &metas {
        if m.is_dir {
            continue;
        }
        let s = m.path.to_str().expect("utf8 path");
        if s.ends_with(".sst") {
            out.push(std::path::PathBuf::from(format!(
                "/{}",
                s.trim_start_matches('/')
            )));
        }
    }
    out
}

#[test]
fn admission_policy_byte_exact_and_gate_engages_on_demand_reads() {
    // FRS-CACHE-BG-EXEMPT + FRS-CACHE-ADMISSION cell: identical correctness
    // bar to the legacy cell — the flags must never produce wrong/missing
    // values through the real engine read stack.
    //
    // Note on gate coverage: the engine opens SST readers EAGERLY at flush
    // time (against the write-through copy), and a post-eviction read goes
    // through the reader's direct remote fallback — neither path is a cache
    // demand-FILL, so engine point reads alone may never consult admission
    // in this composition. The demand-fill regime that admission governs is
    // checkpoint-staging / restore-class whole-file reads (and readers
    // opened post-eviction); we drive it below through the SAME
    // CachedFileSystem handle the engine uses.
    let policy = CachePolicy {
        background_exempt: true,
        admission: Some(AdmissionParams::default()),
    };
    let (db, cache, remote, _cache_dir) = build_pressured_db(policy).expect("build admission cell");
    let fs: Arc<dyn FileSystem> = Arc::new(CachedFileSystem::new(
        Arc::clone(&remote),
        Arc::clone(&cache),
    ));
    read_soak_byte_exact(&db, "admission");
    assert!(
        cache.current_bytes() <= cache.capacity_bytes(),
        "cache exceeded its budget: {} > {}",
        cache.current_bytes(),
        cache.capacity_bytes()
    );

    // Checkpoint-staging-class sweep: sequential whole-file demand reads of
    // every SST. Most SSTs are long-evicted (budget holds ~2 of 32), so the
    // first sweep must hit the gate (first-touch rejections) while still
    // serving the bytes (size-validated against the remote metadata inside
    // fetch_through_cache; an admitted-but-truncated entry would error).
    let paths = sst_paths(&remote);
    assert!(paths.len() >= 16, "expected many SSTs, got {}", paths.len());
    for p in &paths {
        let mut r = fs.open_sequential_file(p).expect("staging read");
        let mut buf = vec![0u8; 64 * 1024];
        let mut total = 0usize;
        loop {
            let n = r.read(&mut buf).expect("read");
            if n == 0 {
                break;
            }
            total += n;
        }
        assert!(total > 0, "SST {} read empty", p.display());
    }
    let (admitted, rejected) = cache.admission_stats();
    assert!(
        rejected > 0,
        "a cold staging sweep over {} SSTs with budget ≪ state must reject \
         first-touch fills (admitted={admitted} rejected={rejected})",
        paths.len()
    );
    assert!(
        admitted <= rejected,
        "admission must suppress most churn (admitted={admitted} rejected={rejected})"
    );
    assert!(
        cache.current_bytes() <= cache.capacity_bytes(),
        "budget bound must survive the staging sweep"
    );
}

#[test]
fn cold_cache_eviction_regime_reads_byte_exact_with_admission() {
    // The post-eviction / restore-read regime: every write-through entry is
    // invalidated (as if the whole hot set was churned out), then the FULL
    // keyspace is read back with admission ON. Reads traverse the remote
    // fallback paths (LocalFirstSstFile fallback + chunked remote reads)
    // and must stay byte-exact — the "evicted hot set must not mean wrong
    // or missing values" half of the q9 regression.
    let policy = CachePolicy {
        background_exempt: true,
        admission: Some(AdmissionParams::default()),
    };
    let (db, cache, remote, _cache_dir) =
        build_pressured_db(policy).expect("build cold-cache cell");

    // Invalidate every whole-file SST entry the write-through admitted
    // (cache keys are the engine's leading-slash path strings).
    let mut invalidated = 0usize;
    for p in sst_paths(&remote) {
        let key = p.to_str().expect("utf8");
        if cache.invalidate(key).expect("invalidate") {
            invalidated += 1;
        }
    }
    assert!(
        invalidated > 0,
        "expected at least one write-through SST entry to invalidate"
    );

    let cf = db.default_cf();
    for i in 0..N_KEYS {
        let got = db.get(&cf, &key_for(i)).expect("get after cold-cache");
        assert_eq!(
            got.as_deref(),
            Some(val_for(i).as_slice()),
            "key {i} wrong/missing in the cold-cache eviction regime"
        );
    }
}

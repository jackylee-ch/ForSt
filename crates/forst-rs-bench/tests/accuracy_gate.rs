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

//! Engine-level NEXMark accuracy gate: ForSt-RS engine vs the vendored RocksDB
//! baseline, on a DETERMINISTIC fixed-seed workload, with a byte-level
//! comparison of the final materialized state.
//!
//! # Why this exists
//!
//! The faithful end-to-end accuracy gate is the fixed-CSV NEXMark harness
//! (`scripts/accuracy-gate/`), but it needs a full Flink distribution + the
//! disaggregated async backend + a JDK25 cluster, so it only runs on a
//! self-hosted / box runner. This test is the *hosted-GHA-viable* smallest
//! faithful variant: it drives the SAME key/value engine path that the NEXMark
//! windowed queries exercise — keyed-aggregation state churn (q5 hop window /
//! q11 session window: per-key counters rewritten every event, tombstoned at
//! window expiry) plus the full-range scan that fires on every window — through
//! both engines on identical input, and asserts the materialized state is
//! byte-identical. Any divergence in put/delete/overwrite/scan/visibility
//! ordering between the engines fails the job.
//!
//! Gated behind the `rocksdb-baseline` Cargo feature (the `rocksdb` crate
//! vendors the C++ sources, ~15 min cold build) — same feature as
//! `benches/rocksdb_compare.rs`.
//!
//! Run:
//! ```bash
//! cargo test -p forst-rs-bench --features rocksdb-baseline --test accuracy_gate -- --nocapture
//! ```
//!
//! Scale: `FRS_ACC_KEYS` (default 2000 distinct keys) × `FRS_ACC_EVENTS`
//! (default 100_000 events) — the 100K NEXMark CI scale. The window-fire
//! cadence (`FRS_ACC_WINDOW`, default every 2000 events) keeps ~50 window
//! boundaries over the run, matching the GEN_TPS=1000 fixed-CSV profile where
//! 100K events span ~100s of event time → ~50 hop windows (the documented
//! degeneracy trap: at too-coarse a cadence everything collapses to one window
//! and a windowed bug is masked).

#![cfg(feature = "rocksdb-baseline")]

use std::collections::BTreeMap;
use std::sync::Arc;

use forst_rs_bench::{create_cf, open_in_memory};
use rocksdb::{ColumnFamilyDescriptor as RocksCfDesc, Options as RocksOpts, DB as RocksDb};
use tempfile::TempDir;

/// Deterministic 64-bit LCG (no external rng crate — reproducibility is the
/// whole point; the same seed must yield byte-identical input on every runner).
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed)
    }
    fn next(&mut self) -> u64 {
        // Numerical Recipes constants.
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// One deterministic NEXMark-shaped event.
#[derive(Clone)]
enum Event {
    /// New bid for an auction key: rewrite the per-key counter (+price).
    Bid { key: u64, price: u64 },
    /// Window expiry for a key: delete its state (tombstone).
    Expire { key: u64 },
}

/// Build the fixed event stream ONCE; both engines replay the identical Vec.
fn build_events(keys: u64, events: usize, window: usize, seed: u64) -> Vec<Event> {
    let mut rng = Lcg::new(seed);
    let mut out = Vec::with_capacity(events + events / window + 1);
    for i in 0..events {
        let key = rng.below(keys);
        let price = 1 + rng.below(1000);
        out.push(Event::Bid { key, price });
        // At each window boundary, expire a deterministic slice of keys —
        // this is the tombstone + version-churn pattern q5/q11 produce.
        if window > 0 && (i + 1) % window == 0 {
            let expired = rng.below(keys);
            out.push(Event::Expire { key: expired });
        }
    }
    out
}

/// Apply the stream to a counter map by NEXMark windowed-agg semantics:
/// Bid adds price to the running per-key sum; Expire removes the key.
/// `flush` is invoked at each window boundary so the engine actually moves
/// state through memtable→SST (read-amp / merge path), matching the harness.
fn run_forst_rs(events: &[Event], window: usize) -> BTreeMap<u64, u64> {
    let db = open_in_memory(4 * 1024 * 1024);
    let cf = create_cf(&db, "acc-cf");
    let mut applied = 0usize;
    for ev in events {
        match ev {
            Event::Bid { key, price } => {
                let kb = key.to_be_bytes();
                let cur = db
                    .get(&cf, &kb)
                    .expect("frs get")
                    .map(|v| u64::from_be_bytes(v[..].try_into().unwrap()))
                    .unwrap_or(0);
                let next = cur + price;
                db.put(&cf, &kb, &next.to_be_bytes()).expect("frs put");
                applied += 1;
                if window > 0 && applied.is_multiple_of(window) {
                    let _ = db.switch_and_flush(&cf);
                }
            }
            Event::Expire { key } => {
                db.delete(&cf, &key.to_be_bytes()).expect("frs delete");
            }
        }
    }
    // Materialize final state via full-range scan (the window-fire read path).
    let mut out = BTreeMap::new();
    let slot = Arc::new(std::sync::Mutex::new(None));
    let it = db
        .scan_iter_owned_arc_with_error_slot(&cf, b"", None, slot)
        .expect("frs scan");
    for r in it {
        let (k, v) = r.expect("frs row");
        let key = u64::from_be_bytes(k[..].try_into().unwrap());
        let val = u64::from_be_bytes(v[..].try_into().unwrap());
        out.insert(key, val);
    }
    out
}

fn run_rocksdb(events: &[Event], window: usize) -> BTreeMap<u64, u64> {
    let tmp = TempDir::new().expect("tempdir");
    let mut opts = RocksOpts::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    let db = Arc::new(
        RocksDb::open_cf_descriptors(
            &opts,
            tmp.path(),
            vec![RocksCfDesc::new("default", RocksOpts::default())],
        )
        .expect("rocksdb open"),
    );
    let cf = db.cf_handle("default").expect("rocksdb cf");
    let mut applied = 0usize;
    for ev in events {
        match ev {
            Event::Bid { key, price } => {
                let kb = key.to_be_bytes();
                let cur = db
                    .get_cf(&cf, kb)
                    .expect("rocksdb get")
                    .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
                    .unwrap_or(0);
                let next = cur + price;
                db.put_cf(&cf, kb, next.to_be_bytes()).expect("rocksdb put");
                applied += 1;
                if window > 0 && applied.is_multiple_of(window) {
                    db.flush_cf(&cf).expect("rocksdb flush");
                }
            }
            Event::Expire { key } => {
                db.delete_cf(&cf, key.to_be_bytes())
                    .expect("rocksdb delete");
            }
        }
    }
    let mut out = BTreeMap::new();
    let it = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
    for item in it {
        let (k, v) = item.expect("rocksdb row");
        let key = u64::from_be_bytes(k.as_ref().try_into().unwrap());
        let val = u64::from_be_bytes(v.as_ref().try_into().unwrap());
        out.insert(key, val);
    }
    out
}

/// FNV-1a digest of the sorted (key,value) materialized state — the verdict
/// hash. Cheap, dependency-free, and stable across runners.
fn digest(state: &BTreeMap<u64, u64>) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for (k, v) in state {
        for b in k.to_be_bytes().iter().chain(v.to_be_bytes().iter()) {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
    }
    h
}

fn envu(name: &str, dflt: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(dflt)
}

#[test]
fn nexmark_windowed_state_matches_rocksdb() {
    let keys = envu("FRS_ACC_KEYS", 2_000);
    let events = envu("FRS_ACC_EVENTS", 100_000) as usize;
    let window = envu("FRS_ACC_WINDOW", 2_000) as usize;
    let seed = envu("FRS_ACC_SEED", 0x5eed_1234_abcd_0001);

    let stream = build_events(keys, events, window, seed);
    eprintln!(
        "accuracy-gate: keys={keys} events={events} window={window} seed={seed:#x} \
         stream_len={}",
        stream.len()
    );

    let frs = run_forst_rs(&stream, window);
    let rdb = run_rocksdb(&stream, window);

    let frs_h = digest(&frs);
    let rdb_h = digest(&rdb);
    eprintln!(
        "accuracy-gate: frs_keys={} rdb_keys={} frs_sha={frs_h:#018x} rdb_sha={rdb_h:#018x}",
        frs.len(),
        rdb.len()
    );

    // Guard against the degeneracy trap: a window cadence that collapses all
    // state into nothing (or never churns) would make the comparison vacuous.
    assert!(
        !frs.is_empty(),
        "materialized state is empty — workload degenerate (check FRS_ACC_WINDOW/KEYS)"
    );

    if frs != rdb {
        // Print up to 20 diverging keys for triage, mirroring compare-print.py.
        let mut diffs = 0;
        for key in frs
            .keys()
            .chain(rdb.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            let f = frs.get(key).copied();
            let r = rdb.get(key).copied();
            if f != r {
                if diffs < 20 {
                    eprintln!("  DIFF key={key}: frs={f:?} rdb={r:?}");
                }
                diffs += 1;
            }
        }
        panic!(
            "ACCURACY MISMATCH: {diffs} diverging keys; frs_sha={frs_h:#018x} rdb_sha={rdb_h:#018x}"
        );
    }

    assert_eq!(
        frs_h, rdb_h,
        "verdict hash mismatch despite equal maps (impossible)"
    );
    eprintln!(
        "accuracy-gate: EQUAL — {} keys, verdict {frs_h:#018x}",
        frs.len()
    );
}

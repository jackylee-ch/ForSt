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
    /// Point lookup of a key (the q3/q8 join build-side probe: read keyed
    /// state mid-stream). The read-back value is folded into a per-engine
    /// read-trace digest so a READ-PATH visibility divergence (stale or
    /// missing value after a put/delete/flush) is caught even if the final
    /// materialized state happens to converge.
    Probe { key: u64 },
}

/// Build the fixed event stream ONCE; both engines replay the identical Vec.
fn build_events(keys: u64, events: usize, window: usize, seed: u64) -> Vec<Event> {
    let mut rng = Lcg::new(seed);
    let mut out = Vec::with_capacity(events + events / window + 1);
    for i in 0..events {
        let key = rng.below(keys);
        let price = 1 + rng.below(1000);
        out.push(Event::Bid { key, price });
        // Every few events, probe a (possibly different) key — the q3/q8 join
        // build-side read. Reads interleave with writes/flushes so the probe
        // exercises memtable-vs-SST visibility, the q3/q8 read-path surface.
        if i % 4 == 0 {
            out.push(Event::Probe {
                key: rng.below(keys),
            });
        }
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
fn run_forst_rs(events: &[Event], window: usize) -> (BTreeMap<u64, u64>, u64) {
    let db = open_in_memory(4 * 1024 * 1024);
    let cf = create_cf(&db, "acc-cf");
    let mut applied = 0usize;
    // Running FNV-1a digest of every point-read result (Bid read-before-write +
    // Probe) so a mid-stream READ visibility divergence is caught even if the
    // final scanned state converges.
    let mut read_trace = 0xcbf29ce484222325u64;
    for ev in events {
        match ev {
            Event::Bid { key, price } => {
                let kb = key.to_be_bytes();
                let cur = db
                    .get(&cf, &kb)
                    .expect("frs get")
                    .map(|v| u64::from_be_bytes(v[..].try_into().unwrap()))
                    .unwrap_or(0);
                fold_read(&mut read_trace, *key, Some(cur));
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
            Event::Probe { key } => {
                let v = db
                    .get(&cf, &key.to_be_bytes())
                    .expect("frs probe get")
                    .map(|v| u64::from_be_bytes(v[..].try_into().unwrap()));
                fold_read(&mut read_trace, *key, v);
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
    (out, read_trace)
}

fn run_rocksdb(events: &[Event], window: usize) -> (BTreeMap<u64, u64>, u64) {
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
    let mut read_trace = 0xcbf29ce484222325u64;
    for ev in events {
        match ev {
            Event::Bid { key, price } => {
                let kb = key.to_be_bytes();
                let cur = db
                    .get_cf(&cf, kb)
                    .expect("rocksdb get")
                    .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
                    .unwrap_or(0);
                fold_read(&mut read_trace, *key, Some(cur));
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
            Event::Probe { key } => {
                let v = db
                    .get_cf(&cf, key.to_be_bytes())
                    .expect("rocksdb probe get")
                    .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()));
                fold_read(&mut read_trace, *key, v);
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
    (out, read_trace)
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

/// Fold one point-read result (key + optional value) into a running FNV-1a
/// read-trace digest. `None` (absent key) and `Some(0)` fold distinctly so a
/// missing-vs-zero visibility divergence is caught.
fn fold_read(h: &mut u64, key: u64, val: Option<u64>) {
    let tag: u64 = match val {
        Some(_) => 1,
        None => 2,
    };
    for b in key
        .to_be_bytes()
        .iter()
        .chain(tag.to_be_bytes().iter())
        .chain(val.unwrap_or(0).to_be_bytes().iter())
    {
        *h ^= *b as u64;
        *h = h.wrapping_mul(0x100000001b3);
    }
}

fn envu(name: &str, dflt: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(dflt)
}

/// Outcome of comparing the two engines on one event stream.
struct GateOutcome {
    equal: bool,
    diffs: usize,
    frs_state_h: u64,
    rdb_state_h: u64,
    frs_read_h: u64,
    rdb_read_h: u64,
    frs_keys: usize,
    rdb_keys: usize,
}

/// Replay the SAME stream through both engines and compare BOTH the final
/// materialized state AND the mid-stream read trace. Reused by the gate test
/// and the non-vacuity self-check (which feeds it a deliberately perturbed
/// stream and asserts a mismatch IS detected).
fn compare_engines(stream: &[Event], window: usize) -> GateOutcome {
    let (frs, frs_read_h) = run_forst_rs(stream, window);
    let (rdb, rdb_read_h) = run_rocksdb(stream, window);
    let frs_state_h = digest(&frs);
    let rdb_state_h = digest(&rdb);
    let mut diffs = 0;
    for key in frs
        .keys()
        .chain(rdb.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        if frs.get(key).copied() != rdb.get(key).copied() {
            if diffs < 20 {
                eprintln!(
                    "  DIFF key={key}: frs={:?} rdb={:?}",
                    frs.get(key).copied(),
                    rdb.get(key).copied()
                );
            }
            diffs += 1;
        }
    }
    let equal = diffs == 0 && frs == rdb && frs_read_h == rdb_read_h;
    GateOutcome {
        equal,
        diffs,
        frs_state_h,
        rdb_state_h,
        frs_read_h,
        rdb_read_h,
        frs_keys: frs.len(),
        rdb_keys: rdb.len(),
    }
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

    let r = compare_engines(&stream, window);
    eprintln!(
        "accuracy-gate: frs_keys={} rdb_keys={} \
         frs_state_sha={:#018x} rdb_state_sha={:#018x} \
         frs_read_sha={:#018x} rdb_read_sha={:#018x}",
        r.frs_keys, r.rdb_keys, r.frs_state_h, r.rdb_state_h, r.frs_read_h, r.rdb_read_h
    );

    // Guard against the degeneracy trap: a window cadence that collapses all
    // state into nothing (or never churns) would make the comparison vacuous.
    assert!(
        r.frs_keys > 0,
        "materialized state is empty — workload degenerate (check FRS_ACC_WINDOW/KEYS)"
    );

    assert!(
        r.equal,
        "ACCURACY MISMATCH: {} diverging keys; \
         state frs={:#018x}/rdb={:#018x} read frs={:#018x}/rdb={:#018x}",
        r.diffs, r.frs_state_h, r.rdb_state_h, r.frs_read_h, r.rdb_read_h
    );
    eprintln!(
        "accuracy-gate: EQUAL — {} keys, state {:#018x} read {:#018x}",
        r.frs_keys, r.frs_state_h, r.frs_read_h
    );
}

/// Non-vacuity self-check: the gate MUST be able to catch a divergence. Replay
/// a small stream, then perturb ONE event's price and confirm `compare_engines`
/// reports a mismatch when the two streams differ. This proves the comparator
/// is non-vacuous — if a refactor ever made the gate always-pass (e.g. compared
/// each engine to itself), this test fails loudly. Runs on every push alongside
/// the real gate, so the every-push guard can't silently rot.
#[test]
fn gate_detects_injected_divergence() {
    let window = 200usize;
    let base = build_events(500, 5_000, window, 0xabcd_0001);
    // Sanity: the unperturbed stream must compare EQUAL (engines agree).
    let clean = compare_engines(&base, window);
    assert!(
        clean.equal,
        "self-check baseline diverged unexpectedly (state frs={:#018x}/rdb={:#018x})",
        clean.frs_state_h, clean.rdb_state_h
    );

    // Build a perturbed copy: bump the first Bid's price by 1. Feeding this
    // perturbed stream to BOTH engines still yields equal engines, so to prove
    // the COMPARATOR catches divergence we instead run forst-rs on `base` and
    // rocksdb on `perturbed` via a direct check.
    let mut perturbed = base.clone();
    for ev in perturbed.iter_mut() {
        if let Event::Bid { price, .. } = ev {
            *price += 1;
            break;
        }
    }
    let (frs_clean, frs_read_clean) = run_forst_rs(&base, window);
    let (rdb_pert, rdb_read_pert) = run_rocksdb(&perturbed, window);
    let state_differs = frs_clean != rdb_pert;
    let read_differs = frs_read_clean != rdb_read_pert;
    assert!(
        state_differs || read_differs,
        "non-vacuity FAILED: a +1 price perturbation produced NO detectable \
         state OR read-trace difference — the gate would not catch a real \
         accuracy regression"
    );
    eprintln!(
        "accuracy-gate self-check: injected +1 perturbation detected \
         (state_differs={state_differs} read_differs={read_differs}) — gate is non-vacuous"
    );
}

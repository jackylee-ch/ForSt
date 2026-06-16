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

//! q9 REALISTIC interval-join probe mini-bench (PMC-1, 2026-06-16).
//!
//! The prior q9 probe benches (`join_probe_open.rs`, `persistent_probe_iter.rs`)
//! use the ADVERSARIAL "every SST spans the full join-key range" fixture, where
//! NO prune can fire and every probe genuinely fans out over all N SSTs. That
//! over-states the real q9 cost and is why it looked "structural / O(N²)".
//!
//! q9 is an interval join `A.id = B.auction AND B.dateTime BETWEEN A.dateTime
//! AND A.expires` → ROW_NUMBER()=1 (max bid per auction). In Flink's
//! IntervalJoin the auction side is buffered in keyed MapState (key = auction id)
//! and each arriving bid probes the buffer for its auction id. Crucially:
//!
//!   * Auctions arrive over TIME, so a given auction id's rows are NOT in every
//!     SST — they cluster in the few SSTs flushed while that auction was live.
//!   * The same auction is probed REPEATEDLY (every bid for it), so the probe
//!     pattern has key locality within a temporal window.
//!
//! So the REALISTIC per-probe fan-out is bounded by the number of SSTs whose key
//! range overlaps ONE auction id — which the prefix-bloom + range prune should
//! collapse to a handful regardless of total SST count. This bench measures the
//! REALIZED effective source count + per-probe latency under that regime, with
//! the leveled-hot-CF lever OFF vs ON, to answer: does the shipped engine stack
//! give a FLAT per-probe curve on the real q9 shape (the "can q9 beat RocksDB on
//! the read path" question), or does a residual fan-out survive?
//!
//! Run: `cargo run -p forst-rs-bench --release --bin q9_interval_join_probe`
//!      `... -- --keys 65536 --ssts 128 --keys-per-sst 1024 --probes 200000`

use std::sync::{Arc, Mutex};
use std::time::Instant;

use forst_rs_common::config::EngineOptions;
use forst_rs_engine::{set_leveled_hot_cf_override, DbImpl, FillOutcome, RowSink};
use forst_rs_io::{FileSystem, MemoryFileSystem};

fn parse(args: &[String], flag: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut s = self.0;
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.0 = s;
        s
    }
}

/// Byte-counting no-op sink: isolates open + merge + drain, no copies.
struct CountBytes {
    bytes: u64,
    rows: u64,
}
impl RowSink for CountBytes {
    fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.bytes += (key.len() + value.len()) as u64;
        self.rows += 1;
        true
    }
}

/// State key `[auction-id BE 8][bid-seq BE 8]`; probe prefix = the 8-byte
/// auction id (mirrors Flink's `[keygroup][ns][user-key]` composite where the
/// probe namespace+key is the join key).
fn state_key(auction: u64, seq: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&auction.to_be_bytes());
    k[8..].copy_from_slice(&seq.to_be_bytes());
    k
}
fn auction_prefix(auction: u64) -> [u8; 8] {
    auction.to_be_bytes()
}

/// Build a DB whose join state lives across `ssts` flushed L0 SSTs, where each
/// SST holds a CONTIGUOUS WINDOW of `keys_per_sst` auction ids (the temporal
/// clustering of the real interval join) rather than the full key range. SST `s`
/// covers auctions `[s*stride .. s*stride + keys_per_sst)` so adjacent SSTs
/// overlap only at the window edges — a probe for auction `a` overlaps only the
/// ~`keys_per_sst/stride` SSTs whose window contains `a`. With `keys_per_sst ==
/// stride` the windows tile exactly: 1 SST per auction (the bloom/range prune
/// ceiling). We make windows OVERLAP slightly (stride < keys_per_sst) so a probe
/// realistically straddles a few SSTs, like auctions live across a few flushes.
fn build_realistic(
    num_keys: u64,
    ssts: u64,
    keys_per_sst: u64,
    bids_per_auction: u64,
    high_l0_trigger: bool,
) -> Arc<DbImpl> {
    if high_l0_trigger {
        // Sustain the L0 fan-out (the long-running-join transient) for the OFF arm.
        std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "1000000");
        std::env::set_var("FRS_L0_STOP_TRIGGER", "1000000");
        std::env::set_var("FRS_L0_SLOWDOWN_TRIGGER", "1000000");
    }
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 64 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    let cf = db.default_cf();
    let val = vec![0xCDu8; 48];

    // stride: how far each SST's window advances. Slight overlap (stride =
    // keys_per_sst * 3/4) so a probe straddles ~1-2 SSTs (realistic), and the
    // windows together span num_keys.
    let stride = (keys_per_sst * 3 / 4).max(1);
    for s in 0..ssts {
        let base = (s * stride) % num_keys.max(1);
        for off in 0..keys_per_sst {
            let auction = (base + off) % num_keys.max(1);
            for seq in 0..bids_per_auction {
                db.put(&cf, &state_key(auction, s * bids_per_auction + seq), &val)
                    .expect("put");
            }
        }
        db.flush_cf(&cf).expect("flush");
    }
    if high_l0_trigger {
        std::env::remove_var("FRS_L0_COMPACTION_TRIGGER");
        std::env::remove_var("FRS_L0_STOP_TRIGGER");
        std::env::remove_var("FRS_L0_SLOWDOWN_TRIGGER");
    }
    db
}

/// Drain one probe via the real stream API; returns (rows, located_source_count).
fn probe(db: &Arc<DbImpl>, prefix: &[u8]) -> (u64, usize) {
    let cf = db.default_cf();
    let mut stream = db
        .prefix_scan_stream_with_error_slot(&cf, prefix, Arc::new(Mutex::new(None)))
        .expect("open stream");
    let n_src = stream.debug_source_count();
    let mut sink = CountBytes { bytes: 0, rows: 0 };
    let outcome = stream.fill_into(&mut sink).expect("fill_into");
    assert_eq!(outcome, FillOutcome::Exhausted);
    (sink.rows, n_src)
}

fn run_arm(
    label: &str,
    leveled: bool,
    num_keys: u64,
    ssts: u64,
    keys_per_sst: u64,
    bids_per_auction: u64,
    probes: u64,
) {
    set_leveled_hot_cf_override(Some(leveled));
    // Leveled arm: let compaction actually roll the L0 up (normal trigger), and
    // arm the CF by forcing the fan-out threshold low so the lever engages.
    let high_l0 = !leveled;
    if leveled {
        std::env::set_var("FRS_RS_LEVELED_HOT_CF_FANOUT_MIN", "2");
        std::env::set_var("FRS_RS_LEVELED_HOT_CF_L0_TRIGGER", "4");
    }
    let db = build_realistic(num_keys, ssts, keys_per_sst, bids_per_auction, high_l0);

    // Warm a representative probe so the CF arms (note_probe_fanout) and, for the
    // leveled arm, trigger a compaction roll-up so the steady state is leveled.
    for a in 0..num_keys.min(2048) {
        let _ = probe(&db, &auction_prefix(a));
    }
    if leveled {
        // Drive the rollup the armed CF would converge to in steady state.
        db.compact_all().ok();
    }

    // Measure: random auctions probed `probes` times (uniform; the real query
    // has temporal locality, so uniform is the PESSIMISTIC fan-out case).
    let mut prng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut total_rows = 0u64;
    let mut src_sum = 0u64;
    let mut src_max = 0usize;
    let sample_every = (probes / 4096).max(1);
    let t = Instant::now();
    for i in 0..probes {
        let a = prng.next() % num_keys.max(1);
        if i % sample_every == 0 {
            let (rows, n_src) = probe(&db, &auction_prefix(a));
            total_rows += rows;
            src_sum += n_src as u64;
            src_max = src_max.max(n_src);
        } else {
            let (rows, _) = probe(&db, &auction_prefix(a));
            total_rows += rows;
        }
    }
    let elapsed = t.elapsed();
    let samples = probes / sample_every + 1;
    let per_probe = elapsed.as_secs_f64() * 1e9 / probes as f64;
    println!(
        "  {label:<18} {per_probe:>8.1} ns/probe   {:>8.3}s   avg_src {:>5.2}  max_src {:>3}  rows/probe {:>5.1}",
        elapsed.as_secs_f64(),
        src_sum as f64 / samples as f64,
        src_max,
        total_rows as f64 / probes as f64,
    );

    drop(db);
    set_leveled_hot_cf_override(None);
    std::env::remove_var("FRS_RS_LEVELED_HOT_CF_FANOUT_MIN");
    std::env::remove_var("FRS_RS_LEVELED_HOT_CF_L0_TRIGGER");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let num_keys = parse(&args, "--keys", 65536) as u64;
    let ssts = parse(&args, "--ssts", 128) as u64;
    let keys_per_sst = parse(&args, "--keys-per-sst", 1024) as u64;
    let bids_per_auction = parse(&args, "--bids", 2) as u64;
    let probes = parse(&args, "--probes", 200_000) as u64;

    // Prefix bloom ON (the q9 read-amp lever) for both arms — measure the lever
    // stack as it actually ships.
    println!("=== q9 REALISTIC interval-join probe (scattered keys, bloom-pruned) ===");
    println!(
        "keys={num_keys}  ssts={ssts}  keys_per_sst={keys_per_sst}  bids/auction={bids_per_auction}  probes={probes}\n"
    );
    println!(
        "  (avg_src = avg located source count per probe; the read-amp the merge fans over)\n"
    );

    run_arm(
        "leveled OFF (sust)",
        false,
        num_keys,
        ssts,
        keys_per_sst,
        bids_per_auction,
        probes,
    );
    run_arm(
        "leveled ON",
        true,
        num_keys,
        ssts,
        keys_per_sst,
        bids_per_auction,
        probes,
    );
}

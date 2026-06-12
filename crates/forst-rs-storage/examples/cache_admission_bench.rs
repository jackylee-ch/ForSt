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

//! FRS-CACHE-ADMISSION / FRS-CACHE-BG-EXEMPT minibench (Phase-2 disagg,
//! ForSt mechanisms §2.1.1-2.1.4 of the 2026-06-13 competitive analysis).
//!
//! Workload model: an operator HOT SET that fits the cache budget, probed
//! continuously by foreground threads, with (a) periodic BACKGROUND
//! compaction-class scans over a large input set and (b) foreground COLD
//! scans over a set ≫ budget (the paper's §5.4 thrash scenario). The metric
//! that matters is the FOREGROUND HOT-SET HIT RATE — the term that collapses
//! when compaction or cold scans are allowed to evict the operator hot set —
//! plus cache-fill churn (bytes written into the cache = local-disk write
//! interference).
//!
//! Four cells: legacy LRU | +bg-exempt | +admission | +both.
//!
//! Run: `cargo run -p forst-rs-storage --release --example cache_admission_bench`

use std::time::Instant;

use forst_rs_storage::local_cache::{AdmissionParams, CachePolicy, LocalCache};
use forst_rs_storage::requester::BackgroundScope;

const FILE_KB: usize = 64; // per-file payload
const BUDGET_FILES: u64 = 64; // cache budget = 64 files (4 MiB)
const HOT_FILES: usize = 32; // operator hot set: half the budget
const SCAN_FILES: usize = 128; // background/cold scan set: 2x the budget
const ROUNDS: usize = 12;
const HOT_PROBES_PER_ROUND: usize = 8; // hot-set sweeps between scans

struct CellResult {
    name: &'static str,
    fg_hit_rate: f64,
    fg_hits: u64,
    fg_misses: u64,
    fills: u64,
    fill_mb: f64,
    elapsed_ms: u128,
}

/// One simulated demand read through the CachedFileSystem contract:
/// hit serves; miss serves pass-through and fills only when admitted
/// (admit_read_fill is `true` always when the admission policy is off —
/// the legacy behavior).
fn demand_read(cache: &LocalCache, key: &str, payload: &[u8], fills: &mut u64) {
    if cache.get(key).expect("cache get").is_some() {
        return;
    }
    if cache.admit_read_fill(key) {
        cache.put(key, payload).expect("cache put");
        *fills += 1;
    }
}

fn run_cell(name: &'static str, policy: CachePolicy) -> CellResult {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let budget = BUDGET_FILES * (FILE_KB as u64) * 1024;
    let cache = LocalCache::open_with_policy(tmp.path(), budget, policy).expect("open cache");
    let payload = vec![0xCDu8; FILE_KB * 1024];

    // Warm the hot set the way production does: write-through at flush time
    // (unconditional put — write-only admission).
    for i in 0..HOT_FILES {
        cache
            .put(&format!("/db/hot-{i:04}.sst"), &payload)
            .expect("warm put");
    }

    let mut fills = 0u64;
    let started = Instant::now();
    let mut fg_hot_hits = 0u64;
    let mut fg_hot_misses = 0u64;

    for round in 0..ROUNDS {
        // Foreground operator probes of the hot set.
        for _ in 0..HOT_PROBES_PER_ROUND {
            for i in 0..HOT_FILES {
                let key = format!("/db/hot-{i:04}.sst");
                if cache.get(&key).expect("get").is_some() {
                    fg_hot_hits += 1;
                } else {
                    fg_hot_misses += 1;
                    // Re-fault the hot file (operator pays the remote read);
                    // production re-fills via the demand path.
                    if cache.admit_read_fill(&key) {
                        cache.put(&key, &payload).expect("refill put");
                        fills += 1;
                    }
                }
            }
        }
        // BACKGROUND compaction-class scan over the large input set.
        {
            let _bg = BackgroundScope::enter();
            for i in 0..SCAN_FILES {
                let key = format!("/db/scan-{i:04}.sst");
                demand_read(&cache, &key, &payload, &mut fills);
            }
        }
        // Foreground COLD scan (checkpoint-staging / one-off long scan class)
        // every 3rd round: working set 2x budget — the paper thrash scenario.
        if round % 3 == 2 {
            for i in 0..SCAN_FILES {
                let key = format!("/db/cold-{i:04}.sst");
                demand_read(&cache, &key, &payload, &mut fills);
            }
        }
    }

    let total = fg_hot_hits + fg_hot_misses;
    CellResult {
        name,
        fg_hit_rate: if total == 0 {
            0.0
        } else {
            fg_hot_hits as f64 * 100.0 / total as f64
        },
        fg_hits: fg_hot_hits,
        fg_misses: fg_hot_misses,
        fills,
        fill_mb: fills as f64 * (FILE_KB as f64) / 1024.0,
        elapsed_ms: started.elapsed().as_millis(),
    }
}

fn main() {
    let admission = Some(AdmissionParams::default());
    let cells = [
        (
            "legacy-lru",
            CachePolicy {
                background_exempt: false,
                admission: None,
            },
        ),
        (
            "bg-exempt",
            CachePolicy {
                background_exempt: true,
                admission: None,
            },
        ),
        (
            "admission",
            CachePolicy {
                background_exempt: false,
                admission,
            },
        ),
        (
            "bg-exempt+admission",
            CachePolicy {
                background_exempt: true,
                admission,
            },
        ),
    ];

    println!(
        "ADMBENCH config: file_kb={FILE_KB} budget_files={BUDGET_FILES} hot={HOT_FILES} \
         scan={SCAN_FILES} rounds={ROUNDS} hot_probes_per_round={HOT_PROBES_PER_ROUND}"
    );
    for (name, policy) in cells {
        let r = run_cell(name, policy);
        println!(
            "ADMBENCH cell={} fg_hot_hit_rate={:.1}% fg_hits={} fg_misses={} \
             fills={} fill_mb={:.1} elapsed_ms={}",
            r.name, r.fg_hit_rate, r.fg_hits, r.fg_misses, r.fills, r.fill_mb, r.elapsed_ms
        );
    }
}

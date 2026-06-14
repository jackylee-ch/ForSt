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

//! FRS-PHASE2 verify-before-build micro-bench — **cache-admission decay policy:
//! FIFO stand-in vs ForSt epoch-decayed cold-list counts**.
//!
//! ## The question this answers (catalog item #2.3)
//!
//! `AdmissionTracker` (`forst-rs-storage/src/local_cache.rs:74-77,142-177`) uses
//! **FIFO aging** as the decay stand-in for ForSt's epoch-decayed cold-list
//! counts (`FileBasedCache.java:377-387`, `secondAccessEpoch`). The catalog
//! (`2026-06-13-forst-optimization-catalog.md` §2.3) flagged the true epoch
//! decay as *"insurance, not a binding lever"* and required a
//! **contention-robust synthetic Zipf access-trace hit-rate micro-bench**
//! (FIFO stand-in vs epoch-decay) BEFORE building it — implement only if the
//! hit-rate benefit is material.
//!
//! ## What the two policies are
//!
//! Both share ForSt's count-to-promote + promoteLimit admission
//! (`access_before_promote` touches before a read-miss fill is admitted; a key
//! evicted `promote_limit`+ times is BLOCKED from re-admission — the anti-thrash
//! cap). They differ ONLY in how the per-key **eviction count** (the thrash
//! signal that drives the block) decays:
//!
//! - **FIFO stand-in (shipped):** an eviction record ages out only when
//!   `tracker_cap` *distinct* keys have since been evicted and push it out of
//!   the FIFO deque. With `tracker_cap` ≫ working-set the record effectively
//!   **never decays** — once a key crosses `promote_limit` evictions it stays
//!   blocked for the rest of the run.
//! - **Epoch decay (ForSt):** the eviction count is halved every `epoch_evicts`
//!   global evictions (a cheap sliding-window decay modelling ForSt's
//!   position-drift reset). A key that was hot-then-cold-then-hot-again sheds
//!   its stale thrash credit and can be re-admitted when it becomes hot again.
//!
//! ## Why this matters for hit-rate
//!
//! Under a Zipf access trace with **working set ≫ cache** the medium-hot tail
//! oscillates in and out of the cache. With non-decaying FIFO a medium-hot key
//! that briefly thrashed early gets *permanently* blocked and can never be
//! re-cached even after it becomes genuinely hot — every future access misses.
//! Epoch decay lets that credit fade so the key is re-admitted. The bench
//! measures whether this difference is **material** (>= ~2 pp hit-rate) or
//! **marginal** (< ~1 pp) on the box's regime.
//!
//! ## What is MODELED (and what is NOT)
//!
//! - Pure CPU simulation: an LRU set of `cache_keys` capacity, an access trace
//!   drawn Zipf(`theta`) over `key_space` keys, replayed `ops` times. No I/O,
//!   no NexMark, no engine.
//! - The LRU + admission decision logic mirrors `LocalCache`: a read-miss only
//!   *fills* (and may evict) when admission says admit; admission counts
//!   foreground touches and consults the per-key eviction count vs
//!   `promote_limit`. Eviction bumps the per-key eviction count.
//! - "Contention-robust": the SAME deterministic trace (seeded xorshift) is
//!   replayed against BOTH policies, and we sweep multiple `theta`/cache-ratio
//!   cells, so the verdict is not a single-point artifact.
//! - Hit-rate is the ONLY metric — it is the cache-accuracy lever the catalog
//!   asked about; throughput/latency are not modelled (the decay policy is a
//!   pure accuracy mechanism, not a timing one).
//!
//! ## Usage
//!
//! ```text
//! cargo run -p forst-rs-bench --bin cache_admission_epoch --release
//! cargo run -p forst-rs-bench --bin cache_admission_epoch --release -- --smoke
//! ```
//! Env overrides (all optional): `FRS_BENCH_OPS`, `FRS_BENCH_KEYSPACE`,
//! `FRS_BENCH_CACHE_RATIO` (cache = ratio x keyspace), `FRS_BENCH_THETA`,
//! `FRS_BENCH_PROMOTE`, `FRS_BENCH_EVICT_LIMIT`, `FRS_BENCH_EPOCH_EVICTS`.

use std::collections::{HashMap, VecDeque};

/// Deterministic, dependency-free PRNG (xorshift64*) so the trace is identical
/// across both policy arms and across runs (contention-robust).
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn next_f64(&mut self) -> f64 {
        // 53-bit mantissa in [0,1)
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// Precomputed Zipf sampler over `n` items with skew `theta` (theta=0 uniform,
/// higher = more skewed). Builds the CDF once, then binary-searches per draw.
struct Zipf {
    cdf: Vec<f64>,
}
impl Zipf {
    fn new(n: usize, theta: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut acc = 0.0f64;
        for i in 1..=n {
            acc += 1.0 / (i as f64).powf(theta);
            cdf.push(acc);
        }
        let total = acc;
        for v in cdf.iter_mut() {
            *v /= total;
        }
        Zipf { cdf }
    }
    /// Returns a rank in `[0, n)` (0 = hottest).
    fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.next_f64();
        match self
            .cdf
            .binary_search_by(|p| p.partial_cmp(&u).unwrap_or(std::cmp::Ordering::Less))
        {
            Ok(i) => i,
            Err(i) => i.min(self.cdf.len() - 1),
        }
    }
}

/// How the eviction-count (thrash signal) decays.
#[derive(Clone, Copy, PartialEq)]
enum Decay {
    /// FIFO stand-in: never decays within the run (tracker_cap >> key_space).
    Fifo,
    /// ForSt epoch: halve every key's eviction count every `epoch_evicts`
    /// global evictions.
    Epoch(u64),
}

struct Params {
    cache_keys: usize,
    access_before_promote: u32,
    promote_limit: u32,
}

/// One LRU + admission simulation. Returns the foreground hit-rate.
fn simulate(trace: &[usize], p: &Params, decay: Decay) -> f64 {
    // LRU as a deque oldest->newest + membership set.
    let mut lru: VecDeque<usize> = VecDeque::with_capacity(p.cache_keys + 1);
    let mut resident: HashMap<usize, ()> = HashMap::new();
    // Admission state (mirrors AdmissionTracker, keyed by usize for speed).
    let mut counts: HashMap<usize, u32> = HashMap::new();
    let mut evictions: HashMap<usize, u32> = HashMap::new();
    let mut global_evicts: u64 = 0;
    let mut hits: u64 = 0;
    let mut total: u64 = 0;

    let touch_lru = |lru: &mut VecDeque<usize>, k: usize| {
        if let Some(pos) = lru.iter().position(|&x| x == k) {
            lru.remove(pos);
        }
        lru.push_back(k);
    };

    for &k in trace {
        total += 1;
        if resident.contains_key(&k) {
            hits += 1;
            touch_lru(&mut lru, k);
            continue;
        }
        // MISS - consult admission.
        let blocked = evictions.get(&k).copied().unwrap_or(0) >= p.promote_limit;
        let admit = if blocked {
            false
        } else {
            let c = counts.entry(k).or_insert(0);
            *c += 1;
            if *c >= p.access_before_promote {
                counts.remove(&k);
                true
            } else {
                false
            }
        };
        if !admit {
            continue;
        }
        // Fill: insert, evicting LRU front if over capacity.
        resident.insert(k, ());
        touch_lru(&mut lru, k);
        while lru.len() > p.cache_keys {
            if let Some(victim) = lru.pop_front() {
                resident.remove(&victim);
                *evictions.entry(victim).or_insert(0) += 1;
                global_evicts += 1;
                // Epoch decay: every `epoch_evicts` global evictions, halve
                // every key's eviction count (the sliding-window decay).
                if let Decay::Epoch(period) = decay {
                    if period > 0 && global_evicts.is_multiple_of(period) {
                        for v in evictions.values_mut() {
                            *v >>= 1;
                        }
                        evictions.retain(|_, v| *v > 0);
                    }
                }
            }
        }
    }
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn build_trace(ops: usize, key_space: usize, theta: f64, seed: u64) -> Vec<usize> {
    let z = Zipf::new(key_space, theta);
    let mut rng = Rng::new(seed);
    // Shuffle rank->key so the hot ranks are not all low-numbered keys (avoids
    // any accidental ordering correlation with the LRU); a fixed permutation.
    let mut perm: Vec<usize> = (0..key_space).collect();
    for i in (1..key_space).rev() {
        let j = (rng.next_u64() as usize) % (i + 1);
        perm.swap(i, j);
    }
    let mut trace = Vec::with_capacity(ops);
    for _ in 0..ops {
        let rank = z.sample(&mut rng);
        trace.push(perm[rank]);
    }
    trace
}

fn main() {
    let smoke = std::env::args().any(|a| a == "--smoke");

    let ops = env_usize("FRS_BENCH_OPS", if smoke { 200_000 } else { 4_000_000 });
    let key_space = env_usize("FRS_BENCH_KEYSPACE", 100_000);
    let cache_ratio = env_f64("FRS_BENCH_CACHE_RATIO", 0.05);
    let promote = env_u32("FRS_BENCH_PROMOTE", 2);
    let evict_limit = env_u32("FRS_BENCH_EVICT_LIMIT", 3);
    let epoch_evicts = env_u64("FRS_BENCH_EPOCH_EVICTS", 0); // 0 = auto (= cache_keys)

    println!("=== cache-admission decay micro-bench: FIFO stand-in vs ForSt epoch ===");
    println!(
        "ops={ops} key_space={key_space} cache_ratio={cache_ratio} \
         promote={promote} evict_limit={evict_limit}"
    );
    println!("(working set = key_space >> cache; same seeded Zipf trace per arm)\n");

    // Sweep skew x cache-ratio so the verdict is not a single-point artifact.
    let thetas: Vec<f64> = if smoke {
        vec![0.9]
    } else {
        vec![0.7, 0.9, 1.1, 1.3]
    };
    let ratios: Vec<f64> = if smoke {
        vec![cache_ratio]
    } else {
        vec![0.02, 0.05, 0.10]
    };

    println!(
        "{:>6} {:>8} {:>10} {:>10} {:>10} {:>8}",
        "theta", "cacheN", "hit_FIFO", "hit_EPOCH", "delta_pp", "x_more"
    );
    let mut max_delta_pp = f64::MIN;
    let mut min_delta_pp = f64::MAX;
    for &theta in &thetas {
        let trace = build_trace(ops, key_space, theta, 0xC0FFEE ^ (theta * 1000.0) as u64);
        for &ratio in &ratios {
            let cache_keys = ((key_space as f64) * ratio).max(1.0) as usize;
            let period = if epoch_evicts == 0 {
                cache_keys as u64
            } else {
                epoch_evicts
            };
            let p = Params {
                cache_keys,
                access_before_promote: promote,
                promote_limit: evict_limit,
            };
            let hit_fifo = simulate(&trace, &p, Decay::Fifo);
            let hit_epoch = simulate(&trace, &p, Decay::Epoch(period));
            let delta_pp = (hit_epoch - hit_fifo) * 100.0;
            let x_more = if hit_fifo > 0.0 {
                hit_epoch / hit_fifo
            } else {
                f64::NAN
            };
            max_delta_pp = max_delta_pp.max(delta_pp);
            min_delta_pp = min_delta_pp.min(delta_pp);
            println!(
                "{:>6.2} {:>8} {:>9.2}% {:>9.2}% {:>+9.2} {:>7.3}x",
                theta,
                cache_keys,
                hit_fifo * 100.0,
                hit_epoch * 100.0,
                delta_pp,
                x_more
            );
        }
    }

    println!();
    println!(
        "max delta = {:+.2} pp   min delta = {:+.2} pp",
        max_delta_pp, min_delta_pp
    );
    // Verdict thresholds (catalog §2.3): material if any cell >= 2.0 pp hit-rate.
    let verdict = if max_delta_pp >= 2.0 {
        "MATERIAL - epoch decay worth building"
    } else if max_delta_pp >= 0.5 {
        "MARGINAL - small benefit, likely not worth the complexity"
    } else {
        "NEGLIGIBLE - FIFO stand-in is equivalent; do NOT build epoch"
    };
    println!("VERDICT: {verdict}");

    if smoke {
        // CI guard: the simulation must run and produce a finite verdict.
        assert!(max_delta_pp.is_finite(), "delta must be finite");
        assert!(min_delta_pp.is_finite(), "delta must be finite");
        println!("[smoke] OK");
    }
}

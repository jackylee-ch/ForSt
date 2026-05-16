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

// SPDX-License-Identifier: Apache-2.0
//! Fault injection harness for L5 tests (umbrella spec §5).
//!
//! # Modes
//!
//! Operates via environment variables — no build-time flags required:
//!
//! | Variable | Mode | Description |
//! |---|---|---|
//! | `FRS_FAULT_<KIND>_PROB=<0.0..1.0>` | Probabilistic | Fire with probability p (soak/fuzz) |
//! | `FRS_FAULT_<KIND>_AT=<call_index>` | Deterministic | Fire exactly on the Nth call (1-indexed) |
//! | `FRS_FAULT_SEED=<u64>` | Replay | Seed for probabilistic mode PRNG |
//!
//! # Fault kinds (F1-F12 per umbrella spec §5)
//!
//! | ID | Kind string | Description |
//! |---|---|---|
//! | F1 | `"vec_get"` | ErrorCodeSubstitution in frs_vectorized_batch_get |
//! | F3 | `"panic"` | EnginePanic — triggers PANIC_CAUGHT → TM fatal path |
//! | F2/F4-F12 | (TODO) | Deferred to P11/P12 integration phase |
//!
//! # Example (deterministic F1 test)
//!
//! ```bash
//! FRS_FAULT_VEC_GET_AT=1 FRS_FAULT_VEC_GET_CODE=300 \
//!   cargo test -p forst-rs-ffi fault_injector_substitutes_error_code
//! ```

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

/// Firing mode for a fault kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultMode {
    /// Fault is disabled (default when no env vars are set).
    Disabled,
    /// Fire with probability p ∈ [0.0, 1.0], stored as microprobability (p × 1_000_000)
    /// to avoid floating-point in the hot path.
    Probability(u32),
    /// Fire exactly on the Nth call (1-indexed, deterministic).
    DeterministicAt(u64),
}

/// Configuration for a single fault kind.
pub struct FaultConfig {
    pub mode: FaultMode,
    /// Replay seed for probabilistic mode; 0 means no seed override.
    pub seed: u64,
}

impl FaultConfig {
    /// Build from environment variables for the given `kind`.
    ///
    /// Looks up:
    /// - `FRS_FAULT_<KIND_UPPER>_PROB` — probabilistic mode
    /// - `FRS_FAULT_<KIND_UPPER>_AT`   — deterministic mode
    /// - `FRS_FAULT_SEED`              — optional replay seed (shared across kinds)
    pub fn from_env(kind: &str) -> Self {
        let kind_upper = kind.to_uppercase();
        let prob_var = format!("FRS_FAULT_{kind_upper}_PROB");
        let at_var = format!("FRS_FAULT_{kind_upper}_AT");
        let seed_var = "FRS_FAULT_SEED";

        let seed = std::env::var(seed_var)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Probabilistic mode takes priority when both are set.
        if let Ok(p) = std::env::var(&prob_var) {
            if let Ok(f) = p.parse::<f64>() {
                let clamped = f.clamp(0.0, 1.0);
                let micro = (clamped * 1_000_000.0) as u32;
                return Self {
                    mode: FaultMode::Probability(micro),
                    seed,
                };
            }
        }

        if let Ok(n) = std::env::var(&at_var) {
            if let Ok(idx) = n.parse::<u64>() {
                return Self {
                    mode: FaultMode::DeterministicAt(idx),
                    seed,
                };
            }
        }

        Self {
            mode: FaultMode::Disabled,
            seed,
        }
    }
}

/// Global fault injector singleton.
///
/// Maintains per-kind call counters and cached [`FaultConfig`] entries.
/// Call [`FaultInjector::reset`] between tests to clear state.
pub struct FaultInjector {
    /// Per-kind monotonic call counters, keyed by kind string.
    counters: Mutex<HashMap<String, AtomicU64>>,
    /// Cached configs parsed from env vars (invalidated on reset).
    config_cache: Mutex<HashMap<String, FaultConfig>>,
}

static GLOBAL: OnceLock<FaultInjector> = OnceLock::new();

impl FaultInjector {
    /// Access the process-global singleton.
    pub fn global() -> &'static Self {
        GLOBAL.get_or_init(|| Self {
            counters: Mutex::new(HashMap::new()),
            config_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Returns `true` if the fault for `kind` should fire on this call.
    ///
    /// The call counter for `kind` is incremented unconditionally (including
    /// when the fault is disabled) so deterministic indices remain stable
    /// even when the injector is queried multiple times per operation.
    pub fn should_fire(&self, kind: &str) -> bool {
        // Increment counter first (always, regardless of mode).
        let n = {
            let mut counters = self.counters.lock().unwrap();
            let counter = counters
                .entry(kind.to_string())
                .or_insert_with(|| AtomicU64::new(0));
            counter.fetch_add(1, Ordering::SeqCst) + 1
        };

        // Resolve config (cached until reset()).
        let mut configs = self.config_cache.lock().unwrap();
        let cfg = configs
            .entry(kind.to_string())
            .or_insert_with(|| FaultConfig::from_env(kind));

        match cfg.mode {
            FaultMode::Disabled => false,
            FaultMode::DeterministicAt(idx) => n == idx,
            FaultMode::Probability(micro) => {
                // Cheap SplitMix64 PRNG seeded with (n, seed) for replay determinism.
                let h = splitmix64(n, cfg.seed);
                let r = (h % 1_000_000) as u32;
                r < micro
            }
        }
    }

    /// Reset all per-kind call counters and the config cache.
    ///
    /// Must be called between tests to ensure isolation (env vars are
    /// re-read from the process environment after reset).
    pub fn reset(&self) {
        self.counters.lock().unwrap().clear();
        self.config_cache.lock().unwrap().clear();
    }
}

/// SplitMix64: a fast, high-quality PRNG suitable for replay-deterministic
/// fault injection. Seeded with `(n, seed)` so each call site has a unique
/// output even when `seed=0`.
fn splitmix64(n: u64, seed: u64) -> u64 {
    let mut z = n
        .wrapping_add(seed)
        .wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: get a fresh injector state without polluting the global singleton
    /// (which can't be re-initialized).  We use a local instance here instead.
    fn fresh() -> FaultInjector {
        FaultInjector {
            counters: Mutex::new(HashMap::new()),
            config_cache: Mutex::new(HashMap::new()),
        }
    }

    #[test]
    fn disabled_never_fires() {
        std::env::remove_var("FRS_FAULT_DTEST_PROB");
        std::env::remove_var("FRS_FAULT_DTEST_AT");
        let inj = fresh();
        for _ in 0..100 {
            assert!(!inj.should_fire("dtest"), "disabled mode must never fire");
        }
    }

    #[test]
    fn deterministic_at_fires_exactly_once() {
        std::env::remove_var("FRS_FAULT_DETAT_PROB");
        std::env::set_var("FRS_FAULT_DETAT_AT", "5");
        let inj = fresh();
        let fires: Vec<u64> = (1..=10)
            .filter(|_| inj.should_fire("detat"))
            .collect();
        assert_eq!(fires, vec![5], "deterministic mode must fire exactly at call 5");
        std::env::remove_var("FRS_FAULT_DETAT_AT");
    }

    #[test]
    fn probability_one_always_fires() {
        std::env::remove_var("FRS_FAULT_PONE_AT");
        std::env::set_var("FRS_FAULT_PONE_PROB", "1.0");
        let inj = fresh();
        for _ in 0..100 {
            assert!(inj.should_fire("pone"), "probability=1.0 must always fire");
        }
        std::env::remove_var("FRS_FAULT_PONE_PROB");
    }

    #[test]
    fn probability_zero_never_fires() {
        std::env::remove_var("FRS_FAULT_PZERO_AT");
        std::env::set_var("FRS_FAULT_PZERO_PROB", "0.0");
        let inj = fresh();
        for _ in 0..100 {
            assert!(!inj.should_fire("pzero"), "probability=0.0 must never fire");
        }
        std::env::remove_var("FRS_FAULT_PZERO_PROB");
    }
}

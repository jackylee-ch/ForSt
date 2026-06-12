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

//! Requester-class tagging for cache decisions (Phase-2 disagg, ForSt
//! mechanism §2.1.4 of the 2026-06-13 competitive analysis).
//!
//! ForSt's `FileBasedCache` only lets *Flink* (state-executor) threads affect
//! LRU order — background compaction reads neither promote cache entries nor
//! pollute hit metrics (`FileBasedCache.java:65,146-164`, wired through
//! `ForStExecutorThreadFactory`). Without that distinction, a large compaction
//! scan can evict the operator's hot read set from a budget-bound cache — the
//! exact thrash failure mode recorded for q9 on 2026-05-31.
//!
//! forst-rs equivalent: the engine marks its process-global background worker
//! threads (flush + compaction pools, `bg_pool.rs`) with
//! [`mark_thread_background`]; cache layers consult [`is_background_thread`]
//! at the point of an access. The mark is a plain thread-local — zero cost on
//! the read path beyond one TLS load — and is *advisory*: every cache keeps
//! byte-identical behavior unless its own policy flag opts in (see
//! `LocalCache` `CachePolicy::background_exempt`, default OFF).

use std::cell::Cell;

thread_local! {
    /// `true` when the current thread performs background work (flush /
    /// compaction). Foreground (operator / state-executor / FFI) threads
    /// never set this.
    static BACKGROUND: Cell<bool> = const { Cell::new(false) };
}

/// Marks the CURRENT thread as a background requester for its remaining
/// lifetime (sticky). Call once at worker-thread startup (engine `bg_pool`).
pub fn mark_thread_background() {
    BACKGROUND.with(|b| b.set(true));
}

/// Returns `true` if the current thread was marked as a background requester
/// (either sticky via [`mark_thread_background`] or scoped via
/// [`BackgroundScope`]).
pub fn is_background_thread() -> bool {
    BACKGROUND.with(|b| b.get())
}

/// RAII guard that marks the current thread background for the guard's
/// lifetime, restoring the previous class on drop. For call sites that do
/// background-class work on a thread they do not own (and for tests).
pub struct BackgroundScope {
    prev: bool,
}

impl BackgroundScope {
    /// Enters background class on the current thread.
    pub fn enter() -> Self {
        let prev = BACKGROUND.with(|b| b.replace(true));
        Self { prev }
    }
}

impl Drop for BackgroundScope {
    fn drop(&mut self) {
        let prev = self.prev;
        BACKGROUND.with(|b| b.set(prev));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_foreground_and_scope_restores() {
        assert!(!is_background_thread(), "threads start foreground");
        {
            let _g = BackgroundScope::enter();
            assert!(is_background_thread());
            {
                let _g2 = BackgroundScope::enter();
                assert!(is_background_thread());
            }
            assert!(is_background_thread(), "inner drop restores OUTER scope");
        }
        assert!(!is_background_thread(), "outer drop restores foreground");
    }

    #[test]
    fn sticky_mark_persists_and_is_per_thread() {
        let h = std::thread::spawn(|| {
            mark_thread_background();
            assert!(is_background_thread());
        });
        h.join().unwrap();
        // The mark is thread-local: this thread is unaffected.
        assert!(!is_background_thread());
    }
}

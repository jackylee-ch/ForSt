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

//! Per-thread requester CLASS — the low-level source of truth shared between
//! the io-layer remote throttle ([`crate::throttle`]) and the storage-layer
//! cache policy (`forst_rs_storage::requester`, which delegates here).
//!
//! Why here and not in storage: `forst-rs-storage` depends on `forst-rs-io`,
//! never the reverse, so the io-layer QoS throttle cannot read a thread-local
//! that lives in storage. The canonical marker therefore lives in io; the
//! storage `requester` module re-exports it so existing call sites are
//! unchanged.
//!
//! Three classes, distinguished only because the slow-remote-write QoS split
//! needs them (design: USER DIRECTIVE 2026-06-15, "RATE-LIMIT uploads"):
//!
//!   - **Foreground** (default) — operator / state-executor / FFI threads, and
//!     the thread that drives a checkpoint's `await_upload` barrier. Never
//!     paced below the full remote rate.
//!   - **Background-flush** — the flush pool. Produces the recently-written L0
//!     SSTs a checkpoint must await; treated as foreground-priority for the
//!     upload split so a checkpoint never queues behind compaction.
//!   - **Compaction** — the compaction pool. Its large, continuous SST
//!     rewrites can saturate a slow remote write channel; the QoS throttle
//!     paces compaction-class writes against a reduced sub-rate so they can
//!     never starve flush / checkpoint.

use std::cell::Cell;

thread_local! {
    /// `true` when the current thread performs background work (flush /
    /// compaction). Foreground threads never set this.
    static BACKGROUND: Cell<bool> = const { Cell::new(false) };
    /// `true` when the current thread is a COMPACTION worker specifically.
    /// Implies [`BACKGROUND`].
    static COMPACTION: Cell<bool> = const { Cell::new(false) };
}

/// Marks the current thread as a background requester (sticky).
pub fn mark_thread_background() {
    BACKGROUND.with(|b| b.set(true));
}

/// Marks the current thread as a COMPACTION worker (sticky). Also background.
pub fn mark_thread_compaction() {
    BACKGROUND.with(|b| b.set(true));
    COMPACTION.with(|c| c.set(true));
}

/// `true` if the current thread is a background requester.
#[inline]
pub fn is_background_thread() -> bool {
    BACKGROUND.with(|b| b.get())
}

/// `true` if the current thread is a compaction worker.
#[inline]
pub fn is_compaction_thread() -> bool {
    COMPACTION.with(|c| c.get())
}

/// Sets the background flag directly (for the storage `BackgroundScope` RAII
/// guard, which must save/restore the prior value). Returns the previous flag.
pub fn replace_background(v: bool) -> bool {
    BACKGROUND.with(|b| b.replace(v))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_foreground() {
        assert!(!is_background_thread());
        assert!(!is_compaction_thread());
    }

    #[test]
    fn compaction_implies_background_per_thread() {
        let h = std::thread::spawn(|| {
            mark_thread_compaction();
            assert!(is_background_thread());
            assert!(is_compaction_thread());
        });
        h.join().unwrap();
        // Thread-local: this thread is unaffected.
        assert!(!is_background_thread());
        assert!(!is_compaction_thread());
    }

    #[test]
    fn flush_is_background_not_compaction() {
        let h = std::thread::spawn(|| {
            mark_thread_background();
            assert!(is_background_thread());
            assert!(!is_compaction_thread());
        });
        h.join().unwrap();
    }
}

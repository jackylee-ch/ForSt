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

//! Back-pressure controller. See `2.8_read_write_paths.md` §2.5.
//!
//! The [`WriteController`] throttles writers when downstream resources
//! (L0 SST files or immutable memtables) approach configured limits.
//! Unlike ForSt/RocksDB's `WriteThread` state machine, this implementation
//! uses a single [`AtomicBool`] plus a [`Condvar`] so uncontended writes
//! are lock-free.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use forst_rs_common::{ForstError, ForstResult};

/// Configuration knobs for [`WriteController`].
#[derive(Debug, Clone)]
pub struct WriteControllerConfig {
    /// L0 file count that triggers slowdown (writers sleep briefly).
    pub l0_slowdown_trigger: u32,
    /// L0 file count that triggers stall (writers block).
    pub l0_stop_trigger: u32,
    /// Maximum number of immutable memtables; exceeding this stalls writes.
    pub max_write_buffer_number: u32,
    /// Sleep duration applied during slowdown per write.
    pub slowdown_delay: Duration,
    /// Upper bound on how long [`WriteController::may_throttle`] will block
    /// before returning an error (prevents deadlocks on shutdown bugs).
    pub stall_timeout: Duration,
}

impl Default for WriteControllerConfig {
    fn default() -> Self {
        Self {
            l0_slowdown_trigger: 20,
            l0_stop_trigger: 36,
            max_write_buffer_number: 3,
            slowdown_delay: Duration::from_micros(100),
            stall_timeout: Duration::from_secs(30),
        }
    }
}

/// Observable throttling decision returned by [`WriteController::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThrottleDecision {
    /// Proceed without delay.
    Proceed,
    /// Sleep for [`WriteControllerConfig::slowdown_delay`], then proceed.
    Slowdown,
    /// Block until [`WriteController::on_flush_complete`] or similar call
    /// unblocks the writer.
    Stall,
}

/// Back-pressure controller for engine write paths.
pub struct WriteController {
    config: WriteControllerConfig,
    stalled: AtomicBool,
    l0_file_count: AtomicU32,
    imm_count: AtomicU32,
    mutex: Mutex<()>,
    condvar: Condvar,
}

impl WriteController {
    /// Creates a new controller with the given config.
    pub fn new(config: WriteControllerConfig) -> Self {
        Self {
            config,
            stalled: AtomicBool::new(false),
            l0_file_count: AtomicU32::new(0),
            imm_count: AtomicU32::new(0),
            mutex: Mutex::new(()),
            condvar: Condvar::new(),
        }
    }

    /// Creates a controller with default thresholds.
    pub fn with_defaults() -> Self {
        Self::new(WriteControllerConfig::default())
    }

    /// Returns the current configuration.
    pub fn config(&self) -> &WriteControllerConfig {
        &self.config
    }

    /// Non-blocking inspection of the current throttle decision.
    pub fn check(&self) -> ThrottleDecision {
        if self.stalled.load(Ordering::Acquire) {
            return ThrottleDecision::Stall;
        }
        let l0 = self.l0_file_count.load(Ordering::Acquire);
        let imm = self.imm_count.load(Ordering::Acquire);
        if l0 >= self.config.l0_stop_trigger || imm >= self.config.max_write_buffer_number {
            ThrottleDecision::Stall
        } else if l0 >= self.config.l0_slowdown_trigger {
            ThrottleDecision::Slowdown
        } else {
            ThrottleDecision::Proceed
        }
    }

    /// Blocks the calling thread (or sleeps briefly) if back-pressure is
    /// active. Returns once the writer may proceed, or an error if the stall
    /// timeout fires.
    pub fn may_throttle(&self) -> ForstResult<()> {
        match self.check() {
            ThrottleDecision::Proceed => Ok(()),
            ThrottleDecision::Slowdown => {
                std::thread::sleep(self.config.slowdown_delay);
                Ok(())
            }
            ThrottleDecision::Stall => self.wait_for_unstall(),
        }
    }

    /// Waits for the stall flag to clear, up to `stall_timeout`.
    fn wait_for_unstall(&self) -> ForstResult<()> {
        let start = Instant::now();
        let mut guard = self.mutex.lock().expect("lock poisoned");
        while self.stalled.load(Ordering::Acquire)
            || self.l0_file_count.load(Ordering::Acquire) >= self.config.l0_stop_trigger
            || self.imm_count.load(Ordering::Acquire) >= self.config.max_write_buffer_number
        {
            let elapsed = start.elapsed();
            if elapsed >= self.config.stall_timeout {
                return Err(ForstError::timed_out(
                    "write stall timeout: flush/compaction backlog not draining",
                ));
            }
            let remaining = self.config.stall_timeout - elapsed;
            let (g, _) = self
                .condvar
                .wait_timeout(guard, remaining)
                .expect("condvar poisoned");
            guard = g;
        }
        Ok(())
    }

    /// Explicitly sets the stall flag. Writers calling [`may_throttle`] will
    /// block until [`clear_stall`] or [`on_flush_complete`] wakes them.
    pub fn set_stall(&self, stalled: bool) {
        self.stalled.store(stalled, Ordering::Release);
        if !stalled {
            self.wake_all();
        }
    }

    /// Returns the currently observed stall flag.
    pub fn is_stalled(&self) -> bool {
        self.stalled.load(Ordering::Acquire)
    }

    /// Clears the stall flag and wakes all waiters.
    pub fn clear_stall(&self) {
        self.set_stall(false);
    }

    /// Updates the tracked L0 file count. Wakes waiters if the count has
    /// dropped below the stop trigger.
    pub fn set_l0_file_count(&self, count: u32) {
        let prev = self.l0_file_count.swap(count, Ordering::Release);
        if prev >= self.config.l0_stop_trigger && count < self.config.l0_stop_trigger {
            self.wake_all();
        }
    }

    /// Updates the tracked immutable memtable count. Wakes waiters if below
    /// the maximum.
    pub fn set_imm_count(&self, count: u32) {
        let prev = self.imm_count.swap(count, Ordering::Release);
        if prev >= self.config.max_write_buffer_number
            && count < self.config.max_write_buffer_number
        {
            self.wake_all();
        }
    }

    /// Convenience hook to be called by the flush subsystem after it writes
    /// a new SST file — clears the stall flag and wakes waiters.
    pub fn on_flush_complete(&self) {
        self.stalled.store(false, Ordering::Release);
        self.wake_all();
    }

    fn wake_all(&self) {
        let _guard = self.mutex.lock().expect("lock poisoned");
        self.condvar.notify_all();
    }

    /// Current L0 file count (for tests / diagnostics).
    pub fn l0_file_count(&self) -> u32 {
        self.l0_file_count.load(Ordering::Acquire)
    }

    /// Current imm memtable count (for tests / diagnostics).
    pub fn imm_count(&self) -> u32 {
        self.imm_count.load(Ordering::Acquire)
    }
}

impl Default for WriteController {
    fn default() -> Self {
        Self::with_defaults()
    }
}

impl std::fmt::Debug for WriteController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteController")
            .field("stalled", &self.is_stalled())
            .field("l0_file_count", &self.l0_file_count())
            .field("imm_count", &self.imm_count())
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_default_decision_is_proceed() {
        let wc = WriteController::with_defaults();
        assert_eq!(wc.check(), ThrottleDecision::Proceed);
    }

    #[test]
    fn test_slowdown_trigger() {
        let wc = WriteController::with_defaults();
        wc.set_l0_file_count(wc.config().l0_slowdown_trigger);
        assert_eq!(wc.check(), ThrottleDecision::Slowdown);
    }

    #[test]
    fn test_stop_trigger() {
        let wc = WriteController::with_defaults();
        wc.set_l0_file_count(wc.config().l0_stop_trigger);
        assert_eq!(wc.check(), ThrottleDecision::Stall);
    }

    #[test]
    fn test_imm_threshold_stalls() {
        let wc = WriteController::with_defaults();
        wc.set_imm_count(wc.config().max_write_buffer_number);
        assert_eq!(wc.check(), ThrottleDecision::Stall);
    }

    #[test]
    fn test_explicit_stall_flag() {
        let wc = WriteController::with_defaults();
        wc.set_stall(true);
        assert!(wc.is_stalled());
        assert_eq!(wc.check(), ThrottleDecision::Stall);
        wc.clear_stall();
        assert!(!wc.is_stalled());
        assert_eq!(wc.check(), ThrottleDecision::Proceed);
    }

    #[test]
    fn test_may_throttle_proceeds_when_clear() {
        let wc = WriteController::with_defaults();
        wc.may_throttle().unwrap();
    }

    #[test]
    fn test_may_throttle_slowdown_does_not_error() {
        let cfg = WriteControllerConfig {
            slowdown_delay: Duration::from_micros(1),
            ..WriteControllerConfig::default()
        };
        let wc = WriteController::new(cfg);
        wc.set_l0_file_count(wc.config().l0_slowdown_trigger);
        let start = Instant::now();
        wc.may_throttle().unwrap();
        assert!(start.elapsed() >= Duration::from_micros(1));
    }

    #[test]
    fn test_stall_timeout_returns_error() {
        let cfg = WriteControllerConfig {
            stall_timeout: Duration::from_millis(10),
            ..WriteControllerConfig::default()
        };
        let wc = WriteController::new(cfg);
        wc.set_stall(true);
        let err = wc.may_throttle().unwrap_err();
        assert!(err.to_string().to_lowercase().contains("timeout")
            || err.to_string().to_lowercase().contains("timed"));
    }

    #[test]
    fn test_on_flush_complete_wakes_waiter() {
        let wc = Arc::new(WriteController::with_defaults());
        wc.set_stall(true);

        let wc2 = Arc::clone(&wc);
        let handle = thread::spawn(move || wc2.may_throttle());

        // Give waiter a moment to enter the condvar.
        thread::sleep(Duration::from_millis(30));
        wc.on_flush_complete();

        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_set_l0_drop_wakes_waiter() {
        let wc = Arc::new(WriteController::with_defaults());
        wc.set_l0_file_count(wc.config().l0_stop_trigger);

        let wc2 = Arc::clone(&wc);
        let handle = thread::spawn(move || wc2.may_throttle());

        thread::sleep(Duration::from_millis(30));
        wc.set_l0_file_count(0);
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_set_imm_drop_wakes_waiter() {
        let wc = Arc::new(WriteController::with_defaults());
        wc.set_imm_count(wc.config().max_write_buffer_number);

        let wc2 = Arc::clone(&wc);
        let handle = thread::spawn(move || wc2.may_throttle());

        thread::sleep(Duration::from_millis(30));
        wc.set_imm_count(0);
        handle.join().unwrap().unwrap();
    }

    #[test]
    fn test_config_accessor() {
        let wc = WriteController::with_defaults();
        assert_eq!(wc.config().l0_stop_trigger, 36);
        assert_eq!(wc.config().max_write_buffer_number, 3);
    }

    #[test]
    fn test_debug_output() {
        let wc = WriteController::with_defaults();
        let dbg = format!("{:?}", wc);
        assert!(dbg.contains("WriteController"));
        assert!(dbg.contains("stalled"));
    }
}

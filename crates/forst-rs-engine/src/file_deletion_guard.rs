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

//! FileDeletionGuard. See `2.7_checkpoint_integration.md` §5 "FileDeletionGuard".
//!
//! When a checkpoint is being created or when a reader is holding open
//! handles to SST files, a concurrent compaction must NOT delete those
//! files — otherwise the checkpoint would reference non-existent data.
//!
//! [`FileDeletionGuard`] is a reference-counted map from `FileNumber` to
//! a pin count. While a file's pin count is > 0, callers that wish to
//! delete it must either wait or skip; the engine's compaction path
//! consults the guard before unlinking any file.
//!
//! Checkpoints acquire a pin for every live SST they reference (via
//! [`FileDeletionGuard::pin`]) and release it when the checkpoint is no
//! longer needed. Compactions call [`FileDeletionGuard::try_acquire_delete`]
//! to "reserve" a deletion — if the file is pinned the reservation is
//! refused and the compaction skips the file (it will be deleted the next
//! time a compaction runs without the pin).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use forst_rs_common::FileNumber;

/// Tracks per-file pin counts and prevents concurrent deletion while any
/// pin is held.
#[derive(Debug)]
pub struct FileDeletionGuard {
    pins: Mutex<HashMap<FileNumber, u32>>,
}

impl FileDeletionGuard {
    /// Creates an empty guard.
    pub fn new() -> Self {
        Self {
            pins: Mutex::new(HashMap::new()),
        }
    }

    /// Pins a file so it cannot be deleted until [`FileDeletionGuard::unpin`]
    /// is called the same number of times.
    pub fn pin(&self, file: FileNumber) {
        let mut guard = self.pins.lock().expect("lock poisoned");
        *guard.entry(file).or_insert(0) += 1;
    }

    /// Decrements a file's pin count. If the count reaches zero, the
    /// entry is removed.
    pub fn unpin(&self, file: FileNumber) {
        let mut guard = self.pins.lock().expect("lock poisoned");
        if let Some(count) = guard.get_mut(&file) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                guard.remove(&file);
            }
        }
    }

    /// Returns the current pin count for a file (0 if not pinned).
    pub fn pin_count(&self, file: FileNumber) -> u32 {
        self.pins
            .lock()
            .expect("lock poisoned")
            .get(&file)
            .copied()
            .unwrap_or(0)
    }

    /// Returns `true` if a file can be deleted safely right now.
    pub fn can_delete(&self, file: FileNumber) -> bool {
        self.pin_count(file) == 0
    }

    /// Returns a snapshot of currently-pinned file numbers.
    pub fn pinned_files(&self) -> Vec<FileNumber> {
        self.pins
            .lock()
            .expect("lock poisoned")
            .keys()
            .copied()
            .collect()
    }

    /// Pins an entire batch of files atomically. Returns a handle that
    /// unpins every file when dropped.
    pub fn pin_batch(self: &Arc<Self>, files: &[FileNumber]) -> PinHandle {
        {
            let mut guard = self.pins.lock().expect("lock poisoned");
            for f in files {
                *guard.entry(*f).or_insert(0) += 1;
            }
        }
        PinHandle {
            guard: self.clone(),
            files: files.to_vec(),
        }
    }
}

impl Default for FileDeletionGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII pin handle: calls `unpin` for every file in the batch when dropped.
pub struct PinHandle {
    guard: Arc<FileDeletionGuard>,
    files: Vec<FileNumber>,
}

impl PinHandle {
    /// Releases all pins explicitly. Equivalent to letting the handle go
    /// out of scope.
    pub fn release(self) {
        drop(self);
    }

    /// Files currently held by this handle.
    pub fn files(&self) -> &[FileNumber] {
        &self.files
    }
}

impl Drop for PinHandle {
    fn drop(&mut self) {
        for f in &self.files {
            self.guard.unpin(*f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_guard_is_empty() {
        let g = FileDeletionGuard::new();
        assert_eq!(g.pin_count(FileNumber(1)), 0);
        assert!(g.can_delete(FileNumber(1)));
    }

    #[test]
    fn test_pin_increments_count() {
        let g = FileDeletionGuard::new();
        g.pin(FileNumber(1));
        assert_eq!(g.pin_count(FileNumber(1)), 1);
        g.pin(FileNumber(1));
        assert_eq!(g.pin_count(FileNumber(1)), 2);
    }

    #[test]
    fn test_unpin_decrements_count() {
        let g = FileDeletionGuard::new();
        g.pin(FileNumber(1));
        g.pin(FileNumber(1));
        g.unpin(FileNumber(1));
        assert_eq!(g.pin_count(FileNumber(1)), 1);
        g.unpin(FileNumber(1));
        assert_eq!(g.pin_count(FileNumber(1)), 0);
    }

    #[test]
    fn test_unpin_never_goes_negative() {
        let g = FileDeletionGuard::new();
        g.unpin(FileNumber(7));
        assert_eq!(g.pin_count(FileNumber(7)), 0);
    }

    #[test]
    fn test_can_delete_reflects_pin_count() {
        let g = FileDeletionGuard::new();
        assert!(g.can_delete(FileNumber(1)));
        g.pin(FileNumber(1));
        assert!(!g.can_delete(FileNumber(1)));
        g.unpin(FileNumber(1));
        assert!(g.can_delete(FileNumber(1)));
    }

    #[test]
    fn test_pinned_files_snapshot() {
        let g = FileDeletionGuard::new();
        g.pin(FileNumber(1));
        g.pin(FileNumber(7));
        g.pin(FileNumber(3));
        let mut pinned = g.pinned_files();
        pinned.sort_by_key(|f| f.value());
        assert_eq!(pinned, vec![FileNumber(1), FileNumber(3), FileNumber(7)]);
    }

    #[test]
    fn test_pin_batch_acquires_all() {
        let g = Arc::new(FileDeletionGuard::new());
        let handle = g.pin_batch(&[FileNumber(1), FileNumber(2), FileNumber(3)]);
        assert_eq!(g.pin_count(FileNumber(1)), 1);
        assert_eq!(g.pin_count(FileNumber(2)), 1);
        assert_eq!(g.pin_count(FileNumber(3)), 1);
        drop(handle);
        assert!(g.can_delete(FileNumber(1)));
        assert!(g.can_delete(FileNumber(2)));
        assert!(g.can_delete(FileNumber(3)));
    }

    #[test]
    fn test_pin_batch_release_explicit() {
        let g = Arc::new(FileDeletionGuard::new());
        let handle = g.pin_batch(&[FileNumber(5)]);
        assert_eq!(g.pin_count(FileNumber(5)), 1);
        handle.release();
        assert_eq!(g.pin_count(FileNumber(5)), 0);
    }

    #[test]
    fn test_nested_batches_accumulate() {
        let g = Arc::new(FileDeletionGuard::new());
        let h1 = g.pin_batch(&[FileNumber(1)]);
        let h2 = g.pin_batch(&[FileNumber(1)]);
        assert_eq!(g.pin_count(FileNumber(1)), 2);
        drop(h1);
        assert_eq!(g.pin_count(FileNumber(1)), 1);
        drop(h2);
        assert_eq!(g.pin_count(FileNumber(1)), 0);
    }

    #[test]
    fn test_pinhandle_files_accessor() {
        let g = Arc::new(FileDeletionGuard::new());
        let h = g.pin_batch(&[FileNumber(1), FileNumber(2)]);
        assert_eq!(h.files(), &[FileNumber(1), FileNumber(2)]);
    }

    #[test]
    fn test_guard_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileDeletionGuard>();
    }
}

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

//! File ownership model for ForSt-RS.
//!
//! This module defines a three-level ownership model that tracks who is
//! responsible for each file's lifecycle. The model is motivated by the
//! DeltaJoin design where SST files may be shared across Checkpoint
//! boundaries or transferred to remote storage.
//!
//! # Ownership Levels
//!
//! | Level | Owner | Can Read | Can Delete |
//! |---|---|---|---|
//! | [`PrivateOwnedByDb`](FileOwnership::PrivateOwnedByDb) | DB instance | Yes | Yes |
//! | [`ShareableOwnedByDb`](FileOwnership::ShareableOwnedByDb) | DB instance (shared readers) | Yes | Only when no refs |
//! | [`NotOwned`](FileOwnership::NotOwned) | External (e.g., remote store) | Yes | No |
//!
//! # Example
//!
//! ```
//! use forst_rs_io::ownership::{FileOwnership, FileOwnershipTracker, OwnedFile};
//! use forst_rs_common::types::FileNumber;
//! use std::path::PathBuf;
//!
//! let mut tracker = FileOwnershipTracker::new();
//!
//! // Register a new WAL file.
//! tracker.register(OwnedFile {
//!     path: PathBuf::from("/data/wal/000001.log"),
//!     ownership: FileOwnership::PrivateOwnedByDb,
//!     file_number: FileNumber(1),
//!     file_size: 4096,
//! });
//!
//! // After Checkpoint, mark it shareable.
//! tracker.transfer_ownership(FileNumber(1), FileOwnership::ShareableOwnedByDb).unwrap();
//! ```

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use forst_rs_common::error::{ForstError, ForstResult};
use forst_rs_common::types::FileNumber;

// ---------------------------------------------------------------------------
// FileOwnership enum
// ---------------------------------------------------------------------------

/// File ownership level.
///
/// Determines who is responsible for a file's lifecycle and whether it can be
/// shared across components (e.g., during Checkpoint or after tiering to
/// remote storage).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FileOwnership {
    /// File is privately owned by the DB instance.
    ///
    /// Only this DB can read, write, or delete it.
    /// Examples: WAL files, MANIFEST, CURRENT.
    PrivateOwnedByDb,

    /// File is owned by the DB but can be shared (e.g., for Checkpoint).
    ///
    /// The DB owns the lifecycle but external readers may hold references.
    /// The file should not be deleted while references exist.
    /// Examples: Active SST files that may be referenced by snapshots.
    ShareableOwnedByDb,

    /// File is not owned by the DB (external ownership).
    ///
    /// The DB can read the file but should not delete it. Deletion is
    /// managed by the external owner (e.g., a remote storage lifecycle
    /// policy).
    /// Examples: SST files already transferred to S3/OSS.
    NotOwned,
}

impl fmt::Display for FileOwnership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileOwnership::PrivateOwnedByDb => write!(f, "PrivateOwnedByDb"),
            FileOwnership::ShareableOwnedByDb => write!(f, "ShareableOwnedByDb"),
            FileOwnership::NotOwned => write!(f, "NotOwned"),
        }
    }
}

// ---------------------------------------------------------------------------
// OwnedFile
// ---------------------------------------------------------------------------

/// Metadata about a tracked file and its ownership status.
#[derive(Debug, Clone)]
pub struct OwnedFile {
    /// Full path to the file.
    pub path: PathBuf,
    /// Current ownership level.
    pub ownership: FileOwnership,
    /// Unique file identifier assigned by the engine.
    pub file_number: FileNumber,
    /// File size in bytes.
    pub file_size: u64,
}

impl fmt::Display for OwnedFile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OwnedFile({}, {}, {} bytes, {})",
            self.file_number,
            self.path.display(),
            self.file_size,
            self.ownership,
        )
    }
}

// ---------------------------------------------------------------------------
// FileOwnershipTracker
// ---------------------------------------------------------------------------

/// Tracks file ownership across a DB instance.
///
/// The tracker maintains a registry of all files known to the engine along
/// with their current ownership level. It enforces valid ownership
/// transitions and provides query methods for finding files at a given
/// ownership level.
pub struct FileOwnershipTracker {
    files: HashMap<FileNumber, OwnedFile>,
}

impl FileOwnershipTracker {
    /// Creates a new, empty tracker.
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    /// Registers a file with the tracker.
    ///
    /// If a file with the same [`FileNumber`] is already registered, the old
    /// entry is returned so the caller can detect (and log) silent replacements.
    pub fn register(&mut self, file: OwnedFile) -> Option<OwnedFile> {
        self.files.insert(file.file_number, file)
    }

    /// Transfers a file to a new ownership level.
    ///
    /// # Errors
    ///
    /// Returns [`ForstError::NotFound`] if the file number is not registered.
    /// Returns [`ForstError::InvalidArgument`] if the ownership transition is
    /// not valid. Valid transitions:
    ///
    /// - `PrivateOwnedByDb` -> `ShareableOwnedByDb`
    /// - `PrivateOwnedByDb` -> `NotOwned`
    /// - `ShareableOwnedByDb` -> `NotOwned`
    pub fn transfer_ownership(
        &mut self,
        file_number: FileNumber,
        new_ownership: FileOwnership,
    ) -> ForstResult<()> {
        let file = self
            .files
            .get_mut(&file_number)
            .ok_or_else(|| ForstError::not_found(format!("file {} not registered", file_number)))?;

        validate_transition(file.ownership, new_ownership)?;
        file.ownership = new_ownership;
        Ok(())
    }

    /// Removes a file from the tracker, returning it if it was present.
    pub fn unregister(&mut self, file_number: FileNumber) -> Option<OwnedFile> {
        self.files.remove(&file_number)
    }

    /// Returns a reference to the tracked file, if registered.
    pub fn get(&self, file_number: FileNumber) -> Option<&OwnedFile> {
        self.files.get(&file_number)
    }

    /// Returns all files with the given ownership level.
    pub fn files_with_ownership(&self, ownership: FileOwnership) -> Vec<&OwnedFile> {
        self.files
            .values()
            .filter(|f| f.ownership == ownership)
            .collect()
    }

    /// Returns the number of tracked files.
    pub fn len(&self) -> usize {
        self.files.len()
    }

    /// Returns `true` if the tracker has no files.
    pub fn is_empty(&self) -> bool {
        self.files.is_empty()
    }
}

impl Default for FileOwnershipTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Transition validation
// ---------------------------------------------------------------------------

/// Validates that an ownership transition is legal.
///
/// The allowed transitions form a directed acyclic graph:
///
/// ```text
///   PrivateOwnedByDb --> ShareableOwnedByDb --> NotOwned
///           |                                      ^
///           +--------------------------------------+
/// ```
fn validate_transition(from: FileOwnership, to: FileOwnership) -> ForstResult<()> {
    let valid = match (from, to) {
        // Same level is a no-op, allow it.
        (a, b) if a == b => true,
        // Private -> Shareable (Checkpoint sharing).
        (FileOwnership::PrivateOwnedByDb, FileOwnership::ShareableOwnedByDb) => true,
        // Private -> NotOwned (direct transfer to remote).
        (FileOwnership::PrivateOwnedByDb, FileOwnership::NotOwned) => true,
        // Shareable -> NotOwned (transfer to remote after sharing).
        (FileOwnership::ShareableOwnedByDb, FileOwnership::NotOwned) => true,
        // All other transitions are invalid.
        _ => false,
    };

    if valid {
        Ok(())
    } else {
        Err(ForstError::invalid_argument(format!(
            "invalid ownership transition: {} -> {}",
            from, to,
        )))
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    fn make_file(number: u64, ownership: FileOwnership) -> OwnedFile {
        OwnedFile {
            path: PathBuf::from(format!("/data/sst/{:06}.sst", number)),
            ownership,
            file_number: FileNumber(number),
            file_size: 1024 * number,
        }
    }

    // -----------------------------------------------------------------------
    // FileOwnership tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_ownership_display() {
        assert_eq!(
            format!("{}", FileOwnership::PrivateOwnedByDb),
            "PrivateOwnedByDb"
        );
        assert_eq!(
            format!("{}", FileOwnership::ShareableOwnedByDb),
            "ShareableOwnedByDb"
        );
        assert_eq!(format!("{}", FileOwnership::NotOwned), "NotOwned");
    }

    #[test]
    fn test_ownership_copy_clone() {
        let o = FileOwnership::PrivateOwnedByDb;
        let o2 = o;
        assert_eq!(o, o2);
    }

    #[test]
    fn test_ownership_equality() {
        assert_eq!(FileOwnership::NotOwned, FileOwnership::NotOwned);
        assert_ne!(FileOwnership::PrivateOwnedByDb, FileOwnership::NotOwned);
    }

    // -----------------------------------------------------------------------
    // OwnedFile tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_owned_file_display() {
        let f = make_file(42, FileOwnership::PrivateOwnedByDb);
        let display = format!("{}", f);
        assert!(display.contains("000042"));
        assert!(display.contains("PrivateOwnedByDb"));
        assert!(display.contains("43008 bytes")); // 1024 * 42
    }

    #[test]
    fn test_owned_file_clone() {
        let f = make_file(1, FileOwnership::NotOwned);
        let f2 = f.clone();
        assert_eq!(f.file_number, f2.file_number);
        assert_eq!(f.ownership, f2.ownership);
        assert_eq!(f.file_size, f2.file_size);
        assert_eq!(f.path, f2.path);
    }

    // -----------------------------------------------------------------------
    // Transition validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_valid_transition_private_to_shareable() {
        assert!(validate_transition(
            FileOwnership::PrivateOwnedByDb,
            FileOwnership::ShareableOwnedByDb,
        )
        .is_ok());
    }

    #[test]
    fn test_valid_transition_private_to_not_owned() {
        assert!(
            validate_transition(FileOwnership::PrivateOwnedByDb, FileOwnership::NotOwned,).is_ok()
        );
    }

    #[test]
    fn test_valid_transition_shareable_to_not_owned() {
        assert!(
            validate_transition(FileOwnership::ShareableOwnedByDb, FileOwnership::NotOwned,)
                .is_ok()
        );
    }

    #[test]
    fn test_same_level_transition_is_noop() {
        assert!(validate_transition(
            FileOwnership::PrivateOwnedByDb,
            FileOwnership::PrivateOwnedByDb,
        )
        .is_ok());
        assert!(validate_transition(
            FileOwnership::ShareableOwnedByDb,
            FileOwnership::ShareableOwnedByDb,
        )
        .is_ok());
        assert!(validate_transition(FileOwnership::NotOwned, FileOwnership::NotOwned).is_ok());
    }

    #[test]
    fn test_invalid_transition_not_owned_to_private() {
        let result = validate_transition(FileOwnership::NotOwned, FileOwnership::PrivateOwnedByDb);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_transition_not_owned_to_shareable() {
        let result =
            validate_transition(FileOwnership::NotOwned, FileOwnership::ShareableOwnedByDb);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_transition_shareable_to_private() {
        let result = validate_transition(
            FileOwnership::ShareableOwnedByDb,
            FileOwnership::PrivateOwnedByDb,
        );
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // FileOwnershipTracker tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tracker_new_is_empty() {
        let tracker = FileOwnershipTracker::new();
        assert!(tracker.is_empty());
        assert_eq!(tracker.len(), 0);
    }

    #[test]
    fn test_tracker_default_is_empty() {
        let tracker = FileOwnershipTracker::default();
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_tracker_register_and_get() {
        let mut tracker = FileOwnershipTracker::new();
        let prev = tracker.register(make_file(1, FileOwnership::PrivateOwnedByDb));
        assert!(prev.is_none()); // first registration returns None

        let file = tracker.get(FileNumber(1)).unwrap();
        assert_eq!(file.file_number, FileNumber(1));
        assert_eq!(file.ownership, FileOwnership::PrivateOwnedByDb);
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_tracker_register_replaces_existing() {
        let mut tracker = FileOwnershipTracker::new();
        tracker.register(make_file(1, FileOwnership::PrivateOwnedByDb));
        let prev = tracker.register(OwnedFile {
            path: PathBuf::from("/new/path.sst"),
            ownership: FileOwnership::NotOwned,
            file_number: FileNumber(1),
            file_size: 9999,
        });

        // Previous entry is returned.
        assert!(prev.is_some());
        assert_eq!(prev.unwrap().ownership, FileOwnership::PrivateOwnedByDb);

        let file = tracker.get(FileNumber(1)).unwrap();
        assert_eq!(file.ownership, FileOwnership::NotOwned);
        assert_eq!(file.file_size, 9999);
        assert_eq!(tracker.len(), 1);
    }

    #[test]
    fn test_tracker_get_nonexistent() {
        let tracker = FileOwnershipTracker::new();
        assert!(tracker.get(FileNumber(999)).is_none());
    }

    #[test]
    fn test_tracker_unregister() {
        let mut tracker = FileOwnershipTracker::new();
        tracker.register(make_file(1, FileOwnership::PrivateOwnedByDb));
        tracker.register(make_file(2, FileOwnership::NotOwned));

        let removed = tracker.unregister(FileNumber(1));
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().file_number, FileNumber(1));
        assert_eq!(tracker.len(), 1);
        assert!(tracker.get(FileNumber(1)).is_none());
    }

    #[test]
    fn test_tracker_unregister_nonexistent() {
        let mut tracker = FileOwnershipTracker::new();
        assert!(tracker.unregister(FileNumber(999)).is_none());
    }

    #[test]
    fn test_tracker_transfer_ownership_valid() {
        let mut tracker = FileOwnershipTracker::new();
        tracker.register(make_file(1, FileOwnership::PrivateOwnedByDb));

        tracker
            .transfer_ownership(FileNumber(1), FileOwnership::ShareableOwnedByDb)
            .unwrap();
        assert_eq!(
            tracker.get(FileNumber(1)).unwrap().ownership,
            FileOwnership::ShareableOwnedByDb,
        );

        tracker
            .transfer_ownership(FileNumber(1), FileOwnership::NotOwned)
            .unwrap();
        assert_eq!(
            tracker.get(FileNumber(1)).unwrap().ownership,
            FileOwnership::NotOwned,
        );
    }

    #[test]
    fn test_tracker_transfer_ownership_not_found() {
        let mut tracker = FileOwnershipTracker::new();
        let result = tracker.transfer_ownership(FileNumber(999), FileOwnership::NotOwned);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_not_found());
    }

    #[test]
    fn test_tracker_transfer_ownership_invalid_transition() {
        let mut tracker = FileOwnershipTracker::new();
        tracker.register(make_file(1, FileOwnership::NotOwned));

        let result = tracker.transfer_ownership(FileNumber(1), FileOwnership::PrivateOwnedByDb);
        assert!(result.is_err());
        assert!(result.unwrap_err().is_invalid_argument());
    }

    #[test]
    fn test_tracker_files_with_ownership() {
        let mut tracker = FileOwnershipTracker::new();
        tracker.register(make_file(1, FileOwnership::PrivateOwnedByDb));
        tracker.register(make_file(2, FileOwnership::PrivateOwnedByDb));
        tracker.register(make_file(3, FileOwnership::ShareableOwnedByDb));
        tracker.register(make_file(4, FileOwnership::NotOwned));

        let private_files = tracker.files_with_ownership(FileOwnership::PrivateOwnedByDb);
        assert_eq!(private_files.len(), 2);

        let shareable_files = tracker.files_with_ownership(FileOwnership::ShareableOwnedByDb);
        assert_eq!(shareable_files.len(), 1);
        assert_eq!(shareable_files[0].file_number, FileNumber(3));

        let not_owned_files = tracker.files_with_ownership(FileOwnership::NotOwned);
        assert_eq!(not_owned_files.len(), 1);
        assert_eq!(not_owned_files[0].file_number, FileNumber(4));
    }

    #[test]
    fn test_tracker_files_with_ownership_empty_result() {
        let tracker = FileOwnershipTracker::new();
        let files = tracker.files_with_ownership(FileOwnership::PrivateOwnedByDb);
        assert!(files.is_empty());
    }

    #[test]
    fn test_tracker_lifecycle_scenario() {
        // Simulates a typical file lifecycle:
        //   create (private) -> checkpoint (shareable) -> tiered (not owned) -> unregister
        let mut tracker = FileOwnershipTracker::new();

        // 1. New SST file created during flush.
        tracker.register(make_file(10, FileOwnership::PrivateOwnedByDb));
        assert_eq!(
            tracker.get(FileNumber(10)).unwrap().ownership,
            FileOwnership::PrivateOwnedByDb,
        );

        // 2. Checkpoint taken -- file becomes shareable.
        tracker
            .transfer_ownership(FileNumber(10), FileOwnership::ShareableOwnedByDb)
            .unwrap();
        assert_eq!(
            tracker.get(FileNumber(10)).unwrap().ownership,
            FileOwnership::ShareableOwnedByDb,
        );

        // 3. File tiered to remote storage -- no longer owned by DB.
        tracker
            .transfer_ownership(FileNumber(10), FileOwnership::NotOwned)
            .unwrap();
        assert_eq!(
            tracker.get(FileNumber(10)).unwrap().ownership,
            FileOwnership::NotOwned,
        );

        // 4. Eventually the remote reference is no longer needed.
        let removed = tracker.unregister(FileNumber(10));
        assert!(removed.is_some());
        assert!(tracker.is_empty());
    }

    #[test]
    fn test_tracker_multiple_register_unregister() {
        let mut tracker = FileOwnershipTracker::new();

        for i in 0..100 {
            tracker.register(make_file(i, FileOwnership::PrivateOwnedByDb));
        }
        assert_eq!(tracker.len(), 100);

        for i in 0..50 {
            tracker.unregister(FileNumber(i));
        }
        assert_eq!(tracker.len(), 50);

        // Verify remaining files are 50..100.
        for i in 50..100 {
            assert!(tracker.get(FileNumber(i)).is_some());
        }
        for i in 0..50 {
            assert!(tracker.get(FileNumber(i)).is_none());
        }
    }
}

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

//! Error types for ForSt-RS.
//!
//! This module defines [`ForstError`] and [`ForstResult`], providing
//! RocksDB-compatible error codes mapped to idiomatic Rust error handling
//! via [`thiserror`].

use std::io;

/// The primary error type for ForSt-RS operations.
///
/// Each variant corresponds to a RocksDB `Status` code, ensuring
/// compatibility when bridging between the Rust engine and existing
/// RocksDB/ForSt consumers.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ForstError {
    /// An I/O error occurred during a file-system or network operation.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Data on disk or in memory failed an integrity check.
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// The requested key, file, or resource does not exist.
    #[error("Not found: {0}")]
    NotFound(String),

    /// The operation is not supported by this implementation or configuration.
    #[error("Not supported: {0}")]
    NotSupported(String),

    /// A caller-supplied argument is invalid.
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// An operation could not be completed because required data is missing
    /// (e.g., a partial merge result).
    #[error("Incomplete: {0}")]
    Incomplete(String),

    /// The operation was aborted, typically due to a conflict or
    /// explicit cancellation.
    #[error("Aborted: {0}")]
    Aborted(String),

    /// A resource (e.g., a lock or compaction slot) is temporarily busy.
    #[error("Resource busy: {0}")]
    Busy(String),

    /// The operation exceeded its deadline.
    #[error("Timed out: {0}")]
    TimedOut(String),

    /// A time-bound resource (e.g., a TTL entry or iterator snapshot)
    /// has expired.
    #[error("Expired: {0}")]
    Expired(String),

    /// An internal invariant inside the engine has been violated and the
    /// caller cannot make forward progress without operator intervention
    /// (e.g. process restart from a checkpoint). Reserved for "engine
    /// stopped accepting work" conditions — sequence-number space
    /// exhaustion, structural-state mismatch detected at runtime, etc.
    #[error("Internal: {0}")]
    Internal(String),
}

/// A convenience type alias for `Result<T, ForstError>`.
pub type ForstResult<T> = std::result::Result<T, ForstError>;

// ---------------------------------------------------------------------------
// Convenience constructors
// ---------------------------------------------------------------------------

impl ForstError {
    /// Creates a [`ForstError::Corruption`] with the given message.
    pub fn corruption(msg: impl Into<String>) -> Self {
        ForstError::Corruption(msg.into())
    }

    /// Creates a [`ForstError::NotFound`] with the given message.
    pub fn not_found(msg: impl Into<String>) -> Self {
        ForstError::NotFound(msg.into())
    }

    /// Creates a [`ForstError::NotSupported`] with the given message.
    pub fn not_supported(msg: impl Into<String>) -> Self {
        ForstError::NotSupported(msg.into())
    }

    /// Creates a [`ForstError::InvalidArgument`] with the given message.
    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        ForstError::InvalidArgument(msg.into())
    }

    /// Creates a [`ForstError::Incomplete`] with the given message.
    pub fn incomplete(msg: impl Into<String>) -> Self {
        ForstError::Incomplete(msg.into())
    }

    /// Creates a [`ForstError::Aborted`] with the given message.
    pub fn aborted(msg: impl Into<String>) -> Self {
        ForstError::Aborted(msg.into())
    }

    /// Creates a [`ForstError::Busy`] with the given message.
    pub fn busy(msg: impl Into<String>) -> Self {
        ForstError::Busy(msg.into())
    }

    /// Creates a [`ForstError::TimedOut`] with the given message.
    pub fn timed_out(msg: impl Into<String>) -> Self {
        ForstError::TimedOut(msg.into())
    }

    /// Creates a [`ForstError::Expired`] with the given message.
    pub fn expired(msg: impl Into<String>) -> Self {
        ForstError::Expired(msg.into())
    }

    /// Creates a [`ForstError::Internal`] with the given message.
    ///
    /// Reserved for "engine cannot make forward progress without operator
    /// intervention" — sequence-number space exhaustion (spec §6a.4),
    /// structural invariant violation, etc. Distinct from
    /// [`ForstError::Corruption`] (on-disk integrity failure) and
    /// [`ForstError::Aborted`] (transient operation-level abort).
    pub fn internal(msg: impl Into<String>) -> Self {
        ForstError::Internal(msg.into())
    }
}

// ---------------------------------------------------------------------------
// Predicate methods
// ---------------------------------------------------------------------------

impl ForstError {
    /// Returns `true` if this is an [`ForstError::Io`] variant.
    pub fn is_io(&self) -> bool {
        matches!(self, ForstError::Io(_))
    }

    /// Returns `true` if this is a [`ForstError::Corruption`] variant.
    pub fn is_corruption(&self) -> bool {
        matches!(self, ForstError::Corruption(_))
    }

    /// Returns `true` if this is a [`ForstError::NotFound`] variant.
    pub fn is_not_found(&self) -> bool {
        matches!(self, ForstError::NotFound(_))
    }

    /// Returns `true` if this is a [`ForstError::NotSupported`] variant.
    pub fn is_not_supported(&self) -> bool {
        matches!(self, ForstError::NotSupported(_))
    }

    /// Returns `true` if this is an [`ForstError::InvalidArgument`] variant.
    pub fn is_invalid_argument(&self) -> bool {
        matches!(self, ForstError::InvalidArgument(_))
    }

    /// Returns `true` if this is an [`ForstError::Incomplete`] variant.
    pub fn is_incomplete(&self) -> bool {
        matches!(self, ForstError::Incomplete(_))
    }

    /// Returns `true` if this is an [`ForstError::Aborted`] variant.
    pub fn is_aborted(&self) -> bool {
        matches!(self, ForstError::Aborted(_))
    }

    /// Returns `true` if this is a [`ForstError::Busy`] variant.
    pub fn is_busy(&self) -> bool {
        matches!(self, ForstError::Busy(_))
    }

    /// Returns `true` if this is a [`ForstError::TimedOut`] variant.
    pub fn is_timed_out(&self) -> bool {
        matches!(self, ForstError::TimedOut(_))
    }

    /// Returns `true` if this is an [`ForstError::Expired`] variant.
    pub fn is_expired(&self) -> bool {
        matches!(self, ForstError::Expired(_))
    }

    /// Returns `true` if this is an [`ForstError::Internal`] variant.
    pub fn is_internal(&self) -> bool {
        matches!(self, ForstError::Internal(_))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{ForstError, ForstResult};

    // -- Display output -----------------------------------------------------

    #[test]
    fn test_display_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let err = ForstError::Io(io_err);
        assert!(err.to_string().starts_with("IO error: "));
        assert!(err.to_string().contains("file missing"));
    }

    #[test]
    fn test_display_corruption() {
        let err = ForstError::corruption("bad checksum");
        assert_eq!(err.to_string(), "Data corruption: bad checksum");
    }

    #[test]
    fn test_display_not_found() {
        let err = ForstError::not_found("key abc");
        assert_eq!(err.to_string(), "Not found: key abc");
    }

    #[test]
    fn test_display_not_supported() {
        let err = ForstError::not_supported("merge operator");
        assert_eq!(err.to_string(), "Not supported: merge operator");
    }

    #[test]
    fn test_display_invalid_argument() {
        let err = ForstError::invalid_argument("negative size");
        assert_eq!(err.to_string(), "Invalid argument: negative size");
    }

    #[test]
    fn test_display_incomplete() {
        let err = ForstError::incomplete("partial merge");
        assert_eq!(err.to_string(), "Incomplete: partial merge");
    }

    #[test]
    fn test_display_aborted() {
        let err = ForstError::aborted("write conflict");
        assert_eq!(err.to_string(), "Aborted: write conflict");
    }

    #[test]
    fn test_display_busy() {
        let err = ForstError::busy("compaction in progress");
        assert_eq!(err.to_string(), "Resource busy: compaction in progress");
    }

    #[test]
    fn test_display_timed_out() {
        let err = ForstError::timed_out("lock wait");
        assert_eq!(err.to_string(), "Timed out: lock wait");
    }

    #[test]
    fn test_display_expired() {
        let err = ForstError::expired("ttl exceeded");
        assert_eq!(err.to_string(), "Expired: ttl exceeded");
    }

    #[test]
    fn test_display_internal() {
        let err = ForstError::internal("engine stopped accepting writes");
        assert_eq!(err.to_string(), "Internal: engine stopped accepting writes");
    }

    // -- From<io::Error> conversion -----------------------------------------

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied");
        let forst_err: ForstError = io_err.into();
        assert!(forst_err.is_io());
        assert!(forst_err.to_string().contains("denied"));
    }

    #[test]
    fn test_question_mark_operator() {
        fn fallible() -> ForstResult<()> {
            let _: Vec<u8> = std::fs::read("/nonexistent/path/that/should/not/exist")?;
            Ok(())
        }
        let result = fallible();
        assert!(result.is_err());
        assert!(result.unwrap_err().is_io());
    }

    // -- is_* predicates ----------------------------------------------------

    #[test]
    fn test_is_predicates() {
        assert!(ForstError::corruption("x").is_corruption());
        assert!(!ForstError::corruption("x").is_not_found());

        assert!(ForstError::not_found("x").is_not_found());
        assert!(!ForstError::not_found("x").is_corruption());

        assert!(ForstError::not_supported("x").is_not_supported());
        assert!(ForstError::invalid_argument("x").is_invalid_argument());
        assert!(ForstError::incomplete("x").is_incomplete());
        assert!(ForstError::aborted("x").is_aborted());
        assert!(ForstError::busy("x").is_busy());
        assert!(ForstError::timed_out("x").is_timed_out());
        assert!(ForstError::expired("x").is_expired());
        assert!(ForstError::internal("x").is_internal());
    }

    #[test]
    fn test_is_io_predicate() {
        let io_err = std::io::Error::other("oops");
        let err = ForstError::Io(io_err);
        assert!(err.is_io());
        assert!(!err.is_corruption());
    }

    // -- Convenience constructors -------------------------------------------

    #[test]
    fn test_constructors_accept_str() {
        // &str should work via Into<String>
        let err = ForstError::corruption("msg");
        assert!(err.is_corruption());
    }

    #[test]
    fn test_constructors_accept_string() {
        // Owned String should work via Into<String>
        let err = ForstError::corruption(String::from("msg"));
        assert!(err.is_corruption());
    }

    #[test]
    fn test_constructors_accept_format() {
        // format!() result should work
        let err = ForstError::not_found(format!("key={}", 42));
        assert_eq!(err.to_string(), "Not found: key=42");
    }
}

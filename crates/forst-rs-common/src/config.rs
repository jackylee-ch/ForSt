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

//! Configuration structures for ForSt-RS.
//!
//! This module provides the main configuration types used to tune the
//! storage engine:
//!
//! - [`EngineOptions`] — Top-level database configuration (write buffers,
//!   compaction, block cache, compression, etc.)
//! - [`CfOptions`] — Per-column-family overrides that fall back to
//!   [`EngineOptions`] defaults when `None`.
//! - [`ReadOptions`] — Per-read operation settings (seek mode, checksums,
//!   cache policy).
//! - [`WriteOptions`] — Per-write operation settings (sync, WAL).
//! - [`ReadTier`] — Controls which storage tiers are consulted during reads.
//! - [`EngineOptionsBuilder`] — Builder for constructing [`EngineOptions`]
//!   with method chaining.

use crate::error::{ForstError, ForstResult};
use crate::types::{CompressionType, MAX_LEVELS};

// ---------------------------------------------------------------------------
// EngineOptions
// ---------------------------------------------------------------------------

/// Top-level database configuration for the ForSt-RS storage engine.
///
/// Controls write buffering, compaction behaviour, block cache sizing,
/// compression, and other global parameters. Use [`EngineOptions::builder()`]
/// for ergonomic construction with method chaining.
///
/// # Validation
///
/// Call [`validate()`](EngineOptions::validate) before opening a database to
/// ensure all invariants are satisfied (e.g. `db_path` is non-empty,
/// `num_levels` is within bounds).
#[derive(Debug, Clone)]
pub struct EngineOptions {
    /// Size of a single memtable write buffer in bytes.
    /// Default: 64 MB.
    pub write_buffer_size: usize,

    /// Maximum number of memtables (including the active one) before
    /// stalling writes. Default: 3.
    pub max_write_buffer_number: usize,

    /// Target size (bytes) for SST files at level-1. Default: 64 MB.
    pub target_file_size_base: usize,

    /// Maximum total bytes for level-1. Higher levels are computed as
    /// `max_bytes_for_level_base * max_bytes_for_level_multiplier^(L-1)`.
    /// Default: 256 MB.
    pub max_bytes_for_level_base: usize,

    /// Multiplier applied per level to compute the size limit of
    /// successive levels. Default: 10.0.
    pub max_bytes_for_level_multiplier: f64,

    /// Number of LSM-tree levels. Must be in `1..=MAX_LEVELS`.
    /// Default: 7.
    pub num_levels: usize,

    /// Maximum number of concurrent background compaction threads.
    /// Default: 4.
    pub max_background_compactions: usize,

    /// Maximum number of concurrent background flush threads.
    /// Default: 2.
    pub max_background_flushes: usize,

    /// Total capacity of the shared block cache in bytes. Default: 256 MB.
    pub block_cache_size: usize,

    /// Size of a single data block in bytes (Arrow DataBlock aligned).
    /// Default: 64 KB.
    pub block_size: usize,

    /// Number of bits per key used by the Bloom filter. Default: 10.
    pub bloom_bits_per_key: usize,

    /// Compression algorithm applied to SST data blocks.
    /// Default: [`CompressionType::Lz4`].
    pub compression: CompressionType,

    /// Whether to collect internal statistics (e.g. read/write amplification).
    /// Default: `true`.
    pub enable_statistics: bool,

    /// Filesystem path for the database directory. Must be set before opening.
    pub db_path: String,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 3,
            target_file_size_base: 64 * 1024 * 1024,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10.0,
            num_levels: 7,
            max_background_compactions: 4,
            max_background_flushes: 2,
            block_cache_size: 256 * 1024 * 1024,
            block_size: 64 * 1024,
            bloom_bits_per_key: 10,
            compression: CompressionType::Lz4,
            enable_statistics: true,
            db_path: String::new(),
        }
    }
}

impl EngineOptions {
    /// Returns a new [`EngineOptionsBuilder`] initialised with default values.
    pub fn builder() -> EngineOptionsBuilder {
        EngineOptionsBuilder {
            inner: EngineOptions::default(),
        }
    }

    /// Validates the configuration, returning an error if any invariant is
    /// violated.
    ///
    /// # Checks
    ///
    /// - `db_path` must not be empty.
    /// - `num_levels` must be in `1..=MAX_LEVELS`.
    /// - `write_buffer_size` must be greater than zero.
    /// - `block_size` must be greater than zero.
    /// - `max_bytes_for_level_multiplier` must be **finite** and **strictly
    ///   greater than 1.0** (R-loop r3 H#1, 2026-05-08). The downstream
    ///   per-level capacity formula is `base * multiplier^(L-1)`; a NaN /
    ///   ±∞ / negative / `≤ 1.0` value either saturates capacities to
    ///   `usize::MAX`, collapses them to 0 (causing infinite compaction
    ///   storms), or never grows the LSM, all of which are DoS vectors
    ///   when the config originates from an untrusted source (FFI / on-
    ///   disk decode).
    pub fn validate(&self) -> ForstResult<()> {
        if self.db_path.is_empty() {
            return Err(ForstError::invalid_argument("db_path must not be empty"));
        }
        if self.num_levels == 0 || self.num_levels > MAX_LEVELS {
            return Err(ForstError::invalid_argument(format!(
                "num_levels must be in 1..={}, got {}",
                MAX_LEVELS, self.num_levels
            )));
        }
        if self.write_buffer_size == 0 {
            return Err(ForstError::invalid_argument(
                "write_buffer_size must be greater than zero",
            ));
        }
        if self.block_size == 0 {
            return Err(ForstError::invalid_argument(
                "block_size must be greater than zero",
            ));
        }
        // R-loop r3 H#1: reject NaN / ±∞ / non-positive multiplier so the
        // downstream level-capacity formula `base * multiplier^(L-1)` cannot
        // saturate or collapse under untrusted config.
        if !self.max_bytes_for_level_multiplier.is_finite()
            || self.max_bytes_for_level_multiplier <= 1.0
        {
            return Err(ForstError::invalid_argument(format!(
                "max_bytes_for_level_multiplier must be finite and > 1.0, got {}",
                self.max_bytes_for_level_multiplier
            )));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EngineOptionsBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing [`EngineOptions`] with method chaining.
///
/// # Example
///
/// ```ignore
/// let opts = EngineOptions::builder()
///     .db_path("/tmp/forst-db")
///     .write_buffer_size(128 * 1024 * 1024)
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct EngineOptionsBuilder {
    inner: EngineOptions,
}

impl EngineOptionsBuilder {
    /// Sets the database directory path.
    pub fn db_path(mut self, path: impl Into<String>) -> Self {
        self.inner.db_path = path.into();
        self
    }

    /// Sets the write buffer (memtable) size in bytes.
    pub fn write_buffer_size(mut self, size: usize) -> Self {
        self.inner.write_buffer_size = size;
        self
    }

    /// Sets the maximum number of write buffers.
    pub fn max_write_buffer_number(mut self, n: usize) -> Self {
        self.inner.max_write_buffer_number = n;
        self
    }

    /// Sets the target SST file size at level-1 in bytes.
    pub fn target_file_size_base(mut self, size: usize) -> Self {
        self.inner.target_file_size_base = size;
        self
    }

    /// Sets the maximum total bytes for level-1.
    pub fn max_bytes_for_level_base(mut self, size: usize) -> Self {
        self.inner.max_bytes_for_level_base = size;
        self
    }

    /// Sets the per-level size multiplier.
    pub fn max_bytes_for_level_multiplier(mut self, multiplier: f64) -> Self {
        self.inner.max_bytes_for_level_multiplier = multiplier;
        self
    }

    /// Sets the number of LSM-tree levels.
    pub fn num_levels(mut self, n: usize) -> Self {
        self.inner.num_levels = n;
        self
    }

    /// Sets the maximum number of background compaction threads.
    pub fn max_background_compactions(mut self, n: usize) -> Self {
        self.inner.max_background_compactions = n;
        self
    }

    /// Sets the maximum number of background flush threads.
    pub fn max_background_flushes(mut self, n: usize) -> Self {
        self.inner.max_background_flushes = n;
        self
    }

    /// Sets the block cache capacity in bytes.
    pub fn block_cache_size(mut self, size: usize) -> Self {
        self.inner.block_cache_size = size;
        self
    }

    /// Sets the data block size in bytes.
    pub fn block_size(mut self, size: usize) -> Self {
        self.inner.block_size = size;
        self
    }

    /// Sets the number of bits per key for the Bloom filter.
    pub fn bloom_bits_per_key(mut self, bits: usize) -> Self {
        self.inner.bloom_bits_per_key = bits;
        self
    }

    /// Sets the compression algorithm for SST data blocks.
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.inner.compression = compression;
        self
    }

    /// Enables or disables internal statistics collection.
    pub fn enable_statistics(mut self, enable: bool) -> Self {
        self.inner.enable_statistics = enable;
        self
    }

    /// Consumes the builder and returns the configured [`EngineOptions`].
    pub fn build(self) -> EngineOptions {
        self.inner
    }
}

// ---------------------------------------------------------------------------
// CfOptions
// ---------------------------------------------------------------------------

/// Per-column-family configuration overrides.
///
/// Fields set to `None` inherit the corresponding value from
/// [`EngineOptions`]. This allows individual column families to diverge
/// from the database-wide defaults where necessary.
#[derive(Debug, Clone, Default)]
pub struct CfOptions {
    /// Override for [`EngineOptions::write_buffer_size`].
    pub write_buffer_size: Option<usize>,

    /// Override for [`EngineOptions::max_write_buffer_number`].
    pub max_write_buffer_number: Option<usize>,

    /// Override for [`EngineOptions::target_file_size_base`].
    pub target_file_size_base: Option<usize>,

    /// Override for [`EngineOptions::compression`].
    pub compression: Option<CompressionType>,

    /// Name of the merge operator to use. `None` means no merge operator.
    pub merge_operator: Option<String>,

    /// Time-to-live in seconds. `None` means entries never expire.
    pub ttl_seconds: Option<u64>,
}

impl CfOptions {
    /// Resolves the effective write buffer size, falling back to the
    /// engine-level default when this option is `None`.
    pub fn effective_write_buffer_size(&self, engine: &EngineOptions) -> usize {
        self.write_buffer_size.unwrap_or(engine.write_buffer_size)
    }

    /// Resolves the effective max write buffer number.
    pub fn effective_max_write_buffer_number(&self, engine: &EngineOptions) -> usize {
        self.max_write_buffer_number
            .unwrap_or(engine.max_write_buffer_number)
    }

    /// Resolves the effective target file size base.
    pub fn effective_target_file_size_base(&self, engine: &EngineOptions) -> usize {
        self.target_file_size_base
            .unwrap_or(engine.target_file_size_base)
    }

    /// Resolves the effective compression type.
    pub fn effective_compression(&self, engine: &EngineOptions) -> CompressionType {
        self.compression.unwrap_or(engine.compression)
    }
}

// ---------------------------------------------------------------------------
// ReadTier
// ---------------------------------------------------------------------------

/// Controls which storage tiers are consulted during a read operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadTier {
    /// Read from memtable, block cache, and SST files (full read path).
    #[default]
    ReadBoth,

    /// Read only from memtable and block cache (no disk I/O).
    BlockCacheOnly,
}

// ---------------------------------------------------------------------------
// ReadOptions
// ---------------------------------------------------------------------------

/// Per-read operation configuration.
///
/// Controls seek behaviour, checksum verification, cache policy, and
/// the storage tier used for reads.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// When `true`, the iterator will stop when it encounters a key with
    /// a different prefix than the seek key. Default: `false`.
    pub prefix_same_as_start: bool,

    /// When `true`, disables prefix-based filtering and forces a total
    /// order seek across all keys. Default: `false`.
    pub total_order_seek: bool,

    /// When `true`, data read from SST files is verified against stored
    /// checksums. Default: `true`.
    pub verify_checksums: bool,

    /// When `true`, data blocks read from disk are inserted into the
    /// block cache. Default: `true`.
    pub fill_cache: bool,

    /// Which storage tiers to consult. Default: [`ReadTier::ReadBoth`].
    pub read_tier: ReadTier,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            prefix_same_as_start: false,
            total_order_seek: false,
            verify_checksums: true,
            fill_cache: true,
            read_tier: ReadTier::ReadBoth,
        }
    }
}

// ---------------------------------------------------------------------------
// WriteOptions
// ---------------------------------------------------------------------------

/// Per-write operation configuration.
///
/// Controls durability guarantees for individual write operations.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// When `true`, the write is flushed to persistent storage before
    /// returning. Default: `false`.
    pub sync: bool,

    /// When `true`, the write is not recorded in the write-ahead log.
    /// This improves write performance but risks data loss on crash.
    /// Default: `false`.
    pub disable_wal: bool,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CompressionType;

    // -----------------------------------------------------------------------
    // EngineOptions defaults
    // -----------------------------------------------------------------------

    #[test]
    fn test_engine_options_default_write_buffer_size() {
        let opts = EngineOptions::default();
        assert_eq!(opts.write_buffer_size, 64 * 1024 * 1024);
    }

    #[test]
    fn test_engine_options_default_max_write_buffer_number() {
        let opts = EngineOptions::default();
        assert_eq!(opts.max_write_buffer_number, 3);
    }

    #[test]
    fn test_engine_options_default_target_file_size_base() {
        let opts = EngineOptions::default();
        assert_eq!(opts.target_file_size_base, 64 * 1024 * 1024);
    }

    #[test]
    fn test_engine_options_default_level_settings() {
        let opts = EngineOptions::default();
        assert_eq!(opts.max_bytes_for_level_base, 256 * 1024 * 1024);
        assert!((opts.max_bytes_for_level_multiplier - 10.0).abs() < f64::EPSILON);
        assert_eq!(opts.num_levels, 7);
    }

    #[test]
    fn test_engine_options_default_background_threads() {
        let opts = EngineOptions::default();
        assert_eq!(opts.max_background_compactions, 4);
        assert_eq!(opts.max_background_flushes, 2);
    }

    #[test]
    fn test_engine_options_default_block_cache_and_size() {
        let opts = EngineOptions::default();
        assert_eq!(opts.block_cache_size, 256 * 1024 * 1024);
        assert_eq!(opts.block_size, 64 * 1024);
    }

    #[test]
    fn test_engine_options_default_bloom_compression_stats() {
        let opts = EngineOptions::default();
        assert_eq!(opts.bloom_bits_per_key, 10);
        assert_eq!(opts.compression, CompressionType::Lz4);
        assert!(opts.enable_statistics);
    }

    #[test]
    fn test_engine_options_default_db_path_empty() {
        let opts = EngineOptions::default();
        assert!(opts.db_path.is_empty());
    }

    // -----------------------------------------------------------------------
    // Builder pattern
    // -----------------------------------------------------------------------

    #[test]
    fn test_builder_sets_all_fields() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/test-db")
            .write_buffer_size(128 * 1024 * 1024)
            .max_write_buffer_number(5)
            .target_file_size_base(32 * 1024 * 1024)
            .max_bytes_for_level_base(512 * 1024 * 1024)
            .max_bytes_for_level_multiplier(8.0)
            .num_levels(5)
            .max_background_compactions(8)
            .max_background_flushes(4)
            .block_cache_size(512 * 1024 * 1024)
            .block_size(32 * 1024)
            .bloom_bits_per_key(16)
            .compression(CompressionType::Zstd)
            .enable_statistics(false)
            .build();

        assert_eq!(opts.db_path, "/tmp/test-db");
        assert_eq!(opts.write_buffer_size, 128 * 1024 * 1024);
        assert_eq!(opts.max_write_buffer_number, 5);
        assert_eq!(opts.target_file_size_base, 32 * 1024 * 1024);
        assert_eq!(opts.max_bytes_for_level_base, 512 * 1024 * 1024);
        assert!((opts.max_bytes_for_level_multiplier - 8.0).abs() < f64::EPSILON);
        assert_eq!(opts.num_levels, 5);
        assert_eq!(opts.max_background_compactions, 8);
        assert_eq!(opts.max_background_flushes, 4);
        assert_eq!(opts.block_cache_size, 512 * 1024 * 1024);
        assert_eq!(opts.block_size, 32 * 1024);
        assert_eq!(opts.bloom_bits_per_key, 16);
        assert_eq!(opts.compression, CompressionType::Zstd);
        assert!(!opts.enable_statistics);
    }

    #[test]
    fn test_builder_defaults_match_engine_options_default() {
        let from_builder = EngineOptions::builder().build();
        let from_default = EngineOptions::default();

        assert_eq!(
            from_builder.write_buffer_size,
            from_default.write_buffer_size
        );
        assert_eq!(
            from_builder.max_write_buffer_number,
            from_default.max_write_buffer_number
        );
        assert_eq!(
            from_builder.target_file_size_base,
            from_default.target_file_size_base
        );
        assert_eq!(
            from_builder.max_bytes_for_level_base,
            from_default.max_bytes_for_level_base
        );
        assert_eq!(from_builder.num_levels, from_default.num_levels);
        assert_eq!(
            from_builder.max_background_compactions,
            from_default.max_background_compactions
        );
        assert_eq!(
            from_builder.max_background_flushes,
            from_default.max_background_flushes
        );
        assert_eq!(from_builder.block_cache_size, from_default.block_cache_size);
        assert_eq!(from_builder.block_size, from_default.block_size);
        assert_eq!(
            from_builder.bloom_bits_per_key,
            from_default.bloom_bits_per_key
        );
        assert_eq!(from_builder.compression, from_default.compression);
        assert_eq!(
            from_builder.enable_statistics,
            from_default.enable_statistics
        );
        assert_eq!(from_builder.db_path, from_default.db_path);
    }

    #[test]
    fn test_builder_accepts_string_and_str_for_db_path() {
        let opts1 = EngineOptions::builder().db_path("literal").build();
        let opts2 = EngineOptions::builder()
            .db_path(String::from("owned"))
            .build();
        assert_eq!(opts1.db_path, "literal");
        assert_eq!(opts2.db_path, "owned");
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_valid_config() {
        let opts = EngineOptions::builder().db_path("/tmp/valid").build();
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn test_validate_fails_for_empty_db_path() {
        let opts = EngineOptions::default();
        let err = opts.validate().unwrap_err();
        assert!(err.is_invalid_argument());
        assert!(err.to_string().contains("db_path"));
    }

    #[test]
    fn test_validate_fails_for_zero_num_levels() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .num_levels(0)
            .build();
        let err = opts.validate().unwrap_err();
        assert!(err.is_invalid_argument());
        assert!(err.to_string().contains("num_levels"));
    }

    #[test]
    fn test_validate_fails_for_num_levels_exceeding_max() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .num_levels(MAX_LEVELS + 1)
            .build();
        let err = opts.validate().unwrap_err();
        assert!(err.is_invalid_argument());
        assert!(err.to_string().contains("num_levels"));
    }

    /// Regression test for R-loop r3 H#1: `validate()` rejects NaN
    /// `max_bytes_for_level_multiplier` (would saturate level capacities to 0,
    /// causing infinite compaction storms).
    #[test]
    fn test_validate_rejects_nan_multiplier() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_multiplier(f64::NAN)
            .build();
        let err = opts.validate().unwrap_err();
        assert!(err.is_invalid_argument());
        assert!(err.to_string().contains("max_bytes_for_level_multiplier"));
    }

    /// R-loop r3 H#1: rejects ±Inf multiplier (would saturate capacities to
    /// usize::MAX cascading through level computations).
    #[test]
    fn test_validate_rejects_inf_multiplier() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_multiplier(f64::INFINITY)
            .build();
        assert!(opts.validate().is_err());

        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_multiplier(f64::NEG_INFINITY)
            .build();
        assert!(opts.validate().is_err());
    }

    /// R-loop r3 H#1: rejects multiplier ≤ 1.0 (LSM doesn't grow → write
    /// amplification cascade or compaction storm).
    #[test]
    fn test_validate_rejects_multiplier_le_one() {
        for v in [0.0f64, 1.0, -1.0, 0.5] {
            let opts = EngineOptions::builder()
                .db_path("/tmp/db")
                .max_bytes_for_level_multiplier(v)
                .build();
            assert!(
                opts.validate().is_err(),
                "multiplier={} should be rejected",
                v
            );
        }
    }

    #[test]
    fn test_validate_fails_for_zero_write_buffer_size() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(0)
            .build();
        let err = opts.validate().unwrap_err();
        assert!(err.is_invalid_argument());
        assert!(err.to_string().contains("write_buffer_size"));
    }

    #[test]
    fn test_validate_fails_for_zero_block_size() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .block_size(0)
            .build();
        let err = opts.validate().unwrap_err();
        assert!(err.is_invalid_argument());
        assert!(err.to_string().contains("block_size"));
    }

    // -----------------------------------------------------------------------
    // CfOptions
    // -----------------------------------------------------------------------

    #[test]
    fn test_cf_options_default_all_none() {
        let cf = CfOptions::default();
        assert!(cf.write_buffer_size.is_none());
        assert!(cf.max_write_buffer_number.is_none());
        assert!(cf.target_file_size_base.is_none());
        assert!(cf.compression.is_none());
        assert!(cf.merge_operator.is_none());
        assert!(cf.ttl_seconds.is_none());
    }

    #[test]
    fn test_cf_options_effective_values_fallback_to_engine() {
        let engine = EngineOptions::default();
        let cf = CfOptions::default();

        assert_eq!(
            cf.effective_write_buffer_size(&engine),
            engine.write_buffer_size
        );
        assert_eq!(
            cf.effective_max_write_buffer_number(&engine),
            engine.max_write_buffer_number
        );
        assert_eq!(
            cf.effective_target_file_size_base(&engine),
            engine.target_file_size_base
        );
        assert_eq!(cf.effective_compression(&engine), engine.compression);
    }

    #[test]
    fn test_cf_options_effective_values_override_engine() {
        let engine = EngineOptions::default();
        let cf = CfOptions {
            write_buffer_size: Some(32 * 1024 * 1024),
            max_write_buffer_number: Some(6),
            target_file_size_base: Some(16 * 1024 * 1024),
            compression: Some(CompressionType::Zstd),
            merge_operator: Some("my_merge".to_string()),
            ttl_seconds: Some(3600),
        };

        assert_eq!(cf.effective_write_buffer_size(&engine), 32 * 1024 * 1024);
        assert_eq!(cf.effective_max_write_buffer_number(&engine), 6);
        assert_eq!(
            cf.effective_target_file_size_base(&engine),
            16 * 1024 * 1024
        );
        assert_eq!(cf.effective_compression(&engine), CompressionType::Zstd);
        assert_eq!(cf.merge_operator.as_deref(), Some("my_merge"));
        assert_eq!(cf.ttl_seconds, Some(3600));
    }

    // -----------------------------------------------------------------------
    // ReadOptions
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_options_defaults() {
        let opts = ReadOptions::default();
        assert!(!opts.prefix_same_as_start);
        assert!(!opts.total_order_seek);
        assert!(opts.verify_checksums);
        assert!(opts.fill_cache);
        assert_eq!(opts.read_tier, ReadTier::ReadBoth);
    }

    #[test]
    fn test_read_tier_default_is_read_both() {
        assert_eq!(ReadTier::default(), ReadTier::ReadBoth);
    }

    #[test]
    fn test_read_tier_equality() {
        assert_eq!(ReadTier::ReadBoth, ReadTier::ReadBoth);
        assert_eq!(ReadTier::BlockCacheOnly, ReadTier::BlockCacheOnly);
        assert_ne!(ReadTier::ReadBoth, ReadTier::BlockCacheOnly);
    }

    // -----------------------------------------------------------------------
    // WriteOptions
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_options_defaults() {
        let opts = WriteOptions::default();
        assert!(!opts.sync);
        assert!(!opts.disable_wal);
    }

    #[test]
    fn test_write_options_custom() {
        let opts = WriteOptions {
            sync: true,
            disable_wal: true,
        };
        assert!(opts.sync);
        assert!(opts.disable_wal);
    }

    // -----------------------------------------------------------------------
    // Clone / Debug traits
    // -----------------------------------------------------------------------

    #[test]
    fn test_engine_options_clone() {
        let original = EngineOptions::builder()
            .db_path("/tmp/clone-test")
            .write_buffer_size(42)
            .build();
        let cloned = original.clone();
        assert_eq!(cloned.db_path, "/tmp/clone-test");
        assert_eq!(cloned.write_buffer_size, 42);
    }

    #[test]
    fn test_engine_options_debug() {
        let opts = EngineOptions::builder().db_path("/tmp/debug").build();
        let debug = format!("{:?}", opts);
        assert!(debug.contains("EngineOptions"));
        assert!(debug.contains("/tmp/debug"));
    }
}

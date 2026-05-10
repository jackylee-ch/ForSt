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

/// Upper bound on `EngineOptions::max_bytes_for_level_multiplier`.
///
/// 1024 covers every realistic LSM tuning (RocksDB defaults at 10×; even
/// 100× is already extreme). Values above this would cascade
/// `base * multiplier^(L-1)` past `f64::MAX` and saturate per-level
/// capacities to `usize::MAX` — re-opening the DoS vector that the r3
/// fix targeted (R-loop r4 H#1 regression of own r3 fix).
pub const MAX_LEVEL_MULTIPLIER: f64 = 1024.0;

/// Upper bound on `EngineOptions::max_bytes_for_level_base` (1 PiB).
///
/// Realistic LSM L1 capacity is ≪ 1 PiB; this cap prevents an untrusted
/// `usize::MAX` from reaching the engine's `pick_compaction_level` and
/// disabling compaction entirely (R-loop r5 Sec H#1).
pub const MAX_LEVEL_BASE: usize = 1 << 50;

/// Upper bound on `EngineOptions::write_buffer_size` (1 TiB).
///
/// A `usize::MAX` write-buffer means the memtable threshold is never
/// crossed → unbounded heap growth → OOM (R-loop r5 Sec H#2).
pub const MAX_WRITE_BUFFER_SIZE: usize = 1 << 40;

/// Upper bound on `EngineOptions::block_cache_size` (1 PiB).
///
/// Prevents an untrusted `usize::MAX` from triggering OOM-abort in
/// block-cache initialization (R-loop r5 Sec H#2).
pub const MAX_BLOCK_CACHE_SIZE: usize = 1 << 50;

/// Upper bound on `EngineOptions::max_background_compactions` /
/// `max_background_flushes`.
///
/// Prevents an untrusted `usize::MAX` from spawning unbounded threads
/// (FD/thread-limit exhaustion DoS — R-loop r5 Sec H#2).
pub const MAX_BACKGROUND_THREADS: usize = 1024;

/// Upper bound on `EngineOptions::bloom_bits_per_key` (R-loop r9 Sec H#1).
///
/// RocksDB defaults at 10; values above 30 are already wasteful. 256 is a
/// generous cap that prevents OOM-abort during Bloom filter allocation
/// (`bits_per_key * num_keys` can overflow / saturate / explode).
pub const MAX_BLOOM_BITS_PER_KEY: usize = 256;

/// Upper bound on `EngineOptions::db_path` length in bytes (R-loop r9 Sec
/// H#2). Matches POSIX `PATH_MAX`.
pub const MAX_DB_PATH_LEN: usize = 4096;

/// Lower bound on `EngineOptions::block_size` (R-loop r6 Sec H#3).
///
/// `block_size = 1` with default `write_buffer_size` causes the SST
/// writer to flush a block per entry, producing multi-GiB index +
/// Bloom-filter payloads per SST → slow-amplification DoS.
pub const MIN_BLOCK_SIZE: usize = 512;

/// Upper bound on `EngineOptions::block_size` (1 GiB; R-loop r6 Sec H#1
/// + r6 Errors H_F1).
///
/// Mirrors the r5 cap on `block_cache_size` / `write_buffer_size`. The
/// SST writer's hot-path `Vec::with_capacity(block_size + 1024)` would
/// OOM-abort or wrap on `usize::MAX`. RocksDB's largest sane block is
/// ~256 KiB; 1 GiB is a generous safety margin.
pub const MAX_BLOCK_SIZE: usize = 1 << 30;

/// Upper bound on `EngineOptions::target_file_size_base` (1 TiB;
/// R-loop r6 Errors H_F2).
///
/// Prevents an untrusted `usize::MAX` per-SST file budget from never
/// triggering a roll → unbounded heap/disk growth, and from saturating
/// `base * multiplier^(L-1)`-style level-target arithmetic when paired
/// with `MAX_LEVEL_BASE`.
pub const MAX_TARGET_FILE_SIZE_BASE: usize = 1 << 40;

/// Lower bound on `EngineOptions::target_file_size_base` (4 KiB; R-loop
/// S2-r7 Sec H#1).
///
/// Symmetric to [`MIN_BLOCK_SIZE`]: at `target_file_size_base = 1..4095`,
/// the SST writer rolls a new file every few records, producing thousands
/// of tiny SST files per memtable flush → inode / FD exhaustion DoS. 4 KiB
/// (one OS page) is generous: realistic SSTs are MiB-scale, but the floor
/// blocks the pathological per-record-rollover regime without breaking
/// small-scale unit tests.
pub const MIN_TARGET_FILE_SIZE_BASE: usize = 1 << 12;

/// Lower bound on `EngineOptions::write_buffer_size` (4 KiB; R-loop
/// S2-r8 Sec H#1).
///
/// Symmetric to [`MIN_TARGET_FILE_SIZE_BASE`]: at `write_buffer_size =
/// 1..4095`, every put trips the active-memtable threshold → forced
/// per-record flush → tiny SST per record (inode/FD exhaustion) AND
/// rapidly saturates `max_write_buffer_number` triggering write-stall.
/// Compounds inode DoS with backpressure-stall DoS.
pub const MIN_WRITE_BUFFER_SIZE: usize = 1 << 12;

/// Lower bound on `EngineOptions::max_bytes_for_level_base` (1 MiB; R-loop
/// S2-r8 Sec H#2).
///
/// At `max_bytes_for_level_base = 1..N`, every level-1 SST exceeds its
/// target capacity → continuous compaction loop → CPU / IO storm. The
/// r7 doc comment for the zero-check explicitly calls out this scenario
/// as "permanent compaction storm" but the per-axis check only rejects
/// `== 0`. 1 MiB is below realistic deployments (RocksDB defaults at
/// 256 MiB) but well above the per-record-pathological regime.
pub const MIN_LEVEL_BASE: usize = 1 << 20;

/// Upper bound on `EngineOptions::max_write_buffer_number` (R-loop r6
/// Sec H#2).
///
/// `usize::MAX` makes the immutable-memtable count comparison
/// `imm >= max_write_buffer_number` never true, bypassing
/// `WriteController` write-stall backpressure → unbounded heap growth.
/// RocksDB defaults at 2–4; 1024 is generous.
pub const MAX_WRITE_BUFFER_NUMBER: usize = 1024;

/// R-loop r19 Sec H#1: joint cap on `write_buffer_size × max_write_buffer_number`.
/// Per-axis caps admit 1 TiB × 1024 = 1 PiB — beyond any plausible physical
/// RAM. 8 TiB picked to defend against the pathological joint product while
/// preserving headroom above existing per-axis acceptance assertions.
pub const MAX_JOINT_MEMTABLE_BYTES: usize = 1 << 43;

/// R-loop r19 Sec H#2: joint cap on per-SST bloom-filter allocation
/// `bloom_bits_per_key × (target_file_size_base / 16) / 8`. Per-axis caps
/// admit 256 × 1 TiB / 128 = 2 TiB per-SST. 256 GiB cap rejects pathological
/// configs while preserving headroom above existing per-axis assertions.
pub const MAX_JOINT_BLOOM_BYTES: usize = 1 << 38;

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

    /// Number of shards used by each per-CF active memtable
    /// (`ShardedMemTable`). Concurrent writers hashing to different shards
    /// never block each other on the per-shard `RwLock`.
    ///
    /// Default: 16 — enough for typical multi-core hosts (≥ JMH bench
    /// `availableProcessors()`) while keeping the per-CF memtable footprint
    /// low. Power-of-two values give a cheap AND-mask shard lookup; other
    /// values fall back to modulus. Clamped to `[1, 256]` by the
    /// `ShardedMemTable` constructor (0 → default).
    pub memtable_shards: usize,
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
            memtable_shards: 16,
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
        // R-loop r9 Sec H#2: cap path length and reject embedded NUL.
        // Traversal-component rejection is consumer-crate scope (filesystem
        // layer); we cover length-DoS + the NUL-truncation TOCTOU between
        // Rust and C string views.
        if self.db_path.len() > MAX_DB_PATH_LEN {
            return Err(ForstError::invalid_argument(format!(
                "db_path length must be ≤ {} bytes (POSIX PATH_MAX), got {}",
                MAX_DB_PATH_LEN,
                self.db_path.len()
            )));
        }
        if self.db_path.as_bytes().contains(&0u8) {
            return Err(ForstError::invalid_argument(
                "db_path must not contain embedded NUL bytes",
            ));
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
        // R-loop r7 Errors H_F1+H_F2+H_F3: symmetric lower-bound checks
        // for fields the r5/r6 upper caps already covered. Each `== 0`
        // case is a distinct DoS vector:
        //
        //   max_bytes_for_level_base = 0    →  every level cap = 0
        //                                       ⇒ permanent compaction storm
        //   target_file_size_base    = 0    →  SST writer rolls per write
        //                                       ⇒ inode/FD exhaustion
        //   max_background_compactions = 0  →  no compaction threads
        //                                       ⇒ L0 grows unbounded
        //   max_background_flushes   = 0    →  no flush threads
        //                                       ⇒ memtable count rises until stall
        //   max_write_buffer_number  = 0    →  imm_count >= 0 always true
        //                                       ⇒ writes stall on every op
        if self.max_bytes_for_level_base == 0 {
            return Err(ForstError::invalid_argument(
                "max_bytes_for_level_base must be greater than zero",
            ));
        }
        if self.target_file_size_base == 0 {
            return Err(ForstError::invalid_argument(
                "target_file_size_base must be greater than zero",
            ));
        }
        if self.max_background_compactions == 0 {
            return Err(ForstError::invalid_argument(
                "max_background_compactions must be greater than zero",
            ));
        }
        if self.max_background_flushes == 0 {
            return Err(ForstError::invalid_argument(
                "max_background_flushes must be greater than zero",
            ));
        }
        if self.max_write_buffer_number == 0 {
            return Err(ForstError::invalid_argument(
                "max_write_buffer_number must be greater than zero",
            ));
        }
        // R-loop r9 Sec H#1: cap bloom_bits_per_key so `bits * num_keys`
        // can't OOM-abort during Bloom filter allocation.
        if self.bloom_bits_per_key > MAX_BLOOM_BITS_PER_KEY {
            return Err(ForstError::invalid_argument(format!(
                "bloom_bits_per_key must be ≤ {}, got {}",
                MAX_BLOOM_BITS_PER_KEY, self.bloom_bits_per_key
            )));
        }
        // R-loop r6 Sec H#1+H#3 / Errors H_F1: bound block_size both ways.
        // Upper: SST writer's `Vec::with_capacity(block_size + 1024)` would
        //        OOM/wrap on usize::MAX.
        // Lower: tiny block_size causes per-entry flushes → multi-GiB
        //        index/bloom payloads → slow-amplification DoS.
        if self.block_size < MIN_BLOCK_SIZE || self.block_size > MAX_BLOCK_SIZE {
            return Err(ForstError::invalid_argument(format!(
                "block_size must be in [{}, {}] bytes, got {}",
                MIN_BLOCK_SIZE, MAX_BLOCK_SIZE, self.block_size
            )));
        }
        // R-loop S2-r7 Sec H#1: floor target_file_size_base (symmetric to
        // r6 MIN_BLOCK_SIZE). At <4 KiB, the SST writer rolls per-record
        // → inode/FD exhaustion DoS during a single memtable flush.
        // Zero-check above (line 298) handles 0; this catches 1..MIN-1.
        if self.target_file_size_base != 0 && self.target_file_size_base < MIN_TARGET_FILE_SIZE_BASE
        {
            return Err(ForstError::invalid_argument(format!(
                "target_file_size_base must be ≥ {} bytes (4 KiB), got {}",
                MIN_TARGET_FILE_SIZE_BASE, self.target_file_size_base
            )));
        }
        // R-loop S2-r8 Sec H#1: floor write_buffer_size (parallel-symmetry
        // gap S2-r7 missed). At <4 KiB, every put trips memtable threshold
        // → per-record flush → inode + write-stall DoS.
        if self.write_buffer_size != 0 && self.write_buffer_size < MIN_WRITE_BUFFER_SIZE {
            return Err(ForstError::invalid_argument(format!(
                "write_buffer_size must be ≥ {} bytes (4 KiB), got {}",
                MIN_WRITE_BUFFER_SIZE, self.write_buffer_size
            )));
        }
        // R-loop S2-r8 Sec H#2: floor max_bytes_for_level_base. At <1 MiB,
        // every L1 SST exceeds level capacity → permanent compaction
        // storm (CPU/IO DoS). The r7 doc comment already names this
        // vector but only the zero-check guards it.
        if self.max_bytes_for_level_base != 0 && self.max_bytes_for_level_base < MIN_LEVEL_BASE {
            return Err(ForstError::invalid_argument(format!(
                "max_bytes_for_level_base must be ≥ {} bytes (1 MiB), got {}",
                MIN_LEVEL_BASE, self.max_bytes_for_level_base
            )));
        }
        // R-loop r6 Errors H_F2: cap target_file_size_base.
        if self.target_file_size_base > MAX_TARGET_FILE_SIZE_BASE {
            return Err(ForstError::invalid_argument(format!(
                "target_file_size_base must be ≤ {} bytes (1 TiB), got {}",
                MAX_TARGET_FILE_SIZE_BASE, self.target_file_size_base
            )));
        }
        // R-loop r6 Sec H#2: cap max_write_buffer_number to prevent
        // write-stall backpressure bypass.
        if self.max_write_buffer_number > MAX_WRITE_BUFFER_NUMBER {
            return Err(ForstError::invalid_argument(format!(
                "max_write_buffer_number must be ≤ {}, got {}",
                MAX_WRITE_BUFFER_NUMBER, self.max_write_buffer_number
            )));
        }
        // R-loop r3 H#1 + r4 H#1: reject NaN / ±∞ / non-positive AND
        // cap the upper bound at MAX_LEVEL_MULTIPLIER so the downstream
        // `base * multiplier^(L-1)` cannot saturate to `usize::MAX` via
        // a finite-but-astronomical input (e.g. `f64::MAX`).
        if !self.max_bytes_for_level_multiplier.is_finite()
            || self.max_bytes_for_level_multiplier <= 1.0
            || self.max_bytes_for_level_multiplier > MAX_LEVEL_MULTIPLIER
        {
            return Err(ForstError::invalid_argument(format!(
                "max_bytes_for_level_multiplier must be finite and in (1.0, {}], got {}",
                MAX_LEVEL_MULTIPLIER, self.max_bytes_for_level_multiplier
            )));
        }
        // R-loop r18 Security H#1 + Correctness H_F1: per-axis r3/r4/r5 caps
        // bound base, multiplier, and num_levels INDEPENDENTLY but their
        // joint product `base * multiplier^(num_levels-1)` can still
        // saturate the downstream `as usize` cast even with all axes at
        // boundary (e.g. 2^50 × 1024^6 = 2^110 >> usize::MAX = 2^64). This
        // re-opens the exact DoS that r3/r4/r5 targeted (level cap
        // collapses to usize::MAX → upper levels never compact → unbounded
        // growth). Bound the product space directly so saturation is
        // structurally impossible. Half of usize::MAX gives a safety
        // margin for downstream arithmetic that may double the value
        // (e.g. cap + buffer).
        let max_level_idx = self.num_levels.saturating_sub(1) as i32;
        let projected = (self.max_bytes_for_level_base as f64)
            * self.max_bytes_for_level_multiplier.powi(max_level_idx);
        let usize_half = (usize::MAX as f64) / 2.0;
        if !projected.is_finite() || projected > usize_half {
            return Err(ForstError::invalid_argument(format!(
                "joint level-capacity product saturates: \
                 base={}, multiplier={}, num_levels={} → projected={:e} > usize::MAX/2 ({:e})",
                self.max_bytes_for_level_base,
                self.max_bytes_for_level_multiplier,
                self.num_levels,
                projected,
                usize_half
            )));
        }
        // R-loop r19 Sec H#1: peak memtable RAM commitment joint cap.
        // Per-axis caps (r5 write_buffer_size ≤ 1 TiB, r6
        // max_write_buffer_number ≤ 1024) admit a 1 PiB joint product —
        // far beyond any plausible physical RAM. A single config call
        // from untrusted input can therefore pin the process toward
        // OOM under flush stall. The pointer-saturation framing
        // (`> usize::MAX/2`) is vacuous on 64-bit (9.2 EiB ceiling), so
        // bound against a physical-RAM ceiling instead. 8 TiB picked to
        // reject the pathological 1 PiB while preserving headroom above
        // existing per-axis acceptance tests (max single buffer × default
        // count = 3 TiB).
        let memtable_peak =
            (self.write_buffer_size as u128).saturating_mul(self.max_write_buffer_number as u128);
        if memtable_peak > MAX_JOINT_MEMTABLE_BYTES as u128 {
            return Err(ForstError::invalid_argument(format!(
                "joint write_buffer_size × max_write_buffer_number = {} bytes exceeds {} bytes \
                 (peak memtable RAM commitment would OOM-abort under flush stall)",
                memtable_peak, MAX_JOINT_MEMTABLE_BYTES
            )));
        }
        // R-loop r19 Sec H#2: per-SST bloom-filter allocation joint cap.
        // Per-axis caps (r6 target_file_size_base ≤ 1 TiB, r9
        // bloom_bits_per_key ≤ 256) admit a 2 TiB per-SST bloom (16-byte
        // entry floor: 1 TiB / 16 × 256 / 8 = 2 TiB). A single SST
        // committing 2 TiB of bloom-filter pages exhausts host memory.
        // 256 GiB cap rejects the pathological 2 TiB while preserving
        // headroom above existing per-axis tests (max file × default
        // bloom = 80 GiB).
        const MIN_AVG_ENTRY_BYTES: usize = 16;
        let projected_bloom_bytes = self
            .target_file_size_base
            .saturating_div(MIN_AVG_ENTRY_BYTES)
            .saturating_mul(self.bloom_bits_per_key)
            .saturating_div(8);
        if projected_bloom_bytes > MAX_JOINT_BLOOM_BYTES {
            return Err(ForstError::invalid_argument(format!(
                "joint bloom_bits_per_key × (target_file_size_base / {}) / 8 = {} bytes \
                 exceeds {} bytes (per-SST bloom allocation would OOM-abort)",
                MIN_AVG_ENTRY_BYTES, projected_bloom_bytes, MAX_JOINT_BLOOM_BYTES
            )));
        }
        // R-loop r5 Sec H#1: cap level-base so the base axis cannot drive
        // saturation downstream the same way the multiplier axis did.
        if self.max_bytes_for_level_base > MAX_LEVEL_BASE {
            return Err(ForstError::invalid_argument(format!(
                "max_bytes_for_level_base must be ≤ {} bytes (1 PiB), got {}",
                MAX_LEVEL_BASE, self.max_bytes_for_level_base
            )));
        }
        // R-loop r5 Sec H#2: cap untrusted-input fields that flow into
        // allocator / thread-spawn paths.
        if self.write_buffer_size > MAX_WRITE_BUFFER_SIZE {
            return Err(ForstError::invalid_argument(format!(
                "write_buffer_size must be ≤ {} bytes (1 TiB), got {}",
                MAX_WRITE_BUFFER_SIZE, self.write_buffer_size
            )));
        }
        if self.block_cache_size > MAX_BLOCK_CACHE_SIZE {
            return Err(ForstError::invalid_argument(format!(
                "block_cache_size must be ≤ {} bytes (1 PiB), got {}",
                MAX_BLOCK_CACHE_SIZE, self.block_cache_size
            )));
        }
        if self.max_background_compactions > MAX_BACKGROUND_THREADS {
            return Err(ForstError::invalid_argument(format!(
                "max_background_compactions must be ≤ {}, got {}",
                MAX_BACKGROUND_THREADS, self.max_background_compactions
            )));
        }
        if self.max_background_flushes > MAX_BACKGROUND_THREADS {
            return Err(ForstError::invalid_argument(format!(
                "max_background_flushes must be ≤ {}, got {}",
                MAX_BACKGROUND_THREADS, self.max_background_flushes
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

    /// Sets the number of shards in the active memtable (E1).
    pub fn memtable_shards(mut self, n: usize) -> Self {
        self.inner.memtable_shards = n;
        self
    }

    /// Consumes the builder and returns the configured [`EngineOptions`]
    /// **without validation**.
    ///
    /// Prefer [`Self::try_build`] when accepting untrusted configuration —
    /// `build()` skips every DoS-bound check landed by R-loops r3–r9 (path
    /// length, multiplier saturation, write-buffer cap, block-cache cap,
    /// background-thread count, bloom_bits cap, block_size lower/upper,
    /// target_file_size cap, max_write_buffer_number, level_base cap, NaN /
    /// ±Inf multiplier rejection, embedded NUL in db_path, etc.).
    /// `build()` remains for trusted-call-site ergonomics and existing
    /// callers (R-loop r12 Errors H#1 mitigation).
    pub fn build(self) -> EngineOptions {
        self.inner
    }

    /// Consumes the builder and returns the configured [`EngineOptions`]
    /// **after running [`EngineOptions::validate`]**.
    ///
    /// This is the recommended entry point for any caller that may receive
    /// untrusted configuration (FFI / on-disk decode / RPC). All upper /
    /// lower bounds and content checks established by R-loops r3–r9 fire
    /// here.
    ///
    /// # Errors
    ///
    /// Returns [`ForstError::InvalidArgument`] for any validation failure;
    /// see [`EngineOptions::validate`] for the precise checks.
    pub fn try_build(self) -> ForstResult<EngineOptions> {
        let opts = self.inner;
        opts.validate()?;
        Ok(opts)
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

    /// Validates per-CF overrides against the same DoS-bound checks
    /// [`EngineOptions::validate`] applies (R-loop S2-r9 H#1).
    ///
    /// `effective_*` accessors silently return `Some(usize::MAX)` (or any
    /// caller-supplied value) without re-checking the per-axis floors and
    /// caps that S2-r5/r7/r8 added on the engine axis. Untrusted CfOptions
    /// from FFI / RPC / on-disk decode could therefore reproduce the exact
    /// inode-exhaustion / write-stall / compaction-storm DoS vectors that
    /// the engine-side caps closed. Call this from any code path that
    /// accepts CfOptions from a non-trusted source.
    ///
    /// # Errors
    ///
    /// Returns [`ForstError::InvalidArgument`] for any override that
    /// violates the same per-axis caps and joint memtable cap as
    /// [`EngineOptions::validate`].
    pub fn validate(&self, engine: &EngineOptions) -> ForstResult<()> {
        if let Some(write_buffer_size) = self.write_buffer_size {
            if write_buffer_size == 0 {
                return Err(ForstError::invalid_argument(
                    "CfOptions.write_buffer_size override must be > 0",
                ));
            }
            if write_buffer_size < MIN_WRITE_BUFFER_SIZE {
                return Err(ForstError::invalid_argument(format!(
                    "CfOptions.write_buffer_size override must be ≥ {} bytes (4 KiB), got {}",
                    MIN_WRITE_BUFFER_SIZE, write_buffer_size
                )));
            }
            if write_buffer_size > MAX_WRITE_BUFFER_SIZE {
                return Err(ForstError::invalid_argument(format!(
                    "CfOptions.write_buffer_size override must be ≤ {} bytes (1 TiB), got {}",
                    MAX_WRITE_BUFFER_SIZE, write_buffer_size
                )));
            }
        }
        if let Some(max_write_buffer_number) = self.max_write_buffer_number {
            if max_write_buffer_number == 0 {
                return Err(ForstError::invalid_argument(
                    "CfOptions.max_write_buffer_number override must be > 0",
                ));
            }
            if max_write_buffer_number > MAX_WRITE_BUFFER_NUMBER {
                return Err(ForstError::invalid_argument(format!(
                    "CfOptions.max_write_buffer_number override must be ≤ {}, got {}",
                    MAX_WRITE_BUFFER_NUMBER, max_write_buffer_number
                )));
            }
        }
        if let Some(target_file_size_base) = self.target_file_size_base {
            if target_file_size_base == 0 {
                return Err(ForstError::invalid_argument(
                    "CfOptions.target_file_size_base override must be > 0",
                ));
            }
            if target_file_size_base < MIN_TARGET_FILE_SIZE_BASE {
                return Err(ForstError::invalid_argument(format!(
                    "CfOptions.target_file_size_base override must be ≥ {} bytes (4 KiB), got {}",
                    MIN_TARGET_FILE_SIZE_BASE, target_file_size_base
                )));
            }
            if target_file_size_base > MAX_TARGET_FILE_SIZE_BASE {
                return Err(ForstError::invalid_argument(format!(
                    "CfOptions.target_file_size_base override must be ≤ {} bytes (1 TiB), got {}",
                    MAX_TARGET_FILE_SIZE_BASE, target_file_size_base
                )));
            }
        }
        // Joint memtable cap: re-run with effective values (override or
        // engine fallback) — same threat as r19 H#1 on the engine axis.
        let eff_buffer = self.effective_write_buffer_size(engine);
        let eff_count = self.effective_max_write_buffer_number(engine);
        let memtable_peak = (eff_buffer as u128).saturating_mul(eff_count as u128);
        if memtable_peak > MAX_JOINT_MEMTABLE_BYTES as u128 {
            return Err(ForstError::invalid_argument(format!(
                "CfOptions joint write_buffer_size × max_write_buffer_number = {} bytes exceeds {} bytes \
                 (peak per-CF memtable RAM commitment would OOM-abort)",
                memtable_peak, MAX_JOINT_MEMTABLE_BYTES
            )));
        }
        // R-loop S2-r10 Sec H#1: joint per-SST bloom-filter cap parallel
        // to r19 H#2 on the engine axis. CfOptions only carries the
        // target_file_size_base override (no per-CF bloom_bits_per_key),
        // so combine the effective file size with the engine-level
        // bloom_bits_per_key to project the per-SST bloom byte budget.
        const MIN_AVG_ENTRY_BYTES: usize = 16;
        let eff_file = self.effective_target_file_size_base(engine);
        let projected_bloom_bytes = eff_file
            .saturating_div(MIN_AVG_ENTRY_BYTES)
            .saturating_mul(engine.bloom_bits_per_key)
            .saturating_div(8);
        if projected_bloom_bytes > MAX_JOINT_BLOOM_BYTES {
            return Err(ForstError::invalid_argument(format!(
                "CfOptions joint bloom_bits_per_key × (target_file_size_base / {}) / 8 = {} bytes \
                 exceeds {} bytes (per-CF SST bloom allocation would OOM-abort)",
                MIN_AVG_ENTRY_BYTES, projected_bloom_bytes, MAX_JOINT_BLOOM_BYTES
            )));
        }
        Ok(())
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

    /// R-loop r5 Sec H#1: cap on `max_bytes_for_level_base` so an
    /// untrusted `usize::MAX` cannot drive the same saturation DoS via
    /// the base axis that the multiplier axis was capped against.
    #[test]
    fn test_validate_rejects_oversized_level_base() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(MAX_LEVEL_BASE + 1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(usize::MAX)
            .build();
        assert!(opts.validate().is_err());
        // The per-axis cap itself is accepted IF combined with a small
        // multiplier (post r18, the joint product check rejects boundary
        // base × default multiplier × 7 levels).
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(MAX_LEVEL_BASE)
            .max_bytes_for_level_multiplier(1.001)
            .num_levels(2)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r9 Sec H#1: bloom_bits_per_key capped at 256.
    #[test]
    fn test_validate_rejects_oversized_bloom_bits_per_key() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .bloom_bits_per_key(MAX_BLOOM_BITS_PER_KEY + 1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .bloom_bits_per_key(MAX_BLOOM_BITS_PER_KEY)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r12 Errors H#1: try_build runs validate; build does not.
    #[test]
    fn test_try_build_validates_runs_caps() {
        // build() accepts unvalidated huge values (caller-trusted path).
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(usize::MAX)
            .build();
        assert_eq!(opts.write_buffer_size, usize::MAX);
        // try_build() catches the same value.
        let res = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(usize::MAX)
            .try_build();
        assert!(res.is_err());
        // try_build with valid config returns Ok.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .try_build()
            .unwrap();
        assert_eq!(opts.db_path, "/tmp/db");
    }

    /// R-loop r9 Sec H#2: db_path length + NUL-byte rejection.
    #[test]
    fn test_validate_db_path_length_and_nul() {
        // Length cap.
        let long_path: String = "/".repeat(MAX_DB_PATH_LEN + 1);
        let opts = EngineOptions::builder().db_path(long_path).build();
        assert!(opts.validate().is_err());
        // Exactly at the cap is accepted.
        let path_at_cap: String = "/".repeat(MAX_DB_PATH_LEN);
        let opts = EngineOptions::builder().db_path(path_at_cap).build();
        assert!(opts.validate().is_ok());
        // Embedded NUL — malicious FFI/C-view TOCTOU.
        let opts = EngineOptions::builder()
            .db_path("/safe/path\0/../../etc/passwd")
            .build();
        assert!(opts.validate().is_err());
    }

    /// R-loop r7 Errors H_F1+H_F2+H_F3: symmetric lower-bound (zero)
    /// rejection for fields whose upper bound was capped in r5/r6.
    #[test]
    fn test_validate_rejects_zero_lower_bounds() {
        for builder_fn in [
            EngineOptions::builder()
                .db_path("/tmp/db")
                .max_bytes_for_level_base(0),
            EngineOptions::builder()
                .db_path("/tmp/db")
                .target_file_size_base(0),
            EngineOptions::builder()
                .db_path("/tmp/db")
                .max_background_compactions(0),
            EngineOptions::builder()
                .db_path("/tmp/db")
                .max_background_flushes(0),
            EngineOptions::builder()
                .db_path("/tmp/db")
                .max_write_buffer_number(0),
        ] {
            let opts = builder_fn.build();
            assert!(
                opts.validate().is_err(),
                "zero value should be rejected, got: {:?}",
                opts
            );
        }
    }

    /// R-loop r6 Sec H#1+H#3 / Errors H_F1: block_size both lower and
    /// upper-bounded.
    #[test]
    fn test_validate_block_size_bounds() {
        // Below MIN_BLOCK_SIZE (but > 0 to skip the older check).
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .block_size(MIN_BLOCK_SIZE - 1)
            .build();
        assert!(opts.validate().is_err());

        // Above MAX_BLOCK_SIZE.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .block_size(MAX_BLOCK_SIZE + 1)
            .build();
        assert!(opts.validate().is_err());

        // Both bounds inclusive.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .block_size(MIN_BLOCK_SIZE)
            .build();
        assert!(opts.validate().is_ok());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .block_size(MAX_BLOCK_SIZE)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r6 Errors H_F2: target_file_size_base capped.
    #[test]
    fn test_validate_rejects_oversized_target_file_size_base() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .target_file_size_base(MAX_TARGET_FILE_SIZE_BASE + 1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .target_file_size_base(MAX_TARGET_FILE_SIZE_BASE)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop S2-r8 Sec H#1: write_buffer_size must be ≥ 4 KiB to
    /// prevent per-record memtable flush → inode + write-stall DoS.
    #[test]
    fn test_validate_rejects_undersized_write_buffer_size() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MIN_WRITE_BUFFER_SIZE - 1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MIN_WRITE_BUFFER_SIZE)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop S2-r8 Sec H#2: max_bytes_for_level_base must be ≥ 1 MiB
    /// to prevent permanent compaction storm.
    #[test]
    fn test_validate_rejects_undersized_max_bytes_for_level_base() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(MIN_LEVEL_BASE - 1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(MIN_LEVEL_BASE)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop S2-r7 Sec H#1: target_file_size_base must be ≥ 4 KiB to
    /// prevent per-record SST rollover → inode/FD exhaustion DoS.
    /// Symmetric to r6 MIN_BLOCK_SIZE.
    #[test]
    fn test_validate_rejects_undersized_target_file_size_base() {
        // 1 byte → rejected (per-record rollover).
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .target_file_size_base(1)
            .build();
        assert!(opts.validate().is_err());
        // MIN - 1 → rejected.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .target_file_size_base(MIN_TARGET_FILE_SIZE_BASE - 1)
            .build();
        assert!(opts.validate().is_err());
        // Exactly MIN → accepted.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .target_file_size_base(MIN_TARGET_FILE_SIZE_BASE)
            .build();
        assert!(opts.validate().is_ok());
        // Default (64 MiB) → accepted.
        let opts = EngineOptions::builder().db_path("/tmp/db").build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r6 Sec H#2: max_write_buffer_number capped.
    #[test]
    fn test_validate_rejects_oversized_max_write_buffer_number() {
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_write_buffer_number(MAX_WRITE_BUFFER_NUMBER + 1)
            .build();
        assert!(opts.validate().is_err());
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_write_buffer_number(MAX_WRITE_BUFFER_NUMBER)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r5 Sec H#2: caps on resource-exhaustion-prone fields.
    #[test]
    fn test_validate_rejects_oversized_resource_fields() {
        // write_buffer_size
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MAX_WRITE_BUFFER_SIZE + 1)
            .build();
        assert!(opts.validate().is_err());

        // block_cache_size
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .block_cache_size(MAX_BLOCK_CACHE_SIZE + 1)
            .build();
        assert!(opts.validate().is_err());

        // max_background_compactions
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_background_compactions(MAX_BACKGROUND_THREADS + 1)
            .build();
        assert!(opts.validate().is_err());

        // max_background_flushes
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_background_flushes(MAX_BACKGROUND_THREADS + 1)
            .build();
        assert!(opts.validate().is_err());

        // Each cap itself is accepted.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MAX_WRITE_BUFFER_SIZE)
            .block_cache_size(MAX_BLOCK_CACHE_SIZE)
            .max_background_compactions(MAX_BACKGROUND_THREADS)
            .max_background_flushes(MAX_BACKGROUND_THREADS)
            .build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r19 Sec H#1: write_buffer_size × max_write_buffer_number
    /// joint product must be capped (peak memtable RAM commitment).
    #[test]
    fn test_validate_rejects_joint_memtable_oom() {
        // Both at max: 1 TiB × 1024 = 1 PiB > 8 TiB cap. Rejected.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MAX_WRITE_BUFFER_SIZE)
            .max_write_buffer_number(MAX_WRITE_BUFFER_NUMBER)
            .build();
        assert!(opts.validate().is_err());
        // Default (64 MiB × 3 = 192 MiB) accepted.
        let opts = EngineOptions::builder().db_path("/tmp/db").build();
        assert!(opts.validate().is_ok());
        // At cap (8 TiB exactly): 1 TiB × 8 = 8 TiB. Accepted.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MAX_WRITE_BUFFER_SIZE)
            .max_write_buffer_number(8)
            .build();
        assert!(opts.validate().is_ok());
        // 1 above cap: 1 TiB × 9 = 9 TiB. Rejected.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .write_buffer_size(MAX_WRITE_BUFFER_SIZE)
            .max_write_buffer_number(9)
            .build();
        assert!(opts.validate().is_err());
    }

    /// R-loop r19 Sec H#2: bloom × target_file_size / 16 / 8 joint
    /// product must be capped (per-SST bloom allocation OOM).
    #[test]
    fn test_validate_rejects_joint_bloom_oom() {
        // bloom=256 × file=1 TiB / 16 / 8 = 2 TiB > 256 GiB cap. Rejected.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .bloom_bits_per_key(MAX_BLOOM_BITS_PER_KEY)
            .target_file_size_base(MAX_TARGET_FILE_SIZE_BASE)
            .build();
        assert!(opts.validate().is_err());
        // Default (64 MiB / 16 × 10 / 8 = 5 MiB) accepted.
        let opts = EngineOptions::builder().db_path("/tmp/db").build();
        assert!(opts.validate().is_ok());
    }

    /// R-loop r18 Security H#1 + Correctness H_F1: joint product
    /// `base * multiplier^(num_levels-1)` saturates `usize` even with
    /// per-axis caps at boundary. Each axis individually accepted, but
    /// the cross-product must be rejected.
    #[test]
    fn test_validate_rejects_joint_product_saturation() {
        // All three axes at their respective caps individually pass —
        // but jointly cascade to `usize::MAX`, which is the DoS r3/r4/r5
        // targeted via per-axis caps but never closed jointly.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(MAX_LEVEL_BASE)
            .max_bytes_for_level_multiplier(MAX_LEVEL_MULTIPLIER)
            .num_levels(7)
            .build();
        assert!(
            opts.validate().is_err(),
            "joint maxima should be rejected (would saturate `usize` downstream)"
        );

        // A more modest example also rejects — base=MAX_LEVEL_BASE,
        // multiplier=8 (well below 1024 cap), num_levels=7:
        // 2^50 × 8^6 = 2^68 > 2^64.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_base(MAX_LEVEL_BASE)
            .max_bytes_for_level_multiplier(8.0)
            .num_levels(7)
            .build();
        assert!(
            opts.validate().is_err(),
            "base=1PiB, mul=8, levels=7 saturates usize"
        );

        // Default config (256MB base × 10× × 7 levels) is well within
        // bounds — this should still pass.
        let opts = EngineOptions::builder().db_path("/tmp/db").build();
        assert!(opts.validate().is_ok(), "default config should validate");
    }

    /// R-loop r4 H#1: regression — finite-but-astronomical multiplier (e.g.
    /// `f64::MAX`) was bypassing the r3 check. Now bounded above by
    /// `MAX_LEVEL_MULTIPLIER`.
    #[test]
    fn test_validate_rejects_multiplier_above_max() {
        for v in [
            MAX_LEVEL_MULTIPLIER + 1.0,
            1.0e10,
            1.0e100,
            1.0e308,
            f64::MAX,
        ] {
            let opts = EngineOptions::builder()
                .db_path("/tmp/db")
                .max_bytes_for_level_multiplier(v)
                .build();
            assert!(
                opts.validate().is_err(),
                "multiplier={} should be rejected (would saturate downstream level capacities to usize::MAX)",
                v
            );
        }
        // The per-axis cap itself is accepted IF combined with a small
        // base/levels (post r18, joint product check rejects
        // default-base × MAX_LEVEL_MULTIPLIER × 7 levels). Post S2-r8
        // the level_base must be ≥ MIN_LEVEL_BASE = 1 MiB.
        let opts = EngineOptions::builder()
            .db_path("/tmp/db")
            .max_bytes_for_level_multiplier(MAX_LEVEL_MULTIPLIER)
            .max_bytes_for_level_base(MIN_LEVEL_BASE)
            .num_levels(2)
            .build();
        assert!(
            opts.validate().is_ok(),
            "MAX_LEVEL_MULTIPLIER itself is accepted with small base+levels"
        );
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

    /// R-loop S2-r9 Correctness H#1: CfOptions::validate rejects per-CF
    /// overrides that bypass the per-axis floors/caps (parallel-symmetry
    /// continuation of S2-r7+r8 on the per-CF axis).
    #[test]
    fn test_cf_options_validate_rejects_under_floor_overrides() {
        let engine = EngineOptions::builder().db_path("/tmp/db").build();
        // write_buffer_size = 1 → rejected (under 4 KiB floor).
        let cf = CfOptions {
            write_buffer_size: Some(1),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_err());
        // target_file_size_base = 1 → rejected.
        let cf = CfOptions {
            target_file_size_base: Some(1),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_err());
        // max_write_buffer_number = 0 → rejected.
        let cf = CfOptions {
            max_write_buffer_number: Some(0),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_err());
        // All None (inherit engine) → OK.
        let cf = CfOptions::default();
        assert!(cf.validate(&engine).is_ok());
        // Override at floor → OK.
        let cf = CfOptions {
            write_buffer_size: Some(MIN_WRITE_BUFFER_SIZE),
            target_file_size_base: Some(MIN_TARGET_FILE_SIZE_BASE),
            max_write_buffer_number: Some(1),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_ok());
    }

    /// R-loop S2-r9 Correctness H#1: CfOptions::validate also rejects
    /// over-cap overrides + joint memtable RAM cap.
    #[test]
    fn test_cf_options_validate_rejects_over_cap_overrides() {
        let engine = EngineOptions::builder().db_path("/tmp/db").build();
        // write_buffer_size > MAX → rejected.
        let cf = CfOptions {
            write_buffer_size: Some(MAX_WRITE_BUFFER_SIZE + 1),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_err());
        // Joint memtable cap: 1 TiB × 1024 = 1 PiB > 8 TiB → rejected.
        let cf = CfOptions {
            write_buffer_size: Some(MAX_WRITE_BUFFER_SIZE),
            max_write_buffer_number: Some(MAX_WRITE_BUFFER_NUMBER),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_err());
    }

    /// R-loop S2-r10 Sec H#1: CfOptions::validate joint-bloom cap parallel
    /// to r19 H#2 on engine axis.
    #[test]
    fn test_cf_options_validate_rejects_joint_bloom_oom() {
        // Engine with max bloom_bits_per_key, plus per-CF target_file at
        // max → joint bloom 2 TiB > 256 GiB cap.
        let engine = EngineOptions::builder()
            .db_path("/tmp/db")
            .bloom_bits_per_key(MAX_BLOOM_BITS_PER_KEY)
            .build();
        let cf = CfOptions {
            target_file_size_base: Some(MAX_TARGET_FILE_SIZE_BASE),
            ..Default::default()
        };
        assert!(cf.validate(&engine).is_err());
    }
}

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

//! Compaction filter. See `2.6_compaction_design.md` §6 "TTL 与过滤".
//!
//! A compaction filter inspects each key-value pair during compaction and
//! can request its removal (for example, entries whose TTL has expired or
//! entries that fail application-level validation). The most common use
//! case is TTL enforcement: the engine discards entries older than a
//! configured retention period so the LSM-tree does not grow unboundedly
//! with stale data.
//!
//! This module provides:
//! - [`CompactionFilter`] — the trait all filters implement.
//! - [`CompactionDecision`] — the three possible verdicts.
//! - [`TtlCompactionFilter`] — a built-in filter that drops entries older
//!   than a configured duration.

use std::time::{SystemTime, UNIX_EPOCH};

use forst_rs_common::OpType;

/// Per-entry verdict returned by a [`CompactionFilter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionDecision {
    /// Keep the entry unchanged.
    Keep,
    /// Drop the entry entirely. The engine will not emit it to the
    /// output SST file.
    Discard,
    /// Replace the entry's value with the returned payload. The key,
    /// sequence, and op_type are preserved.
    Replace,
}

/// Inspects entries during compaction and decides whether they should be
/// kept, discarded, or rewritten.
///
/// Implementations must be `Send + Sync` so the engine can share a single
/// filter across compaction threads.
pub trait CompactionFilter: Send + Sync {
    /// Examine a single entry. The default implementation keeps everything.
    ///
    /// The `value_out` parameter is used when returning
    /// [`CompactionDecision::Replace`]: the filter writes the replacement
    /// bytes into `value_out` and the engine uses that as the new value.
    fn filter(
        &self,
        level: u32,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
        op_type: OpType,
        value_out: &mut Vec<u8>,
    ) -> CompactionDecision;

    /// Human-readable name for logging / validation.
    fn name(&self) -> &str;
}

/// TTL filter: drops entries whose encoded timestamp is older than the
/// configured retention window.
///
/// The value layout is:
///
/// ```text
/// [timestamp_seconds_since_unix_epoch: u64 LE | 8 bytes] | [payload: ...]
/// ```
///
/// Older stored formats (values shorter than 8 bytes) are treated as
/// "no timestamp present" and always kept — upgrading to TTL does not
/// retroactively drop legacy entries.
pub struct TtlCompactionFilter {
    ttl_seconds: u64,
    now_fn: fn() -> u64,
}

impl TtlCompactionFilter {
    /// Creates a filter with the given TTL (in seconds).
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            ttl_seconds,
            now_fn: unix_seconds_now,
        }
    }

    /// Creates a filter with a caller-supplied clock (used by tests to
    /// inject deterministic time).
    pub fn with_clock(ttl_seconds: u64, now_fn: fn() -> u64) -> Self {
        Self {
            ttl_seconds,
            now_fn,
        }
    }

    fn is_expired(&self, value: &[u8]) -> bool {
        if value.len() < 8 {
            // No timestamp prefix → we cannot determine age; keep it.
            return false;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&value[..8]);
        let ts = u64::from_le_bytes(buf);
        let now = (self.now_fn)();
        now.saturating_sub(ts) > self.ttl_seconds
    }
}

impl CompactionFilter for TtlCompactionFilter {
    fn filter(
        &self,
        _level: u32,
        _key: &[u8],
        value: Option<&[u8]>,
        _sequence: u64,
        op_type: OpType,
        _value_out: &mut Vec<u8>,
    ) -> CompactionDecision {
        match op_type {
            // Never drop tombstones — their presence shadows older versions.
            OpType::Delete | OpType::SingleDelete => CompactionDecision::Keep,
            OpType::Put | OpType::Merge => match value {
                Some(v) if self.is_expired(v) => CompactionDecision::Discard,
                _ => CompactionDecision::Keep,
            },
        }
    }

    fn name(&self) -> &str {
        "TtlCompactionFilter"
    }
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Encodes a timestamp-prefixed value of the form `[ts_u64_le | payload]`.
/// Useful helper for applications using [`TtlCompactionFilter`].
pub fn encode_ttl_value(timestamp_seconds: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + payload.len());
    buf.extend_from_slice(&timestamp_seconds.to_le_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Decodes the payload half of a timestamp-prefixed value, returning the
/// payload slice (or an empty slice when no timestamp is present).
pub fn decode_ttl_payload(value: &[u8]) -> &[u8] {
    if value.len() < 8 {
        value
    } else {
        &value[8..]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_now_1000() -> u64 {
        1000
    }

    #[test]
    fn test_ttl_filter_name() {
        let f = TtlCompactionFilter::new(60);
        assert_eq!(f.name(), "TtlCompactionFilter");
    }

    #[test]
    fn test_ttl_keep_recent_entry() {
        // Entry timestamped at t=900; now=1000; TTL=200; age=100 → KEEP.
        let f = TtlCompactionFilter::with_clock(200, fixed_now_1000);
        let value = encode_ttl_value(900, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&value), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    #[test]
    fn test_ttl_discard_expired_entry() {
        // Entry timestamped at t=500; now=1000; TTL=100; age=500 → DISCARD.
        let f = TtlCompactionFilter::with_clock(100, fixed_now_1000);
        let value = encode_ttl_value(500, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&value), 1, OpType::Put, &mut out),
            CompactionDecision::Discard
        );
    }

    #[test]
    fn test_ttl_exactly_at_ttl_kept() {
        // age == ttl: kept (boundary is saturating_sub > ttl_seconds).
        let f = TtlCompactionFilter::with_clock(100, fixed_now_1000);
        let value = encode_ttl_value(900, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&value), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    #[test]
    fn test_ttl_delete_always_kept() {
        let f = TtlCompactionFilter::with_clock(0, fixed_now_1000);
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", None, 1, OpType::Delete, &mut out),
            CompactionDecision::Keep
        );
    }

    #[test]
    fn test_ttl_single_delete_always_kept() {
        let f = TtlCompactionFilter::with_clock(0, fixed_now_1000);
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", None, 1, OpType::SingleDelete, &mut out),
            CompactionDecision::Keep
        );
    }

    #[test]
    fn test_ttl_value_without_timestamp_kept() {
        let f = TtlCompactionFilter::with_clock(1, fixed_now_1000);
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(b"short"), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    #[test]
    fn test_ttl_future_timestamp_kept() {
        // ts=2000, now=1000 → saturating_sub = 0 → kept.
        let f = TtlCompactionFilter::with_clock(100, fixed_now_1000);
        let value = encode_ttl_value(2000, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&value), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    #[test]
    fn test_encode_decode_ttl_roundtrip() {
        let encoded = encode_ttl_value(42, b"hello");
        assert_eq!(&encoded[0..8], &42u64.to_le_bytes());
        assert_eq!(decode_ttl_payload(&encoded), b"hello");
    }

    #[test]
    fn test_decode_ttl_short_returns_input() {
        assert_eq!(decode_ttl_payload(b"abc"), b"abc");
    }

    #[test]
    fn test_ttl_filter_merge_expired_discarded() {
        let f = TtlCompactionFilter::with_clock(50, fixed_now_1000);
        let expired = encode_ttl_value(500, b"m");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&expired), 1, OpType::Merge, &mut out),
            CompactionDecision::Discard
        );
    }

    #[test]
    fn test_filter_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TtlCompactionFilter>();
    }
}

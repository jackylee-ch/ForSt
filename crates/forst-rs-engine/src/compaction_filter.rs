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
//!   than a configured duration (seconds, ts at value prefix).
//! - [`FlinkTtlCompactionFilter`] — a Flink-shaped TTL filter that mirrors
//!   the ABI surface of `org.forstdb.FlinkCompactionFilter`: configurable
//!   state type (Disabled / Value / List), millisecond TTL, configurable
//!   timestamp byte offset inside the value, and an injectable wall-clock
//!   supplier (used by tests to drive deterministic expiry decisions).

use std::sync::Arc;
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

    /// Human-readable identity for the filter — used by the engine's
    /// cross-CF homogeneity check (see `DbImpl::check_cf_homogeneity_locked`).
    ///
    /// # Identity contract (R47-H1)
    ///
    /// The returned string MUST encode every semantically-relevant
    /// configuration field. Two filters that disagree on any field that
    /// changes their `filter()` decision MUST return distinct names —
    /// otherwise the homogeneity check will admit them as "the same
    /// filter" across CFs and silently produce wrong results during
    /// cross-CF L0 compaction. For example, `FlinkTtlCompactionFilter`
    /// encodes `ttl_ms`, `state_type`, and `timestamp_offset` into its
    /// name; two filters that differ only in `ttl_ms` are NOT homogeneous.
    ///
    /// `String` (rather than `&str`) lets implementations format
    /// per-instance config without leaking a static buffer.
    fn name(&self) -> String;
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

    fn name(&self) -> String {
        // TTL is the only semantically-relevant field — `now_fn` is a
        // test/clock hook that does not affect compaction decisions.
        format!("TtlCompactionFilter(ttl_seconds={})", self.ttl_seconds)
    }
}

fn unix_seconds_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn unix_millis_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// State-type tag understood by [`FlinkTtlCompactionFilter`].
///
/// Mirrors the ordinals of `org.apache.flink.contrib.streaming.state.ttl
/// .RocksDbTtlCompactionFilter.StateType` — kept identical so the JNI shim
/// can pass the raw `jint` ordinal through without translation.
///
/// - `Disabled` (0): the filter is a no-op (every entry is kept). The
///   engine still pays a method-dispatch cost; consumers that truly want
///   "no filter" should leave the CF's filter slot `None` instead.
/// - `Value` (1): the value layout is `[ts_u64_le | payload]` starting at
///   `timestamp_offset` bytes into the value. Used for Flink's `ValueState`,
///   `ReducingState`, `AggregatingState`, and `MapState` element values.
/// - `List` (2): the value is a length-prefixed sequence of TTL entries.
///   The current implementation treats `List` like `Value` for the purposes
///   of expiring the *whole* state — per-element pruning is a follow-up that
///   needs the `ListElementFilterFactory` JNI surface. See the doc-comment
///   on [`FlinkTtlCompactionFilter::filter`] for the conservative behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlStateType {
    /// No-op filter. Every entry is kept regardless of timestamp.
    Disabled = 0,
    /// Value state: timestamp lives at `value[timestamp_offset..+8]`.
    Value = 1,
    /// List state: same timestamp layout, conservative whole-state expiry.
    List = 2,
}

impl TtlStateType {
    /// Builds a [`TtlStateType`] from the `jint` ordinal the JNI shim
    /// hands us. Unknown ordinals fall back to [`Self::Disabled`] so an
    /// out-of-band Flink upgrade can never silently enable a state type
    /// the engine doesn't understand.
    pub fn from_ordinal(ordinal: i32) -> Self {
        match ordinal {
            1 => Self::Value,
            2 => Self::List,
            _ => Self::Disabled,
        }
    }

    /// Returns the JNI ordinal this state type was constructed from.
    pub fn ordinal(self) -> i32 {
        self as i32
    }
}

/// Wall-clock supplier used by [`FlinkTtlCompactionFilter`].
///
/// Returns the current Unix timestamp in **milliseconds**. The trait-object
/// shape lets tests inject a deterministic clock without relying on
/// `SystemTime::now()`.
pub type CurrentTimeSupplier = Arc<dyn Fn() -> u64 + Send + Sync>;
pub type ListElementOffsetSupplier = Arc<dyn Fn(&[u8], u64, u64) -> usize + Send + Sync>;

/// Flink-shaped TTL filter mirroring the surface that
/// `org.forstdb.FlinkCompactionFilter` exposes.
///
/// Compared to the simpler [`TtlCompactionFilter`]:
/// - Time unit is **milliseconds** (Flink's wire convention) rather than
///   seconds.
/// - The timestamp byte offset inside the value is **configurable** (Flink
///   maps put the timestamp after a 1-byte tag prefix, so `timestamp_offset
///   = 1` is the common case for `MapState`).
/// - The clock is an `Arc<dyn Fn>` so tests and embedders can inject
///   deterministic time (the JNI `TimeProvider` callback would wire here in
///   a future iteration).
///
/// # Decision matrix
///
/// | state_type | op_type           | last_access_ms + ttl_ms <= now | decision |
/// |------------|-------------------|----------------------|----------|
/// | Disabled   | any               | n/a                  | Keep     |
/// | Value/List | Delete/SingleDel  | n/a                  | Keep     |
/// | Value/List | Put/Merge         | yes                            | Discard  |
/// | Value/List | Put/Merge         | no                             | Keep     |
/// | Value/List | Put/Merge w/ short value (< offset+8) | n/a | Keep |
/// | Value/List | Put/Merge w/ future last-access ts | n/a | Keep |
///
/// `ttl_ms == 0` means "never expire" (mirrors the C++ ConfigHolder
/// semantics where a zero TTL is treated as "no enforcement").
pub struct FlinkTtlCompactionFilter {
    ttl_ms: u64,
    state_type: TtlStateType,
    timestamp_offset: usize,
    current_time_supplier: CurrentTimeSupplier,
    fixed_element_length: Option<usize>,
    list_element_offset_supplier: Option<ListElementOffsetSupplier>,
}

impl FlinkTtlCompactionFilter {
    /// Builds a filter with the system clock as the time source.
    pub fn new(ttl_ms: u64, state_type: TtlStateType, timestamp_offset: usize) -> Self {
        Self {
            ttl_ms,
            state_type,
            timestamp_offset,
            current_time_supplier: Arc::new(unix_millis_now),
            fixed_element_length: None,
            list_element_offset_supplier: None,
        }
    }

    /// Builds a filter with a caller-supplied clock (used by tests).
    pub fn with_supplier(
        ttl_ms: u64,
        state_type: TtlStateType,
        timestamp_offset: usize,
        current_time_supplier: CurrentTimeSupplier,
    ) -> Self {
        Self {
            ttl_ms,
            state_type,
            timestamp_offset,
            current_time_supplier,
            fixed_element_length: None,
            list_element_offset_supplier: None,
        }
    }

    pub fn with_list_filter(
        ttl_ms: u64,
        state_type: TtlStateType,
        timestamp_offset: usize,
        current_time_supplier: CurrentTimeSupplier,
        fixed_element_length: Option<usize>,
        list_element_offset_supplier: Option<ListElementOffsetSupplier>,
    ) -> Self {
        Self {
            ttl_ms,
            state_type,
            timestamp_offset,
            current_time_supplier,
            fixed_element_length,
            list_element_offset_supplier,
        }
    }

    /// Returns the configured TTL in milliseconds.
    pub fn ttl_ms(&self) -> u64 {
        self.ttl_ms
    }

    /// Returns the configured state-type tag.
    pub fn state_type(&self) -> TtlStateType {
        self.state_type
    }

    /// Returns the configured timestamp offset inside the value.
    pub fn timestamp_offset(&self) -> usize {
        self.timestamp_offset
    }

    /// Returns `true` iff the value at `value[timestamp_offset..+8]`
    /// decodes (big-endian) to Flink runtime's last-access timestamp
    /// and `last_access + ttl_ms <= now`. This mirrors
    /// `TtlUtils.expired(lastAccessTimestamp, ttl, currentTimestamp)`
    /// used by the community ForSt/RocksDB TTL serializers.
    ///
    /// Conservative on partial values: any value shorter than
    /// `timestamp_offset + 8` is treated as "no timestamp present" → kept.
    /// This protects against rolling upgrades where pre-TTL entries lack a
    /// timestamp prefix.
    fn is_expired(&self, value: &[u8]) -> bool {
        if self.ttl_ms == 0 {
            return false; // 0 = "never expire"
        }
        let end = self.timestamp_offset.saturating_add(8);
        if value.len() < end {
            return false;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&value[self.timestamp_offset..end]);
        let last_access_ms = u64::from_be_bytes(buf);
        let now = (self.current_time_supplier)();
        let expiration_ms = last_access_ms.saturating_add(self.ttl_ms);
        let expired = expiration_ms <= now;
        expired
    }

    fn list_unexpired_offset(&self, value: &[u8]) -> usize {
        if self.ttl_ms == 0 || value.is_empty() {
            return 0;
        }
        let now = (self.current_time_supplier)();
        if let Some(fixed_element_length) = self.fixed_element_length.filter(|len| *len > 0) {
            let mut offset = 0usize;
            while offset < value.len() {
                let ts_end = offset.saturating_add(self.timestamp_offset).saturating_add(8);
                if value.len() < ts_end {
                    break;
                }
                let mut buf = [0u8; 8];
                let ts_offset = offset + self.timestamp_offset;
                buf.copy_from_slice(&value[ts_offset..ts_offset + 8]);
                let last_access_ms = u64::from_be_bytes(buf);
                if last_access_ms.saturating_add(self.ttl_ms) > now {
                    break;
                }
                offset = offset.saturating_add(fixed_element_length).min(value.len());
            }
            return offset;
        }
        if let Some(ref supplier) = self.list_element_offset_supplier {
            return supplier(value, self.ttl_ms, now).min(value.len());
        }
        if self.is_expired(value) {
            value.len()
        } else {
            0
        }
    }

    fn filter_list_value(&self, value: &[u8], value_out: &mut Vec<u8>) -> CompactionDecision {
        let offset = self.list_unexpired_offset(value);
        if offset == 0 {
            CompactionDecision::Keep
        } else if offset >= value.len() {
            CompactionDecision::Discard
        } else {
            value_out.clear();
            value_out.extend_from_slice(&value[offset..]);
            CompactionDecision::Replace
        }
    }
}

impl CompactionFilter for FlinkTtlCompactionFilter {
    /// Implements the decision matrix in the struct-level doc-comment.
    ///
    /// `List` state currently uses the same expiry rule as `Value` —
    /// per-element pruning (the `ListElementFilterFactory` JNI surface)
    /// is a follow-up: it requires both the element-length parameter
    /// and a re-encoded value to emit via `CompactionDecision::Replace`.
    /// Until then the conservative behavior is "expire the entire list
    /// when the head timestamp is past TTL" — strictly safer than letting
    /// expired list state grow unbounded.
    fn filter(
        &self,
        _level: u32,
        _key: &[u8],
        value: Option<&[u8]>,
        _sequence: u64,
        op_type: OpType,
        value_out: &mut Vec<u8>,
    ) -> CompactionDecision {
        if self.state_type == TtlStateType::Disabled {
            return CompactionDecision::Keep;
        }
        match op_type {
            // Tombstones are ALWAYS kept — dropping them would resurrect
            // older Puts shadowed at lower levels.
            OpType::Delete | OpType::SingleDelete => CompactionDecision::Keep,
            OpType::Put | OpType::Merge => match (self.state_type, value) {
                (TtlStateType::List, Some(v)) => self.filter_list_value(v, value_out),
                (_, Some(v)) if self.is_expired(v) => CompactionDecision::Discard,
                _ => CompactionDecision::Keep,
            },
        }
    }

    fn name(&self) -> String {
        // R47-H1: encode every semantically-relevant config into the
        // identity. The homogeneity check at the engine compares filters
        // by-value via this string, so two FlinkTtlCompactionFilters
        // that differ in `ttl_ms`, `state_type`, or `timestamp_offset`
        // are distinct identities and must not be admitted as
        // "homogeneous" across CFs.
        let list_filter = match (self.fixed_element_length, self.list_element_offset_supplier.is_some()) {
            (Some(len), _) => format!("fixed:{len}"),
            (None, true) => "callback".to_string(),
            (None, false) => "none".to_string(),
        };
        format!(
            "FlinkTtlCompactionFilter(ttl_ms={},state_type={},timestamp_offset={},list_filter={})",
            self.ttl_ms,
            self.state_type.ordinal(),
            self.timestamp_offset,
            list_filter,
        )
    }
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
        // R47-H1: name encodes ttl_seconds so two filters with different
        // TTLs are NOT admitted as homogeneous.
        let f = TtlCompactionFilter::new(60);
        assert_eq!(f.name(), "TtlCompactionFilter(ttl_seconds=60)");
        let g = TtlCompactionFilter::new(120);
        assert_ne!(f.name(), g.name());
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

    // ---- FlinkTtlCompactionFilter ----

    /// Build a value of the form `[prefix | ts_ms_be | payload]` matching
    /// the layout that Flink's `MapState` writes. R81-H1: the wire format
    /// is big-endian (`DataOutput.writeLong`), NOT little-endian — prior
    /// tests used `to_le_bytes` which only "passed" because the production
    /// decoder was also `from_le_bytes` (a self-consistent bug). Fixing
    /// the decoder to `from_be_bytes` requires this helper to match.
    fn flink_value(prefix: &[u8], ts_ms: u64, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(prefix.len() + 8 + payload.len());
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(&ts_ms.to_be_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    /// Returns a `CurrentTimeSupplier` that always reports the given
    /// number of milliseconds. Used by deterministic tests.
    fn fixed_supplier(now_ms: u64) -> CurrentTimeSupplier {
        Arc::new(move || now_ms)
    }

    /// `Value` state: entry whose last-access timestamp plus TTL is past
    /// `now` must be discarded, matching Flink runtime `TtlUtils`.
    #[test]
    fn test_ttl_filter_value_expired() {
        // lastAccess=500, ttl=500, now=1100 -> 1000 <= 1100 -> discard.
        let f = FlinkTtlCompactionFilter::with_supplier(
            500,
            TtlStateType::Value,
            0,
            fixed_supplier(1100),
        );
        let v = flink_value(b"", 500, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Discard
        );
        // R47-H1: name encodes ttl_ms / state_type / timestamp_offset.
        assert_eq!(
            f.name(),
            "FlinkTtlCompactionFilter(ttl_ms=500,state_type=1,timestamp_offset=0,list_filter=none)"
        );
        // Two filters that differ in ttl_ms only must produce distinct names.
        let g = FlinkTtlCompactionFilter::with_supplier(
            1000,
            TtlStateType::Value,
            0,
            fixed_supplier(1100),
        );
        assert_ne!(f.name(), g.name());
    }

    /// `Value` state: entry whose last-access timestamp plus TTL is in
    /// the future is kept verbatim.
    #[test]
    fn test_ttl_filter_value_not_expired() {
        // lastAccess=2000, ttl=1000, now=1000 -> expiration is in the future.
        let f = FlinkTtlCompactionFilter::with_supplier(
            1000,
            TtlStateType::Value,
            0,
            fixed_supplier(1000),
        );
        let v = flink_value(b"", 2000, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    /// `Disabled` state never expires anything, even values whose age is
    /// arbitrarily large. Used by Flink to install a no-op filter on CFs
    /// that don't carry TTL state.
    #[test]
    fn test_ttl_filter_disabled_keeps() {
        // Even for a "very expired" entry, Disabled returns Keep.
        let f = FlinkTtlCompactionFilter::with_supplier(
            1,
            TtlStateType::Disabled,
            0,
            fixed_supplier(u64::MAX),
        );
        let v = flink_value(b"", 0, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    /// MapState layout has a 1-byte tag prefix before the timestamp.
    /// Verifies `timestamp_offset` is honored.
    #[test]
    fn test_ttl_filter_value_with_offset() {
        let f = FlinkTtlCompactionFilter::with_supplier(
            500,
            TtlStateType::Value,
            1,
            fixed_supplier(2000),
        );
        // lastAccess=1000 sitting after a 1-byte tag, ttl=500, now=2000 -> discard.
        let v = flink_value(b"\x01", 1000, b"value");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Discard
        );
    }

    #[test]
    fn test_ttl_filter_fixed_list_prunes_expired_prefix() {
        let f = FlinkTtlCompactionFilter::with_list_filter(
            100,
            TtlStateType::List,
            0,
            fixed_supplier(250),
            Some(10),
            None,
        );
        let mut v = Vec::new();
        v.extend_from_slice(&100u64.to_be_bytes());
        v.extend_from_slice(b"a,");
        v.extend_from_slice(&200u64.to_be_bytes());
        v.extend_from_slice(b"b,");
        v.extend_from_slice(&300u64.to_be_bytes());
        v.extend_from_slice(b"c,");

        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Replace
        );
        assert_eq!(out, v[10..]);
    }

    #[test]
    fn test_ttl_filter_fixed_list_discards_all_expired_elements() {
        let f = FlinkTtlCompactionFilter::with_list_filter(
            100,
            TtlStateType::List,
            0,
            fixed_supplier(500),
            Some(10),
            None,
        );
        let mut v = Vec::new();
        v.extend_from_slice(&100u64.to_be_bytes());
        v.extend_from_slice(b"a,");
        v.extend_from_slice(&200u64.to_be_bytes());
        v.extend_from_slice(b"b,");

        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Discard
        );
    }

    /// Tombstones are kept regardless of state-type / TTL configuration.
    #[test]
    fn test_ttl_filter_keeps_tombstones() {
        let f = FlinkTtlCompactionFilter::with_supplier(
            1,
            TtlStateType::Value,
            0,
            fixed_supplier(u64::MAX),
        );
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", None, 1, OpType::Delete, &mut out),
            CompactionDecision::Keep
        );
        assert_eq!(
            f.filter(0, b"k", None, 1, OpType::SingleDelete, &mut out),
            CompactionDecision::Keep
        );
    }

    /// Values shorter than `timestamp_offset + 8` are treated as "legacy
    /// (no timestamp prefix)" and kept — protects against rolling upgrades
    /// where existing on-disk entries pre-date the TTL feature.
    #[test]
    fn test_ttl_filter_short_value_kept() {
        let f = FlinkTtlCompactionFilter::with_supplier(
            1,
            TtlStateType::Value,
            0,
            fixed_supplier(u64::MAX),
        );
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(b"abc"), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    /// `ttl_ms = 0` means "never expire". Mirrors the C++ ConfigHolder
    /// semantics where a zero TTL disables enforcement (used by tests
    /// that exercise the wiring without actually expiring anything).
    #[test]
    fn test_ttl_filter_zero_ttl_keeps() {
        let f = FlinkTtlCompactionFilter::with_supplier(
            0,
            TtlStateType::Value,
            0,
            fixed_supplier(1000),
        );
        let v = flink_value(b"", 0, b"payload");
        let mut out = Vec::new();
        assert_eq!(
            f.filter(0, b"k", Some(&v), 1, OpType::Put, &mut out),
            CompactionDecision::Keep
        );
    }

    /// `TtlStateType::from_ordinal` round-trips known values and falls back
    /// to `Disabled` for unknown ordinals (defense against a future Flink
    /// release adding a new state type our build doesn't understand).
    #[test]
    fn test_ttl_state_type_from_ordinal() {
        assert_eq!(TtlStateType::from_ordinal(0), TtlStateType::Disabled);
        assert_eq!(TtlStateType::from_ordinal(1), TtlStateType::Value);
        assert_eq!(TtlStateType::from_ordinal(2), TtlStateType::List);
        // Out-of-range ordinals fall back to Disabled.
        assert_eq!(TtlStateType::from_ordinal(99), TtlStateType::Disabled);
        assert_eq!(TtlStateType::from_ordinal(-1), TtlStateType::Disabled);
        // Round-trip ordinal() against from_ordinal().
        assert_eq!(TtlStateType::Value.ordinal(), 1);
        assert_eq!(TtlStateType::List.ordinal(), 2);
    }

    /// `FlinkTtlCompactionFilter` must be `Send + Sync` because the engine
    /// shares one instance across compaction worker threads.
    #[test]
    fn test_flink_filter_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FlinkTtlCompactionFilter>();
    }
}

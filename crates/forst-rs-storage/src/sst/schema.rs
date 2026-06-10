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

//! SST schema definitions and format constants.
//!
//! The SST Arrow schema defines four columns:
//! - `key` (Binary, non-nullable) — the user key
//! - `value` (Binary, nullable) — the value (null for deletes)
//! - `sequence` (UInt64, non-nullable) — MVCC sequence number
//! - `op_type` (UInt8, non-nullable) — operation type discriminant

use arrow::datatypes::{DataType, Field, Schema};

/// Magic bytes identifying a ForSt-RS SST file: `b"FRST"`.
pub const SST_MAGIC: &[u8; 4] = b"FRST";

/// Current SST file format version.
///
/// Version history:
/// * `1` — initial layout (no per-CF identification).
/// * `2` — adds `cf_id: u32` at the end of the fixed-field area of
///   [`super::footer::FooterV1`] to enforce per-CF SST isolation (R49-H1).
///   v1 footers continue to decode; their `cf_id` defaults to
///   [`forst_rs_common::DEFAULT_CF_ID`].
/// * `3` — adds the prefix-bloom section pointer (`prefix_bloom_offset: u64`,
///   `prefix_bloom_size: u32`) after `cf_id`: a second Sbbf over the first
///   [`PREFIX_BLOOM_LEN`] bytes of each key (keys shorter than that do not
///   contribute — they can never match a probe prefix of that length), so
///   prefix scans skip SSTs containing no keys for the probe's prefix (the
///   q7/q9/q20 read-volume lever, 2026-06-10). v1/v2 footers decode with
///   0/0 = absent → readers skip pruning for those SSTs.
pub const SST_FORMAT_VERSION: u16 = 3;

/// Fixed prefix length (bytes) the v3 prefix bloom is built over. Probes with
/// a prefix shorter than this bypass the filter (conservative). 16 bytes
/// covers key-group + stateId + join key in the composite key layouts the
/// NEXMark joins probe by.
pub const PREFIX_BLOOM_LEN: usize = 16;

/// Block type discriminant for data blocks.
///
/// `0x01` — v1 Arrow-IPC `RecordBatch` payload (legacy; still read).
/// `0x02` — v2 KV payload (C / [`super::kv_block`]): prefix-compressed sorted KV
///   rows + restart points. Decode is a pointer-walk (no Arrow array build / no
///   FlatBuffers / no offset validation). The reader dispatches on this byte, so
///   v1 and v2 blocks coexist in the same DB with no migration.
pub const BLOCK_TYPE_DATA: u8 = 0x01;

/// Block type discriminant for v2 KV data blocks (see [`BLOCK_TYPE_DATA`]).
pub const BLOCK_TYPE_DATA_KV: u8 = 0x02;

/// Size in bytes of a block header (type + compression + reserved + sizes + checksum).
pub const BLOCK_HEADER_SIZE: usize = 16;

/// Size in bytes of the SST file header (magic + format_version + flags + reserved).
pub const FILE_HEADER_SIZE: usize = 16;

/// Returns the Arrow [`Schema`] used for SST data blocks.
///
/// Columns:
/// - `key`: `Binary` (non-nullable)
/// - `value`: `Binary` (nullable — null for tombstones)
/// - `sequence`: `UInt64` (non-nullable)
/// - `op_type`: `UInt8` (non-nullable)
pub fn sst_schema() -> Schema {
    Schema::new(vec![
        Field::new("key", DataType::Binary, false),
        Field::new("value", DataType::Binary, true),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("op_type", DataType::UInt8, false),
    ])
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_has_four_columns() {
        let schema = sst_schema();
        assert_eq!(schema.fields().len(), 4);
    }

    #[test]
    fn test_schema_column_names() {
        let schema = sst_schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["key", "value", "sequence", "op_type"]);
    }

    #[test]
    fn test_schema_column_types() {
        let schema = sst_schema();
        assert_eq!(*schema.field(0).data_type(), DataType::Binary);
        assert_eq!(*schema.field(1).data_type(), DataType::Binary);
        assert_eq!(*schema.field(2).data_type(), DataType::UInt64);
        assert_eq!(*schema.field(3).data_type(), DataType::UInt8);
    }

    #[test]
    fn test_schema_nullability() {
        let schema = sst_schema();
        assert!(!schema.field(0).is_nullable(), "key must be non-nullable");
        assert!(schema.field(1).is_nullable(), "value must be nullable");
        assert!(
            !schema.field(2).is_nullable(),
            "sequence must be non-nullable"
        );
        assert!(
            !schema.field(3).is_nullable(),
            "op_type must be non-nullable"
        );
    }

    #[test]
    fn test_magic_bytes() {
        assert_eq!(SST_MAGIC, b"FRST");
    }

    #[test]
    fn test_format_version() {
        // v3: footer carries cf_id (R49-H1) + the prefix-bloom pointer
        // (the q7/q9/q20 scan-pruning lever, 2026-06-10).
        assert_eq!(SST_FORMAT_VERSION, 3);
        assert_eq!(PREFIX_BLOOM_LEN, 16);
    }

    #[test]
    fn test_block_header_size() {
        assert_eq!(BLOCK_HEADER_SIZE, 16);
    }

    #[test]
    fn test_file_header_size() {
        assert_eq!(FILE_HEADER_SIZE, 16);
    }

    #[test]
    fn test_block_type_data() {
        assert_eq!(BLOCK_TYPE_DATA, 0x01);
    }
}

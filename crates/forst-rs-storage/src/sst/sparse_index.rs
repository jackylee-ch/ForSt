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

//! Sparse index and block statistics for SST Index Section.
//!
//! The Index Section follows all DataBlocks and precedes the Footer.
//! It contains two parallel arrays: one [`SparseIndexEntry`] per DataBlock
//! (for binary-search point lookup) and one [`BlockStats`] per DataBlock
//! (for range pruning during scans and compaction).

use forst_rs_common::{
    get_fixed32, get_fixed64, put_fixed32, put_fixed64, ForstError, ForstResult,
};

/// Sparse index entry for one DataBlock.
///
/// `last_key` is the largest key in the block. Binary search over a sorted
/// array of entries finds the first entry where `last_key >= target_key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseIndexEntry {
    /// The last (largest) key in this DataBlock.
    pub last_key: Vec<u8>,
    /// Byte offset of the DataBlock within the SST file.
    pub block_offset: u64,
    /// Size in bytes of the DataBlock (including the 16-byte BlockHeader).
    pub block_size: u32,
}

/// Per-DataBlock statistics used for range pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockStats {
    /// Smallest key in this DataBlock.
    pub min_key: Vec<u8>,
    /// Largest key in this DataBlock.
    pub max_key: Vec<u8>,
    /// Number of key-value entries in this DataBlock.
    pub entry_count: u32,
    /// Smallest sequence number in this DataBlock.
    pub min_sequence: u64,
    /// Largest sequence number in this DataBlock.
    pub max_sequence: u64,
}

/// Encodes sparse index entries and block stats into the Index Section binary format.
///
/// # Panics
/// Panics if `entries.len() != stats.len()`.
pub fn encode_index(entries: &[SparseIndexEntry], stats: &[BlockStats]) -> Vec<u8> {
    assert_eq!(
        entries.len(),
        stats.len(),
        "entries and stats must have the same length"
    );
    debug_assert!(entries.len() <= u32::MAX as usize, "too many index entries");
    let num_blocks = entries.len() as u32;
    let estimated_size = 4
        + entries
            .iter()
            .map(|e| 2 + e.last_key.len() + 8 + 4)
            .sum::<usize>()
        + stats
            .iter()
            .map(|s| 2 + s.min_key.len() + 2 + s.max_key.len() + 4 + 8 + 8)
            .sum::<usize>();
    let mut buf = Vec::with_capacity(estimated_size);

    // Header: num_blocks
    put_fixed32(&mut buf, num_blocks);

    // SparseIndex entries
    for entry in entries {
        debug_assert!(
            entry.last_key.len() <= u16::MAX as usize,
            "key too long for u16 length prefix"
        );
        let key_len = entry.last_key.len() as u16;
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(&entry.last_key);
        put_fixed64(&mut buf, entry.block_offset);
        put_fixed32(&mut buf, entry.block_size);
    }

    // BlockStats entries
    for stat in stats {
        debug_assert!(
            stat.min_key.len() <= u16::MAX as usize,
            "key too long for u16 length prefix"
        );
        let min_key_len = stat.min_key.len() as u16;
        debug_assert!(
            stat.max_key.len() <= u16::MAX as usize,
            "key too long for u16 length prefix"
        );
        let max_key_len = stat.max_key.len() as u16;
        buf.extend_from_slice(&min_key_len.to_le_bytes());
        buf.extend_from_slice(&stat.min_key);
        buf.extend_from_slice(&max_key_len.to_le_bytes());
        buf.extend_from_slice(&stat.max_key);
        put_fixed32(&mut buf, stat.entry_count);
        put_fixed64(&mut buf, stat.min_sequence);
        put_fixed64(&mut buf, stat.max_sequence);
    }

    buf
}

/// Decodes the Index Section binary format back into entries and stats.
pub fn decode_index(data: &[u8]) -> ForstResult<(Vec<SparseIndexEntry>, Vec<BlockStats>)> {
    if data.len() < 4 {
        return Err(ForstError::corruption(format!(
            "index section too short: expected at least 4 bytes, got {}",
            data.len()
        )));
    }

    let mut offset = 0;
    let (num_blocks, n) = get_fixed32(&data[offset..])?;
    offset += n;
    let num_blocks = num_blocks as usize;

    // Decode SparseIndex entries
    let mut entries = Vec::with_capacity(num_blocks);
    for _ in 0..num_blocks {
        if offset + 2 > data.len() {
            return Err(ForstError::corruption(
                "index section truncated at entry key_len",
            ));
        }
        let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + key_len > data.len() {
            return Err(ForstError::corruption(
                "index section truncated at entry key",
            ));
        }
        let last_key = data[offset..offset + key_len].to_vec();
        offset += key_len;
        let (block_offset, n) = get_fixed64(&data[offset..])?;
        offset += n;
        let (block_size, n) = get_fixed32(&data[offset..])?;
        offset += n;
        entries.push(SparseIndexEntry {
            last_key,
            block_offset,
            block_size,
        });
    }

    // Decode BlockStats entries
    let mut stats = Vec::with_capacity(num_blocks);
    for _ in 0..num_blocks {
        if offset + 2 > data.len() {
            return Err(ForstError::corruption(
                "index section truncated at stats min_key_len",
            ));
        }
        let min_key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + min_key_len > data.len() {
            return Err(ForstError::corruption(
                "index section truncated at stats min_key",
            ));
        }
        let min_key = data[offset..offset + min_key_len].to_vec();
        offset += min_key_len;

        if offset + 2 > data.len() {
            return Err(ForstError::corruption(
                "index section truncated at stats max_key_len",
            ));
        }
        let max_key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        offset += 2;
        if offset + max_key_len > data.len() {
            return Err(ForstError::corruption(
                "index section truncated at stats max_key",
            ));
        }
        let max_key = data[offset..offset + max_key_len].to_vec();
        offset += max_key_len;

        let (entry_count, n) = get_fixed32(&data[offset..])?;
        offset += n;
        let (min_sequence, n) = get_fixed64(&data[offset..])?;
        offset += n;
        let (max_sequence, n) = get_fixed64(&data[offset..])?;
        offset += n;

        stats.push(BlockStats {
            min_key,
            max_key,
            entry_count,
            min_sequence,
            max_sequence,
        });
    }

    if offset != data.len() {
        return Err(ForstError::corruption(format!(
            "index section has {} trailing bytes",
            data.len() - offset
        )));
    }

    Ok((entries, stats))
}

/// Binary-searches the sparse index for the block containing `target_key`.
///
/// Returns the index of the first entry where `last_key >= target_key`,
/// or `None` if `target_key` is greater than all last keys.
#[must_use]
pub fn search_index(entries: &[SparseIndexEntry], target_key: &[u8]) -> Option<usize> {
    if entries.is_empty() {
        return None;
    }
    let idx = entries.partition_point(|e| e.last_key.as_slice() < target_key);
    if idx < entries.len() {
        Some(idx)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entries() -> Vec<SparseIndexEntry> {
        vec![
            SparseIndexEntry {
                last_key: b"ccc".to_vec(),
                block_offset: 16,
                block_size: 1024,
            },
            SparseIndexEntry {
                last_key: b"fff".to_vec(),
                block_offset: 1040,
                block_size: 2048,
            },
            SparseIndexEntry {
                last_key: b"zzz".to_vec(),
                block_offset: 3088,
                block_size: 512,
            },
        ]
    }

    fn sample_stats() -> Vec<BlockStats> {
        vec![
            BlockStats {
                min_key: b"aaa".to_vec(),
                max_key: b"ccc".to_vec(),
                entry_count: 100,
                min_sequence: 1,
                max_sequence: 100,
            },
            BlockStats {
                min_key: b"ddd".to_vec(),
                max_key: b"fff".to_vec(),
                entry_count: 200,
                min_sequence: 101,
                max_sequence: 300,
            },
            BlockStats {
                min_key: b"ggg".to_vec(),
                max_key: b"zzz".to_vec(),
                entry_count: 50,
                min_sequence: 301,
                max_sequence: 350,
            },
        ]
    }

    #[test]
    fn test_encode_starts_with_num_blocks() {
        let entries = sample_entries();
        let stats = sample_stats();
        let encoded = encode_index(&entries, &stats);
        let (num_blocks, _) = get_fixed32(&encoded).unwrap();
        assert_eq!(num_blocks, 3);
    }

    #[test]
    fn test_roundtrip() {
        let entries = sample_entries();
        let stats = sample_stats();
        let encoded = encode_index(&entries, &stats);
        let (decoded_entries, decoded_stats) = decode_index(&encoded).unwrap();
        assert_eq!(decoded_entries, entries);
        assert_eq!(decoded_stats, stats);
    }

    #[test]
    fn test_roundtrip_empty() {
        let encoded = encode_index(&[], &[]);
        let (entries, stats) = decode_index(&encoded).unwrap();
        assert!(entries.is_empty());
        assert!(stats.is_empty());
    }

    #[test]
    fn test_roundtrip_single_block() {
        let entries = vec![SparseIndexEntry {
            last_key: b"only".to_vec(),
            block_offset: 16,
            block_size: 4096,
        }];
        let stats = vec![BlockStats {
            min_key: b"only".to_vec(),
            max_key: b"only".to_vec(),
            entry_count: 1,
            min_sequence: 42,
            max_sequence: 42,
        }];
        let encoded = encode_index(&entries, &stats);
        let (de, ds) = decode_index(&encoded).unwrap();
        assert_eq!(de, entries);
        assert_eq!(ds, stats);
    }

    #[test]
    fn test_roundtrip_large_keys() {
        let entries = vec![SparseIndexEntry {
            last_key: vec![0xFF; 1024],
            block_offset: 16,
            block_size: 65536,
        }];
        let stats = vec![BlockStats {
            min_key: vec![0x00; 512],
            max_key: vec![0xFF; 1024],
            entry_count: 5000,
            min_sequence: 1,
            max_sequence: 5000,
        }];
        let encoded = encode_index(&entries, &stats);
        let (de, ds) = decode_index(&encoded).unwrap();
        assert_eq!(de, entries);
        assert_eq!(ds, stats);
    }

    #[test]
    fn test_decode_truncated_data() {
        let result = decode_index(&[0u8; 2]);
        assert!(result.is_err());
    }

    #[test]
    fn test_search_finds_first_block() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"aaa"), Some(0));
        assert_eq!(search_index(&entries, b"bbb"), Some(0));
        assert_eq!(search_index(&entries, b"ccc"), Some(0));
    }

    #[test]
    fn test_search_finds_middle_block() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"ddd"), Some(1));
        assert_eq!(search_index(&entries, b"fff"), Some(1));
    }

    #[test]
    fn test_search_finds_last_block() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"ggg"), Some(2));
        assert_eq!(search_index(&entries, b"zzz"), Some(2));
    }

    #[test]
    fn test_search_beyond_last_key_returns_none() {
        let entries = sample_entries();
        assert_eq!(search_index(&entries, b"zzz\x00"), None);
    }

    #[test]
    fn test_search_empty_index() {
        assert_eq!(search_index(&[], b"anything"), None);
    }

    #[test]
    fn test_encode_panics_on_mismatched_lengths() {
        let result = std::panic::catch_unwind(|| {
            encode_index(&sample_entries(), &[]);
        });
        assert!(result.is_err());
    }
}

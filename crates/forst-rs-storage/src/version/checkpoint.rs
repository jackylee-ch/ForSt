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

//! Checkpoint blob serialization and deserialization.
//!
//! The checkpoint blob captures the full VersionSet state (all levels, SST file
//! metadata, next_file_number, last_sequence) in a compact binary format that
//! can be written to persistent storage and restored on startup.
//!
//! ## Binary Format
//!
//! ```text
//! +------------------------------------------+
//! | Header (16 bytes)                        |
//! |   magic: "FRCP" (4B)                     |
//! |   format_version: u16 (LE)               |
//! |   flags: u16 (LE, reserved)              |
//! |   blob_size: u64 (LE)                    |
//! +------------------------------------------+
//! | VersionSet Snapshot                      |
//! |   next_file_number: u64 (LE)             |
//! |   last_sequence: u64 (LE)                |
//! |   num_levels: u32 (LE)                   |
//! |   for each level:                        |
//! |     level_id: u32 (LE)                   |
//! |     num_files: u32 (LE)                  |
//! |     for each file:                       |
//! |       file_number: u64 (LE)              |
//! |       file_size: u64 (LE)                |
//! |       smallest_key_len: u32 (LE)         |
//! |       smallest_key: [u8]                 |
//! |       largest_key_len: u32 (LE)          |
//! |       largest_key: [u8]                  |
//! |       min_sequence: u64 (LE)             |
//! |       max_sequence: u64 (LE)             |
//! |       num_entries: u64 (LE)              |
//! +------------------------------------------+
//! | Footer (12 bytes)                        |
//! |   checksum: u32 (CRC32C of all above)    |
//! |   blob_length: u64 (LE, total incl footer)|
//! +------------------------------------------+
//! ```

use forst_rs_common::{
    crc32c, get_fixed32, get_fixed64, put_fixed32, put_fixed64, FileNumber, ForstError,
    ForstResult, SequenceNumber, MAX_LEVELS,
};

use super::{LevelMeta, SstFileMeta, Version, VersionSetImpl, VersionSetSnapshot};

/// Magic bytes for checkpoint blob header.
const CHECKPOINT_MAGIC: &[u8; 4] = b"FRCP";

/// Current format version.
const FORMAT_VERSION: u16 = 1;

/// Defense-in-depth cap on `num_files` per level decoded from a checkpoint
/// blob. Prevents OOM-DoS from a crafted blob claiming `num_files = u32::MAX`
/// driving `Vec::with_capacity` to allocate gigabytes (Sweep R4 H by Reviewers
/// 2 + 5). Set well above A1 §11's "≤ 100k active SSTs per TM" engineering
/// bar so legitimate state never trips this gate.
const MAX_FILES_PER_LEVEL_CHECKPOINT: u32 = 1_000_000;

/// Header size in bytes: magic(4) + version(2) + flags(2) + blob_size(8) = 16.
const HEADER_SIZE: usize = 16;

/// Footer size in bytes: checksum(4) + blob_length(8) = 12.
const FOOTER_SIZE: usize = 12;

/// Serialize a VersionSetSnapshot to a checkpoint blob.
pub fn serialize_to_blob(snapshot: &VersionSetSnapshot) -> ForstResult<Vec<u8>> {
    let mut buf = Vec::with_capacity(4096);

    // --- Header (placeholder for blob_size, filled later) ---
    buf.extend_from_slice(CHECKPOINT_MAGIC);
    buf.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    buf.extend_from_slice(&0u16.to_le_bytes()); // flags (reserved)
    buf.extend_from_slice(&0u64.to_le_bytes()); // blob_size placeholder

    // --- VersionSet snapshot data ---
    put_fixed64(&mut buf, snapshot.next_file_number);
    put_fixed64(&mut buf, snapshot.last_sequence);

    let version = &snapshot.version;
    put_fixed32(&mut buf, version.levels.len() as u32);

    for level in &version.levels {
        put_fixed32(&mut buf, level.level);
        put_fixed32(&mut buf, level.files.len() as u32);

        for file in &level.files {
            put_fixed64(&mut buf, file.file_number.value());
            put_fixed64(&mut buf, file.file_size);

            // smallest_key
            put_fixed32(&mut buf, file.smallest_key.len() as u32);
            buf.extend_from_slice(&file.smallest_key);

            // largest_key
            put_fixed32(&mut buf, file.largest_key.len() as u32);
            buf.extend_from_slice(&file.largest_key);

            put_fixed64(&mut buf, file.min_sequence.value());
            put_fixed64(&mut buf, file.max_sequence.value());
            put_fixed64(&mut buf, file.num_entries);
        }
    }

    // --- Footer ---
    // Compute total size first so we can fill in the header before checksumming.
    // total = current buf len + checksum(4) + blob_length(8)
    let total_size = buf.len() + FOOTER_SIZE;

    // Fill in the blob_size in the header (offset 8..16) BEFORE computing CRC
    let blob_size_bytes = (total_size as u64).to_le_bytes();
    buf[8..16].copy_from_slice(&blob_size_bytes);

    // CRC32C of everything so far (header with correct blob_size + data)
    let checksum = crc32c(&buf);
    put_fixed32(&mut buf, checksum);

    // blob_length (redundant with header, but allows trailer-based scanning)
    put_fixed64(&mut buf, total_size as u64);

    Ok(buf)
}

/// Deserialize a checkpoint blob back into a VersionSetSnapshot.
pub fn restore_from_blob(data: &[u8]) -> ForstResult<VersionSetSnapshot> {
    if data.len() < HEADER_SIZE + FOOTER_SIZE {
        return Err(ForstError::corruption("checkpoint blob too small"));
    }

    // --- Parse Header ---
    if &data[..4] != CHECKPOINT_MAGIC {
        return Err(ForstError::corruption("invalid checkpoint magic"));
    }
    let version = u16::from_le_bytes([data[4], data[5]]);
    if version != FORMAT_VERSION {
        return Err(ForstError::corruption(format!(
            "unsupported checkpoint format version: {}",
            version
        )));
    }
    // flags at [6..8] -- reserved, ignore
    let blob_size = u64::from_le_bytes(data[8..16].try_into().unwrap());
    if blob_size != data.len() as u64 {
        return Err(ForstError::corruption(format!(
            "blob size mismatch: header says {}, actual {}",
            blob_size,
            data.len()
        )));
    }

    // --- Verify Footer checksum ---
    let footer_start = data.len() - FOOTER_SIZE;
    let (stored_checksum, _) = get_fixed32(&data[footer_start..footer_start + 4])?;
    let computed_checksum = crc32c(&data[..footer_start]);
    if stored_checksum != computed_checksum {
        return Err(ForstError::corruption(format!(
            "checkpoint checksum mismatch: stored={:#010x}, computed={:#010x}",
            stored_checksum, computed_checksum
        )));
    }

    // --- Parse VersionSet data ---
    let mut pos = HEADER_SIZE;

    let (next_file_number, n) = get_fixed64(&data[pos..])?;
    pos += n;
    let (last_sequence, n) = get_fixed64(&data[pos..])?;
    pos += n;
    let (num_levels, n) = get_fixed32(&data[pos..])?;
    pos += n;

    // SECURITY: bound num_levels against MAX_LEVELS before allocation to
    // prevent OOM-DoS from a crafted checkpoint blob (Sweep R4 H).
    if num_levels as usize > MAX_LEVELS {
        return Err(ForstError::corruption(format!(
            "checkpoint num_levels {} exceeds MAX_LEVELS {}",
            num_levels, MAX_LEVELS
        )));
    }
    let mut levels = Vec::with_capacity(num_levels as usize);
    for _ in 0..num_levels {
        let (level_id, n) = get_fixed32(&data[pos..])?;
        pos += n;
        let (num_files, n) = get_fixed32(&data[pos..])?;
        pos += n;

        // SECURITY: bound num_files against MAX_FILES_PER_LEVEL_CHECKPOINT
        // before allocation; prevents OOM-DoS from a crafted level claiming
        // ~4 GiB worth of files (Sweep R4 H).
        if num_files > MAX_FILES_PER_LEVEL_CHECKPOINT {
            return Err(ForstError::corruption(format!(
                "checkpoint num_files {} on level {} exceeds cap {}",
                num_files, level_id, MAX_FILES_PER_LEVEL_CHECKPOINT
            )));
        }
        let mut files = Vec::with_capacity(num_files as usize);
        for _ in 0..num_files {
            let (file_number, n) = get_fixed64(&data[pos..])?;
            pos += n;
            let (file_size, n) = get_fixed64(&data[pos..])?;
            pos += n;

            let (sk_len, n) = get_fixed32(&data[pos..])?;
            pos += n;
            if pos + sk_len as usize > footer_start {
                return Err(ForstError::corruption("smallest_key extends past data"));
            }
            let smallest_key = data[pos..pos + sk_len as usize].to_vec();
            pos += sk_len as usize;

            let (lk_len, n) = get_fixed32(&data[pos..])?;
            pos += n;
            if pos + lk_len as usize > footer_start {
                return Err(ForstError::corruption("largest_key extends past data"));
            }
            let largest_key = data[pos..pos + lk_len as usize].to_vec();
            pos += lk_len as usize;

            let (min_sequence, n) = get_fixed64(&data[pos..])?;
            pos += n;
            let (max_sequence, n) = get_fixed64(&data[pos..])?;
            pos += n;
            let (num_entries, n) = get_fixed64(&data[pos..])?;
            pos += n;

            files.push(SstFileMeta {
                file_number: FileNumber(file_number),
                file_size,
                smallest_key,
                largest_key,
                min_sequence: SequenceNumber(min_sequence),
                max_sequence: SequenceNumber(max_sequence),
                num_entries,
            });
        }

        levels.push(LevelMeta {
            level: level_id,
            files,
        });
    }

    // R31-M2: after parsing N levels × M files, `pos` must land exactly on
    // the footer start. Any gap means the encoder wrote extra padding (or a
    // mismatched length field elsewhere bumped pos past where we expect) —
    // both are corruption indicators that earlier length-checked reads can
    // miss when the over-read still fits inside footer_start.
    if pos != footer_start {
        return Err(ForstError::corruption(format!(
            "checkpoint meta-blob trailing-byte mismatch: parsed up to {}, footer starts at {}",
            pos, footer_start
        )));
    }

    Ok(VersionSetSnapshot {
        version: std::sync::Arc::new(Version { levels }),
        next_file_number,
        last_sequence,
    })
}

/// Convenience: restore a full VersionSetImpl from a checkpoint blob.
pub fn restore_version_set(data: &[u8]) -> ForstResult<VersionSetImpl> {
    let snapshot = restore_from_blob(data)?;
    let version = (*snapshot.version).clone();
    Ok(VersionSetImpl::from_restored(
        version,
        snapshot.next_file_number,
        snapshot.last_sequence,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version::VersionEdit;
    use forst_rs_common::MAX_LEVELS;
    use std::sync::Arc;

    fn make_file(num: u64, smallest: &[u8], largest: &[u8]) -> SstFileMeta {
        SstFileMeta {
            file_number: FileNumber(num),
            file_size: 4096,
            smallest_key: smallest.to_vec(),
            largest_key: largest.to_vec(),
            min_sequence: SequenceNumber(1),
            max_sequence: SequenceNumber(100),
            num_entries: 50,
        }
    }

    fn make_snapshot(files: Vec<(u32, SstFileMeta)>) -> VersionSetSnapshot {
        let mut version = Version::new();
        for (level, file) in files {
            version.levels[level as usize].files.push(file);
        }
        VersionSetSnapshot {
            version: Arc::new(version),
            next_file_number: 42,
            last_sequence: 1000,
        }
    }

    #[test]
    fn test_serialize_empty_snapshot() {
        let snap = VersionSetSnapshot {
            version: Arc::new(Version::new()),
            next_file_number: 1,
            last_sequence: 0,
        };
        let blob = serialize_to_blob(&snap).unwrap();
        assert!(blob.len() >= HEADER_SIZE + FOOTER_SIZE);
        assert_eq!(&blob[..4], CHECKPOINT_MAGIC);
    }

    #[test]
    fn test_roundtrip_empty() {
        let snap = VersionSetSnapshot {
            version: Arc::new(Version::new()),
            next_file_number: 1,
            last_sequence: 0,
        };
        let blob = serialize_to_blob(&snap).unwrap();
        let restored = restore_from_blob(&blob).unwrap();
        assert_eq!(restored.next_file_number, 1);
        assert_eq!(restored.last_sequence, 0);
        assert_eq!(restored.version.levels.len(), MAX_LEVELS);
    }

    #[test]
    fn test_roundtrip_with_files() {
        let snap = make_snapshot(vec![
            (0, make_file(1, b"a", b"d")),
            (0, make_file(2, b"e", b"h")),
            (1, make_file(3, b"a", b"z")),
        ]);
        let blob = serialize_to_blob(&snap).unwrap();
        let restored = restore_from_blob(&blob).unwrap();

        assert_eq!(restored.next_file_number, 42);
        assert_eq!(restored.last_sequence, 1000);
        assert_eq!(restored.version.levels[0].files.len(), 2);
        assert_eq!(restored.version.levels[1].files.len(), 1);

        // Verify file metadata roundtrips
        let f0 = &restored.version.levels[0].files[0];
        assert_eq!(f0.file_number, FileNumber(1));
        assert_eq!(f0.smallest_key, b"a");
        assert_eq!(f0.largest_key, b"d");
        assert_eq!(f0.file_size, 4096);
        assert_eq!(f0.min_sequence, SequenceNumber(1));
        assert_eq!(f0.max_sequence, SequenceNumber(100));
        assert_eq!(f0.num_entries, 50);
    }

    #[test]
    fn test_roundtrip_binary_keys() {
        let snap = make_snapshot(vec![(0, make_file(1, &[0x00, 0x01, 0xFF], &[0xFE, 0xFF]))]);
        let blob = serialize_to_blob(&snap).unwrap();
        let restored = restore_from_blob(&blob).unwrap();
        let f = &restored.version.levels[0].files[0];
        assert_eq!(f.smallest_key, vec![0x00, 0x01, 0xFF]);
        assert_eq!(f.largest_key, vec![0xFE, 0xFF]);
    }

    #[test]
    fn test_roundtrip_empty_keys() {
        let snap = make_snapshot(vec![(0, make_file(1, b"", b""))]);
        let blob = serialize_to_blob(&snap).unwrap();
        let restored = restore_from_blob(&blob).unwrap();
        let f = &restored.version.levels[0].files[0];
        assert!(f.smallest_key.is_empty());
        assert!(f.largest_key.is_empty());
    }

    #[test]
    fn test_restore_version_set_convenience() {
        let snap = make_snapshot(vec![(0, make_file(1, b"a", b"z"))]);
        let blob = serialize_to_blob(&snap).unwrap();
        let vs = restore_version_set(&blob).unwrap();

        assert_eq!(vs.next_file_number(), 42);
        assert_eq!(vs.last_sequence(), 1000);
        assert_eq!(vs.current().levels[0].files.len(), 1);
    }

    /// Regression test for Sweep R4 H (Reviewers 2 + 5): a crafted blob
    /// claiming `num_levels > MAX_LEVELS` must be rejected BEFORE the
    /// `Vec::with_capacity(num_levels as usize)` allocation, to prevent
    /// OOM-DoS on attacker-controlled checkpoint input.
    #[test]
    fn test_restore_rejects_oversized_num_levels() {
        // Build a valid empty blob, then mutate the num_levels field
        // (4 bytes at offset HEADER_SIZE + 8 + 8 = 32) to a huge value.
        let snap = make_snapshot(vec![]);
        let mut blob = serialize_to_blob(&snap).unwrap();
        // Header is 16 bytes; then next_file_number (8) + last_sequence (8)
        // = 32 bytes used; num_levels is the next u32 (LE).
        let num_levels_offset = 16 + 8 + 8;
        blob[num_levels_offset..num_levels_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        // Re-checksum so the blob passes the integrity check.
        let footer_start = blob.len() - FOOTER_SIZE;
        let new_crc = crc32c(&blob[..footer_start]);
        blob[footer_start..footer_start + 4].copy_from_slice(&new_crc.to_le_bytes());

        let err = match restore_from_blob(&blob) {
            Ok(_) => panic!("must reject oversized num_levels"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("num_levels") && msg.contains("exceeds MAX_LEVELS"),
            "expected num_levels cap error; got: {}",
            msg
        );
    }

    /// Regression test for Sweep R4 H (Reviewers 2 + 5): a crafted blob
    /// claiming `num_files > MAX_FILES_PER_LEVEL_CHECKPOINT` on any level
    /// must be rejected BEFORE the per-level `Vec::with_capacity(num_files
    /// as usize)` allocation.
    #[test]
    fn test_restore_rejects_oversized_num_files() {
        // Build a valid blob with one level + zero files, then mutate
        // num_files on that level to u32::MAX.
        let snap = make_snapshot(vec![(0, make_file(1, b"a", b"z"))]);
        let mut blob = serialize_to_blob(&snap).unwrap();

        // Layout: header 16 + next_file_number 8 + last_sequence 8 +
        //         num_levels 4 + level_id 4 + num_files 4 = 44.
        // Mutate the num_files field at offset 40.
        let num_files_offset = 16 + 8 + 8 + 4 + 4;
        blob[num_files_offset..num_files_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        let footer_start = blob.len() - FOOTER_SIZE;
        let new_crc = crc32c(&blob[..footer_start]);
        blob[footer_start..footer_start + 4].copy_from_slice(&new_crc.to_le_bytes());

        let err = match restore_from_blob(&blob) {
            Ok(_) => panic!("must reject oversized num_files"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("num_files") && msg.contains("exceeds cap"),
            "expected num_files cap error; got: {}",
            msg
        );
    }

    #[test]
    fn test_corrupted_magic_fails() {
        let snap = make_snapshot(vec![]);
        let mut blob = serialize_to_blob(&snap).unwrap();
        blob[0] = b'X'; // corrupt magic
        assert!(restore_from_blob(&blob).is_err());
    }

    #[test]
    fn test_corrupted_checksum_fails() {
        let snap = make_snapshot(vec![(0, make_file(1, b"a", b"z"))]);
        let mut blob = serialize_to_blob(&snap).unwrap();
        // Corrupt a data byte (after header, before footer)
        if blob.len() > HEADER_SIZE + 2 {
            blob[HEADER_SIZE + 1] ^= 0xFF;
        }
        assert!(restore_from_blob(&blob).is_err());
    }

    #[test]
    fn test_truncated_blob_fails() {
        let snap = make_snapshot(vec![(0, make_file(1, b"a", b"z"))]);
        let blob = serialize_to_blob(&snap).unwrap();
        // Truncate to just the header
        let truncated = &blob[..HEADER_SIZE];
        assert!(restore_from_blob(truncated).is_err());
    }

    #[test]
    fn test_blob_size_mismatch_fails() {
        let snap = make_snapshot(vec![]);
        let mut blob = serialize_to_blob(&snap).unwrap();
        // Corrupt blob_size in header
        blob[8..16].copy_from_slice(&999u64.to_le_bytes());
        assert!(restore_from_blob(&blob).is_err());
    }

    #[test]
    fn test_unsupported_version_fails() {
        let snap = make_snapshot(vec![]);
        let mut blob = serialize_to_blob(&snap).unwrap();
        // Set format version to 99
        blob[4..6].copy_from_slice(&99u16.to_le_bytes());
        assert!(restore_from_blob(&blob).is_err());
    }

    #[test]
    fn test_many_files_roundtrip() {
        let files: Vec<(u32, SstFileMeta)> = (0..100)
            .map(|i| {
                let level = (i % MAX_LEVELS as u64) as u32;
                let key = format!("key_{:06}", i);
                (level, make_file(i + 1, key.as_bytes(), key.as_bytes()))
            })
            .collect();
        let snap = make_snapshot(files);
        let blob = serialize_to_blob(&snap).unwrap();
        let restored = restore_from_blob(&blob).unwrap();

        // Count total files
        let total: usize = restored.version.levels.iter().map(|l| l.files.len()).sum();
        assert_eq!(total, 100);
    }

    #[test]
    fn test_full_version_set_serialize_restore_cycle() {
        // Create a VersionSet, apply multiple edits, then serialize and restore
        let vs = VersionSetImpl::new();

        // Flush 1
        vs.apply(&VersionEdit {
            new_files: vec![(0, make_file(1, b"a", b"d"))],
            next_file_number: Some(FileNumber(2)),
            last_sequence: Some(SequenceNumber(100)),
            ..Default::default()
        })
        .unwrap();

        // Flush 2
        vs.apply(&VersionEdit {
            new_files: vec![(0, make_file(2, b"e", b"h"))],
            next_file_number: Some(FileNumber(3)),
            last_sequence: Some(SequenceNumber(200)),
            ..Default::default()
        })
        .unwrap();

        // Compaction: L0 -> L1
        vs.apply(&VersionEdit {
            deleted_files: vec![(0, FileNumber(1)), (0, FileNumber(2))],
            new_files: vec![(1, make_file(3, b"a", b"h"))],
            next_file_number: Some(FileNumber(4)),
            ..Default::default()
        })
        .unwrap();

        // Snapshot -> serialize -> restore
        let snap = vs.snapshot();
        let blob = serialize_to_blob(&snap).unwrap();
        let restored_vs = restore_version_set(&blob).unwrap();

        // Verify restored state matches
        assert_eq!(restored_vs.next_file_number(), vs.next_file_number());
        assert_eq!(restored_vs.last_sequence(), vs.last_sequence());
        let original = vs.current();
        let restored = restored_vs.current();
        assert_eq!(
            original.levels[0].files.len(),
            restored.levels[0].files.len()
        );
        assert_eq!(
            original.levels[1].files.len(),
            restored.levels[1].files.len()
        );
    }
}

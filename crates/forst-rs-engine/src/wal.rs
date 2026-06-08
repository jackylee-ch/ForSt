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

//! FRS-WAL (2026-06-06): write-ahead log — **Phase 1: writer + reader foundation**.
//!
//! ## Why (the q4-beat-RocksDB lever)
//! Measured fair A/B (`docs/superpowers/specs/2026-06-06-q4-beat-rocksdb-gap-root-cause-final.md`):
//! q4 local forst-rs 322s vs RocksDB 283s. The 322s is a hard LSM tradeoff floor:
//!   * `noflush=false` keeps state COMPACT (fast scans) but forces a memtable
//!     flush+compaction at every checkpoint (CPU churn on the saturated host).
//!   * `noflush=true` skips the forced flush (cheap checkpoint) but keeps the
//!     memtable RESIDENT + UNCOMPACTED (44 GB; bloated scans).
//! Neither gives BOTH compact state AND a cheap checkpoint. RocksDB gets both via a
//! **write-ahead log**: background WBM flushes keep the memtable compact while a
//! checkpoint merely fsyncs the WAL (no forced flush). This module is the first
//! phase of giving forst-rs that same capability — and it adds crash durability
//! the engine currently lacks.
//!
//! ## Phase plan
//! 1. **(this module)** WAL record format + `WalWriter` (group-commit append+fsync)
//!    + `WalReader` (recovery scan with torn-tail tolerance). Isolated; touches no
//!    existing path.
//! 2. Wire the write path to append each mutation to the WAL before/with the memtable
//!    insert (behind a flag).
//! 3. Checkpoint: fsync the WAL + reference live SSTs, dropping the forced flush
//!    (the `noflush=false` compact-state path WITHOUT its checkpoint-flush cost).
//! 4. Recovery: open the SST set, then replay WAL records with `sequence` greater
//!    than the highest flushed seq into fresh memtables.
//! 5. GC WAL segments once their data is durably in SSTs (seq below the flush floor).
//!
//! ## On-disk record format (little-endian)
//! ```text
//! [payload_len: u32][crc32c(payload): u32][payload]
//!   payload = [cf_id: u32][sequence: u64][op_type: u8]
//!             [key_len: u32][key bytes]
//!             [val_present: u8][val_len: u32 (iff present)][val bytes (iff present)]
//! ```
//! A torn final record (partial header/payload) or a CRC mismatch terminates the
//! recovery scan — every record BEFORE it is durable and replayable, matching the
//! standard WAL crash-recovery contract (the last group-commit may be incomplete).

use forst_rs_common::{ForstError, ForstResult};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;

/// One logical mutation as stored in the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Column-family id the mutation targets.
    pub cf_id: u32,
    /// Engine-assigned global sequence number (MVCC ordering + flush floor).
    pub sequence: u64,
    /// Operation type byte (`OpType` as u8: Put/Delete/SingleDelete/Merge).
    pub op_type: u8,
    /// User key bytes.
    pub key: Vec<u8>,
    /// Value bytes; `None` for a tombstone (Delete/SingleDelete).
    pub value: Option<Vec<u8>>,
}

impl WalRecord {
    /// Serializes the payload (everything the CRC covers) into `out`.
    fn encode_payload(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.cf_id.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.push(self.op_type);
        out.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.key);
        match &self.value {
            Some(v) => {
                out.push(1u8);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v);
            }
            None => out.push(0u8),
        }
    }

    /// Decodes a payload produced by [`Self::encode_payload`]. Returns
    /// `Err` on any structural inconsistency (treated as a torn record by
    /// the reader).
    fn decode_payload(buf: &[u8]) -> ForstResult<WalRecord> {
        let mut p = 0usize;
        let take = |p: &mut usize, n: usize| -> ForstResult<()> {
            if *p + n > buf.len() {
                return Err(ForstError::corruption("WAL payload truncated"));
            }
            *p += n;
            Ok(())
        };
        let start = p;
        take(&mut p, 4)?;
        let cf_id = u32::from_le_bytes(buf[start..start + 4].try_into().unwrap());
        let s = p;
        take(&mut p, 8)?;
        let sequence = u64::from_le_bytes(buf[s..s + 8].try_into().unwrap());
        let op_pos = p;
        take(&mut p, 1)?;
        let op_type = buf[op_pos];
        let kl_pos = p;
        take(&mut p, 4)?;
        let key_len = u32::from_le_bytes(buf[kl_pos..kl_pos + 4].try_into().unwrap()) as usize;
        let key_pos = p;
        take(&mut p, key_len)?;
        let key = buf[key_pos..key_pos + key_len].to_vec();
        let vp_pos = p;
        take(&mut p, 1)?;
        let value = match buf[vp_pos] {
            0 => None,
            1 => {
                let vl_pos = p;
                take(&mut p, 4)?;
                let val_len =
                    u32::from_le_bytes(buf[vl_pos..vl_pos + 4].try_into().unwrap()) as usize;
                let v_pos = p;
                take(&mut p, val_len)?;
                Some(buf[v_pos..v_pos + val_len].to_vec())
            }
            other => {
                return Err(ForstError::corruption(format!(
                    "WAL val_present byte invalid: {other}"
                )))
            }
        };
        if p != buf.len() {
            return Err(ForstError::corruption("WAL payload has trailing bytes"));
        }
        Ok(WalRecord {
            cf_id,
            sequence,
            op_type,
            key,
            value,
        })
    }
}

/// Appends WAL records to a single log segment file, with group-commit:
/// callers [`append`](Self::append) many records, then call
/// [`sync`](Self::sync) ONCE to flush + fsync the batch as a unit. This
/// amortizes the fsync that gives durability — the same mechanism RocksDB
/// uses so a checkpoint need not force a memtable flush.
pub struct WalWriter {
    inner: BufWriter<File>,
    /// Bytes appended since construction (the segment's logical size, used by
    /// Phase 5 rotation/GC to bound segment size).
    bytes_written: u64,
    /// Records appended since the last successful `sync` (group-commit batch).
    pending: u64,
}

impl WalWriter {
    /// Opens `path` for appending, creating it if absent. Existing content is
    /// preserved (segments are append-only; recovery reads the whole file).
    pub fn open(path: &Path) -> ForstResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| {
                ForstError::Io(std::io::Error::other(format!("WAL open {path:?}: {e}")))
            })?;
        let bytes_written = file
            .metadata()
            .map(|m| m.len())
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("WAL stat: {e}"))))?;
        Ok(WalWriter {
            inner: BufWriter::new(file),
            bytes_written,
            pending: 0,
        })
    }

    /// Buffers one record for the current group-commit batch. NOT durable until
    /// [`sync`](Self::sync). Returns the number of bytes the framed record adds.
    pub fn append(&mut self, rec: &WalRecord) -> ForstResult<usize> {
        let mut payload =
            Vec::with_capacity(32 + rec.key.len() + rec.value.as_ref().map_or(0, |v| v.len()));
        rec.encode_payload(&mut payload);
        let crc = crc32c::crc32c(&payload);
        let frame_len = 8 + payload.len();
        // Frame: [payload_len][crc][payload]
        self.inner
            .write_all(&(payload.len() as u32).to_le_bytes())
            .and_then(|_| self.inner.write_all(&crc.to_le_bytes()))
            .and_then(|_| self.inner.write_all(&payload))
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("WAL append: {e}"))))?;
        self.bytes_written += frame_len as u64;
        self.pending += 1;
        Ok(frame_len)
    }

    /// Flushes the buffered batch to the OS and fsyncs it to stable storage.
    /// After this returns Ok, every record appended since the last `sync` is
    /// durable (survives a crash). This is the group-commit point.
    pub fn sync(&mut self) -> ForstResult<()> {
        self.inner
            .flush()
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("WAL flush: {e}"))))?;
        self.inner
            .get_ref()
            .sync_data()
            .map_err(|e| ForstError::Io(std::io::Error::other(format!("WAL fsync: {e}"))))?;
        self.pending = 0;
        Ok(())
    }

    /// Logical size of this segment in bytes (for Phase 5 rotation).
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    /// Records buffered but not yet `sync`ed (un-durable tail).
    pub fn pending_records(&self) -> u64 {
        self.pending
    }
}

/// Outcome of a recovery scan over a WAL segment.
#[derive(Debug)]
pub struct WalScan {
    /// Every record that was framed intact AND passed its CRC, in file order.
    pub records: Vec<WalRecord>,
    /// `true` if the file ended exactly on a record boundary with no torn/corrupt
    /// tail; `false` if the scan stopped early at a partial or corrupt final
    /// record (expected after a crash mid-group-commit — the prefix is still valid).
    pub clean_eof: bool,
}

/// Reads all intact records from a WAL segment, tolerating a torn final record.
///
/// Stops (and sets `clean_eof = false`) at the first frame whose header is
/// incomplete, whose declared length runs past EOF, or whose CRC does not match
/// — every record BEFORE that point is returned and is safe to replay.
pub fn read_segment(path: &Path) -> ForstResult<WalScan> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WalScan {
                records: Vec::new(),
                clean_eof: true,
            })
        }
        Err(e) => {
            return Err(ForstError::Io(std::io::Error::other(format!(
                "WAL open: {e}"
            ))))
        }
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|e| ForstError::Io(std::io::Error::other(format!("WAL read: {e}"))))?;

    let mut records = Vec::new();
    let mut pos = 0usize;
    let clean_eof = loop {
        if pos == bytes.len() {
            break true; // ended exactly on a boundary
        }
        // Need the 8-byte frame header.
        if pos + 8 > bytes.len() {
            break false; // torn header
        }
        let payload_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[pos + 4..pos + 8].try_into().unwrap());
        let payload_start = pos + 8;
        if payload_start + payload_len > bytes.len() {
            break false; // torn payload
        }
        let payload = &bytes[payload_start..payload_start + payload_len];
        if crc32c::crc32c(payload) != crc {
            break false; // corrupt record — stop (tail is untrustworthy)
        }
        match WalRecord::decode_payload(payload) {
            Ok(rec) => records.push(rec),
            Err(_) => break false, // structurally bad despite CRC (paranoia) — stop
        }
        pos = payload_start + payload_len;
    };

    Ok(WalScan { records, clean_eof })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn rec(cf: u32, seq: u64, op: u8, k: &[u8], v: Option<&[u8]>) -> WalRecord {
        WalRecord {
            cf_id: cf,
            sequence: seq,
            op_type: op,
            key: k.to_vec(),
            value: v.map(|x| x.to_vec()),
        }
    }

    #[test]
    fn append_sync_read_round_trips_all_records() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("0.wal");
        let recs = vec![
            rec(0, 1, 0, b"alpha", Some(b"one")),
            rec(2, 2, 3, b"beta", Some(b"merge-op")),
            rec(0, 3, 1, b"gamma", None), // tombstone
            rec(1, 4, 0, b"", Some(b"")), // empty key + empty value (both valid)
        ];
        {
            let mut w = WalWriter::open(&path).unwrap();
            for r in &recs {
                w.append(r).unwrap();
            }
            assert_eq!(w.pending_records(), 4);
            w.sync().unwrap();
            assert_eq!(w.pending_records(), 0);
        }
        let scan = read_segment(&path).unwrap();
        assert!(scan.clean_eof, "intact file must report clean EOF");
        assert_eq!(scan.records, recs);
    }

    #[test]
    fn tombstone_distinguished_from_empty_value() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.wal");
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&rec(0, 1, 1, b"k", None)).unwrap(); // Delete (None)
        w.append(&rec(0, 2, 0, b"k", Some(b""))).unwrap(); // Put empty (Some(""))
        w.sync().unwrap();
        let scan = read_segment(&path).unwrap();
        assert_eq!(scan.records[0].value, None);
        assert_eq!(scan.records[1].value, Some(Vec::new()));
    }

    #[test]
    fn torn_tail_returns_valid_prefix_not_error() {
        // Simulates a crash mid-group-commit: the last record is half-written.
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn.wal");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&rec(0, 1, 0, b"durable-a", Some(b"v1"))).unwrap();
            w.append(&rec(0, 2, 0, b"durable-b", Some(b"v2"))).unwrap();
            w.sync().unwrap();
        }
        // Truncate the file by 3 bytes to corrupt the final record's tail.
        let full = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 3).unwrap();
        drop(f);

        let scan = read_segment(&path).unwrap();
        assert!(!scan.clean_eof, "torn tail must report unclean EOF");
        assert_eq!(
            scan.records.len(),
            1,
            "only the fully-durable record survives"
        );
        assert_eq!(scan.records[0].key, b"durable-a");
    }

    #[test]
    fn crc_mismatch_stops_scan_at_corrupt_record() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("crc.wal");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&rec(0, 1, 0, b"good", Some(b"v"))).unwrap();
            w.append(&rec(0, 2, 0, b"bad", Some(b"v"))).unwrap();
            w.sync().unwrap();
        }
        // Flip a byte inside the SECOND record's payload (past the first frame).
        let mut bytes = std::fs::read(&path).unwrap();
        let first_payload_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let second_payload_start = 8 + first_payload_len + 8;
        bytes[second_payload_start] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let scan = read_segment(&path).unwrap();
        assert!(!scan.clean_eof);
        assert_eq!(scan.records.len(), 1);
        assert_eq!(scan.records[0].key, b"good");
    }

    #[test]
    fn empty_or_missing_segment_is_clean_and_empty() {
        let dir = tempdir().unwrap();
        let missing = dir.path().join("nope.wal");
        let scan = read_segment(&missing).unwrap();
        assert!(scan.clean_eof);
        assert!(scan.records.is_empty());

        let empty = dir.path().join("empty.wal");
        WalWriter::open(&empty).unwrap().sync().unwrap();
        let scan2 = read_segment(&empty).unwrap();
        assert!(scan2.clean_eof);
        assert!(scan2.records.is_empty());
    }

    #[test]
    fn reopen_appends_after_existing_records() {
        // Phase 2 relies on reopening a segment and continuing to append.
        let dir = tempdir().unwrap();
        let path = dir.path().join("re.wal");
        {
            let mut w = WalWriter::open(&path).unwrap();
            w.append(&rec(0, 1, 0, b"first", Some(b"1"))).unwrap();
            w.sync().unwrap();
        }
        {
            let mut w = WalWriter::open(&path).unwrap();
            assert!(w.bytes_written() > 0, "reopen sees existing segment size");
            w.append(&rec(0, 2, 0, b"second", Some(b"2"))).unwrap();
            w.sync().unwrap();
        }
        let scan = read_segment(&path).unwrap();
        assert_eq!(scan.records.len(), 2);
        assert_eq!(scan.records[0].key, b"first");
        assert_eq!(scan.records[1].key, b"second");
    }

    #[test]
    fn large_values_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("big.wal");
        let big = vec![0xABu8; 200_000];
        let mut w = WalWriter::open(&path).unwrap();
        w.append(&rec(7, 99, 0, b"bigkey", Some(&big))).unwrap();
        w.sync().unwrap();
        let scan = read_segment(&path).unwrap();
        assert_eq!(scan.records.len(), 1);
        assert_eq!(scan.records[0].value.as_ref().unwrap().len(), 200_000);
        assert_eq!(scan.records[0].value, Some(big));
    }
}

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

//! C (2026-06-04): v2 KV data-block codec — prefix-compressed sorted KV rows +
//! restart points, as an alternative to the v1 Arrow-IPC `RecordBatch` block.
//!
//! Motivation (heavy-join read path): the v1 block decodes via Arrow
//! `StreamDecoder` (FlatBuffers schema parse + build every column array +
//! `validate_offsets_full`) — ~13% of the per-block read-path CPU — even though
//! every consumer (point `get`, scan `for_each_row`) immediately reads the data
//! back out as rows. A KV block is decoded by a pointer-walk: no Arrow arrays,
//! no FlatBuffers, no offset validation. This module is the encode/iterate/seek
//! half; the reader dispatches v1-vs-v2 on the block-header `block_type` byte.
//!
//! ## Payload layout (the bytes that are compressed + checksummed, after the
//! standard 16-byte [`BlockHeader`]; identical header machinery as v1):
//!
//! ```text
//! [entries ...]
//! [restart_offset : fixed32 LE] × num_restarts   // byte offset (from payload[0])
//! [num_restarts   : fixed32 LE]                   // ALWAYS the last 4 bytes
//! ```
//!
//! Each entry, in `(key ASC, sequence DESC)` order (the writer's on-disk order):
//!
//! ```text
//! [shared      : varint32]   // bytes of key shared with the previous entry
//! [non_shared  : varint32]   // bytes of key that follow
//! [value_tag   : varint64]   // 0 = no value (tombstone); else value_len + 1
//! [sequence    : fixed64 LE]
//! [op_type     : u8]
//! [non_shared key bytes]
//! [value bytes]              // value_tag-1 bytes, only if value_tag > 0
//! ```
//!
//! Every `KV_RESTART_INTERVAL`-th entry is a *restart*: `shared = 0` (full key),
//! and its payload offset is recorded in the restart array. Binary search over
//! the restart full-keys + a ≤K linear scan gives intra-block seek without
//! decoding the whole block.

use arrow::array::{Array, BinaryArray, RecordBatch, UInt64Array, UInt8Array};
use forst_rs_common::{
    crc32c, get_fixed32, get_fixed64, get_varint32, get_varint64, mask_crc, put_fixed32,
    put_fixed64, put_varint32, put_varint64, CompressionType, ForstError, ForstResult, OpType,
};

use super::block_header::BlockHeader;
use super::compression::{compress, decompress};
use super::reader::RowView;
use super::schema::{BLOCK_HEADER_SIZE, BLOCK_TYPE_DATA_KV};

/// Number of entries per restart interval (full key stored every K entries).
pub const KV_RESTART_INTERVAL: usize = 16;

/// C (2026-06-04): whether the SST WRITE path should emit v2 KV data blocks
/// instead of v1 Arrow-IPC blocks. Default = `false` (v1 — the safe default
/// until v2 is proven). `FRS_SST_KV_BLOCK_FORMAT=1` flips writers (flush +
/// compaction) to v2; the reader auto-detects per block, so v1 and v2 SSTs
/// coexist with no migration. Cached once (the flag never changes at runtime).
pub fn sst_write_kv_format() -> bool {
    use std::sync::OnceLock;
    static KV: OnceLock<bool> = OnceLock::new();
    // FLIPPED TO DEFAULT-ON 2026-06-04: the #31 scan-heavy v1-vs-v2 gate came back
    // 5/5 neutral-or-better (q5/q8 finish faster, q7 +23%, q11 v2-finishes-v1-
    // doesn't, q15 ≈) + q4 exercised v2 throughout + correctness proven both modes
    // (dual-version byte-identical, engine ground-truth, full suites v2-forced).
    // `FRS_SST_KV_BLOCK_FORMAT=0` is the instant opt-out back to v1 Arrow blocks.
    // (Coverage caveat: gated on the scan/iter-heavy RISK queries + q4, not the
    // full q0–q22; light queries are source-bound + block-format-agnostic.)
    // PRODUCTION-RELYING PREREQUISITE (not yet done): a full q0–q22 v1-vs-v2
    // correctness diff (task #34). Until then, env=0 above is the interim revert.
    // Ops/release note: docs/superpowers/specs/2026-06-04-KV-default-revert-and-prerequisite.md
    *KV.get_or_init(|| {
        !matches!(
            std::env::var("FRS_SST_KV_BLOCK_FORMAT").ok().as_deref(),
            Some("0") | Some("false") | Some("FALSE")
        )
    })
}

/// Length of the longest common byte prefix of `a` and `b`.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let max = a.len().min(b.len());
    let mut i = 0;
    while i < max && a[i] == b[i] {
        i += 1;
    }
    i
}

fn col_corruption(col: usize, ty: &str) -> ForstError {
    ForstError::corruption(format!("SST KV block column {col} not {ty}"))
}

/// Encodes an SST-schema [`RecordBatch`] (`key, value, sequence, op_type`) into
/// a v2 KV data block: a 16-byte [`BlockHeader`] (`block_type =
/// BLOCK_TYPE_DATA_KV`) followed by the compressed KV payload.
///
/// Rows are emitted in their existing batch order, which the SST writer
/// guarantees is `(key ASC, sequence DESC)`.
pub fn encode_kv_data_block(
    batch: &RecordBatch,
    compression: CompressionType,
) -> ForstResult<Vec<u8>> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| col_corruption(0, "BinaryArray"))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| col_corruption(1, "BinaryArray"))?;
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| col_corruption(2, "UInt64Array"))?;
    let op_types = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| col_corruption(3, "UInt8Array"))?;

    let n = batch.num_rows();
    let mut payload: Vec<u8> = Vec::new();
    let mut restarts: Vec<u32> = Vec::new();
    let mut prev_key: &[u8] = &[];

    for i in 0..n {
        let key = keys.value(i);
        let restart = i % KV_RESTART_INTERVAL == 0;
        let shared = if restart {
            0
        } else {
            common_prefix_len(prev_key, key)
        };
        let non_shared = key.len() - shared;

        if restart {
            restarts.push(payload.len() as u32);
        }

        put_varint32(&mut payload, shared as u32);
        put_varint32(&mut payload, non_shared as u32);
        // value_tag: 0 = tombstone/no-value; else value_len + 1 (so an empty
        // present value Some(&[]) encodes as 1, distinct from None).
        let value_tag: u64 = if values.is_null(i) {
            0
        } else {
            values.value(i).len() as u64 + 1
        };
        put_varint64(&mut payload, value_tag);
        put_fixed64(&mut payload, sequences.value(i));
        payload.push(op_types.value(i));
        payload.extend_from_slice(&key[shared..]);
        if value_tag != 0 {
            payload.extend_from_slice(values.value(i));
        }

        prev_key = key;
    }

    // Trailer: restart offset array, then the count (always the last 4 bytes).
    for off in &restarts {
        put_fixed32(&mut payload, *off);
    }
    put_fixed32(&mut payload, restarts.len() as u32);

    let uncompressed_size = payload.len() as u32;
    let compressed = compress(&payload, compression)?;
    let checksum = mask_crc(crc32c(&compressed));
    let header = BlockHeader {
        block_type: BLOCK_TYPE_DATA_KV,
        compression,
        uncompressed_size,
        compressed_size: compressed.len() as u32,
        checksum,
    };
    let mut result = header.encode();
    result.extend_from_slice(&compressed);
    Ok(result)
}

/// A decoded v2 KV block: owns the decompressed payload bytes and supports
/// zero-copy-ish row iteration (values borrow the payload; keys are
/// reconstructed into a reused scratch buffer) and binary-search seek.
#[derive(Clone)]
pub struct KvBlock {
    /// Decompressed payload (entries region + restart array + count).
    payload: Vec<u8>,
    /// Byte offset where the restart array begins (== end of the entries region).
    restarts_start: usize,
    /// Number of restart points.
    num_restarts: usize,
}

impl KvBlock {
    /// Decodes a full v2 KV block (`header + compressed payload`) into a
    /// [`KvBlock`]. Verifies the crc32c iff `verify_checksum` (gated identically
    /// to the v1 path via `FRS_SST_SKIP_READ_CHECKSUM`).
    pub fn decode(block: &[u8], verify_checksum: bool) -> ForstResult<KvBlock> {
        if block.len() < BLOCK_HEADER_SIZE {
            return Err(ForstError::corruption(format!(
                "KV data block too short for header: expected at least {} bytes, got {}",
                BLOCK_HEADER_SIZE,
                block.len()
            )));
        }
        let header = BlockHeader::decode(block)?;
        if header.block_type != BLOCK_TYPE_DATA_KV {
            return Err(ForstError::corruption(format!(
                "KV data block has wrong block_type 0x{:02X} (expected 0x{:02X})",
                header.block_type, BLOCK_TYPE_DATA_KV
            )));
        }

        let payload_start = BLOCK_HEADER_SIZE;
        let payload_end = payload_start + header.compressed_size as usize;
        if block.len() < payload_end {
            return Err(ForstError::corruption(format!(
                "KV data block truncated: header says {} compressed bytes, only {} available",
                header.compressed_size,
                block.len() - BLOCK_HEADER_SIZE
            )));
        }
        let compressed = &block[payload_start..payload_end];

        if verify_checksum {
            let actual = mask_crc(crc32c(compressed));
            if actual != header.checksum {
                return Err(ForstError::corruption(format!(
                    "KV data block checksum mismatch: expected 0x{:08X}, got 0x{:08X}",
                    header.checksum, actual
                )));
            }
        }

        let payload = decompress(
            compressed,
            header.compression,
            header.uncompressed_size as usize,
        )?;

        // Parse the trailer: last 4 bytes = num_restarts; preceding
        // num_restarts*4 bytes = restart offsets.
        if payload.len() < 4 {
            return Err(ForstError::corruption(
                "KV data block payload too short for restart count",
            ));
        }
        let (num_restarts, _) = get_fixed32(&payload[payload.len() - 4..])?;
        let num_restarts = num_restarts as usize;
        let restarts_bytes = num_restarts
            .checked_mul(4)
            .and_then(|b| b.checked_add(4))
            .ok_or_else(|| ForstError::corruption("KV data block restart array overflow"))?;
        if payload.len() < restarts_bytes {
            return Err(ForstError::corruption(format!(
                "KV data block payload {} too short for {} restarts",
                payload.len(),
                num_restarts
            )));
        }
        let restarts_start = payload.len() - restarts_bytes;

        Ok(KvBlock {
            payload,
            restarts_start,
            num_restarts,
        })
    }

    /// Byte offset of restart `i` (into the payload). Panics-free; returns a
    /// corruption error if `i` is out of range or the stored offset is invalid.
    fn restart_offset(&self, i: usize) -> ForstResult<usize> {
        if i >= self.num_restarts {
            return Err(ForstError::corruption("KV restart index out of range"));
        }
        let at = self.restarts_start + i * 4;
        let (off, _) = get_fixed32(&self.payload[at..])?;
        let off = off as usize;
        if off > self.restarts_start {
            return Err(ForstError::corruption("KV restart offset past entries region"));
        }
        Ok(off)
    }

    /// Invokes `cb` once per row, in on-disk order, with a [`RowView`] whose
    /// `key` borrows a reused scratch buffer and whose `value` borrows the
    /// payload. Both are valid only for the duration of the `cb` call.
    pub fn for_each_row<F>(&self, mut cb: F) -> ForstResult<()>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        let mut pos = 0usize;
        let mut key_buf: Vec<u8> = Vec::new();
        while pos < self.restarts_start {
            pos = self.read_row_at(pos, &mut key_buf, &mut cb)?;
        }
        Ok(())
    }

    /// Invokes `cb` for every row whose key is `>= target`, in on-disk order.
    /// Binary-searches the restart points, then linearly scans from the
    /// enclosing restart (reconstructing — but not yielding — the prefix keys
    /// before `target`). This is the seek primitive for point `get` (caller
    /// stops at the first key `> target`) and range scans (lower bound).
    pub fn for_each_row_from<F>(&self, target: &[u8], mut cb: F) -> ForstResult<()>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        let mut pos = self.seek_restart(target)?;
        let mut key_buf: Vec<u8> = Vec::new();
        let mut started = false;
        while pos < self.restarts_start {
            let e = self.parse_entry(pos)?;
            if e.shared > key_buf.len() {
                return Err(ForstError::corruption(
                    "KV entry shared-prefix len exceeds previous key",
                ));
            }
            key_buf.truncate(e.shared);
            key_buf.extend_from_slice(&self.payload[e.key_range.0..e.key_range.1]);
            if !started {
                if key_buf.as_slice() < target {
                    // Reconstructed for prefix-chain continuity, but skipped.
                    pos = e.next;
                    continue;
                }
                started = true;
            }
            let op_type = OpType::from_u8(e.op_byte).ok_or_else(|| {
                ForstError::corruption(format!("invalid op_type in KV block: {}", e.op_byte))
            })?;
            let value = e.value_range.map(|(s, en)| &self.payload[s..en]);
            cb(RowView {
                key: &key_buf,
                value,
                sequence: e.sequence,
                op_type,
            })?;
            pos = e.next;
        }
        Ok(())
    }

    /// Approximate in-memory size of this block (the decompressed payload).
    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }

    /// Highest-sequence visible row for *exactly* `key` (or `None`). Mirrors the
    /// v1 `get` semantics: returns the raw value (`None` for a tombstone) of the
    /// max-sequence matching row. Early-terminates once the scan passes `key`.
    pub fn lookup(&self, key: &[u8]) -> ForstResult<Option<(Option<Vec<u8>>, u64, OpType)>> {
        let mut best: Option<(Option<Vec<u8>>, u64, OpType)> = None;
        let mut best_seq = 0u64;
        self.walk_key(key, |value_range, sequence, op_byte, payload| {
            if best.is_none() || sequence > best_seq {
                best_seq = sequence;
                let op = OpType::from_u8(op_byte).ok_or_else(|| {
                    ForstError::corruption(format!("invalid op_type in KV block: {op_byte}"))
                })?;
                let value = value_range.map(|(s, e)| payload[s..e].to_vec());
                best = Some((value, sequence, op));
            }
            Ok(())
        })?;
        Ok(best)
    }

    /// Appends every version of *exactly* `key` to `out` as
    /// `(raw_value, sequence, op_type)`. Used by merge resolution
    /// (`get_versions`), which sorts across blocks afterwards.
    pub fn collect_versions(
        &self,
        key: &[u8],
        out: &mut Vec<(Option<Vec<u8>>, u64, OpType)>,
    ) -> ForstResult<()> {
        self.walk_key(key, |value_range, sequence, op_byte, payload| {
            let op = OpType::from_u8(op_byte).ok_or_else(|| {
                ForstError::corruption(format!("invalid op_type in KV block: {op_byte}"))
            })?;
            let value = value_range.map(|(s, e)| payload[s..e].to_vec());
            out.push((value, sequence, op));
            Ok(())
        })
    }

    /// Seeks to `key` and invokes `f(value_range, sequence, op_byte, payload)`
    /// for each entry whose key equals `key`, in on-disk order, then stops at
    /// the first key `> key`. Shared core of [`Self::lookup`] /
    /// [`Self::collect_versions`].
    fn walk_key<F>(&self, key: &[u8], mut f: F) -> ForstResult<()>
    where
        F: FnMut(Option<(usize, usize)>, u64, u8, &[u8]) -> ForstResult<()>,
    {
        let mut pos = self.seek_restart(key)?;
        let mut key_buf: Vec<u8> = Vec::new();
        while pos < self.restarts_start {
            let e = self.parse_entry(pos)?;
            if e.shared > key_buf.len() {
                return Err(ForstError::corruption(
                    "KV entry shared-prefix len exceeds previous key",
                ));
            }
            key_buf.truncate(e.shared);
            key_buf.extend_from_slice(&self.payload[e.key_range.0..e.key_range.1]);
            match key_buf.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    f(e.value_range, e.sequence, e.op_byte, &self.payload)?;
                }
                std::cmp::Ordering::Greater => break,
            }
            pos = e.next;
        }
        Ok(())
    }

    /// Returns the entries-region offset at which a linear scan for `target`
    /// must begin: the start of the last restart interval whose first (full)
    /// key is `<= target`, or 0 if `target` precedes the first key / there are
    /// no restarts.
    fn seek_restart(&self, target: &[u8]) -> ForstResult<usize> {
        if self.num_restarts == 0 {
            return Ok(0);
        }
        // `lo` = count of restarts whose full key is STRICTLY LESS than target
        // (== leftmost restart with key >= target). The comparison MUST be
        // strict `<`: restart keys carry only the user key, so many restarts can
        // share the target key when one key has many versions. A non-strict
        // `<=` would advance past all equal-keyed restarts and land on the LAST
        // one — skipping the newest versions (on-disk order is key ASC, seq
        // DESC, so the newest version is the FIRST occurrence). We start the
        // linear scan at the last restart with key < target (or 0), which is at
        // or before the first entry whose key >= target.
        let mut lo = 0usize;
        let mut hi = self.num_restarts;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let off = self.restart_offset(mid)?;
            let key = self.full_key_at_restart(off)?;
            if key < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let r = lo.saturating_sub(1);
        self.restart_offset(r)
    }

    /// The full key of a restart entry at payload offset `off`. Restart entries
    /// are written with `shared = 0`, so the key is contiguous in the payload.
    fn full_key_at_restart(&self, off: usize) -> ForstResult<&[u8]> {
        let e = self.parse_entry(off)?;
        if e.shared != 0 {
            return Err(ForstError::corruption(
                "KV restart entry has non-zero shared prefix",
            ));
        }
        Ok(&self.payload[e.key_range.0..e.key_range.1])
    }

    /// Decodes the entry at `pos`, reconstructing its key into `key_buf`, and
    /// invokes `cb` with the resulting [`RowView`]. Returns the offset of the
    /// next entry.
    fn read_row_at<F>(
        &self,
        pos: usize,
        key_buf: &mut Vec<u8>,
        cb: &mut F,
    ) -> ForstResult<usize>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        let e = self.parse_entry(pos)?;
        if e.shared > key_buf.len() {
            return Err(ForstError::corruption(
                "KV entry shared-prefix len exceeds previous key",
            ));
        }
        key_buf.truncate(e.shared);
        key_buf.extend_from_slice(&self.payload[e.key_range.0..e.key_range.1]);
        let op_type = OpType::from_u8(e.op_byte)
            .ok_or_else(|| ForstError::corruption(format!("invalid op_type in KV block: {}", e.op_byte)))?;
        let value = e.value_range.map(|(s, en)| &self.payload[s..en]);
        cb(RowView {
            key: key_buf,
            value,
            sequence: e.sequence,
            op_type,
        })?;
        Ok(e.next)
    }

    /// Parses the fixed-size header + spans of one entry starting at `pos`
    /// (offset into the entries region). All slicing is bounds-checked against
    /// `restarts_start`, so a corrupt block yields a corruption error, never a
    /// panic.
    fn parse_entry(&self, pos: usize) -> ForstResult<EntryParse> {
        let end = self.restarts_start;
        let p = &self.payload;
        if pos >= end {
            return Err(ForstError::corruption("KV entry offset past entries region"));
        }
        let (shared, n1) = get_varint32(&p[pos..end])?;
        let mut at = pos + n1;
        let (non_shared, n2) = get_varint32(&p[at..end])?;
        at += n2;
        let (value_tag, n3) = get_varint64(&p[at..end])?;
        at += n3;
        if at + 8 > end {
            return Err(ForstError::corruption("KV entry truncated at sequence"));
        }
        let (sequence, _) = get_fixed64(&p[at..])?;
        at += 8;
        if at + 1 > end {
            return Err(ForstError::corruption("KV entry truncated at op_type"));
        }
        let op_byte = p[at];
        at += 1;
        let key_start = at;
        let key_end = key_start
            .checked_add(non_shared as usize)
            .filter(|&e| e <= end)
            .ok_or_else(|| ForstError::corruption("KV entry key span out of range"))?;
        at = key_end;
        let value_range = if value_tag == 0 {
            None
        } else {
            let vlen = (value_tag - 1) as usize;
            let ve = at
                .checked_add(vlen)
                .filter(|&e| e <= end)
                .ok_or_else(|| ForstError::corruption("KV entry value span out of range"))?;
            let vs = at;
            at = ve;
            Some((vs, ve))
        };
        Ok(EntryParse {
            shared: shared as usize,
            key_range: (key_start, key_end),
            value_range,
            sequence,
            op_byte,
            next: at,
        })
    }
}

/// Decoded spans of one KV entry (offsets into the decompressed payload). No
/// allocation; the key must be reconstructed by prepending the shared prefix.
struct EntryParse {
    /// Bytes of key shared with the previous entry.
    shared: usize,
    /// Payload range of the non-shared key suffix.
    key_range: (usize, usize),
    /// Payload range of the value, or `None` for a tombstone.
    value_range: Option<(usize, usize)>,
    sequence: u64,
    op_byte: u8,
    /// Offset of the next entry.
    next: usize,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::sst_schema;
    use std::sync::Arc;

    /// Builds an SST-schema RecordBatch from rows, preserving order.
    fn make_batch(rows: &[(&[u8], Option<&[u8]>, u64, OpType)]) -> RecordBatch {
        let keys: Vec<&[u8]> = rows.iter().map(|r| r.0).collect();
        let vals: Vec<Option<&[u8]>> = rows.iter().map(|r| r.1).collect();
        let seqs: Vec<u64> = rows.iter().map(|r| r.2).collect();
        let ops: Vec<u8> = rows.iter().map(|r| r.3 as u8).collect();
        RecordBatch::try_new(
            Arc::new(sst_schema()),
            vec![
                Arc::new(BinaryArray::from_iter_values(keys)),
                Arc::new(BinaryArray::from_iter(vals)),
                Arc::new(UInt64Array::from(seqs)),
                Arc::new(UInt8Array::from(ops)),
            ],
        )
        .unwrap()
    }

    /// Drains a KvBlock into owned rows for assertion.
    fn collect(kv: &KvBlock) -> Vec<(Vec<u8>, Option<Vec<u8>>, u64, OpType)> {
        let mut out = Vec::new();
        kv.for_each_row(|v| {
            out.push((v.key.to_vec(), v.value.map(|b| b.to_vec()), v.sequence, v.op_type));
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn roundtrip_basic_rows_with_prefix_tombstone_and_empty_value() {
        // Shared prefixes ("user:1" / "user:2"), a tombstone (None), and an
        // empty-but-present value (Some(&[])) — the last must NOT collapse to
        // a tombstone.
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = vec![
            (b"user:1", Some(b"alice".as_ref()), 10, OpType::Put),
            (b"user:2", Some(b"".as_ref()), 9, OpType::Put),
            (b"user:3", None, 8, OpType::Delete),
        ];
        let batch = make_batch(&rows);
        let block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        // block_type byte must mark this as a v2 KV block.
        assert_eq!(block[0], BLOCK_TYPE_DATA_KV);

        let kv = KvBlock::decode(&block, true).unwrap();
        let got = collect(&kv);

        let want: Vec<(Vec<u8>, Option<Vec<u8>>, u64, OpType)> = vec![
            (b"user:1".to_vec(), Some(b"alice".to_vec()), 10, OpType::Put),
            (b"user:2".to_vec(), Some(b"".to_vec()), 9, OpType::Put),
            (b"user:3".to_vec(), None, 8, OpType::Delete),
        ];
        assert_eq!(got, want);
    }

    /// Builds a 50-row block of sorted distinct keys `key0000..key0049` (so
    /// restart intervals fall at entries 0/16/32/48), all Put. Exercises
    /// prefix-compression + restart boundaries + binary-search seek.
    fn big_block() -> Vec<u8> {
        let keys: Vec<Vec<u8>> = (0..50).map(|i| format!("key{i:04}").into_bytes()).collect();
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.as_slice(), Some(b"v".as_ref()), 100 - i as u64, OpType::Put))
            .collect();
        let batch = make_batch(&rows);
        encode_kv_data_block(&batch, CompressionType::None).unwrap()
    }

    fn collect_from(kv: &KvBlock, target: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        kv.for_each_row_from(target, |v| {
            out.push(v.key.to_vec());
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn iterate_all_across_restart_boundaries() {
        let block = big_block();
        let kv = KvBlock::decode(&block, true).unwrap();
        let got: Vec<Vec<u8>> = {
            let mut v = Vec::new();
            kv.for_each_row(|r| {
                v.push(r.key.to_vec());
                Ok(())
            })
            .unwrap();
            v
        };
        let want: Vec<Vec<u8>> = (0..50).map(|i| format!("key{i:04}").into_bytes()).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn seek_lands_on_first_key_ge_target_across_restarts() {
        let block = big_block();
        let kv = KvBlock::decode(&block, true).unwrap();

        // Exact key present (spans into the 2nd restart interval).
        let from20 = collect_from(&kv, b"key0020");
        assert_eq!(from20.len(), 30);
        assert_eq!(from20[0], b"key0020");
        assert_eq!(from20[29], b"key0049");

        // Absent key between 0019 and 0020 → first >= target is key0020.
        let between = collect_from(&kv, b"key0019x");
        assert_eq!(between[0], b"key0020");
        assert_eq!(between.len(), 30);

        // Target before everything → all 50.
        assert_eq!(collect_from(&kv, b"").len(), 50);
        assert_eq!(collect_from(&kv, b"key0000")[0], b"key0000");

        // Target after everything → none.
        assert!(collect_from(&kv, b"zzzz").is_empty());

        // Land exactly on a restart entry (entry 32 = key0032, shared=0).
        let from32 = collect_from(&kv, b"key0032");
        assert_eq!(from32[0], b"key0032");
        assert_eq!(from32.len(), 18);
    }

    #[test]
    fn lz4_roundtrip_matches_uncompressed() {
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = (0..40)
            .map(|_| (b"k".as_ref(), Some(b"value-bytes".as_ref()), 1u64, OpType::Put))
            .collect::<Vec<_>>();
        // distinct keys
        let keys: Vec<Vec<u8>> = (0..40).map(|i| format!("key{i:03}").into_bytes()).collect();
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = keys
            .iter()
            .zip(rows.iter())
            .map(|(k, r)| (k.as_slice(), r.1, r.2, r.3))
            .collect();
        let batch = make_batch(&rows);
        let block = encode_kv_data_block(&batch, CompressionType::Lz4).unwrap();
        assert_eq!(block[0], BLOCK_TYPE_DATA_KV);
        let kv = KvBlock::decode(&block, true).unwrap();
        assert_eq!(collect(&kv).len(), 40);
        assert_eq!(collect_from(&kv, b"key020")[0], b"key020");
    }

    #[test]
    fn mvcc_duplicate_keys_preserve_seq_descending_order() {
        // Same key, 3 versions newest-first (key ASC, seq DESC on-disk order).
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = vec![
            (b"k", Some(b"v3".as_ref()), 30, OpType::Put),
            (b"k", Some(b"v2".as_ref()), 20, OpType::Merge),
            (b"k", Some(b"v1".as_ref()), 10, OpType::Put),
            (b"m", None, 5, OpType::Delete),
        ];
        let batch = make_batch(&rows);
        let block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        let kv = KvBlock::decode(&block, true).unwrap();
        let got = collect(&kv);
        assert_eq!(got[0], (b"k".to_vec(), Some(b"v3".to_vec()), 30, OpType::Put));
        assert_eq!(got[1], (b"k".to_vec(), Some(b"v2".to_vec()), 20, OpType::Merge));
        assert_eq!(got[2], (b"k".to_vec(), Some(b"v1".to_vec()), 10, OpType::Put));
        assert_eq!(got[3], (b"m".to_vec(), None, 5, OpType::Delete));
        // seek to the duplicate key yields all 3 versions then 'm'.
        assert_eq!(collect_from(&kv, b"k").len(), 4);
    }

    #[test]
    fn checksum_gate_rejects_corruption_only_when_verifying() {
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> =
            vec![(b"a", Some(b"x".as_ref()), 1, OpType::Put)];
        let batch = make_batch(&rows);
        let mut block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        // Flip a payload byte (just past the 16-byte header) — corrupts the crc.
        block[BLOCK_HEADER_SIZE] ^= 0xFF;
        assert!(KvBlock::decode(&block, true).is_err(), "verify=true must reject");
        // verify=false: crc skipped. The payload-length framing still holds
        // (we flipped a content byte, not a size), so decode succeeds.
        assert!(
            KvBlock::decode(&block, false).is_ok(),
            "verify=false must skip the crc and decode"
        );
    }

    #[test]
    fn seek_with_many_versions_of_one_key_spanning_restarts() {
        // 50 versions of the SAME user key "k", on-disk order (key ASC, seq
        // DESC): entry 0 has the HIGHEST seq. With restart interval 16, restart
        // keys are all "k" — the seek must NOT overshoot past equal-keyed
        // restarts, or it skips the newest (earliest-on-disk) versions. This
        // reproduces the v2-compaction point-read regression (got v103, want
        // newest) at the unit level.
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = (0..50u64)
            .map(|i| (b"k".as_ref(), Some(b"v".as_ref()), 50 - i, OpType::Put))
            .collect();
        let batch = make_batch(&rows);
        let block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        let kv = KvBlock::decode(&block, true).unwrap();

        // Point lookup must return the MAX sequence (50), at entry 0.
        let (_, seq, _) = kv.lookup(b"k").unwrap().unwrap();
        assert_eq!(seq, 50, "lookup must return the newest (max-seq) version");

        // Seek must yield ALL 50 versions (not a tail subset).
        assert_eq!(collect_from(&kv, b"k").len(), 50);

        // collect_versions must return all 50.
        let mut out = Vec::new();
        kv.collect_versions(b"k", &mut out).unwrap();
        assert_eq!(out.len(), 50);
        assert_eq!(out[0].1, 50);
    }

    #[test]
    fn lookup_returns_max_seq_version_and_collect_returns_all() {
        let rows: Vec<(&[u8], Option<&[u8]>, u64, OpType)> = vec![
            (b"a", Some(b"a1".as_ref()), 5, OpType::Put),
            (b"k", Some(b"newest".as_ref()), 30, OpType::Put),
            (b"k", Some(b"mid".as_ref()), 20, OpType::Merge),
            (b"k", Some(b"old".as_ref()), 10, OpType::Put),
            (b"z", None, 99, OpType::Delete),
        ];
        let batch = make_batch(&rows);
        let block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        let kv = KvBlock::decode(&block, true).unwrap();

        // Point lookup: highest sequence wins.
        let (val, seq, op) = kv.lookup(b"k").unwrap().unwrap();
        assert_eq!(val, Some(b"newest".to_vec()));
        assert_eq!(seq, 30);
        assert_eq!(op, OpType::Put);

        // Tombstone lookup returns op=Delete, value=None.
        let (val, _, op) = kv.lookup(b"z").unwrap().unwrap();
        assert_eq!(val, None);
        assert_eq!(op, OpType::Delete);

        // Absent key.
        assert!(kv.lookup(b"missing").unwrap().is_none());

        // collect_versions returns all 3 versions of "k".
        let mut out = Vec::new();
        kv.collect_versions(b"k", &mut out).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].1, 30);
        assert_eq!(out[2].1, 10);
    }

    #[test]
    fn empty_block_decodes_to_zero_rows() {
        let batch = make_batch(&[]);
        let block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        let kv = KvBlock::decode(&block, true).unwrap();
        assert_eq!(collect(&kv).len(), 0);
        assert!(collect_from(&kv, b"anything").is_empty());
    }
}

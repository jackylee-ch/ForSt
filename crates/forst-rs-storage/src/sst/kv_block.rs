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
use std::sync::Arc;

/// Number of entries per restart interval (full key stored every K entries).
pub const KV_RESTART_INTERVAL: usize = 16;

/// Result row returned by [`KvDataBlock::lookup`].
pub type KvLookupResult = Option<(Option<Vec<u8>>, u64, OpType)>;

/// R-1 (read-path upgrade): one matched version of a point-get, with the value
/// expressed as a **zero-copy `(offset, len)` range into the block's
/// decompressed payload** ([`KvBlock::payload_bytes`]) instead of an owned
/// `Vec<u8>`. This is the RocksDB-shape point read: the restart-array binary
/// search lands on the key, and the value bytes are referenced in place — no
/// per-probe `Vec` allocation, no Arrow/row materialization.
///
/// Resolution: `value` is `None` for a tombstone; otherwise
/// `payload_bytes()[offset..offset+len]`. An empty-but-present value is
/// `Some((off, off))` (len 0), never collapsed to `None`. Offsets are produced
/// over bounds-checked entry parses, so they always fall inside the entries
/// region (before the restart trailer) and stay valid for the block's lifetime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointVersion {
    /// `(offset, len)` of the value in [`KvBlock::payload_bytes`], or `None`
    /// for a tombstone.
    pub value: Option<(u32, u32)>,
    /// Sequence number of this version.
    pub sequence: u64,
    /// Operation type of this version.
    pub op_type: OpType,
}

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

/// S2-1 (pinned-rows design §2.1 W1a): a borrowed-bytes *reference* to a row
/// field, expressed as an `(offset, len)` range into one of two stable backing
/// stores instead of a borrowed slice — so callers can capture row positions
/// during a block walk and resolve the bytes LATER, while the block (and the
/// caller-owned key arena) stay alive.
///
/// Resolution contract (D2, locked):
/// * `Arena` — indexes the caller-owned `key_arena` passed to
///   [`KvBlock::for_each_row_ranges`]. v2 KV-block KEYS are always `Arena`
///   refs: prefix-compressed keys are reconstructed by appending into the
///   arena exactly once (the same byte copy `for_each_row`'s scratch pays),
///   because the block payload does NOT contain contiguous full keys.
/// * `Block` — indexes a stable backing buffer owned by the decoded block:
///   the [`KvBlock`] decompressed payload ([`KvBlock::payload_bytes`]) for v2
///   values, or the respective Arrow `BinaryArray` values buffer for v1 keys
///   AND values (`batch_key_value_data` in `reader.rs`). Whether a `Block`
///   ref means "key buffer" or "value buffer" is positional: the `key` field
///   resolves against the key buffer, the `value` field against the value
///   buffer (for v2 both are the payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SliceRef {
    /// Range into the caller-owned key arena.
    Arena {
        /// Byte offset into the arena.
        offset: u32,
        /// Length in bytes.
        len: u32,
    },
    /// Range into the decoded block's stable backing buffer.
    Block {
        /// Byte offset into the block buffer.
        offset: u32,
        /// Length in bytes.
        len: u32,
    },
}

impl SliceRef {
    /// Checked constructor for an `Arena` ref (u32 framing guard).
    pub fn arena(offset: usize, len: usize) -> ForstResult<Self> {
        Ok(SliceRef::Arena {
            offset: u32::try_from(offset)
                .map_err(|_| ForstError::corruption("SliceRef arena offset exceeds u32"))?,
            len: u32::try_from(len)
                .map_err(|_| ForstError::corruption("SliceRef arena len exceeds u32"))?,
        })
    }

    /// Checked constructor for a `Block` ref (u32 framing guard).
    pub fn block(offset: usize, len: usize) -> ForstResult<Self> {
        Ok(SliceRef::Block {
            offset: u32::try_from(offset)
                .map_err(|_| ForstError::corruption("SliceRef block offset exceeds u32"))?,
            len: u32::try_from(len)
                .map_err(|_| ForstError::corruption("SliceRef block len exceeds u32"))?,
        })
    }

    /// Length in bytes of the referenced slice.
    pub fn len(&self) -> usize {
        match self {
            SliceRef::Arena { len, .. } | SliceRef::Block { len, .. } => *len as usize,
        }
    }

    /// Whether the referenced slice is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Resolves the reference against the two backing stores. `arena` is the
    /// caller-owned key arena; `block` is the stable block buffer appropriate
    /// for the FIELD this ref came from (see the type docs). Offsets are
    /// produced by the visitors in this crate over bounds-checked entry
    /// parses, so out-of-range here is an internal logic error (panics).
    pub fn resolve<'a>(&self, arena: &'a [u8], block: &'a [u8]) -> &'a [u8] {
        match *self {
            SliceRef::Arena { offset, len } => &arena[offset as usize..(offset + len) as usize],
            SliceRef::Block { offset, len } => &block[offset as usize..(offset + len) as usize],
        }
    }
}

/// S2-1: one row yielded by the range-exposing visitors
/// ([`KvBlock::for_each_row_ranges`] / `for_each_row_ranges_in_batch`) — the
/// offsets-only counterpart of [`RowView`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRanges {
    /// The full user key (v2: `Arena` ref into the caller's key arena;
    /// v1: `Block` ref into the key column's values buffer).
    pub key: SliceRef,
    /// The value (`Block` ref), or `None` for a tombstone. An empty-but-
    /// present value yields `Some` with `len == 0` (never collapses to
    /// `None`).
    pub value: Option<SliceRef>,
    /// Sequence number.
    pub sequence: u64,
    /// Operation type.
    pub op_type: OpType,
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

/// R-1: converts a `(start, end)` payload byte range into the `(offset, len)`
/// form [`PointVersion`] carries, with the u32 framing guard (block payloads
/// are well under 4 GiB, so a failure is a corruption signal, never a panic).
fn pack_value_range(range: Option<(usize, usize)>) -> ForstResult<Option<(u32, u32)>> {
    match range {
        None => Ok(None),
        Some((s, e)) => {
            let off = u32::try_from(s)
                .map_err(|_| ForstError::corruption("KV value offset exceeds u32"))?;
            let len = u32::try_from(e - s)
                .map_err(|_| ForstError::corruption("KV value len exceeds u32"))?;
            Ok(Some((off, len)))
        }
    }
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
            return Err(ForstError::corruption(
                "KV restart offset past entries region",
            ));
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

    /// S2-1: the stable decompressed payload bytes — the backing store every
    /// [`SliceRef::Block`] produced by [`Self::for_each_row_ranges`] resolves
    /// against. Stable for the lifetime of this `KvBlock` (the struct owns the
    /// `Vec`); value ranges always fall inside the entries region (before the
    /// restart trailer).
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload
    }

    /// S2-1 (pinned-rows design §2.1 W1a): like [`Self::for_each_row`], but
    /// (a) reconstructs each full key APPENDED into the caller-owned `arena`
    /// (never cleared by this method), yielding its range as a
    /// [`SliceRef::Arena`], and (b) yields the value as a [`SliceRef::Block`]
    /// range into [`Self::payload_bytes`] (`None` for tombstones), plus
    /// sequence and op-type. One pass, the same decode work as
    /// [`Self::for_each_row`] — strictly additive API.
    ///
    /// The callback receives `(arena_so_far, row)`: an immutable view of the
    /// arena (so callers can compare the current key against earlier arena
    /// rows during the walk) and the row's ranges. Ranges remain valid after
    /// the walk completes as long as the arena is only appended to and the
    /// block stays alive — that is the whole point (the engine's pinned
    /// `SstBlockBuf` resolves them at emit time).
    pub fn for_each_row_ranges<F>(&self, arena: &mut Vec<u8>, mut cb: F) -> ForstResult<()>
    where
        F: FnMut(&[u8], RowRanges) -> ForstResult<()>,
    {
        let mut pos = 0usize;
        // (offset, len) of the previous row's full key inside `arena` — the
        // prefix-chain source for the next row's shared bytes.
        let mut prev: Option<(usize, usize)> = None;
        while pos < self.restarts_start {
            let e = self.parse_entry(pos)?;
            let (prev_off, prev_len) = prev.unwrap_or((arena.len(), 0));
            if e.shared > prev_len {
                return Err(ForstError::corruption(
                    "KV entry shared-prefix len exceeds previous key",
                ));
            }
            let key_off = arena.len();
            // Shared prefix from the previous key's arena bytes, then the
            // non-shared tail straight from the payload. Identical bytes to
            // the `for_each_row` scratch reconstruction.
            arena.extend_from_within(prev_off..prev_off + e.shared);
            arena.extend_from_slice(&self.payload[e.key_range.0..e.key_range.1]);
            let key_len = arena.len() - key_off;
            let op_type = OpType::from_u8(e.op_byte).ok_or_else(|| {
                ForstError::corruption(format!("invalid op_type in KV block: {}", e.op_byte))
            })?;
            let row = RowRanges {
                key: SliceRef::arena(key_off, key_len)?,
                value: match e.value_range {
                    None => None,
                    Some((s, en)) => Some(SliceRef::block(s, en - s)?),
                },
                sequence: e.sequence,
                op_type,
            };
            cb(arena, row)?;
            prev = Some((key_off, key_len));
            pos = e.next;
        }
        Ok(())
    }

    /// Highest-sequence visible row for *exactly* `key` (or `None`). Mirrors the
    /// v1 `get` semantics: returns the raw value (`None` for a tombstone) of the
    /// max-sequence matching row. Early-terminates once the scan passes `key`.
    pub fn lookup(&self, key: &[u8]) -> ForstResult<KvLookupResult> {
        let mut best: KvLookupResult = None;
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

    /// R-1: highest-sequence visible version for *exactly* `key` as a
    /// **zero-copy** [`PointVersion`] (value referenced in-place in
    /// [`Self::payload_bytes`]; `None` if the key is absent). Byte-identical to
    /// [`Self::lookup`] — same restart-array binary search + linear scan,
    /// same max-sequence selection — but it never copies the value out of the
    /// payload (no per-probe `Vec` alloc, no full-block decode). This is the
    /// point-get read primitive the SST `get` path uses.
    pub fn point_lookup_in_block(&self, key: &[u8]) -> ForstResult<Option<PointVersion>> {
        let mut best: Option<PointVersion> = None;
        self.walk_key(key, |value_range, sequence, op_byte, _payload| {
            if best.is_none_or(|b| sequence > b.sequence) {
                let op = OpType::from_u8(op_byte).ok_or_else(|| {
                    ForstError::corruption(format!("invalid op_type in KV block: {op_byte}"))
                })?;
                best = Some(PointVersion {
                    value: pack_value_range(value_range)?,
                    sequence,
                    op_type: op,
                });
            }
            Ok(())
        })?;
        Ok(best)
    }

    /// R-1: zero-copy twin of [`Self::collect_versions`] — appends every
    /// version of *exactly* `key` to `out` as [`PointVersion`] (value ranges
    /// into [`Self::payload_bytes`]) in on-disk order, with no per-version
    /// `Vec` allocation. Used by the SST `get_versions` merge-resolution path.
    pub fn point_lookup_versions(
        &self,
        key: &[u8],
        out: &mut Vec<PointVersion>,
    ) -> ForstResult<()> {
        self.walk_key(key, |value_range, sequence, op_byte, _payload| {
            let op = OpType::from_u8(op_byte).ok_or_else(|| {
                ForstError::corruption(format!("invalid op_type in KV block: {op_byte}"))
            })?;
            out.push(PointVersion {
                value: pack_value_range(value_range)?,
                sequence,
                op_type: op,
            });
            Ok(())
        })
    }

    /// Seeks to `key` and invokes `f(value_range, sequence, op_byte, payload)`
    /// for each entry whose key equals `key`, in on-disk order, then stops at
    /// the first key `> key`. Shared core of [`Self::lookup`] /
    /// [`Self::collect_versions`] / [`Self::point_lookup_in_block`] /
    /// [`Self::point_lookup_versions`].
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
    fn read_row_at<F>(&self, pos: usize, key_buf: &mut Vec<u8>, cb: &mut F) -> ForstResult<usize>
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
        let op_type = OpType::from_u8(e.op_byte).ok_or_else(|| {
            ForstError::corruption(format!("invalid op_type in KV block: {}", e.op_byte))
        })?;
        let value = e.value_range.map(|(s, en)| &self.payload[s..en]);
        cb(RowView {
            key: key_buf,
            value,
            sequence: e.sequence,
            op_type,
        })?;
        Ok(e.next)
    }

    /// S2-3: seek-aware, early-stopping twin of [`Self::for_each_row_ranges`]
    /// — the ranges counterpart of [`Self::for_each_row_from`]. Binary-seeks
    /// the restart points, reconstructs the (skipped) prefix-chain keys
    /// before `lower` into a LOCAL scratch (never the caller's arena — at
    /// most one restart interval of throwaway work), then yields every row
    /// whose key is `>= lower` with the key APPENDED into `arena` exactly as
    /// the full visitor does. The callback returns `Ok(true)` to continue or
    /// `Ok(false)` to stop the walk (the caller's upper-bound early
    /// termination — the remaining rows are not decoded at all).
    ///
    /// This exists because the engine's pinned replenish probes are often
    /// point-shaped: walking (and arena-copying) a whole ~64 KiB block to
    /// accept a handful of rows measured +54 % on the deep-fan-out probe
    /// cell; this visitor bounds the waste to `< KV_RESTART_INTERVAL` rows.
    pub fn for_each_row_ranges_from<F>(
        &self,
        lower: &[u8],
        arena: &mut Vec<u8>,
        mut cb: F,
    ) -> ForstResult<()>
    where
        F: FnMut(&[u8], RowRanges) -> ForstResult<bool>,
    {
        let mut pos = self.seek_restart(lower)?;
        // Prefix-chain scratch for rows BEFORE `lower` (skipped, not yielded).
        let mut scratch: Vec<u8> = Vec::new();
        // (offset, len) of the previous YIELDED row's key inside `arena`.
        let mut prev_arena: Option<(usize, usize)> = None;
        let mut started = false;
        while pos < self.restarts_start {
            let e = self.parse_entry(pos)?;
            if !started {
                if e.shared > scratch.len() {
                    return Err(ForstError::corruption(
                        "KV entry shared-prefix len exceeds previous key",
                    ));
                }
                scratch.truncate(e.shared);
                scratch.extend_from_slice(&self.payload[e.key_range.0..e.key_range.1]);
                if scratch.as_slice() < lower {
                    // Reconstructed for prefix-chain continuity, but skipped.
                    pos = e.next;
                    continue;
                }
                started = true;
                // First in-range key: copy the scratch into the arena so the
                // yielded ref obeys the D2 contract (Arena, stable).
                let key_off = arena.len();
                arena.extend_from_slice(&scratch);
                let key_len = scratch.len();
                let row = RowRanges {
                    key: SliceRef::arena(key_off, key_len)?,
                    value: match e.value_range {
                        None => None,
                        Some((s, en)) => Some(SliceRef::block(s, en - s)?),
                    },
                    sequence: e.sequence,
                    op_type: OpType::from_u8(e.op_byte).ok_or_else(|| {
                        ForstError::corruption(format!(
                            "invalid op_type in KV block: {}",
                            e.op_byte
                        ))
                    })?,
                };
                prev_arena = Some((key_off, key_len));
                if !cb(arena, row)? {
                    return Ok(());
                }
                pos = e.next;
                continue;
            }
            let (prev_off, prev_len) = prev_arena.expect("set when started");
            if e.shared > prev_len {
                return Err(ForstError::corruption(
                    "KV entry shared-prefix len exceeds previous key",
                ));
            }
            let key_off = arena.len();
            arena.extend_from_within(prev_off..prev_off + e.shared);
            arena.extend_from_slice(&self.payload[e.key_range.0..e.key_range.1]);
            let key_len = arena.len() - key_off;
            let row = RowRanges {
                key: SliceRef::arena(key_off, key_len)?,
                value: match e.value_range {
                    None => None,
                    Some((s, en)) => Some(SliceRef::block(s, en - s)?),
                },
                sequence: e.sequence,
                op_type: OpType::from_u8(e.op_byte).ok_or_else(|| {
                    ForstError::corruption(format!("invalid op_type in KV block: {}", e.op_byte))
                })?,
            };
            prev_arena = Some((key_off, key_len));
            if !cb(arena, row)? {
                return Ok(());
            }
            pos = e.next;
        }
        Ok(())
    }

    /// Parses the fixed-size header + spans of one entry starting at `pos`
    /// (offset into the entries region). All slicing is bounds-checked against
    /// `restarts_start`, so a corrupt block yields a corruption error, never a
    /// panic.
    fn parse_entry(&self, pos: usize) -> ForstResult<EntryParse> {
        let end = self.restarts_start;
        let p = &self.payload;
        if pos >= end {
            return Err(ForstError::corruption(
                "KV entry offset past entries region",
            ));
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

/// FRS-ZERO-COPY-MERGE (2026-06-05): a pull cursor over a [`KvBlock`], yielding
/// rows sequentially by reference for the k-way compaction merge. Mirrors
/// [`KvBlock::for_each_row`] exactly (same prefix-key reconstruction), but as a
/// `current`/`advance` stepper so multiple inputs can be merged by a heap
/// without materializing a `Vec` of owned entries. The value is borrowed
/// straight from the payload (zero copy); only the running key is reconstructed
/// into a reused buffer (one truncate+extend per row, not a fresh alloc).
pub(crate) struct KvBlockCursor {
    block: Arc<KvBlock>,
    /// Offset of the NEXT entry to decode (already advanced past `cur_*`).
    pos: usize,
    /// Running reconstructed key of the CURRENT entry (reused across advances).
    key_buf: Vec<u8>,
    cur_value: Option<(usize, usize)>,
    cur_seq: u64,
    cur_op: u8,
    valid: bool,
}

impl KvBlockCursor {
    /// Creates a cursor positioned at the first row (or invalid if empty).
    pub(crate) fn new(block: Arc<KvBlock>) -> ForstResult<Self> {
        let mut c = Self {
            block,
            pos: 0,
            key_buf: Vec::new(),
            cur_value: None,
            cur_seq: 0,
            cur_op: 0,
            valid: false,
        };
        c.load()?;
        Ok(c)
    }

    /// Decodes the entry at `self.pos` into the current slot and advances `pos`
    /// to the following entry. Sets `valid=false` at end-of-block.
    fn load(&mut self) -> ForstResult<()> {
        if self.pos >= self.block.restarts_start {
            self.valid = false;
            return Ok(());
        }
        let e = self.block.parse_entry(self.pos)?;
        if e.shared > self.key_buf.len() {
            return Err(ForstError::corruption(
                "KV cursor entry shared-prefix len exceeds previous key",
            ));
        }
        self.key_buf.truncate(e.shared);
        self.key_buf
            .extend_from_slice(&self.block.payload[e.key_range.0..e.key_range.1]);
        self.cur_value = e.value_range;
        self.cur_seq = e.sequence;
        self.cur_op = e.op_byte;
        self.pos = e.next;
        self.valid = true;
        Ok(())
    }

    #[inline]
    pub(crate) fn valid(&self) -> bool {
        self.valid
    }
    #[inline]
    pub(crate) fn key(&self) -> &[u8] {
        &self.key_buf
    }
    #[inline]
    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.cur_value.map(|(s, e)| &self.block.payload[s..e])
    }
    #[inline]
    pub(crate) fn sequence(&self) -> u64 {
        self.cur_seq
    }
    #[inline]
    pub(crate) fn op_byte(&self) -> u8 {
        self.cur_op
    }
    /// Advances to the next row; after this `valid()` reports availability.
    pub(crate) fn advance(&mut self) -> ForstResult<()> {
        self.load()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::sst_schema;
    use std::sync::Arc;

    type TestRow<'a> = (&'a [u8], Option<&'a [u8]>, u64, OpType);
    type OwnedTestRow = (Vec<u8>, Option<Vec<u8>>, u64, OpType);

    /// Builds an SST-schema RecordBatch from rows, preserving order.
    fn make_batch(rows: &[TestRow<'_>]) -> RecordBatch {
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
    fn collect(kv: &KvBlock) -> Vec<OwnedTestRow> {
        let mut out = Vec::new();
        kv.for_each_row(|v| {
            out.push((
                v.key.to_vec(),
                v.value.map(|b| b.to_vec()),
                v.sequence,
                v.op_type,
            ));
            Ok(())
        })
        .unwrap();
        out
    }

    /// Drains a KvBlock via the pull cursor for assertion.
    fn collect_cursor(kv: Arc<KvBlock>) -> Vec<OwnedTestRow> {
        let mut out = Vec::new();
        let mut c = KvBlockCursor::new(kv).unwrap();
        while c.valid() {
            out.push((
                c.key().to_vec(),
                c.value().map(|b| b.to_vec()),
                c.sequence(),
                OpType::from_u8(c.op_byte()).unwrap(),
            ));
            c.advance().unwrap();
        }
        out
    }

    #[test]
    fn kv_block_cursor_matches_for_each_row() {
        // (a) prefix + tombstone + empty-value block
        let rows: Vec<TestRow<'_>> = vec![
            (b"user:1", Some(b"alice".as_ref()), 10, OpType::Put),
            (b"user:2", Some(b"".as_ref()), 9, OpType::Put),
            (b"user:3", None, 8, OpType::Delete),
        ];
        let blk = KvBlock::decode(
            &encode_kv_data_block(&make_batch(&rows), CompressionType::None).unwrap(),
            true,
        )
        .unwrap();
        let want = collect(&blk);
        assert_eq!(collect_cursor(Arc::new(blk)), want, "basic block");

        // (b) 50-row multi-restart block (crosses restart boundaries 0/16/32/48)
        let big = KvBlock::decode(&big_block(), true).unwrap();
        let want_big = collect(&big);
        assert_eq!(want_big.len(), 50);
        assert_eq!(collect_cursor(Arc::new(big)), want_big, "big block");
    }

    #[test]
    fn roundtrip_basic_rows_with_prefix_tombstone_and_empty_value() {
        // Shared prefixes ("user:1" / "user:2"), a tombstone (None), and an
        // empty-but-present value (Some(&[])) — the last must NOT collapse to
        // a tombstone.
        let rows: Vec<TestRow<'_>> = vec![
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

        let want: Vec<OwnedTestRow> = vec![
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
        let rows: Vec<TestRow<'_>> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.as_slice(),
                    Some(b"v".as_ref()),
                    100 - i as u64,
                    OpType::Put,
                )
            })
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
        let rows: Vec<TestRow<'_>> = (0..40)
            .map(|_| {
                (
                    b"k".as_ref(),
                    Some(b"value-bytes".as_ref()),
                    1u64,
                    OpType::Put,
                )
            })
            .collect::<Vec<_>>();
        // distinct keys
        let keys: Vec<Vec<u8>> = (0..40).map(|i| format!("key{i:03}").into_bytes()).collect();
        let rows: Vec<TestRow<'_>> = keys
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
        let rows: Vec<TestRow<'_>> = vec![
            (b"k", Some(b"v3".as_ref()), 30, OpType::Put),
            (b"k", Some(b"v2".as_ref()), 20, OpType::Merge),
            (b"k", Some(b"v1".as_ref()), 10, OpType::Put),
            (b"m", None, 5, OpType::Delete),
        ];
        let batch = make_batch(&rows);
        let block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        let kv = KvBlock::decode(&block, true).unwrap();
        let got = collect(&kv);
        assert_eq!(
            got[0],
            (b"k".to_vec(), Some(b"v3".to_vec()), 30, OpType::Put)
        );
        assert_eq!(
            got[1],
            (b"k".to_vec(), Some(b"v2".to_vec()), 20, OpType::Merge)
        );
        assert_eq!(
            got[2],
            (b"k".to_vec(), Some(b"v1".to_vec()), 10, OpType::Put)
        );
        assert_eq!(got[3], (b"m".to_vec(), None, 5, OpType::Delete));
        // seek to the duplicate key yields all 3 versions then 'm'.
        assert_eq!(collect_from(&kv, b"k").len(), 4);
    }

    #[test]
    fn checksum_gate_rejects_corruption_only_when_verifying() {
        let rows: Vec<TestRow<'_>> = vec![(b"a", Some(b"x".as_ref()), 1, OpType::Put)];
        let batch = make_batch(&rows);
        let mut block = encode_kv_data_block(&batch, CompressionType::None).unwrap();
        // Flip a payload byte (just past the 16-byte header) — corrupts the crc.
        block[BLOCK_HEADER_SIZE] ^= 0xFF;
        assert!(
            KvBlock::decode(&block, true).is_err(),
            "verify=true must reject"
        );
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
        let rows: Vec<TestRow<'_>> = (0..50u64)
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
        let rows: Vec<TestRow<'_>> = vec![
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

    // =======================================================================
    // R-1: zero-copy point_lookup_in_block / point_lookup_versions byte-equality
    // =======================================================================

    /// Resolves a [`PointVersion`] (zero-copy range) into the owned
    /// `(value, seq, op)` triple `lookup`/`collect_versions` produce, so the
    /// two paths can be compared byte-for-byte.
    fn resolve_pv(kv: &KvBlock, pv: &PointVersion) -> (Option<Vec<u8>>, u64, OpType) {
        let value = pv
            .value
            .map(|(off, len)| kv.payload_bytes()[off as usize..(off + len) as usize].to_vec());
        (value, pv.sequence, pv.op_type)
    }

    /// The zero-copy `point_lookup_in_block` must return EXACTLY the value
    /// bytes / seq / op the full-decode `lookup` does, for every probe class.
    #[test]
    fn r1_point_lookup_in_block_matches_lookup() {
        // (a) prefix + tombstone + empty-value, distinct keys.
        let rows_a: Vec<TestRow<'_>> = vec![
            (b"user:1", Some(b"alice".as_ref()), 10, OpType::Put),
            (b"user:2", Some(b"".as_ref()), 9, OpType::Put),
            (b"user:3", None, 8, OpType::Delete),
        ];
        // (b) 50 distinct keys spanning restart boundaries (varint key/value).
        let rows_b_keys: Vec<Vec<u8>> =
            (0..50).map(|i| format!("key{i:04}").into_bytes()).collect();
        let rows_b: Vec<TestRow<'_>> = rows_b_keys
            .iter()
            .enumerate()
            .map(|(i, k)| {
                (
                    k.as_slice(),
                    Some(b"vvvv".as_ref()),
                    100 - i as u64,
                    OpType::Put,
                )
            })
            .collect();
        // (c) MVCC: many versions of ONE key spanning restarts (newest wins).
        let rows_c: Vec<TestRow<'_>> = (0..50u64)
            .map(|i| (b"k".as_ref(), Some(b"v".as_ref()), 50 - i, OpType::Put))
            .collect();
        // (d) single-entry block.
        let rows_d: Vec<TestRow<'_>> = vec![(b"solo", Some(b"only".as_ref()), 7, OpType::Put)];

        for (label, rows) in [
            ("a", &rows_a),
            ("b", &rows_b),
            ("c", &rows_c),
            ("d", &rows_d),
        ] {
            for compression in [CompressionType::None, CompressionType::Lz4] {
                let block = encode_kv_data_block(&make_batch(rows), compression).unwrap();
                let kv = KvBlock::decode(&block, true).unwrap();

                // Probe set: every present key + boundary misses + first/last.
                let mut probes: Vec<Vec<u8>> = rows.iter().map(|r| r.0.to_vec()).collect();
                probes.push(Vec::new()); // before-all
                probes.push(b"zzzzzzzz".to_vec()); // after-all
                probes.push(b"key0019x".to_vec()); // between-keys miss (block b)
                probes.push(b"missing".to_vec());
                probes.dedup();

                for probe in &probes {
                    let want = kv.lookup(probe).unwrap();
                    let got = kv.point_lookup_in_block(probe).unwrap();
                    let got = got.map(|pv| resolve_pv(&kv, &pv));
                    assert_eq!(
                        got, want,
                        "block={label} compression={compression:?} probe={probe:?}"
                    );
                }
            }
        }

        // (e) empty block: every probe is a miss.
        let kv = KvBlock::decode(
            &encode_kv_data_block(&make_batch(&[]), CompressionType::None).unwrap(),
            true,
        )
        .unwrap();
        assert!(kv.point_lookup_in_block(b"anything").unwrap().is_none());
    }

    /// `point_lookup_versions` (zero-copy) must yield EXACTLY what
    /// `collect_versions` does, in the same order, for every probe class.
    #[test]
    fn r1_point_lookup_versions_matches_collect_versions() {
        let rows_mvcc: Vec<TestRow<'_>> = vec![
            (b"a", Some(b"a1".as_ref()), 5, OpType::Put),
            (b"k", Some(b"newest".as_ref()), 30, OpType::Put),
            (b"k", Some(b"mid".as_ref()), 20, OpType::Merge),
            (b"k", None, 10, OpType::Delete),
            (b"z", Some(b"".as_ref()), 99, OpType::Put),
        ];
        // many versions of one key across restarts
        let rows_many: Vec<TestRow<'_>> = (0..50u64)
            .map(|i| (b"k".as_ref(), Some(b"v".as_ref()), 50 - i, OpType::Merge))
            .collect();

        for (label, rows) in [("mvcc", &rows_mvcc), ("many", &rows_many)] {
            for compression in [CompressionType::None, CompressionType::Lz4] {
                let block = encode_kv_data_block(&make_batch(rows), compression).unwrap();
                let kv = KvBlock::decode(&block, true).unwrap();

                let mut probes: Vec<Vec<u8>> = rows.iter().map(|r| r.0.to_vec()).collect();
                probes.push(b"missing".to_vec());
                probes.push(Vec::new());
                probes.dedup();

                for probe in &probes {
                    let mut want: Vec<(Option<Vec<u8>>, u64, OpType)> = Vec::new();
                    kv.collect_versions(probe, &mut want).unwrap();
                    let mut got_pv: Vec<PointVersion> = Vec::new();
                    kv.point_lookup_versions(probe, &mut got_pv).unwrap();
                    let got: Vec<(Option<Vec<u8>>, u64, OpType)> =
                        got_pv.iter().map(|pv| resolve_pv(&kv, pv)).collect();
                    assert_eq!(
                        got, want,
                        "block={label} compression={compression:?} probe={probe:?}"
                    );
                }
            }
        }
    }

    // =======================================================================
    // S2-1 G1: ranges-visitor vs for_each_row byte-equality (property test)
    // =======================================================================

    /// Deterministic xorshift64* (no external deps).
    fn xorshift(state: &mut u64) -> u64 {
        let mut x = *state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *state = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    /// Builds a random sorted `(key ASC, seq DESC)` row set designed to
    /// exercise the prefix-compression edges: long shared prefixes, a
    /// shared-prefix length GREATER than the previous key's non-shared tail
    /// (forcing `extend_from_within` across the previous key's full arena
    /// span), restart boundaries (>16 and >32 rows), duplicate user keys
    /// (multi-version), tombstones, merge ops, and empty-but-present values.
    fn random_rows(seed: u64) -> Vec<OwnedTestRow> {
        let mut s = seed;
        let n_keys = 1 + (xorshift(&mut s) % 60) as usize;
        // Tiny alphabet + nested key shapes → deep shared prefixes.
        let mut keys: Vec<Vec<u8>> = (0..n_keys)
            .map(|_| {
                let len = 1 + (xorshift(&mut s) % 38) as usize;
                (0..len)
                    .map(|_| b"ab"[(xorshift(&mut s) % 2) as usize])
                    .collect()
            })
            .collect();
        keys.sort();
        keys.dedup();
        let mut rows: Vec<OwnedTestRow> = Vec::new();
        for key in keys {
            let versions = 1 + (xorshift(&mut s) % 3);
            let base_seq = 1000 + (xorshift(&mut s) % 1000);
            for v in 0..versions {
                let op = match xorshift(&mut s) % 4 {
                    0 => OpType::Delete,
                    1 => OpType::Merge,
                    _ => OpType::Put,
                };
                let value = if op == OpType::Delete {
                    None
                } else {
                    // Includes the empty-but-present value edge.
                    let vlen = (xorshift(&mut s) % 50) as usize;
                    Some(
                        (0..vlen)
                            .map(|_| (xorshift(&mut s) & 0xFF) as u8)
                            .collect::<Vec<u8>>(),
                    )
                };
                rows.push((key.clone(), value, base_seq - v, op));
            }
        }
        rows
    }

    /// Walks `kv` via the ranges visitor and resolves AFTER the full walk
    /// completes (per the D2 gate construction: a v2 key that escaped as a
    /// `Block` ref, or an arena range corrupted by later appends, fails the
    /// post-walk byte equality — a per-row assert could not catch it).
    fn collect_ranges_post_walk(kv: &KvBlock, arena_preseed: &[u8]) -> Vec<OwnedTestRow> {
        let mut arena: Vec<u8> = arena_preseed.to_vec();
        let mut metas: Vec<RowRanges> = Vec::new();
        kv.for_each_row_ranges(&mut arena, |arena_view, row| {
            // The in-walk view must already resolve the CURRENT key (the
            // engine's replenish compares against it for dedup).
            assert_eq!(
                row.key.resolve(arena_view, kv.payload_bytes()),
                &arena_view[arena_view.len() - row.key.len()..],
                "current key must be the arena tail during the walk"
            );
            // D2 trap arm: a v2 key must NEVER be a Block ref.
            assert!(
                matches!(row.key, SliceRef::Arena { .. }),
                "v2 KV-block key escaped as a Block ref (D2 violation)"
            );
            metas.push(row);
            Ok(())
        })
        .unwrap();
        // Post-walk resolution (the actual G1 equality input).
        metas
            .into_iter()
            .map(|m| {
                (
                    m.key.resolve(&arena, kv.payload_bytes()).to_vec(),
                    m.value
                        .map(|v| v.resolve(&arena, kv.payload_bytes()).to_vec()),
                    m.sequence,
                    m.op_type,
                )
            })
            .collect()
    }

    #[test]
    fn s2_ranges_visitor_matches_for_each_row_property() {
        for seed in 1..=40u64 {
            let rows = random_rows(seed.wrapping_mul(0x9E3779B97F4A7C15));
            let refs: Vec<TestRow<'_>> = rows
                .iter()
                .map(|(k, v, s, o)| (k.as_slice(), v.as_deref(), *s, *o))
                .collect();
            let batch = make_batch(&refs);
            for compression in [CompressionType::None, CompressionType::Lz4] {
                let block = encode_kv_data_block(&batch, compression).unwrap();
                let kv = KvBlock::decode(&block, true).unwrap();
                let want = collect(&kv);
                // Pre-seeded arena: the visitor must APPEND (caller-owned,
                // not cleared) and ranges must account for the preseed.
                let got = collect_ranges_post_walk(&kv, b"PRESEED");
                assert_eq!(
                    got, want,
                    "seed={seed} compression={compression:?}: ranges walk != for_each_row"
                );
            }
        }
    }

    /// S2-3 G1: the seek-aware `for_each_row_ranges_from` yields, for every
    /// target, EXACTLY the rows `for_each_row_from` yields (byte-equality,
    /// resolved post-walk per D2), and the stop-callback truncates the walk.
    #[test]
    fn s2_ranges_from_visitor_matches_for_each_row_from_property() {
        for seed in 1..=25u64 {
            let rows = random_rows(seed.wrapping_mul(0xA24B_AED4_963E_E407));
            let refs: Vec<TestRow<'_>> = rows
                .iter()
                .map(|(k, v, s, o)| (k.as_slice(), v.as_deref(), *s, *o))
                .collect();
            let batch = make_batch(&refs);
            for compression in [CompressionType::None, CompressionType::Lz4] {
                let block = encode_kv_data_block(&batch, compression).unwrap();
                let kv = KvBlock::decode(&block, true).unwrap();
                // Targets: before-all, an existing key, a between-keys probe,
                // after-all.
                let mut targets: Vec<Vec<u8>> = vec![Vec::new(), b"zzzzzz".to_vec()];
                if let Some((k, ..)) = rows.first() {
                    targets.push(k.clone());
                }
                if let Some((k, ..)) = rows.get(rows.len() / 2) {
                    targets.push(k.clone());
                    let mut between = k.clone();
                    between.push(0x00);
                    targets.push(between);
                }
                for target in targets {
                    let mut want: Vec<OwnedTestRow> = Vec::new();
                    kv.for_each_row_from(&target, |v| {
                        want.push((
                            v.key.to_vec(),
                            v.value.map(|b| b.to_vec()),
                            v.sequence,
                            v.op_type,
                        ));
                        Ok(())
                    })
                    .unwrap();

                    let mut arena = b"PRESEED".to_vec();
                    let mut metas: Vec<RowRanges> = Vec::new();
                    kv.for_each_row_ranges_from(&target, &mut arena, |_, row| {
                        assert!(matches!(row.key, SliceRef::Arena { .. }), "D2");
                        metas.push(row);
                        Ok(true)
                    })
                    .unwrap();
                    let got: Vec<OwnedTestRow> = metas
                        .iter()
                        .map(|m| {
                            (
                                m.key.resolve(&arena, kv.payload_bytes()).to_vec(),
                                m.value
                                    .map(|v| v.resolve(&arena, kv.payload_bytes()).to_vec()),
                                m.sequence,
                                m.op_type,
                            )
                        })
                        .collect();
                    assert_eq!(
                        got, want,
                        "seed={seed} compression={compression:?} target={target:?}"
                    );

                    // Stop-callback: cb returning false after k rows yields a
                    // strict prefix of `want`.
                    if want.len() > 1 {
                        let stop_after = want.len() / 2;
                        let mut arena = Vec::new();
                        let mut metas: Vec<RowRanges> = Vec::new();
                        kv.for_each_row_ranges_from(&target, &mut arena, |_, row| {
                            metas.push(row);
                            Ok(metas.len() < stop_after)
                        })
                        .unwrap();
                        assert_eq!(metas.len(), stop_after, "stop must truncate the walk");
                        let got_keys: Vec<Vec<u8>> = metas
                            .iter()
                            .map(|m| m.key.resolve(&arena, kv.payload_bytes()).to_vec())
                            .collect();
                        let want_keys: Vec<Vec<u8>> =
                            want[..stop_after].iter().map(|r| r.0.clone()).collect();
                        assert_eq!(got_keys, want_keys);
                    }
                }
            }
        }
    }

    #[test]
    fn s2_ranges_visitor_edge_blocks() {
        // Single-row block.
        let one: Vec<TestRow<'_>> = vec![(b"solo", Some(b"v".as_ref()), 7, OpType::Put)];
        let kv = KvBlock::decode(
            &encode_kv_data_block(&make_batch(&one), CompressionType::None).unwrap(),
            true,
        )
        .unwrap();
        assert_eq!(collect_ranges_post_walk(&kv, &[]), collect(&kv));

        // Empty block.
        let kv = KvBlock::decode(
            &encode_kv_data_block(&make_batch(&[]), CompressionType::None).unwrap(),
            true,
        )
        .unwrap();
        assert!(collect_ranges_post_walk(&kv, &[]).is_empty());

        // 50-row multi-restart block (restart entries at 0/16/32/48 reset
        // shared=0 mid-walk).
        let kv = KvBlock::decode(&big_block(), true).unwrap();
        let want = collect(&kv);
        assert_eq!(want.len(), 50);
        assert_eq!(collect_ranges_post_walk(&kv, &[]), want);

        // Tombstone + empty-present-value discrimination.
        let rows: Vec<TestRow<'_>> = vec![
            (b"user:1", Some(b"".as_ref()), 9, OpType::Put),
            (b"user:2", None, 8, OpType::Delete),
        ];
        let kv = KvBlock::decode(
            &encode_kv_data_block(&make_batch(&rows), CompressionType::None).unwrap(),
            true,
        )
        .unwrap();
        let got = collect_ranges_post_walk(&kv, &[]);
        assert_eq!(got[0].1, Some(Vec::new()), "empty value stays Some");
        assert_eq!(got[1].1, None, "tombstone stays None");
    }
}

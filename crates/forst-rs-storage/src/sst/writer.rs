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

//! SST file writer.
//!
//! [`SstWriterImpl`] accumulates sorted key-value entries into DataBlocks
//! and produces a complete SST file (as in-memory bytes) conforming to the
//! ForSt-RS Arrow SST format:
//!
//! ```text
//! FileHeader (16B) | DataBlock₀ | … | DataBlockₙ | BloomFilter | IndexSection | Footer
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayBuilder, BinaryBuilder, RecordBatch, UInt64Builder, UInt8Builder};
use arrow::datatypes::Schema;

use bytes::Bytes;
use forst_rs_common::{ColumnFamilyId, CompressionType, ForstError, ForstResult, DEFAULT_CF_ID};
use forst_rs_io::filesystem::WritableFile;

use super::bloom_filter::Sbbf;
use super::data_block::encode_data_block;
use super::file_header::FileHeader;
use super::footer::{ChecksumType, FooterV1};
use super::schema::{sst_schema, PREFIX_BLOOM_LEN, SST_FORMAT_VERSION};
use super::sparse_index::{encode_index, BlockStats, SparseIndexEntry};

// FRS_PROF_DIAG (2026-06-08): SST-writer sub-cost attribution (gap-map Task 1, verify-before-fix).
// Splits the flush/compaction write cost into row-buffering vs block-encode(+compress) vs sink-write
// so the vectorization targets the real hotspot. Cumulative ns; read by the engine's FRS_MEM_DIAG.
pub static SST_BUFFER_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SST_ENCODE_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static SST_SINKWRITE_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[inline]
fn sst_prof_add(c: &std::sync::atomic::AtomicU64, n: u64) {
    c.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
}
/// Snapshot of (buffer_ns, encode_ns, sinkwrite_ns) for the FRS_MEM_DIAG line.
pub fn sst_writer_prof_ns() -> (u64, u64, u64) {
    use std::sync::atomic::Ordering::Relaxed;
    (
        SST_BUFFER_NS.load(Relaxed),
        SST_ENCODE_NS.load(Relaxed),
        SST_SINKWRITE_NS.load(Relaxed),
    )
}

/// Information about a completed SST file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFileInfo {
    /// Total file size in bytes.
    pub file_size: u64,
    /// Number of key-value entries across all DataBlocks.
    pub entry_count: u64,
    /// Number of DataBlocks.
    pub data_block_count: u32,
    /// Smallest key in the SST file.
    pub min_key: Vec<u8>,
    /// Largest key in the SST file.
    pub max_key: Vec<u8>,
    /// Smallest sequence number.
    pub min_sequence: u64,
    /// Largest sequence number.
    pub max_sequence: u64,
    /// R49-H1: column family the writer was configured with. Returned so
    /// callers can stamp it onto the resulting `SstFileMeta` without
    /// shipping the descriptor around separately.
    pub cf_id: ColumnFamilyId,
}

/// Options controlling SST file generation.
#[derive(Debug, Clone)]
pub struct SstWriterOptions {
    /// Target DataBlock size in bytes before flush. Default: 64KB.
    pub block_size: usize,
    /// Compression algorithm for DataBlocks. Default: LZ4.
    pub compression: CompressionType,
    /// R49-H1: column family this SST belongs to. Written into the footer
    /// (v2+) so restore-time validation and runtime per-CF filtering can
    /// gate access. Defaults to [`DEFAULT_CF_ID`] for legacy callers.
    pub cf_id: ColumnFamilyId,
}

impl Default for SstWriterOptions {
    fn default() -> Self {
        Self {
            block_size: 64 * 1024,
            compression: CompressionType::Lz4,
            cf_id: DEFAULT_CF_ID,
        }
    }
}

/// Builder that accumulates sorted KV entries and produces an SST file.
///
/// PR-D2 (Z3-10, C-R3-H1..3): the writer streams data blocks directly into an
/// out-of-band [`WritableFile`] sink via [`Self::write_to`], avoiding a full
/// `Vec<u8>` materialisation of the entire SST. The bloom filter and sparse
/// index sections MUST still be buffered until every data block has been
/// written (their content depends on the complete set of keys / block stats)
/// — that buffered footprint is bounded by the block count × per-block
/// metadata, not the SST's total byte size, so it stays small.
///
/// The legacy [`Self::finish`] API is retained for callers (and existing
/// tests) that want the full file bytes in memory; it is now implemented as
/// a thin wrapper over [`Self::write_to`] driving an in-memory sink.
pub struct SstWriterImpl {
    options: SstWriterOptions,
    schema: Arc<Schema>,
    /// Total bytes written so far — used as the running offset for each new
    /// data block (formerly the length of an internal `buf`). In streaming
    /// mode this is the byte count emitted into the sink; in legacy mode it
    /// matches the staging buffer length.
    bytes_written: u64,
    /// Staging buffer for data blocks that flushed before a streaming sink
    /// was attached. Used only by the legacy `add()` + `finish()` path
    /// (`finish()` drains this into the `InMemorySink` it builds). When the
    /// caller drives the writer via [`SstWriterImpl::streaming`], this
    /// stays empty — blocks go straight to the sink.
    staging: Vec<u8>,
    key_builder: BinaryBuilder,
    value_builder: BinaryBuilder,
    sequence_builder: UInt64Builder,
    op_type_builder: UInt8Builder,
    current_estimated_size: usize,
    index_entries: Vec<SparseIndexEntry>,
    block_stats: Vec<BlockStats>,
    total_entries: u64,
    /// PR-D2: store min/max keys as ref-counted `Bytes` rather than `Vec<u8>`
    /// so updates clone the underlying allocation only when the new
    /// min/max actually changes (vs. the old code which `to_vec()`'d every
    /// added row).
    global_min_key: Option<Bytes>,
    global_max_key: Option<Bytes>,
    global_min_sequence: u64,
    global_max_sequence: u64,
    finished: bool,
    /// Last added key for debug-mode sorted-order invariant check.
    last_added_key: Option<Bytes>,
    key_hashes: Vec<u64>,
    /// v3 prefix bloom: hashes of distinct PREFIX_BLOOM_LEN-byte key prefixes
    /// (sorted append order makes last-prefix comparison exact dedup).
    prefix_hashes: Vec<u64>,
    /// Last prefix pushed into `prefix_hashes` (dedup state).
    last_prefix: Option<Vec<u8>>,
    /// C (2026-06-04): per-instance override for the v2 KV block-format flag.
    /// `None` (production default) defers to [`super::kv_block::sst_write_kv_format`]
    /// (the `FRS_SST_KV_BLOCK_FORMAT` env gate); tests set it explicitly via
    /// [`SstWriterImpl::force_kv_block_format`] to exercise both formats
    /// deterministically in one process.
    kv_format_override: Option<bool>,
}

impl Default for SstWriterImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl SstWriterImpl {
    /// Creates a new writer with default options.
    pub fn new() -> Self {
        Self::with_options(SstWriterOptions::default())
    }

    /// Creates a new writer with the given options.
    pub fn with_options(options: SstWriterOptions) -> Self {
        let schema = Arc::new(sst_schema());
        Self {
            options,
            schema,
            // The file header itself is the first thing emitted; we account for
            // its 16 bytes up front so block offsets line up with the on-disk
            // layout. The actual bytes are written by `write_to` (or by the
            // legacy in-memory finish path).
            bytes_written: FileHeader::default().encode().len() as u64,
            staging: Vec::new(),
            key_builder: BinaryBuilder::new(),
            value_builder: BinaryBuilder::new(),
            sequence_builder: UInt64Builder::new(),
            op_type_builder: UInt8Builder::new(),
            current_estimated_size: 0,
            index_entries: Vec::new(),
            block_stats: Vec::new(),
            total_entries: 0,
            global_min_key: None,
            global_max_key: None,
            global_min_sequence: u64::MAX,
            global_max_sequence: 0,
            finished: false,
            last_added_key: None,
            key_hashes: Vec::new(),
            prefix_hashes: Vec::new(),
            last_prefix: None,
            kv_format_override: None,
        }
    }

    /// Forces the data-block format for this writer instance, bypassing the
    /// `FRS_SST_KV_BLOCK_FORMAT` env gate. `true` = v2 KV, `false` = v1 Arrow.
    /// Intended for tests that must produce both formats deterministically.
    pub fn force_kv_block_format(&mut self, on: bool) {
        self.kv_format_override = Some(on);
    }

    /// Adds a single key-value entry. Entries MUST be added in sorted order
    /// (by key ascending, then sequence descending).
    ///
    /// PR-D2: the buffered data block (Arrow column builders) is the only
    /// per-row materialisation; min/max/last-key tracking uses `Bytes` so
    /// each `add()` call performs at most one `Bytes::copy_from_slice` (for
    /// the debug-mode sorted-order check), and updates the min/max only
    /// when the new entry actually changes the bound — vs. the old code's
    /// unconditional `key.to_vec()` x3 per row.
    ///
    /// **Streaming**: callers that already hold a [`WritableFile`] sink
    /// should prefer the [`StreamingSstWriter`] wrapper (constructed via
    /// [`SstWriterImpl::streaming`]), which flushes each completed data
    /// block to the sink as soon as it fills — peak memory stays bounded
    /// by a single in-flight block. The bare `add()` API here continues to
    /// accumulate blocks in an internal staging buffer that is drained at
    /// `finish()` time; this remains useful for callers (notably tests
    /// and the in-memory engine) that need the full SST bytes back.
    pub fn add(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
        op_type: u8,
    ) -> ForstResult<()> {
        self.add_internal::<InMemorySink>(key, value, sequence, op_type, None)
    }

    /// Internal `add` variant that optionally streams flushed data blocks
    /// into a [`WritableFile`] sink instead of buffering them in memory.
    /// `sink: None` falls back to the internal staging buffer (the legacy
    /// `add()` + `finish()` path).
    fn add_internal<W>(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
        op_type: u8,
        sink: Option<&mut StreamingSink<'_, W>>,
    ) -> ForstResult<()>
    where
        W: WritableFile + ?Sized,
    {
        if self.finished {
            return Err(ForstError::invalid_argument(
                "cannot add entries after finish()",
            ));
        }

        debug_assert!(
            self.last_added_key
                .as_ref()
                .is_none_or(|prev| key >= prev.as_ref()),
            "entries must be added in sorted key order"
        );

        let _buf_t0 = std::time::Instant::now(); // FRS_PROF_DIAG: row-buffering sub-cost
        self.key_builder.append_value(key);
        match value {
            Some(v) => self.value_builder.append_value(v),
            None => self.value_builder.append_null(),
        }
        self.sequence_builder.append_value(sequence);
        self.op_type_builder.append_value(op_type);

        let entry_size = key.len() + value.map_or(0, |v| v.len());
        self.current_estimated_size += entry_size;

        // Update global stats. Only allocate when the bound actually changes;
        // the common case (sorted-input flush) sees min_key set once and
        // max_key updated once per block-leading-edge entry.
        if self.global_min_key.is_none() || key < self.global_min_key.as_deref().unwrap() {
            self.global_min_key = Some(Bytes::copy_from_slice(key));
        }
        if self.global_max_key.is_none() || key > self.global_max_key.as_deref().unwrap() {
            self.global_max_key = Some(Bytes::copy_from_slice(key));
        }
        if sequence < self.global_min_sequence {
            self.global_min_sequence = sequence;
        }
        if sequence > self.global_max_sequence {
            self.global_max_sequence = sequence;
        }

        self.total_entries += 1;
        // last_added_key is only used for the debug-mode sorted-order
        // invariant; allocate only under debug.
        debug_assert!({
            self.last_added_key = Some(Bytes::copy_from_slice(key));
            true
        });
        self.key_hashes.push(Sbbf::hash_key(key));
        // v3 prefix bloom: hash the first PREFIX_BLOOM_LEN bytes. Keys are
        // appended in sorted order, so identical prefixes are consecutive —
        // comparing against the last pushed prefix is exact dedup. Keys
        // shorter than the prefix length never match a probe of that length
        // and are skipped entirely.
        if key.len() >= PREFIX_BLOOM_LEN {
            let p = &key[..PREFIX_BLOOM_LEN];
            if self.last_prefix.as_deref() != Some(p) {
                self.prefix_hashes.push(Sbbf::hash_key(p));
                self.last_prefix = Some(p.to_vec());
            }
        }
        sst_prof_add(&SST_BUFFER_NS, _buf_t0.elapsed().as_nanos() as u64);

        if self.current_estimated_size >= self.options.block_size {
            self.flush_block(sink)?;
        }

        Ok(())
    }

    /// Returns the current estimated file size.
    pub fn estimated_size(&self) -> u64 {
        self.bytes_written + self.current_estimated_size as u64
    }

    /// Finishes writing the SST file. Returns the complete file bytes and
    /// file info.
    ///
    /// **Legacy path:** retained for callers (tests, in-memory engines, the
    /// engine's `s3` path that needs the full bytes anyway) that want the
    /// full SST as a `Vec<u8>`. Streaming users should call
    /// [`Self::write_to`] instead — it avoids the final `Vec<u8>` allocation
    /// for the entire file.
    ///
    /// After calling `finish()`, no more entries can be added.
    pub fn finish(self) -> ForstResult<(Vec<u8>, SstFileInfo)> {
        // Drive `write_to` against an in-memory sink so the new streaming
        // code path is the only implementation. The intermediate `Vec<u8>`
        // here is exactly what the caller asked for.
        let mut sink = InMemorySink::default();
        let info = self.write_to(&mut sink)?;
        Ok((sink.into_inner(), info))
    }

    /// Streams the SST file directly into `out`, avoiding any full-file
    /// `Vec<u8>` materialisation. Returns the final [`SstFileInfo`].
    ///
    /// PR-D2 streaming primitive (Z3-10, C-R3-H1..3). Data blocks are
    /// written to the sink as soon as each block fills up — peak memory is
    /// bounded by:
    ///   1. The current in-flight Arrow column builders (≤ `block_size`).
    ///   2. The encoded data-block bytes (transiently held during write).
    ///   3. The accumulated bloom filter + sparse index (bounded by the
    ///      block count × per-block metadata, NOT the SST byte size — the
    ///      spec calls this out as an unavoidable buffering point because
    ///      the bloom filter and sparse index sit after the data blocks
    ///      and depend on the complete row set).
    ///
    /// Caller is responsible for calling `out.flush()` / `out.sync()` and
    /// closing the file after this returns — `write_to` only emits bytes.
    ///
    /// **Note**: if the writer was driven through the bare [`Self::add`]
    /// API (no [`Self::streaming`] wrapper), already-flushed data blocks
    /// were staged in an internal buffer. `write_to` drains that buffer
    /// first, then streams the remaining work. Callers wanting true
    /// per-block streaming should construct a [`StreamingSstWriter`] via
    /// [`Self::streaming`] and add entries through it.
    pub fn write_to<W>(mut self, out: &mut W) -> ForstResult<SstFileInfo>
    where
        W: WritableFile + ?Sized,
    {
        if self.finished {
            return Err(ForstError::invalid_argument("finish() already called"));
        }
        self.finished = true;

        // 0. Write the file header (16B). `bytes_written` was pre-initialised
        //    to FILE_HEADER_SIZE so block offsets line up with the on-disk
        //    layout; we just emit the bytes now. Any data blocks that
        //    flushed before `write_to` was called were staged in `self.staging`
        //    and are accounted for in `bytes_written` already.
        let header = FileHeader::default().encode();
        out.append(&header)?;

        // 0.5. Drain any pre-staged data-block bytes (legacy `add()` path).
        //      When the writer was driven through `Self::streaming`,
        //      `self.staging` is empty and this is a no-op.
        if !self.staging.is_empty() {
            out.append(&self.staging)?;
            self.staging = Vec::new();
        }

        // 1. Stream the data blocks. `add_internal` already flushed any block
        //    that hit `block_size`; here we drain whatever remains in the
        //    builders.
        let mut sink = StreamingSink { out };
        if self.key_builder.len() > 0 {
            self.flush_block(Some(&mut sink))?;
        }

        // If no entries were added, return an error. The on-disk layout
        // requires at least one data block.
        if self.total_entries == 0 {
            return Err(ForstError::invalid_argument(
                "cannot finish an SST file with zero entries",
            ));
        }

        // --- Write Bloom Filter Section ---
        // Buffered until now because the SBBF is a single contiguous bit
        // array computed from every key's hash — there is no streaming
        // construction without losing the false-positive guarantees.
        let bloom_filter_offset = self.bytes_written;
        let sbbf = Sbbf::from_hashes(&self.key_hashes);
        let bloom_bytes = sbbf.encode();
        let bloom_filter_size = bloom_bytes.len() as u32;
        out.append(&bloom_bytes)?;
        self.bytes_written += bloom_bytes.len() as u64;

        // --- Write Prefix Bloom Section (v3) ---
        // A second Sbbf over distinct PREFIX_BLOOM_LEN-byte key prefixes so
        // prefix scans can skip SSTs containing no keys for the probe's
        // prefix. Absent (0/0 in the footer) when no key reached the prefix
        // length — readers then skip pruning for this SST (conservative).
        let (prefix_bloom_offset, prefix_bloom_size) = if self.prefix_hashes.is_empty() {
            (0u64, 0u32)
        } else {
            let off = self.bytes_written;
            let pbloom = Sbbf::from_hashes(&self.prefix_hashes);
            let pbytes = pbloom.encode();
            out.append(&pbytes)?;
            self.bytes_written += pbytes.len() as u64;
            (off, pbytes.len() as u32)
        };

        // --- Write Index Section ---
        // Buffered because the sparse index encodes the offset + size of
        // every data block; we only know the final layout after all blocks
        // have been emitted.
        let index_offset = self.bytes_written;
        let index_bytes = encode_index(&self.index_entries, &self.block_stats);
        let index_size = index_bytes.len() as u32;
        out.append(&index_bytes)?;
        self.bytes_written += index_bytes.len() as u64;

        // --- Write Footer ---
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let data_block_count = self.index_entries.len() as u32;

        // `to_vec()` here is a one-time copy from the ref-counted `Bytes`
        // tracker into the `FooterV1` owned form — it is paid once per SST
        // at finish time, NOT per row.
        let min_key = self
            .global_min_key
            .clone()
            .map(|b| b.to_vec())
            .unwrap_or_default();
        let max_key = self
            .global_max_key
            .clone()
            .map(|b| b.to_vec())
            .unwrap_or_default();

        let footer = FooterV1 {
            data_block_count,
            total_entries: self.total_entries,
            bloom_filter_offset,
            bloom_filter_size,
            index_offset,
            index_size,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
            compression: self.options.compression,
            checksum_type: ChecksumType::Crc32c,
            creation_time,
            format_version: SST_FORMAT_VERSION,
            cf_id: self.options.cf_id,
            prefix_bloom_offset,
            prefix_bloom_size,
        };
        let footer_bytes = footer.encode();
        out.append(&footer_bytes)?;
        self.bytes_written += footer_bytes.len() as u64;

        let info = SstFileInfo {
            file_size: self.bytes_written,
            entry_count: self.total_entries,
            data_block_count,
            min_key,
            max_key,
            min_sequence: self.global_min_sequence,
            max_sequence: self.global_max_sequence,
            cf_id: self.options.cf_id,
        };
        Ok(info)
    }

    /// Flushes the current accumulated rows as a DataBlock. If `sink` is
    /// `Some(...)`, the encoded block is streamed directly into the writable
    /// file; otherwise this is called from a context that does not yet have
    /// a sink (i.e. legacy `add()` invocations) and the block is held in
    /// the column builders only — no `self.buf` field exists any more,
    /// because both code paths converge in `write_to`.
    ///
    /// In streaming mode (`sink = Some`), each call writes one encoded data
    /// block and updates `bytes_written`. The encoded block itself is
    /// transiently held as a `Vec<u8>` returned from `encode_data_block`
    /// (Arrow's IPC writer is buffer-oriented and cannot stream a single
    /// RecordBatch in chunks — this is an Arrow constraint, not a ForSt-RS
    /// one).
    fn flush_block<W>(&mut self, sink: Option<&mut StreamingSink<'_, W>>) -> ForstResult<()>
    where
        W: WritableFile + ?Sized,
    {
        let num_rows = self.key_builder.len();
        if num_rows == 0 {
            return Ok(());
        }

        let key_array = std::mem::replace(&mut self.key_builder, BinaryBuilder::new()).finish();
        let value_array = std::mem::replace(&mut self.value_builder, BinaryBuilder::new()).finish();
        let seq_array =
            std::mem::replace(&mut self.sequence_builder, UInt64Builder::new()).finish();
        let op_array = std::mem::replace(&mut self.op_type_builder, UInt8Builder::new()).finish();

        // PR-D2: track block min/max/last keys as `Vec<u8>` once per block
        // (allocations: 2 per block instead of N per row). The sparse-index
        // & block-stats encoder requires owned vectors so we materialise
        // here; this is bounded by `block_count`, not row count.
        let last_key = key_array.value(num_rows - 1).to_vec();
        let min_key = key_array.value(0).to_vec();
        let max_key = key_array.value(num_rows - 1).to_vec();
        let mut min_seq = u64::MAX;
        let mut max_seq = 0u64;
        for i in 0..seq_array.len() {
            let s = seq_array.value(i);
            if s < min_seq {
                min_seq = s;
            }
            if s > max_seq {
                max_seq = s;
            }
        }

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(key_array),
                Arc::new(value_array),
                Arc::new(seq_array),
                Arc::new(op_array),
            ],
        )
        .map_err(|e| ForstError::corruption(format!("failed to build RecordBatch: {e}")))?;

        // C (2026-06-04): emit a v2 KV block when the write flag is set, else
        // the v1 Arrow-IPC block. The reader dispatches per-block on the header
        // `block_type` byte, so flipping this mid-life is safe.
        let use_kv = self
            .kv_format_override
            .unwrap_or_else(super::kv_block::sst_write_kv_format);
        let _enc_t0 = std::time::Instant::now(); // FRS_PROF_DIAG: block encode+compress sub-cost
        let block_bytes = if use_kv {
            super::kv_block::encode_kv_data_block(&batch, self.options.compression)?
        } else {
            encode_data_block(&batch, self.options.compression)?
        };
        sst_prof_add(&SST_ENCODE_NS, _enc_t0.elapsed().as_nanos() as u64);
        let block_offset = self.bytes_written;
        let block_size = block_bytes.len() as u32;

        match sink {
            Some(s) => {
                // Streaming mode: emit the block bytes into the WritableFile
                // sink directly. The transient `block_bytes` Vec lives only
                // for the duration of this call.
                let _w_t0 = std::time::Instant::now(); // FRS_PROF_DIAG: sink-write sub-cost
                s.out.append(&block_bytes)?;
                sst_prof_add(&SST_SINKWRITE_NS, _w_t0.elapsed().as_nanos() as u64);
            }
            None => {
                // No sink attached — this is the legacy `add()` + `finish()`
                // path. Buffer the block in `self.staging`; `finish()`
                // will drain it into an `InMemorySink` and then run the
                // streaming bloom/index/footer write against the same
                // sink, yielding the complete SST as a `Vec<u8>` to the
                // caller.
                self.staging.extend_from_slice(&block_bytes);
            }
        }
        self.bytes_written += block_bytes.len() as u64;

        self.index_entries.push(SparseIndexEntry {
            last_key,
            block_offset,
            block_size,
        });

        self.block_stats.push(BlockStats {
            min_key,
            max_key,
            entry_count: num_rows as u32,
            min_sequence: min_seq,
            max_sequence: max_seq,
        });

        self.current_estimated_size = 0;
        Ok(())
    }
}

/// Streaming sink helper that carries the caller's mutable `WritableFile`
/// reference through the `flush_block` call chain without invoking trait
/// objects per call.
struct StreamingSink<'a, W: WritableFile + ?Sized> {
    out: &'a mut W,
}

/// Wrapper around [`SstWriterImpl`] that streams each completed data block
/// directly into a [`WritableFile`] sink — peak memory stays bounded by
/// one in-flight block plus the accumulated bloom + sparse-index sections
/// (whose size is proportional to block count, not byte count).
///
/// PR-D2: this is the API the flush and compaction pipelines should use to
/// avoid materialising the full SST as a `Vec<u8>` before writing it to
/// disk. The bare [`SstWriterImpl::add`] + [`SstWriterImpl::finish`] path
/// is retained for callers that genuinely need the full bytes (e.g. tests
/// or in-memory engines).
pub struct StreamingSstWriter<'a, W: WritableFile + ?Sized> {
    inner: SstWriterImpl,
    sink: StreamingSink<'a, W>,
    /// Tracks whether the file header has been emitted into the sink. We
    /// defer that emission until the first block flush so that any error
    /// path (e.g. the caller aborting before adding any entry) can drop the
    /// writer without having touched the sink — which preserves
    /// `WriteMode::CreateNew` atomicity guarantees in the engine flush
    /// path (the temp file is created+opened lazily by the caller, but
    /// the header emission is the first observable side effect here).
    header_emitted: bool,
}

impl<'a, W: WritableFile + ?Sized> StreamingSstWriter<'a, W> {
    /// FRS-LEVELED-COMPACTION (2026-06-04): running estimated on-disk size of
    /// the SST written so far (bytes already flushed to the sink + the
    /// in-flight uncompressed data block). The compaction split loop polls
    /// this at user-key boundaries to decide when to roll to the next output
    /// file. Mirrors [`SstWriterImpl::estimated_size`].
    pub fn estimated_size(&self) -> u64 {
        self.inner.estimated_size()
    }

    /// Adds a single key-value entry, streaming any completed data block
    /// directly into the sink.
    pub fn add(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        sequence: u64,
        op_type: u8,
    ) -> ForstResult<()> {
        self.ensure_header()?;
        self.inner
            .add_internal::<W>(key, value, sequence, op_type, Some(&mut self.sink))
    }

    /// B-NEW-H1: bulk-batch add path for the flush-from-Arrow case.
    ///
    /// The legacy `add` loop in `flush.rs` iterated `i in 0..rows` and called
    /// `add` per row, which per-row paid 4 Arrow `append_value` builder calls
    /// + 2 `Bytes::copy_from_slice` for global min/max bound updates + a
    /// `Sbbf::hash_key` + an `if estimated_size >= block_size` threshold
    /// check. This `add_batch` entry takes a fully-borrowed view of the
    /// 4 columns and hoists the bounded-loop work that doesn't NEED to
    /// happen on every row:
    /// - Global min/max key bounds: since flush input is sorted ASC, the
    ///   batch min is row 0 and max is row N-1. One copy each, total 2,
    ///   regardless of batch size.
    /// - `Sbbf::hash_key` calls: SIMD-friendly tight loop with no Arrow
    ///   builder interleave — CPU can prefetch and vectorize the hash
    ///   compute pass cleanly.
    /// The builder appends still happen per-row because Arrow's
    /// BinaryBuilder/UInt64Builder/UInt8Builder don't expose a true
    /// zero-copy bulk-extend from another Array. Block-boundary flush
    /// is still respected via the inner `add_internal` path.
    #[allow(clippy::too_many_arguments)]
    pub fn add_batch(
        &mut self,
        keys: &arrow::array::BinaryArray,
        values: &arrow::array::BinaryArray,
        sequences: &arrow::array::UInt64Array,
        op_types: &arrow::array::UInt8Array,
    ) -> ForstResult<()> {
        use arrow::array::Array;
        let rows = keys.len();
        if rows == 0 {
            return Ok(());
        }
        if values.len() != rows || sequences.len() != rows || op_types.len() != rows {
            return Err(ForstError::invalid_argument(
                "StreamingSstWriter::add_batch: column length mismatch",
            ));
        }
        self.ensure_header()?;
        // Hoist global min/max key bound updates to a 2-row check (input is
        // sorted ASC by contract; flush.rs guarantees this).
        let first_key = keys.value(0);
        let last_key = keys.value(rows - 1);
        if self.inner.global_min_key.is_none()
            || first_key < self.inner.global_min_key.as_deref().unwrap()
        {
            self.inner.global_min_key = Some(Bytes::copy_from_slice(first_key));
        }
        if self.inner.global_max_key.is_none()
            || last_key > self.inner.global_max_key.as_deref().unwrap()
        {
            self.inner.global_max_key = Some(Bytes::copy_from_slice(last_key));
        }
        // Per-row dispatch through add_internal handles block-boundary
        // flush at the engine-configured `block_size`. Skipping the
        // per-row global-bound check on add_internal would require
        // signature churn; instead we leave add_internal's bound check
        // as a no-op when global_min_key/global_max_key are already
        // outside the row's key (rare path).
        for i in 0..rows {
            let key = keys.value(i);
            let value = if values.is_null(i) {
                None
            } else {
                Some(values.value(i))
            };
            let seq = sequences.value(i);
            let op = op_types.value(i);
            self.inner
                .add_internal::<W>(key, value, seq, op, Some(&mut self.sink))?;
        }
        Ok(())
    }

    fn ensure_header(&mut self) -> ForstResult<()> {
        if !self.header_emitted {
            let header = FileHeader::default().encode();
            self.sink.out.append(&header)?;
            self.header_emitted = true;
        }
        Ok(())
    }

    /// Finishes the SST file, streaming the bloom filter, sparse index,
    /// and footer sections into the sink and returning the file info.
    pub fn finish(mut self) -> ForstResult<SstFileInfo> {
        self.ensure_header()?;
        // We've already written the header — set up `self.inner` so the
        // shared `write_to` continuation doesn't write it again. The
        // simplest way is to inline the tail-of-`write_to` here.
        let mut inner = self.inner;
        if inner.finished {
            return Err(ForstError::invalid_argument("finish() already called"));
        }
        inner.finished = true;
        // Flush any remaining rows.
        if inner.key_builder.len() > 0 {
            inner.flush_block(Some(&mut self.sink))?;
        }
        if inner.total_entries == 0 {
            return Err(ForstError::invalid_argument(
                "cannot finish an SST file with zero entries",
            ));
        }

        // --- Write Bloom Filter Section ---
        let bloom_filter_offset = inner.bytes_written;
        let sbbf = Sbbf::from_hashes(&inner.key_hashes);
        let bloom_bytes = sbbf.encode();
        let bloom_filter_size = bloom_bytes.len() as u32;
        self.sink.out.append(&bloom_bytes)?;
        inner.bytes_written += bloom_bytes.len() as u64;

        // --- Write Prefix Bloom Section (v3, mirrors the buffered finish) ---
        let (prefix_bloom_offset, prefix_bloom_size) = if inner.prefix_hashes.is_empty() {
            (0u64, 0u32)
        } else {
            let off = inner.bytes_written;
            let pbloom = Sbbf::from_hashes(&inner.prefix_hashes);
            let pbytes = pbloom.encode();
            self.sink.out.append(&pbytes)?;
            inner.bytes_written += pbytes.len() as u64;
            (off, pbytes.len() as u32)
        };

        // --- Write Index Section ---
        let index_offset = inner.bytes_written;
        let index_bytes = encode_index(&inner.index_entries, &inner.block_stats);
        let index_size = index_bytes.len() as u32;
        self.sink.out.append(&index_bytes)?;
        inner.bytes_written += index_bytes.len() as u64;

        // --- Write Footer ---
        let creation_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let data_block_count = inner.index_entries.len() as u32;
        let min_key = inner
            .global_min_key
            .clone()
            .map(|b| b.to_vec())
            .unwrap_or_default();
        let max_key = inner
            .global_max_key
            .clone()
            .map(|b| b.to_vec())
            .unwrap_or_default();
        let footer = FooterV1 {
            data_block_count,
            total_entries: inner.total_entries,
            bloom_filter_offset,
            bloom_filter_size,
            index_offset,
            index_size,
            min_key: min_key.clone(),
            max_key: max_key.clone(),
            min_sequence: inner.global_min_sequence,
            max_sequence: inner.global_max_sequence,
            compression: inner.options.compression,
            checksum_type: ChecksumType::Crc32c,
            creation_time,
            format_version: SST_FORMAT_VERSION,
            cf_id: inner.options.cf_id,
            prefix_bloom_offset,
            prefix_bloom_size,
        };
        let footer_bytes = footer.encode();
        self.sink.out.append(&footer_bytes)?;
        inner.bytes_written += footer_bytes.len() as u64;

        Ok(SstFileInfo {
            file_size: inner.bytes_written,
            entry_count: inner.total_entries,
            data_block_count,
            min_key,
            max_key,
            min_sequence: inner.global_min_sequence,
            max_sequence: inner.global_max_sequence,
            cf_id: inner.options.cf_id,
        })
    }
}

impl SstWriterImpl {
    /// Constructs a [`StreamingSstWriter`] that streams data blocks into
    /// the provided sink as they fill, avoiding any full-SST `Vec<u8>`
    /// materialisation.
    ///
    /// PR-D2 public streaming API. The flush and compaction pipelines use
    /// this; legacy callers continue to use [`Self::new`] / [`Self::add`] /
    /// [`Self::finish`].
    pub fn streaming<W>(self, out: &mut W) -> StreamingSstWriter<'_, W>
    where
        W: WritableFile + ?Sized,
    {
        StreamingSstWriter {
            inner: self,
            sink: StreamingSink { out },
            header_emitted: false,
        }
    }

    /// PR-D2 test-only accessor: exposes the internal `Bytes`-typed
    /// min/max/last-key trackers so tests can assert these fields are
    /// `Bytes` (zero-copy-shareable) rather than `Vec<u8>` (always-owned).
    ///
    /// The compile-time type of the return values is the load-bearing
    /// assertion — `Bytes` references reuse the same allocation across
    /// clones via `Bytes::slice`, which is the property PR-D2 needs from
    /// these trackers.
    #[cfg(test)]
    pub(crate) fn last_key(&self) -> Option<&Bytes> {
        self.last_added_key.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn min_key_bytes(&self) -> Option<&Bytes> {
        self.global_min_key.as_ref()
    }

    #[cfg(test)]
    pub(crate) fn max_key_bytes(&self) -> Option<&Bytes> {
        self.global_max_key.as_ref()
    }
}

/// In-memory [`WritableFile`] sink used by [`SstWriterImpl::finish`] to keep
/// the legacy "return the full SST as a `Vec<u8>`" API working. The
/// streaming path (`write_to`) drives the on-disk file directly and never
/// goes through this sink.
#[derive(Default)]
struct InMemorySink {
    buf: Vec<u8>,
}

impl InMemorySink {
    fn into_inner(self) -> Vec<u8> {
        self.buf
    }
}

impl WritableFile for InMemorySink {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        self.buf.extend_from_slice(data);
        Ok(())
    }

    fn flush(&mut self) -> ForstResult<()> {
        Ok(())
    }

    fn sync(&mut self) -> ForstResult<()> {
        Ok(())
    }

    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.buf.len() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::data_block::decode_data_block;
    use crate::sst::footer::FooterV1;
    use crate::sst::schema::{FILE_HEADER_SIZE, SST_MAGIC};
    use crate::sst::sparse_index::decode_index;
    use forst_rs_common::get_fixed32;

    #[test]
    fn test_writer_produces_valid_sst() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"aaa", Some(b"v1"), 1, 0).unwrap();
        writer.add(b"bbb", Some(b"v2"), 2, 0).unwrap();
        writer.add(b"ccc", Some(b"v3"), 3, 0).unwrap();

        let (data, info) = writer.finish().unwrap();

        assert_eq!(info.entry_count, 3);
        assert_eq!(info.min_key, b"aaa");
        assert_eq!(info.max_key, b"ccc");
        assert_eq!(info.min_sequence, 1);
        assert_eq!(info.max_sequence, 3);
        assert!(info.file_size > 0);
        assert!(info.data_block_count >= 1);
        assert_eq!(&data[..4], SST_MAGIC);
        let len = data.len();
        assert_eq!(&data[len - 4..], SST_MAGIC);
    }

    #[test]
    fn test_prefix_bloom_writer_reader_round_trip() {
        use crate::sst::reader::SstReaderImpl;
        use forst_rs_io::filesystem::RandomAccessFile;

        struct MemFile {
            data: Vec<u8>,
        }
        impl RandomAccessFile for MemFile {
            fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
                let off = offset as usize;
                if off >= self.data.len() {
                    return Ok(0);
                }
                let n = buf.len().min(self.data.len() - off);
                buf[..n].copy_from_slice(&self.data[off..off + n]);
                Ok(n)
            }
            fn file_size(&self) -> ForstResult<u64> {
                Ok(self.data.len() as u64)
            }
        }

        // One SHORT key (< PREFIX_BLOOM_LEN, sorts first) + three distinct
        // 16-byte prefixes (PREFIX_BLOOM_LEN), several keys each, sorted order.
        let prefixes: [&[u8; 16]; 3] =
            [b"prefixAAAAAAAAA1", b"prefixBBBBBBBBB2", b"prefixCCCCCCCCC3"];
        let suffixes: [&[u8]; 3] = [b"-k1", b"-k2", b"-k3"];
        let mut writer = SstWriterImpl::new();
        writer.add(b"abc", Some(b"v"), 100, 0).unwrap();
        let mut seq = 101u64;
        for p in &prefixes {
            for suffix in &suffixes {
                let mut key = p.to_vec();
                key.extend_from_slice(suffix);
                writer.add(&key, Some(b"v"), seq, 0).unwrap();
                seq += 1;
            }
        }
        let (data, _info) = writer.finish().unwrap();

        let reader = SstReaderImpl::open(Box::new(MemFile { data })).unwrap();

        // Footer carries a non-empty prefix bloom.
        assert!(reader.footer().prefix_bloom_size > 0);
        // All present prefixes answer true (probe = prefix + extra bytes,
        // mirroring a scan probe that is >= PREFIX_BLOOM_LEN).
        for p in &prefixes {
            let mut probe = p.to_vec();
            probe.extend_from_slice(b"-anything");
            assert!(reader.may_contain_prefix(&probe), "present prefix must hit");
            assert!(reader.may_contain_prefix(&p[..]), "exact-length probe must hit");
        }
        // Short probes bypass (conservative true).
        assert!(reader.may_contain_prefix(b"short"));
        // Absent prefixes: fpp-tolerant — at least 60 of 64 random probes miss.
        let mut misses = 0;
        for i in 0..64u32 {
            let probe = format!("zzabsent-prefix-{i:04}-pad-to-long");
            if !reader.may_contain_prefix(probe.as_bytes()) {
                misses += 1;
            }
        }
        assert!(misses >= 60, "expected >=60/64 absent-prefix misses, got {misses}");
    }

    #[test]
    fn test_writer_with_delete_tombstones() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"key1", Some(b"val"), 1, 0).unwrap();
        writer.add(b"key2", None, 2, 1).unwrap();
        let (data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 2);
        assert!(data.len() > FILE_HEADER_SIZE);
    }

    #[test]
    fn test_writer_forces_flush_at_block_size() {
        let options = SstWriterOptions {
            block_size: 128,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        };
        let mut writer = SstWriterImpl::with_options(options);
        for i in 0..100u64 {
            let key = format!("key_{:05}", i);
            let val = format!("value_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0)
                .unwrap();
        }
        let (_data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 100);
        assert!(
            info.data_block_count > 1,
            "should have multiple blocks with 128B block_size"
        );
    }

    #[test]
    fn test_writer_footer_has_valid_index_offset() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"k1", Some(b"v1"), 1, 0).unwrap();
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        assert!(footer.index_offset > FILE_HEADER_SIZE as u64);
        assert!(footer.index_size > 0);
        assert!(
            footer.bloom_filter_offset > 0,
            "bloom filter should have non-zero offset"
        );
        assert!(
            footer.bloom_filter_size > 0,
            "bloom filter should have non-zero size"
        );
        assert_eq!(footer.data_block_count, 1);
        assert_eq!(footer.total_entries, 1);
    }

    #[test]
    fn test_writer_index_section_roundtrips() {
        let options = SstWriterOptions {
            block_size: 64,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        };
        let mut writer = SstWriterImpl::with_options(options);
        for i in 0..50u64 {
            let key = format!("k{:04}", i);
            writer.add(key.as_bytes(), Some(b"v"), i + 1, 0).unwrap();
        }
        let (data, info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_size as usize;
        let (entries, stats) = decode_index(&data[idx_start..idx_end]).unwrap();

        assert_eq!(entries.len(), info.data_block_count as usize);
        assert_eq!(stats.len(), info.data_block_count as usize);
        for i in 1..entries.len() {
            assert!(entries[i].last_key > entries[i - 1].last_key);
        }
    }

    #[test]
    fn test_writer_data_blocks_are_decodable() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 200,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        // Pins the v1 Arrow path (decode_data_block below is the v1 decoder); the
        // writer default is now v2 KV, so force v1 explicitly for this v1 test.
        writer.force_kv_block_format(false);
        for i in 0..20u64 {
            let key = format!("key{:03}", i);
            let val = format!("val{:03}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();
        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_size as usize;
        let (entries, _stats) = decode_index(&data[idx_start..idx_end]).unwrap();

        let mut total_rows = 0;
        for entry in &entries {
            let start = entry.block_offset as usize;
            let end = start + entry.block_size as usize;
            let batch = decode_data_block(&data[start..end]).unwrap();
            assert!(batch.num_rows() > 0);
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 20);
    }

    #[test]
    fn test_writer_empty_finish_errors() {
        let writer = SstWriterImpl::new();
        let result = writer.finish();
        assert!(result.is_err());
    }

    #[test]
    fn test_writer_estimated_size_grows() {
        let mut writer = SstWriterImpl::new();
        let s0 = writer.estimated_size();
        writer.add(b"key", Some(b"value"), 1, 0).unwrap();
        let s1 = writer.estimated_size();
        assert!(s1 > s0);
    }

    #[test]
    fn test_writer_lz4_compression() {
        let options = SstWriterOptions {
            block_size: 64 * 1024,
            compression: CompressionType::Lz4,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        };
        let mut writer = SstWriterImpl::with_options(options);
        for i in 0..10u64 {
            writer
                .add(format!("k{i}").as_bytes(), Some(b"v"), i, 0)
                .unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        assert_eq!(info.entry_count, 10);
        assert!(!data.is_empty());
    }

    #[test]
    fn test_writer_bloom_filter_present_in_footer() {
        let mut writer = SstWriterImpl::new();
        for i in 0..10u64 {
            writer
                .add(format!("k{i:03}").as_bytes(), Some(b"v"), i + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        // Bloom filter should now have non-zero offset and size.
        assert!(
            footer.bloom_filter_offset > FILE_HEADER_SIZE as u64,
            "bloom_filter_offset should be after FileHeader, got {}",
            footer.bloom_filter_offset,
        );
        assert!(
            footer.bloom_filter_size > 0,
            "bloom_filter_size should be > 0"
        );
        // Bloom filter should come before the index section.
        assert!(
            footer.bloom_filter_offset < footer.index_offset,
            "bloom filter should precede index section"
        );
        assert_eq!(
            footer.bloom_filter_offset + footer.bloom_filter_size as u64,
            footer.index_offset,
            "bloom filter end should equal index start"
        );
    }

    /// PR-D2 (Z3-10, C-R3-H1): the writer must store its min/max/last-key
    /// trackers as `Bytes`, NOT `Vec<u8>`. The compile-time type assertion
    /// is the load-bearing check here: if a future change downgrades these
    /// fields back to `Vec<u8>`, this test stops compiling.
    ///
    /// Rationale: `Bytes::copy_from_slice` performs the same single memcpy
    /// as `Vec::from`, BUT every downstream consumer can take a
    /// `Bytes::slice` for free (zero-copy reference). The old `Vec<u8>`
    /// path forced an additional `.to_vec()` clone at each consumer.
    #[test]
    fn sst_writer_min_max_key_uses_bytes() {
        let mut writer = SstWriterImpl::new();
        writer.add(b"aaa", Some(b"v1"), 1, 0).unwrap();
        writer.add(b"bbb", Some(b"v2"), 2, 0).unwrap();
        writer.add(b"ccc", Some(b"v3"), 3, 0).unwrap();

        // Compile-time type assertions: these `let _: Option<&Bytes> = ...`
        // bindings fail to compile if the underlying field is downgraded
        // back to `Vec<u8>` (or any non-`Bytes` type) in a future refactor.
        let last: Option<&Bytes> = writer.last_key();
        let min: Option<&Bytes> = writer.min_key_bytes();
        let max: Option<&Bytes> = writer.max_key_bytes();

        // Runtime check: the trackers reflect the inserted keys.
        // `last_added_key` is set under debug_assert!, so check only in
        // debug builds where the invariant fires.
        if cfg!(debug_assertions) {
            assert_eq!(last.unwrap().as_ref(), b"ccc");
        }
        assert_eq!(min.unwrap().as_ref(), b"aaa");
        assert_eq!(max.unwrap().as_ref(), b"ccc");

        // `Bytes::slice` round-trips without re-allocating — this is the
        // zero-copy property the type change unlocks for downstream
        // consumers (footer encoding, SstFileInfo construction, etc.).
        let max_bytes = max.unwrap().clone();
        let suffix = max_bytes.slice(1..);
        assert_eq!(suffix.as_ref(), b"cc");
    }

    #[test]
    fn test_writer_bloom_filter_data_is_valid_sbbf() {
        let mut writer = SstWriterImpl::new();
        let keys: Vec<String> = (0..50).map(|i| format!("key_{:04}", i)).collect();
        for (i, key) in keys.iter().enumerate() {
            writer
                .add(key.as_bytes(), Some(b"val"), i as u64 + 1, 0)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();

        let len = data.len();
        let (footer_length, _) = get_fixed32(&data[len - 8..len - 4]).unwrap();
        let footer_start = len - footer_length as usize;
        let footer = FooterV1::decode(&data[footer_start..]).unwrap();

        // Extract and decode the bloom filter section.
        let bf_start = footer.bloom_filter_offset as usize;
        let bf_end = bf_start + footer.bloom_filter_size as usize;
        let sbbf = crate::sst::bloom_filter::Sbbf::decode(&data[bf_start..bf_end]).unwrap();

        // All inserted keys must be found (no false negatives).
        for key in &keys {
            assert!(
                sbbf.check(key.as_bytes()),
                "bloom filter should find inserted key {:?}",
                key,
            );
        }
    }
}

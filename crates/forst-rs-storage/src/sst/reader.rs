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

//! SST file reader for point lookups.
//!
//! [`SstReaderImpl`] opens a completed SST file (produced by [`crate::sst::writer::SstWriterImpl`])
//! via a [`RandomAccessFile`] handle and serves point lookups through the
//! following algorithm:
//!
//! 1. Key-range check against footer min/max keys
//! 2. Bloom filter (SBBF) negative lookup
//! 3. Sparse index binary search to locate candidate DataBlock
//! 4. Read and decode the DataBlock (Arrow RecordBatch)
//! 5. Binary search within the RecordBatch for the target key
//! 6. Return the latest version (highest sequence) with op_type awareness

use arrow::array::{Array, BinaryArray, RecordBatch, UInt64Array, UInt8Array};
use forst_rs_common::{get_fixed32, ForstError, ForstResult, OpType};
use forst_rs_io::filesystem::RandomAccessFile;

use std::sync::Arc;

use crate::cache::{BlockCache, CacheEntry, CacheKey, CachePriority};

use super::bloom_filter::Sbbf;
use super::data_block::decode_data_block_zerocopy;
use super::footer::{FooterV1, FOOTER_TAIL_SIZE};
use super::kv_block::{KvBlock, RowRanges, SliceRef};
use super::schema::{
    BLOCK_TYPE_DATA, BLOCK_TYPE_DATA_KV, FILE_HEADER_SIZE, PREFIX_BLOOM_LEN, SST_MAGIC,
};
use super::sparse_index::{decode_index, search_index, BlockStats, SparseIndexEntry};

/// A decoded SST data block — either a v1 Arrow `RecordBatch` or a v2 KV block
/// (C / [`KvBlock`]). The reader dispatches on the block-header `block_type`
/// byte and returns this so the read paths (point `get`, `get_versions`, scan)
/// need not know the on-disk format.
pub enum DecodedBlock {
    /// v1 Arrow-IPC `RecordBatch` (`block_type = BLOCK_TYPE_DATA`).
    Arrow(RecordBatch),
    /// v2 KV block (`block_type = BLOCK_TYPE_DATA_KV`), shared via `Arc` with
    /// the decoded-block cache.
    Kv(Arc<KvBlock>),
}

impl DecodedBlock {
    /// Invokes `cb` once per row, in on-disk `(key ASC, sequence DESC)` order,
    /// dispatching to the format-specific row walker.
    pub fn for_each_row<F>(&self, cb: F) -> ForstResult<()>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        match self {
            DecodedBlock::Arrow(batch) => for_each_row_in_batch(batch, cb),
            DecodedBlock::Kv(kv) => kv.for_each_row(cb),
        }
    }

    /// S2-1: range-exposing twin of [`Self::for_each_row`] — yields each row
    /// as offsets ([`RowRanges`]) instead of borrowed slices, so the caller
    /// can capture row positions and resolve the bytes later against this
    /// (pinned) block plus the caller-owned `arena`.
    ///
    /// Format split (D2, locked): v2 KV blocks append every reconstructed key
    /// into `arena` ([`SliceRef::Arena`]) and reference values in the payload
    /// ([`SliceRef::Block`]); v1 Arrow blocks reference BOTH key and value in
    /// the batch's stable `BinaryArray` buffers (`Block` refs; `arena` is
    /// passed through untouched). Resolve `Block` refs against
    /// [`Self::key_value_buffers`].
    pub fn for_each_row_ranges<F>(&self, arena: &mut Vec<u8>, mut cb: F) -> ForstResult<()>
    where
        F: FnMut(&[u8], RowRanges) -> ForstResult<()>,
    {
        match self {
            DecodedBlock::Arrow(batch) => {
                for_each_row_ranges_in_batch(batch, |row| cb(arena.as_slice(), row))
            }
            DecodedBlock::Kv(kv) => kv.for_each_row_ranges(arena, cb),
        }
    }

    /// S2-3: seek-aware, early-stopping twin of [`Self::for_each_row_ranges`]
    /// — yields only rows with key `>= lower`; the callback returns
    /// `Ok(false)` to stop the walk (upper-bound early termination). See
    /// `KvBlock::for_each_row_ranges_from` for the rationale (bounds the
    /// per-block walk to the probed window instead of the whole block).
    pub fn for_each_row_ranges_from<F>(
        &self,
        lower: &[u8],
        arena: &mut Vec<u8>,
        mut cb: F,
    ) -> ForstResult<()>
    where
        F: FnMut(&[u8], RowRanges) -> ForstResult<bool>,
    {
        match self {
            DecodedBlock::Arrow(batch) => {
                for_each_row_ranges_in_batch_from(batch, lower, |row| cb(arena.as_slice(), row))
            }
            DecodedBlock::Kv(kv) => kv.for_each_row_ranges_from(lower, arena, cb),
        }
    }

    /// S2-1: the stable `(key_buffer, value_buffer)` pair that
    /// [`SliceRef::Block`] refs from [`Self::for_each_row_ranges`] resolve
    /// against — `key` field refs index the first slice, `value` field refs
    /// the second. For v2 KV blocks both are the decompressed payload; for v1
    /// Arrow blocks they are the key/value columns' `BinaryArray` values
    /// buffers. Stable for this block's lifetime.
    pub fn key_value_buffers(&self) -> ForstResult<(&[u8], &[u8])> {
        match self {
            DecodedBlock::Arrow(batch) => batch_key_value_data(batch),
            DecodedBlock::Kv(kv) => {
                let p = kv.payload_bytes();
                Ok((p, p))
            }
        }
    }
}

/// A single row produced by [`SstReaderImpl::scan`]:
/// `(key, value, sequence, op_type)`.
pub type SstScanRow = (Vec<u8>, Option<Vec<u8>>, u64, OpType);

/// Zero-copy borrowed view of a single SST row.
///
/// The `key` and `value` slices borrow directly from an Arrow `BinaryArray`
/// backing buffer (which itself wraps the bytes read from the SST data block).
/// No `Vec<u8>` is allocated per row. The lifetime `'a` ties the view to the
/// `RecordBatch` that owns the underlying Arrow buffer; callers must consume
/// the view before the batch is dropped, or convert to owned bytes themselves
/// via `key.to_vec()` / `value.map(|v| v.to_vec())`.
///
/// PR-D2 (Z3-10, Z3-11, C-R3-H1..3): replaces per-row `Vec<u8>` materialization
/// in the SST scan/get hot path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowView<'a> {
    /// Key bytes, borrowed from the Arrow key column.
    pub key: &'a [u8],
    /// Value bytes, `None` if this row is a delete tombstone.
    pub value: Option<&'a [u8]>,
    /// Sequence number.
    pub sequence: u64,
    /// Operation type.
    pub op_type: OpType,
}

/// Result of a point lookup: the value bytes and operation type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupResult {
    /// The value bytes. `None` if the operation is a Delete/SingleDelete.
    pub value: Option<Vec<u8>>,
    /// The sequence number of this entry.
    pub sequence: u64,
    /// The operation type.
    pub op_type: OpType,
}

/// Binary-searches a RecordBatch's key column for `target_key`.
///
/// Returns the index of the first row where key == target_key,
/// or `None` if the key is not present. The RecordBatch keys must
/// be sorted in ascending order.
///
/// R39-M3: a crafted or corrupt SST whose schema doesn't match the
/// writer-side contract surfaces as a corruption error rather than a
/// panic — matches the `ok_or_else(corruption)` pattern used in
/// [`SstReaderImpl::get`] downstream of this call (sister fix to
/// R38-M1).
fn search_key_in_batch(batch: &RecordBatch, target_key: &[u8]) -> ForstResult<Option<usize>> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;

    let num_rows = keys.len();
    if num_rows == 0 {
        return Ok(None);
    }

    // Binary search for the first row where key >= target_key.
    let mut lo = 0usize;
    let mut hi = num_rows;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if keys.value(mid) < target_key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if lo < num_rows && keys.value(lo) == target_key {
        Ok(Some(lo))
    } else {
        Ok(None)
    }
}

/// An SST file reader that serves point lookups.
///
/// Created via [`SstReaderImpl::open`], which reads the footer, bloom filter,
/// and sparse index into memory. DataBlocks are read on-demand during `get()`.
pub struct SstReaderImpl {
    file: Box<dyn RandomAccessFile>,
    /// Cached at `open()` to avoid per-`read_data_block` syscall on the
    /// hot lookup path. Used to bound-check untrusted `block_size` values
    /// from the sparse index before allocating the read buffer (Sweep
    /// R3 H by Reviewers 2 + 5).
    file_size: u64,
    footer: FooterV1,
    index_entries: Vec<SparseIndexEntry>,
    #[allow(dead_code)] // Stored for future range-scan and compaction support.
    index_stats: Vec<BlockStats>,
    bloom_filter: Sbbf,
    /// v3 prefix bloom over PREFIX_BLOOM_LEN-byte key prefixes; `None` for
    /// pre-v3 SSTs or SSTs with no key reaching the prefix length. See
    /// [`Self::may_contain_prefix`].
    prefix_bloom: Option<Sbbf>,
    /// 2026-05-30 DECODED-BLOCK CACHE: optional shared L1 cache of DECODED data
    /// blocks (`CacheEntry::DecodedBatch`), keyed by `(file_number, block_offset)`.
    /// `read_data_block` checks it before reading+decompressing — eliminating the
    /// repeated `serial_read_at` + `decode_data_block` + decompress cost that the
    /// q9 prefix-iterator profile showed dominating (each join probe re-read and
    /// re-decoded the same SST blocks). SSTs are write-once with globally-unique
    /// file numbers, so cached decoded blocks are immutable — no invalidation
    /// needed. `None` (the bare `open`) keeps the un-cached behavior for tests /
    /// callers that do not pass a cache.
    block_cache: Option<Arc<dyn BlockCache>>,
    /// `db_id`-qualified file identity used as the cache key's file-number
    /// component: `(db_id << 40) | (file_number & ((1<<40)-1))`. Qualifying by
    /// `db_id` is REQUIRED because the block cache is now process-shared across
    /// all DB instances — two instances each assign file number `1`, so an
    /// un-qualified key would collide and serve one DB's block to another
    /// (silent cross-DB corruption). `db_id < 2^24` (few instances) and
    /// `file_number < 2^40` (≤ 1 T SSTs) in any real deployment, so the packed
    /// id is collision-free. `0` when no cache is wired.
    cache_file_id: u64,
    /// 2026-06-02: count of `read_block_at` calls served by this reader. Used by
    /// diagnostics and by the prefix-scan upper-bound-early-termination test to
    /// prove an empty/tail prefix scan does NOT read the whole SST tail. Relaxed
    /// — a monotone counter with no cross-thread ordering requirement.
    blocks_read: std::sync::atomic::AtomicU64,
}

// R74-H1: the read-fully helper (a `read_at` loop that requires all `buf.len()` bytes and
// treats a mid-file short read as `Corruption`) is documented at its own definition below.
fn read_at_exact(file: &dyn RandomAccessFile, offset: u64, buf: &mut [u8]) -> ForstResult<()> {
    let mut filled: usize = 0;
    while filled < buf.len() {
        let n = file.read_at(offset + filled as u64, &mut buf[filled..])?;
        if n == 0 {
            return Err(ForstError::corruption(format!(
                "SST read_at_exact: short read at offset {} (filled {} of {})",
                offset,
                filled,
                buf.len()
            )));
        }
        filled = filled.saturating_add(n);
    }
    Ok(())
}

impl SstReaderImpl {
    /// Opens an SST file for reading.
    ///
    /// Reads the footer (from the file tail), then loads the bloom filter
    /// and sparse index sections into memory.
    pub fn open(file: Box<dyn RandomAccessFile>) -> ForstResult<Self> {
        let file_size = file.file_size()?;
        if file_size < (FILE_HEADER_SIZE + FOOTER_TAIL_SIZE) as u64 {
            return Err(ForstError::corruption(format!(
                "SST file too small: {} bytes",
                file_size
            )));
        }

        // Step 1: Read the last 8 bytes to get footer_length + magic.
        let mut tail = [0u8; 8];
        read_at_exact(file.as_ref(), file_size - 8, &mut tail)?;
        if &tail[4..8] != SST_MAGIC {
            return Err(ForstError::corruption("SST file missing trailing magic"));
        }
        let (footer_length, _) = get_fixed32(&tail[0..4])?;

        // Step 2: Read and decode footer.
        // SECURITY: bound `footer_length` against `file_size` before allocation.
        // Untrusted input from a crafted SST could otherwise drive
        // `vec![0u8; footer_length as usize]` to allocate up to ~4 GiB and OOM
        // the process (Sweep R2 H by Reviewer 5).
        let footer_start = file_size.checked_sub(footer_length as u64).ok_or_else(|| {
            ForstError::corruption(format!(
                "SST footer_length {} exceeds file_size {}",
                footer_length, file_size
            ))
        })?;
        // Also guard against footer_length == 0 (would zero-size alloc + decode fail later
        // anyway, but explicit rejection produces a clearer error).
        if footer_length == 0 {
            return Err(ForstError::corruption("SST footer_length is zero"));
        }
        let mut footer_buf = vec![0u8; footer_length as usize];
        read_at_exact(file.as_ref(), footer_start, &mut footer_buf)?;
        let footer = FooterV1::decode(&footer_buf)?;

        // Step 3: Read and decode bloom filter.
        // SECURITY: validate bloom_filter_offset + bloom_filter_size fit within
        // file_size to prevent OOM from a crafted footer.
        let bloom_end = (footer.bloom_filter_offset)
            .checked_add(footer.bloom_filter_size as u64)
            .ok_or_else(|| ForstError::corruption("SST bloom_filter offset+size overflow"))?;
        if bloom_end > file_size {
            return Err(ForstError::corruption(format!(
                "SST bloom_filter range [{}, {}) exceeds file_size {}",
                footer.bloom_filter_offset, bloom_end, file_size
            )));
        }
        let mut bloom_buf = vec![0u8; footer.bloom_filter_size as usize];
        read_at_exact(file.as_ref(), footer.bloom_filter_offset, &mut bloom_buf)?;
        let bloom_filter = Sbbf::decode(&bloom_buf)?;

        // Step 4: Read and decode sparse index.
        // SECURITY: same bounds check as bloom filter range.
        let index_end = (footer.index_offset)
            .checked_add(footer.index_size as u64)
            .ok_or_else(|| ForstError::corruption("SST index offset+size overflow"))?;
        if index_end > file_size {
            return Err(ForstError::corruption(format!(
                "SST index range [{}, {}) exceeds file_size {}",
                footer.index_offset, index_end, file_size
            )));
        }
        let mut index_buf = vec![0u8; footer.index_size as usize];
        read_at_exact(file.as_ref(), footer.index_offset, &mut index_buf)?;
        let (index_entries, index_stats) = decode_index(&index_buf)?;

        // Step 5 (v3): read and decode the PREFIX bloom, if present. v1/v2
        // SSTs (and v3 SSTs whose keys never reached PREFIX_BLOOM_LEN) carry
        // 0/0 → `None` → prefix pruning is skipped for this SST.
        let prefix_bloom = if footer.prefix_bloom_size > 0 {
            let pb_end = (footer.prefix_bloom_offset)
                .checked_add(footer.prefix_bloom_size as u64)
                .ok_or_else(|| ForstError::corruption("SST prefix_bloom offset+size overflow"))?;
            if pb_end > file_size {
                return Err(ForstError::corruption(format!(
                    "SST prefix_bloom range [{}, {}) exceeds file_size {}",
                    footer.prefix_bloom_offset, pb_end, file_size
                )));
            }
            let mut pb_buf = vec![0u8; footer.prefix_bloom_size as usize];
            read_at_exact(file.as_ref(), footer.prefix_bloom_offset, &mut pb_buf)?;
            Some(Sbbf::decode(&pb_buf)?)
        } else {
            None
        };

        Ok(Self {
            file,
            file_size,
            footer,
            index_entries,
            index_stats,
            bloom_filter,
            prefix_bloom,
            block_cache: None,
            cache_file_id: 0,
            blocks_read: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// v3 prefix bloom: returns `false` only when this SST PROVABLY contains
    /// no key whose first [`PREFIX_BLOOM_LEN`] bytes equal the probe's. Probes
    /// shorter than the filter length — and SSTs without a prefix bloom
    /// (pre-v3, or no key reached the length) — return `true` (conservative).
    /// The filter is built over ALL entries including tombstones and merges,
    /// so a `false` answer is safe for MVCC scans at any sequence.
    pub fn may_contain_prefix(&self, probe_prefix: &[u8]) -> bool {
        match &self.prefix_bloom {
            Some(pb) if probe_prefix.len() >= PREFIX_BLOOM_LEN => {
                pb.check_hash(Sbbf::hash_key(&probe_prefix[..PREFIX_BLOOM_LEN]))
            }
            _ => true,
        }
    }

    /// 2026-05-30 DECODED-BLOCK CACHE: attach the process-shared decoded-block
    /// cache and the file's `db_id`-qualified identity (for the cache key).
    /// Returns `self` for chaining at the `get_or_open_sst_reader` call site.
    /// Without this, `read_data_block` reads + decompresses on every call (the
    /// profiled prefix-iterator hot path). `db_id` qualification keeps the shared
    /// cache collision-free across DB instances (see `cache_file_id`).
    pub fn with_block_cache(
        mut self,
        cache: Arc<dyn BlockCache>,
        db_id: u64,
        file_number: u64,
    ) -> Self {
        const FILE_BITS: u32 = 40;
        let qualified = (db_id << FILE_BITS) | (file_number & ((1u64 << FILE_BITS) - 1));
        self.block_cache = Some(cache);
        self.cache_file_id = qualified;
        self
    }

    /// Returns a reference to the parsed footer.
    pub fn footer(&self) -> &FooterV1 {
        &self.footer
    }

    /// Returns a reference to the bloom filter.
    pub fn bloom_filter(&self) -> &Sbbf {
        &self.bloom_filter
    }

    /// Returns the number of data blocks in this SST file.
    pub fn data_block_count(&self) -> usize {
        self.index_entries.len()
    }

    /// Returns the total number of entries as recorded in the footer.
    pub fn total_entries(&self) -> u64 {
        self.footer.total_entries
    }

    /// Reads and decodes a DataBlock at the given offset and size.
    ///
    /// SECURITY: validates `block_offset + block_size` fits within
    /// `self.file_size` before allocating the read buffer. This prevents
    /// OOM-DoS from a crafted sparse index claiming an oversized
    /// `block_size` (Sweep R3 H by Reviewers 2 + 5). The `file_size`
    /// is cached at `open()` so this check costs no syscall on the
    /// hot lookup path.
    ///
    /// §2.1: `pub(crate)` so [`crate::sst::prefetch::BlockPrefetcher`] can use
    /// it as its DEMAND (cold-state) fetch — byte-identical to today's
    /// demand-paged path, including the cache-first check and the
    /// `CachePriority::Low` insert.
    pub(crate) fn read_decoded_block(
        &self,
        block_offset: u64,
        block_size: u32,
    ) -> ForstResult<DecodedBlock> {
        // 2026-05-30 DECODED-BLOCK CACHE: serve a decoded block from the shared
        // L1 cache when present — skips the `serial_read_at` + decompress +
        // decode chain that dominated the q9 prefix-iterator profile. Cloning a
        // `RecordBatch` (Arc-shared column buffers) / an `Arc<KvBlock>` is cheap.
        let cache_key = self
            .block_cache
            .as_ref()
            .map(|_| CacheKey::new(self.cache_file_id, block_offset));
        if let (Some(cache), Some(key)) = (self.block_cache.as_ref(), cache_key) {
            if let Some(entry) = cache.get(&key) {
                match entry.as_ref() {
                    CacheEntry::DecodedBatch(batch) => {
                        return Ok(DecodedBlock::Arrow((**batch).clone()))
                    }
                    CacheEntry::DecodedKv(kv) => return Ok(DecodedBlock::Kv(Arc::clone(kv))),
                    CacheEntry::RawBlock(_) => {}
                }
            }
        }

        let block_end = block_offset
            .checked_add(block_size as u64)
            .ok_or_else(|| ForstError::corruption("SST data block offset+size overflow"))?;
        if block_end > self.file_size {
            return Err(ForstError::corruption(format!(
                "SST data block range [{}, {}) exceeds file_size {}",
                block_offset, block_end, self.file_size
            )));
        }
        // FRS-ZEROCOPY (2026-06-02) + A-orthogonal buffer reuse (2026-06-04):
        // read the raw block into a REUSED thread-local scratch (avoids the
        // per-read `vec![0u8;blk]` alloc + zero-fill — measured ~85-170 ns/read,
        // policy-clean, no unsafe, independent of A/mmap). The first header byte
        // is the `block_type` discriminant — dispatch v1 Arrow vs v2 KV.
        //   * v2 KV: `KvBlock::decode` COPIES the payload out (decompress returns
        //     an owned Vec even for None), so the scratch is transient and reused
        //     across calls — this is where the win lands (q4 is all-KV).
        //   * v1 Arrow: `decode_data_block_zerocopy` slices a `Buffer` that must
        //     OWN the bytes, so we `mem::take` the scratch (it becomes empty →
        //     the next read reallocates). Identical cost to the old fresh-vec
        //     path — NO extra copy, NO regression for v1.
        // SST blocks are immutable, so cached entries never go stale.
        let verify = super::data_block::sst_read_verify_checksum();
        let bs = block_size as usize;

        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }

        let decoded = SCRATCH.with(|cell| -> ForstResult<DecodedBlock> {
            let mut scratch = cell.borrow_mut();
            // No-op (no realloc, no zero-fill) once warmed to a uniform block
            // size; grows/shrinks only when the block size changes.
            scratch.resize(bs, 0);
            read_at_exact(self.file.as_ref(), block_offset, &mut scratch[..bs])?;
            match scratch[0] {
                BLOCK_TYPE_DATA => {
                    // Hand ownership to the zero-copy Arrow Buffer; scratch is
                    // left empty (reallocated on the next read).
                    let raw = std::mem::take(&mut *scratch);
                    let block_buf = arrow::buffer::Buffer::from_vec(raw);
                    Ok(DecodedBlock::Arrow(decode_data_block_zerocopy(
                        &block_buf, verify,
                    )?))
                }
                BLOCK_TYPE_DATA_KV => {
                    // decode copies the payload out → scratch stays reusable.
                    Ok(DecodedBlock::Kv(Arc::new(KvBlock::decode(
                        &scratch[..bs],
                        verify,
                    )?)))
                }
                other => Err(ForstError::corruption(format!(
                    "unknown SST data block_type 0x{other:02X}"
                ))),
            }
        })?;

        // Populate the decoded-block cache (best-effort) per variant.
        if let (Some(cache), Some(key)) = (self.block_cache.as_ref(), cache_key) {
            match &decoded {
                DecodedBlock::Arrow(batch) => {
                    let arc = Arc::new(batch.clone());
                    let charge = CacheEntry::DecodedBatch(Arc::clone(&arc)).charge();
                    cache.insert(
                        key,
                        CacheEntry::DecodedBatch(arc),
                        charge,
                        CachePriority::Low,
                    );
                }
                DecodedBlock::Kv(kv) => {
                    let charge = CacheEntry::DecodedKv(Arc::clone(kv)).charge();
                    cache.insert(
                        key,
                        CacheEntry::DecodedKv(Arc::clone(kv)),
                        charge,
                        CachePriority::Low,
                    );
                }
            }
        }
        Ok(decoded)
    }

    /// Performs a point lookup for `key`.
    ///
    /// Returns the [`LookupResult`] for the latest version of the key
    /// (highest sequence number), or `None` if the key is not in this SST file.
    ///
    /// The lookup algorithm:
    /// 1. Key-range check: if key < min_key or key > max_key, return None
    /// 2. Bloom filter: if SBBF says "definitely not present", return None
    /// 3. Sparse index: binary search to find candidate DataBlock
    /// 4. Read and decode the DataBlock
    /// 5. Binary search within the RecordBatch for the key
    /// 6. Among matching rows, pick the one with the highest sequence number
    /// 7. If op_type is Delete/SingleDelete, return LookupResult with value=None
    pub fn get(&self, key: &[u8]) -> ForstResult<Option<LookupResult>> {
        // 1. Key-range check.
        if key < self.footer.min_key.as_slice() || key > self.footer.max_key.as_slice() {
            return Ok(None);
        }

        // 2. Bloom filter check.
        if !self.bloom_filter.check(key) {
            return Ok(None);
        }

        // 3. Sparse index binary search.
        let block_idx = match search_index(&self.index_entries, key) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // 4. Read and decode the DataBlock (v1 Arrow or v2 KV).
        let entry = &self.index_entries[block_idx];
        match self.read_decoded_block(entry.block_offset, entry.block_size)? {
            DecodedBlock::Arrow(batch) => {
                // 5. Binary search within the RecordBatch.
                let first_row = match search_key_in_batch(&batch, key)? {
                    Some(idx) => idx,
                    None => return Ok(None),
                };

                // 6. Find the row with the highest sequence among matching keys.
                //
                // R38-M1: a crafted or corrupt SST whose schema doesn't match
                // the writer-side contract must surface as a corruption error,
                // not a panic.
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
                let sequences = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .ok_or_else(|| ForstError::corruption("SST batch column 2 not UInt64Array"))?;
                let op_types = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .ok_or_else(|| ForstError::corruption("SST batch column 3 not UInt8Array"))?;
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;

                let mut best_row = first_row;
                let mut best_seq = sequences.value(first_row);

                let mut row = first_row + 1;
                while row < batch.num_rows() && keys.value(row) == key {
                    let seq = sequences.value(row);
                    if seq > best_seq {
                        best_seq = seq;
                        best_row = row;
                    }
                    row += 1;
                }

                // 7. Build result based on op_type.
                let op_byte = op_types.value(best_row);
                let op = OpType::from_u8(op_byte).ok_or_else(|| {
                    ForstError::corruption(format!("invalid op_type byte: {}", op_byte))
                })?;

                let value = match op {
                    OpType::Delete | OpType::SingleDelete => None,
                    OpType::Put | OpType::Merge => {
                        if values.is_null(best_row) {
                            None
                        } else {
                            Some(values.value(best_row).to_vec())
                        }
                    }
                };

                Ok(Some(LookupResult {
                    value,
                    sequence: best_seq,
                    op_type: op,
                }))
            }
            DecodedBlock::Kv(kv) => match kv.lookup(key)? {
                None => Ok(None),
                Some((raw_value, sequence, op_type)) => {
                    // Mirror the v1 op-type semantics exactly.
                    let value = match op_type {
                        OpType::Delete | OpType::SingleDelete => None,
                        OpType::Put | OpType::Merge => raw_value,
                    };
                    Ok(Some(LookupResult {
                        value,
                        sequence,
                        op_type,
                    }))
                }
            },
        }
    }

    /// Returns every visible version of `key` in this SST, newest first.
    ///
    /// `get()` intentionally returns the highest-sequence row for legacy point lookups. Merge
    /// resolution needs more: a single flushed SST can legally contain `Put(base), Merge(a),
    /// Merge(b)` for the same user key. Returning only `Merge(b)` loses `base` and `a`. This
    /// method scans the candidate block and any adjacent blocks whose key range still contains
    /// `key`, then sorts matching rows by descending sequence.
    pub fn get_versions(&self, key: &[u8]) -> ForstResult<Vec<LookupResult>> {
        if key < self.footer.min_key.as_slice() || key > self.footer.max_key.as_slice() {
            return Ok(Vec::new());
        }
        if !self.bloom_filter.check(key) {
            return Ok(Vec::new());
        }

        let Some(start_idx) = search_index(&self.index_entries, key) else {
            return Ok(Vec::new());
        };

        let mut out = Vec::new();
        for block_idx in start_idx..self.index_entries.len() {
            let stats = &self.index_stats[block_idx];
            if key < stats.min_key.as_slice() {
                break;
            }
            if key > stats.max_key.as_slice() {
                continue;
            }

            let entry = &self.index_entries[block_idx];
            match self.read_decoded_block(entry.block_offset, entry.block_size)? {
                DecodedBlock::Arrow(batch) => {
                    let Some(first_row) = search_key_in_batch(&batch, key)? else {
                        continue;
                    };
                    let keys = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            ForstError::corruption("SST batch column 0 not BinaryArray")
                        })?;
                    let values = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<BinaryArray>()
                        .ok_or_else(|| {
                            ForstError::corruption("SST batch column 1 not BinaryArray")
                        })?;
                    let sequences = batch
                        .column(2)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .ok_or_else(|| {
                            ForstError::corruption("SST batch column 2 not UInt64Array")
                        })?;
                    let op_types = batch
                        .column(3)
                        .as_any()
                        .downcast_ref::<UInt8Array>()
                        .ok_or_else(|| {
                            ForstError::corruption("SST batch column 3 not UInt8Array")
                        })?;

                    let mut row = first_row;
                    while row < batch.num_rows() && keys.value(row) == key {
                        let op_byte = op_types.value(row);
                        let op = OpType::from_u8(op_byte).ok_or_else(|| {
                            ForstError::corruption(format!("invalid op_type byte: {}", op_byte))
                        })?;
                        let value = match op {
                            OpType::Delete | OpType::SingleDelete => None,
                            OpType::Put | OpType::Merge => {
                                if values.is_null(row) {
                                    None
                                } else {
                                    Some(values.value(row).to_vec())
                                }
                            }
                        };
                        out.push(LookupResult {
                            value,
                            sequence: sequences.value(row),
                            op_type: op,
                        });
                        row += 1;
                    }
                }
                DecodedBlock::Kv(kv) => {
                    let mut raw = Vec::new();
                    kv.collect_versions(key, &mut raw)?;
                    for (raw_value, sequence, op) in raw {
                        let value = match op {
                            OpType::Delete | OpType::SingleDelete => None,
                            OpType::Put | OpType::Merge => raw_value,
                        };
                        out.push(LookupResult {
                            value,
                            sequence,
                            op_type: op,
                        });
                    }
                }
            }
        }

        out.sort_by_key(|x| std::cmp::Reverse(x.sequence));
        Ok(out)
    }

    /// Returns the number of index entries (one per data block).
    /// Used by streaming callers (e.g. compaction) to iterate blocks via
    /// [`Self::read_block_at`].
    pub fn index_entry_count(&self) -> usize {
        self.index_entries.len()
    }

    /// 2026-06-02: number of `read_block_at` calls served so far (diagnostics +
    /// prefix-scan early-termination test).
    pub fn blocks_read(&self) -> u64 {
        self.blocks_read.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// FRS-PREFIX-SEEK: index of the first data block that could contain keys
    /// `>= key`, via binary search over the sparse index (the same primitive
    /// `get`/`get_versions` use). Returns `index_entry_count()` when `key` is
    /// past the SST's last key (no candidate block → caller treats as EOF).
    ///
    /// Prefix/range scans MUST start streaming here rather than at block 0:
    /// blocks before this index have `max_key < key` and cannot contribute,
    /// so reading+decompressing them is pure waste. For a heavy-join state
    /// scan over SST-backed state (q7 ckpt-on), starting at block 0 made each
    /// per-record prefix scan O(blocks-before-prefix) → the dominant cost once
    /// state flushed out of the memtable. Seeking makes it O(matching blocks).
    pub fn first_block_ge(&self, key: &[u8]) -> usize {
        search_index(&self.index_entries, key).unwrap_or(self.index_entries.len())
    }

    /// Decode-FREE prune for prefix/range scans: returns `false` when this SST
    /// provably holds NO key in `[lower, upper)`, using ONLY the in-memory block
    /// index (`last_key`) plus per-block stats (`min_key`) — no block read,
    /// decompress, or Arrow decode.
    ///
    /// `search_index(lower)` finds the first block whose `last_key >= lower`;
    /// every earlier block is entirely `< lower`. If that block's `min_key`
    /// (smallest key in the block) is `>= upper`, then that block — and every
    /// later block, which sorts higher — lies entirely at/above `upper`, so the
    /// half-open range `[lower, upper)` is empty in this file.
    ///
    /// Motivation (profiled 2026-05-31, q4): a per-key join prefix scan opens a
    /// streaming source over EVERY L0 SST whose coarse `[smallest,largest]`
    /// range overlaps the prefix, then lazily decodes that source's first block
    /// to discover it holds nothing for the prefix. With ~142 overlapping L0
    /// SSTs and the key present in only a few, ~140 first-block
    /// decompress+Arrow-decodes were pure waste per probe. This check skips
    /// those sources before any decode. Conservative: if `upper` is unbounded
    /// or per-block stats are unavailable, it returns `true` (no prune), so it
    /// can only avoid provably-empty work — never change results.
    pub fn may_contain_range(&self, lower: &[u8], upper: Option<&[u8]>) -> bool {
        let Some(idx) = search_index(&self.index_entries, lower) else {
            // `lower` is past the last block's `last_key` → no key `>= lower`.
            return false;
        };
        if let Some(hi) = upper {
            if let Some(stats) = self.index_stats.get(idx) {
                if stats.min_key.as_slice() >= hi {
                    return false;
                }
            }
        }
        true
    }

    // -----------------------------------------------------------------------
    // §2.1 BlockPrefetcher support (streaming-read redesign)
    // -----------------------------------------------------------------------

    /// `(block_offset, block_size)` of data block `block_idx` from the sparse
    /// index, or `None` past EOF. Used by the prefetcher to build multi-block
    /// contiguous read windows (blocks are laid out back-to-back by the
    /// writer; the prefetcher still verifies physical contiguity defensively).
    pub fn block_region(&self, block_idx: usize) -> Option<(u64, u32)> {
        self.index_entries
            .get(block_idx)
            .map(|e| (e.block_offset, e.block_size))
    }

    /// §2.1.5 clamp: the first block index in `[start, count)` that provably
    /// holds NO key `< upper` (its per-block `min_key >= upper` ⇒ every key in
    /// it and all later blocks sorts `>= upper`). The prefetcher never reads
    /// at/past this index. Conservative: unbounded `upper` or missing
    /// per-block stats ⇒ `index_entry_count()` (no clamp).
    pub fn end_block_for_upper(&self, start: usize, upper: Option<&[u8]>) -> usize {
        let count = self.index_entries.len();
        let Some(hi) = upper else { return count };
        if self.index_stats.len() != count {
            return count; // stats unavailable — no clamp (never changes results)
        }
        // Blocks are key-ascending; binary-search the first min_key >= hi.
        let mut lo = start.min(count);
        let mut hi_idx = count;
        while lo < hi_idx {
            let mid = lo + (hi_idx - lo) / 2;
            if self.index_stats[mid].min_key.as_slice() >= hi {
                hi_idx = mid;
            } else {
                lo = mid + 1;
            }
        }
        hi_idx
    }

    /// Whether this reader's file currently serves from local storage — picks
    /// the prefetcher's readahead regime (see `RandomAccessFile::is_local`).
    pub fn is_local_file(&self) -> bool {
        self.file.is_local()
    }

    /// Decodes ONE data block from already-read raw bytes (the prefetcher's
    /// multi-block pread hands each block its slice of the window buffer).
    /// v2 KV blocks decode straight from the borrowed slice (`KvBlock::decode`
    /// copies the payload out exactly like the demand path's scratch — same
    /// copy count as today). v1 Arrow blocks need an owned buffer for the
    /// zero-copy `Buffer` wrap, so they pay one slice-to-Vec copy here (v1 is
    /// the legacy format; v2 is default-on).
    pub(crate) fn decode_block_from_slice(&self, bytes: &[u8]) -> ForstResult<DecodedBlock> {
        if bytes.is_empty() {
            return Err(ForstError::corruption("empty SST data block"));
        }
        let verify = super::data_block::sst_read_verify_checksum();
        match bytes[0] {
            BLOCK_TYPE_DATA => {
                let block_buf = arrow::buffer::Buffer::from_vec(bytes.to_vec());
                Ok(DecodedBlock::Arrow(decode_data_block_zerocopy(
                    &block_buf, verify,
                )?))
            }
            BLOCK_TYPE_DATA_KV => Ok(DecodedBlock::Kv(Arc::new(KvBlock::decode(bytes, verify)?))),
            other => Err(ForstError::corruption(format!(
                "unknown SST data block_type 0x{other:02X}"
            ))),
        }
    }

    /// Decoded-block cache lookup for the prefetcher's cache-first window
    /// splitting (§2.1: cached blocks are removed from the I/O window).
    pub(crate) fn cache_get_decoded(&self, block_offset: u64) -> Option<DecodedBlock> {
        let cache = self.block_cache.as_ref()?;
        let key = CacheKey::new(self.cache_file_id, block_offset);
        match cache.get(&key)?.as_ref() {
            CacheEntry::DecodedBatch(batch) => Some(DecodedBlock::Arrow((**batch).clone())),
            CacheEntry::DecodedKv(kv) => Some(DecodedBlock::Kv(Arc::clone(kv))),
            CacheEntry::RawBlock(_) => None,
        }
    }

    /// Decoded-block cache insert at an explicit priority. §2.1: demand blocks
    /// insert at `Low` (today's behaviour); deep-ramp prefetch blocks insert
    /// at `Bottom` so a streaming scan is the first evicted and never
    /// displaces hot point-get blocks. NEVER bypassed entirely — re-probes of
    /// recently scanned windows (interval joins) are common and the decoded
    /// hit is the cheapest read.
    pub(crate) fn cache_insert_decoded(
        &self,
        block_offset: u64,
        decoded: &DecodedBlock,
        priority: CachePriority,
    ) {
        let Some(cache) = self.block_cache.as_ref() else {
            return;
        };
        let key = CacheKey::new(self.cache_file_id, block_offset);
        match decoded {
            DecodedBlock::Arrow(batch) => {
                let arc = Arc::new(batch.clone());
                let charge = CacheEntry::DecodedBatch(Arc::clone(&arc)).charge();
                cache.insert(key, CacheEntry::DecodedBatch(arc), charge, priority);
            }
            DecodedBlock::Kv(kv) => {
                let charge = CacheEntry::DecodedKv(Arc::clone(kv)).charge();
                cache.insert(key, CacheEntry::DecodedKv(Arc::clone(kv)), charge, priority);
            }
        }
    }

    /// Vectored window read for the prefetcher: every `(offset, len)` region
    /// is read FULLY into `buf`, packed back-to-back in order (short read ⇒
    /// corruption — regions come from the sparse index). Backend selection
    /// (io_uring stage): when the file currently serves from a LOCAL fd and
    /// the io_uring backend is available (Linux + kernel probe +
    /// `FRS_IO_URING` gate, see `forst-rs-io-uring`), all regions go down as
    /// ONE ring submission; otherwise the portable serial-pread
    /// [`forst_rs_io::PreadBlockIo`] fallback runs — bit-identical results.
    pub(crate) fn read_block_regions(
        &self,
        regions: &[(u64, usize)],
        buf: &mut [u8],
    ) -> ForstResult<()> {
        // SECURITY: bound every region against file_size (same rationale as
        // read_decoded_block — the sparse index is untrusted input).
        for &(off, len) in regions {
            let end = off
                .checked_add(len as u64)
                .ok_or_else(|| ForstError::corruption("SST window offset+len overflow"))?;
            if end > self.file_size {
                return Err(ForstError::corruption(format!(
                    "SST window [{}, {}) exceeds file_size {}",
                    off, end, self.file_size
                )));
            }
        }
        if let Some(handle) = self.file.local_file_handle() {
            if let Some(uring) = forst_rs_io_uring::UringBlockIo::new(handle) {
                use forst_rs_io::BlockIo as _;
                return uring.read_at_vectored(regions, buf);
            }
        }
        use forst_rs_io::BlockIo as _;
        forst_rs_io::PreadBlockIo(self.file.as_ref()).read_at_vectored(regions, buf)
    }

    /// Bumps the `blocks_read` diagnostic counter — the prefetcher calls this
    /// once per block DELIVERED to its consumer, preserving the counter's
    /// "blocks the scan actually consumed" semantics (`for_each_row_in_block`
    /// bumps it on the legacy demand path).
    pub(crate) fn note_block_read(&self) {
        self.blocks_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Reads and decodes the data block at `block_idx` (0-based). Returns the
    /// `RecordBatch` plus the index entry's `last_key`. Used by the streaming
    /// compaction path to feed rows into the k-way merge without first
    /// materialising every entry into a `Vec<u8>`.
    ///
    /// PR-D2: this is the public streaming primitive that replaces the
    /// `scan() -> Vec<SstScanRow>` materialisation. Callers iterate the
    /// returned `RecordBatch` directly via [`for_each_row_in_batch`] (which
    /// yields zero-copy [`RowView`]s borrowing from the batch buffers).
    ///
    /// v1-only: errors on a v2 KV block. New streaming callers should use the
    /// format-agnostic [`Self::for_each_row_in_block`] instead.
    pub fn read_block_at(&self, block_idx: usize) -> ForstResult<RecordBatch> {
        self.blocks_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = self.index_entries.get(block_idx).ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "SST block index {} out of range (have {} blocks)",
                block_idx,
                self.index_entries.len()
            ))
        })?;
        match self.read_decoded_block(entry.block_offset, entry.block_size)? {
            DecodedBlock::Arrow(batch) => Ok(batch),
            DecodedBlock::Kv(_) => Err(ForstError::corruption(
                "read_block_at called on a v2 KV block; use for_each_row_in_block",
            )),
        }
    }

    /// Format-agnostic streaming primitive: invokes `cb` once per row of the
    /// data block at `block_idx`, in on-disk `(key ASC, sequence DESC)` order,
    /// dispatching v1 Arrow vs v2 KV internally. The block is decoded (or served
    /// from the decoded-block cache) and dropped when this returns, so the
    /// callback must consume each [`RowView`] inline or copy it out.
    pub fn for_each_row_in_block<F>(&self, block_idx: usize, cb: F) -> ForstResult<()>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        self.blocks_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let entry = self.index_entries.get(block_idx).ok_or_else(|| {
            ForstError::invalid_argument(format!(
                "SST block index {} out of range (have {} blocks)",
                block_idx,
                self.index_entries.len()
            ))
        })?;
        let block = self.read_decoded_block(entry.block_offset, entry.block_size)?;
        block.for_each_row(cb)
    }

    /// Scans the SST file for all entries in the `[lower, upper)` key range,
    /// invoking `cb` once per matching row with a zero-copy [`RowView`] that
    /// borrows from the underlying Arrow batch buffer.
    ///
    /// PR-D2 zero-copy primitive: no `Vec<u8>` is allocated per row. Each
    /// `RecordBatch` is dropped between blocks, so the callback must either
    /// process the view inline or copy out via `view.key.to_vec()` /
    /// `view.value.map(|v| v.to_vec())`.
    ///
    /// Entries are visited in `(key ASC, sequence DESC)` order — the on-disk
    /// order produced by [`crate::sst::writer::SstWriterImpl`]. Unlike
    /// [`SstReaderImpl::get`], this visits ALL versions of each key; callers
    /// resolve visibility and merges themselves.
    pub fn scan_borrowed<F>(&self, lower: &[u8], upper: Option<&[u8]>, mut cb: F) -> ForstResult<()>
    where
        F: FnMut(RowView<'_>) -> ForstResult<()>,
    {
        // Short-circuit if the scan range doesn't intersect [min_key, max_key].
        if let Some(hi) = upper {
            if hi <= self.footer.min_key.as_slice() {
                return Ok(());
            }
        }
        if lower > self.footer.max_key.as_slice() {
            return Ok(());
        }

        for (entry, stats) in self.index_entries.iter().zip(self.index_stats.iter()) {
            // Skip blocks whose key ranges lie entirely outside [lower, upper).
            if entry.last_key.as_slice() < lower {
                continue;
            }
            if let Some(hi) = upper {
                if stats.min_key.as_slice() >= hi {
                    break;
                }
            }

            let block = self.read_decoded_block(entry.block_offset, entry.block_size)?;
            block.for_each_row(|view| {
                if view.key < lower {
                    return Ok(());
                }
                if let Some(hi) = upper {
                    if view.key >= hi {
                        // We could `break` if we had a control-flow channel; the
                        // wrapper loop in `for_each_row_in_batch` is row-linear,
                        // so we just no-op the remaining rows of this block via
                        // an unrolled compare on the caller side. For batch sizes
                        // ≤ 8192 (FLUSH_BATCH_SIZE) this is at worst a few µs.
                        return Ok(());
                    }
                }
                cb(view)
            })?;
        }
        Ok(())
    }

    /// Backwards-compatible owned-row scan. Reimplemented atop
    /// [`Self::scan_borrowed`] so the single place that pays the `to_vec`
    /// cost is here, in callers that explicitly opt into ownership.
    ///
    /// Prefer [`Self::scan_borrowed`] in new code (compaction, replication,
    /// snapshot reads) — it skips the per-row allocation entirely.
    pub fn scan(&self, lower: &[u8], upper: Option<&[u8]>) -> ForstResult<Vec<SstScanRow>> {
        let mut out = Vec::new();
        self.scan_borrowed(lower, upper, |view| {
            out.push((
                view.key.to_vec(),
                view.value.map(|v| v.to_vec()),
                view.sequence,
                view.op_type,
            ));
            Ok(())
        })?;
        Ok(out)
    }
}

/// Iterates every row of a decoded SST data-block `RecordBatch`, invoking
/// `cb` once per row with a zero-copy [`RowView`] that borrows from the batch
/// buffers.
///
/// Internal helper exposed at module scope so streaming callers (compaction,
/// flush re-write) can drive the iteration themselves after fetching the
/// batch via [`SstReaderImpl::read_block_at`].
pub fn for_each_row_in_batch<F>(batch: &RecordBatch, mut cb: F) -> ForstResult<()>
where
    F: FnMut(RowView<'_>) -> ForstResult<()>,
{
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 2 not UInt64Array"))?;
    let op_types = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 3 not UInt8Array"))?;

    for row in 0..batch.num_rows() {
        let key = keys.value(row);
        let value = if values.is_null(row) {
            None
        } else {
            Some(values.value(row))
        };
        let op_byte = op_types.value(row);
        let op_type = OpType::from_u8(op_byte).ok_or_else(|| {
            ForstError::corruption(format!("invalid op_type in SST batch: {}", op_byte))
        })?;
        cb(RowView {
            key,
            value,
            sequence: sequences.value(row),
            op_type,
        })?;
    }
    Ok(())
}

/// S2-1: the stable `(key_buffer, value_buffer)` backing pair of a v1 Arrow
/// SST data-block batch — the key column's and value column's `BinaryArray`
/// values buffers. [`SliceRef::Block`] ranges produced by
/// [`for_each_row_ranges_in_batch`] index these buffers (key refs → first,
/// value refs → second). Kept here so the BinaryArray offset math lives in
/// exactly one place (work-order §6.2).
pub fn batch_key_value_data(batch: &RecordBatch) -> ForstResult<(&[u8], &[u8])> {
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;
    Ok((keys.value_data(), values.value_data()))
}

/// S2-1: range-exposing twin of [`for_each_row_in_batch`] for v1 Arrow
/// blocks. Yields each row's key/value as [`SliceRef::Block`] ranges into the
/// respective `BinaryArray` values buffers (see [`batch_key_value_data`]) —
/// Arrow key bytes are stable in the batch buffers, so no arena is needed
/// (D2: the arena is a v2-only requirement).
pub fn for_each_row_ranges_in_batch<F>(batch: &RecordBatch, mut cb: F) -> ForstResult<()>
where
    F: FnMut(RowRanges) -> ForstResult<()>,
{
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 2 not UInt64Array"))?;
    let op_types = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 3 not UInt8Array"))?;

    // `value_offsets()` are absolute indices into `value_data()` (arrow keeps
    // offsets un-rebased for sliced arrays), so the ranges below pair exactly
    // with the buffers `batch_key_value_data` hands out.
    let key_offsets = keys.value_offsets();
    let value_offsets = values.value_offsets();
    for row in 0..batch.num_rows() {
        let ks = key_offsets[row] as usize;
        let ke = key_offsets[row + 1] as usize;
        let value = if values.is_null(row) {
            None
        } else {
            let vs = value_offsets[row] as usize;
            let ve = value_offsets[row + 1] as usize;
            Some(SliceRef::block(vs, ve - vs)?)
        };
        let op_byte = op_types.value(row);
        let op_type = OpType::from_u8(op_byte).ok_or_else(|| {
            ForstError::corruption(format!("invalid op_type in SST batch: {}", op_byte))
        })?;
        cb(RowRanges {
            key: SliceRef::block(ks, ke - ks)?,
            value,
            sequence: sequences.value(row),
            op_type,
        })?;
    }
    Ok(())
}

/// S2-3: seek-aware, early-stopping ranges twin for v1 Arrow blocks —
/// binary-searches the first row with key `>= lower` (keys are sorted ASC),
/// then yields rows until exhaustion or the callback returns `Ok(false)`.
pub fn for_each_row_ranges_in_batch_from<F>(
    batch: &RecordBatch,
    lower: &[u8],
    mut cb: F,
) -> ForstResult<()>
where
    F: FnMut(RowRanges) -> ForstResult<bool>,
{
    let keys = batch
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 0 not BinaryArray"))?;
    let values = batch
        .column(1)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| ForstError::corruption("SST batch column 1 not BinaryArray"))?;
    let sequences = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 2 not UInt64Array"))?;
    let op_types = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .ok_or_else(|| ForstError::corruption("SST batch column 3 not UInt8Array"))?;

    // Lower-bound binary search: first row with key >= lower.
    let num_rows = batch.num_rows();
    let mut lo = 0usize;
    let mut hi = num_rows;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if keys.value(mid) < lower {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let key_offsets = keys.value_offsets();
    let value_offsets = values.value_offsets();
    for row in lo..num_rows {
        let ks = key_offsets[row] as usize;
        let ke = key_offsets[row + 1] as usize;
        let value = if values.is_null(row) {
            None
        } else {
            let vs = value_offsets[row] as usize;
            let ve = value_offsets[row + 1] as usize;
            Some(SliceRef::block(vs, ve - vs)?)
        };
        let op_byte = op_types.value(row);
        let op_type = OpType::from_u8(op_byte).ok_or_else(|| {
            ForstError::corruption(format!("invalid op_type in SST batch: {}", op_byte))
        })?;
        let keep_going = cb(RowRanges {
            key: SliceRef::block(ks, ke - ks)?,
            value,
            sequence: sequences.value(row),
            op_type,
        })?;
        if !keep_going {
            return Ok(());
        }
    }
    Ok(())
}

/// FRS-ZERO-COPY-MERGE (2026-06-05): a pull cursor over an ENTIRE SST file,
/// yielding every row (all versions) in on-disk `(key ASC, seq DESC)` order by
/// reference. This is the per-input iterator for the streaming k-way compaction
/// merge — it replaces the alloc-heavy `scan_borrowed → push CompactionEntry`
/// gather. Values are borrowed straight from the current block (zero copy);
/// only the small running key is materialized (by the KV stepper). Handles both
/// v1 Arrow and v2 KV blocks; reads one block at a time (peak memory = one block).
pub struct SstBlockCursor {
    reader: Arc<SstReaderImpl>,
    /// Index of the NEXT block to load (Demand mode only; the Windowed
    /// source keeps its own cursor inside the prefetcher).
    next_block: usize,
    /// L4 (2026-06-12 windowed-readpath design §2.2): where blocks come from.
    source: BlockSource,
    inner: CursorInner,
}

/// L4: block delivery source for [`SstBlockCursor`].
enum BlockSource {
    /// Today's path: strictly serial, demand-paged `read_decoded_block`
    /// (cache-first check + `Low` insert).
    Demand,
    /// Compaction-input mode: fixed-window, double-buffered
    /// [`BlockPrefetcher`] in `for_compaction` mode — input I/O + decompress
    /// + decode overlap the merge on the read-I/O pool, with
    /// `CacheFillPolicy::Skip` (cache-first read, NO insert).
    Windowed(crate::sst::prefetch::BlockPrefetcher),
}

enum CursorInner {
    Done,
    Arrow { batch: RecordBatch, row: usize },
    Kv(crate::sst::kv_block::KvBlockCursor),
}

impl SstBlockCursor {
    /// Creates a cursor positioned at the first row of the file (invalid if the
    /// SST has no data rows).
    pub fn new(reader: Arc<SstReaderImpl>) -> ForstResult<Self> {
        let mut c = Self {
            reader,
            next_block: 0,
            source: BlockSource::Demand,
            inner: CursorInner::Done,
        };
        c.load_next_nonempty()?;
        Ok(c)
    }

    /// L4: creates a compaction-input cursor whose blocks arrive via a
    /// fixed-`window_blocks`, double-buffered [`BlockPrefetcher`] in
    /// compaction mode (full-file scan, cache-first read, Skip insert).
    /// The row stream is byte-identical to [`Self::new`]; only the block
    /// production (overlap + cache policy) differs. The caller (the engine's
    /// `CompactionJob::run_streaming`) gates this behind `FRS_COMPACT_WINDOWED`
    /// and clamps `window_blocks` against the fan-in prefetch budget.
    pub fn new_windowed(reader: Arc<SstReaderImpl>, window_blocks: u32) -> ForstResult<Self> {
        let prefetcher = crate::sst::prefetch::BlockPrefetcher::for_compaction(
            Arc::clone(&reader),
            window_blocks,
        );
        let mut c = Self {
            reader,
            next_block: 0,
            source: BlockSource::Windowed(prefetcher),
            inner: CursorInner::Done,
        };
        c.load_next_nonempty()?;
        Ok(c)
    }

    /// L4 W5: compaction-read telemetry of the windowed source (`None` for a
    /// demand cursor). The engine sums these across a job's inputs.
    pub fn windowed_read_stats(&self) -> Option<crate::sst::prefetch::CompactionReadStats> {
        match &self.source {
            BlockSource::Windowed(pf) => Some(pf.compaction_stats()),
            BlockSource::Demand => None,
        }
    }

    /// Loads successive blocks until a non-empty one is positioned, or marks the
    /// cursor Done at end-of-file.
    fn load_next_nonempty(&mut self) -> ForstResult<()> {
        loop {
            let block = match &mut self.source {
                BlockSource::Demand => {
                    if self.next_block >= self.reader.index_entries.len() {
                        self.inner = CursorInner::Done;
                        return Ok(());
                    }
                    let entry = &self.reader.index_entries[self.next_block];
                    let block = self
                        .reader
                        .read_decoded_block(entry.block_offset, entry.block_size)?;
                    self.next_block += 1;
                    block
                }
                BlockSource::Windowed(pf) => match pf.next_decoded()? {
                    Some(block) => block,
                    None => {
                        self.inner = CursorInner::Done;
                        return Ok(());
                    }
                },
            };
            match block {
                DecodedBlock::Arrow(batch) => {
                    if batch.num_rows() > 0 {
                        self.inner = CursorInner::Arrow { batch, row: 0 };
                        return Ok(());
                    }
                }
                DecodedBlock::Kv(kv) => {
                    let cur = crate::sst::kv_block::KvBlockCursor::new(kv)?;
                    if cur.valid() {
                        self.inner = CursorInner::Kv(cur);
                        return Ok(());
                    }
                }
            }
            // empty block — keep scanning
        }
    }

    #[inline]
    pub fn valid(&self) -> bool {
        !matches!(self.inner, CursorInner::Done)
    }

    /// Advances to the next row, crossing block boundaries as needed.
    pub fn advance(&mut self) -> ForstResult<()> {
        match &mut self.inner {
            CursorInner::Done => Ok(()),
            CursorInner::Arrow { batch, row } => {
                *row += 1;
                if *row >= batch.num_rows() {
                    self.load_next_nonempty()
                } else {
                    Ok(())
                }
            }
            CursorInner::Kv(cur) => {
                cur.advance()?;
                if !cur.valid() {
                    self.load_next_nonempty()
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Current row's key. Empty slice when invalid. Schema validated at decode,
    /// so the column downcasts cannot fail for engine-produced SSTs.
    pub fn key(&self) -> &[u8] {
        match &self.inner {
            CursorInner::Arrow { batch, row } => batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("SST col0 BinaryArray")
                .value(*row),
            CursorInner::Kv(cur) => cur.key(),
            CursorInner::Done => &[],
        }
    }

    /// Current row's value (`None` = tombstone).
    pub fn value(&self) -> Option<&[u8]> {
        match &self.inner {
            CursorInner::Arrow { batch, row } => {
                let vals = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("SST col1 BinaryArray");
                if vals.is_null(*row) {
                    None
                } else {
                    Some(vals.value(*row))
                }
            }
            CursorInner::Kv(cur) => cur.value(),
            CursorInner::Done => None,
        }
    }

    /// Current row's sequence number.
    pub fn sequence(&self) -> u64 {
        match &self.inner {
            CursorInner::Arrow { batch, row } => batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("SST col2 UInt64Array")
                .value(*row),
            CursorInner::Kv(cur) => cur.sequence(),
            CursorInner::Done => 0,
        }
    }

    /// Current row's op type.
    pub fn op_type(&self) -> OpType {
        let byte = match &self.inner {
            CursorInner::Arrow { batch, row } => batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .expect("SST col3 UInt8Array")
                .value(*row),
            CursorInner::Kv(cur) => cur.op_byte(),
            CursorInner::Done => 0,
        };
        OpType::from_u8(byte).expect("SST op_type validated at decode")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::schema::sst_schema;
    use crate::sst::writer::{SstWriterImpl, SstWriterOptions};
    use arrow::array::{BinaryArray, UInt64Array, UInt8Array};
    use forst_rs_common::CompressionType;
    use std::sync::Arc;

    /// In-memory RandomAccessFile backed by a Vec<u8>.
    struct MemRandomAccessFile {
        data: Arc<Vec<u8>>,
    }

    impl RandomAccessFile for MemRandomAccessFile {
        fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
            let start = offset as usize;
            if start >= self.data.len() {
                return Ok(0);
            }
            let end = std::cmp::min(start + buf.len(), self.data.len());
            let n = end - start;
            buf[..n].copy_from_slice(&self.data[start..end]);
            Ok(n)
        }

        fn file_size(&self) -> ForstResult<u64> {
            Ok(self.data.len() as u64)
        }
    }

    fn write_test_sst(n: usize) -> Arc<Vec<u8>> {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        for i in 0..n {
            let key = format!("key_{:05}", i);
            let val = format!("val_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 1) // OpType::Put = 1 (RocksDB byte-compat)
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();
        Arc::new(data)
    }

    /// Builds an SST in a forced format (kv=true → v2 KV, false → v1 Arrow) with
    /// shared key prefixes + tombstones + an empty value, across multiple blocks.
    fn write_test_sst_fmt(n: usize, kv: bool) -> Arc<Vec<u8>> {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 1024, // small → force many blocks for cross-block coverage
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        writer.force_kv_block_format(kv);
        for i in 0..n {
            let key = format!("user:{:05}", i); // shared "user:" prefix exercises KV compression
                                                // every 7th row a tombstone; every 5th an empty-but-present value
            if i % 7 == 0 {
                writer.add(key.as_bytes(), None, i as u64 + 1, 0).unwrap(); // Delete=0
            } else if i % 5 == 0 {
                writer
                    .add(key.as_bytes(), Some(b""), i as u64 + 1, 1)
                    .unwrap();
            } else {
                let val = format!("value_for_{:05}", i);
                writer
                    .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 1)
                    .unwrap();
            }
        }
        let (data, _info) = writer.finish().unwrap();
        Arc::new(data)
    }

    #[test]
    fn sst_block_cursor_matches_scan_borrowed() {
        for &kv in &[false, true] {
            for &n in &[1usize, 7, 100, 5000] {
                let sst = write_test_sst_fmt(n, kv);
                let reader = Arc::new(
                    SstReaderImpl::open(Box::new(MemRandomAccessFile { data: sst.clone() }))
                        .unwrap(),
                );
                // Oracle: the existing push-based scan over the whole key range.
                let mut oracle = Vec::new();
                reader
                    .scan_borrowed(b"", None, |r| {
                        oracle.push((
                            r.key.to_vec(),
                            r.value.map(|x| x.to_vec()),
                            r.sequence,
                            r.op_type,
                        ));
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(oracle.len(), n, "oracle row count kv={kv} n={n}");
                // Pull cursor must yield the identical sequence.
                let mut cur = SstBlockCursor::new(reader.clone()).unwrap();
                let mut got = Vec::new();
                while cur.valid() {
                    got.push((
                        cur.key().to_vec(),
                        cur.value().map(|x| x.to_vec()),
                        cur.sequence(),
                        cur.op_type(),
                    ));
                    cur.advance().unwrap();
                }
                assert_eq!(got, oracle, "cursor vs scan_borrowed kv={kv} n={n}");
            }
        }
    }

    /// L4 G0 (2026-06-12 windowed-readpath design): the WINDOWED compaction
    /// cursor yields exactly the demand cursor's row stream — v1 Arrow + v2
    /// KV, tombstones + empty values, multi-block, for window sizes that do
    /// and don't divide the block count (incl. window=1).
    #[test]
    fn sst_block_cursor_windowed_matches_demand() {
        for &kv in &[false, true] {
            for &n in &[1usize, 7, 100, 5000] {
                let sst = write_test_sst_fmt(n, kv);
                let reader = Arc::new(
                    SstReaderImpl::open(Box::new(MemRandomAccessFile { data: sst.clone() }))
                        .unwrap(),
                );
                let drain = |mut cur: SstBlockCursor| {
                    let mut rows = Vec::new();
                    while cur.valid() {
                        rows.push((
                            cur.key().to_vec(),
                            cur.value().map(|x| x.to_vec()),
                            cur.sequence(),
                            cur.op_type(),
                        ));
                        cur.advance().unwrap();
                    }
                    rows
                };
                let expected = drain(SstBlockCursor::new(reader.clone()).unwrap());
                for &w in &[1u32, 3, 64] {
                    let got = drain(SstBlockCursor::new_windowed(reader.clone(), w).unwrap());
                    assert_eq!(got, expected, "windowed vs demand kv={kv} n={n} w={w}");
                }
            }
        }
    }

    #[test]
    fn test_open_reads_footer_correctly() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile {
            data: sst_data.clone(),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        assert_eq!(reader.footer().total_entries, 100);
        assert_eq!(reader.footer().min_key, b"key_00000");
        assert_eq!(reader.footer().max_key, b"key_00099");
    }

    #[test]
    fn test_open_loads_bloom_filter() {
        let sst_data = write_test_sst(50);
        let file = Box::new(MemRandomAccessFile {
            data: sst_data.clone(),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        // All inserted keys must pass bloom filter
        for i in 0..50 {
            let key = format!("key_{:05}", i);
            assert!(reader.bloom_filter().check(key.as_bytes()));
        }
    }

    #[test]
    fn test_may_contain_range_prunes_decode_free() {
        // Keys key_00000..key_00199 across multiple blocks (block_size 4096).
        let sst_data = write_test_sst(200);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();

        // Present key → must NOT be pruned.
        assert!(reader.may_contain_range(b"key_00050", Some(b"key_00051")));
        assert!(reader.may_contain_range(b"key_00000", Some(b"key_00001")));
        assert!(reader.may_contain_range(b"key_00199", Some(b"key_0019:"))); // last key

        // Range entirely BELOW the first key (min_key >= upper) → pruned.
        assert!(!reader.may_contain_range(b"aaa", Some(b"bbb")));

        // Range entirely ABOVE the last key (search_index → None) → pruned.
        assert!(!reader.may_contain_range(b"zzz", Some(b"zzzz")));
        assert!(!reader.may_contain_range(b"key_99999", Some(b"key_99999\xff")));

        // A gap WITHIN a block (matched block holds nearby keys, so min_key <
        // upper) is NOT pruned — the prune is conservative and only fires when
        // the matched block lies entirely at/above `upper`. Decoding is still
        // required to confirm this narrow window is empty.
        assert!(reader.may_contain_range(b"key_00050x", Some(b"key_00050y")));

        // Unbounded upper → never pruned (conservative).
        assert!(reader.may_contain_range(b"key_00050", None));
    }

    #[test]
    fn test_open_loads_index() {
        let sst_data = write_test_sst(200);
        let file = Box::new(MemRandomAccessFile {
            data: sst_data.clone(),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        assert!(reader.data_block_count() >= 1);
        assert_eq!(reader.total_entries(), 200);
    }

    #[test]
    fn test_open_rejects_too_small_file() {
        let tiny = Arc::new(vec![0u8; 10]);
        let file = Box::new(MemRandomAccessFile { data: tiny });
        let result = SstReaderImpl::open(file);
        assert!(result.is_err());
    }

    #[test]
    fn test_open_rejects_bad_magic() {
        let bad = vec![0u8; 100];
        // No valid magic at tail
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(bad),
        });
        let result = SstReaderImpl::open(file);
        assert!(result.is_err());
    }

    /// Regression test for Sweep R2 H (Reviewer 5): a crafted SST with a
    /// `footer_length` value that exceeds the actual file size must be
    /// rejected BEFORE the `vec![0u8; footer_length as usize]` allocation,
    /// to prevent OOM-DoS on attacker-controlled input. Pre-fix this
    /// would have triggered an underflow panic on `file_size - footer_length`
    /// or attempted a multi-GiB allocation.
    #[test]
    fn test_open_rejects_oversized_footer_length() {
        // 100-byte file, last 8 bytes encode footer_length=u32::MAX + magic.
        let mut data = vec![0u8; 100];
        // Place a footer_length value larger than the entire file in the
        // last 8 bytes' first 4 (footer_length) followed by SST_MAGIC.
        let footer_length: u32 = 0x7fff_ffff; // huge but file is only 100 bytes
        data[92..96].copy_from_slice(&footer_length.to_le_bytes());
        data[96..100].copy_from_slice(SST_MAGIC);
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let err = match SstReaderImpl::open(file) {
            Ok(_) => panic!("must reject oversized footer_length"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("exceeds file_size"),
            "expected error message to mention size violation; got: {}",
            msg
        );
    }

    /// Regression test for Sweep R2 H (Reviewer 5): an SST with a footer
    /// claiming `bloom_filter_offset + bloom_filter_size > file_size` (or
    /// triggering u64 overflow) must be rejected.
    ///
    /// Direct construction of such a file requires bypassing the writer
    /// (which always emits well-formed footers), so we check the validation
    /// path indirectly via the underflow guard on footer_length being zero.
    /// (The full bloom/index range overflow paths share identical
    /// `checked_add` + `> file_size` logic; they're trivially correct by
    /// inspection given footer_length validation passes.)
    #[test]
    fn test_open_rejects_zero_footer_length() {
        let mut data = vec![0u8; 100];
        data[92..96].copy_from_slice(&0u32.to_le_bytes()); // footer_length=0
        data[96..100].copy_from_slice(SST_MAGIC);
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let err = match SstReaderImpl::open(file) {
            Ok(_) => panic!("must reject zero footer_length"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("footer_length is zero"),
            "expected zero-footer-length error; got: {}",
            msg
        );
    }

    // -----------------------------------------------------------------------
    // search_key_in_batch tests
    // -----------------------------------------------------------------------

    fn make_batch(keys: &[&[u8]], vals: &[&[u8]], seqs: &[u64], ops: &[u8]) -> RecordBatch {
        let schema = Arc::new(sst_schema());
        let key_array = BinaryArray::from_iter_values(keys.iter().copied());
        let val_array = BinaryArray::from_iter_values(vals.iter().copied());
        let seq_array = UInt64Array::from(seqs.to_vec());
        let op_array = UInt8Array::from(ops.to_vec());
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(key_array),
                Arc::new(val_array),
                Arc::new(seq_array),
                Arc::new(op_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_search_key_in_batch_found() {
        let batch = make_batch(
            &[b"aaa", b"bbb", b"ccc", b"ddd"],
            &[b"v1", b"v2", b"v3", b"v4"],
            &[1, 2, 3, 4],
            &[0, 0, 0, 0],
        );
        assert_eq!(search_key_in_batch(&batch, b"bbb").unwrap(), Some(1));
        assert_eq!(search_key_in_batch(&batch, b"aaa").unwrap(), Some(0));
        assert_eq!(search_key_in_batch(&batch, b"ddd").unwrap(), Some(3));
    }

    #[test]
    fn test_search_key_in_batch_not_found() {
        let batch = make_batch(
            &[b"aaa", b"ccc", b"eee"],
            &[b"v1", b"v2", b"v3"],
            &[1, 2, 3],
            &[0, 0, 0],
        );
        assert_eq!(search_key_in_batch(&batch, b"bbb").unwrap(), None);
        assert_eq!(search_key_in_batch(&batch, b"zzz").unwrap(), None);
    }

    #[test]
    fn test_search_key_in_batch_empty() {
        let schema = Arc::new(sst_schema());
        let batch = RecordBatch::new_empty(schema);
        assert_eq!(search_key_in_batch(&batch, b"any").unwrap(), None);
    }

    #[test]
    fn test_search_key_in_batch_duplicate_keys_finds_first() {
        // Same key with different sequences — should find the first occurrence
        let batch = make_batch(
            &[b"key", b"key", b"key"],
            &[b"v1", b"v2", b"v3"],
            &[100, 50, 10],
            &[0, 0, 0],
        );
        assert_eq!(search_key_in_batch(&batch, b"key").unwrap(), Some(0));
    }

    // -----------------------------------------------------------------------
    // get() tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_get_existing_key() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();

        let result = reader.get(b"key_00050").unwrap();
        assert!(result.is_some());
        let lr = result.unwrap();
        assert_eq!(lr.value, Some(b"val_00050".to_vec()));
        assert_eq!(lr.sequence, 51);
        assert_eq!(lr.op_type, OpType::Put);
    }

    /// Regression test for Sweep R3 H (Reviewers 2 + 5): a sparse-index
    /// entry claiming a `block_size` that overflows past the file end
    /// must be rejected by `read_data_block` BEFORE allocation, not
    /// after attempting `read_at` (which might OOM on the
    /// `vec![0u8; block_size as usize]` first). We simulate the
    /// malicious-index condition by mutating the cached `file_size`
    /// to a value smaller than any real block range — the validation
    /// then trips on otherwise-valid block reads.
    #[test]
    fn test_read_data_block_rejects_oob_range() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let mut reader = SstReaderImpl::open(file).unwrap();
        // Pretend the file is 16 bytes — any real block read will trip
        // the `block_end > self.file_size` check.
        reader.file_size = 16;
        let err = match reader.get(b"key_00050") {
            Ok(_) => panic!("read_data_block must reject OOB block range"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("data block range") && msg.contains("exceeds file_size"),
            "expected OOB-block error message; got: {}",
            msg
        );
    }

    #[test]
    fn test_get_first_key() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        let result = reader.get(b"key_00000").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Some(b"val_00000".to_vec()));
    }

    #[test]
    fn test_get_last_key() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        let result = reader.get(b"key_00099").unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Some(b"val_00099".to_vec()));
    }

    #[test]
    fn test_get_missing_key_out_of_range() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        // Before min key
        assert!(reader.get(b"aaa").unwrap().is_none());
        // After max key
        assert!(reader.get(b"zzz").unwrap().is_none());
    }

    #[test]
    fn test_get_missing_key_in_range() {
        let sst_data = write_test_sst(100);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();
        // Key in range but not present
        assert!(reader.get(b"key_00100").unwrap().is_none());
    }

    #[test]
    fn test_get_delete_tombstone() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        writer.add(b"aaa", Some(b"val"), 1, 1).unwrap(); // Put (OpType::Put = 1, RocksDB byte-compat)
        writer.add(b"bbb", None, 2, 0).unwrap(); // Delete (OpType::Delete = 0, RocksDB byte-compat)
        writer.add(b"ccc", Some(b"val3"), 3, 1).unwrap(); // Put (OpType::Put = 1, RocksDB byte-compat)
        let (data, _) = writer.finish().unwrap();

        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        let result = reader.get(b"bbb").unwrap().unwrap();
        assert_eq!(result.op_type, OpType::Delete);
        assert_eq!(result.value, None);
    }

    #[test]
    fn test_get_all_keys_100_percent_hit() {
        let n = 500;
        let sst_data = write_test_sst(n);
        let file = Box::new(MemRandomAccessFile { data: sst_data });
        let reader = SstReaderImpl::open(file).unwrap();

        for i in 0..n {
            let key = format!("key_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "key {} should be found", key);
            let lr = result.unwrap();
            let expected_val = format!("val_{:05}", i);
            assert_eq!(lr.value, Some(expected_val.into_bytes()));
        }
    }

    #[test]
    fn test_get_multi_block_small_block_size() {
        // Force many small blocks
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 128,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        for i in 0..200u64 {
            let key = format!("mb_{:05}", i);
            let val = format!("v_{:05}", i);
            writer
                .add(key.as_bytes(), Some(val.as_bytes()), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
                .unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        assert!(info.data_block_count > 1, "should have multiple blocks");

        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        // Verify all keys found
        for i in 0..200u64 {
            let key = format!("mb_{:05}", i);
            let expected_val = format!("v_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "key {} not found", key);
            assert_eq!(result.unwrap().value, Some(expected_val.into_bytes()));
        }
    }

    #[test]
    fn test_get_with_lz4_compression() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::Lz4,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        for i in 0..100u64 {
            let key = format!("lz4_{:05}", i);
            writer
                .add(key.as_bytes(), Some(b"value"), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
                .unwrap();
        }
        let (data, _) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        for i in 0..100u64 {
            let key = format!("lz4_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "LZ4 key {} not found", key);
        }
    }

    #[test]
    fn test_get_with_zstd_compression() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::Zstd,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        for i in 0..100u64 {
            let key = format!("zstd_{:05}", i);
            writer
                .add(key.as_bytes(), Some(b"value"), i + 1, 1) // Put (OpType::Put = 1, RocksDB byte-compat)
                .unwrap();
        }
        let (data, _) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        for i in 0..100u64 {
            let key = format!("zstd_{:05}", i);
            let result = reader.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Zstd key {} not found", key);
        }
    }

    // -----------------------------------------------------------------------
    // PR-D2 zero-copy scan tests (Z3-10, Z3-11, C-R3-H1..3)
    // -----------------------------------------------------------------------

    /// PR-D2 invariant: `RowView::key` and `RowView::value` borrow into the
    /// Arrow `BinaryArray` backing buffer of the decoded data block — they
    /// must NOT be a fresh `Vec` allocated per row. We assert this by
    /// checking the addresses fall inside a single decoded `RecordBatch`'s
    /// key/value buffers (which themselves are heap allocations owned by the
    /// batch's Arc-backed `Buffer`s).
    #[test]
    fn sst_read_arrow_zero_copy() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 4096,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        // This test asserts the v1 Arrow zero-copy decode path specifically; the
        // writer default is now v2 KV, so force v1 for this v1-path test.
        writer.force_kv_block_format(false);
        for i in 0..32u64 {
            writer
                .add(
                    format!("zc_{:04}", i).as_bytes(),
                    Some(format!("zc_val_{:04}", i).as_bytes()),
                    i + 1,
                    1,
                )
                .unwrap();
        }
        let (data, info) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();
        assert!(info.data_block_count >= 1);

        // Read the first data block ourselves so we can keep the batch alive
        // while comparing pointer ranges with what `for_each_row_in_batch`
        // yields.
        let batch = reader.read_block_at(0).unwrap();
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        let key_buf = keys.values().as_slice();
        let val_buf = values.values().as_slice();
        let key_buf_range = key_buf.as_ptr() as usize..(key_buf.as_ptr() as usize + key_buf.len());
        let val_buf_range = val_buf.as_ptr() as usize..(val_buf.as_ptr() as usize + val_buf.len());

        let mut rows_seen = 0usize;
        for_each_row_in_batch(&batch, |view| {
            let kp = view.key.as_ptr() as usize;
            assert!(
                key_buf_range.contains(&kp),
                "RowView::key must borrow into the Arrow BinaryArray buffer (got ptr {:p}, buf range {:?})",
                view.key.as_ptr(),
                key_buf_range,
            );
            if let Some(v) = view.value {
                let vp = v.as_ptr() as usize;
                assert!(
                    val_buf_range.contains(&vp),
                    "RowView::value must borrow into the Arrow BinaryArray buffer"
                );
            }
            rows_seen += 1;
            Ok(())
        })
        .unwrap();
        assert!(rows_seen > 0);
    }

    /// PR-D2: `scan_borrowed` yields the same logical rows as the legacy
    /// `scan()` API. This is the round-trip parity test required by the spec.
    #[test]
    fn sst_scan_borrowed_matches_scan() {
        let mut writer = SstWriterImpl::with_options(SstWriterOptions {
            block_size: 256,
            compression: CompressionType::None,
            cf_id: forst_rs_common::DEFAULT_CF_ID,
        });
        for i in 0..100u64 {
            writer
                .add(
                    format!("scan_{:05}", i).as_bytes(),
                    Some(format!("v_{:05}", i).as_bytes()),
                    i + 1,
                    1,
                )
                .unwrap();
        }
        let (data, _info) = writer.finish().unwrap();
        let file = Box::new(MemRandomAccessFile {
            data: Arc::new(data),
        });
        let reader = SstReaderImpl::open(file).unwrap();

        let owned: Vec<_> = reader.scan(b"", None).unwrap();
        let mut borrowed_count = 0usize;
        reader
            .scan_borrowed(b"", None, |view| {
                let (ek, ev, eseq, eop) = &owned[borrowed_count];
                assert_eq!(view.key, ek.as_slice());
                assert_eq!(view.value, ev.as_deref());
                assert_eq!(view.sequence, *eseq);
                assert_eq!(view.op_type, *eop);
                borrowed_count += 1;
                Ok(())
            })
            .unwrap();
        assert_eq!(borrowed_count, owned.len());
        assert_eq!(borrowed_count, 100);
    }

    // =======================================================================
    // S2-1 G1: v1 ranges twin vs for_each_row_in_batch byte-equality
    // =======================================================================

    /// Builds an SST-schema batch with shared prefixes, duplicate user keys
    /// (multi-version), tombstones, empty-present values and merge ops.
    fn s2_v1_test_batch() -> RecordBatch {
        let mut keys: Vec<Vec<u8>> = Vec::new();
        let mut vals: Vec<Option<Vec<u8>>> = Vec::new();
        let mut seqs: Vec<u64> = Vec::new();
        let mut ops: Vec<u8> = Vec::new();
        for i in 0..60u64 {
            let key = format!("user:{:04}", i / 2).into_bytes(); // dup keys
            let (val, op): (Option<Vec<u8>>, u8) = match i % 5 {
                0 => (None, 0),                                       // Delete
                1 => (Some(Vec::new()), 1),                           // empty Put
                2 => (Some(format!("merge-{i}").into_bytes()), 2),    // Merge
                _ => (Some(format!("value-{i}-xx").into_bytes()), 1), // Put
            };
            keys.push(key);
            vals.push(val);
            seqs.push(1000 - i);
            ops.push(op);
        }
        RecordBatch::try_new(
            Arc::new(sst_schema()),
            vec![
                Arc::new(BinaryArray::from_iter_values(keys.iter())),
                Arc::new(BinaryArray::from_iter(vals.iter().map(|v| v.as_deref()))),
                Arc::new(UInt64Array::from(seqs)),
                Arc::new(UInt8Array::from(ops)),
            ],
        )
        .unwrap()
    }

    /// D2 post-walk assertion: capture ALL ranges first, resolve only after
    /// the walk completes, then compare byte-for-byte with `for_each_row`.
    #[test]
    fn s2_ranges_twin_matches_for_each_row_in_batch_v1() {
        let batch = s2_v1_test_batch();

        let mut want: Vec<SstScanRow> = Vec::new();
        for_each_row_in_batch(&batch, |v| {
            want.push((
                v.key.to_vec(),
                v.value.map(|b| b.to_vec()),
                v.sequence,
                v.op_type,
            ));
            Ok(())
        })
        .unwrap();
        assert!(!want.is_empty());

        let mut metas: Vec<RowRanges> = Vec::new();
        for_each_row_ranges_in_batch(&batch, |row| {
            // v1: BOTH key and value must be Block refs (arena unused, D2).
            assert!(matches!(row.key, SliceRef::Block { .. }));
            if let Some(v) = row.value {
                assert!(matches!(v, SliceRef::Block { .. }));
            }
            metas.push(row);
            Ok(())
        })
        .unwrap();

        let (key_buf, val_buf) = batch_key_value_data(&batch).unwrap();
        let got: Vec<_> = metas
            .iter()
            .map(|m| {
                (
                    m.key.resolve(&[], key_buf).to_vec(),
                    m.value.map(|v| v.resolve(&[], val_buf).to_vec()),
                    m.sequence,
                    m.op_type,
                )
            })
            .collect();
        assert_eq!(got, want);
    }

    /// S2-3 G1: the v1 seek-aware twin yields exactly the `key >= lower`
    /// suffix of the full walk, and the stop-callback truncates it.
    #[test]
    fn s2_ranges_from_twin_matches_filtered_full_walk_v1() {
        let batch = s2_v1_test_batch();
        let (key_buf, val_buf) = batch_key_value_data(&batch).unwrap();
        let mut all: Vec<SstScanRow> = Vec::new();
        for_each_row_in_batch(&batch, |v| {
            all.push((
                v.key.to_vec(),
                v.value.map(|b| b.to_vec()),
                v.sequence,
                v.op_type,
            ));
            Ok(())
        })
        .unwrap();

        for lower in [
            b"".as_ref(),
            b"user:0005".as_ref(),
            b"user:0005x".as_ref(),
            b"zzzz".as_ref(),
        ] {
            let want: Vec<SstScanRow> = all
                .iter()
                .filter(|r| r.0.as_slice() >= lower)
                .cloned()
                .collect();
            let mut metas: Vec<RowRanges> = Vec::new();
            for_each_row_ranges_in_batch_from(&batch, lower, |row| {
                metas.push(row);
                Ok(true)
            })
            .unwrap();
            let got: Vec<SstScanRow> = metas
                .iter()
                .map(|m| {
                    (
                        m.key.resolve(&[], key_buf).to_vec(),
                        m.value.map(|v| v.resolve(&[], val_buf).to_vec()),
                        m.sequence,
                        m.op_type,
                    )
                })
                .collect();
            assert_eq!(got, want, "lower={lower:?}");

            // Stop truncation.
            if want.len() > 1 {
                let stop_after = want.len() / 2;
                let mut n = 0usize;
                for_each_row_ranges_in_batch_from(&batch, lower, |_row| {
                    n += 1;
                    Ok(n < stop_after)
                })
                .unwrap();
                assert_eq!(n, stop_after);
            }
        }
    }

    /// `DecodedBlock` dispatch: the Arrow arm must leave the caller's arena
    /// untouched (v1 keys are batch-stable), and `key_value_buffers` must
    /// hand out the buffers the Block refs resolve against — for BOTH block
    /// formats, byte-equal to `for_each_row`.
    #[test]
    fn s2_decoded_block_ranges_dispatch_both_formats() {
        // v1 Arrow arm.
        let block = DecodedBlock::Arrow(s2_v1_test_batch());
        let mut arena = b"CALLER-OWNED".to_vec();
        let arena_before = arena.clone();
        let mut metas: Vec<RowRanges> = Vec::new();
        block
            .for_each_row_ranges(&mut arena, |_, row| {
                metas.push(row);
                Ok(())
            })
            .unwrap();
        assert_eq!(arena, arena_before, "v1 walk must not touch the arena");
        let mut want: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        block
            .for_each_row(|v| {
                want.push((v.key.to_vec(), v.value.map(|b| b.to_vec())));
                Ok(())
            })
            .unwrap();
        let (kb, vb) = block.key_value_buffers().unwrap();
        let got: Vec<_> = metas
            .iter()
            .map(|m| {
                (
                    m.key.resolve(&arena, kb).to_vec(),
                    m.value.map(|v| v.resolve(&arena, vb).to_vec()),
                )
            })
            .collect();
        assert_eq!(got, want);

        // v2 KV arm (via the kv encoder; keys land in the arena APPENDED
        // after the pre-seed).
        let kv_bytes =
            crate::sst::kv_block::encode_kv_data_block(&s2_v1_test_batch(), CompressionType::Lz4)
                .unwrap();
        let kv = crate::sst::kv_block::KvBlock::decode(&kv_bytes, true).unwrap();
        let block = DecodedBlock::Kv(Arc::new(kv));
        let mut arena = b"PRESEED".to_vec();
        let mut metas: Vec<RowRanges> = Vec::new();
        block
            .for_each_row_ranges(&mut arena, |_, row| {
                metas.push(row);
                Ok(())
            })
            .unwrap();
        let mut want: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
        block
            .for_each_row(|v| {
                want.push((v.key.to_vec(), v.value.map(|b| b.to_vec())));
                Ok(())
            })
            .unwrap();
        let (kb, vb) = block.key_value_buffers().unwrap();
        let got: Vec<_> = metas
            .iter()
            .map(|m| {
                (
                    m.key.resolve(&arena, kb).to_vec(),
                    m.value.map(|v| v.resolve(&arena, vb).to_vec()),
                )
            })
            .collect();
        assert_eq!(got, want);
    }
}

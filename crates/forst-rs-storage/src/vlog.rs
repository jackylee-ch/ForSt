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

//! FRS-WA-V2 (2026-06-13 write-path redesign survey §3.1/§6 stage V2):
//! append-only VALUE-LOG segments + value pointers — the WiscKey-class
//! KV-separation substrate for unbounded value-heavy CFs.
//!
//! **LIVE** as of V2a-2/V2b/V2c (survey §10.2/§10.3/§12): the engine flush
//! path (`FlushJob::run_kv`) and compaction GC (`KvGcState::relocate`) write
//! through these primitives when `FRS_KV_SEPARATION` is on (default OFF).
//! FRS-WA-V2c adds per-segment value compression (the codec is stamped per
//! record so the reader is self-describing) — KV-separated values bypass SST
//! block compression, so without this the big bytes would hit disk
//! UNCOMPRESSED and forfeit the M5 compression-parity win on exactly the
//! dominant byte source. The two primitives the V2 write path composes:
//!
//! 1. [`ValuePointer`] — the fixed-size record stored INSIDE the key-LSM in
//!    place of a large value. Survey §3.1 arithmetic: with only
//!    `key + pointer` (~36 B) riding flush+compaction instead of ~236 B,
//!    the measured key-LSM write-amp was 2.10× (cell E′) and the assembled
//!    full-workload write-amp ≈ 1.36×.
//! 2. [`VlogWriter`] / [`VlogReader`] — append-once segment files
//!    (`<seg_id>.vlog`) holding the value bytes, CRC-framed + codec-tagged
//!    per record (FRS-WA-V2c).
//!    Segments are immutable once sealed — the same lifecycle class as
//!    SSTs, so checkpoint = link, restore = adopt, and GC composes with the
//!    V1 machinery: for lifecycle CFs, value-log GC **is** whole-segment
//!    expiry (survey §3.1 "death order ≈ arrival order"); for unbounded
//!    CFs, the BlobDB-style compaction-coupled relocation (age cutoff)
//!    rewrites pointers during key-LSM compaction (V2 wiring, not here).
//!
//! Exclusions carried from the survey: merge-operand CFs cannot separate
//! (P12 — concat needs bytes, pointers don't concat), and values below a
//! `min_blob_size`-style threshold stay inline (the cold-S3 dereference
//! bound). Both are WRITE-PATH policies enforced at the call site
//! (`DbImpl::kv_sep_spec_for`).

use std::path::{Path, PathBuf};
use std::sync::Arc;

use forst_rs_common::{crc32c, CompressionType, ForstError, ForstResult};
use forst_rs_io::{FileSystem, RandomAccessFile, WriteMode};

use crate::sst::compression::{compress, decompress};

/// Tag byte opening an encoded [`ValuePointer`]. NOTE: no assumption is
/// made that user values cannot start with this byte — discrimination by
/// tag alone is NOT sound at the read path. The V2 write path must record
/// inline-vs-separated per entry (e.g. a distinct op kind or per-CF "all
/// values ≥ threshold are separated" rule); [`ValuePointer::decode`]'s
/// `None` arm exists for the codec round-trip, not as the production
/// discriminator.
pub const VALUE_POINTER_TAG: u8 = 0xF7;

/// Encoded size of a [`ValuePointer`]: tag(1) + segment_id(8) + offset(8) +
/// len(4) = 21 bytes.
pub const VALUE_POINTER_LEN: usize = 21;

/// FRS-WA-V2c: per-record framing overhead inside a segment:
/// stored_len(4) + crc(4) + codec(1) + uncompressed_len(4) = 13 bytes. The
/// `codec` + `uncompressed_len` fields (added 2026-06-13 cycle 2 for vlog
/// compression — survey §10.1 item 3) make each record SELF-DESCRIBING: a
/// reader decodes the payload without any external per-segment metadata, so
/// the read path (`VlogReader`) needs no codec parameter and mixed-codec
/// segments (a compaction relocating uncompressed legacy records into a
/// compressed output, or vice-versa) round-trip correctly.
pub const VLOG_RECORD_HEADER: usize = 13;

/// A reference to value bytes living in an append-only value-log segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValuePointer {
    /// Segment file id (`<segment_id>.vlog` under the db dir).
    pub segment_id: u64,
    /// Byte offset of the RECORD HEADER inside the segment.
    pub offset: u64,
    /// Length of the STORED (on-disk, possibly compressed) value payload —
    /// excludes the record header. FRS-WA-V2c: with compression this is the
    /// compressed byte count; the logical value length lives in the record
    /// header (`uncompressed_len`) and is recovered by [`VlogReader::get`].
    /// All space-amp / GC liveness accounting is in these STORED bytes (one
    /// consistent unit — the actual disk footprint), so a compressed vlog's
    /// `live_bytes` reflects real on-disk space.
    pub len: u32,
}

impl ValuePointer {
    /// Serializes to the fixed [`VALUE_POINTER_LEN`] wire form.
    pub fn encode(&self) -> [u8; VALUE_POINTER_LEN] {
        let mut out = [0u8; VALUE_POINTER_LEN];
        out[0] = VALUE_POINTER_TAG;
        out[1..9].copy_from_slice(&self.segment_id.to_le_bytes());
        out[9..17].copy_from_slice(&self.offset.to_le_bytes());
        out[17..21].copy_from_slice(&self.len.to_le_bytes());
        out
    }

    /// Decodes the wire form; `None` when `bytes` is not a tagged pointer
    /// (wrong length or tag) — the inline-value arm.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != VALUE_POINTER_LEN || bytes[0] != VALUE_POINTER_TAG {
            return None;
        }
        Some(Self {
            segment_id: u64::from_le_bytes(bytes[1..9].try_into().ok()?),
            offset: u64::from_le_bytes(bytes[9..17].try_into().ok()?),
            len: u32::from_le_bytes(bytes[17..21].try_into().ok()?),
        })
    }
}

/// Path of value-log segment `segment_id` under `db_path`:
/// `<db_path>/<segment_id:06>.vlog`.
pub fn vlog_segment_path(db_path: &Path, segment_id: u64) -> PathBuf {
    db_path.join(format!("{:06}.vlog", segment_id))
}

/// Append-only writer for ONE value-log segment. Records are framed
/// `[stored_len: u32 LE][crc32c(stored): u32 LE][codec: u8][uncompressed_len:
/// u32 LE][stored_payload]` (FRS-WA-V2c — `stored_payload` is the value
/// compressed with this segment's codec; the per-record `codec` byte makes
/// the record self-describing so [`VlogReader`] needs no codec parameter).
/// The writer returns a [`ValuePointer`] per append (its `len` = `stored_len`).
/// Call [`Self::sync`] before publishing any pointer durably (the V2 write
/// path orders vlog-sync BEFORE the key-LSM write, mirroring WiscKey).
pub struct VlogWriter {
    file: Box<dyn forst_rs_io::WritableFile>,
    segment_id: u64,
    offset: u64,
    /// FRS-WA-V2b: STORED payload bytes appended (excludes record headers) —
    /// the initial `live_bytes` of the segment's manifest entry. FRS-WA-V2c:
    /// "stored" = the on-disk (post-compression) byte count, so the
    /// space-amp / GC accounting reflects real disk footprint.
    payload_bytes: u64,
    /// FRS-WA-V2c: per-segment value codec. Values are compressed with this
    /// before framing; the codec is recorded PER RECORD so the reader is
    /// self-describing. `None` = passthrough (the pre-V2c behaviour).
    compression: CompressionType,
}

impl VlogWriter {
    /// Creates segment `segment_id` (fails if it exists — segments are
    /// allocate-once like SST file numbers). Values are written UNCOMPRESSED
    /// ([`CompressionType::None`]); use [`Self::create_with_compression`] to
    /// compress value payloads (survey §10.1 item 3 — KV-separated values
    /// bypass SST block compression, so an uncompressed vlog would forfeit
    /// the compression win on exactly the big bytes).
    pub fn create(fs: &dyn FileSystem, db_path: &Path, segment_id: u64) -> ForstResult<Self> {
        Self::create_with_compression(fs, db_path, segment_id, CompressionType::None)
    }

    /// FRS-WA-V2c: creates a value-log segment whose value payloads are
    /// compressed with `compression`. The codec is stamped per record, so
    /// the reader recovers each value without external metadata and segments
    /// produced under different codecs (e.g. a relocation output) interoperate.
    pub fn create_with_compression(
        fs: &dyn FileSystem,
        db_path: &Path,
        segment_id: u64,
        compression: CompressionType,
    ) -> ForstResult<Self> {
        let path = vlog_segment_path(db_path, segment_id);
        let file = fs.open_writable_file(&path, WriteMode::CreateNew)?;
        Ok(Self {
            file,
            segment_id,
            offset: 0,
            payload_bytes: 0,
            compression,
        })
    }

    /// Appends one value; returns its pointer. FRS-WA-V2c: the value is
    /// compressed with this segment's codec before framing; `ptr.len` is the
    /// STORED (compressed) byte count and the record header carries the
    /// codec + uncompressed length so [`VlogReader::get`] recovers the value.
    pub fn append(&mut self, value: &[u8]) -> ForstResult<ValuePointer> {
        let uncompressed_len = u32::try_from(value.len())
            .map_err(|_| ForstError::invalid_argument("vlog value exceeds u32::MAX bytes"))?;
        let stored: Vec<u8> = compress(value, self.compression)?;
        let stored_len = u32::try_from(stored.len()).map_err(|_| {
            ForstError::invalid_argument("vlog stored value exceeds u32::MAX bytes")
        })?;
        let ptr = ValuePointer {
            segment_id: self.segment_id,
            offset: self.offset,
            len: stored_len,
        };
        self.file.append(&stored_len.to_le_bytes())?;
        self.file.append(&crc32c(&stored).to_le_bytes())?;
        self.file.append(&[self.compression as u8])?;
        self.file.append(&uncompressed_len.to_le_bytes())?;
        self.file.append(&stored)?;
        self.offset += (VLOG_RECORD_HEADER + stored.len()) as u64;
        self.payload_bytes += stored.len() as u64;
        Ok(ptr)
    }

    /// FRS-WA-V2b: STORED payload bytes appended so far (excludes headers) —
    /// the segment's initial manifest `live_bytes` (on-disk byte count).
    pub fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Bytes appended so far (the segment-roll threshold input).
    pub fn size(&self) -> u64 {
        self.offset
    }

    /// Flush + fsync. Must complete before any pointer into this segment is
    /// published to the key-LSM.
    pub fn sync(&mut self) -> ForstResult<()> {
        self.file.flush()?;
        self.file.sync()
    }
}

/// FRS-WA-V2a-2 deref-cost: forward read-ahead granule for [`VlogReader`].
/// Scan-path derefs have strong intra-segment locality (a flush appends
/// values in KEY order, so the rows one prefix-scan emits from one segment
/// sit contiguously) — caching the last-read chunk turns ~chunk/record
/// consecutive derefs into ONE pread. Random point-gets pay at most one
/// page-cache-warm 64 KiB read + memcpy per miss.
const VLOG_READ_CHUNK: usize = 64 * 1024;

/// FRS-VLOG-COALESCE: max spanning byte range a single coalesced batched read
/// ([`VlogReader::get_coalesced`]) will buffer. A flush segment's values are
/// packed contiguously in append (= offset) order, so a scattered batch's
/// offset-sorted group spans ≈ the sum of its record sizes; this cap only trips
/// for pathologically sparse pointer sets, which fall back to per-record reads.
/// 16 MiB bounds the transient coalesce buffer well under a typical segment.
const VLOG_COALESCE_MAX_SPAN: usize = 16 * 1024 * 1024;

/// Random-access reader for a value-log segment (the post-visibility
/// dereference of the V2 read path).
pub struct VlogReader {
    file: Box<dyn RandomAccessFile>,
    /// Last-read chunk `(start_offset, bytes)` — see [`VLOG_READ_CHUNK`].
    chunk: std::sync::Mutex<Option<(u64, Vec<u8>)>>,
}

impl VlogReader {
    pub fn open(fs: &dyn FileSystem, db_path: &Path, segment_id: u64) -> ForstResult<Self> {
        let path = vlog_segment_path(db_path, segment_id);
        Ok(Self {
            file: fs.open_random_access_file(&path)?,
            chunk: std::sync::Mutex::new(None),
        })
    }

    /// Reads + CRC-verifies the value `ptr` points at. `ptr.segment_id` must
    /// match the opened segment (caller routes by id).
    pub fn get(&self, ptr: &ValuePointer) -> ForstResult<Vec<u8>> {
        let total = VLOG_RECORD_HEADER + ptr.len as usize;
        // Oversized records bypass the chunk cache (one direct pread).
        if total > VLOG_READ_CHUNK {
            let mut record = vec![0u8; total];
            let n = self.file.read_at(ptr.offset, &mut record)?;
            if n != total {
                return Err(ForstError::corruption("vlog record short read"));
            }
            return Self::parse_record(&record, ptr);
        }
        let mut guard = self.chunk.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((start, bytes)) = guard.as_ref() {
            if ptr.offset >= *start && ptr.offset + total as u64 <= *start + bytes.len() as u64 {
                let lo = (ptr.offset - *start) as usize;
                return Self::parse_record(&bytes[lo..lo + total], ptr);
            }
        }
        // Miss: read forward from the record start (scans walk forward).
        let mut buf = vec![0u8; VLOG_READ_CHUNK];
        let n = self.file.read_at(ptr.offset, &mut buf)?;
        if n < total {
            return Err(ForstError::corruption("vlog record short read"));
        }
        buf.truncate(n);
        let out = Self::parse_record(&buf[..total], ptr);
        *guard = Some((ptr.offset, buf));
        out
    }

    /// FRS-VLOG-COALESCE: coalesced batched deref of MANY pointers into THIS
    /// segment in ONE pass. `ptrs` must all target this segment and be SORTED by
    /// `offset` (the caller groups by `segment_id` + sorts). The reader computes
    /// the spanning byte range `[first.offset, last.offset + last_total)` and
    /// issues a SINGLE positioned read for it, then slices + CRC-verifies +
    /// decompresses each value out of the in-memory span. On local FS this turns
    /// N scattered chunk reads into one sequential read; on the disagg/remote
    /// path it collapses N scattered ranged GETs into ~1 ranged GET per segment
    /// (the disagg-critical win). Returns one value per input pointer, IN INPUT
    /// (offset) ORDER — the caller scatters them back to key slots.
    ///
    /// Records that individually exceed the span budget are read with the
    /// per-record `get` path (one direct pread each) so a single huge value can
    /// never force an unbounded coalesce buffer. An empty `ptrs` is a no-op.
    pub fn get_coalesced(&self, ptrs: &[&ValuePointer]) -> ForstResult<Vec<Vec<u8>>> {
        if ptrs.is_empty() {
            return Ok(Vec::new());
        }
        if ptrs.len() == 1 {
            return Ok(vec![self.get(ptrs[0])?]);
        }
        // The spanning range across the (offset-sorted) group.
        let first_off = ptrs[0].offset;
        let last = ptrs[ptrs.len() - 1];
        let last_total = VLOG_RECORD_HEADER + last.len as usize;
        let span_end = last.offset + last_total as u64;
        let span_len = (span_end - first_off) as usize;

        // Guard: if the offset-sorted group spans more than VLOG_COALESCE_MAX_SPAN
        // (sparse pointers far apart in a large segment), fall back to per-record
        // reads so the coalesce buffer stays bounded. The hot case — a flush
        // segment dereffed by a scattered batch — has values packed contiguously,
        // so the span ~= sum of record sizes and this guard does not trip.
        if span_len > VLOG_COALESCE_MAX_SPAN {
            return ptrs.iter().map(|p| self.get(p)).collect();
        }

        let mut span = vec![0u8; span_len];
        let mut filled = 0usize;
        while filled < span_len {
            let n = self
                .file
                .read_at(first_off + filled as u64, &mut span[filled..])?;
            if n == 0 {
                return Err(ForstError::corruption(
                    "vlog coalesced span short read (EOF before span end)",
                ));
            }
            filled += n;
        }
        let mut out = Vec::with_capacity(ptrs.len());
        for p in ptrs {
            let total = VLOG_RECORD_HEADER + p.len as usize;
            let lo = (p.offset - first_off) as usize;
            let hi = lo + total;
            if hi > span.len() {
                return Err(ForstError::corruption(
                    "vlog coalesced record extends past span",
                ));
            }
            out.push(Self::parse_record(&span[lo..hi], p)?);
        }
        Ok(out)
    }

    /// Validates one framed record (`record` spans exactly header + stored
    /// payload) against `ptr` and returns the owned, DECOMPRESSED value.
    /// FRS-WA-V2c: the record is self-describing — a per-record codec byte +
    /// uncompressed length drive decompression with a trusted size bound.
    fn parse_record(record: &[u8], ptr: &ValuePointer) -> ForstResult<Vec<u8>> {
        let stored_len = u32::from_le_bytes(record[0..4].try_into().expect("4 bytes"));
        let stored_crc = u32::from_le_bytes(record[4..8].try_into().expect("4 bytes"));
        let codec_byte = record[8];
        let uncompressed_len = u32::from_le_bytes(record[9..13].try_into().expect("4 bytes"));
        if stored_len != ptr.len {
            return Err(ForstError::corruption(format!(
                "vlog pointer/record length mismatch: pointer {} record {}",
                ptr.len, stored_len
            )));
        }
        let stored = &record[VLOG_RECORD_HEADER..];
        if crc32c(stored) != stored_crc {
            return Err(ForstError::corruption("vlog payload checksum mismatch"));
        }
        let compression = match codec_byte {
            0 => CompressionType::None,
            1 => CompressionType::Lz4,
            2 => CompressionType::Zstd,
            other => {
                return Err(ForstError::corruption(format!(
                    "vlog record carries unknown compression codec {other}"
                )))
            }
        };
        // `decompress` validates the output against `uncompressed_len` (the
        // trusted size bound — defends against a crafted compressed frame).
        decompress(stored, compression, uncompressed_len as usize)
    }
}

/// FRS-WA-V2a-2-LRU (q9 KV-sep OOM fix, 2026-06-14): default cap on the
/// number of open [`VlogReader`] handles a DB keeps resident. See the
/// root-cause note `docs/superpowers/specs/2026-06-14-q9-kvsep-oom-rootcause.md`:
/// the prior cache was an UNBOUNDED `HashMap<u64, Arc<VlogReader>>` — under
/// `FRS_KV_SEPARATION` + a scattered-death join (q9), segments never reach
/// `live_bytes == 0`, so the reader set (open file handle + 64 KiB chunk
/// buffer each) grew with the run and busted the cgroup. A generous default
/// keeps the working set warm; re-open on a miss is always safe because a
/// segment is immutable once any pointer to it is version-visible.
pub const DEFAULT_VLOG_READER_CACHE_CAP: usize = 2048;

/// FRS-AKV-B1 (adaptive KV-sep, 2026-06-14): the charged RESIDENT byte cost
/// attributed to one open [`VlogReader`] for the byte-budget bound. A reader
/// holds an OS file handle plus a lazily-filled chunk buffer of at most
/// `VLOG_READ_CHUNK` (64 KiB) bytes; we charge the full chunk granule (the
/// dominant, worst-case term) plus a small fixed overhead for the handle +
/// `Arc`/map bookkeeping. The charge is intentionally a fixed UPPER bound per
/// reader so `resident_bytes == len() * CHARGE` is exact and the budget can
/// never be under-counted (a deref that has not yet filled its chunk simply
/// uses less than its charge — conservative, never OOMs).
pub const VLOG_READER_CHARGE_BYTES: usize = VLOG_READ_CHUNK + 4096;

/// Bounded LRU cache of open [`VlogReader`] handles, keyed by segment id.
///
/// **Correctness:** byte-identical to an unbounded cache for every read — a
/// vlog segment is immutable once published, so an evicted reader simply
/// re-opens on the next access and returns the same bytes. Eviction drops the
/// victim `Arc<VlogReader>` (closing its file handle + freeing its chunk
/// buffer once no in-flight `get` still holds a clone).
///
/// **Bound:** the resident reader count never exceeds `cap` (proved by
/// [`Self::len`] in tests). Resident vlog-reader cost is therefore `O(cap)`,
/// independent of the number of segments the run ever touches.
///
/// **Discipline:** mirrors the lazy-LRU recency deque used by the SST block
/// `local_cache` (`local_cache.rs`) — a `VecDeque<u64>` records access order,
/// dedup happens at eviction time, and a victim is only evicted once it is no
/// longer the live MRU entry. A `cap == 0` configuration is treated as
/// "uncapped" so a mini-bench / A-B can reproduce the pre-fix behaviour.
pub struct VlogReaderCache {
    inner: std::sync::Mutex<VlogReaderCacheInner>,
    cap: usize,
    /// FRS-AKV-B1: charged-byte budget for resident readers (the q9 never-OOM
    /// bound the count cap alone could not provide). `usize::MAX` = no byte
    /// bound (default / pre-AKV behaviour — only the count `cap` applies).
    /// When set, eviction also trims to `byte_budget`, so resident vlog cost
    /// is `min(cap, byte_budget / VLOG_READER_CHARGE_BYTES)` readers,
    /// independent of how many segments the run ever touches.
    byte_budget: usize,
}

struct VlogReaderCacheInner {
    /// O(1) lookup of resident readers by segment id.
    readers: std::collections::HashMap<u64, Arc<VlogReader>>,
    /// Access-recency order (front = LRU victim, back = MRU). May hold stale
    /// duplicates; the live entry is the one still present in `readers`.
    lru: std::collections::VecDeque<u64>,
}

impl VlogReaderCache {
    /// Builds a cache bounded to `cap` resident readers. `cap == 0` means
    /// UNCAPPED (used only by the mini-bench / pre-fix A-B arm). No byte
    /// budget (byte bound disabled) — see [`Self::with_capacity_and_budget`].
    pub fn with_capacity(cap: usize) -> Self {
        Self::with_capacity_and_budget(cap, usize::MAX)
    }

    /// FRS-AKV-B1: builds a cache bounded by BOTH a resident-reader `cap`
    /// (count) AND a charged `byte_budget` (bytes). `byte_budget == usize::MAX`
    /// disables the byte bound (count-only, the pre-AKV behaviour, default).
    /// `cap == 0` is uncapped on count; the two bounds compose (eviction trims
    /// to whichever is tighter).
    pub fn with_capacity_and_budget(cap: usize, byte_budget: usize) -> Self {
        Self {
            inner: std::sync::Mutex::new(VlogReaderCacheInner {
                readers: std::collections::HashMap::new(),
                lru: std::collections::VecDeque::new(),
            }),
            cap,
            byte_budget,
        }
    }

    /// The configured capacity (0 == uncapped).
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// FRS-AKV-B1: the configured charged-byte budget (`usize::MAX` = none).
    pub fn byte_budget(&self) -> usize {
        self.byte_budget
    }

    /// FRS-AKV-B1: the charged RESIDENT bytes for the current reader set
    /// (`len() * VLOG_READER_CHARGE_BYTES`). This is the value bounded by
    /// `byte_budget`. Exact (each reader charges a fixed upper bound).
    pub fn resident_bytes(&self) -> usize {
        self.len().saturating_mul(VLOG_READER_CHARGE_BYTES)
    }

    /// Number of resident readers. Never exceeds `cap` when `cap > 0`.
    pub fn len(&self) -> usize {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .readers
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the resident reader for `segment_id` if present, promoting it
    /// to MRU. Used for the lock-free fast path before an open.
    pub fn get(&self, segment_id: u64) -> Option<Arc<VlogReader>> {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(r) = g.readers.get(&segment_id).cloned() {
            g.lru.push_back(segment_id);
            Some(r)
        } else {
            None
        }
    }

    /// Returns the cached reader for `segment_id`, or opens one via `open`,
    /// inserting it and evicting the LRU victim if the cap is exceeded. The
    /// `open` closure runs WITHOUT the cache lock held; a concurrent open of
    /// the same id resolves to a single resident reader (first writer wins,
    /// the loser's freshly-opened handle is dropped).
    pub fn get_or_open<F>(&self, segment_id: u64, open: F) -> ForstResult<Arc<VlogReader>>
    where
        F: FnOnce() -> ForstResult<VlogReader>,
    {
        if let Some(r) = self.get(segment_id) {
            return Ok(r);
        }
        let opened = Arc::new(open()?);
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        // Double-check: a racing opener may have installed it meanwhile.
        if let Some(r) = g.readers.get(&segment_id).cloned() {
            g.lru.push_back(segment_id);
            return Ok(r);
        }
        g.readers.insert(segment_id, opened.clone());
        g.lru.push_back(segment_id);
        Self::evict_to_cap(&mut g, self.cap, self.byte_budget);
        Ok(opened)
    }

    /// Explicitly drops the reader for a dead/retired segment (called when a
    /// segment reaches `live_bytes == 0` and is unlinked). Idempotent.
    pub fn remove(&self, segment_id: u64) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.readers.remove(&segment_id);
        // The stale `lru` entry (if any) is pruned lazily at eviction time.
    }

    /// Lazy-LRU eviction: while over the EFFECTIVE max resident count, pop the
    /// front of the recency deque and evict it IFF it is still the live entry
    /// AND not a stale duplicate (a later access re-pushed it to the back).
    /// Bounded work per call: each pop either evicts (reduces `readers`) or
    /// skips a stale id.
    ///
    /// FRS-AKV-B1: the effective max is `min(cap, byte_budget / charge)` — the
    /// byte budget is translated to a reader count via the fixed per-reader
    /// charge so resident bytes (`len * charge`) never exceed `byte_budget`.
    /// `cap == 0` (uncapped count) with a finite budget enforces ONLY the byte
    /// bound; both `MAX`/`0` (no bounds) is the pre-fix uncapped behaviour.
    fn evict_to_cap(g: &mut VlogReaderCacheInner, cap: usize, byte_budget: usize) {
        // Translate the byte budget to a max resident-reader count.
        let budget_count = if byte_budget == usize::MAX {
            usize::MAX
        } else {
            // floor div: keep resident_bytes = count*charge <= byte_budget.
            byte_budget / VLOG_READER_CHARGE_BYTES
        };
        let count_cap = if cap == 0 { usize::MAX } else { cap };
        let effective = count_cap.min(budget_count);
        if effective == usize::MAX {
            return; // no bound active (mini-bench / pre-fix arm)
        }
        while g.readers.len() > effective {
            let Some(victim) = g.lru.pop_front() else {
                break; // recency deque drained; nothing more to evict
            };
            // Skip stale duplicates: an id later in the deque means this is
            // not the true LRU position for `victim`.
            if g.lru.contains(&victim) {
                continue;
            }
            // Live victim → evict (drop the Arc; handle + buffer free once
            // any in-flight `get` clone is released).
            g.readers.remove(&victim);
        }
        // Keep the recency deque from growing without bound under heavy
        // re-touch: if it has accumulated many stale duplicates, rebuild it
        // from the resident set (mirrors local_cache.rs's stale-compaction).
        if g.lru.len() > g.readers.len().saturating_mul(2) + 64 {
            let mut fresh: std::collections::VecDeque<u64> =
                std::collections::VecDeque::with_capacity(g.readers.len());
            let mut seen = std::collections::HashSet::with_capacity(g.readers.len());
            for id in g.lru.iter().rev() {
                if g.readers.contains_key(id) && seen.insert(*id) {
                    fresh.push_front(*id);
                }
            }
            g.lru = fresh;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_io::MemoryFileSystem;

    #[test]
    fn test_pointer_codec_roundtrip_and_inline_discrimination() {
        let p = ValuePointer {
            segment_id: 7,
            offset: 12_345,
            len: 200,
        };
        let enc = p.encode();
        assert_eq!(enc.len(), VALUE_POINTER_LEN);
        assert_eq!(ValuePointer::decode(&enc), Some(p));
        // Inline arms: wrong length / wrong tag both decode None.
        assert_eq!(ValuePointer::decode(&enc[..20]), None);
        let mut bad_tag = enc;
        bad_tag[0] ^= 0xFF;
        assert_eq!(ValuePointer::decode(&bad_tag), None);
    }

    #[test]
    fn test_vlog_append_sync_read_roundtrip() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let mut w = VlogWriter::create(&fs, dir, 1).unwrap();
        let values: Vec<Vec<u8>> = (0..50u32)
            .map(|i| vec![i as u8; (i as usize % 7) * 33 + 1])
            .collect();
        let ptrs: Vec<ValuePointer> = values.iter().map(|v| w.append(v).unwrap()).collect();
        w.sync().unwrap();
        assert_eq!(
            w.size(),
            values
                .iter()
                .map(|v| (VLOG_RECORD_HEADER + v.len()) as u64)
                .sum::<u64>()
        );
        let r = VlogReader::open(&fs, dir, 1).unwrap();
        for (v, p) in values.iter().zip(&ptrs) {
            assert_eq!(&r.get(p).unwrap(), v, "value at {:?}", p);
        }
    }

    /// FRS-VLOG-COALESCE: `get_coalesced` returns each pointer's value
    /// byte-identically to per-`get`, IN INPUT ORDER, for an offset-sorted group
    /// — incl. compressed segments. Empty = []; single = one value. The caller
    /// (engine) groups by segment + sorts by offset before calling, so this test
    /// feeds the offset-sorted run a real batch would produce.
    #[test]
    fn test_vlog_get_coalesced_byte_identical_and_in_order() {
        for codec in [CompressionType::None, CompressionType::Lz4] {
            let fs = MemoryFileSystem::new();
            let dir = Path::new("/db");
            fs.create_dir_all(dir).unwrap();
            let mut w = VlogWriter::create_with_compression(&fs, dir, 9, codec).unwrap();
            // Mixed sizes; semi-compressible so lz4 has work but framing varies.
            let values: Vec<Vec<u8>> = (0..64u32)
                .map(|i| {
                    let n = (i as usize % 11) * 40 + 1;
                    (0..n).map(|j| ((i as usize + j) % 251) as u8).collect()
                })
                .collect();
            let ptrs: Vec<ValuePointer> = values.iter().map(|v| w.append(v).unwrap()).collect();
            w.sync().unwrap();
            let r = VlogReader::open(&fs, dir, 9).unwrap();

            // Empty + single no-op.
            assert!(r.get_coalesced(&[]).unwrap().is_empty());
            assert_eq!(
                r.get_coalesced(&[&ptrs[7]]).unwrap(),
                vec![values[7].clone()]
            );

            // Full offset-sorted group (== append order) — one ranged read.
            let refs: Vec<&ValuePointer> = ptrs.iter().collect();
            let got = r.get_coalesced(&refs).unwrap();
            assert_eq!(got, values, "coalesced group must match per-get values");

            // A scattered SUBSET, then sorted by offset (what the engine does):
            // result is in the (sorted) input order and equals per-get.
            let mut subset: Vec<usize> = vec![40, 3, 17, 0, 63, 28, 9];
            subset.sort_by_key(|&i| ptrs[i].offset);
            let sub_refs: Vec<&ValuePointer> = subset.iter().map(|&i| &ptrs[i]).collect();
            let sub_got = r.get_coalesced(&sub_refs).unwrap();
            for (slot, &i) in sub_got.iter().zip(&subset) {
                assert_eq!(slot, &values[i]);
                assert_eq!(slot, &r.get(&ptrs[i]).unwrap());
            }
        }
    }

    #[test]
    fn test_vlog_detects_corruption_and_bad_pointers() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let mut w = VlogWriter::create(&fs, dir, 2).unwrap();
        let p = w.append(b"hello-vlog").unwrap();
        w.sync().unwrap();

        // Pointer with the wrong length → length-mismatch corruption.
        let bad_len = ValuePointer { len: 4, ..p };
        let r = VlogReader::open(&fs, dir, 2).unwrap();
        assert!(r.get(&bad_len).is_err());

        // Pointer past EOF → short-read corruption.
        let past = ValuePointer {
            offset: 10_000,
            ..p
        };
        assert!(r.get(&past).is_err());

        // Good pointer still reads.
        assert_eq!(r.get(&p).unwrap(), b"hello-vlog");
    }

    /// FRS-WA-V2c: a compressed segment round-trips every value, the
    /// reader recovers the LOGICAL bytes, and a compressible payload's
    /// on-disk footprint (writer `size()`/`payload_bytes()`) is strictly
    /// smaller than the uncompressed sum — proving the vlog compresses the
    /// big bytes KV-separation diverts past SST block compression.
    #[test]
    fn test_vlog_compression_roundtrip_and_shrinks() {
        for codec in [CompressionType::Lz4, CompressionType::Zstd] {
            let fs = MemoryFileSystem::new();
            let dir = Path::new("/db");
            fs.create_dir_all(dir).unwrap();
            let mut w = VlogWriter::create_with_compression(&fs, dir, 10, codec).unwrap();
            // Low-entropy, NexMark-shaped values (repeated fields) compress well.
            let values: Vec<Vec<u8>> = (0..40u32)
                .map(|i| {
                    format!(
                        "{{\"auction\":{},\"bidder\":{},\"price\":100}}",
                        i % 7,
                        i % 5
                    )
                    .repeat(6)
                    .into_bytes()
                })
                .collect();
            let logical_total: usize = values.iter().map(|v| v.len()).sum();
            let ptrs: Vec<ValuePointer> = values.iter().map(|v| w.append(v).unwrap()).collect();
            w.sync().unwrap();
            // On-disk payload (excludes headers) must be smaller than logical.
            assert!(
                (w.payload_bytes() as usize) < logical_total,
                "{codec:?}: stored {} should be < logical {}",
                w.payload_bytes(),
                logical_total
            );
            let r = VlogReader::open(&fs, dir, 10).unwrap();
            for (v, p) in values.iter().zip(&ptrs) {
                assert_eq!(&r.get(p).unwrap(), v, "{codec:?} value at {p:?}");
            }
        }
    }

    /// FRS-WA-V2c: an empty value under a compressing codec round-trips
    /// (the 13-byte header + a tiny compressed frame; `record[13..]` is the
    /// stored payload, which the decompressor maps back to zero bytes).
    #[test]
    fn test_vlog_compression_empty_value() {
        for codec in [CompressionType::Lz4, CompressionType::Zstd] {
            let fs = MemoryFileSystem::new();
            let dir = Path::new("/db");
            fs.create_dir_all(dir).unwrap();
            let mut w = VlogWriter::create_with_compression(&fs, dir, 30, codec).unwrap();
            let p = w.append(b"").unwrap();
            w.sync().unwrap();
            let r = VlogReader::open(&fs, dir, 30).unwrap();
            assert_eq!(r.get(&p).unwrap(), b"", "{codec:?} empty value");
        }
    }

    /// FRS-WA-V2c: records are self-describing, so MIXED codecs in one
    /// segment (the relocation case: a compaction output written under a
    /// different codec than the source) all read back correctly.
    #[test]
    fn test_vlog_records_self_describing_across_codecs() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        // One reader cannot mix codecs within a single writer (codec is
        // per-writer), but two segments written under different codecs both
        // decode via the per-record tag with the SAME reader logic.
        let mut none_w =
            VlogWriter::create_with_compression(&fs, dir, 20, CompressionType::None).unwrap();
        let mut lz4_w =
            VlogWriter::create_with_compression(&fs, dir, 21, CompressionType::Lz4).unwrap();
        let payload = b"the quick brown fox the quick brown fox the quick brown fox".to_vec();
        let p_none = none_w.append(&payload).unwrap();
        let p_lz4 = lz4_w.append(&payload).unwrap();
        none_w.sync().unwrap();
        lz4_w.sync().unwrap();
        assert_eq!(
            VlogReader::open(&fs, dir, 20)
                .unwrap()
                .get(&p_none)
                .unwrap(),
            payload
        );
        assert_eq!(
            VlogReader::open(&fs, dir, 21).unwrap().get(&p_lz4).unwrap(),
            payload
        );
    }

    #[test]
    fn test_vlog_segment_allocate_once() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let _w = VlogWriter::create(&fs, dir, 3).unwrap();
        assert!(
            VlogWriter::create(&fs, dir, 3).is_err(),
            "segment ids are allocate-once"
        );
    }

    // ---- FRS-WA-V2a-2-LRU: bounded vlog-reader cache (q9 OOM fix) ----

    /// Writes `n_segments` single-value segments to `dir`, returning the
    /// (segment_id, pointer, expected_bytes) for each so a test can read back
    /// and byte-compare across eviction.
    fn write_kvsep_segments(
        fs: &MemoryFileSystem,
        dir: &Path,
        n_segments: u64,
    ) -> Vec<(u64, ValuePointer, Vec<u8>)> {
        let mut out = Vec::with_capacity(n_segments as usize);
        for seg in 1..=n_segments {
            // Distinct payload per segment (carries the segment id) so an
            // eviction-induced re-open that returned the WRONG segment's bytes
            // would be caught.
            let payload: Vec<u8> = (0..200u32)
                .map(|i| (seg.wrapping_mul(31).wrapping_add(i as u64)) as u8)
                .collect();
            let mut w = VlogWriter::create(fs, dir, seg).unwrap();
            let p = w.append(&payload).unwrap();
            w.sync().unwrap();
            out.push((seg, p, payload));
        }
        out
    }

    /// (b) The cache never exceeds its cap when the touched-segment set far
    /// exceeds the cap (the q9 "wide live-segment set" shape).
    #[test]
    fn test_vlog_reader_cache_respects_cap() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 200);

        let cap = 16;
        let cache = VlogReaderCache::with_capacity(cap);
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
            assert!(
                cache.len() <= cap,
                "resident readers {} exceeded cap {}",
                cache.len(),
                cap
            );
        }
        assert_eq!(cache.len(), cap, "warm cache should be exactly at cap");
    }

    /// (a) Reads AFTER eviction return byte-identical values (a miss just
    /// re-opens the immutable segment). Workload exceeds the cap, so every
    /// segment except the last `cap` has been evicted and must re-open.
    #[test]
    fn test_vlog_reader_cache_evict_then_read_byte_identical() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 128);

        let cap = 8;
        let cache = VlogReaderCache::with_capacity(cap);
        // Warm-touch all segments in order → only the last `cap` stay resident.
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
        }
        assert_eq!(cache.len(), cap);

        // Now read EVERY segment back (early ones are evicted → re-open path)
        // and assert byte-identical to what was written.
        for (seg, p, expected) in &segs {
            let s = *seg;
            let r = cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
            assert_eq!(
                &r.get(p).unwrap(),
                expected,
                "segment {s} bytes after evict"
            );
            assert!(cache.len() <= cap, "cap still held during re-read");
        }
    }

    /// (c) Evicted readers are actually DROPPED — no handle leak. Holding a
    /// clone of a reader does not keep it resident in the cache; once evicted
    /// and the external clone released, the only strong ref is gone.
    #[test]
    fn test_vlog_reader_cache_evicts_drop_no_leak() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 64);

        let cap = 4;
        let cache = VlogReaderCache::with_capacity(cap);

        // Open segment 1 and keep an external clone.
        let first = segs[0].0;
        let held = cache
            .get_or_open(first, || VlogReader::open(&fs, dir, first))
            .unwrap();
        // strong refs: cache + `held` == 2.
        assert_eq!(Arc::strong_count(&held), 2);

        // Touch enough OTHER segments to push segment 1 out of the cache.
        for (seg, _p, _v) in segs.iter().skip(1) {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
        }
        assert!(cache.get(first).is_none(), "segment 1 must be evicted");
        // The cache released its strong ref on eviction → only `held` remains.
        assert_eq!(
            Arc::strong_count(&held),
            1,
            "evicted reader's cache ref must be dropped (no handle leak)"
        );

        // Explicit remove() of an absent id is a no-op (idempotent).
        cache.remove(first);
        assert!(cache.len() <= cap);

        // Drop the external clone → reader fully reclaimed.
        drop(held);

        // Re-open segment 1 from scratch → byte-identical (proves the evicted
        // handle did not corrupt or pin anything).
        let (s1, p1, v1) = &segs[0];
        let s = *s1;
        let r = cache
            .get_or_open(s, || VlogReader::open(&fs, dir, s))
            .unwrap();
        assert_eq!(&r.get(p1).unwrap(), v1);
    }

    /// `cap == 0` means UNCAPPED (the mini-bench / pre-fix A-B arm): the
    /// resident set grows with the touched-segment count.
    #[test]
    fn test_vlog_reader_cache_zero_cap_is_uncapped() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 50);

        let cache = VlogReaderCache::with_capacity(0);
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
        }
        assert_eq!(cache.len(), 50, "cap==0 keeps every reader resident");
    }

    /// Re-touching an entry promotes it to MRU so it survives eviction (true
    /// LRU victim selection, mirrors local_cache.rs).
    #[test]
    fn test_vlog_reader_cache_lru_promotes_retouched() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 10);

        let cap = 3;
        let cache = VlogReaderCache::with_capacity(cap);
        // Fill: 1,2,3 resident.
        for seg in 1..=3u64 {
            cache
                .get_or_open(seg, || VlogReader::open(&fs, dir, seg))
                .unwrap();
        }
        // Re-touch segment 1 → now LRU order is 2 (oldest), 3, 1 (MRU).
        assert!(cache.get(1).is_some());
        // Insert segment 4 → evicts segment 2, NOT segment 1.
        cache
            .get_or_open(4, || VlogReader::open(&fs, dir, 4))
            .unwrap();
        assert!(cache.get(1).is_some(), "re-touched seg 1 must survive");
        assert!(cache.get(2).is_none(), "seg 2 was the true LRU victim");
        assert_eq!(cache.len(), cap);
        let _ = &segs; // segments exist on disk for the re-opens above
    }

    /// FRS-AKV-B1: the BYTE budget bounds resident readers independent of the
    /// count cap — resident_bytes plateaus at the budget, NOT O(segments).
    /// Budget = 4 readers' worth of charge; count cap is generous (uncapped),
    /// so the byte bound is what holds.
    #[test]
    fn test_vlog_reader_cache_byte_budget_bounds_resident() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 64);

        // Budget for exactly 4 resident readers; count cap = 0 (uncapped) so
        // ONLY the byte bound is active.
        let budget_readers = 4usize;
        let budget = budget_readers * VLOG_READER_CHARGE_BYTES;
        let cache = VlogReaderCache::with_capacity_and_budget(0, budget);
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
            assert!(
                cache.resident_bytes() <= budget,
                "resident_bytes {} exceeded byte budget {}",
                cache.resident_bytes(),
                budget
            );
        }
        assert_eq!(
            cache.len(),
            budget_readers,
            "byte-budgeted cache plateaus at budget/charge readers (O(budget), not O(segments))"
        );
    }

    /// FRS-AKV-B1: a read AFTER a byte-budget eviction is byte-identical — the
    /// budget bound carries the same re-open-on-miss correctness as the count
    /// cap (a vlog segment is immutable once published).
    #[test]
    fn test_vlog_reader_cache_byte_budget_evict_then_read_byte_identical() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 32);

        let budget = 4 * VLOG_READER_CHARGE_BYTES;
        let cache = VlogReaderCache::with_capacity_and_budget(0, budget);
        // Warm-touch all → only the last few stay resident (byte-budgeted).
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
        }
        // Re-read EVERY segment; evicted ones re-open and must match exactly.
        for (seg, p, expected) in &segs {
            let s = *seg;
            let r = cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
            assert_eq!(
                &r.get(p).unwrap(),
                expected,
                "segment {s} bytes after byte-budget evict"
            );
            assert!(
                cache.resident_bytes() <= budget,
                "byte budget still held during re-read"
            );
        }
    }

    /// FRS-AKV-B1: count cap and byte budget COMPOSE — the tighter bound wins.
    /// Count cap = 10, byte budget = 3 readers → resident plateaus at 3.
    #[test]
    fn test_vlog_reader_cache_count_and_byte_budget_compose() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 40);

        let cache = VlogReaderCache::with_capacity_and_budget(10, 3 * VLOG_READER_CHARGE_BYTES);
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
        }
        assert_eq!(
            cache.len(),
            3,
            "tighter (byte) bound wins over the count cap"
        );
    }

    /// FRS-AKV-B1: `usize::MAX` budget = byte bound DISABLED → only the count
    /// cap applies (byte-identical to the pre-AKV `with_capacity` path).
    #[test]
    fn test_vlog_reader_cache_max_budget_is_count_only() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/db");
        fs.create_dir_all(dir).unwrap();
        let segs = write_kvsep_segments(&fs, dir, 20);

        let cap = 5;
        let cache = VlogReaderCache::with_capacity_and_budget(cap, usize::MAX);
        for (seg, _p, _v) in &segs {
            let s = *seg;
            cache
                .get_or_open(s, || VlogReader::open(&fs, dir, s))
                .unwrap();
        }
        assert_eq!(
            cache.len(),
            cap,
            "MAX budget defers entirely to the count cap"
        );
    }
}

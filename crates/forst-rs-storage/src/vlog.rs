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
}

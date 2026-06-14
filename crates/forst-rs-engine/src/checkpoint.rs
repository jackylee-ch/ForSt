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

//! Checkpoint write + restore pipeline. See `2.7_checkpoint_integration.md`.
//!
//! A checkpoint captures the full engine state into a target directory:
//!
//! ```text
//! <target>/CHECKPOINT.blob   — serialised VersionSet snapshot
//! <target>/000001.sst       — copy of every live SST file
//! <target>/000002.sst
//! ...
//! ```
//!
//! The checkpoint is consistent: before writing the blob we flush every
//! pending memtable to an L0 file so the in-memory state is fully captured
//! in the SST layer.
//!
//! Restoration reads the blob, instantiates a [`forst_rs_storage::version::VersionSetImpl`] from the
//! snapshot, and opens every referenced SST file from the target directory.

use std::path::{Path, PathBuf};

use forst_rs_common::{crc32c, ForstError, ForstResult};
use forst_rs_io::{FileSystem, WriteMode};
use forst_rs_storage::version::{checkpoint, SstFileMeta};

use crate::flush::{sst_file_path, sst_temp_path};

/// File name used for the serialised checkpoint blob.
pub const CHECKPOINT_BLOB_NAME: &str = "CHECKPOINT.blob";

/// Defense-in-depth cap on the on-disk checkpoint blob size. The blob carries
/// only LSM metadata (not data); even at A1 §11's hundred-TB engineering scale
/// (≤ 100k SSTs × ~200 bytes per SST entry ~= 20 MiB) it remains comfortably
/// under this cap. A crafted or corrupted blob reporting ~4 GiB would
/// otherwise drive the `vec![0u8; size]` allocation in `read_blob` to OOM
/// (Sweep R4 H by Reviewers 2 + 5).
pub const MAX_CHECKPOINT_BLOB_SIZE: u64 = 100 * 1024 * 1024; // 100 MiB

/// Metadata returned by a successful checkpoint.
#[derive(Debug, Clone)]
pub struct CheckpointManifest {
    /// Directory where the checkpoint lives.
    pub target_dir: PathBuf,
    /// Files copied into the checkpoint (relative basenames).
    pub sst_files: Vec<PathBuf>,
    /// Total bytes written.
    pub total_bytes: u64,
}

/// Copies `src` to `dst` through the supplied filesystem. Used by the
/// checkpoint to duplicate SST files into the checkpoint directory.
///
/// R40-M2: writes to a `.<basename>.tmp` staging path first, then renames into place
/// atomically. Mirrors the tmp+rename pattern in `flush.rs` and `compaction.rs` so an
/// interrupt mid-copy never leaves a partial `<num>.sst` at the canonical name — which
/// a retry would otherwise observe as an inode/size mismatch and reject. The orphan-scan
/// in `db::open_from_checkpoint` already recognises `.*.sst.tmp` (via `sst_temp_path`'s
/// constants), so a leftover staging file is cleaned up automatically on next open.
///
/// On any error along the streaming-copy → flush → sync → rename path we best-effort
/// delete the tmp file before propagating. The delete is best-effort because the file
/// may not exist yet (open failure), or the FS may reject the delete (in which case
/// the next restore's orphan-scan still rescues us).
/// Atomic `src → dst` copy with tmp + rename and short-read detection.
///
/// **Precondition (R77-M1):** `src` MUST be a fully-written, write-once file
/// (canonical SST `<N>.sst` is the only intended caller — `copy_live_ssts`
/// only passes those). The R76-H1 short-read check captures `src`'s size
/// BEFORE opening the stream; a caller that passes a live, still-growing
/// file (WAL, in-progress checkpoint blob) would see either a false-positive
/// Corruption (if more bytes arrived) or a silent acceptance below the
/// captured size (also Corruption). Either way, the function is unsafe for
/// non-write-once inputs.
pub fn copy_file(fs: &dyn FileSystem, src: &Path, dst: &Path) -> ForstResult<u64> {
    // R76-H1: capture the expected source size BEFORE streaming so we can
    // detect a short read after the loop and refuse to publish a truncated
    // SST at the canonical `<num>.sst` path. Pre-fix a short-read on the
    // source (network blip, OpenDAL ranged-read truncation — the same failure
    // mode R75-M1/M2 closed for other sites) produced a truncated SST that
    // the orphan-scan does NOT catch (valid name), and the poisoned file
    // would then become a live L0 in the checkpoint manifest. We treat
    // metadata-unavailable (`expected_size = None`) as best-effort and skip
    // the comparison, consistent with R75-M2's `cap_hint == 0` branch.
    // 2026-05-30 WRITE-BACK CHECKPOINT FIX: await any in-flight async upload of
    // `src` before reading it from the remote, so a write-back SST whose upload
    // is still in flight is not read as a 404 (no-op on local / already-durable).
    fs.await_upload(src)?;
    let expected_size: Option<u64> = fs.get_file_metadata(src).ok().map(|m| m.size);
    let mut reader = fs.open_sequential_file(src)?;
    if let Some(parent) = dst.parent() {
        fs.create_dir_all(parent)?;
    }
    // FRS-S3-CKPTBLOB (sibling of write_blob / flush.rs FRS-S3-SSTRENAME): on a
    // local FS stage to a `.tmp` and atomically rename; on object stores there
    // is no atomic rename (OpenDAL → Unsupported, which would FAIL the copy and
    // thus a full checkpoint/restore on S3), so stream straight to the final
    // path — multipart-complete-on-close is crash-atomic, CreateOrTruncate
    // overwrites a stale orphan from a crashed prior attempt.
    let atomic_rename = fs.supports_atomic_rename();
    let (write_target, write_mode) = if atomic_rename {
        (sst_temp_path(dst), WriteMode::CreateNew)
    } else {
        (dst.to_path_buf(), WriteMode::CreateOrTruncate)
    };
    let result: ForstResult<u64> = (|| {
        let mut writer = fs.open_writable_file(&write_target, write_mode)?;
        let mut buf = vec![0u8; 64 * 1024];
        let mut total = 0u64;
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            writer.append(&buf[..n])?;
            total += n as u64;
        }
        writer.flush()?;
        writer.sync()?;
        if let Some(want) = expected_size {
            if total != want {
                return Err(ForstError::corruption(format!(
                    "copy_file short read: expected {} bytes, got {} from {}",
                    want,
                    total,
                    src.display()
                )));
            }
        }
        Ok(total)
    })();
    let total = match result {
        Ok(total) => total,
        Err(e) => {
            let _ = fs.delete_file(&write_target);
            return Err(e);
        }
    };
    if atomic_rename {
        if let Err(e) = fs.rename(&write_target, dst) {
            let _ = fs.delete_file(&write_target);
            return Err(e);
        }
    }
    // R49-H3: fsync(parent_dir) so the rename's dirent change is durable.
    if let Some(parent) = dst.parent() {
        if let Err(e) = fs.sync_dir(parent) {
            tracing::warn!(
                "copy_file: sync_dir({}) failed after rename: {} (R49-H3)",
                parent.display(),
                e
            );
        }
    }
    Ok(total)
}

/// Writes the checkpoint blob bytes atomically via a tmp file + rename.
///
/// R39-H2: same rename-then-leak-on-error pattern fixed in `flush.rs` and
/// `compaction.rs`. On rename failure the `.<CHECKPOINT_BLOB_NAME>.tmp`
/// orphan would otherwise persist forever; `open_from_checkpoint`'s
/// orphan-scan only catches `.sst` / `.sst.tmp`. We delete the tmp file
/// before propagating the rename error so a future restore is not left with
/// a stale partial blob in the checkpoint directory.
pub fn write_blob(fs: &dyn FileSystem, target_dir: &Path, blob: &[u8]) -> ForstResult<PathBuf> {
    fs.create_dir_all(target_dir)?;
    let final_path = target_dir.join(CHECKPOINT_BLOB_NAME);
    // FRS-S3-CKPTBLOB: mirror the SST flush/compaction object-store path
    // (FRS-S3-SSTRENAME in flush.rs). On a local FS, stage to a `.tmp` file and
    // atomically `rename` into place. On object stores (S3/GCS/OSS/Azure) there
    // is NO atomic rename — OpenDAL surfaces it as `Unsupported`, which the FFI
    // mapped to NOT_SUPPORTED and FAILED EVERY incremental checkpoint (the Flink
    // CheckpointCoordinator then aborted the job at the tolerable-failure
    // threshold). For those backends stream the blob straight to its final key:
    // an object-store multipart upload only publishes on a successful close()
    // (CompleteMultipartUpload), so a mid-write crash leaves an incomplete
    // upload that never becomes visible — crash-atomic without a rename.
    // `CreateOrTruncate` overwrites a stale orphan blob from a crashed prior
    // attempt at the same checkpoint id (mirrors FRS-S3-ORPHAN-FIX).
    if fs.supports_atomic_rename() {
        let tmp_path = target_dir.join(format!(".{}.tmp", CHECKPOINT_BLOB_NAME));
        {
            let mut wf = fs.open_writable_file(&tmp_path, WriteMode::CreateNew)?;
            wf.append(blob)?;
            wf.flush()?;
            wf.sync()?;
        }
        if let Err(e) = fs.rename(&tmp_path, &final_path) {
            let _ = fs.delete_file(&tmp_path);
            return Err(e);
        }
    } else {
        let mut wf = fs.open_writable_file(&final_path, WriteMode::CreateOrTruncate)?;
        wf.append(blob)?;
        wf.flush()?;
        wf.sync()?;
    }
    // R49-H3: fsync the checkpoint directory so the blob's dirent change
    // (and any SST dirents from copy_live_ssts that ran earlier — R49-M1
    // reorders the writer so copy precedes blob) is durable on power-loss.
    if let Err(e) = fs.sync_dir(target_dir) {
        tracing::warn!(
            "write_blob: sync_dir({}) failed after rename: {} (R49-H3)",
            target_dir.display(),
            e
        );
    }
    Ok(final_path)
}

/// Reads the checkpoint blob from `target_dir`.
pub fn read_blob(fs: &dyn FileSystem, target_dir: &Path) -> ForstResult<Vec<u8>> {
    let path = target_dir.join(CHECKPOINT_BLOB_NAME);
    if !fs.file_exists(&path)? {
        return Err(ForstError::not_found(format!(
            "checkpoint blob not found at {}",
            path.display()
        )));
    }
    let meta = fs.get_file_metadata(&path)?;
    // SECURITY: bound the file size before allocation. A malicious or corrupted
    // checkpoint blob reporting a multi-GiB size would otherwise drive the
    // `vec![0u8; size]` below to OOM (Sweep R4 H).
    if meta.size > MAX_CHECKPOINT_BLOB_SIZE {
        return Err(ForstError::corruption(format!(
            "checkpoint blob at {} reports size {} bytes, exceeds cap {} bytes",
            path.display(),
            meta.size,
            MAX_CHECKPOINT_BLOB_SIZE
        )));
    }
    // R77-L1: removed dead `open_random_access_file` call — the actual read
    // uses `open_sequential_file` below. On S3/OpenDAL backends the dead
    // RAC open issued a wasted GET/HEAD per restore.
    let size = meta.size as usize;
    let mut buf = vec![0u8; size];
    // Sequential read for large blobs.
    let mut seq = fs.open_sequential_file(&path)?;
    let mut offset = 0usize;
    while offset < size {
        let n = seq.read(&mut buf[offset..])?;
        if n == 0 {
            break;
        }
        offset += n;
    }
    // R75-M1: surface a short read as corruption rather than silently
    // truncating the buffer. `read_blob` is on the checkpoint-restore
    // critical path; a truncated blob whose prefix happens to parse
    // (e.g., the version-set header decodes but the L0 file list is
    // cut) would silently restore the engine to a stale state. Source
    // of short reads: OpenDAL ranged sequential reads under network
    // flake. The `meta.size` upper bound came from the same FS so a
    // legitimate EOF before `meta.size` indicates corruption / out-of-
    // band truncation.
    if offset != size {
        return Err(ForstError::corruption(format!(
            "checkpoint blob short read: expected {} bytes, got {} at {}",
            size,
            offset,
            path.display()
        )));
    }
    Ok(buf)
}

/// FRS-CKPT-NOFLUSH (2026-06-01): writes an arbitrary checkpoint artifact file
/// (e.g. a per-CF memtable Arrow-IPC snapshot) atomically, mirroring
/// [`write_blob`]'s tmp+rename (local FS) / stream-to-final (object store)
/// crash-atomicity. Used by the checkpoint-without-flush path to persist the
/// live memtable to S3 without folding it into an L0 SST.
pub fn write_artifact_file(fs: &dyn FileSystem, path: &Path, bytes: &[u8]) -> ForstResult<()> {
    if let Some(parent) = path.parent() {
        fs.create_dir_all(parent)?;
    }
    if fs.supports_atomic_rename() {
        let fname = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| ForstError::invalid_argument("artifact path has no file name"))?;
        let tmp_path = path.with_file_name(format!(".{fname}.tmp"));
        {
            let mut wf = fs.open_writable_file(&tmp_path, WriteMode::CreateNew)?;
            wf.append(bytes)?;
            wf.flush()?;
            wf.sync()?;
        }
        if let Err(e) = fs.rename(&tmp_path, path) {
            let _ = fs.delete_file(&tmp_path);
            return Err(e);
        }
    } else {
        let mut wf = fs.open_writable_file(path, WriteMode::CreateOrTruncate)?;
        wf.append(bytes)?;
        wf.flush()?;
        wf.sync()?;
    }
    Ok(())
}

/// Reads an artifact file written by [`write_artifact_file`]. Size-bounded by
/// `MAX_CHECKPOINT_BLOB_SIZE` (same OOM guard as [`read_blob`]).
pub fn read_artifact_file(fs: &dyn FileSystem, path: &Path) -> ForstResult<Vec<u8>> {
    if !fs.file_exists(path)? {
        return Err(ForstError::not_found(format!(
            "checkpoint artifact not found at {}",
            path.display()
        )));
    }
    let meta = fs.get_file_metadata(path)?;
    if meta.size > MAX_CHECKPOINT_BLOB_SIZE {
        return Err(ForstError::corruption(format!(
            "checkpoint artifact at {} reports size {} bytes, exceeds cap {} bytes",
            path.display(),
            meta.size,
            MAX_CHECKPOINT_BLOB_SIZE
        )));
    }
    let size = meta.size as usize;
    let mut buf = vec![0u8; size];
    let mut seq = fs.open_sequential_file(path)?;
    let mut offset = 0usize;
    while offset < size {
        let n = seq.read(&mut buf[offset..])?;
        if n == 0 {
            break;
        }
        offset += n;
    }
    if offset != size {
        return Err(ForstError::corruption(format!(
            "checkpoint artifact short read: expected {} bytes, got {} at {}",
            size,
            offset,
            path.display()
        )));
    }
    Ok(buf)
}

/// Copies every live SST file from `source_dir` into `target_dir`. Returns
/// the total bytes copied and the list of produced files.
///
/// FRS-PHASE2 catalog #4 — dispatches to the parallel multi-file PUT
/// ([`copy_live_ssts_parallel`]) when `FRS_CKPT_PARALLEL_UPLOAD` is set, else
/// the byte-identical serial loop ([`copy_live_ssts_serial`]). Default OFF:
/// bytes AND manifest order are identical to the pre-Phase-2 serial path.
pub fn copy_live_ssts(
    fs: &dyn FileSystem,
    source_dir: &Path,
    target_dir: &Path,
    live: &[SstFileMeta],
) -> ForstResult<(u64, Vec<PathBuf>)> {
    match ckpt_parallel_upload_workers() {
        Some(workers) => copy_live_ssts_parallel(fs, source_dir, target_dir, live, workers),
        None => copy_live_ssts_serial(fs, source_dir, target_dir, live),
    }
}

/// The serial copy loop — one `copy_file` per live SST, in ascending
/// `file_number` order. This is the byte-identical default and the reference
/// the parallel path is validated against.
pub fn copy_live_ssts_serial(
    fs: &dyn FileSystem,
    source_dir: &Path,
    target_dir: &Path,
    live: &[SstFileMeta],
) -> ForstResult<(u64, Vec<PathBuf>)> {
    fs.create_dir_all(target_dir)?;
    let mut total = 0u64;
    let mut files = Vec::with_capacity(live.len());
    for meta in live {
        let src = sst_file_path(source_dir, meta.file_number);
        let dst = sst_file_path(target_dir, meta.file_number);
        let bytes = copy_file(fs, &src, &dst)?;
        total += bytes;
        files.push(dst);
    }
    Ok((total, files))
}

/// Process-global default upload concurrency for [`copy_live_ssts_parallel`] —
/// mirrors the opendal backend's `MAX_INFLIGHT_UPLOADS=8` so the checkpoint
/// barrier can fill (but not over-fill) the same write-back pipe.
const CKPT_PARALLEL_UPLOAD_DEFAULT: usize = 8;

/// Returns the configured parallel-upload bound, or `None` when the flag is OFF
/// (the byte-identical serial default). `FRS_CKPT_PARALLEL_UPLOAD` accepts:
/// `0`/empty/unset/`false`/`off` ⇒ OFF; `1`/`true`/`on` ⇒ the default bound
/// ([`CKPT_PARALLEL_UPLOAD_DEFAULT`]); any other positive integer ⇒ that
/// explicit worker count.
fn ckpt_parallel_upload_workers() -> Option<usize> {
    let raw = std::env::var("FRS_CKPT_PARALLEL_UPLOAD").ok()?;
    let v = raw.trim();
    match v {
        "" | "0" | "false" | "off" | "no" => None,
        "1" | "true" | "on" | "yes" => Some(CKPT_PARALLEL_UPLOAD_DEFAULT),
        other => match other.parse::<usize>() {
            Ok(0) => None,
            Ok(n) => Some(n),
            Err(_) => None,
        },
    }
}

/// FRS-PHASE2 catalog #4 — checkpoint multi-file parallel PUT.
///
/// Twin of [`copy_live_ssts_serial`] that fans the per-SST copies across a
/// bounded pool of scoped worker threads instead of the serial `for` loop. On
/// disaggregated/remote state each `copy_file` ends in a `flush()`+`sync()`
/// that completes an INDEPENDENT object PUT (distinct `<N>.sst` key) — a full
/// remote round-trip — so the serial loop pays `N × PUT-RTT` at the barrier
/// even though the object-store backend can absorb many in flight. This
/// overlaps them, cutting the barrier wall to ≈ `ceil(N / workers) × PUT-RTT`
/// (mini-bench `checkpoint_upload`: ~7.6–7.9× at the 8-wide bound, at both
/// dev-RTT 23 ms and intra-DC sim-S3 RTT 2 ms — it is an op-latency term, not
/// bandwidth).
///
/// **Byte-identity:** each worker runs the SAME `copy_file` (same tmp/rename,
/// short-read check, sync_dir) on a DISJOINT destination key, so the produced
/// bytes are identical to the serial path; only the issue ORDER differs. The
/// returned `files` are sorted by `file_number` to keep the manifest order
/// deterministic regardless of completion order. `fs` is `Send + Sync`
/// (`FileSystem` supertrait), so sharing `&dyn FileSystem` across the scope is
/// sound. On ANY worker error the scope still joins all threads (no detached
/// work) and the FIRST error is returned; partial destination files are left
/// for the next restore's orphan-scan (identical to the serial path's
/// mid-loop-error behaviour).
pub fn copy_live_ssts_parallel(
    fs: &dyn FileSystem,
    source_dir: &Path,
    target_dir: &Path,
    live: &[SstFileMeta],
    workers: usize,
) -> ForstResult<(u64, Vec<PathBuf>)> {
    // Degenerate fan-outs run inline (no thread/sync overhead) and stay
    // byte-identical to the serial path.
    if live.len() <= 1 || workers <= 1 {
        return copy_live_ssts_serial(fs, source_dir, target_dir, live);
    }
    fs.create_dir_all(target_dir)?;
    let n = live.len();
    let workers = workers.min(n);
    let next = std::sync::atomic::AtomicUsize::new(0);
    // Accumulated copy results, guarded for the scoped workers.
    let total = std::sync::Mutex::new(0u64);
    let first_err: std::sync::Mutex<Option<ForstError>> = std::sync::Mutex::new(None);
    let produced: std::sync::Mutex<Vec<(u64, PathBuf)>> =
        std::sync::Mutex::new(Vec::with_capacity(n));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let next = &next;
            let total = &total;
            let first_err = &first_err;
            let produced = &produced;
            scope.spawn(move || loop {
                let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= n {
                    break;
                }
                // Stop pulling new work once a sibling has failed — bounds the
                // blast radius of a remote outage at the barrier.
                if first_err.lock().expect("ckpt err lock").is_some() {
                    break;
                }
                let meta = &live[i];
                let src = sst_file_path(source_dir, meta.file_number);
                let dst = sst_file_path(target_dir, meta.file_number);
                match copy_file(fs, &src, &dst) {
                    Ok(bytes) => {
                        *total.lock().expect("ckpt total lock") += bytes;
                        produced
                            .lock()
                            .expect("ckpt produced lock")
                            .push((meta.file_number.value(), dst));
                    }
                    Err(e) => {
                        let mut slot = first_err.lock().expect("ckpt err lock");
                        if slot.is_none() {
                            *slot = Some(e);
                        }
                    }
                }
            });
        }
    });

    if let Some(e) = first_err.into_inner().expect("ckpt err lock") {
        return Err(e);
    }
    let total = total.into_inner().expect("ckpt total lock");
    let mut produced = produced.into_inner().expect("ckpt produced lock");
    // Deterministic manifest order (== serial path's ascending file_number).
    produced.sort_by_key(|(fnum, _)| *fnum);
    let files = produced.into_iter().map(|(_, dst)| dst).collect();
    Ok((total, files))
}

// ---------------------------------------------------------------------------
// FRS-PHASE2-S1: mapping-snapshot trailer (design §2.4)
// ---------------------------------------------------------------------------

/// Magic terminating a mapping-snapshot trailer appended to `CHECKPOINT.blob`.
///
/// The Phase-2 file-mapping layer's consistent snapshot
/// (`FileMappingManager::snapshot_bytes`) is embedded INTO the checkpoint
/// blob so every checkpoint carries the logical→physical mapping it was
/// taken under, atomically with the manifest (design risk R1). The base
/// VersionSet blob format is strict (`blob_size == data.len()`), so the
/// embed is an *envelope*: `[base blob][payload][crc32c(payload) u32]
/// [payload_len u32][magic 4B]`. A legacy blob ends with its u64
/// `blob_length` footer whose high bytes are zero, which can never equal the
/// magic — detection from the tail is unambiguous. Default (no mapping
/// attached) writes NO trailer: bytes byte-identical to pre-Phase-2.
pub const MAPPING_TRAILER_MAGIC: &[u8; 4] = b"FRMT";

/// Appends a mapping-snapshot trailer to `blob` (see
/// [`MAPPING_TRAILER_MAGIC`] for the envelope layout).
pub fn append_mapping_trailer(blob: &mut Vec<u8>, mapping: &[u8]) {
    blob.extend_from_slice(mapping);
    blob.extend_from_slice(&crc32c(mapping).to_le_bytes());
    blob.extend_from_slice(&(mapping.len() as u32).to_le_bytes());
    blob.extend_from_slice(MAPPING_TRAILER_MAGIC);
}

/// Splits checkpoint-blob bytes into `(base_blob, mapping_snapshot)`.
/// Returns `(data, None)` for legacy blobs without a trailer; validates
/// length + CRC when the trailer magic is present (corruption otherwise).
pub fn split_mapping_trailer(data: &[u8]) -> ForstResult<(&[u8], Option<&[u8]>)> {
    const TRAILER_FIXED: usize = 4 + 4 + 4; // crc + len + magic
    if data.len() < TRAILER_FIXED || &data[data.len() - 4..] != MAPPING_TRAILER_MAGIC {
        return Ok((data, None));
    }
    let len_off = data.len() - 8;
    let payload_len =
        u32::from_le_bytes(data[len_off..len_off + 4].try_into().expect("4 bytes")) as usize;
    let total = payload_len + TRAILER_FIXED;
    if data.len() < total {
        return Err(ForstError::corruption(format!(
            "mapping trailer payload_len {} exceeds blob size {}",
            payload_len,
            data.len()
        )));
    }
    let payload_start = data.len() - total;
    let payload = &data[payload_start..payload_start + payload_len];
    let crc_off = data.len() - 12;
    let stored_crc = u32::from_le_bytes(data[crc_off..crc_off + 4].try_into().expect("4 bytes"));
    if stored_crc != crc32c(payload) {
        return Err(ForstError::corruption("mapping trailer checksum mismatch"));
    }
    Ok((&data[..payload_start], Some(payload)))
}

/// Serialises a VersionSet snapshot to the checkpoint blob format.
pub fn serialize_snapshot(
    snapshot: &forst_rs_storage::version::VersionSetSnapshot,
) -> ForstResult<Vec<u8>> {
    checkpoint::serialize_to_blob(snapshot)
}

/// Restores a VersionSet snapshot from a checkpoint blob.
pub fn deserialize_snapshot(
    data: &[u8],
) -> ForstResult<forst_rs_storage::version::VersionSetSnapshot> {
    checkpoint::restore_from_blob(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use forst_rs_io::MemoryFileSystem;

    #[test]
    fn test_write_then_read_blob_roundtrip() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/ck");
        let data = b"hello checkpoint";
        let path = write_blob(&fs, dir, data).unwrap();
        assert!(fs.file_exists(&path).unwrap());
        let read = read_blob(&fs, dir).unwrap();
        assert_eq!(read, data);
    }

    #[test]
    fn test_read_missing_blob_returns_not_found() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/ck");
        fs.create_dir_all(dir).unwrap();
        let err = read_blob(&fs, dir).unwrap_err();
        assert!(err.is_not_found());
    }

    /// Regression test for Sweep R4 H (Reviewers 2 + 5): a checkpoint blob
    /// whose filesystem-reported size exceeds `MAX_CHECKPOINT_BLOB_SIZE`
    /// must be rejected BEFORE the `vec![0u8; size]` allocation, to prevent
    /// OOM-DoS on a malicious or corrupted blob.
    ///
    /// MemoryFileSystem doesn't let us spoof a metadata size larger than
    /// the actual content, so we instead write a real blob that exceeds the
    /// cap and assert the cap fires. (Real-world attack vectors would be
    /// crafted on-disk metadata or symlinks pointing to huge files; we
    /// only need to verify the gate triggers.)
    #[test]
    fn test_read_blob_rejects_oversized_metadata() {
        let fs = MemoryFileSystem::new();
        let dir = Path::new("/ck");
        // Write a payload that exceeds MAX_CHECKPOINT_BLOB_SIZE by 1 byte.
        // 100 MiB + 1 is well above any legitimate checkpoint blob.
        let oversize = MAX_CHECKPOINT_BLOB_SIZE as usize + 1;
        let payload = vec![0u8; oversize];
        let _ = write_blob(&fs, dir, &payload).unwrap();
        let err = read_blob(&fs, dir).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("exceeds cap"),
            "expected cap-exceeded error; got: {}",
            msg
        );
    }

    /// FRS-PHASE2-S1: mapping-trailer envelope round-trip + legacy-blob
    /// pass-through + corruption detection.
    #[test]
    fn test_mapping_trailer_roundtrip_and_legacy_passthrough() {
        let base = b"FRCP-fake-base-blob-bytes".to_vec();
        // Legacy blob (no trailer): split is identity.
        let (b, m) = split_mapping_trailer(&base).unwrap();
        assert_eq!(b, &base[..]);
        assert!(m.is_none());

        // Round-trip.
        let mapping = b"FRMS-fake-mapping-snapshot".to_vec();
        let mut blob = base.clone();
        append_mapping_trailer(&mut blob, &mapping);
        let (b, m) = split_mapping_trailer(&blob).unwrap();
        assert_eq!(b, &base[..]);
        assert_eq!(m.unwrap(), &mapping[..]);

        // Empty mapping payload round-trips too.
        let mut blob2 = base.clone();
        append_mapping_trailer(&mut blob2, &[]);
        let (b2, m2) = split_mapping_trailer(&blob2).unwrap();
        assert_eq!(b2, &base[..]);
        assert_eq!(m2.unwrap(), &[] as &[u8]);

        // Corrupt the payload: CRC must fire.
        let mut corrupted = base.clone();
        append_mapping_trailer(&mut corrupted, &mapping);
        let idx = base.len() + 2;
        corrupted[idx] ^= 0xFF;
        assert!(split_mapping_trailer(&corrupted).is_err());
    }

    #[test]
    fn test_copy_file_preserves_content() {
        let fs = MemoryFileSystem::new();
        let src = Path::new("/src/a.bin");
        let dst = Path::new("/dst/a.bin");
        fs.create_dir_all(src.parent().unwrap()).unwrap();
        let mut wf = fs.open_writable_file(src, WriteMode::CreateNew).unwrap();
        wf.append(b"payload").unwrap();
        wf.sync().unwrap();
        drop(wf);
        let bytes = copy_file(&fs, src, dst).unwrap();
        assert_eq!(bytes, 7);
        assert!(fs.file_exists(dst).unwrap());
    }

    // ----- FRS-PHASE2 catalog #4: parallel checkpoint upload -----

    /// Seeds `n` source SST files of distinct content at `src_dir` and returns
    /// the matching `live` metas (file_number = i).
    fn seed_live_ssts(fs: &dyn FileSystem, src_dir: &Path, n: u64) -> Vec<SstFileMeta> {
        let mut live = Vec::with_capacity(n as usize);
        for i in 0..n {
            let p = sst_file_path(src_dir, forst_rs_common::FileNumber(i));
            if let Some(parent) = p.parent() {
                fs.create_dir_all(parent).unwrap();
            }
            // Distinct content per file so a byte-identity check is meaningful.
            let payload: Vec<u8> = (0..(64 + i)).map(|b| (b ^ i) as u8).collect();
            let mut wf = fs
                .open_writable_file(&p, WriteMode::CreateOrTruncate)
                .unwrap();
            wf.append(&payload).unwrap();
            wf.flush().unwrap();
            wf.sync().unwrap();
            live.push(SstFileMeta::new_default_cf(
                forst_rs_common::FileNumber(i),
                payload.len() as u64,
                vec![i as u8],
                vec![i as u8],
                forst_rs_common::SequenceNumber(i),
                forst_rs_common::SequenceNumber(i),
                1,
            ));
        }
        live
    }

    /// The parallel path must be BYTE-IDENTICAL to the serial path: same total
    /// bytes, same produced file set + manifest order, and the destination
    /// content must match the source for every file.
    #[test]
    fn parallel_copy_matches_serial_byte_for_byte() {
        let fs = MemoryFileSystem::new();
        let src = Path::new("/db");
        let live = seed_live_ssts(&fs, src, 17); // > 1 and not a multiple of pool

        let (ser_bytes, ser_files) =
            copy_live_ssts_serial(&fs, src, Path::new("/ckpt-ser"), &live).unwrap();
        let (par_bytes, par_files) =
            copy_live_ssts_parallel(&fs, src, Path::new("/ckpt-par"), &live, 8).unwrap();

        assert_eq!(ser_bytes, par_bytes, "total bytes differ");
        // Manifest order is deterministic (ascending file_number) for BOTH.
        let ser_names: Vec<_> = ser_files
            .iter()
            .map(|p| p.file_name().unwrap().to_owned())
            .collect();
        let par_names: Vec<_> = par_files
            .iter()
            .map(|p| p.file_name().unwrap().to_owned())
            .collect();
        assert_eq!(ser_names, par_names, "manifest order/file set differ");

        // Every parallel-copied destination matches its source content.
        for (dst_ser, dst_par) in ser_files.iter().zip(par_files.iter()) {
            let a = read_artifact_file(&fs, dst_ser).unwrap();
            let b = read_artifact_file(&fs, dst_par).unwrap();
            assert_eq!(a, b, "copied content differs for {:?}", dst_par);
        }
    }

    /// Degenerate fan-outs (0/1 file, or workers<=1) must run the serial path
    /// and still produce the correct result.
    #[test]
    fn parallel_copy_degenerate_fanouts() {
        let fs = MemoryFileSystem::new();
        let src = Path::new("/db");

        // Zero files.
        let (b, f) = copy_live_ssts_parallel(&fs, src, Path::new("/ck0"), &[], 8).unwrap();
        assert_eq!(b, 0);
        assert!(f.is_empty());

        // One file with many workers => inline.
        let live1 = seed_live_ssts(&fs, src, 1);
        let (b1, f1) = copy_live_ssts_parallel(&fs, src, Path::new("/ck1"), &live1, 8).unwrap();
        assert_eq!(f1.len(), 1);
        assert_eq!(b1, live1[0].file_size);

        // Many files but workers=1 => serial-equivalent.
        let live = seed_live_ssts(&fs, src, 5);
        let (b5, f5) = copy_live_ssts_parallel(&fs, src, Path::new("/ck5"), &live, 1).unwrap();
        assert_eq!(f5.len(), 5);
        let (bs, _) = copy_live_ssts_serial(&fs, src, Path::new("/ck5s"), &live).unwrap();
        assert_eq!(b5, bs);
    }

    /// A missing source surfaces as an error (not a panic / silent partial),
    /// matching the serial path's fail-fast contract.
    #[test]
    fn parallel_copy_propagates_error() {
        let fs = MemoryFileSystem::new();
        let src = Path::new("/db");
        let mut live = seed_live_ssts(&fs, src, 4);
        // Add a meta whose source SST was never written.
        live.push(SstFileMeta::new_default_cf(
            forst_rs_common::FileNumber(999),
            10,
            vec![0],
            vec![0],
            forst_rs_common::SequenceNumber(0),
            forst_rs_common::SequenceNumber(0),
            1,
        ));
        let r = copy_live_ssts_parallel(&fs, src, Path::new("/cke"), &live, 8);
        assert!(r.is_err(), "missing source must error");
    }

    /// The env flag parser: OFF by default and for falsey values; ON maps to
    /// the default bound; explicit positive integers pass through. Also asserts
    /// `copy_live_ssts` dispatches to the serial path when the flag is OFF.
    #[test]
    fn parallel_upload_flag_parser_and_dispatch() {
        // Snapshot + restore the env var so the test is hermetic.
        let prev = std::env::var("FRS_CKPT_PARALLEL_UPLOAD").ok();
        let cases = [
            (None, None),
            (Some("0"), None),
            (Some(""), None),
            (Some("false"), None),
            (Some("off"), None),
            (Some("1"), Some(CKPT_PARALLEL_UPLOAD_DEFAULT)),
            (Some("true"), Some(CKPT_PARALLEL_UPLOAD_DEFAULT)),
            (Some("on"), Some(CKPT_PARALLEL_UPLOAD_DEFAULT)),
            (Some("4"), Some(4)),
            (Some("16"), Some(16)),
            (Some("garbage"), None),
        ];
        for (set, want) in cases {
            match set {
                Some(v) => std::env::set_var("FRS_CKPT_PARALLEL_UPLOAD", v),
                None => std::env::remove_var("FRS_CKPT_PARALLEL_UPLOAD"),
            }
            assert_eq!(ckpt_parallel_upload_workers(), want, "for {:?}", set);
        }

        // Flag OFF => copy_live_ssts is byte-identical to the serial path.
        std::env::remove_var("FRS_CKPT_PARALLEL_UPLOAD");
        let fs = MemoryFileSystem::new();
        let live = seed_live_ssts(&fs, Path::new("/db"), 6);
        let (auto_b, auto_f) =
            copy_live_ssts(&fs, Path::new("/db"), Path::new("/a"), &live).unwrap();
        let (ser_b, ser_f) =
            copy_live_ssts_serial(&fs, Path::new("/db"), Path::new("/s"), &live).unwrap();
        assert_eq!(auto_b, ser_b);
        let an: Vec<_> = auto_f
            .iter()
            .map(|p| p.file_name().unwrap().to_owned())
            .collect();
        let sn: Vec<_> = ser_f
            .iter()
            .map(|p| p.file_name().unwrap().to_owned())
            .collect();
        assert_eq!(an, sn);

        match prev {
            Some(v) => std::env::set_var("FRS_CKPT_PARALLEL_UPLOAD", v),
            None => std::env::remove_var("FRS_CKPT_PARALLEL_UPLOAD"),
        }
    }
}

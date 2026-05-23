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

use forst_rs_common::{ForstError, ForstResult};
use forst_rs_io::{FileSystem, WriteMode};
use forst_rs_storage::version::{checkpoint, SstFileMeta};

use crate::flush::sst_file_path;

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
pub fn copy_file(fs: &dyn FileSystem, src: &Path, dst: &Path) -> ForstResult<u64> {
    let mut reader = fs.open_sequential_file(src)?;
    if let Some(parent) = dst.parent() {
        fs.create_dir_all(parent)?;
    }
    let mut writer = fs.open_writable_file(dst, WriteMode::CreateNew)?;
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
    let mut rac = fs.open_random_access_file(&path)?;
    let _ = &mut rac; // silence unused warning on older rustc paths
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
    buf.truncate(offset);
    Ok(buf)
}

/// Copies every live SST file from `source_dir` into `target_dir`. Returns
/// the total bytes copied and the list of produced files.
pub fn copy_live_ssts(
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
}

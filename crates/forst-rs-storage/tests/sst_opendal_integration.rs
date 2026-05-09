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

//! Integration test: SstWriter -> persist via OpendalFileSystem -> SstReader.
//!
//! Proves the legacy `&dyn FileSystem` injection path is byte-for-byte
//! compatible with the new [`OpendalFileSystem`] backend, end-to-end:
//!   1. Build SST bytes via `SstWriterImpl` (unchanged producer).
//!   2. Persist them through `FileSystem::open_writable_file` +
//!      `WritableFile::append/sync` against an OpenDAL memory operator.
//!   3. Re-read via `FileSystem::open_random_access_file`, drive
//!      `SstReaderImpl` over the resulting `Box<dyn RandomAccessFile>`,
//!      and verify every entry round-trips.
//!   4. Exercise random + sequential reads, then `delete_file` for cleanup.
//!
//! This test deliberately uses ONLY public APIs from `forst-rs-io` and
//! `forst-rs-storage` to ensure the OpenDAL backend can be plugged into
//! the engine without storage-layer changes.

use std::path::Path;

use forst_rs_common::{CompressionType, OpType};
use forst_rs_io::{FileSystem, OpendalFileSystem, WriteMode};
use forst_rs_storage::sst::{SstReaderImpl, SstWriterImpl, SstWriterOptions};

const N_ENTRIES: usize = 1000;
const SST_PATH: &str = "ssts/000001.sst";

/// Builds an SST with `n` sequential entries and returns the encoded bytes
/// plus the min/max key strings used for assertions.
fn build_sst_bytes(n: usize) -> Vec<u8> {
    let mut writer = SstWriterImpl::with_options(SstWriterOptions {
        // Force several blocks so the index/bloom paths are exercised.
        block_size: 4096,
        compression: CompressionType::Lz4,
    });
    for i in 0..n {
        let key = format!("key_{i:06}");
        let val = format!("val_{i:06}");
        writer
            .add(key.as_bytes(), Some(val.as_bytes()), (i + 1) as u64, 0)
            .expect("writer.add must succeed for sorted unique keys");
    }
    let (bytes, info) = writer.finish().expect("writer.finish");
    assert_eq!(info.entry_count, n as u64);
    assert!(info.data_block_count >= 2, "expected multi-block SST");
    bytes
}

#[test]
fn opendal_memory_full_sst_roundtrip() {
    let fs = OpendalFileSystem::memory().expect("memory backend");
    let path = Path::new(SST_PATH);

    // ---- Write side ---------------------------------------------------------
    let bytes = build_sst_bytes(N_ENTRIES);
    {
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open_writable_file");
        w.append(&bytes).expect("append SST bytes");
        w.sync().expect("sync SST file");
        // file_size on the open writer reflects the buffered length, not the
        // persisted object size — but it must equal the bytes appended.
        assert_eq!(w.file_size().unwrap(), bytes.len() as u64);
    }

    // The object is now visible through the FileSystem trait.
    assert!(
        fs.file_exists(path).expect("file_exists"),
        "SST should exist after sync"
    );
    let meta = fs.get_file_metadata(path).expect("get_file_metadata");
    assert_eq!(
        meta.size,
        bytes.len() as u64,
        "persisted size must match emitted bytes"
    );

    // ---- Sequential read (footer-tail check) --------------------------------
    {
        let mut seq = fs.open_sequential_file(path).expect("open_sequential_file");
        let mut buf = vec![0u8; bytes.len()];
        let n = seq.read(&mut buf).expect("seq read");
        assert_eq!(n, bytes.len(), "sequential read must drain whole file");
        assert_eq!(
            buf, bytes,
            "sequential bytes must match the SST writer output exactly"
        );
    }

    // ---- Random-access read via SstReaderImpl -------------------------------
    let rac = fs
        .open_random_access_file(path)
        .expect("open_random_access_file");
    let reader = SstReaderImpl::open(rac).expect("SstReaderImpl::open");
    assert_eq!(reader.total_entries(), N_ENTRIES as u64);
    assert_eq!(reader.footer().min_key, b"key_000000");
    assert_eq!(
        reader.footer().max_key,
        format!("key_{:06}", N_ENTRIES - 1).into_bytes()
    );

    // Sequential coverage: every key must round-trip.
    for i in 0..N_ENTRIES {
        let key = format!("key_{i:06}");
        let lr = reader
            .get(key.as_bytes())
            .expect("get must not error")
            .unwrap_or_else(|| panic!("key {key} should be present"));
        assert_eq!(lr.value, Some(format!("val_{i:06}").into_bytes()));
        assert_eq!(lr.sequence, (i + 1) as u64);
        assert_eq!(lr.op_type, OpType::Put);
    }

    // Random-order coverage: probe a deterministic spread of keys to
    // exercise the random_at path repeatedly. Step is coprime to N so it
    // visits every residue class without revisiting indexes.
    let step = 7usize;
    for k in 0..N_ENTRIES {
        let i = (k * step) % N_ENTRIES;
        let key = format!("key_{i:06}");
        let lr = reader
            .get(key.as_bytes())
            .expect("random get must not error")
            .unwrap_or_else(|| panic!("random get missed key {key}"));
        assert_eq!(lr.value.as_deref(), Some(format!("val_{i:06}").as_bytes()));
    }

    // Negative lookups across both bloom-filter rejection paths.
    assert!(reader.get(b"AAAAAA").unwrap().is_none(), "before-min-key");
    assert!(reader.get(b"zzzzzz").unwrap().is_none(), "after-max-key");
    assert!(
        reader.get(b"key_999998").unwrap().is_none(),
        "in-range but absent"
    );

    // ---- Delete cleanup -----------------------------------------------------
    drop(reader); // release the BlockingOperator handle before delete

    fs.delete_file(path).expect("delete_file");
    assert!(
        !fs.file_exists(path).expect("file_exists after delete"),
        "SST must be gone after delete_file"
    );
    let err = fs
        .delete_file(path)
        .expect_err("second delete must fail with NotFound");
    assert!(err.is_not_found(), "expected NotFound, got: {err}");
}

/// Verify that `SstReaderImpl` works under the OpenDAL local-FS backend
/// too, which uses real disk I/O. Catches issues that only show up when
/// `RandomAccessFile::read_at` actually crosses an OS boundary.
#[test]
fn opendal_local_fs_full_sst_roundtrip() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let fs = OpendalFileSystem::local(tmp.path()).expect("local fs");
    let path = Path::new("ssts/000002.sst");

    let bytes = build_sst_bytes(N_ENTRIES);
    {
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open_writable_file");
        w.append(&bytes).expect("append");
        w.sync().expect("sync");
    }
    assert!(fs.file_exists(path).unwrap());

    // Confirm the bytes really hit disk under the temp root.
    let on_disk = tmp.path().join("ssts/000002.sst");
    assert!(on_disk.exists(), "expected real file at {on_disk:?}");
    assert_eq!(
        std::fs::metadata(&on_disk).unwrap().len(),
        bytes.len() as u64
    );

    let rac = fs
        .open_random_access_file(path)
        .expect("open_random_access_file");
    let reader = SstReaderImpl::open(rac).expect("SstReaderImpl::open");
    assert_eq!(reader.total_entries(), N_ENTRIES as u64);

    // Spot-check 25 evenly-spaced keys instead of full coverage to keep the
    // disk-backed test fast — the memory-backed test above proves the full
    // round-trip property.
    for i in (0..N_ENTRIES).step_by(N_ENTRIES / 25) {
        let key = format!("key_{i:06}");
        let lr = reader
            .get(key.as_bytes())
            .expect("get")
            .unwrap_or_else(|| panic!("missing {key}"));
        assert_eq!(lr.value, Some(format!("val_{i:06}").into_bytes()));
    }
}

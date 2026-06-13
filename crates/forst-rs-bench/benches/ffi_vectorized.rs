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

//! B1 — `ffi_vectorized`: the FFI boundary-tax bench
//! (design doc `2026-06-12-local-ffi-flush-compaction-bench-design.md` §B1).
//!
//! Every other bench in this crate calls `DbImpl` directly; the macro
//! stack's hot path is the REAL `extern "C"` surface (`frs_vectorized_*`,
//! `frs_vec_iter_*`) that ForStRsLinker binds. This bench drives those
//! entry points with buffers laid out exactly as the Java classifier lays
//! them out (Arrow i32-offsets + contiguous data column), next to
//! engine-direct equivalents at the same batch sizes, so the difference —
//! the per-row boundary tax (validation, offset walks, slice rebuilds,
//! output copy) — is measured explicitly.
//!
//! Groups (matrix per design §B1):
//! - `put` — `frs_vectorized_batch_put` vs
//!   `DbImpl::batch_put_borrowed_single_cf`, rows {64,256,1024} × value
//!   {64 B, 256 B, 1 KiB}
//! - `mixed` — `frs_vectorized_batch_mixed` (Delete/Put/Merge interleaved)
//!   vs the same engine entry with an op_types column; merge fraction
//!   {0 %, 20 %}
//! - `get_warm` — `frs_vectorized_batch_get` vs `DbImpl::batch_get`, all
//!   keys memtable-resident, rows {64,256,1024}
//! - `get_multisst` — same at rows=256 after 8 × switch_and_flush
//! - `iter_drain` — `frs_vec_iter_prefix_open` + `_next` chunked drain
//!   (64 KiB chunks, FRS_CHUNK_EOF auto-close honored), 100 prefixes ×
//!   1000 rows/prefix
//! - `iter_open_batch` — `frs_vec_iter_prefix_open_batch_parallel`, K=64
//! - `q7_iter_probe_ffi` — q7-like many-prefix, tiny-result probes
//!   comparing serial `frs_vec_iter_prefix_open` with batched parallel open
//! - `q19_iter_topn_ffi` — q19 TopN-like many small prefixes with K={64,256}
//! - `q19_append_merge_chain_ffi` — `frs_vec_merge_append_batch` distinct-key
//!   and same-key-chain shapes, verified via `frs_vectorized_batch_get`
//! - `q19_merge_chain_read_lifecycle_ffi` — merge-chain reads while
//!   memtable-resident, after flush, and after compaction
//! - `q19_iter_open_alloc_split_ffi` — separates batch-open caller allocation
//!   cost from native open/fill cost with reused caller buffers
//! - `compat_jni_prefix_proxy` — Rust-level proxy for compat JNI
//!   `prefixLookupNext` / `iteratorNext` row-at-a-time scans vs the chunked
//!   prefix iterator lower bound
//! - `compat_jni_multiget_proxy` — Rust-level proxy for compat JNI
//!   `multiGetAsList`: scalar `frs_get` loop vs single-CF `frs_batch_get`
//!
//! After the criterion groups, `boundary_tax_summary()` prints the derived
//! headline number per size: tax ns/row = (FFI ns/row) − (engine ns/row),
//! measured back-to-back (median-of-30) in the same process.
//!
//! B4 capture (design §B4): macOS has NO jemalloc (compile-gated off,
//! recorded TSD SIGSEGV) — a 1 Hz RSS sampler reports peak/end RSS instead.
//! Linux runs can additionally set `FRS_MEM_DIAG=1` for jemalloc stats.

use std::ffi::CString;
use std::ffi::OsString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, BatchSize, BenchmarkId, Criterion, Throughput};

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl, DEFAULT_CF_NAME};
use forst_rs_ffi::{
    frs_batch_get, frs_bytes_free, frs_compact_cf, frs_db_create_cf_with_merge, frs_db_open,
    frs_flush_cf, frs_get, frs_iterator_close, frs_iterator_next, frs_iterator_next_chunk,
    frs_prefix_lookup_open, frs_vec_iter_prefix_close, frs_vec_iter_prefix_next,
    frs_vec_iter_prefix_open, frs_vec_iter_prefix_open_batch,
    frs_vec_iter_prefix_open_batch_parallel, frs_vec_merge_append_batch, frs_vectorized_batch_get,
    frs_vectorized_batch_mixed, frs_vectorized_batch_put, FrsBytes, FrsCfHandle, FrsChunk, FrsDb,
    FrsIterator, FRS_CHUNK_EOF, FRS_STATUS_OK,
};
use forst_rs_io::{FileSystem, LocalFileSystem};
use forst_rs_storage::merge_operator::RawConcatMergeOperator;

const ROW_COUNTS: &[usize] = &[64, 256, 1024];
const VALUE_SIZES: &[usize] = &[64, 256, 1024];
const CHUNK_CAP: u32 = 64 * 1024; // 64 KiB — the production chunk size
const ITER_PREFIXES: usize = 100;
const ITER_ROWS_PER_PREFIX: usize = 1000;
const OPEN_BATCH_K: usize = 64;
const GET_POOL: usize = 10_000;
const Q7_PREFIX_COUNTS: &[usize] = &[64, 256];
const Q7_ROWS_PER_PREFIX: &[usize] = &[1, 4, 16];
const Q19_ROWS_PER_PREFIX: &[usize] = &[1, 4, 16, 32];
const Q19_BATCH_OPEN_K: &[usize] = &[64, 256];
const Q19_ALLOC_SPLIT_ROWS_PER_PREFIX: &[usize] = &[1, 16, 32];
const Q19_MERGE_KEYS: usize = 64;
const Q19_CHAIN_LENGTHS: &[usize] = &[1, 4, 16];
const COMPAT_PROXY_PREFIX_COUNTS: &[usize] = &[64, 256];
const COMPAT_PROXY_ROWS_PER_PREFIX: &[usize] = &[1, 4, 16, 32];
const COMPAT_PROXY_NEXT_CHUNK_ROWS: usize = 64;
const COMPAT_PROXY_NEXT_CHUNK_DATA_CAP: usize = 64 * 1024;
const COMPAT_MULTIGET_BATCH_GROUP_THRESHOLD: usize = 64;

// ---------------------------------------------------------------------------
// DB fixtures
// ---------------------------------------------------------------------------

/// FFI-side DB: opened through the REAL `frs_db_open` (LocalFileSystem on a
/// tempdir, RawConcat default CF — the exact shape the Java backend gets).
/// A dedicated bench CF is created via `frs_db_create_cf_with_merge` with
/// RawConcat so the mixed group's Merge rows are accepted.
struct FfiDb {
    db: FrsDb,
    cf: FrsCfHandle,
    _dir: tempfile::TempDir,
}

impl FfiDb {
    fn open() -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = CString::new(dir.path().to_string_lossy().into_owned()).expect("cstring");
        let mut db: FrsDb = std::ptr::null_mut();
        unsafe {
            assert_eq!(frs_db_open(path.as_ptr(), &mut db), FRS_STATUS_OK);
        }
        let cf_name = CString::new("bench").unwrap();
        let op_name = CString::new("RawConcatMergeOperator").unwrap();
        let mut cf: FrsCfHandle = std::ptr::null_mut();
        unsafe {
            assert_eq!(
                frs_db_create_cf_with_merge(db, cf_name.as_ptr(), op_name.as_ptr(), &mut cf),
                FRS_STATUS_OK
            );
        }
        FfiDb { db, cf, _dir: dir }
    }

    /// Setup-only escape hatch: the `FrsDb` handle is a leaked
    /// `Box<Arc<DbImpl>>` (the same contract `db_from_handle` relies on).
    /// Used ONLY for fixture work the C surface doesn't expose
    /// (switch_and_flush for the multi-SST get cell) — never inside a
    /// measured closure.
    fn engine(&self) -> &Arc<DbImpl> {
        unsafe { &*(self.db as *const Arc<DbImpl>) }
    }

    fn engine_cf(&self) -> ColumnFamilyHandle {
        unsafe { (*(self.cf as *const ColumnFamilyHandle)).clone() }
    }
}

impl Drop for FfiDb {
    fn drop(&mut self) {
        unsafe {
            forst_rs_ffi::frs_cf_close(self.cf);
            forst_rs_ffi::frs_db_close(self.db);
        }
    }
}

/// Engine-direct comparator DB: identical options + CF shape, no FFI.
struct EngineDb {
    db: Arc<DbImpl>,
    cf: ColumnFamilyHandle,
    _dir: tempfile::TempDir,
}

impl EngineDb {
    fn open() -> Self {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let opts = EngineOptions {
            db_path: dir.path().to_string_lossy().into_owned(),
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(LocalFileSystem::new());
        let default_desc = ColumnFamilyDescriptor::new(DEFAULT_CF_NAME)
            .with_merge_operator(Arc::new(RawConcatMergeOperator::new()));
        let db = DbImpl::open_with_fs_and_default_cf(opts, fs, default_desc).expect("open");
        let cf = db
            .create_column_family(
                ColumnFamilyDescriptor::new("bench")
                    .with_merge_operator(Arc::new(RawConcatMergeOperator::new())),
            )
            .expect("create bench cf");
        EngineDb { db, cf, _dir: dir }
    }
}

// ---------------------------------------------------------------------------
// Arrow-offsets column builders (mirror the FFI UT layout helpers)
// ---------------------------------------------------------------------------

/// One Arrow BinaryArray-style column: `count + 1` i32 offsets + data blob.
struct Cols {
    offs: Vec<i32>,
    data: Vec<u8>,
}

fn cols_from<'a>(items: impl Iterator<Item = &'a [u8]>) -> Cols {
    let mut offs = vec![0i32];
    let mut data = Vec::new();
    for it in items {
        data.extend_from_slice(it);
        offs.push(data.len() as i32);
    }
    Cols { offs, data }
}

fn make_keys(n: usize, salt: &str) -> Vec<Vec<u8>> {
    (0..n)
        .map(|i| format!("k/{salt}/{i:012}").into_bytes())
        .collect()
}

fn make_value(len: usize, seed: usize) -> Vec<u8> {
    // Deterministic xorshift fill (same pattern as the merge_operator UTs).
    let mut state = (seed as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut v = Vec::with_capacity(len);
    while v.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        v.extend_from_slice(&state.to_le_bytes());
    }
    v.truncate(len);
    v
}

/// Mixed-batch kinds column: a deterministic 5-cycle.
/// merge_pct = 0  → [Put, Put, Put, Delete, Put]            (20 % delete)
/// merge_pct = 20 → [Put, Put, Merge, Delete, Put]          (20 % delete, 20 % merge)
fn mixed_kinds(rows: usize, merge_pct: usize) -> Vec<u8> {
    (0..rows)
        .map(|i| match i % 5 {
            2 if merge_pct > 0 => 2u8, // Merge
            3 => 0u8,                  // Delete
            _ => 1u8,                  // Put
        })
        .collect()
}

/// Values column honoring the mixed contract: Delete rows MUST be
/// zero-length (offsets equal).
fn mixed_value_cols(kinds: &[u8], vsize: usize) -> Cols {
    let mut offs = vec![0i32];
    let mut data = Vec::new();
    for (i, k) in kinds.iter().enumerate() {
        if *k != 0 {
            data.extend_from_slice(&make_value(vsize, i));
        }
        offs.push(data.len() as i32);
    }
    Cols { offs, data }
}

fn q_iter_prefix(ns: &str, prefix_id: usize) -> Vec<u8> {
    format!("{ns}/p{prefix_id:05}/").into_bytes()
}

fn q_iter_key(ns: &str, prefix_id: usize, row: usize) -> Vec<u8> {
    format!("{ns}/p{prefix_id:05}/r{row:04}").into_bytes()
}

fn q_iter_value(ns: &str, prefix_id: usize, row: usize) -> Vec<u8> {
    format!("{ns}/v{prefix_id:05}/{row:04}").into_bytes()
}

fn populate_iter_fixture(
    d: &FfiDb,
    ns: &str,
    prefix_count: usize,
    rows_per_prefix: usize,
) -> (Vec<Vec<u8>>, Vec<Vec<(Vec<u8>, Vec<u8>)>>) {
    let eng = d.engine().clone();
    let cfh = d.engine_cf();
    let prefixes: Vec<Vec<u8>> = (0..prefix_count).map(|p| q_iter_prefix(ns, p)).collect();
    let mut expected = Vec::with_capacity(prefix_count);
    for p in 0..prefix_count {
        let mut rows = Vec::with_capacity(rows_per_prefix);
        for r in 0..rows_per_prefix {
            let key = q_iter_key(ns, p, r);
            let value = q_iter_value(ns, p, r);
            eng.put(&cfh, &key, &value).expect("iter fixture put");
            rows.push((key, value));
        }
        expected.push(rows);
    }
    (prefixes, expected)
}

fn decode_chunk_rows(buf: &[u8], bytes_used: u32, row_count: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::with_capacity(row_count as usize);
    let mut pos = 0usize;
    let limit = bytes_used as usize;
    for _ in 0..row_count {
        assert!(pos + 8 <= limit, "chunk row header truncated");
        let klen = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        let vlen = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap()) as usize;
        pos += 8;
        assert!(pos + klen + vlen <= limit, "chunk row payload truncated");
        let key = buf[pos..pos + klen].to_vec();
        pos += klen;
        let value = buf[pos..pos + vlen].to_vec();
        pos += vlen;
        out.push((key, value));
    }
    assert_eq!(pos, limit, "chunk bytes_used must match decoded rows");
    out
}

fn drain_prefix_rows_serial(d: &FfiDb, prefix: &[u8], buf: &mut [u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut handle = 0u64;
    let mut rows = 0u32;
    let mut bytes = 0u32;
    let rc = unsafe {
        frs_vec_iter_prefix_open(
            d.db,
            d.cf,
            prefix.as_ptr(),
            prefix.len() as u32,
            buf.as_mut_ptr(),
            buf.len() as u32,
            &mut handle,
            &mut rows,
            &mut bytes,
        )
    };
    assert_eq!(rc, 0, "frs_vec_iter_prefix_open failed: {rc}");
    let mut out = decode_chunk_rows(buf, bytes, rows);
    if handle != 0 {
        loop {
            let rc = unsafe {
                frs_vec_iter_prefix_next(
                    handle,
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                    &mut rows,
                    &mut bytes,
                )
            };
            assert_eq!(rc, 0, "frs_vec_iter_prefix_next failed: {rc}");
            if rows == 0 {
                frs_vec_iter_prefix_close(handle);
                break;
            }
            out.extend(decode_chunk_rows(buf, bytes, rows));
        }
    }
    out
}

fn drain_prefixes_batch_parallel(
    d: &FfiDb,
    prefixes: &[Vec<u8>],
    chunk_cap: u32,
) -> Vec<Vec<(Vec<u8>, Vec<u8>)>> {
    let prefix_cols = cols_from(prefixes.iter().map(|p| p.as_slice()));
    let offs_u32: Vec<u32> = prefix_cols.offs.iter().map(|&o| o as u32).collect();
    let mut bufs: Vec<Vec<u8>> = (0..prefixes.len())
        .map(|_| vec![0u8; chunk_cap as usize])
        .collect();
    let mut handles = vec![0u64; prefixes.len()];
    let mut chunks: Vec<FrsChunk> = bufs
        .iter_mut()
        .map(|bb| FrsChunk {
            buf_ptr: bb.as_mut_ptr(),
            buf_cap: chunk_cap,
            row_count: 0,
            bytes_used: 0,
            _reserved: 0,
        })
        .collect();
    let rc = unsafe {
        frs_vec_iter_prefix_open_batch_parallel(
            d.db,
            d.cf,
            offs_u32.as_ptr(),
            prefix_cols.data.as_ptr(),
            prefixes.len() as u32,
            handles.as_mut_ptr(),
            chunks.as_mut_ptr(),
            chunk_cap,
        )
    };
    assert_eq!(rc, 0, "open_batch_parallel failed: {rc}");

    let mut all = Vec::with_capacity(prefixes.len());
    for i in 0..prefixes.len() {
        assert_ne!(handles[i], 0, "batch probe {i} handle must be non-zero");
        let mut rows = decode_chunk_rows(&bufs[i], chunks[i].bytes_used, chunks[i].row_count);
        let eof = chunks[i]._reserved & FRS_CHUNK_EOF != 0;
        if !eof {
            loop {
                let mut n_rows = 0u32;
                let mut n_bytes = 0u32;
                let rc = unsafe {
                    frs_vec_iter_prefix_next(
                        handles[i],
                        bufs[i].as_mut_ptr(),
                        chunk_cap,
                        &mut n_rows,
                        &mut n_bytes,
                    )
                };
                assert_eq!(rc, 0, "batch probe {i} next failed: {rc}");
                if n_rows == 0 {
                    frs_vec_iter_prefix_close(handles[i]);
                    break;
                }
                rows.extend(decode_chunk_rows(&bufs[i], n_bytes, n_rows));
            }
        }
        all.push(rows);
    }
    all
}

fn assert_iter_fixture_correct(
    d: &FfiDb,
    prefixes: &[Vec<u8>],
    expected: &[Vec<(Vec<u8>, Vec<u8>)>],
) {
    let mut buf = vec![0u8; CHUNK_CAP as usize];
    for (i, prefix) in prefixes.iter().enumerate() {
        let rows = drain_prefix_rows_serial(d, prefix, &mut buf);
        assert_eq!(rows, expected[i], "serial rows mismatch for prefix {i}");
    }
    let batch_rows = drain_prefixes_batch_parallel(d, prefixes, CHUNK_CAP);
    assert_eq!(batch_rows.len(), expected.len());
    for (i, rows) in batch_rows.into_iter().enumerate() {
        assert_eq!(rows, expected[i], "parallel rows mismatch for prefix {i}");
    }
}

fn drain_prefixes_serial_count(d: &FfiDb, prefixes: &[Vec<u8>], buf: &mut [u8]) -> u64 {
    prefixes
        .iter()
        .map(|prefix| drain_prefix(d, prefix, buf).0)
        .sum()
}

fn drain_prefixes_parallel_count(d: &FfiDb, prefixes: &[Vec<u8>]) -> u64 {
    drain_prefixes_batch_parallel(d, prefixes, CHUNK_CAP)
        .iter()
        .map(|rows| rows.len() as u64)
        .sum()
}

fn build_batch_chunks(n: usize, chunk_cap: u32) -> (Vec<Vec<u8>>, Vec<FrsChunk>) {
    let mut bufs: Vec<Vec<u8>> = (0..n).map(|_| vec![0u8; chunk_cap as usize]).collect();
    let chunks = bufs
        .iter_mut()
        .map(|bb| FrsChunk {
            buf_ptr: bb.as_mut_ptr(),
            buf_cap: chunk_cap,
            row_count: 0,
            bytes_used: 0,
            _reserved: 0,
        })
        .collect();
    (bufs, chunks)
}

type PrefixBatchOpenFn = unsafe extern "C" fn(
    FrsDb,
    FrsCfHandle,
    *const u32,
    *const u8,
    u32,
    *mut u64,
    *mut FrsChunk,
    u32,
) -> i32;

fn open_batch_count_close_with(
    d: &FfiDb,
    prefix_cols: &Cols,
    offs_u32: &[u32],
    handles: &mut [u64],
    chunks: &mut [FrsChunk],
    chunk_cap: u32,
    open_batch: PrefixBatchOpenFn,
    label: &str,
) -> u64 {
    handles.fill(0);
    for chunk in chunks.iter_mut() {
        chunk.row_count = 0;
        chunk.bytes_used = 0;
        chunk._reserved = 0;
    }
    let rc = unsafe {
        open_batch(
            d.db,
            d.cf,
            offs_u32.as_ptr(),
            prefix_cols.data.as_ptr(),
            handles.len() as u32,
            handles.as_mut_ptr(),
            chunks.as_mut_ptr(),
            chunk_cap,
        )
    };
    assert_eq!(rc, 0, "{label} failed: {rc}");

    let mut rows = 0u64;
    for (i, &handle) in handles.iter().enumerate() {
        assert_ne!(handle, 0, "batch probe {i} handle must be non-zero");
        rows += chunks[i].row_count as u64;
        if chunks[i]._reserved & FRS_CHUNK_EOF == 0 {
            frs_vec_iter_prefix_close(handle);
        }
    }
    rows
}

fn open_batch_count_close(
    d: &FfiDb,
    prefix_cols: &Cols,
    offs_u32: &[u32],
    handles: &mut [u64],
    chunks: &mut [FrsChunk],
    chunk_cap: u32,
) -> u64 {
    open_batch_count_close_with(
        d,
        prefix_cols,
        offs_u32,
        handles,
        chunks,
        chunk_cap,
        frs_vec_iter_prefix_open_batch_parallel,
        "open_batch_parallel",
    )
}

struct BatchOpenOnlyFixture {
    prefix_cols: Cols,
    offs_u32: Vec<u32>,
    _bufs: Vec<Vec<u8>>,
    handles: Vec<u64>,
    chunks: Vec<FrsChunk>,
    chunk_cap: u32,
}

impl BatchOpenOnlyFixture {
    fn new(prefixes: &[Vec<u8>], chunk_cap: u32) -> Self {
        let prefix_cols = cols_from(prefixes.iter().map(|p| p.as_slice()));
        let offs_u32: Vec<u32> = prefix_cols.offs.iter().map(|&o| o as u32).collect();
        let (bufs, chunks) = build_batch_chunks(prefixes.len(), chunk_cap);
        Self {
            prefix_cols,
            offs_u32,
            _bufs: bufs,
            handles: vec![0u64; prefixes.len()],
            chunks,
            chunk_cap,
        }
    }

    fn open_count_close(&mut self, d: &FfiDb) -> u64 {
        open_batch_count_close(
            d,
            &self.prefix_cols,
            &self.offs_u32,
            &mut self.handles,
            &mut self.chunks,
            self.chunk_cap,
        )
    }

    fn open_count_close_with(
        &mut self,
        d: &FfiDb,
        open_batch: PrefixBatchOpenFn,
        label: &str,
    ) -> u64 {
        open_batch_count_close_with(
            d,
            &self.prefix_cols,
            &self.offs_u32,
            &mut self.handles,
            &mut self.chunks,
            self.chunk_cap,
            open_batch,
            label,
        )
    }
}

fn batch_open_count_alloc_each_iter_with(
    d: &FfiDb,
    prefix_cols: &Cols,
    offs_u32: &[u32],
    open_batch: PrefixBatchOpenFn,
    label: &str,
) -> u64 {
    let n = offs_u32.len() - 1;
    let (_bufs, mut chunks) = build_batch_chunks(n, CHUNK_CAP);
    let mut handles = vec![0u64; n];
    open_batch_count_close_with(
        d,
        prefix_cols,
        offs_u32,
        &mut handles,
        &mut chunks,
        CHUNK_CAP,
        open_batch,
        label,
    )
}

#[derive(Clone, Copy)]
enum MergeShape {
    DistinctKeys,
    SameKeyChain,
}

struct MergeBatchFixture {
    keys_off: Vec<u32>,
    keys_data: Vec<u8>,
    ops_off: Vec<u32>,
    ops_data: Vec<u8>,
    read_keys: Vec<Vec<u8>>,
    expected_values: Vec<Vec<u8>>,
    rows: usize,
}

struct MergeReadFixture {
    keys: Cols,
    expected_values: Vec<Vec<u8>>,
    out_offsets: Vec<i32>,
    out_data: Vec<u8>,
    out_validity: Vec<u8>,
}

#[derive(Clone, Copy)]
enum MergeReadStage {
    Memtable,
    Flushed,
    Compacted,
}

impl MergeReadStage {
    fn label(self) -> &'static str {
        match self {
            MergeReadStage::Memtable => "memtable_read",
            MergeReadStage::Flushed => "flushed_read",
            MergeReadStage::Compacted => "compacted_read",
        }
    }
}

fn append_u32_col(col_off: &mut Vec<u32>, col_data: &mut Vec<u8>, value: &[u8]) {
    col_data.extend_from_slice(value);
    col_off.push(col_data.len() as u32);
}

fn merge_operand(salt: usize, key_id: usize, chain_idx: usize) -> Vec<u8> {
    format!("op/{salt:08}/{key_id:04}/{chain_idx:02};").into_bytes()
}

fn build_merge_batch_fixture(
    shape: MergeShape,
    chain_len: usize,
    salt: usize,
) -> MergeBatchFixture {
    let mut keys_off = vec![0u32];
    let mut keys_data = Vec::new();
    let mut ops_off = vec![0u32];
    let mut ops_data = Vec::new();
    let mut read_keys = Vec::new();
    let mut expected_values = Vec::new();

    match shape {
        MergeShape::DistinctKeys => {
            let rows = Q19_MERGE_KEYS * chain_len;
            read_keys.reserve(rows);
            expected_values.reserve(rows);
            for row in 0..rows {
                let key = format!("q19/distinct/{salt:08}/k{row:05}").into_bytes();
                let operand = merge_operand(salt, row, 0);
                append_u32_col(&mut keys_off, &mut keys_data, &key);
                append_u32_col(&mut ops_off, &mut ops_data, &operand);
                read_keys.push(key);
                expected_values.push(operand);
            }
        }
        MergeShape::SameKeyChain => {
            read_keys.reserve(Q19_MERGE_KEYS);
            expected_values.reserve(Q19_MERGE_KEYS);
            for key_id in 0..Q19_MERGE_KEYS {
                let key = format!("q19/chain/{salt:08}/k{key_id:04}").into_bytes();
                let mut expected = Vec::new();
                for chain_idx in 0..chain_len {
                    let operand = merge_operand(salt, key_id, chain_idx);
                    append_u32_col(&mut keys_off, &mut keys_data, &key);
                    append_u32_col(&mut ops_off, &mut ops_data, &operand);
                    expected.extend_from_slice(&operand);
                }
                read_keys.push(key);
                expected_values.push(expected);
            }
        }
    }

    let rows = keys_off.len() - 1;
    MergeBatchFixture {
        keys_off,
        keys_data,
        ops_off,
        ops_data,
        read_keys,
        expected_values,
        rows,
    }
}

fn run_merge_append_batch(d: &FfiDb, fixture: &MergeBatchFixture) {
    let rc = unsafe {
        frs_vec_merge_append_batch(
            d.db,
            d.cf,
            fixture.keys_off.as_ptr(),
            fixture.keys_data.as_ptr(),
            fixture.keys_data.len(),
            fixture.ops_off.as_ptr(),
            fixture.ops_data.as_ptr(),
            fixture.ops_data.len(),
            fixture.rows as u32,
        )
    };
    assert_eq!(rc, 0, "frs_vec_merge_append_batch failed: {rc}");
}

fn ffi_flush_cf(d: &FfiDb) {
    let rc = unsafe { frs_flush_cf(d.db, d.cf) };
    assert_eq!(rc, FRS_STATUS_OK, "frs_flush_cf failed: {rc}");
}

fn ffi_compact_cf(d: &FfiDb) {
    let rc = unsafe { frs_compact_cf(d.db, d.cf) };
    assert_eq!(rc, FRS_STATUS_OK, "frs_compact_cf failed: {rc}");
}

fn build_merge_read_fixture(fixture: &MergeBatchFixture) -> MergeReadFixture {
    let keys = cols_from(fixture.read_keys.iter().map(|k| k.as_slice()));
    let expected_bytes: usize = fixture.expected_values.iter().map(|v| v.len()).sum();
    MergeReadFixture {
        keys,
        expected_values: fixture.expected_values.clone(),
        out_offsets: vec![0i32; fixture.read_keys.len() + 1],
        out_data: vec![0u8; expected_bytes + fixture.read_keys.len() * 8 + 1],
        out_validity: vec![0u8; fixture.read_keys.len()],
    }
}

fn run_merge_read_get(d: &FfiDb, fixture: &mut MergeReadFixture) -> usize {
    fixture.out_offsets.fill(0);
    fixture.out_validity.fill(0);
    let mut out_len = 0usize;
    let rc = unsafe {
        frs_vectorized_batch_get(
            d.db,
            d.cf,
            fixture.keys.offs.as_ptr(),
            fixture.keys.data.as_ptr(),
            fixture.keys.data.len(),
            fixture.expected_values.len(),
            fixture.out_offsets.as_mut_ptr(),
            fixture.out_data.as_mut_ptr(),
            fixture.out_validity.as_mut_ptr(),
            fixture.out_data.len(),
            &mut out_len,
        )
    };
    assert_eq!(rc, 0, "frs_vectorized_batch_get failed: {rc}");
    for i in 0..fixture.expected_values.len() {
        assert_eq!(
            fixture.out_validity[i], 1,
            "missing merge value for key {i}"
        );
        let start = fixture.out_offsets[i] as usize;
        let end = fixture.out_offsets[i + 1] as usize;
        assert_eq!(
            &fixture.out_data[start..end],
            fixture.expected_values[i].as_slice(),
            "merged value mismatch for key {i}"
        );
    }
    assert_eq!(
        fixture.out_offsets[fixture.expected_values.len()] as usize,
        out_len
    );
    out_len
}

fn copy_expected_values_to_vec_get_buffers(
    expected_values: &[Vec<u8>],
    out_offsets: &mut [i32],
    out_data: &mut [u8],
    out_validity: &mut [u8],
) -> usize {
    out_offsets.fill(0);
    out_validity.fill(0);
    let mut pos = 0usize;
    for (i, value) in expected_values.iter().enumerate() {
        let end = pos + value.len();
        out_data[pos..end].copy_from_slice(value);
        out_validity[i] = 1;
        out_offsets[i + 1] = end as i32;
        pos = end;
    }
    pos
}

fn assert_merge_fixture_readable(d: &FfiDb, fixture: &MergeBatchFixture) {
    let mut read_fixture = build_merge_read_fixture(fixture);
    run_merge_read_get(d, &mut read_fixture);
}

fn prepare_merge_read_db(
    shape: MergeShape,
    chain_len: usize,
    salt: usize,
    stage: MergeReadStage,
) -> (FfiDb, MergeReadFixture) {
    let d = FfiDb::open();
    let fixture = build_merge_batch_fixture(shape, chain_len, salt);
    run_merge_append_batch(&d, &fixture);
    match stage {
        MergeReadStage::Memtable => {}
        MergeReadStage::Flushed => ffi_flush_cf(&d),
        MergeReadStage::Compacted => ffi_compact_cf(&d),
    }
    let mut read_fixture = build_merge_read_fixture(&fixture);
    run_merge_read_get(&d, &mut read_fixture);
    (d, read_fixture)
}

// ---------------------------------------------------------------------------
// put: frs_vectorized_batch_put vs engine batch_put_borrowed_single_cf
// ---------------------------------------------------------------------------

fn ffi_put_once(d: &FfiDb, keys: &Cols, vals: &Cols, rows: usize) {
    let rc = unsafe {
        frs_vectorized_batch_put(
            d.db,
            d.cf,
            keys.offs.as_ptr(),
            keys.data.as_ptr(),
            keys.data.len(),
            vals.offs.as_ptr(),
            vals.data.as_ptr(),
            vals.data.len(),
            rows,
        )
    };
    assert_eq!(rc, 0, "frs_vectorized_batch_put failed: {rc}");
}

fn engine_put_once(
    db: &DbImpl,
    cf: &ColumnFamilyHandle,
    keys: &[&[u8]],
    vals: &[Option<&[u8]>],
    op_types: &[u8],
) {
    db.batch_put_borrowed_single_cf(cf, keys, vals, op_types)
        .expect("engine batch put");
}

fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/put");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_millis(900));
    group.warm_up_time(std::time::Duration::from_millis(300));

    for &rows in ROW_COUNTS {
        for &vsize in VALUE_SIZES {
            let key_vecs = make_keys(rows, "put");
            let val_vecs: Vec<Vec<u8>> = (0..rows).map(|i| make_value(vsize, i)).collect();
            let keys = cols_from(key_vecs.iter().map(|k| k.as_slice()));
            let vals = cols_from(val_vecs.iter().map(|v| v.as_slice()));

            group.throughput(Throughput::Elements(rows as u64));

            let d = FfiDb::open();
            group.bench_with_input(
                BenchmarkId::new("ffi", format!("r{rows}_v{vsize}")),
                &rows,
                |b, &rows| b.iter(|| ffi_put_once(&d, &keys, &vals, rows)),
            );
            drop(d);

            let e = EngineDb::open();
            let key_slices: Vec<&[u8]> = key_vecs.iter().map(|k| k.as_slice()).collect();
            let val_slices: Vec<Option<&[u8]>> =
                val_vecs.iter().map(|v| Some(v.as_slice())).collect();
            let op_types = vec![1u8; rows];
            group.bench_with_input(
                BenchmarkId::new("engine", format!("r{rows}_v{vsize}")),
                &rows,
                |b, _| {
                    b.iter(|| engine_put_once(&e.db, &e.cf, &key_slices, &val_slices, &op_types))
                },
            );
        }
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// mixed: frs_vectorized_batch_mixed vs the same engine entry
// ---------------------------------------------------------------------------

fn bench_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/mixed");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_millis(900));
    group.warm_up_time(std::time::Duration::from_millis(300));

    for &rows in ROW_COUNTS {
        for &vsize in VALUE_SIZES {
            for &merge_pct in &[0usize, 20] {
                let key_vecs = make_keys(rows, "mix");
                let kinds = mixed_kinds(rows, merge_pct);
                let keys = cols_from(key_vecs.iter().map(|k| k.as_slice()));
                let vals = mixed_value_cols(&kinds, vsize);
                let label = format!("r{rows}_v{vsize}_m{merge_pct}");

                group.throughput(Throughput::Elements(rows as u64));

                let d = FfiDb::open();
                group.bench_with_input(BenchmarkId::new("ffi", &label), &rows, |b, &rows| {
                    b.iter(|| {
                        let rc = unsafe {
                            frs_vectorized_batch_mixed(
                                d.db,
                                d.cf,
                                kinds.as_ptr(),
                                rows,
                                keys.offs.as_ptr(),
                                keys.data.as_ptr(),
                                keys.data.len(),
                                vals.offs.as_ptr(),
                                vals.data.as_ptr(),
                                vals.data.len(),
                            )
                        };
                        assert_eq!(rc, 0, "frs_vectorized_batch_mixed failed: {rc}");
                    })
                });
                drop(d);

                let e = EngineDb::open();
                let key_slices: Vec<&[u8]> = key_vecs.iter().map(|k| k.as_slice()).collect();
                let val_slices: Vec<Option<&[u8]>> = kinds
                    .iter()
                    .enumerate()
                    .map(|(i, k)| {
                        if *k == 0 {
                            None
                        } else {
                            let s = vals.offs[i] as usize;
                            let t = vals.offs[i + 1] as usize;
                            Some(&vals.data[s..t])
                        }
                    })
                    .collect();
                group.bench_with_input(BenchmarkId::new("engine", &label), &rows, |b, _| {
                    b.iter(|| engine_put_once(&e.db, &e.cf, &key_slices, &val_slices, &kinds))
                });
            }
        }
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// get: warm (memtable-resident) + multi-SST
// ---------------------------------------------------------------------------

struct GetFixture {
    keys: Cols,
    key_slices_owned: Vec<Vec<u8>>,
    out_offs: Vec<i32>,
    out_data: Vec<u8>,
    out_validity: Vec<u8>,
}

fn get_fixture(rows: usize, vsize: usize) -> GetFixture {
    let pool = make_keys(GET_POOL, "get");
    let key_slices_owned: Vec<Vec<u8>> = (0..rows)
        .map(|i| pool[(i * 37) % GET_POOL].clone())
        .collect();
    let keys = cols_from(key_slices_owned.iter().map(|k| k.as_slice()));
    GetFixture {
        keys,
        key_slices_owned,
        out_offs: vec![0i32; rows + 1],
        out_data: vec![0u8; rows * (vsize + 16)],
        out_validity: vec![0u8; rows],
    }
}

fn populate_get_pool(put: &mut dyn FnMut(&[u8], &[u8]), vsize: usize) {
    let pool = make_keys(GET_POOL, "get");
    for (i, k) in pool.iter().enumerate() {
        put(k, &make_value(vsize, i));
    }
}

fn ffi_get_once(d: &FfiDb, f: &mut GetFixture, rows: usize) {
    let mut out_len = 0usize;
    let rc = unsafe {
        frs_vectorized_batch_get(
            d.db,
            d.cf,
            f.keys.offs.as_ptr(),
            f.keys.data.as_ptr(),
            f.keys.data.len(),
            rows,
            f.out_offs.as_mut_ptr(),
            f.out_data.as_mut_ptr(),
            f.out_validity.as_mut_ptr(),
            f.out_data.len(),
            &mut out_len,
        )
    };
    assert_eq!(rc, 0, "frs_vectorized_batch_get failed: {rc}");
    assert!(f.out_validity.iter().all(|&v| v == 1), "missed key");
}

fn bench_get(c: &mut Criterion) {
    const VSIZE: usize = 64;

    // --- warm: everything memtable-resident ---
    {
        let mut group = c.benchmark_group("ffi_vectorized/get_warm");
        group.sample_size(20);
        group.measurement_time(std::time::Duration::from_millis(900));
        group.warm_up_time(std::time::Duration::from_millis(300));

        let d = FfiDb::open();
        {
            let eng = d.engine().clone();
            let cfh = d.engine_cf();
            populate_get_pool(
                &mut |k, v| {
                    eng.put(&cfh, k, v).expect("put");
                },
                VSIZE,
            );
        }
        let e = EngineDb::open();
        populate_get_pool(
            &mut |k, v| {
                e.db.put(&e.cf, k, v).expect("put");
            },
            VSIZE,
        );

        for &rows in ROW_COUNTS {
            let mut f = get_fixture(rows, VSIZE);
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_with_input(BenchmarkId::new("ffi", rows), &rows, |b, &rows| {
                b.iter(|| ffi_get_once(&d, &mut f, rows))
            });

            let key_slices: Vec<&[u8]> = f.key_slices_owned.iter().map(|k| k.as_slice()).collect();
            group.bench_with_input(BenchmarkId::new("engine", rows), &rows, |b, _| {
                b.iter(|| {
                    let res = e.db.batch_get(&e.cf, &key_slices).expect("batch_get");
                    assert!(res.iter().all(|r| r.is_some()));
                })
            });
        }
        group.finish();
    }

    // --- multi-SST: pool spread across 8 flushed L0 SSTs ---
    {
        let mut group = c.benchmark_group("ffi_vectorized/get_multisst");
        group.sample_size(20);
        group.measurement_time(std::time::Duration::from_millis(900));
        group.warm_up_time(std::time::Duration::from_millis(300));

        const ROWS: usize = 256;
        let d = FfiDb::open();
        let e = EngineDb::open();
        {
            // 8 write-then-flush waves (setup only; switch_and_flush is not
            // on the C surface, so the FFI DB uses the documented
            // Box<Arc<DbImpl>> handle contract for FIXTURE work).
            let eng = d.engine().clone();
            let cfh = d.engine_cf();
            let pool = make_keys(GET_POOL, "get");
            for wave in 0..8 {
                for (i, k) in pool.iter().enumerate().filter(|(i, _)| i % 8 == wave) {
                    eng.put(&cfh, k, &make_value(VSIZE, i)).expect("put");
                    e.db.put(&e.cf, k, &make_value(VSIZE, i)).expect("put");
                }
                eng.switch_and_flush(&cfh).expect("flush");
                e.db.switch_and_flush(&e.cf).expect("flush");
            }
        }

        let mut f = get_fixture(ROWS, VSIZE);
        group.throughput(Throughput::Elements(ROWS as u64));
        group.bench_with_input(BenchmarkId::new("ffi", ROWS), &ROWS, |b, &rows| {
            b.iter(|| ffi_get_once(&d, &mut f, rows))
        });
        let key_slices: Vec<&[u8]> = f.key_slices_owned.iter().map(|k| k.as_slice()).collect();
        group.bench_with_input(BenchmarkId::new("engine", ROWS), &ROWS, |b, _| {
            b.iter(|| {
                let res = e.db.batch_get(&e.cf, &key_slices).expect("batch_get");
                assert!(res.iter().all(|r| r.is_some()));
            })
        });
        group.finish();
    }
}

fn compat_scalar_get_loop_once(d: &FfiDb, keys: &[Vec<u8>]) -> usize {
    let mut found = 0usize;
    for key in keys {
        let mut out = FrsBytes {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let rc = unsafe { frs_get(d.db, d.cf, key.as_ptr(), key.len(), &mut out) };
        assert_eq!(rc, FRS_STATUS_OK, "frs_get failed: {rc}");
        assert!(!out.data.is_null(), "missing key");
        found += 1;
        unsafe {
            let _ = frs_bytes_free(&mut out);
        }
    }
    found
}

fn compat_batch_get_single_cf_once(d: &FfiDb, keys: &[Vec<u8>]) -> usize {
    let key_ptrs: Vec<*const u8> = keys.iter().map(|k| k.as_ptr()).collect();
    let key_lens: Vec<usize> = keys.iter().map(|k| k.len()).collect();
    let mut out_slots: Vec<FrsBytes> = (0..keys.len())
        .map(|_| FrsBytes {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        })
        .collect();
    let rc = unsafe {
        frs_batch_get(
            d.db,
            d.cf,
            key_ptrs.as_ptr(),
            key_lens.as_ptr(),
            keys.len(),
            out_slots.as_mut_ptr(),
        )
    };
    assert_eq!(rc, FRS_STATUS_OK, "frs_batch_get failed: {rc}");
    let found = out_slots.iter().filter(|slot| !slot.data.is_null()).count();
    assert_eq!(found, keys.len(), "missing key");
    for slot in out_slots.iter_mut() {
        unsafe {
            let _ = frs_bytes_free(slot);
        }
    }
    found
}

fn compat_adaptive_get_once(d: &FfiDb, keys: &[Vec<u8>]) -> usize {
    if keys.len() <= COMPAT_MULTIGET_BATCH_GROUP_THRESHOLD {
        compat_batch_get_single_cf_once(d, keys)
    } else {
        compat_scalar_get_loop_once(d, keys)
    }
}

fn bench_compat_jni_multiget_proxy(c: &mut Criterion) {
    const VSIZE: usize = 64;
    let mut group = c.benchmark_group("ffi_vectorized/compat_jni_multiget_proxy");
    group.sample_size(20);
    group.measurement_time(std::time::Duration::from_millis(900));
    group.warm_up_time(std::time::Duration::from_millis(300));

    let d = FfiDb::open();
    {
        let eng = d.engine().clone();
        let cfh = d.engine_cf();
        populate_get_pool(
            &mut |k, v| {
                eng.put(&cfh, k, v).expect("put");
            },
            VSIZE,
        );
    }

    for &rows in ROW_COUNTS {
        let fixture = get_fixture(rows, VSIZE);
        let keys = fixture.key_slices_owned;
        assert_eq!(compat_scalar_get_loop_once(&d, &keys), rows);
        assert_eq!(compat_batch_get_single_cf_once(&d, &keys), rows);
        assert_eq!(compat_adaptive_get_once(&d, &keys), rows);

        group.throughput(Throughput::Elements(rows as u64));
        group.bench_with_input(BenchmarkId::new("scalar_get_loop", rows), &rows, |b, _| {
            b.iter(|| {
                let found = compat_scalar_get_loop_once(&d, &keys);
                std::hint::black_box(found);
            })
        });
        group.bench_with_input(
            BenchmarkId::new("batch_get_single_cf", rows),
            &rows,
            |b, _| {
                b.iter(|| {
                    let found = compat_batch_get_single_cf_once(&d, &keys);
                    std::hint::black_box(found);
                })
            },
        );
        group.bench_with_input(BenchmarkId::new("adaptive_grouped", rows), &rows, |b, _| {
            b.iter(|| {
                let found = compat_adaptive_get_once(&d, &keys);
                std::hint::black_box(found);
            })
        });
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// iter: chunked prefix drain + batched parallel open
// ---------------------------------------------------------------------------

/// Fully drains one prefix through the REAL chunked-iterator surface.
/// Returns (rows, crossings). Honors the FRS_CHUNK_EOF / handle==0
/// auto-close contract.
fn drain_prefix(d: &FfiDb, prefix: &[u8], buf: &mut [u8]) -> (u64, u64) {
    let mut handle = 0u64;
    let mut rows = 0u32;
    let mut bytes = 0u32;
    let mut crossings = 1u64;
    let rc = unsafe {
        frs_vec_iter_prefix_open(
            d.db,
            d.cf,
            prefix.as_ptr(),
            prefix.len() as u32,
            buf.as_mut_ptr(),
            buf.len() as u32,
            &mut handle,
            &mut rows,
            &mut bytes,
        )
    };
    assert_eq!(rc, 0, "frs_vec_iter_prefix_open failed: {rc}");
    let mut total = rows as u64;
    // handle == 0 ⇒ exhausted at open (auto-closed); skip _next/_close.
    if handle != 0 {
        loop {
            let rc = unsafe {
                frs_vec_iter_prefix_next(
                    handle,
                    buf.as_mut_ptr(),
                    buf.len() as u32,
                    &mut rows,
                    &mut bytes,
                )
            };
            assert_eq!(rc, 0, "frs_vec_iter_prefix_next failed: {rc}");
            crossings += 1;
            if rows == 0 {
                frs_vec_iter_prefix_close(handle);
                crossings += 1;
                break;
            }
            total += rows as u64;
        }
    }
    (total, crossings)
}

fn bench_iter(c: &mut Criterion) {
    const VSIZE: usize = 64;
    let d = FfiDb::open();
    {
        let eng = d.engine().clone();
        let cfh = d.engine_cf();
        for p in 0..ITER_PREFIXES {
            for i in 0..ITER_ROWS_PER_PREFIX {
                let k = format!("p{p:03}/{i:06}");
                eng.put(&cfh, k.as_bytes(), &make_value(VSIZE, p * 7 + i))
                    .expect("put");
            }
        }
    }
    let prefixes: Vec<Vec<u8>> = (0..ITER_PREFIXES)
        .map(|p| format!("p{p:03}/").into_bytes())
        .collect();
    let mut buf = vec![0u8; CHUNK_CAP as usize];

    // Sanity + chunks/crossing report (once, outside measurement).
    let mut total_rows = 0u64;
    let mut total_crossings = 0u64;
    for p in &prefixes {
        let (r, x) = drain_prefix(&d, p, &mut buf);
        total_rows += r;
        total_crossings += x;
    }
    assert_eq!(
        total_rows,
        (ITER_PREFIXES * ITER_ROWS_PER_PREFIX) as u64,
        "iter fixture must round-trip every row"
    );
    println!(
        "[iter_drain] {} prefixes x {} rows, 64 KiB chunks: {} FFI crossings total \
         ({:.2} crossings/prefix, {:.1} rows/crossing)",
        ITER_PREFIXES,
        ITER_ROWS_PER_PREFIX,
        total_crossings,
        total_crossings as f64 / ITER_PREFIXES as f64,
        total_rows as f64 / total_crossings as f64,
    );

    {
        let mut group = c.benchmark_group("ffi_vectorized/iter_drain");
        group.sample_size(10);
        group.measurement_time(std::time::Duration::from_secs(3));
        group.throughput(Throughput::Elements(total_rows));
        group.bench_function("ffi_chunked_drain_100x1000", |b| {
            b.iter(|| {
                let mut n = 0u64;
                for p in &prefixes {
                    n += drain_prefix(&d, p, &mut buf).0;
                }
                assert_eq!(n, total_rows);
            })
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("ffi_vectorized/iter_open_batch");
        group.sample_size(10);
        group.measurement_time(std::time::Duration::from_secs(3));
        group.throughput(Throughput::Elements(OPEN_BATCH_K as u64));

        // K=64 probes over the first 64 prefixes; per-probe 64 KiB chunk.
        let probe_cols = cols_from(prefixes.iter().take(OPEN_BATCH_K).map(|p| p.as_slice()));
        let offs_u32: Vec<u32> = probe_cols.offs.iter().map(|&o| o as u32).collect();
        let mut bufs: Vec<Vec<u8>> = (0..OPEN_BATCH_K)
            .map(|_| vec![0u8; CHUNK_CAP as usize])
            .collect();

        group.bench_function("ffi_open_batch_parallel_k64", |b| {
            b.iter(|| {
                let mut handles = vec![0u64; OPEN_BATCH_K];
                let mut chunks: Vec<FrsChunk> = bufs
                    .iter_mut()
                    .map(|bb| FrsChunk {
                        buf_ptr: bb.as_mut_ptr(),
                        buf_cap: CHUNK_CAP,
                        row_count: 0,
                        bytes_used: 0,
                        _reserved: 0,
                    })
                    .collect();
                let rc = unsafe {
                    frs_vec_iter_prefix_open_batch_parallel(
                        d.db,
                        d.cf,
                        offs_u32.as_ptr(),
                        probe_cols.data.as_ptr(),
                        OPEN_BATCH_K as u32,
                        handles.as_mut_ptr(),
                        chunks.as_mut_ptr(),
                        CHUNK_CAP,
                    )
                };
                assert_eq!(rc, 0, "open_batch_parallel failed: {rc}");
                // Close every still-registered handle (EOF-auto-closed
                // probes have handle slots that are no-ops to close).
                for (i, &h) in handles.iter().enumerate() {
                    assert!(chunks[i].row_count > 0, "probe {i} returned no rows");
                    if h != 0 && chunks[i]._reserved & FRS_CHUNK_EOF == 0 {
                        frs_vec_iter_prefix_close(h);
                    }
                }
            })
        });
        group.finish();
    }
}

// ---------------------------------------------------------------------------
// q7/q19 probes: many tiny prefix iterators + merge append chains
// ---------------------------------------------------------------------------

fn bench_q7_iter_probe_ffi(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/q7_iter_probe_ffi");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_millis(800));
    group.warm_up_time(std::time::Duration::from_millis(200));

    for &prefix_count in Q7_PREFIX_COUNTS {
        for &rows_per_prefix in Q7_ROWS_PER_PREFIX {
            let d = FfiDb::open();
            let ns = format!("q7/p{prefix_count}/r{rows_per_prefix}");
            let (prefixes, expected) =
                populate_iter_fixture(&d, &ns, prefix_count, rows_per_prefix);
            assert_iter_fixture_correct(&d, &prefixes, &expected);
            let expected_rows = (prefix_count * rows_per_prefix) as u64;
            let label = format!("p{prefix_count}_r{rows_per_prefix}");

            group.throughput(Throughput::Elements(prefix_count as u64));
            let mut serial_buf = vec![0u8; CHUNK_CAP as usize];
            group.bench_with_input(
                BenchmarkId::new("serial_open_next_close", &label),
                &prefix_count,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_serial_count(&d, &prefixes, &mut serial_buf);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("batch_open_parallel", &label),
                &prefix_count,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_parallel_count(&d, &prefixes);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
        }
    }
    group.finish();
}

fn bench_q19_iter_topn_ffi(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/q19_iter_topn_ffi");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_millis(800));
    group.warm_up_time(std::time::Duration::from_millis(200));

    for &batch_k in Q19_BATCH_OPEN_K {
        for &rows_per_prefix in Q19_ROWS_PER_PREFIX {
            let d = FfiDb::open();
            let ns = format!("q19/topn/k{batch_k}/r{rows_per_prefix}");
            let (prefixes, expected) = populate_iter_fixture(&d, &ns, batch_k, rows_per_prefix);
            assert_iter_fixture_correct(&d, &prefixes, &expected);
            let expected_rows = (batch_k * rows_per_prefix) as u64;
            let label = format!("k{batch_k}_r{rows_per_prefix}");

            group.throughput(Throughput::Elements(batch_k as u64));
            let mut serial_buf = vec![0u8; CHUNK_CAP as usize];
            group.bench_with_input(
                BenchmarkId::new("serial_open_next_close", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_serial_count(&d, &prefixes, &mut serial_buf);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("batch_open_parallel", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_parallel_count(&d, &prefixes);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
        }
    }
    group.finish();
}

fn drain_prefix_compat_row_by_row(d: &FfiDb, prefix: &[u8], copy_payloads: bool) -> u64 {
    let mut iter: FrsIterator = std::ptr::null_mut();
    let rc =
        unsafe { frs_prefix_lookup_open(d.db, d.cf, prefix.as_ptr(), prefix.len(), &mut iter) };
    assert_eq!(rc, FRS_STATUS_OK, "frs_prefix_lookup_open failed: {rc}");
    let mut rows = 0u64;
    loop {
        let mut key = FrsBytes {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut value = FrsBytes {
            data: std::ptr::null_mut(),
            len: 0,
            capacity: 0,
        };
        let mut valid = false;
        let rc = unsafe { frs_iterator_next(iter, &mut key, &mut value, &mut valid) };
        assert_eq!(rc, FRS_STATUS_OK, "frs_iterator_next failed: {rc}");
        if !valid {
            unsafe {
                let _ = frs_bytes_free(&mut key);
                let _ = frs_bytes_free(&mut value);
            }
            break;
        }
        if copy_payloads {
            let key_slice = unsafe { std::slice::from_raw_parts(key.data, key.len) };
            let value_slice = unsafe { std::slice::from_raw_parts(value.data, value.len) };
            std::hint::black_box((key_slice.to_vec(), value_slice.to_vec()));
        }
        unsafe {
            let _ = frs_bytes_free(&mut key);
            let _ = frs_bytes_free(&mut value);
        }
        rows += 1;
    }
    let rc = unsafe { frs_iterator_close(iter) };
    assert_eq!(rc, FRS_STATUS_OK, "frs_iterator_close failed: {rc}");
    rows
}

fn drain_prefixes_compat_row_by_row_count(
    d: &FfiDb,
    prefixes: &[Vec<u8>],
    copy_payloads: bool,
) -> u64 {
    prefixes
        .iter()
        .map(|prefix| drain_prefix_compat_row_by_row(d, prefix, copy_payloads))
        .sum()
}

struct CompatNextChunkBuffers {
    key_offsets: Vec<i32>,
    value_offsets: Vec<i32>,
    key_data: Vec<u8>,
    value_data: Vec<u8>,
    value_validity: Vec<u8>,
}

impl CompatNextChunkBuffers {
    fn new() -> Self {
        Self {
            key_offsets: vec![0_i32; COMPAT_PROXY_NEXT_CHUNK_ROWS + 1],
            value_offsets: vec![0_i32; COMPAT_PROXY_NEXT_CHUNK_ROWS + 1],
            key_data: vec![0_u8; COMPAT_PROXY_NEXT_CHUNK_DATA_CAP],
            value_data: vec![0_u8; COMPAT_PROXY_NEXT_CHUNK_DATA_CAP],
            value_validity: vec![0_u8; COMPAT_PROXY_NEXT_CHUNK_ROWS],
        }
    }
}

fn drain_prefix_compat_next_chunk_count(
    d: &FfiDb,
    prefix: &[u8],
    buffers: &mut CompatNextChunkBuffers,
) -> u64 {
    let mut iter: FrsIterator = std::ptr::null_mut();
    let rc =
        unsafe { frs_prefix_lookup_open(d.db, d.cf, prefix.as_ptr(), prefix.len(), &mut iter) };
    assert_eq!(rc, FRS_STATUS_OK, "frs_prefix_lookup_open failed: {rc}");

    let mut rows = 0_u64;

    loop {
        let mut count = 0_u32;
        let mut eof = false;
        let rc = unsafe {
            frs_iterator_next_chunk(
                iter,
                COMPAT_PROXY_NEXT_CHUNK_ROWS as u32,
                buffers.key_offsets.as_mut_ptr(),
                buffers.key_data.as_mut_ptr(),
                buffers.key_data.len(),
                buffers.value_offsets.as_mut_ptr(),
                buffers.value_data.as_mut_ptr(),
                buffers.value_data.len(),
                buffers.value_validity.as_mut_ptr(),
                &mut count,
                &mut eof,
            )
        };
        assert_eq!(rc, FRS_STATUS_OK, "frs_iterator_next_chunk failed: {rc}");
        rows += count as u64;
        std::hint::black_box((
            &buffers.key_offsets,
            &buffers.key_data,
            &buffers.value_offsets,
            &buffers.value_data,
        ));
        if eof {
            break;
        }
        assert_ne!(count, 0, "next_chunk made no progress before eof");
    }

    let rc = unsafe { frs_iterator_close(iter) };
    assert_eq!(rc, FRS_STATUS_OK, "frs_iterator_close failed: {rc}");
    rows
}

fn drain_prefixes_compat_next_chunk_count(d: &FfiDb, prefixes: &[Vec<u8>]) -> u64 {
    let mut buffers = CompatNextChunkBuffers::new();
    prefixes
        .iter()
        .map(|prefix| drain_prefix_compat_next_chunk_count(d, prefix, &mut buffers))
        .sum()
}

fn bench_compat_jni_prefix_proxy(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/compat_jni_prefix_proxy");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_millis(800));
    group.warm_up_time(std::time::Duration::from_millis(200));

    for &prefix_count in COMPAT_PROXY_PREFIX_COUNTS {
        for &rows_per_prefix in COMPAT_PROXY_ROWS_PER_PREFIX {
            let d = FfiDb::open();
            let ns = format!("compat/p{prefix_count}/r{rows_per_prefix}");
            let (prefixes, expected) =
                populate_iter_fixture(&d, &ns, prefix_count, rows_per_prefix);
            assert_iter_fixture_correct(&d, &prefixes, &expected);
            let expected_rows = (prefix_count * rows_per_prefix) as u64;
            let label = format!("p{prefix_count}_r{rows_per_prefix}");

            group.throughput(Throughput::Elements(expected_rows));
            group.bench_with_input(
                BenchmarkId::new("compat_row_native", &label),
                &expected_rows,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_compat_row_by_row_count(&d, &prefixes, false);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("compat_row_copy_proxy", &label),
                &expected_rows,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_compat_row_by_row_count(&d, &prefixes, true);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );

            group.bench_with_input(
                BenchmarkId::new("compat_next_chunk_offsets", &label),
                &expected_rows,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_compat_next_chunk_count(&d, &prefixes);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );

            let mut chunk_buf = vec![0u8; CHUNK_CAP as usize];
            group.bench_with_input(
                BenchmarkId::new("chunked_prefix_lower_bound", &label),
                &expected_rows,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_serial_count(&d, &prefixes, &mut chunk_buf);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
        }
    }
    group.finish();
}

fn bench_q19_append_merge_chain_ffi(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/q19_append_merge_chain_ffi");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_millis(800));
    group.warm_up_time(std::time::Duration::from_millis(200));

    for &chain_len in Q19_CHAIN_LENGTHS {
        for &(shape_name, shape) in &[
            ("distinct_keys", MergeShape::DistinctKeys),
            ("same_key_chain", MergeShape::SameKeyChain),
        ] {
            let d = FfiDb::open();
            let fixture = build_merge_batch_fixture(shape, chain_len, 0);
            run_merge_append_batch(&d, &fixture);
            assert_merge_fixture_readable(&d, &fixture);

            let rows = fixture.rows;
            let label = format!("{shape_name}_chain{chain_len}");
            let salt = AtomicU64::new(1);
            group.throughput(Throughput::Elements(rows as u64));
            group.bench_with_input(
                BenchmarkId::new("merge_append_batch", &label),
                &rows,
                |b, _| {
                    b.iter_batched(
                        || {
                            let next_salt = salt.fetch_add(1, Ordering::Relaxed) as usize;
                            build_merge_batch_fixture(shape, chain_len, next_salt)
                        },
                        |fixture| {
                            run_merge_append_batch(&d, &fixture);
                            std::hint::black_box(fixture.rows);
                        },
                        BatchSize::SmallInput,
                    )
                },
            );
        }
    }
    group.finish();
}

fn bench_q19_merge_chain_read_lifecycle_ffi(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/q19_merge_chain_read_lifecycle_ffi");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_millis(800));
    group.warm_up_time(std::time::Duration::from_millis(200));

    let mut salt = 10_000usize;
    for &chain_len in Q19_CHAIN_LENGTHS {
        for &(shape_name, shape) in &[
            ("distinct_keys", MergeShape::DistinctKeys),
            ("same_key_chain", MergeShape::SameKeyChain),
        ] {
            for &stage in &[
                MergeReadStage::Memtable,
                MergeReadStage::Flushed,
                MergeReadStage::Compacted,
            ] {
                salt += 1;
                let (d, mut read_fixture) = prepare_merge_read_db(shape, chain_len, salt, stage);
                let returned_keys = read_fixture.expected_values.len();
                let label = format!("{shape_name}_chain{chain_len}");

                group.throughput(Throughput::Elements(returned_keys as u64));
                group.bench_with_input(
                    BenchmarkId::new(stage.label(), &label),
                    &returned_keys,
                    |b, _| {
                        b.iter(|| {
                            let bytes = run_merge_read_get(&d, &mut read_fixture);
                            std::hint::black_box(bytes);
                        })
                    },
                );

                let expected_values = read_fixture.expected_values.clone();
                let expected_bytes: usize = expected_values.iter().map(|v| v.len()).sum();
                let mut copy_offsets = vec![0i32; expected_values.len() + 1];
                let mut copy_data = vec![0u8; expected_bytes + expected_values.len() * 8 + 1];
                let mut copy_validity = vec![0u8; expected_values.len()];
                group.bench_with_input(
                    BenchmarkId::new(format!("{}_output_copy_only", stage.label()), &label),
                    &returned_keys,
                    |b, _| {
                        b.iter(|| {
                            let bytes = copy_expected_values_to_vec_get_buffers(
                                &expected_values,
                                &mut copy_offsets,
                                &mut copy_data,
                                &mut copy_validity,
                            );
                            assert_eq!(bytes, expected_bytes);
                            std::hint::black_box(bytes);
                        })
                    },
                );
            }
        }
    }
    group.finish();
}

fn bench_q19_iter_open_alloc_split_ffi(c: &mut Criterion) {
    let mut group = c.benchmark_group("ffi_vectorized/q19_iter_open_alloc_split_ffi");
    group.sample_size(10);
    group.measurement_time(std::time::Duration::from_millis(800));
    group.warm_up_time(std::time::Duration::from_millis(200));

    for &batch_k in Q19_BATCH_OPEN_K {
        for &rows_per_prefix in Q19_ALLOC_SPLIT_ROWS_PER_PREFIX {
            let d = FfiDb::open();
            let ns = format!("q19/allocsplit/k{batch_k}/r{rows_per_prefix}");
            let (prefixes, expected) = populate_iter_fixture(&d, &ns, batch_k, rows_per_prefix);
            assert_iter_fixture_correct(&d, &prefixes, &expected);
            let expected_rows = (batch_k * rows_per_prefix) as u64;
            let label = format!("k{batch_k}_r{rows_per_prefix}");

            let prefix_cols = cols_from(prefixes.iter().map(|p| p.as_slice()));
            let offs_u32: Vec<u32> = prefix_cols.offs.iter().map(|&o| o as u32).collect();
            let mut serial_buf = vec![0u8; CHUNK_CAP as usize];
            let mut serial_batch_reuse_fixture = BatchOpenOnlyFixture::new(&prefixes, CHUNK_CAP);
            let mut parallel_batch_reuse_fixture = BatchOpenOnlyFixture::new(&prefixes, CHUNK_CAP);

            group.throughput(Throughput::Elements(batch_k as u64));
            group.bench_with_input(
                BenchmarkId::new("serial_reuse_buf", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = drain_prefixes_serial_count(&d, &prefixes, &mut serial_buf);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("serial_batch_alloc_each_iter", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = batch_open_count_alloc_each_iter_with(
                            &d,
                            &prefix_cols,
                            &offs_u32,
                            frs_vec_iter_prefix_open_batch,
                            "open_batch_serial",
                        );
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("serial_batch_reuse_caller_buffers", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = serial_batch_reuse_fixture.open_count_close_with(
                            &d,
                            frs_vec_iter_prefix_open_batch,
                            "open_batch_serial",
                        );
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("parallel_batch_alloc_each_iter", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = batch_open_count_alloc_each_iter_with(
                            &d,
                            &prefix_cols,
                            &offs_u32,
                            frs_vec_iter_prefix_open_batch_parallel,
                            "open_batch_parallel",
                        );
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
            group.bench_with_input(
                BenchmarkId::new("parallel_batch_reuse_caller_buffers", &label),
                &batch_k,
                |b, _| {
                    b.iter(|| {
                        let rows = parallel_batch_reuse_fixture.open_count_close(&d);
                        assert_eq!(rows, expected_rows);
                        std::hint::black_box(rows);
                    })
                },
            );
        }
    }
    group.finish();
}

// ---------------------------------------------------------------------------
// Boundary-tax summary (the §B1 headline derived number)
// ---------------------------------------------------------------------------

fn median_ns_per_row(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples[samples.len() / 2]
}

fn boundary_tax_summary() {
    println!();
    println!("=== B1 boundary-tax summary (median-of-30, same-process back-to-back) ===");
    println!("tax ns/row = FFI ns/row - engine-direct ns/row, identical batches");
    println!(
        "{:<10} {:>6} {:>6} {:>14} {:>14} {:>12}",
        "group", "rows", "vsize", "ffi ns/row", "engine ns/row", "tax ns/row"
    );

    // put
    for &rows in ROW_COUNTS {
        for &vsize in VALUE_SIZES {
            let key_vecs = make_keys(rows, "tax");
            let val_vecs: Vec<Vec<u8>> = (0..rows).map(|i| make_value(vsize, i)).collect();
            let keys = cols_from(key_vecs.iter().map(|k| k.as_slice()));
            let vals = cols_from(val_vecs.iter().map(|v| v.as_slice()));

            let d = FfiDb::open();
            let mut ffi_samples = Vec::with_capacity(30);
            for rep in 0..35 {
                let t = Instant::now();
                ffi_put_once(&d, &keys, &vals, rows);
                if rep >= 5 {
                    ffi_samples.push(t.elapsed().as_nanos() as f64 / rows as f64);
                }
            }
            drop(d);

            let e = EngineDb::open();
            let key_slices: Vec<&[u8]> = key_vecs.iter().map(|k| k.as_slice()).collect();
            let val_slices: Vec<Option<&[u8]>> =
                val_vecs.iter().map(|v| Some(v.as_slice())).collect();
            let op_types = vec![1u8; rows];
            let mut eng_samples = Vec::with_capacity(30);
            for rep in 0..35 {
                let t = Instant::now();
                engine_put_once(&e.db, &e.cf, &key_slices, &val_slices, &op_types);
                if rep >= 5 {
                    eng_samples.push(t.elapsed().as_nanos() as f64 / rows as f64);
                }
            }

            let f = median_ns_per_row(ffi_samples);
            let g = median_ns_per_row(eng_samples);
            println!(
                "{:<10} {:>6} {:>6} {:>14.1} {:>14.1} {:>12.1}",
                "put",
                rows,
                vsize,
                f,
                g,
                f - g
            );
        }
    }

    // get warm (vsize fixed at 64 B)
    for &rows in ROW_COUNTS {
        const VSIZE: usize = 64;
        let d = FfiDb::open();
        {
            let eng = d.engine().clone();
            let cfh = d.engine_cf();
            populate_get_pool(
                &mut |k, v| {
                    eng.put(&cfh, k, v).expect("put");
                },
                VSIZE,
            );
        }
        let mut f = get_fixture(rows, VSIZE);
        let mut ffi_samples = Vec::with_capacity(30);
        for rep in 0..35 {
            let t = Instant::now();
            ffi_get_once(&d, &mut f, rows);
            if rep >= 5 {
                ffi_samples.push(t.elapsed().as_nanos() as f64 / rows as f64);
            }
        }
        drop(d);

        let e = EngineDb::open();
        populate_get_pool(
            &mut |k, v| {
                e.db.put(&e.cf, k, v).expect("put");
            },
            VSIZE,
        );
        let key_slices: Vec<&[u8]> = f.key_slices_owned.iter().map(|k| k.as_slice()).collect();
        let mut eng_samples = Vec::with_capacity(30);
        for rep in 0..35 {
            let t = Instant::now();
            let res = e.db.batch_get(&e.cf, &key_slices).expect("batch_get");
            assert!(res.iter().all(|r| r.is_some()));
            if rep >= 5 {
                eng_samples.push(t.elapsed().as_nanos() as f64 / rows as f64);
            }
        }

        let fv = median_ns_per_row(ffi_samples);
        let gv = median_ns_per_row(eng_samples);
        println!(
            "{:<10} {:>6} {:>6} {:>14.1} {:>14.1} {:>12.1}",
            "get_warm",
            rows,
            VSIZE,
            fv,
            gv,
            fv - gv
        );
    }
    println!("==========================================================================");
}

// ---------------------------------------------------------------------------
// B4: RSS capture (macOS — no jemalloc; see design §B4)
// ---------------------------------------------------------------------------

struct RssSampler {
    stop: Arc<AtomicBool>,
    peak_kb: Arc<AtomicU64>,
    join: Option<std::thread::JoinHandle<()>>,
}

fn rss_kb() -> u64 {
    let pid = std::process::id().to_string();
    std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &pid])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

impl RssSampler {
    fn start() -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let peak_kb = Arc::new(AtomicU64::new(0));
        let s2 = stop.clone();
        let p2 = peak_kb.clone();
        let join = std::thread::spawn(move || {
            while !s2.load(Ordering::Relaxed) {
                let kb = rss_kb();
                p2.fetch_max(kb, Ordering::Relaxed);
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
        RssSampler {
            stop,
            peak_kb,
            join: Some(join),
        }
    }

    fn report(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
        println!(
            "[B4] RSS peak={} MiB end={} MiB (1 Hz sampler)",
            self.peak_kb.load(Ordering::Relaxed) / 1024,
            rss_kb() / 1024
        );
    }
}

fn should_run_boundary_tax_summary<I, S>(args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: Into<OsString>,
{
    match std::env::var("FRS_FFI_BOUNDARY_SUMMARY").ok().as_deref() {
        Some("1") | Some("true") | Some("TRUE") => return true,
        Some("0") | Some("false") | Some("FALSE") => return false,
        _ => {}
    }

    let args: Vec<OsString> = args.into_iter().map(Into::into).collect();
    let user_args = args.get(1..).unwrap_or(&[]);
    if user_args.iter().any(|a| {
        let s = a.to_string_lossy();
        matches!(s.as_ref(), "--test" | "--list" | "--help" | "-h")
    }) {
        return false;
    }

    !user_args.iter().any(|a| {
        let s = a.to_string_lossy();
        !s.starts_with('-')
    })
}

criterion_group!(
    benches,
    bench_put,
    bench_mixed,
    bench_get,
    bench_compat_jni_multiget_proxy,
    bench_iter,
    bench_q7_iter_probe_ffi,
    bench_q19_iter_topn_ffi,
    bench_compat_jni_prefix_proxy,
    bench_q19_append_merge_chain_ffi,
    bench_q19_merge_chain_read_lifecycle_ffi,
    bench_q19_iter_open_alloc_split_ffi
);

fn main() {
    println!(
        "[B4 capture rule] Mac numbers are system-allocator numbers; absolute \
         alloc/resident values do not transfer to the Linux deployment — use Mac \
         runs for regressions (same-box A/B), Linux runs for absolute footprints. \
         (Linux: additionally set FRS_MEM_DIAG=1 for jemalloc stats.)"
    );
    let sampler = RssSampler::start();
    benches();
    criterion::Criterion::default()
        .configure_from_args()
        .final_summary();
    if should_run_boundary_tax_summary(std::env::args_os()) {
        boundary_tax_summary();
    } else {
        println!(
            "[B1 boundary-tax summary] skipped for filtered/test run; set \
             FRS_FFI_BOUNDARY_SUMMARY=1 to force it."
        );
    }
    sampler.report();
}

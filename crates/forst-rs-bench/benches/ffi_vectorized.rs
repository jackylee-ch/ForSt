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
//!
//! After the criterion groups, `boundary_tax_summary()` prints the derived
//! headline number per size: tax ns/row = (FFI ns/row) − (engine ns/row),
//! measured back-to-back (median-of-30) in the same process.
//!
//! B4 capture (design §B4): macOS has NO jemalloc (compile-gated off,
//! recorded TSD SIGSEGV) — a 1 Hz RSS sampler reports peak/end RSS instead.
//! Linux runs can additionally set `FRS_MEM_DIAG=1` for jemalloc stats.

use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use criterion::{criterion_group, BenchmarkId, Criterion, Throughput};

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl, DEFAULT_CF_NAME};
use forst_rs_ffi::{
    frs_db_create_cf_with_merge, frs_db_open, frs_vec_iter_prefix_close, frs_vec_iter_prefix_next,
    frs_vec_iter_prefix_open, frs_vec_iter_prefix_open_batch_parallel, frs_vectorized_batch_get,
    frs_vectorized_batch_mixed, frs_vectorized_batch_put, FrsCfHandle, FrsChunk, FrsDb,
    FRS_CHUNK_EOF, FRS_STATUS_OK,
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
                |b, _| b.iter(|| engine_put_once(&e.db, &e.cf, &key_slices, &val_slices, &op_types)),
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

criterion_group!(benches, bench_put, bench_mixed, bench_get, bench_iter);

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
    boundary_tax_summary();
    sampler.report();
}

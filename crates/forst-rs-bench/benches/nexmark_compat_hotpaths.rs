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

//! Nexmark-shaped FFI hot-path benchmarks for the ForStBackend compat layer.
//!
//! These benches intentionally use the public C ABI entry points instead of
//! the Rust engine API. The goal is to keep a small, local signal for the same
//! interface classes exercised by the Java ForStBackend JNI adapter:
//! buffered state writes, join-state multi-get probes, and prefix-bounded
//! MapState/window scans.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_ffi::{
    frs_batch_get, frs_batch_put, frs_bytes_free, frs_cf_close, frs_db_close, frs_db_default_cf,
    frs_db_open_memory, frs_flush, frs_get, frs_iterator_close, frs_iterator_next,
    frs_iterator_open, frs_iterator_seek, frs_prefix_lookup_close, frs_prefix_lookup_next,
    frs_prefix_lookup_open, frs_put, FrsBytes, FrsCfHandle, FrsDb, FRS_STATUS_OK,
};
use std::ptr;
use std::slice;
use std::time::Duration;

const BATCH_SIZE: usize = 1_024;
const JOIN_KEY_COUNT: usize = 4_096;
const PREFIX_WINDOW_COUNT: usize = 64;
const PREFIX_ROWS_PER_WINDOW: usize = 64;

struct FfiDb {
    db: FrsDb,
    cf: FrsCfHandle,
}

impl FfiDb {
    fn open() -> Self {
        let mut db = ptr::null_mut();
        assert_ok(
            unsafe { frs_db_open_memory(&mut db) },
            "open in-memory FFI db",
        );
        let mut cf = ptr::null_mut();
        assert_ok(
            unsafe { frs_db_default_cf(db, &mut cf) },
            "open default column family",
        );
        Self { db, cf }
    }
}

impl Drop for FfiDb {
    fn drop(&mut self) {
        assert_ok(unsafe { frs_cf_close(self.cf) }, "close column family");
        assert_ok(unsafe { frs_db_close(self.db) }, "close FFI db");
    }
}

struct ByteBatch {
    ptrs: Vec<*const u8>,
    lens: Vec<usize>,
}

impl ByteBatch {
    fn from_bytes(bytes: &[Vec<u8>]) -> Self {
        Self {
            ptrs: bytes.iter().map(|b| b.as_ptr()).collect(),
            lens: bytes.iter().map(|b| b.len()).collect(),
        }
    }
}

fn assert_ok(status: i32, context: &str) {
    assert_eq!(status, FRS_STATUS_OK, "{context}");
}

fn make_join_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("{prefix}/auction={:08}/bidder={:08}", i, i % 997).into_bytes())
        .collect()
}

fn make_values(count: usize, value_len: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let mut value = vec![0_u8; value_len];
            value[..8].copy_from_slice(&(i as u64).to_le_bytes());
            value
        })
        .collect()
}

fn make_prefix_rows() -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = Vec::with_capacity(PREFIX_WINDOW_COUNT * PREFIX_ROWS_PER_WINDOW);
    for window in 0..PREFIX_WINDOW_COUNT {
        for seq in 0..PREFIX_ROWS_PER_WINDOW {
            let key = format!("window/{window:04}/auction/{seq:04}").into_bytes();
            let value = format!("bid={seq:08};price={}", window * 1_000 + seq).into_bytes();
            rows.push((key, value));
        }
    }
    rows
}

fn put_one(db: FrsDb, cf: FrsCfHandle, key: &[u8], value: &[u8]) {
    assert_ok(
        unsafe { frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()) },
        "frs_put",
    );
}

fn preload(db: FrsDb, cf: FrsCfHandle, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    let key_batch = ByteBatch::from_bytes(keys);
    let value_batch = ByteBatch::from_bytes(values);
    batch_put(db, cf, &key_batch, &value_batch);
}

fn batch_put(db: FrsDb, cf: FrsCfHandle, keys: &ByteBatch, values: &ByteBatch) {
    assert_eq!(keys.ptrs.len(), values.ptrs.len(), "batch length mismatch");
    assert_ok(
        unsafe {
            frs_batch_put(
                db,
                cf,
                keys.ptrs.as_ptr(),
                keys.lens.as_ptr(),
                values.ptrs.as_ptr(),
                values.lens.as_ptr(),
                keys.ptrs.len(),
            )
        },
        "frs_batch_put",
    );
}

fn get_one(db: FrsDb, cf: FrsCfHandle, key: &[u8]) -> usize {
    let mut out = FrsBytes::default();
    assert_ok(
        unsafe { frs_get(db, cf, key.as_ptr(), key.len(), &mut out) },
        "frs_get",
    );
    let len = out.len;
    assert_ok(unsafe { frs_bytes_free(&mut out) }, "frs_bytes_free(get)");
    len
}

fn batch_get(db: FrsDb, cf: FrsCfHandle, keys: &ByteBatch) -> usize {
    let mut out_values: Vec<FrsBytes> = (0..keys.ptrs.len()).map(|_| FrsBytes::default()).collect();
    assert_ok(
        unsafe {
            frs_batch_get(
                db,
                cf,
                keys.ptrs.as_ptr(),
                keys.lens.as_ptr(),
                keys.ptrs.len(),
                out_values.as_mut_ptr(),
            )
        },
        "frs_batch_get",
    );
    let mut total_len = 0usize;
    for value in &mut out_values {
        total_len += value.len;
        assert_ok(
            unsafe { frs_bytes_free(value) },
            "frs_bytes_free(batch_get)",
        );
    }
    total_len
}

fn scan_prefix_via_full_iterator(db: FrsDb, cf: FrsCfHandle, prefix: &[u8]) -> usize {
    let mut iter = ptr::null_mut();
    assert_ok(
        unsafe { frs_iterator_open(db, cf, &mut iter) },
        "frs_iterator_open",
    );
    assert_ok(
        unsafe { frs_iterator_seek(iter, prefix.as_ptr(), prefix.len()) },
        "frs_iterator_seek",
    );

    let mut total_len = 0usize;
    loop {
        let mut key = FrsBytes::default();
        let mut value = FrsBytes::default();
        let mut valid = false;
        assert_ok(
            unsafe { frs_iterator_next(iter, &mut key, &mut value, &mut valid) },
            "frs_iterator_next",
        );
        if !valid {
            assert_ok(unsafe { frs_bytes_free(&mut key) }, "free iterator key");
            assert_ok(unsafe { frs_bytes_free(&mut value) }, "free iterator value");
            break;
        }
        let key_matches =
            unsafe { slice::from_raw_parts(key.data as *const u8, key.len) }.starts_with(prefix);
        if key_matches {
            total_len += value.len;
        }
        assert_ok(unsafe { frs_bytes_free(&mut key) }, "free iterator key");
        assert_ok(unsafe { frs_bytes_free(&mut value) }, "free iterator value");
        if !key_matches {
            break;
        }
    }
    assert_ok(unsafe { frs_iterator_close(iter) }, "frs_iterator_close");
    total_len
}

fn scan_prefix_via_prefix_iterator(db: FrsDb, cf: FrsCfHandle, prefix: &[u8]) -> usize {
    let mut iter = ptr::null_mut();
    assert_ok(
        unsafe { frs_prefix_lookup_open(db, cf, prefix.as_ptr(), prefix.len(), &mut iter) },
        "frs_prefix_lookup_open",
    );

    let mut total_len = 0usize;
    loop {
        let mut key = FrsBytes::default();
        let mut value = FrsBytes::default();
        let mut valid = false;
        assert_ok(
            unsafe { frs_prefix_lookup_next(iter, &mut key, &mut value, &mut valid) },
            "frs_prefix_lookup_next",
        );
        if !valid {
            assert_ok(unsafe { frs_bytes_free(&mut key) }, "free prefix key");
            assert_ok(unsafe { frs_bytes_free(&mut value) }, "free prefix value");
            break;
        }
        total_len += value.len;
        assert_ok(unsafe { frs_bytes_free(&mut key) }, "free prefix key");
        assert_ok(unsafe { frs_bytes_free(&mut value) }, "free prefix value");
    }
    assert_ok(
        unsafe { frs_prefix_lookup_close(iter) },
        "frs_prefix_lookup_close",
    );
    total_len
}

fn bench_write_batch_window_update(c: &mut Criterion) {
    let keys = make_join_keys("window-update", BATCH_SIZE);
    let values = make_values(BATCH_SIZE, 64);
    let key_batch = ByteBatch::from_bytes(&keys);
    let value_batch = ByteBatch::from_bytes(&values);

    let mut group = c.benchmark_group("nexmark_compat/write_batch_window_update");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    group.bench_function(BenchmarkId::new("per_key_put", BATCH_SIZE), |b| {
        let ffi = FfiDb::open();
        b.iter(|| {
            for (key, value) in keys.iter().zip(&values) {
                put_one(ffi.db, ffi.cf, key, value);
            }
        });
    });
    group.bench_function(BenchmarkId::new("batch_put", BATCH_SIZE), |b| {
        let ffi = FfiDb::open();
        b.iter(|| batch_put(ffi.db, ffi.cf, &key_batch, &value_batch));
    });
    group.finish();
}

fn bench_join_probe_multi_get(c: &mut Criterion) {
    let keys = make_join_keys("join-probe", JOIN_KEY_COUNT);
    let values = make_values(JOIN_KEY_COUNT, 48);
    let probe_keys: Vec<Vec<u8>> = keys.iter().take(BATCH_SIZE).cloned().collect();
    let probe_batch = ByteBatch::from_bytes(&probe_keys);

    let mut group = c.benchmark_group("nexmark_compat/join_probe_multi_get");
    group.throughput(Throughput::Elements(BATCH_SIZE as u64));
    group.bench_function(BenchmarkId::new("memtable_per_key_get", BATCH_SIZE), |b| {
        let ffi = FfiDb::open();
        preload(ffi.db, ffi.cf, &keys, &values);
        b.iter(|| {
            let total_len: usize = probe_keys
                .iter()
                .map(|key| get_one(ffi.db, ffi.cf, key))
                .sum();
            black_box(total_len);
        });
    });
    group.bench_function(BenchmarkId::new("memtable_batch_get", BATCH_SIZE), |b| {
        let ffi = FfiDb::open();
        preload(ffi.db, ffi.cf, &keys, &values);
        b.iter(|| black_box(batch_get(ffi.db, ffi.cf, &probe_batch)));
    });
    group.bench_function(BenchmarkId::new("flushed_per_key_get", BATCH_SIZE), |b| {
        let ffi = FfiDb::open();
        preload(ffi.db, ffi.cf, &keys, &values);
        assert_ok(unsafe { frs_flush(ffi.db) }, "frs_flush");
        b.iter(|| {
            let total_len: usize = probe_keys
                .iter()
                .map(|key| get_one(ffi.db, ffi.cf, key))
                .sum();
            black_box(total_len);
        });
    });
    group.bench_function(BenchmarkId::new("flushed_batch_get", BATCH_SIZE), |b| {
        let ffi = FfiDb::open();
        preload(ffi.db, ffi.cf, &keys, &values);
        assert_ok(unsafe { frs_flush(ffi.db) }, "frs_flush");
        b.iter(|| black_box(batch_get(ffi.db, ffi.cf, &probe_batch)));
    });
    group.finish();
}

fn bench_prefix_window_scan(c: &mut Criterion) {
    let rows = make_prefix_rows();
    let prefix = b"window/0017/";

    let mut group = c.benchmark_group("nexmark_compat/prefix_window_scan");
    group.throughput(Throughput::Elements(PREFIX_ROWS_PER_WINDOW as u64));
    group.bench_function(
        BenchmarkId::new("full_iterator_seek_then_next", PREFIX_ROWS_PER_WINDOW),
        |b| {
            let ffi = FfiDb::open();
            for (key, value) in &rows {
                put_one(ffi.db, ffi.cf, key, value);
            }
            b.iter(|| black_box(scan_prefix_via_full_iterator(ffi.db, ffi.cf, prefix)));
        },
    );
    group.bench_function(
        BenchmarkId::new("prefix_lookup_iterator", PREFIX_ROWS_PER_WINDOW),
        |b| {
            let ffi = FfiDb::open();
            for (key, value) in &rows {
                put_one(ffi.db, ffi.cf, key, value);
            }
            b.iter(|| black_box(scan_prefix_via_prefix_iterator(ffi.db, ffi.cf, prefix)));
        },
    );
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(300));
    targets = bench_write_batch_window_update, bench_join_probe_multi_get, bench_prefix_window_scan
}
criterion_main!(benches);

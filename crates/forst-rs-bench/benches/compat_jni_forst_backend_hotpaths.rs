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

//! ForStBackend-shaped compat-JNI hot-path benchmarks.
//!
//! This is intentionally a small local guardrail before the full Nexmark
//! container run. It drives the same engine-facing C ABI that the compat-JNI
//! layer resolves to after translating ForStBackend calls: point writes,
//! WriteBatch-like batched writes, multiGet join probes, and read-after-flush /
//! compaction lookups.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_ffi::{
    frs_batch_get, frs_batch_put, frs_bytes_free, frs_cf_close, frs_compact_cf, frs_db_close,
    frs_db_default_cf, frs_db_open_memory, frs_flush, frs_get, frs_put, FrsBytes, FrsCfHandle,
    FrsDb, FRS_STATUS_OK,
};
use std::ptr;
use std::time::Duration;

const WRITE_BATCH_ROWS: usize = 512;
const JOIN_PROBE_ROWS: usize = 1_024;
const JOIN_STATE_ROWS: usize = 8_192;

struct FfiDb {
    db: FrsDb,
    cf: FrsCfHandle,
}

impl FfiDb {
    fn open() -> Self {
        let mut db = ptr::null_mut();
        assert_ok(unsafe { frs_db_open_memory(&mut db) }, "frs_db_open_memory");
        let mut cf = ptr::null_mut();
        assert_ok(
            unsafe { frs_db_default_cf(db, &mut cf) },
            "frs_db_default_cf",
        );
        Self { db, cf }
    }
}

impl Drop for FfiDb {
    fn drop(&mut self) {
        assert_ok(unsafe { frs_cf_close(self.cf) }, "frs_cf_close");
        assert_ok(unsafe { frs_db_close(self.db) }, "frs_db_close");
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
            lens: bytes.iter().map(Vec::len).collect(),
        }
    }
}

fn assert_ok(status: i32, context: &str) {
    assert_eq!(status, FRS_STATUS_OK, "{context}");
}

fn make_keys(prefix: &str, count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| format!("{prefix}/key-group={:04}/key={:08}", i % 128, i).into_bytes())
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

fn put_one(db: FrsDb, cf: FrsCfHandle, key: &[u8], value: &[u8]) {
    assert_ok(
        unsafe { frs_put(db, cf, key.as_ptr(), key.len(), value.as_ptr(), value.len()) },
        "frs_put",
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

fn preload(db: FrsDb, cf: FrsCfHandle, keys: &[Vec<u8>], values: &[Vec<u8>]) {
    let key_batch = ByteBatch::from_bytes(keys);
    let value_batch = ByteBatch::from_bytes(values);
    batch_put(db, cf, &key_batch, &value_batch);
}

fn bench_forst_backend_write_batch_translation(c: &mut Criterion) {
    let keys = make_keys("q4-window-update", WRITE_BATCH_ROWS);
    let values = make_values(WRITE_BATCH_ROWS, 96);
    let key_batch = ByteBatch::from_bytes(&keys);
    let value_batch = ByteBatch::from_bytes(&values);

    let mut group = c.benchmark_group("compat_jni_forst_backend/write_batch_translation");
    group.throughput(Throughput::Elements(WRITE_BATCH_ROWS as u64));
    group.bench_function(BenchmarkId::new("per_entry_put", WRITE_BATCH_ROWS), |b| {
        let ffi = FfiDb::open();
        b.iter(|| {
            for (key, value) in keys.iter().zip(&values) {
                put_one(ffi.db, ffi.cf, black_box(key), black_box(value));
            }
        });
    });
    group.bench_function(BenchmarkId::new("ffi_batch_put", WRITE_BATCH_ROWS), |b| {
        let ffi = FfiDb::open();
        b.iter(|| {
            batch_put(
                ffi.db,
                ffi.cf,
                black_box(&key_batch),
                black_box(&value_batch),
            )
        });
    });
    group.finish();
}

fn bench_forst_backend_join_probe_multiget(c: &mut Criterion) {
    let keys = make_keys("q3-join-state", JOIN_STATE_ROWS);
    let values = make_values(JOIN_STATE_ROWS, 64);
    let probe_keys: Vec<Vec<u8>> = keys.iter().take(JOIN_PROBE_ROWS).cloned().collect();
    let probe_batch = ByteBatch::from_bytes(&probe_keys);

    let mut group = c.benchmark_group("compat_jni_forst_backend/join_probe_multiget");
    group.throughput(Throughput::Elements(JOIN_PROBE_ROWS as u64));
    group.bench_function(BenchmarkId::new("per_key_get", JOIN_PROBE_ROWS), |b| {
        let ffi = FfiDb::open();
        preload(ffi.db, ffi.cf, &keys, &values);
        b.iter(|| {
            let total: usize = probe_keys
                .iter()
                .map(|key| get_one(ffi.db, ffi.cf, black_box(key)))
                .sum();
            black_box(total);
        });
    });
    group.bench_function(BenchmarkId::new("ffi_batch_get", JOIN_PROBE_ROWS), |b| {
        let ffi = FfiDb::open();
        preload(ffi.db, ffi.cf, &keys, &values);
        b.iter(|| black_box(batch_get(ffi.db, ffi.cf, black_box(&probe_batch))));
    });
    group.finish();
}

fn bench_forst_backend_compacted_join_probe(c: &mut Criterion) {
    let keys = make_keys("q11-session-state", JOIN_STATE_ROWS);
    let values = make_values(JOIN_STATE_ROWS, 48);
    let probe_keys: Vec<Vec<u8>> = keys.iter().take(JOIN_PROBE_ROWS).cloned().collect();
    let probe_batch = ByteBatch::from_bytes(&probe_keys);

    let mut group = c.benchmark_group("compat_jni_forst_backend/compacted_join_probe");
    group.throughput(Throughput::Elements(JOIN_PROBE_ROWS as u64));
    group.bench_function(
        BenchmarkId::new("flushed_compacted_per_key_get", JOIN_PROBE_ROWS),
        |b| {
            let ffi = FfiDb::open();
            preload(ffi.db, ffi.cf, &keys, &values);
            assert_ok(unsafe { frs_flush(ffi.db) }, "frs_flush");
            assert_ok(unsafe { frs_compact_cf(ffi.db, ffi.cf) }, "frs_compact_cf");
            b.iter(|| {
                let total: usize = probe_keys
                    .iter()
                    .map(|key| get_one(ffi.db, ffi.cf, black_box(key)))
                    .sum();
                black_box(total);
            });
        },
    );
    group.bench_function(
        BenchmarkId::new("flushed_compacted_batch_get", JOIN_PROBE_ROWS),
        |b| {
            let ffi = FfiDb::open();
            preload(ffi.db, ffi.cf, &keys, &values);
            assert_ok(unsafe { frs_flush(ffi.db) }, "frs_flush");
            assert_ok(unsafe { frs_compact_cf(ffi.db, ffi.cf) }, "frs_compact_cf");
            b.iter(|| black_box(batch_get(ffi.db, ffi.cf, black_box(&probe_batch))));
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
    targets =
        bench_forst_backend_write_batch_translation,
        bench_forst_backend_join_probe_multiget,
        bench_forst_backend_compacted_join_probe
}
criterion_main!(benches);

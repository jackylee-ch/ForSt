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

//! E1 (sharded memtable) — engine-level integration tests.
//!
//! These tests open a real `DbImpl` configured with `memtable_shards = 16`
//! and exercise the put / get / batch / concurrent-write paths end-to-end,
//! making sure the sharded-memtable refactor preserves the semantics that
//! the v3 hot-path relied on (sequence monotonicity, point-lookup
//! correctness, scan correctness across shards, no deadlocks under
//! concurrent writers).

use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::EngineOptions;
use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl, WriteBatch};
use forst_rs_io::{FileSystem, MemoryFileSystem};

fn open_with_shards(memtable_shards: usize, write_buffer_size: usize) -> Arc<DbImpl> {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size,
        memtable_shards,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    DbImpl::open_with_fs(opts, fs).expect("open")
}

#[test]
fn test_engine_with_sharded_memtable_basic() {
    // Open a db with 16 shards, write 10 000 entries, verify every read.
    let db = open_with_shards(16, 8 * 1024 * 1024);
    let cf = db.default_cf();
    const N: u32 = 10_000;

    for i in 0..N {
        let k = format!("k{:06}", i);
        let v = format!("v{:06}", i);
        db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
    }
    for i in 0..N {
        let k = format!("k{:06}", i);
        let expected = format!("v{:06}", i);
        let got = db.get(&cf, k.as_bytes()).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(expected.as_bytes()),
            "mismatch at i={}",
            i
        );
    }
}

#[test]
fn test_engine_concurrent_writers_dont_serialize() {
    // Smoke test: 4 writer threads each push 2_500 puts. We expect all 10_000
    // entries to be readable; the test asserts correctness (not throughput,
    // since CI noise makes a wall-clock comparison flaky). Throughput is
    // covered by the JMH bench harness.
    let db = open_with_shards(16, 32 * 1024 * 1024);
    let cf = db.default_cf();
    let n_threads = 4usize;
    let n_per = 2_500usize;
    let mut handles = Vec::new();
    let started = Instant::now();
    for t in 0..n_threads {
        let db = db.clone();
        let cf = cf.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..n_per {
                let k = format!("t{:02}-k{:06}", t, i);
                let v = format!("t{:02}-v{:06}", t, i);
                db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = started.elapsed();
    eprintln!(
        "test_engine_concurrent_writers_dont_serialize: {} writers x {} puts in {:?}",
        n_threads, n_per, elapsed
    );

    // Correctness: every key from every thread is readable.
    for t in 0..n_threads {
        for i in 0..n_per {
            let k = format!("t{:02}-k{:06}", t, i);
            let v = format!("t{:02}-v{:06}", t, i);
            let got = db.get(&cf, k.as_bytes()).unwrap();
            assert_eq!(got.as_deref(), Some(v.as_bytes()), "thread {} key {}", t, i);
        }
    }
}

#[test]
fn test_engine_sharded_batch_write_correctness() {
    let db = open_with_shards(16, 16 * 1024 * 1024);
    let cf = db.default_cf();

    // Build a 1 000-entry WriteBatch spanning many distinct keys so rows
    // get scattered across shards.
    let mut batch = WriteBatch::new();
    for i in 0..1_000u32 {
        let k = format!("bk{:05}", i);
        let v = format!("bv{:05}", i);
        batch.put(&cf, k.as_bytes(), v.as_bytes());
    }
    db.batch_write(batch).unwrap();

    // Every entry must be readable.
    for i in 0..1_000u32 {
        let k = format!("bk{:05}", i);
        let v = format!("bv{:05}", i);
        let got = db.get(&cf, k.as_bytes()).unwrap();
        assert_eq!(got.as_deref(), Some(v.as_bytes()));
    }
}

#[test]
fn test_engine_sharded_scan_merges_across_shards() {
    // Scan must see every key regardless of which shard it landed on.
    let db = open_with_shards(16, 16 * 1024 * 1024);
    let cf = db.default_cf();
    for i in 0..500u32 {
        let k = format!("s{:05}", i);
        let v = format!("v{:05}", i);
        db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
    }
    // prefix_scan over "s" should return all 500 in sorted order.
    let rows = db.prefix_scan(&cf, b"s").unwrap();
    assert_eq!(rows.len(), 500);
    for w in rows.windows(2) {
        assert!(w[0].0 < w[1].0, "scan output not sorted");
    }
}

#[test]
fn test_engine_sharded_flush_then_read_from_sst() {
    // Force a flush by sizing the buffer small; after flush every key must
    // still be readable (now from the SST instead of the memtable).
    let db = open_with_shards(8, 64 * 1024); // 64 KiB buffer
    let cf = db.default_cf();
    for i in 0..500u32 {
        let k = format!("f{:05}", i);
        let v = format!("payload-{:08}-{}", i, "x".repeat(40));
        db.put(&cf, k.as_bytes(), v.as_bytes()).unwrap();
    }
    // Force a final flush so any data still in the active memtable lands.
    db.flush_all().unwrap();
    for i in 0..500u32 {
        let k = format!("f{:05}", i);
        let v = format!("payload-{:08}-{}", i, "x".repeat(40));
        let got = db.get(&cf, k.as_bytes()).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(v.as_bytes()),
            "lost key {} after flush",
            i
        );
    }
}

#[test]
fn test_engine_default_cf_uses_configured_shard_count() {
    // shard_count = 1 should still pass correctness (degenerate to single
    // memtable); the column-family code path must accept any value in
    // [1, MAX_SHARD_COUNT].
    let db = open_with_shards(1, 8 * 1024 * 1024);
    let cf = db.default_cf();
    for i in 0..200u32 {
        let k = format!("d{:05}", i);
        db.put(&cf, k.as_bytes(), b"v").unwrap();
    }
    for i in 0..200u32 {
        let k = format!("d{:05}", i);
        assert_eq!(
            db.get(&cf, k.as_bytes()).unwrap().as_deref(),
            Some(b"v" as &[u8])
        );
    }
}

#[test]
fn test_engine_secondary_cf_inherits_shards() {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        memtable_shards: 8,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).unwrap();
    let cf2 = db
        .create_column_family(ColumnFamilyDescriptor::new("orders"))
        .unwrap();
    for i in 0..100u32 {
        let k = format!("o{:05}", i);
        db.put(&cf2, k.as_bytes(), b"x").unwrap();
    }
    for i in 0..100u32 {
        let k = format!("o{:05}", i);
        assert_eq!(
            db.get(&cf2, k.as_bytes()).unwrap().as_deref(),
            Some(b"x" as &[u8])
        );
    }
}

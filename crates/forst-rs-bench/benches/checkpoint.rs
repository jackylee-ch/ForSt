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

//! BM-1.4 Checkpoint / Restore timing.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::{FileSystem, MemoryFileSystem};

fn bench_checkpoint_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("checkpoint_create");
    group.sample_size(10);
    for &n in &[1_000u32, 10_000] {
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            write_buffer_size: 8 * 1024 * 1024,
            ..EngineOptions::default()
        };
        let db = DbImpl::open_with_fs(opts, fs.clone()).unwrap();
        let cf = db.default_cf();
        for i in 0..n {
            let k = format!("k{:08}", i);
            db.put(&cf, k.as_bytes(), b"payload").unwrap();
        }

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            let mut counter = 0u32;
            b.iter(|| {
                let target = format!("/ckpt-{}", counter);
                counter += 1;
                db.create_checkpoint(std::path::Path::new(&target))
                    .expect("create_checkpoint");
            });
        });
    }
    group.finish();
}

fn bench_open_from_checkpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("open_from_checkpoint");
    group.sample_size(10);
    for &n in &[1_000u32, 10_000] {
        // Build a checkpoint once.
        let opts = EngineOptions {
            db_path: "/db".to_string(),
            write_buffer_size: 8 * 1024 * 1024,
            ..EngineOptions::default()
        };
        let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
        let db = DbImpl::open_with_fs(opts, fs.clone()).unwrap();
        let cf = db.default_cf();
        for i in 0..n {
            let k = format!("k{:08}", i);
            db.put(&cf, k.as_bytes(), b"payload").unwrap();
        }
        db.create_checkpoint(std::path::Path::new("/ckpt")).unwrap();

        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let opts = EngineOptions {
                    db_path: "/ckpt".to_string(),
                    ..EngineOptions::default()
                };
                let _restored = DbImpl::open_from_checkpoint(opts, fs.clone()).unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_checkpoint_create, bench_open_from_checkpoint);
criterion_main!(benches);

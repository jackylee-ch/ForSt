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

//! BM-1.3 Sustained write throughput.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use forst_rs_bench::open_in_memory;

fn bench_sustained_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("sustained_put");
    group.sample_size(10);
    for &n in &[10_000u32, 50_000] {
        let db = open_in_memory(1024 * 1024); // 1 MB — forces flushes + compactions
        let cf = db.default_cf();

        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &count| {
            b.iter(|| {
                for i in 0..count {
                    let k = format!("k{:08}", i);
                    db.put(&cf, k.as_bytes(), b"payload_16_bytes").expect("put");
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_sustained_put);
criterion_main!(benches);

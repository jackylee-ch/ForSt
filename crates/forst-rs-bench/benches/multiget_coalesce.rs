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

//! FRS-MULTIGET-COALESCE mini-bench: the disaggregated MultiRead win on a
//! cold multi-key SST batch lookup.
//!
//! Motivation (ForSt mechanism `BlockBasedTable::RetrieveMultipleBlocks`,
//! `table/block_based/block_based_table_reader_sync_and_async.h`): forst-rs's
//! `batch_get_vectorized` groups pending keys by FILE but resolves each key with
//! an INDEPENDENT `get_versions(k)` — so N keys landing in N distinct COLD blocks
//! of one SST pay N separate block reads. On a disaggregated store each cold read
//! is a remote GET round-trip; `SstReaderImpl::prefetch_blocks_for_keys` (the new
//! lever) collapses those distinct blocks into ONE vectored read.
//!
//! ## Sim-S3 model
//!
//! The SST is served through a `RandomAccessFile` that adds a FIXED per-read
//! latency (`FRS_BENCH_GET_RTT_US`, default 200 µs) to every `read_at` — the
//! round-trip fixed cost of a remote GET. A byte-rate throttle alone cannot
//! capture this because coalescing collapses ROUND-TRIPS, not bytes.
//!
//! ## Why bench at the reader layer (not the full engine)
//!
//! The win is on the FIRST cold batch over an SST; a second batch is block-cache
//! warm. Benching the reader directly lets each iteration open a FRESH reader
//! with a COLD block cache (the worst case the coalesce targets) without engine
//! cache-clear plumbing — the measurement is exactly the per-file block-read I/O
//! the engine's L1+ loop performs.
//!
//! ## Arms (SAME SST: small block_size → many blocks; keys scattered across them)
//!
//!   * `off` — per-key `get_versions` over a fresh cold reader (today's path:
//!     one block read per distinct block, each an RTT round-trip).
//!   * `on`  — `prefetch_blocks_for_keys` (ONE coalesced vectored read warms
//!     every candidate block) THEN per-key `get_versions` (served from warm
//!     cache, zero further reads).
//!
//! Run: `FRS_BENCH_GET_RTT_US=200 cargo bench -p forst-rs-bench --bench multiget_coalesce`

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use forst_rs_common::{CompressionType, ForstResult};
use forst_rs_io::filesystem::RandomAccessFile;
use forst_rs_storage::cache::clock::ShardedClockCache;
use forst_rs_storage::cache::BlockCache;
use forst_rs_storage::sst::reader::SstReaderImpl;
use forst_rs_storage::sst::writer::{SstWriterImpl, SstWriterOptions};

/// Per-read RTT (µs) the sim-S3 file adds to every `read_at`.
fn get_rtt_us() -> u64 {
    std::env::var("FRS_BENCH_GET_RTT_US")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

/// A `RandomAccessFile` over an in-memory buffer that sleeps a fixed RTT before
/// each read — the round-trip fixed cost of a remote GET. Reports `is_local() ==
/// false` (the remote regime).
struct RttMemFile {
    data: Arc<Vec<u8>>,
    rtt: Duration,
}

impl RandomAccessFile for RttMemFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        if !self.rtt.is_zero() {
            std::thread::sleep(self.rtt);
        }
        let start = offset as usize;
        if start >= self.data.len() {
            return Ok(0);
        }
        let end = std::cmp::min(start + buf.len(), self.data.len());
        buf[..end - start].copy_from_slice(&self.data[start..end]);
        Ok(end - start)
    }
    fn file_size(&self) -> ForstResult<u64> {
        Ok(self.data.len() as u64)
    }
    fn is_local(&self) -> bool {
        false
    }
}

const N: usize = 2000;

/// Build the SST bytes once: many small blocks so a scattered key batch touches
/// several distinct blocks.
fn build_sst() -> Arc<Vec<u8>> {
    let mut writer = SstWriterImpl::with_options(SstWriterOptions {
        block_size: 512,
        compression: CompressionType::None,
        cf_id: forst_rs_common::DEFAULT_CF_ID,
    });
    for i in 0..N {
        let key = format!("key_{i:06}");
        let val = format!("value-for-key-{i:06}-{}", "p".repeat(48));
        writer
            .add(key.as_bytes(), Some(val.as_bytes()), i as u64 + 1, 1)
            .expect("add");
    }
    let (data, _info) = writer.finish().expect("finish");
    Arc::new(data)
}

/// Open a FRESH reader (cold block cache) over the RTT sim-S3 file.
fn fresh_reader(data: &Arc<Vec<u8>>) -> SstReaderImpl {
    let file = Box::new(RttMemFile {
        data: Arc::clone(data),
        rtt: Duration::from_micros(get_rtt_us()),
    });
    let cache: Arc<dyn BlockCache> = Arc::new(ShardedClockCache::new(64 * 1024 * 1024, 4));
    SstReaderImpl::open(file)
        .expect("open")
        .with_block_cache(cache, 1, 7)
}

/// A scattered batch of `count` keys.
fn scattered(count: usize) -> Vec<Vec<u8>> {
    let mut order: Vec<usize> = (0..N).collect();
    let mut s: u64 = 0xD1B54A32D192ED03;
    for i in (1..order.len()).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let j = (s as usize) % (i + 1);
        order.swap(i, j);
    }
    order
        .into_iter()
        .take(count)
        .map(|i| format!("key_{i:06}").into_bytes())
        .collect()
}

fn bench(c: &mut Criterion) {
    let data = build_sst();
    let mut group = c.benchmark_group("multiget_coalesce");
    group.sample_size(20);

    for &batch in &[32usize, 128, 256] {
        let probe = scattered(batch);

        // OFF: per-key get_versions over a cold reader (one RTT read per block).
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("off_batch_{batch}")),
            &batch,
            |b, &_n| {
                b.iter(|| {
                    let reader = fresh_reader(&data);
                    let mut found = 0usize;
                    for k in &probe {
                        if !reader.get_versions(k).expect("get").is_empty() {
                            found += 1;
                        }
                    }
                    black_box(found);
                });
            },
        );

        // ON: ONE coalesced prefetch warms every block, then per-key get is free.
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("on_batch_{batch}")),
            &batch,
            |b, &_n| {
                b.iter(|| {
                    let reader = fresh_reader(&data);
                    let refs: Vec<&[u8]> = probe.iter().map(|k| k.as_slice()).collect();
                    reader.prefetch_blocks_for_keys(&refs);
                    let mut found = 0usize;
                    for k in &probe {
                        if !reader.get_versions(k).expect("get").is_empty() {
                            found += 1;
                        }
                    }
                    black_box(found);
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);

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

//! R-1 mini-bench (PMC-1, 2026-06-17): isolated per-get cost of the SST
//! point-read on a COMPACTED v2 KV block — the dominant q11/q17 read-half term
//! (ARM C in the q11 read-path design, ~534 ns/get full-stack).
//!
//! Two arms over the SAME decompressed blocks (warmed once, so this measures
//! the in-block seek + value materialization, NOT decompression or I/O):
//!
//!   * BASELINE — `KvBlock::lookup`: restart-array binary search, then the
//!     matched value is COPIED out with `.to_vec()` (one heap alloc per hit) —
//!     the pre-R-1 shape `SstReaderImpl::get` used.
//!   * R-1      — `KvBlock::point_lookup_in_block`: identical restart-array
//!     binary search, but the value is returned as a ZERO-COPY `(offset, len)`
//!     range into the payload — no per-get alloc.
//!
//! Build with symbols for a profile:
//!   CARGO_PROFILE_RELEASE_STRIP=none cargo run --release \
//!     --bin q11_point_lookup_block -- --keys 1000000 --value-size 8
//!
//! Byte-identity is asserted (the bench panics on any mismatch) so the win is
//! never a correctness regression.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use forst_rs_common::{CompressionType, OpType};
use forst_rs_storage::sst::{encode_kv_data_block, schema::sst_schema, KvBlock};

use arrow::array::{BinaryArray, RecordBatch, UInt64Array, UInt8Array};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;

/// A pass-through allocator that counts allocations — local to THIS binary
/// (the `#[global_allocator]` only applies to this `main`), so it can report
/// the per-run heap-alloc count the `.to_vec()`-per-hit baseline pays.
struct CountingAlloc;
static ALLOCS: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Ordering::Relaxed);
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

fn alloc_count() -> u64 {
    ALLOCS.load(Ordering::Relaxed)
}
fn reset_alloc_count() {
    ALLOCS.store(0, Ordering::Relaxed);
}

/// Deterministic xorshift64* — same generator the kv_block property tests use.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

fn parse_arg(name: &str, default: usize) -> usize {
    let args: Vec<String> = std::env::args().collect();
    for w in args.windows(2) {
        if w[0] == name {
            return w[1].parse().unwrap_or(default);
        }
    }
    default
}

/// Rows per ~64 KiB compacted block (q11's 8-byte accumulator value class:
/// key ~16 B + value 8 B + framing ~12 B ≈ 36 B/row → ~1800 rows / 64 KiB).
const ROWS_PER_BLOCK: usize = 1800;

fn build_block(
    start: u64,
    count: usize,
    value_size: usize,
    compression: CompressionType,
) -> Vec<u8> {
    // Distinct, sorted keys "k{:012}" — the scattered session-key shape.
    let keys: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("k{:012}", start + i as u64).into_bytes())
        .collect();
    let value = vec![0xABu8; value_size];
    let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let val_refs: Vec<Option<&[u8]>> = (0..count).map(|_| Some(value.as_slice())).collect();
    let seqs: Vec<u64> = (0..count as u64).map(|i| 1000 + i).collect();
    let ops: Vec<u8> = vec![OpType::Put as u8; count];
    let batch = RecordBatch::try_new(
        Arc::new(sst_schema()),
        vec![
            Arc::new(BinaryArray::from_iter_values(key_refs)),
            Arc::new(BinaryArray::from_iter(val_refs)),
            Arc::new(UInt64Array::from(seqs)),
            Arc::new(UInt8Array::from(ops)),
        ],
    )
    .unwrap();
    encode_kv_data_block(&batch, compression).unwrap()
}

fn main() {
    let total_keys = parse_arg("--keys", 1_000_000);
    let value_size = parse_arg("--value-size", 8);
    let iters = parse_arg("--iters", 3);

    let compression = CompressionType::Lz4;
    let n_blocks = total_keys.div_ceil(ROWS_PER_BLOCK);

    // Build + DECODE (decompress) every block once — warm, so the timed loop
    // measures only the in-block seek + value handling (the cache-warm regime
    // R-1 targets; the decompressed `Arc<KvBlock>` IS the cached raw block).
    eprintln!(
        "building {n_blocks} compacted KV blocks ({ROWS_PER_BLOCK} rows each, value={value_size}B, {compression:?}) ..."
    );
    let mut blocks: Vec<KvBlock> = Vec::with_capacity(n_blocks);
    let mut key_for_block: Vec<u64> = Vec::with_capacity(n_blocks);
    for b in 0..n_blocks {
        let start = (b * ROWS_PER_BLOCK) as u64;
        let raw = build_block(start, ROWS_PER_BLOCK, value_size, compression);
        blocks.push(KvBlock::decode(&raw, true).unwrap());
        key_for_block.push(start);
    }

    // Scattered probes: `total_keys` probes total, each a (block, present key)
    // pair drawn at random — the q11 "scattered session key, different block
    // most probes" access pattern over WARM (already-decompressed) blocks.
    let mut s = 0x1234_5678_9ABC_DEF0u64;
    let probe_pairs: Vec<(usize, Vec<u8>)> = (0..total_keys)
        .map(|_| {
            let b = (xorshift(&mut s) as usize) % n_blocks;
            let off = (xorshift(&mut s) as usize) % ROWS_PER_BLOCK;
            (
                b,
                format!("k{:012}", key_for_block[b] + off as u64).into_bytes(),
            )
        })
        .collect();
    let n = probe_pairs.len();
    eprintln!("probing {n} scattered point-gets x {iters} iters over {n_blocks} warm blocks\n");

    // --- Byte-identity gate: R-1 == baseline for every probe ---
    for (b, probe) in &probe_pairs {
        let blk = &blocks[*b];
        let want = blk.lookup(probe).unwrap();
        let got = blk.point_lookup_in_block(probe).unwrap().map(|pv| {
            let v = pv
                .value
                .map(|(o, l)| blk.payload_bytes()[o as usize..(o + l) as usize].to_vec());
            (v, pv.sequence, pv.op_type)
        });
        assert_eq!(got, want, "R-1 byte-identity violation on {probe:?}");
    }
    eprintln!("byte-identity: OK ({n} probes match KvBlock::lookup)\n");

    let mut best_base = f64::MAX;
    let mut best_r1 = f64::MAX;
    let mut base_allocs = 0u64;
    let mut r1_allocs = 0u64;

    for it in 0..iters {
        // BASELINE: lookup + .to_vec() (the pre-R-1 get path).
        reset_alloc_count();
        let t = Instant::now();
        let mut sink = 0usize;
        for (b, probe) in &probe_pairs {
            if let Some((Some(v), seq, _)) = blocks[*b].lookup(probe).unwrap() {
                sink ^= v.len() ^ seq as usize;
            }
        }
        let base_ns = t.elapsed().as_nanos() as f64 / n as f64;
        base_allocs = alloc_count();
        std::hint::black_box(sink);

        // R-1: point_lookup_in_block — zero-copy range, no per-get alloc.
        reset_alloc_count();
        let t = Instant::now();
        let mut sink = 0usize;
        for (b, probe) in &probe_pairs {
            if let Some(pv) = blocks[*b].point_lookup_in_block(probe).unwrap() {
                if let Some((o, l)) = pv.value {
                    sink ^= l as usize ^ pv.sequence as usize ^ o as usize;
                }
            }
        }
        let r1_ns = t.elapsed().as_nanos() as f64 / n as f64;
        r1_allocs = alloc_count();
        std::hint::black_box(sink);

        eprintln!("iter {it}: baseline {base_ns:7.1} ns/get   R-1 {r1_ns:7.1} ns/get");
        best_base = best_base.min(base_ns);
        best_r1 = best_r1.min(r1_ns);
    }

    println!("\n=== R-1 point_lookup_in_block mini-bench ===");
    println!("keys={total_keys} value={value_size}B blocks={n_blocks} probes={n}");
    println!("baseline (lookup + .to_vec):     {best_base:7.1} ns/get   allocs/run={base_allocs}");
    println!("R-1      (point_lookup_in_block): {best_r1:7.1} ns/get   allocs/run={r1_allocs}");
    println!(
        "delta: {:.1} ns/get faster ({:.1}%), {} fewer heap allocs/run",
        best_base - best_r1,
        (best_base - best_r1) / best_base * 100.0,
        base_allocs.saturating_sub(r1_allocs)
    );
}

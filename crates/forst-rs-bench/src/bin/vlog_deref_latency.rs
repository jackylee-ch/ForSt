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

//! KVSEP-OPTIMAL deref-latency mini-bench (2026-06-14 investigation):
//! isolate the READ-SIDE cost of a KV-separated value deref, to answer:
//!
//! 1. What does ONE `VlogReader::get` cost in three regimes — (a) cold reader
//!    miss (first deref of a segment, fills the 64 KiB chunk), (b) warm
//!    sequential deref (scan locality — consecutive values in the same chunk),
//!    (c) warm random deref (point-get re-hitting the chunk cache or missing)?
//! 2. How much of that is the per-deref `Vec<u8>` allocation + CRC + (when
//!    compressed) decompress vs the read itself?
//! 3. CAN scan-locality drive the amortized per-deref cost toward ~zero
//!    (chunk-cache hit = memcpy only)? This is the Q4 "make deref zero-overhead
//!    on reads" question, and the basis for an S2-style universal coalesce.
//!
//! It does NOT run NexMark; it is a storage-layer latency probe on a local FS.
//!
//! Run: `cargo run -p forst-rs-bench --release --bin vlog_deref_latency`
//!      `... -- --records 100000 --value-size 512`

use std::path::Path;
use std::time::Instant;

use forst_rs_common::CompressionType;
use forst_rs_io::{FileSystem, LocalFileSystem};
use forst_rs_storage::vlog::{ValuePointer, VlogReader, VlogWriter};

fn parse_arg(args: &[String], flag: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Writes ONE segment holding `n` records of `value_size` bytes (the q-flush
/// shape: a flush appends values in key order into one segment). Returns the
/// pointers in append order (== key order, the scan-deref order).
fn write_one_segment(
    fs: &dyn FileSystem,
    dir: &Path,
    seg: u64,
    n: usize,
    value_size: usize,
    codec: CompressionType,
) -> Vec<ValuePointer> {
    let mut w = VlogWriter::create_with_compression(fs, dir, seg, codec).expect("create");
    let mut ptrs = Vec::with_capacity(n);
    // Pseudo-random but compressible-ish payload (mix so lz4 has some work).
    let mut v = vec![0u8; value_size];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(31).wrapping_add(7);
    }
    for i in 0..n {
        v[0] = i as u8;
        ptrs.push(w.append(&v).expect("append"));
    }
    w.sync().expect("sync");
    ptrs
}

struct Stats {
    label: String,
    n: usize,
    total_ns: u128,
    bytes: usize,
}
impl Stats {
    fn report(&self) {
        let per = self.total_ns as f64 / self.n as f64;
        let mbps = (self.bytes as f64) / (self.total_ns as f64 / 1e9) / (1024.0 * 1024.0);
        println!(
            "  {:<34} {:>8.1} ns/deref   {:>9} derefs   {:>8.1} MiB/s",
            self.label, per, self.n, mbps
        );
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n = parse_arg(&args, "--records", 200_000);
    let value_size = parse_arg(&args, "--value-size", 512);

    let tmp = std::env::temp_dir().join(format!("vlog-deref-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let fs = LocalFileSystem::new();

    println!("=== KVSEP deref-latency mini-bench ===");
    println!("records = {n}  value_size = {value_size} B  (one segment, key-order append)\n");

    for codec in [CompressionType::None, CompressionType::Lz4] {
        let codec_name = match codec {
            CompressionType::None => "uncompressed",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        };
        let seg = match codec {
            CompressionType::None => 1u64,
            _ => 2u64,
        };
        let ptrs = write_one_segment(&fs, &tmp, seg, n, value_size, codec);

        // ---- A: cold sequential — fresh reader, derefs in key (append) order.
        // This is the SCAN deref pattern (q4/q7/q9/q20 value-carrying drain):
        // strong intra-segment locality, so the 64 KiB chunk amortizes over
        // many consecutive records (chunk/record derefs per pread).
        {
            let reader = VlogReader::open(&fs, &tmp, seg).expect("open");
            let mut bytes = 0usize;
            let t = Instant::now();
            for p in &ptrs {
                let v = reader.get(p).expect("get");
                bytes += v.len();
            }
            Stats {
                label: format!("[{codec_name}] cold SEQ (scan locality)"),
                n,
                total_ns: t.elapsed().as_nanos(),
                bytes,
            }
            .report();
        }

        // ---- B: warm random — fresh reader, derefs in shuffled order. This is
        // the POINT-GET pattern (a join probe hitting scattered keys): each
        // deref likely misses the single-slot chunk cache → one pread + memcpy.
        {
            let reader = VlogReader::open(&fs, &tmp, seg).expect("open");
            // Deterministic shuffle (xorshift index walk).
            let mut order: Vec<usize> = (0..n).collect();
            let mut s: u64 = 0x9E3779B97F4A7C15;
            for i in (1..n).rev() {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                let j = (s as usize) % (i + 1);
                order.swap(i, j);
            }
            let mut bytes = 0usize;
            let t = Instant::now();
            for &i in &order {
                let v = reader.get(&ptrs[i]).expect("get");
                bytes += v.len();
            }
            Stats {
                label: format!("[{codec_name}] random (point-get, chunk miss)"),
                n,
                total_ns: t.elapsed().as_nanos(),
                bytes,
            }
            .report();
        }

        // ---- C: hot single-record re-deref — same pointer repeatedly. Isolates
        // the per-deref FIXED cost ABOVE the I/O: chunk-cache HIT path = bounds
        // check + memcpy(value_size) + CRC32 + (lz4) decompress + Vec alloc.
        // This is the floor a perfect cache/coalesce could reach (Q4).
        {
            let reader = VlogReader::open(&fs, &tmp, seg).expect("open");
            // Warm the chunk on the target record first.
            let target = &ptrs[n / 2];
            let _ = reader.get(target).expect("warm");
            let mut bytes = 0usize;
            let t = Instant::now();
            for _ in 0..n {
                let v = reader.get(target).expect("get");
                bytes += v.len();
            }
            Stats {
                label: format!("[{codec_name}] hot HIT (cache-hit floor)"),
                n,
                total_ns: t.elapsed().as_nanos(),
                bytes,
            }
            .report();
        }

        // ---- D: COALESCED random — the SAME scattered (random-order) batch as
        // arm B, but the derefs are first SORTED by (segment, offset) before
        // issuing them to the reader. This models the proposed fix: a batch_get
        // that collects all BlobRef pointers, sorts them by physical location,
        // and derefs in offset order so the 64 KiB chunk cache HITS instead of
        // thrashing. The RESULT is reordered back to key order by the caller
        // (zero correctness cost — values are independent). This is the Q4/Q5
        // "make the deref zero-overhead via batched coalescing" arm.
        {
            let reader = VlogReader::open(&fs, &tmp, seg).expect("open");
            // Same shuffled set as arm B (the scattered batch), then sort by
            // physical offset — the coalesce.
            let mut order: Vec<usize> = (0..n).collect();
            let mut s: u64 = 0x9E3779B97F4A7C15;
            for i in (1..n).rev() {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                let j = (s as usize) % (i + 1);
                order.swap(i, j);
            }
            order.sort_by_key(|&i| ptrs[i].offset);
            let mut bytes = 0usize;
            let t = Instant::now();
            for &i in &order {
                let v = reader.get(&ptrs[i]).expect("get");
                bytes += v.len();
            }
            Stats {
                label: format!("[{codec_name}] COALESCED (sort-by-offset fix)"),
                n,
                total_ns: t.elapsed().as_nanos(),
                bytes,
            }
            .report();
        }
        println!();
    }

    let _ = std::fs::remove_dir_all(&tmp);
    println!("Interpretation:");
    println!("  cold SEQ ~= the scan/value-carrying drain deref cost (q4/q7/q9/q20).");
    println!("  random   ~= the point-get / scattered join probe deref cost.");
    println!("  hot HIT  ~= the irreducible per-deref CPU floor (memcpy+CRC+alloc[+lz4]).");
    println!("  Gap cold->hot = the I/O+chunk-fill amortization a coalesce/cache removes.");
}

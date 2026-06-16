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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use forst_rs_common::{CompressionType, ForstResult};
use forst_rs_engine::{
    set_kv_separation_override, set_vlog_coalesce_deref_override, ColumnFamilyDescriptor, DbImpl,
};
use forst_rs_io::{FileSystem, LocalFileSystem, RandomAccessFile};
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

        // ---- B': warm random POINT-DEREF — the SAME scattered batch as arm B,
        // but each deref uses `get_point` (FRS-VLOG-POINT-DEREF): read EXACTLY the
        // record bytes, no 64 KiB chunk fill. This is the q11/q17 scattered-RMW
        // fix at the primitive level — for a scattered point-get there is no
        // subsequent same-segment hit to amortize the chunk fill, so the chunk
        // read in arm B is pure read-amp. Byte-identical value to arm B's `get`.
        {
            let reader = VlogReader::open(&fs, &tmp, seg).expect("open");
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
                let v = reader.get_point(&ptrs[i]).expect("get_point");
                bytes += v.len();
            }
            Stats {
                label: format!("[{codec_name}] random POINT (point-deref fix)"),
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
    println!("  random   ~= the point-get / scattered join probe deref cost (chunk-fill get).");
    println!("  random POINT ~= the SAME scattered batch via get_point (FRS-VLOG-POINT-DEREF):");
    println!("             reads EXACTLY the record (no 64 KiB chunk fill) — the q11/q17 fix.");
    println!("  hot HIT  ~= the irreducible per-deref CPU floor (memcpy+CRC+alloc[+lz4]).");
    println!("  Gap cold->hot = the I/O+chunk-fill amortization a coalesce/cache removes.");

    // ===================================================================
    // ENGINE ARM: the REAL `batch_get_vectorized` deref path (not the model),
    // per-key (FRS_VLOG_COALESCE_DEREF OFF) vs coalesced (ON), on the q9-probe
    // shape — many separated values read in SCATTERED order through the engine.
    // ===================================================================
    engine_batched_deref_arm(value_size);

    // ===================================================================
    // SIM-S3 ARM: a latency-injecting RandomAccessFile (each read_at pays an
    // RTT) shows the deref I/O count + wall collapse N scattered point GETs →
    // ~1 ranged GET per segment (the disagg-critical win). Counts the actual
    // read_at calls each path issues.
    // ===================================================================
    sim_s3_ranged_get_arm();
}

/// ENGINE ARM (confirm gate): drive the REAL `DbImpl::batch_get_vectorized`
/// deref path on a scattered KV-sep batch, flag OFF (per-key inline deref) vs ON
/// (coalesced). Proves the engine batch path realizes the storage-level coalesce
/// win (the same 8-105× shape), not just the isolated model arm above.
fn engine_batched_deref_arm(value_size: usize) {
    println!("=== ENGINE batch_get_vectorized deref (OFF=per-key vs ON=coalesced) ===");
    let value_size = value_size.max(256); // must separate (≥ kv_min_blob_size)
    const N: usize = 20_000;

    // Build a fresh DB, separate N values across SST tiers, return (db, cf, keys
    // in SCATTERED order, expected values). Built identically per arm so the only
    // difference is the coalesce flag.
    let build = || {
        set_kv_separation_override(Some(true));
        let db = DbImpl::open_default().expect("open");
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new("eng-coalesce"))
            .expect("cf");
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(N);
        let mut payload = vec![0u8; value_size];
        for i in 0..N {
            // semi-random payload so lz4 keeps it ≥ threshold
            for (j, b) in payload.iter_mut().enumerate() {
                *b = ((i.wrapping_mul(131).wrapping_add(j)) % 251) as u8;
            }
            let k = format!("k{i:07}").into_bytes();
            db.put(&cf, &k, &payload).expect("put");
            keys.push(k);
        }
        db.switch_and_flush(&cf).expect("flush").expect("sst");
        // Scatter the read order (deterministic shuffle) = the join-probe shape.
        let mut s: u64 = 0x9E3779B97F4A7C15;
        for i in (1..keys.len()).rev() {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            let j = (s as usize) % (i + 1);
            keys.swap(i, j);
        }
        (db, cf, keys)
    };

    let run = |coalesce: bool| -> (f64, usize) {
        set_vlog_coalesce_deref_override(Some(coalesce));
        let (db, cf, keys) = build();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let t = Instant::now();
        let got = db
            .batch_get_vectorized(&cf, &key_refs, u64::MAX)
            .expect("batch");
        let ns = t.elapsed().as_nanos() as f64 / keys.len() as f64;
        let hits = got.iter().filter(|v| v.is_some()).count();
        (ns, hits)
    };

    let (off_ns, off_hits) = run(false);
    let (on_ns, on_hits) = run(true);
    set_vlog_coalesce_deref_override(None);
    set_kv_separation_override(None);

    assert_eq!(off_hits, N, "OFF arm must resolve every key");
    assert_eq!(on_hits, N, "ON arm must resolve every key");
    println!("  value_size = {value_size} B   keys = {N}  (scattered, separated → SST tier)");
    println!("  OFF (per-key inline deref)   {off_ns:>9.1} ns/key");
    println!("  ON  (coalesced batched)      {on_ns:>9.1} ns/key");
    let speedup = if on_ns > 0.0 {
        off_ns / on_ns
    } else {
        f64::INFINITY
    };
    println!("  speedup OFF/ON               {speedup:>9.2}x\n");
}

/// SIM-S3 ARM: count + time the deref READS under an injected per-read RTT.
/// Per-key `get` issues ONE read_at per scattered value (N round-trips); the
/// coalesced `get_coalesced` over the offset-sorted group issues ONE spanning
/// read_at (1 round-trip per segment). This is the disagg N→~1 ranged-GET win.
fn sim_s3_ranged_get_arm() {
    println!("=== SIM-S3 ranged-GET coalesce (per-read RTT injected) ===");
    const N: usize = 4_000;
    const VALUE_SIZE: usize = 512;
    let rtt = Duration::from_micros(200); // modeled intra-DC object-store RTT

    let tmp = std::env::temp_dir().join(format!("vlog-s3sim-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();
    let real = LocalFileSystem::new();
    let ptrs = write_one_segment(&real, &tmp, 1, N, VALUE_SIZE, CompressionType::None);

    // The scattered (random) order = a join probe hitting keys out of order.
    let mut order: Vec<usize> = (0..N).collect();
    let mut s: u64 = 0x9E3779B97F4A7C15;
    for i in (1..N).rev() {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        let j = (s as usize) % (i + 1);
        order.swap(i, j);
    }

    let counter = Arc::new(AtomicUsize::new(0));
    let fs = LatencyFs {
        inner: LocalFileSystem::new(),
        rtt,
        reads: counter.clone(),
    };

    // Per-key arm: deref each scattered pointer with its own reader.get → one
    // RTT-paying read_at per (chunk-miss) value.
    let reader = VlogReader::open(&fs, &tmp, 1).expect("open");
    counter.store(0, Ordering::Relaxed);
    let t = Instant::now();
    for &i in &order {
        let _ = reader.get(&ptrs[i]).expect("get");
    }
    let per_key_ms = t.elapsed().as_secs_f64() * 1e3;
    let per_key_reads = counter.load(Ordering::Relaxed);

    // Coalesced arm: sort the SAME scattered batch by offset, one ranged read.
    let mut sorted = order.clone();
    sorted.sort_by_key(|&i| ptrs[i].offset);
    let refs: Vec<&ValuePointer> = sorted.iter().map(|&i| &ptrs[i]).collect();
    let reader2 = VlogReader::open(&fs, &tmp, 1).expect("open");
    counter.store(0, Ordering::Relaxed);
    let t = Instant::now();
    let _ = reader2.get_coalesced(&refs).expect("coalesced");
    let coalesced_ms = t.elapsed().as_secs_f64() * 1e3;
    let coalesced_reads = counter.load(Ordering::Relaxed);

    let _ = std::fs::remove_dir_all(&tmp);
    println!(
        "  keys = {N}  value_size = {VALUE_SIZE} B  RTT = {} us  (one segment)",
        rtt.as_micros()
    );
    println!("  per-key  get        {per_key_reads:>7} ranged GETs   {per_key_ms:>9.1} ms");
    println!("  coalesced get       {coalesced_reads:>7} ranged GETs   {coalesced_ms:>9.1} ms");
    println!(
        "  GET reduction       {:>7}x   wall speedup {:>6.1}x\n",
        per_key_reads
            .checked_div(coalesced_reads)
            .unwrap_or(per_key_reads),
        if coalesced_ms > 0.0 {
            per_key_ms / coalesced_ms
        } else {
            f64::INFINITY
        }
    );
    println!("Interpretation (disagg): per-key deref = N scattered remote GETs (N×RTT);");
    println!("  coalesced = ~1 ranged GET per segment — the disagg-critical N→1 collapse.");
}

/// A latency-injecting FileSystem: every `read_at` sleeps `rtt` (a modeled
/// object-store round-trip) and bumps a read counter. Used by the SIM-S3 arm to
/// count the ranged GETs each deref strategy issues. Local FS does the real I/O.
struct LatencyFs {
    inner: LocalFileSystem,
    rtt: Duration,
    reads: Arc<AtomicUsize>,
}

struct LatencyFile {
    inner: Box<dyn RandomAccessFile>,
    rtt: Duration,
    reads: Arc<AtomicUsize>,
}

impl RandomAccessFile for LatencyFile {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> ForstResult<usize> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        std::thread::sleep(self.rtt);
        self.inner.read_at(offset, buf)
    }

    fn file_size(&self) -> ForstResult<u64> {
        self.inner.file_size()
    }
}

impl FileSystem for LatencyFs {
    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        Ok(Box::new(LatencyFile {
            inner: self.inner.open_random_access_file(path)?,
            rtt: self.rtt,
            reads: self.reads.clone(),
        }))
    }

    fn open_sequential_file(
        &self,
        path: &Path,
    ) -> ForstResult<Box<dyn forst_rs_io::SequentialFile>> {
        self.inner.open_sequential_file(path)
    }

    fn open_writable_file(
        &self,
        path: &Path,
        mode: forst_rs_io::WriteMode,
    ) -> ForstResult<Box<dyn forst_rs_io::WritableFile>> {
        self.inner.open_writable_file(path, mode)
    }

    fn create_dir_all(&self, path: &Path) -> ForstResult<()> {
        self.inner.create_dir_all(path)
    }

    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        self.inner.file_exists(path)
    }

    fn get_file_metadata(&self, path: &Path) -> ForstResult<forst_rs_io::FileMetadata> {
        self.inner.get_file_metadata(path)
    }

    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<forst_rs_io::FileMetadata>> {
        self.inner.list_dir(dir)
    }

    fn delete_file(&self, path: &Path) -> ForstResult<()> {
        self.inner.delete_file(path)
    }

    fn delete_dir(&self, path: &Path, recursive: bool) -> ForstResult<()> {
        self.inner.delete_dir(path, recursive)
    }

    fn rename(&self, src: &Path, dst: &Path) -> ForstResult<()> {
        self.inner.rename(src, dst)
    }

    fn name(&self) -> &str {
        "LatencyFs(vlog_deref_latency sim-S3 arm)"
    }

    fn is_local(&self) -> bool {
        // Model a REMOTE object store so any is_local() gate takes the remote path.
        false
    }
}

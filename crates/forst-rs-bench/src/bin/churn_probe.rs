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

//! H1 microbench (2026-06-12 sorted-run-discipline design §B): reproduce the
//! q7 regime — sustained interleaved append-heavy writes on two key streams
//! with TTL-ish deletes — while concurrently measuring:
//!
//! 1. **write-amp**: physical bytes appended to disk (counting `FileSystem`
//!    wrapper around `LocalFileSystem`) ÷ logical bytes put by the workload;
//! 2. **sorted-run fan-out over time**: per-level live-file counts every
//!    sample tick (`list_live_files`), plus the engine's own per-probe
//!    `n_ovl` attribution when launched with `FRS_BULK_SAMPLE` (the
//!    `[DECAY_ATTR …]` stderr lines from `build_lazy_prefix_key_stream`);
//! 3. **probe latency degradation**: a sidecar thread runs `prefix_scan`
//!    probes (the q7 interval-join probe path) and the sampler reports
//!    windowed p50/p99 — early-vs-late windows = the degradation curve.
//!
//! Cells are selected by ENV in the LAUNCHING shell (the engine reads the
//! write-controller knobs once per process), one process per cell:
//!
//! ```bash
//! # A: shipped defaults (L0 trigger 4 / slowdown 40 / stop 64)
//! cargo run -p forst-rs-bench --release --bin churn_probe -- --runs 3
//! # B: RocksDB L0 discipline (slowdown 20 / stop 36)
//! FRS_L0_SLOWDOWN_TRIGGER=20 FRS_L0_STOP_TRIGGER=36 cargo run ... -- --runs 3
//! # C: dose-response control — background compaction OFF (overlap grows
//! #    unbounded; the probe-latency-vs-L0-count curve is the direct H1 dose)
//! FRS_L0_COMPACTION_TRIGGER=100000 cargo run ... -- --runs 3 --label nocompact
//! # D: leveled-emulation upper bound — periodic full `compact_all` sweeps
//! cargo run ... -- --runs 3 --compact-every-s 15 --label leveled-emu
//! ```
//!
//! Methodology (binding): same-box same-session A/B only, n>=3 per cell,
//! median reported. Mac numbers are system-allocator numbers.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use forst_rs_common::{EngineOptions, ForstResult};
use forst_rs_engine::DbImpl;
use forst_rs_io::{
    FileMetadata, FileSystem, LocalFileSystem, RandomAccessFile, SequentialFile, WritableFile,
    WriteMode,
};

// ---------------------------------------------------------------------------
// Write-counting FileSystem wrapper: counts every byte appended through
// `open_writable_file` (flush SSTs + compaction SSTs + manifest). Reads are
// passed through UNTOUCHED so the local pread/file-handle fast paths stay on
// the production code path (read volume is not a primary metric here;
// fan-out + probe latency are).
// ---------------------------------------------------------------------------

struct CountingWritable {
    inner: Box<dyn WritableFile>,
    written: Arc<AtomicU64>,
}

impl WritableFile for CountingWritable {
    fn append(&mut self, data: &[u8]) -> ForstResult<()> {
        self.written.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.append(data)
    }
    fn flush(&mut self) -> ForstResult<()> {
        self.inner.flush()
    }
    fn sync(&mut self) -> ForstResult<()> {
        self.inner.sync()
    }
    fn file_size(&self) -> ForstResult<u64> {
        self.inner.file_size()
    }
}

struct CountingFs {
    inner: LocalFileSystem,
    written: Arc<AtomicU64>,
}

impl FileSystem for CountingFs {
    fn open_sequential_file(&self, path: &Path) -> ForstResult<Box<dyn SequentialFile>> {
        self.inner.open_sequential_file(path)
    }
    fn open_random_access_file(&self, path: &Path) -> ForstResult<Box<dyn RandomAccessFile>> {
        self.inner.open_random_access_file(path)
    }
    fn open_writable_file(
        &self,
        path: &Path,
        mode: WriteMode,
    ) -> ForstResult<Box<dyn WritableFile>> {
        let inner = self.inner.open_writable_file(path, mode)?;
        Ok(Box::new(CountingWritable {
            inner,
            written: Arc::clone(&self.written),
        }))
    }
    fn file_exists(&self, path: &Path) -> ForstResult<bool> {
        self.inner.file_exists(path)
    }
    fn get_file_metadata(&self, path: &Path) -> ForstResult<FileMetadata> {
        self.inner.get_file_metadata(path)
    }
    fn list_dir(&self, dir: &Path) -> ForstResult<Vec<FileMetadata>> {
        self.inner.list_dir(dir)
    }
    fn create_dir_all(&self, dir: &Path) -> ForstResult<()> {
        self.inner.create_dir_all(dir)
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
    fn supports_atomic_rename(&self) -> bool {
        self.inner.supports_atomic_rename()
    }
    fn sync_dir(&self, dir: &Path) -> ForstResult<()> {
        self.inner.sync_dir(dir)
    }
    fn name(&self) -> &str {
        "counting-local"
    }
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Args {
    duration_s: u64,
    write_rate: u64, // rows/s target (both streams combined)
    value_bytes: usize,
    buckets: u64,     // join-key cardinality (prefix universe)
    window_rows: u64, // TTL window: delete row (seq - window_rows) as seq advances
    probe_threads: usize,
    sample_s: u64,
    compact_every_s: u64, // 0 = off; >0 = leveled-emulation sweeps
    runs: usize,
    label: String,
    engine: String, // "frs" (default) | "rocksdb" (needs --features rocksdb-baseline)
    smoke: bool,
    /// 2026-06-13 write-path survey cell (b): TTL-segment / FIFO-drop MODEL —
    /// suppress tombstone deletes entirely (a time-segmented FIFO design never
    /// writes per-key deletes; expiry = whole-segment drop). Combined with
    /// FRS_L0_COMPACTION_TRIGGER=100000 (compaction off) this measures the
    /// flush-only physical write floor of a drop-instead-of-compact engine.
    no_deletes: bool,
    /// 2026-06-13 write-path survey §5 cell: memtable-pressure dose —
    /// override `EngineOptions::write_buffer_size` (MiB; 0 = engine default).
    wbuf_mib: usize,
    /// FRS-M3 G3 cell (2026-06-12 sorted-run §4 M3): number of column
    /// families to spread the churn over (q7 churns join sides + timers =
    /// multiple CFs; cross-CF concurrency is M3's primary claim). Rows are
    /// assigned cf = bucket % cfs so the TTL deleter lands on the same CF.
    /// 1 (default) = the original single-CF cell.
    cfs: usize,
    /// FRS-WA-V1 gate cell (2026-06-13 survey §6 stage V1): lifecycle-
    /// segment mode. Declares every CF `Windowed{ttl = window_rows}` (clock
    /// = row seq), suppresses per-key deletes (expiry replaces them),
    /// advances the engine watermark to the write head, notes event-time
    /// bounds ahead of writes, and runs a PREMATURE-DROP VERIFIER thread
    /// that point-gets safely-live keys continuously — any miss is the
    /// correctness falsifier firing (process exits non-zero). Forces the
    /// engine flag ON via `set_lifecycle_segments_override` (no env needed).
    lifecycle: bool,
    /// FRS-WA-V2a-2 gate cell (2026-06-13 survey §6 stage V2 / §10.1 item
    /// 3): flush-time KV separation mode. Forces `FRS_KV_SEPARATION` ON via
    /// `set_kv_separation_override` (no env needed). The workload's 200-B
    /// values exceed the default 128-B blob threshold, so every flushed Put
    /// separates: compaction then moves 21-B pointers instead of values —
    /// the cell measures the REAL assembled write-amp the §3.1 model
    /// predicts (~1.36×) plus the probe-side deref cost (gate ≤1.3× warm
    /// baseline p50).
    kvsep: bool,
    /// FRS-WA-V3 gate cell: TRIVIAL-MOVE-SHAPED workload — keys are
    /// globally monotone (`s{seq:015}`), so every flush covers a fresh
    /// disjoint range and every L0 rollup / level demotion is
    /// non-overlapping. Combined with `--no-deletes` this isolates the V3
    /// write-amp delta: rewrite-compactions vs metadata-only moves.
    seq_keys: bool,
    /// FRS-WA-V3: force `FRS_TRIVIAL_MOVE` ON for this process.
    trivial_move: bool,
    /// FRS-M5 cell (2026-06-13 cycle 2): generate COMPRESSIBLE,
    /// NexMark-shaped values instead of the default random (incompressible)
    /// fill. The default fill is random bytes BY CONSTRUCTION, so neither SST
    /// block compression nor vlog compression can shrink it — which is why
    /// cycle-1 could not measure M5 (the compression-parity lever). With this
    /// flag every value is an auction/bid-shaped record (low-entropy repeated
    /// fields + a common-prefix URL + repetitive padding). It is HIGHLY
    /// compressible (the synthetic padding compresses harder than typical
    /// real NexMark state — treat the measured ratio as an UPPER bound on the
    /// M5 win; real data compresses less, same-signed). What it establishes
    /// rigorously is the DIRECTION + COMPOUNDING (none<lz4<zstd ordering; does
    /// compression stack with KV-sep), which hold at any compressibility.
    /// Combined with `FRS_SST_COMPRESSION={none|lz4|zstd}` (SST blocks) and
    /// `FRS_VLOG_COMPRESSION={inherit|none|lz4|zstd}` (vlog under --kvsep)
    /// this measures write-amp + bytes-to-disk for the codec × KV-sep matrix.
    compressible: bool,
    /// FRS-AKV (adaptive KV-sep, 2026-06-14): run the 3-SHAPE adaptive
    /// mini-bench (the V3-confirm gate, NOT NexMark) under ONE uniform config
    /// (threshold 256 + resident budget + adaptive pressure/GC). Reports, per
    /// shape: did it separate, write-amp, resident vlog bytes, live segments.
    ///   - q7/q19-shape FIFO  → FULL separation, resident bounded, write-amp low
    ///   - q9-shape scattered → backs off under pressure, resident ≤ budget
    ///   - q11/q17-shape small → stays INLINE (no vlog writes)
    akv_shapes: bool,
    /// FRS-AKV: resident vlog byte budget for `--akv-shapes` (MiB).
    akv_budget_mib: u64,
}

impl Args {
    fn parse() -> Self {
        let mut a = Args {
            duration_s: 90,
            write_rate: 200_000,
            value_bytes: 200,
            buckets: 4096,
            window_rows: 4_000_000,
            probe_threads: 1,
            sample_s: 5,
            compact_every_s: 0,
            runs: 1,
            label: String::from("default"),
            engine: String::from("frs"),
            smoke: false,
            no_deletes: false,
            wbuf_mib: 0,
            cfs: 1,
            lifecycle: false,
            kvsep: false,
            seq_keys: false,
            trivial_move: false,
            compressible: false,
            akv_shapes: false,
            akv_budget_mib: 2,
        };
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < argv.len() {
            let take = |i: &mut usize| -> String {
                *i += 1;
                argv.get(*i)
                    .unwrap_or_else(|| panic!("missing value for {}", argv[*i - 1]))
                    .clone()
            };
            match argv[i].as_str() {
                "--duration-s" => a.duration_s = take(&mut i).parse().unwrap(),
                "--write-rate" => a.write_rate = take(&mut i).parse().unwrap(),
                "--value-bytes" => a.value_bytes = take(&mut i).parse().unwrap(),
                "--buckets" => a.buckets = take(&mut i).parse().unwrap(),
                "--window-rows" => a.window_rows = take(&mut i).parse().unwrap(),
                "--probe-threads" => a.probe_threads = take(&mut i).parse().unwrap(),
                "--sample-s" => a.sample_s = take(&mut i).parse().unwrap(),
                "--compact-every-s" => a.compact_every_s = take(&mut i).parse().unwrap(),
                "--runs" => a.runs = take(&mut i).parse().unwrap(),
                "--label" => a.label = take(&mut i),
                "--engine" => a.engine = take(&mut i),
                "--smoke" => a.smoke = true,
                "--no-deletes" => a.no_deletes = true,
                "--wbuf-mib" => a.wbuf_mib = take(&mut i).parse().unwrap(),
                "--cfs" => a.cfs = take(&mut i).parse().unwrap(),
                "--lifecycle" => a.lifecycle = true,
                "--kvsep" => a.kvsep = true,
                "--seq-keys" => a.seq_keys = true,
                "--trivial-move" => a.trivial_move = true,
                "--compressible" => a.compressible = true,
                "--akv-shapes" => a.akv_shapes = true,
                "--akv-budget-mib" => a.akv_budget_mib = take(&mut i).parse().unwrap(),
                other => panic!("unknown arg {other}"),
            }
            i += 1;
        }
        if a.smoke {
            a.duration_s = 10;
            a.write_rate = 50_000;
            a.window_rows = 200_000;
            a.runs = 1;
        }
        a
    }
}

struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }
}

/// Deterministic bucket for row `seq` of stream `s` — lets the TTL deleter
/// reconstruct the exact key it must tombstone without remembering it.
fn bucket_of(seq: u64, stream: u8, buckets: u64) -> u64 {
    let mut x = seq ^ (0x9E37_79B9_7F4A_7C15u64.rotate_left(stream as u32));
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    x ^= x >> 33;
    x % buckets
}

/// Key layout mirrors the Flink prefix layout: stream tag + zero-padded
/// bucket (the probe prefix) + zero-padded seq (interval-join timestamp).
fn make_key(stream: u8, bucket: u64, seq: u64) -> Vec<u8> {
    format!("{}{:06}|{:012}", stream as char, bucket, seq).into_bytes()
}

fn make_prefix(stream: u8, bucket: u64) -> Vec<u8> {
    format!("{}{:06}|", stream as char, bucket).into_bytes()
}

/// FRS-M5: fill `value` with a COMPRESSIBLE, NexMark-shaped record (write the
/// bytes in place to keep the writer's zero-alloc hot loop). Real NexMark
/// auction/bid state is low-entropy: small integer ids drawn from a bounded
/// universe, a channel string from a tiny set, a URL sharing a long common
/// prefix, and free-text padding with repetition — LZ4/Snappy shrink it
/// ~2-3×, unlike the random default fill. `seq`/`bucket` vary the leading
/// fields so rows are not byte-identical (that would over-state the ratio).
fn fill_compressible(value: &mut [u8], seq: u64, bucket: u64) {
    // A canonical NexMark-ish bid/auction JSON-ish line. The trailing URL +
    // "extra" padding is the bulk and is highly repetitive (the realistic
    // compressible part). Low-cardinality fields (bucket-derived) repeat
    // across rows; seq keeps each row distinct.
    let head = format!(
        "{{\"auction\":{},\"bidder\":{},\"price\":{},\"channel\":\"channel-{}\",\
         \"url\":\"https://www.nexmark.com/item/path/to/auction?id=",
        bucket,
        seq % 1000,
        (seq % 950) + 50,
        bucket % 8,
    );
    let head = head.as_bytes();
    let n = value.len();
    let mut pos = 0usize;
    let copy = head.len().min(n);
    value[..copy].copy_from_slice(&head[..copy]);
    pos += copy;
    // Pad the remainder with a repeating low-entropy filler (the "extra"
    // NexMark field is generated this way: a fixed phrase repeated).
    const FILLER: &[u8] = b"+item&category=10&price&channel&AAAAAAAA ";
    while pos < n {
        let take = FILLER.len().min(n - pos);
        value[pos..pos + take].copy_from_slice(&FILLER[..take]);
        pos += take;
    }
}

/// Windowed probe-latency accumulator: probes push ns, sampler swaps out.
#[derive(Default)]
struct ProbeWindow {
    lat_ns: Mutex<Vec<u64>>,
}

impl ProbeWindow {
    fn record(&self, ns: u64) {
        self.lat_ns.lock().unwrap().push(ns);
    }
    /// (count, p50_us, p99_us) of the window, then reset.
    fn drain(&self) -> (usize, f64, f64) {
        let mut v = std::mem::take(&mut *self.lat_ns.lock().unwrap());
        if v.is_empty() {
            return (0, 0.0, 0.0);
        }
        v.sort_unstable();
        let p50 = v[v.len() / 2] as f64 / 1e3;
        let p99 = v[(v.len() * 99) / 100] as f64 / 1e3;
        (v.len(), p50, p99)
    }
}

#[derive(Clone, Debug, Default)]
struct Sample {
    t_s: f64,
    l0: usize,
    files_per_level: Vec<usize>,
    live_bytes: u64,
    /// FRS-WA-V2b: total on-disk bytes of *.vlog segments (the GC
    /// space-amp gate: bounded vs unbounded growth).
    vlog_bytes: u64,
    phys_write_bytes: u64,
    logical_bytes: u64,
    rows_written: u64,
    probe_n: usize,
    probe_p50_us: f64,
    probe_p99_us: f64,
}

struct RunSummary {
    write_amp: f64,
    /// FRS-WA-V2b: last-sample vlog footprint (MiB).
    last_vlog_mib: f64,
    rows_written: u64,
    logical_mib: f64,
    phys_mib: f64,
    probe_p50_early_us: f64,
    probe_p50_late_us: f64,
    probe_p99_early_us: f64,
    probe_p99_late_us: f64,
    max_l0: usize,
    last_l0: usize,
    max_total_files: usize,
    achieved_write_rate: f64,
    /// FRS-WA-V1 (--lifecycle only): premature-drop verifier results.
    verify_checks: u64,
    premature_miss: u64,
}

fn one_run(args: &Args, run_idx: usize, workroot: &Path) -> RunSummary {
    let db_path = workroot.join(format!("run{run_idx}"));
    std::fs::create_dir_all(&db_path).expect("create run dir");
    let written = Arc::new(AtomicU64::new(0));
    let fs: Arc<dyn FileSystem> = Arc::new(CountingFs {
        inner: LocalFileSystem,
        written: Arc::clone(&written),
    });
    let mut opts = EngineOptions {
        db_path: db_path.to_string_lossy().into_owned(),
        ..EngineOptions::default()
    };
    if args.wbuf_mib > 0 {
        opts.write_buffer_size = args.wbuf_mib * 1024 * 1024;
    }
    // FRS-WA-V1: lifecycle cell — force the engine flag ON for this process
    // (default OFF everywhere else; the override is the test-safe hook).
    if args.lifecycle {
        forst_rs_engine::set_lifecycle_segments_override(Some(true));
    }
    // FRS-WA-V2a-2: kvsep cell — force the engine flag ON for this process.
    if args.kvsep {
        forst_rs_engine::set_kv_separation_override(Some(true));
    }
    // FRS-WA-V3: trivial-move cell.
    forst_rs_engine::set_trivial_move_override(if args.trivial_move { Some(true) } else { None });
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    // FRS-M3 G3: cf[0] = default; cf[1..] = extra churn CFs. Bucket-affine
    // assignment keeps put/delete/probe for a key on ONE cf.
    let cfs: Vec<_> = (0..args.cfs.max(1))
        .map(|i| {
            if i == 0 {
                db.default_cf()
            } else {
                db.create_column_family(forst_rs_engine::ColumnFamilyDescriptor::new(format!(
                    "churn{i}"
                )))
                .expect("create cf")
            }
        })
        .collect();
    // FRS-WA-V1: declare every churn CF Windowed{ttl = window_rows} (the
    // lifecycle clock is the row SEQ — caller-defined units by contract).
    if args.lifecycle {
        for cf in &cfs {
            db.set_cf_lifecycle(
                cf,
                forst_rs_engine::CfLifecycle::Windowed {
                    ttl: args.window_rows,
                },
            )
            .expect("set lifecycle");
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let logical = Arc::new(AtomicU64::new(0));
    let rows = Arc::new(AtomicU64::new(0));
    let max_seq = Arc::new(AtomicU64::new(0)); // probe upper bound
                                               // FRS-WA-V1: the watermark value last SENT to the engine (verifier reads
                                               // it to pick provably-live keys).
    let wm_sent = Arc::new(AtomicU64::new(0));

    // ---- writer thread (both streams interleaved + TTL deletes) ----
    let writer = {
        let db = Arc::clone(&db);
        let cfs = cfs.clone();
        let stop = Arc::clone(&stop);
        let logical = Arc::clone(&logical);
        let rows = Arc::clone(&rows);
        let max_seq = Arc::clone(&max_seq);
        let wm_sent = Arc::clone(&wm_sent);
        let a = args.clone();
        std::thread::spawn(move || {
            let mut rng = XorShift(0xC0FFEE ^ (run_idx as u64 + 1));
            let mut value = vec![0u8; a.value_bytes];
            let t0 = Instant::now();
            let mut seq: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                // rate control: stay at or below write_rate rows/s
                let due = (t0.elapsed().as_secs_f64() * a.write_rate as f64) as u64;
                if seq >= due {
                    std::thread::sleep(std::time::Duration::from_micros(200));
                    continue;
                }
                // FRS-WA-V1: advance the event-time bound BEFORE the rows it
                // covers (the stamping soundness contract): every 256 rows,
                // bound = seq + 255 covers the upcoming block on every CF.
                if a.lifecycle && seq.is_multiple_of(256) {
                    for cf in &cfs {
                        db.note_cf_max_event_time(cf, seq + 255)
                            .expect("note event time");
                    }
                }
                let stream = if seq.is_multiple_of(2) { b'a' } else { b'b' };
                let bucket = bucket_of(seq, stream, a.buckets);
                // FRS-WA-V3 seq-keys: globally monotone keys ⇒ key-disjoint
                // flushes ⇒ the trivial-move-shaped compaction stream.
                let key = if a.seq_keys {
                    format!("s{seq:015}").into_bytes()
                } else {
                    make_key(stream, bucket, seq)
                };
                // FRS-M5: compressible (NexMark-shaped) vs the default
                // random/incompressible fill. The random fill makes ANY codec
                // a no-op — the compressible mode is the only one that can
                // measure the compression-parity lever.
                if a.compressible {
                    fill_compressible(&mut value, seq, bucket);
                } else {
                    for chunk in value.chunks_mut(8) {
                        let w = rng.next().to_le_bytes();
                        chunk.copy_from_slice(&w[..chunk.len()]);
                    }
                }
                let cf = &cfs[(bucket % cfs.len() as u64) as usize];
                db.put(cf, &key, &value).expect("put");
                logical.fetch_add((key.len() + a.value_bytes) as u64, Ordering::Relaxed);
                // TTL-ish delete: expire the row that fell out of the window.
                // FRS-WA-V1 lifecycle mode: NO per-key deletes — expiry is
                // whole-segment drop at the watermark (= the write head seq).
                if a.lifecycle {
                    if seq.is_multiple_of(1024) {
                        for cf in &cfs {
                            db.advance_cf_watermark(cf, seq).expect("advance wm");
                        }
                        wm_sent.store(seq, Ordering::Relaxed);
                    }
                } else if !a.no_deletes && seq >= a.window_rows {
                    let old = seq - a.window_rows;
                    let old_stream = if old.is_multiple_of(2) { b'a' } else { b'b' };
                    let old_bucket = bucket_of(old, old_stream, a.buckets);
                    let old_key = make_key(old_stream, old_bucket, old);
                    let old_cf = &cfs[(old_bucket % cfs.len() as u64) as usize];
                    db.delete(old_cf, &old_key).expect("delete");
                    logical.fetch_add(old_key.len() as u64, Ordering::Relaxed);
                }
                seq += 1;
                rows.fetch_add(1, Ordering::Relaxed);
                max_seq.store(seq, Ordering::Relaxed);
            }
        })
    };

    // ---- FRS-WA-V1 premature-drop verifier (--lifecycle only) ----
    // Continuously point-gets keys that are PROVABLY live under the
    // watermark contract: key seq t is dead only once wm > t + window, so
    // any t ≥ wm_sent - window + margin must be present. A miss = the
    // correctness falsifier firing (premature whole-segment drop).
    let verify_checks = Arc::new(AtomicU64::new(0));
    let premature_miss = Arc::new(AtomicU64::new(0));
    let verifier = args.lifecycle.then(|| {
        let db = Arc::clone(&db);
        let cfs = cfs.clone();
        let stop = Arc::clone(&stop);
        let max_seq = Arc::clone(&max_seq);
        let wm_sent = Arc::clone(&wm_sent);
        let checks = Arc::clone(&verify_checks);
        let misses = Arc::clone(&premature_miss);
        let a = args.clone();
        std::thread::spawn(move || {
            let mut rng = XorShift(0x5EED ^ (run_idx as u64 + 1));
            // Safety margin over the wm-advance cadence (1024 rows) so a
            // concurrent advance can never race a just-checked key over the
            // boundary: ~0.5 s of rows at the target rate.
            let margin = (a.write_rate / 2).max(8 * 1024);
            while !stop.load(Ordering::Relaxed) {
                let head = max_seq.load(Ordering::Relaxed);
                let wm = wm_sent.load(Ordering::Relaxed);
                let lo = (wm.saturating_sub(a.window_rows)).saturating_add(margin);
                let hi = head.saturating_sub(1);
                if hi <= lo {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    continue;
                }
                let t = lo + rng.next() % (hi - lo);
                let stream = if t.is_multiple_of(2) { b'a' } else { b'b' };
                let bucket = bucket_of(t, stream, a.buckets);
                let key = make_key(stream, bucket, t);
                let cf = &cfs[(bucket % cfs.len() as u64) as usize];
                match db.get(cf, &key) {
                    Ok(Some(_)) => {
                        checks.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => {
                        checks.fetch_add(1, Ordering::Relaxed);
                        misses.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "[FALSIFIER] premature drop: live key seq={t} missing \
                             (wm_sent={wm}, head={head}, window={})",
                            a.window_rows
                        );
                    }
                    Err(e) => panic!("verifier get failed: {e}"),
                }
                // ~2 K checks/s — enough coverage without skewing the probe
                // latency measurement.
                std::thread::sleep(std::time::Duration::from_micros(500));
            }
        })
    });

    // ---- probe threads (q7 probe shape: prefix_scan over a join bucket) ----
    let window = Arc::new(ProbeWindow::default());
    let probers: Vec<_> = (0..args.probe_threads)
        .map(|p| {
            let db = Arc::clone(&db);
            let cfs = cfs.clone();
            let stop = Arc::clone(&stop);
            let win = Arc::clone(&window);
            let max_seq_p = Arc::clone(&max_seq);
            let a = args.clone();
            std::thread::spawn(move || {
                let mut rng = XorShift(0xBADF00D ^ ((p as u64 + 1) << 32));
                let mut drained: u64 = 0;
                while !stop.load(Ordering::Relaxed) {
                    let bucket = rng.next() % a.buckets;
                    // probe the OPPOSITE stream of the bucket's parity — both
                    // get probed over time (interval join probes both sides)
                    let stream = if rng.next().is_multiple_of(2) {
                        b'a'
                    } else {
                        b'b'
                    };
                    // FRS-WA-V3 seq-keys: probe a random recent 1000-key
                    // block (the same ~1K-row probe shape as the q7 cell).
                    let prefix = if a.seq_keys {
                        let head = max_seq_p.load(Ordering::Relaxed).max(1);
                        let t = rng.next() % head;
                        format!("s{:012}", t / 1000).into_bytes()
                    } else {
                        make_prefix(stream, bucket)
                    };
                    let cf = &cfs[(bucket % cfs.len() as u64) as usize];
                    let t = Instant::now();
                    let entries = db.prefix_scan(cf, &prefix).expect("probe");
                    win.record(t.elapsed().as_nanos() as u64);
                    drained += entries.len() as u64;
                }
                drained
            })
        })
        .collect();

    // ---- optional leveled-emulation sweeper ----
    let sweeper = (args.compact_every_s > 0).then(|| {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let every = args.compact_every_s;
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                for _ in 0..(every * 10) {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(100));
                }
                let t = Instant::now();
                db.compact_all().expect("compact_all");
                eprintln!("[sweep] compact_all took {:.1}s", t.elapsed().as_secs_f64());
            }
        })
    });

    // ---- sampler (main thread) ----
    let t0 = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    while t0.elapsed().as_secs() < args.duration_s {
        std::thread::sleep(std::time::Duration::from_secs(args.sample_s));
        let live = db.list_live_files(false).expect("live files");
        // FRS-WA-V2b: on-disk vlog footprint (kvsep cells; 0 otherwise).
        let vlog_bytes: u64 = std::fs::read_dir(&db_path)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("vlog"))
                    .filter_map(|e| e.metadata().ok().map(|m| m.len()))
                    .sum()
            })
            .unwrap_or(0);
        let max_level = live.iter().map(|f| f.level).max().unwrap_or(0) as usize;
        let mut per_level = vec![0usize; max_level + 1];
        for f in &live {
            per_level[f.level as usize] += 1;
        }
        let (n, p50, p99) = window.drain();
        let s = Sample {
            t_s: t0.elapsed().as_secs_f64(),
            l0: *per_level.first().unwrap_or(&0),
            files_per_level: per_level,
            live_bytes: live.iter().map(|f| f.size).sum(),
            vlog_bytes,
            phys_write_bytes: written.load(Ordering::Relaxed),
            logical_bytes: logical.load(Ordering::Relaxed),
            rows_written: rows.load(Ordering::Relaxed),
            probe_n: n,
            probe_p50_us: p50,
            probe_p99_us: p99,
        };
        println!(
            "{{\"sample\":1,\"label\":\"{}\",\"run\":{},\"t_s\":{:.0},\"files_per_level\":{:?},\
             \"live_mib\":{:.0},\"vlog_mib\":{:.0},\"phys_mib\":{:.0},\"logical_mib\":{:.0},\"wamp\":{:.2},\
             \"rows\":{},\"probe_n\":{},\"probe_p50_us\":{:.0},\"probe_p99_us\":{:.0}}}",
            args.label,
            run_idx,
            s.t_s,
            s.files_per_level,
            s.live_bytes as f64 / 1048576.0,
            s.vlog_bytes as f64 / 1048576.0,
            s.phys_write_bytes as f64 / 1048576.0,
            s.logical_bytes as f64 / 1048576.0,
            s.phys_write_bytes as f64 / s.logical_bytes.max(1) as f64,
            s.rows_written,
            s.probe_n,
            s.probe_p50_us,
            s.probe_p99_us,
        );
        samples.push(s);
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer join");
    for p in probers {
        let _ = p.join().expect("prober join");
    }
    if let Some(h) = sweeper {
        h.join().expect("sweeper join");
    }
    if let Some(h) = verifier {
        h.join().expect("verifier join");
    }

    // early = samples [1..=2] (skip warmup sample 0), late = last 2
    let pick = |sl: &[Sample], f: fn(&Sample) -> f64| -> f64 {
        if sl.is_empty() {
            return 0.0;
        }
        sl.iter().map(f).sum::<f64>() / sl.len() as f64
    };
    let n = samples.len();
    let early = &samples[1.min(n.saturating_sub(1))..3.min(n)];
    let late = &samples[n.saturating_sub(2)..];
    let last = samples.last().cloned().unwrap_or_default();
    let summary = RunSummary {
        write_amp: last.phys_write_bytes as f64 / last.logical_bytes.max(1) as f64,
        last_vlog_mib: last.vlog_bytes as f64 / 1048576.0,
        rows_written: last.rows_written,
        logical_mib: last.logical_bytes as f64 / 1048576.0,
        phys_mib: last.phys_write_bytes as f64 / 1048576.0,
        probe_p50_early_us: pick(early, |s| s.probe_p50_us),
        probe_p50_late_us: pick(late, |s| s.probe_p50_us),
        probe_p99_early_us: pick(early, |s| s.probe_p99_us),
        probe_p99_late_us: pick(late, |s| s.probe_p99_us),
        max_l0: samples.iter().map(|s| s.l0).max().unwrap_or(0),
        last_l0: last.l0,
        max_total_files: samples
            .iter()
            .map(|s| s.files_per_level.iter().sum::<usize>())
            .max()
            .unwrap_or(0),
        achieved_write_rate: last.rows_written as f64 / last.t_s.max(0.001),
        verify_checks: verify_checks.load(Ordering::Relaxed),
        premature_miss: premature_miss.load(Ordering::Relaxed),
    };
    drop(db);
    let _ = std::fs::remove_dir_all(&db_path);
    summary
}

// ---------------------------------------------------------------------------
// RocksDB comparison cell (feature `rocksdb-baseline`): IDENTICAL workload
// against leveled RocksDB configured to match the frs EngineOptions defaults
// (64 MB write buffer, 3 buffers, 64 MB target file, 256 MB level base,
// mult 10, LZ4, WAL off — frs puts have no WAL). Physical write bytes come
// from the engine's own tickers (`rocksdb.flush.write.bytes` +
// `rocksdb.compact.write.bytes`), the exact counterpart of the counting-FS
// wrapper on the frs side.
// ---------------------------------------------------------------------------
#[cfg(feature = "rocksdb-baseline")]
fn one_run_rocksdb(args: &Args, run_idx: usize, workroot: &Path) -> RunSummary {
    use rocksdb::{IteratorMode, Options, WriteOptions, DB};

    let db_path = workroot.join(format!("rdb-run{run_idx}"));
    std::fs::create_dir_all(&db_path).expect("create run dir");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(64 * 1024 * 1024);
    opts.set_max_write_buffer_number(3);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_max_bytes_for_level_base(256 * 1024 * 1024);
    opts.set_max_bytes_for_level_multiplier(10.0);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.enable_statistics();
    let db = Arc::new(DB::open(&opts, &db_path).expect("open rocksdb"));

    let stop = Arc::new(AtomicBool::new(false));
    let logical = Arc::new(AtomicU64::new(0));
    let rows = Arc::new(AtomicU64::new(0));

    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        let logical = Arc::clone(&logical);
        let rows = Arc::clone(&rows);
        let a = args.clone();
        std::thread::spawn(move || {
            let mut rng = XorShift(0xC0FFEE ^ (run_idx as u64 + 1));
            let mut value = vec![0u8; a.value_bytes];
            let mut wo = WriteOptions::default();
            wo.disable_wal(true);
            let t0 = Instant::now();
            let mut seq: u64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let due = (t0.elapsed().as_secs_f64() * a.write_rate as f64) as u64;
                if seq >= due {
                    std::thread::sleep(std::time::Duration::from_micros(200));
                    continue;
                }
                let stream = if seq % 2 == 0 { b'a' } else { b'b' };
                let bucket = bucket_of(seq, stream, a.buckets);
                let key = make_key(stream, bucket, seq);
                for chunk in value.chunks_mut(8) {
                    let w = rng.next().to_le_bytes();
                    chunk.copy_from_slice(&w[..chunk.len()]);
                }
                db.put_opt(&key, &value, &wo).expect("put");
                logical.fetch_add((key.len() + a.value_bytes) as u64, Ordering::Relaxed);
                if !a.no_deletes && seq >= a.window_rows {
                    let old = seq - a.window_rows;
                    let old_stream = if old % 2 == 0 { b'a' } else { b'b' };
                    let old_key = make_key(old_stream, bucket_of(old, old_stream, a.buckets), old);
                    db.delete_opt(&old_key, &wo).expect("delete");
                    logical.fetch_add(old_key.len() as u64, Ordering::Relaxed);
                }
                seq += 1;
                rows.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    let window = Arc::new(ProbeWindow::default());
    let probers: Vec<_> = (0..args.probe_threads)
        .map(|p| {
            let db = Arc::clone(&db);
            let stop = Arc::clone(&stop);
            let win = Arc::clone(&window);
            let a = args.clone();
            std::thread::spawn(move || {
                let mut rng = XorShift(0xBADF00D ^ ((p as u64 + 1) << 32));
                let mut drained: u64 = 0;
                while !stop.load(Ordering::Relaxed) {
                    let bucket = rng.next() % a.buckets;
                    let stream = if rng.next() % 2 == 0 { b'a' } else { b'b' };
                    let prefix = make_prefix(stream, bucket);
                    let t = Instant::now();
                    let iter =
                        db.iterator(IteratorMode::From(&prefix, rocksdb::Direction::Forward));
                    for item in iter {
                        let (k, _v) = item.expect("iter");
                        if !k.starts_with(&prefix) {
                            break;
                        }
                        drained += 1;
                    }
                    win.record(t.elapsed().as_nanos() as u64);
                }
                drained
            })
        })
        .collect();

    // statistics tickers: "rocksdb.flush.write.bytes COUNT : N"
    let phys_bytes = |opts: &Options| -> u64 {
        let stats = opts.get_statistics().unwrap_or_default();
        let mut total = 0u64;
        for line in stats.lines() {
            if line.starts_with("rocksdb.flush.write.bytes")
                || line.starts_with("rocksdb.compact.write.bytes")
            {
                if let Some(n) = line.split(':').nth(1) {
                    total += n.trim().parse::<u64>().unwrap_or(0);
                }
            }
        }
        total
    };

    let t0 = Instant::now();
    let mut samples: Vec<Sample> = Vec::new();
    while t0.elapsed().as_secs() < args.duration_s {
        std::thread::sleep(std::time::Duration::from_secs(args.sample_s));
        let mut per_level = Vec::new();
        for lvl in 0..7 {
            let n = db
                .property_int_value(&format!("rocksdb.num-files-at-level{lvl}"))
                .ok()
                .flatten()
                .unwrap_or(0) as usize;
            per_level.push(n);
        }
        while per_level.len() > 1 && *per_level.last().unwrap() == 0 {
            per_level.pop();
        }
        let live_bytes = db
            .property_int_value("rocksdb.total-sst-files-size")
            .ok()
            .flatten()
            .unwrap_or(0);
        let (n, p50, p99) = window.drain();
        let s = Sample {
            t_s: t0.elapsed().as_secs_f64(),
            l0: *per_level.first().unwrap_or(&0),
            files_per_level: per_level,
            live_bytes,
            // RocksDB-baseline arm has no vlog segments (KV-separation is a
            // forst-rs engine feature); report 0 so the Sample initializer is
            // complete under --features rocksdb-baseline.
            vlog_bytes: 0,
            phys_write_bytes: phys_bytes(&opts),
            logical_bytes: logical.load(Ordering::Relaxed),
            rows_written: rows.load(Ordering::Relaxed),
            probe_n: n,
            probe_p50_us: p50,
            probe_p99_us: p99,
        };
        println!(
            "{{\"sample\":1,\"label\":\"{}\",\"run\":{},\"t_s\":{:.0},\"files_per_level\":{:?},\
             \"live_mib\":{:.0},\"phys_mib\":{:.0},\"logical_mib\":{:.0},\"wamp\":{:.2},\
             \"rows\":{},\"probe_n\":{},\"probe_p50_us\":{:.0},\"probe_p99_us\":{:.0}}}",
            args.label,
            run_idx,
            s.t_s,
            s.files_per_level,
            s.live_bytes as f64 / 1048576.0,
            s.phys_write_bytes as f64 / 1048576.0,
            s.logical_bytes as f64 / 1048576.0,
            s.phys_write_bytes as f64 / s.logical_bytes.max(1) as f64,
            s.rows_written,
            s.probe_n,
            s.probe_p50_us,
            s.probe_p99_us,
        );
        samples.push(s);
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer join");
    for p in probers {
        let _ = p.join().expect("prober join");
    }

    let pick = |sl: &[Sample], f: fn(&Sample) -> f64| -> f64 {
        if sl.is_empty() {
            return 0.0;
        }
        sl.iter().map(f).sum::<f64>() / sl.len() as f64
    };
    let n = samples.len();
    let early = &samples[1.min(n.saturating_sub(1))..3.min(n)];
    let late = &samples[n.saturating_sub(2)..];
    let last = samples.last().cloned().unwrap_or_default();
    let summary = RunSummary {
        write_amp: last.phys_write_bytes as f64 / last.logical_bytes.max(1) as f64,
        last_vlog_mib: 0.0,
        rows_written: last.rows_written,
        logical_mib: last.logical_bytes as f64 / 1048576.0,
        phys_mib: last.phys_write_bytes as f64 / 1048576.0,
        probe_p50_early_us: pick(early, |s| s.probe_p50_us),
        probe_p50_late_us: pick(late, |s| s.probe_p50_us),
        probe_p99_early_us: pick(early, |s| s.probe_p99_us),
        probe_p99_late_us: pick(late, |s| s.probe_p99_us),
        max_l0: samples.iter().map(|s| s.l0).max().unwrap_or(0),
        last_l0: last.l0,
        max_total_files: samples
            .iter()
            .map(|s| s.files_per_level.iter().sum::<usize>())
            .max()
            .unwrap_or(0),
        achieved_write_rate: last.rows_written as f64 / last.t_s.max(0.001),
        verify_checks: 0,
        premature_miss: 0,
    };
    drop(db);
    let _ = std::fs::remove_dir_all(&db_path);
    summary
}

#[cfg(not(feature = "rocksdb-baseline"))]
fn one_run_rocksdb(_args: &Args, _run_idx: usize, _workroot: &Path) -> RunSummary {
    panic!("--engine rocksdb requires --features rocksdb-baseline");
}

/// FRS-AKV (adaptive KV-sep, 2026-06-14): the 3-SHAPE adaptive mini-bench —
/// the V3-confirm gate (contention-robust, NOT NexMark). Drives THREE
/// workload shapes through a real engine under ONE uniform config and reports
/// whether the adaptive mechanism (Layer A size gate + B1 pressure back-off +
/// B2 GC) produces the per-shape behaviour the design predicts:
///
/// - q7/q19 FIFO      → FULL separation, resident bounded, write-amp low
/// - q9     scattered → backs off under pressure, resident ≤ budget (no OOM)
/// - q11/17 small     → stays INLINE (no vlog writes)
///
/// All under the SAME config — the per-shape difference is the engine sensing
/// value size / reclaim rate / resident pressure at runtime (no per-query flag).
fn run_akv_shapes(args: &Args) {
    use forst_rs_engine::{ColumnFamilyDescriptor, DbImpl};

    let budget_mib = args.akv_budget_mib;
    // ONE uniform config for all three shapes (the deliverable contract).
    // Default threshold is already 256 (FRS-AKV Layer A). Force the adaptive
    // flags ON via the test-safe overrides (default-OFF everywhere else).
    std::env::set_var("FRS_KV_MIN_BLOB_SIZE", "256");
    forst_rs_engine::set_kv_separation_override(Some(true));
    forst_rs_engine::set_vlog_resident_budget_mb_override(Some(budget_mib));
    forst_rs_engine::set_kv_adaptive_pressure_override(Some(true));
    forst_rs_engine::set_vlog_gc_adaptive_override(Some(true));

    let charge = forst_rs_storage::vlog::VLOG_READER_CHARGE_BYTES as u64;
    let budget_bytes = budget_mib * 1024 * 1024;
    // Size each shape so it OPENS far more segments than budget/charge — i.e.
    // it would blow O(segments) resident if the byte bound did not hold.
    let target_segments = (budget_bytes / charge) * 4 + 40;

    println!("=== FRS-AKV 3-shape adaptive mini-bench (ONE uniform config) ===");
    println!(
        "config: FRS_KV_MIN_BLOB_SIZE=256 FRS_VLOG_RESIDENT_BUDGET_MB={budget_mib} \
         FRS_KV_ADAPTIVE_PRESSURE=1 FRS_VLOG_GC_ADAPTIVE=1  (charge/reader={} KiB, \
         budget/charge={} readers, target_segments/shape={})",
        charge / 1024,
        budget_bytes / charge,
        target_segments,
    );

    #[derive(Debug)]
    struct ShapeResult {
        shape: &'static str,
        value_bytes: usize,
        logical_bytes: u64,
        phys_bytes: u64,
        separated_segments_seen: u64,
        inline_flushes: u64,
        resident_bytes_peak: usize,
        live_segments_peak: usize,
    }

    // Run one shape in its own engine (CountingFs measures physical bytes).
    let run_shape = |shape: &'static str, value_bytes: usize, scattered: bool| -> ShapeResult {
        let workroot = std::path::PathBuf::from("target").join(format!(
            "akv_shapes_{}_{}",
            shape,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&workroot);
        std::fs::create_dir_all(&workroot).expect("mkdir");
        let written = Arc::new(AtomicU64::new(0));
        let fs: Arc<dyn FileSystem> = Arc::new(CountingFs {
            inner: LocalFileSystem,
            written: Arc::clone(&written),
        });
        let opts = EngineOptions {
            db_path: workroot.to_string_lossy().into_owned(),
            // Config MATCHES ForSt: noflush=false, wbuf 1 GiB (the uniform
            // production config). Small here so flushes actually fire in the
            // mini-bench timeframe while staying append-shaped.
            write_buffer_size: 4 * 1024 * 1024,
            ..EngineOptions::default()
        };
        let db = DbImpl::open_with_fs(opts, fs).expect("open");
        let cf = db
            .create_column_family(ColumnFamilyDescriptor::new(shape))
            .expect("cf");

        let mut value = vec![0u8; value_bytes];
        let mut logical = 0u64;
        let mut separated_seen = 0u64;
        let mut inline_flushes = 0u64;
        let mut resident_peak = 0usize;
        let mut segments_peak = 0usize;
        let mut prev_segments = 0usize;

        // Key universe: FIFO = monotone fresh keys (each flush a disjoint
        // range, segments die in arrival order). Scattered = small recycled
        // key space overwritten in a NON-arrival order so segments accumulate
        // mixed-lifetime pointers and never reach live_bytes==0 (the q9
        // signature) → resident climbs → budget → back-off.
        let scattered_universe: u64 = 4096;
        let flushes = target_segments + 20;
        for f in 0..flushes {
            // ~256 KiB of rows per flush (fills the 4 MiB wbuf in ~16 flushes;
            // we drive many flushes so resident would blow without the bound).
            let rows_per_flush = (256 * 1024 / value_bytes.max(1)).max(1);
            for r in 0..rows_per_flush {
                let seq = f * rows_per_flush as u64 + r as u64;
                let key = if scattered {
                    // Scattered overwrite: hash the seq into a small recycled
                    // space so deaths do NOT follow arrival order.
                    let h = (seq.wrapping_mul(0x9E3779B97F4A7C15)) % scattered_universe;
                    format!("k{h:06}")
                } else {
                    // FIFO: globally monotone (disjoint per-flush ranges).
                    format!("k{seq:012}")
                };
                value[0] = seq as u8;
                if value_bytes > 1 {
                    value[value_bytes - 1] = (seq >> 8) as u8;
                }
                db.put(&cf, key.as_bytes(), &value).expect("put");
                logical += (key.len() + value_bytes) as u64;
            }
            let segs_before = db.vlog_live_segment_count();
            db.switch_and_flush(&cf).expect("flush");
            let segs_after = db.vlog_live_segment_count();
            if segs_after > segs_before {
                separated_seen += 1;
            } else if value_bytes >= 256 {
                // An eligible-by-size flush that produced NO new segment = a
                // back-off (inline) flush (Layer B1) OR all-shadowed reclaim.
                inline_flushes += 1;
            }
            // Deref to materialise vlog readers (resident charge). FIFO: one
            // recent key (narrow working set → low resident). Scattered (q9):
            // probe a WIDE set of keys spread across the recycled universe so
            // their pointers hit MANY distinct live segments at once — the q9
            // wide-live-segment-set read pattern that drives resident up to
            // the budget and triggers the adaptive back-off.
            if scattered {
                for p in 0..512u64 {
                    let h = (p.wrapping_mul(0x9E3779B1)) % scattered_universe;
                    let _ = db.get(&cf, format!("k{h:06}").as_bytes());
                }
            } else {
                let probe_key = format!("k{:012}", f * rows_per_flush as u64);
                let _ = db.get(&cf, probe_key.as_bytes());
            }
            // FIFO: expire the OLDEST flush's range so segments die whole
            // (the q7 TTL/whole-segment drop → reclaim → full separation).
            if !scattered && f >= 16 {
                let old_base = (f - 16) * rows_per_flush as u64;
                for r in 0..rows_per_flush {
                    let old_key = format!("k{:012}", old_base + r as u64);
                    db.delete(&cf, old_key.as_bytes()).expect("delete");
                }
                if f % 4 == 0 {
                    db.compact_all().expect("compact"); // drive reclaim
                }
            }
            resident_peak = resident_peak.max(db.vlog_resident_bytes());
            segments_peak = segments_peak.max(db.vlog_live_segment_count());
            prev_segments = segs_after;
        }
        let _ = prev_segments;
        let phys = written.load(Ordering::Relaxed);
        drop(db);
        let _ = std::fs::remove_dir_all(&workroot);
        ShapeResult {
            shape,
            value_bytes,
            logical_bytes: logical,
            phys_bytes: phys,
            separated_segments_seen: separated_seen,
            inline_flushes,
            resident_bytes_peak: resident_peak,
            live_segments_peak: segments_peak,
        }
    };

    let fifo = run_shape("q7q19-FIFO ", 512, false);
    let scattered = run_shape("q9-scatter ", 512, true);
    let small = run_shape("q11q17-tiny", 64, false);

    let human = |b: u64| -> String {
        if b >= 1 << 20 {
            format!("{:.1} MiB", b as f64 / (1u64 << 20) as f64)
        } else {
            format!("{:.1} KiB", b as f64 / 1024.0)
        }
    };
    let report = |r: &ShapeResult| {
        let wa = r.phys_bytes as f64 / r.logical_bytes.max(1) as f64;
        println!(
            "  {:<11} val={:>4}B  write_amp={:>5.2}x  separated_flushes={:>4}  \
             inline/backoff_flushes={:>4}  resident_peak={:>9}  segments_peak={:>4}  (budget={})",
            r.shape,
            r.value_bytes,
            wa,
            r.separated_segments_seen,
            r.inline_flushes,
            human(r.resident_bytes_peak as u64),
            r.live_segments_peak,
            human(budget_bytes),
        );
    };
    println!("--- results (ONE config; behaviour differs by RUNTIME sensing) ---");
    report(&fifo);
    report(&scattered);
    report(&small);

    // VERDICTS (the gate). Restore overrides first so a panic does not leak.
    forst_rs_engine::set_kv_separation_override(None);
    forst_rs_engine::set_vlog_resident_budget_mb_override(None);
    forst_rs_engine::set_kv_adaptive_pressure_override(None);
    forst_rs_engine::set_vlog_gc_adaptive_override(None);

    // (1) FIFO fully separates AND resident stays bounded by the budget.
    assert!(
        fifo.separated_segments_seen > 0,
        "FIFO shape must SEPARATE (large values, full separation)"
    );
    assert!(
        fifo.resident_bytes_peak as u64 <= budget_bytes,
        "FIFO resident_peak {} must stay <= budget {}",
        fifo.resident_bytes_peak,
        budget_bytes
    );
    // (2) Scattered-death backs off: resident ≤ budget (the OOM regime closed)
    //     AND it produced inline/back-off flushes (did NOT separate forever).
    assert!(
        scattered.resident_bytes_peak as u64 <= budget_bytes,
        "SCATTERED resident_peak {} must stay <= budget {} (q9 never-OOM)",
        scattered.resident_bytes_peak,
        budget_bytes
    );
    assert!(
        scattered.inline_flushes > 0,
        "SCATTERED shape must BACK OFF (some flushes inline) under pressure"
    );
    // (3) Small values stay inline → NO vlog segments ever.
    assert_eq!(
        small.separated_segments_seen, 0,
        "SMALL (q11/q17) values must stay INLINE (no vlog writes)"
    );
    assert_eq!(
        small.live_segments_peak, 0,
        "SMALL shape must never create a vlog segment"
    );
    println!(
        "VERDICT OK: FIFO fully-separates+bounded; scattered-death backs-off (resident <= budget, \
         no unbounded growth); small-value stays inline — ALL under ONE config."
    );
}

fn main() {
    let args = Args::parse();
    if args.akv_shapes {
        run_akv_shapes(&args);
        return;
    }
    eprintln!(
        "churn_probe: {:?} env: FRS_L0_COMPACTION_TRIGGER={:?} FRS_L0_SLOWDOWN_TRIGGER={:?} \
         FRS_L0_STOP_TRIGGER={:?} FRS_BULK_SAMPLE={:?}",
        args,
        std::env::var("FRS_L0_COMPACTION_TRIGGER").ok(),
        std::env::var("FRS_L0_SLOWDOWN_TRIGGER").ok(),
        std::env::var("FRS_L0_STOP_TRIGGER").ok(),
        std::env::var("FRS_BULK_SAMPLE").ok(),
    );
    let workroot = std::path::PathBuf::from("target")
        .join(format!("churn_probe_bench_{}", std::process::id()));
    std::fs::create_dir_all(&workroot).expect("create workroot");

    let mut summaries = Vec::with_capacity(args.runs);
    for run_idx in 0..args.runs {
        summaries.push(if args.engine == "rocksdb" {
            one_run_rocksdb(&args, run_idx, &workroot)
        } else {
            one_run(&args, run_idx, &workroot)
        });
    }
    let _ = std::fs::remove_dir_all(&workroot);

    let median = |mut v: Vec<f64>| -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    for (i, s) in summaries.iter().enumerate() {
        println!(
            "{{\"summary\":1,\"label\":\"{}\",\"run\":{},\"write_amp\":{:.2},\"rows\":{},\
             \"logical_mib\":{:.0},\"phys_mib\":{:.0},\"achieved_rows_per_s\":{:.0},\
             \"probe_p50_early_us\":{:.0},\"probe_p50_late_us\":{:.0},\
             \"probe_p99_early_us\":{:.0},\"probe_p99_late_us\":{:.0},\
             \"max_l0\":{},\"last_l0\":{},\"max_total_files\":{},\
             \"verify_checks\":{},\"premature_miss\":{}}}",
            args.label,
            i,
            s.write_amp,
            s.rows_written,
            s.logical_mib,
            s.phys_mib,
            s.achieved_write_rate,
            s.probe_p50_early_us,
            s.probe_p50_late_us,
            s.probe_p99_early_us,
            s.probe_p99_late_us,
            s.max_l0,
            s.last_l0,
            s.max_total_files,
            s.verify_checks,
            s.premature_miss,
        );
    }
    println!(
        "MEDIANS label={} write_amp={:.2} p50_late_us={:.0} p99_late_us={:.0} \
         p50_degradation={:.2}x last_l0={} vlog_mib={:.0} (n={})",
        args.label,
        median(summaries.iter().map(|s| s.write_amp).collect()),
        median(summaries.iter().map(|s| s.probe_p50_late_us).collect()),
        median(summaries.iter().map(|s| s.probe_p99_late_us).collect()),
        median(
            summaries
                .iter()
                .map(|s| s.probe_p50_late_us / s.probe_p50_early_us.max(1.0))
                .collect()
        ),
        median(summaries.iter().map(|s| s.last_l0 as f64).collect()) as u64,
        median(summaries.iter().map(|s| s.last_vlog_mib).collect()),
        summaries.len()
    );
    if args.lifecycle {
        let checks: u64 = summaries.iter().map(|s| s.verify_checks).sum();
        let misses: u64 = summaries.iter().map(|s| s.premature_miss).sum();
        println!("LIFECYCLE-VERIFY checks={checks} premature_miss={misses}");
        assert!(checks > 0, "lifecycle verifier must have run");
        assert_eq!(
            misses, 0,
            "PREMATURE-DROP FALSIFIER FIRED: {misses} live keys missing"
        );
    }
    if args.smoke {
        assert!(summaries[0].rows_written > 0, "smoke: must write rows");
        println!("SMOKE OK");
    }
}

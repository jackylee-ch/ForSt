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
    phys_write_bytes: u64,
    logical_bytes: u64,
    rows_written: u64,
    probe_n: usize,
    probe_p50_us: f64,
    probe_p99_us: f64,
}

struct RunSummary {
    write_amp: f64,
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
                let key = make_key(stream, bucket, seq);
                for chunk in value.chunks_mut(8) {
                    let w = rng.next().to_le_bytes();
                    chunk.copy_from_slice(&w[..chunk.len()]);
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
                    let prefix = make_prefix(stream, bucket);
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
            phys_write_bytes: written.load(Ordering::Relaxed),
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

fn main() {
    let args = Args::parse();
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
         p50_degradation={:.2}x last_l0={} (n={})",
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

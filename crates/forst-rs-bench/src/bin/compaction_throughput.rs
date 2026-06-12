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

//! B3 `compaction_throughput` (2026-06-12 local FFI/flush/compaction bench
//! design §B3): repeatable reproduction target for the recorded
//! 3 ns/byte-isolated vs 21 ns/byte-live compaction merge numbers, and the
//! before/after harness for the L4 windowed compaction read path
//! (`FRS_COMPACT_WINDOWED=1` — set it in the LAUNCHING environment; the
//! engine reads it once per process).
//!
//! Build phase: writes + `switch_and_flush` until L0 holds `--l0-files` SSTs
//! of ~`--sst-mib` MiB each, with `--overlap-pct` key overlap between
//! adjacent files and `--tombstone-pct` deletes. Measure phase: times
//! `compact_l0` (the streaming k-way merge over every L0 + L1 input) and
//! reports ns/byte-input, MB/s, reclaimed %. `--with-read-load` runs a
//! sidecar point-get thread against the same CF during the compaction (the
//! "live" cell of the 3-vs-21 pair).
//!
//! Methodology rules (binding, design §3): same-session A/B only, n≥3 per
//! cell, never compare across machines. Mac numbers are system-allocator
//! numbers (jemalloc is compile-gated off on macOS) — same-box A/B only.
//!
//! NOT built in v1 (design allows): `--merge-chain` operand cells.
//!
//! ```bash
//! cargo run -p forst-rs-bench --release --bin compaction_throughput -- --smoke
//! FRS_COMPACT_WINDOWED=1 cargo run -p forst-rs-bench --release \
//!     --bin compaction_throughput -- --runs 3
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::EngineOptions;
use forst_rs_engine::DbImpl;
use forst_rs_io::FileSystem;

#[derive(Clone, Debug)]
struct Args {
    l0_files: usize,
    sst_mib: usize,
    value_bytes: usize,
    overlap_pct: usize,
    tombstone_pct: usize,
    with_read_load: bool,
    runs: usize,
    smoke: bool,
}

impl Args {
    fn parse() -> Self {
        let mut a = Args {
            l0_files: 8,
            sst_mib: 64,
            value_bytes: 256,
            overlap_pct: 50,
            tombstone_pct: 20,
            with_read_load: false,
            runs: 1,
            smoke: false,
        };
        let argv: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < argv.len() {
            let take = |i: &mut usize| -> usize {
                *i += 1;
                argv.get(*i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or_else(|| panic!("missing/invalid value for {}", argv[*i - 1]))
            };
            match argv[i].as_str() {
                "--l0-files" => a.l0_files = take(&mut i),
                "--sst-mib" => a.sst_mib = take(&mut i),
                "--value-bytes" => a.value_bytes = take(&mut i),
                "--overlap-pct" => a.overlap_pct = take(&mut i),
                "--tombstone-pct" => a.tombstone_pct = take(&mut i),
                "--runs" => a.runs = take(&mut i),
                "--with-read-load" => a.with_read_load = true,
                "--isolated" => a.with_read_load = false,
                "--smoke" => a.smoke = true,
                other => panic!("unknown arg {other}"),
            }
            i += 1;
        }
        if a.smoke {
            a.l0_files = 4;
            a.sst_mib = 2;
            a.runs = 1;
        }
        a
    }
}

/// Deterministic xorshift64* (same pattern as the merge-operator UTs).
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

/// macOS/Linux RSS sample in MiB (B4 fallback sampler: `ps`, phase
/// boundaries only — no jemalloc stats on the Mac, compile-gated off).
fn rss_mib() -> u64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output();
    out.ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|kib| kib / 1024)
        .unwrap_or(0)
}

struct RunResult {
    input_bytes: u64,
    output_bytes: u64,
    wall_ms: f64,
    ns_per_byte: f64,
    sidecar_gets_per_s: f64,
}

fn one_run(args: &Args, run_idx: usize, workroot: &std::path::Path) -> RunResult {
    let db_path = workroot.join(format!("run{run_idx}"));
    std::fs::create_dir_all(&db_path).expect("create run dir");
    let fs: Arc<dyn FileSystem> = Arc::new(forst_rs_io::LocalFileSystem);
    let opts = EngineOptions {
        db_path: db_path.to_string_lossy().into_owned(),
        // Headroom over the per-file logical bytes so the active memtable
        // never auto-switches before our explicit switch_and_flush.
        write_buffer_size: args.sst_mib * 2 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    let cf = db.default_cf();

    // ---- build phase: l0_files SSTs of ~sst_mib MiB, overlapping keys ----
    let key_len = 14usize; // "key_" + 10 digits
    let row_logical = key_len + args.value_bytes;
    let rows_per_file = (args.sst_mib * 1024 * 1024) / row_logical;
    let stride = (rows_per_file * (100 - args.overlap_pct.min(100))) / 100;
    let mut rng = XorShift(0x9E3779B97F4A7C15 ^ (run_idx as u64 + 1));
    let mut value = vec![0u8; args.value_bytes];
    let mut max_key = 0usize;
    let t_build = Instant::now();
    for f in 0..args.l0_files {
        let lo = f * stride.max(1);
        for r in 0..rows_per_file {
            let k = lo + r;
            max_key = max_key.max(k);
            let key = format!("key_{:010}", k);
            if args.tombstone_pct > 0 && (rng.next() % 100) < args.tombstone_pct as u64 {
                db.delete(&cf, key.as_bytes()).expect("delete");
            } else {
                // Pseudorandom filler (xorshift per 8-byte word) so LZ4
                // cannot collapse the values — keeps `--sst-mib` ≈ the real
                // on-disk input bytes the compaction read path must move.
                for chunk in value.chunks_mut(8) {
                    let w = rng.next().to_le_bytes();
                    chunk.copy_from_slice(&w[..chunk.len()]);
                }
                db.put(&cf, key.as_bytes(), &value).expect("put");
            }
        }
        db.switch_and_flush(&cf).expect("flush").expect("nonempty");
    }
    let build_s = t_build.elapsed().as_secs_f64();

    // Design §3 rule 3: the measured phase must actually cycle — assert the
    // build produced the requested L0 fan-in before timing anything.
    let live = db.list_live_files(false).expect("live files");
    let l0: Vec<_> = live.iter().filter(|f| f.level == 0).collect();
    assert_eq!(
        l0.len(),
        args.l0_files,
        "build phase must leave exactly the requested L0 fan-in \
         (auto-compaction interfered? raise FRS_L0_COMPACTION_TRIGGER)"
    );
    let input_bytes: u64 = live.iter().map(|f| f.size).sum();
    eprintln!(
        "[build] run={run_idx} files={} input_mib={:.1} build_s={:.1} rss_mib={}",
        l0.len(),
        input_bytes as f64 / (1024.0 * 1024.0),
        build_s,
        rss_mib()
    );

    // ---- measure phase: time compact_l0 (streaming k-way merge) ----
    let stop = Arc::new(AtomicBool::new(false));
    let gets = Arc::new(AtomicU64::new(0));
    let sidecar = args.with_read_load.then(|| {
        let db2 = Arc::clone(&db);
        let cf2 = cf.clone();
        let stop2 = Arc::clone(&stop);
        let gets2 = Arc::clone(&gets);
        let universe = max_key as u64 + 1;
        std::thread::spawn(move || {
            let mut rng = XorShift(0xDEADBEEFCAFEF00D);
            while !stop2.load(Ordering::Relaxed) {
                let key = format!("key_{:010}", rng.next() % universe);
                let _ = db2.get(&cf2, key.as_bytes());
                gets2.fetch_add(1, Ordering::Relaxed);
            }
        })
    });

    let t0 = Instant::now();
    let out_meta = db.compact_l0(&cf).expect("compact_l0").expect("had L0 input");
    let wall = t0.elapsed();

    stop.store(true, Ordering::Relaxed);
    if let Some(h) = sidecar {
        h.join().expect("sidecar join");
    }
    // `compact_l0` returns only the FIRST output file's meta; a rolled
    // multi-file output is the norm at this input size — sum the surviving
    // live files instead (L0 was fully drained into them).
    let _ = out_meta;
    let output_bytes: u64 = db
        .list_live_files(false)
        .expect("live files post-compaction")
        .iter()
        .map(|f| f.size)
        .sum();
    let wall_s = wall.as_secs_f64();
    let result = RunResult {
        input_bytes,
        output_bytes,
        wall_ms: wall_s * 1e3,
        ns_per_byte: wall.as_nanos() as f64 / input_bytes as f64,
        sidecar_gets_per_s: if args.with_read_load {
            gets.load(Ordering::Relaxed) as f64 / wall_s
        } else {
            0.0
        },
    };
    eprintln!("[measure] run={run_idx} rss_mib={}", rss_mib());
    drop(db);
    let _ = std::fs::remove_dir_all(&db_path);
    result
}

fn main() {
    let args = Args::parse();
    // Keep the background L0 trigger far above the build fan-in so the
    // measured compact_l0 is the ONLY compaction (read before DB open).
    if std::env::var("FRS_L0_COMPACTION_TRIGGER").is_err() {
        // SAFETY-free std API on this single-threaded startup path.
        std::env::set_var("FRS_L0_COMPACTION_TRIGGER", "100000");
    }
    let windowed = matches!(
        std::env::var("FRS_COMPACT_WINDOWED").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    );
    eprintln!(
        "compaction_throughput: {:?} FRS_COMPACT_WINDOWED={} \
         (Mac numbers are system-allocator numbers; same-box A/B only)",
        args, windowed
    );

    let workroot = std::path::PathBuf::from("target").join(format!(
        "compaction_throughput_bench_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&workroot).expect("create workroot");

    let mut results = Vec::with_capacity(args.runs);
    for run_idx in 0..args.runs {
        results.push(one_run(&args, run_idx, &workroot));
    }
    let _ = std::fs::remove_dir_all(&workroot);

    let mut ns: Vec<f64> = results.iter().map(|r| r.ns_per_byte).collect();
    ns.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let median_ns = ns[ns.len() / 2];

    for (i, r) in results.iter().enumerate() {
        println!(
            "{{\"bench\":\"compaction_throughput\",\"run\":{},\"windowed\":{},\
             \"l0_files\":{},\"sst_mib\":{},\"overlap_pct\":{},\"tombstone_pct\":{},\
             \"with_read_load\":{},\"input_bytes\":{},\"output_bytes\":{},\
             \"wall_ms\":{:.1},\"ns_per_byte\":{:.3},\"mb_per_s\":{:.1},\
             \"reclaimed_pct\":{:.1},\"sidecar_gets_per_s\":{:.0}}}",
            i,
            windowed,
            args.l0_files,
            args.sst_mib,
            args.overlap_pct,
            args.tombstone_pct,
            args.with_read_load,
            r.input_bytes,
            r.output_bytes,
            r.wall_ms,
            r.ns_per_byte,
            (r.input_bytes as f64 / (1024.0 * 1024.0)) / (r.wall_ms / 1e3),
            100.0 * (1.0 - r.output_bytes as f64 / r.input_bytes as f64),
            r.sidecar_gets_per_s,
        );
    }
    println!(
        "MEDIAN ns/byte-input = {median_ns:.3} (windowed={windowed}, n={})",
        results.len()
    );
    if args.smoke {
        assert!(results[0].output_bytes > 0, "smoke: output must be non-empty");
        println!("SMOKE OK");
    }
}

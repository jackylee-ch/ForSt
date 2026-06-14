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

//! FRS-WA-V2a-2-LRU mini-bench — **bounded vlog-reader cache resident
//! footprint: uncapped (pre-fix) vs bounded (cap=2048)**.
//!
//! ## The bottleneck this quantifies (q9 KV-sep OOM)
//!
//! Root-cause note `docs/superpowers/specs/2026-06-14-q9-kvsep-oom-rootcause.md`:
//! under `FRS_KV_SEPARATION`, `DbImpl::vlog_readers` cached one open
//! [`VlogReader`] per vlog segment EVER dereferenced — an open file handle plus
//! a 64 KiB chunk buffer each — and the only reclaim path fires when a segment
//! reaches `live_bytes == 0`. q9's multi-way interval JOIN + Rank has scattered
//! (non-FIFO) segment death, and the default `FRS_VLOG_GC_AGE_CUTOFF=0` disables
//! relocation, so segments essentially never reach 0 → the reader set grew
//! monotonically with the run (one per flush segment) and busted the 16 g/TM
//! cgroup. ForSt/RocksDB bound the equivalent via a charged block cache /
//! `max_open_files`; forst-rs KV-sep had no bound.
//!
//! The fix replaces the unbounded `HashMap` with a bounded LRU
//! ([`VlogReaderCache`], cap via `FRS_VLOG_READER_CACHE_CAP`, default 2048).
//! Re-open on a miss is byte-identical (segments are immutable once published),
//! so the bound is free of correctness cost.
//!
//! ## What this measures (REAL, not modeled)
//!
//! It opens `--segments N` distinct vlog segments on a memory FS and
//! dereferences one value from each (the q9 "wide live-segment set" access
//! pattern), under two cache arms:
//!   * **uncapped** (cap = 0, the pre-fix behaviour): keeps every reader.
//!   * **bounded** (cap = 2048): evicts the LRU reader past the cap.
//!
//! Each resident reader's 64 KiB chunk buffer is MATERIALISED by the deref, so
//! the reported numbers are real allocations:
//!   * resident reader count = `cache.len()`
//!   * resident chunk bytes  = `cache.len() × 64 KiB` (the dominant term)
//!   * process RSS delta across the arm (whole-process, includes handles)
//!
//! The headline: resident footprint goes from `O(segments)` (uncapped) to
//! `O(cap)` (bounded) — the bound holds.
//!
//! Run: `cargo run -p forst-rs-bench --release --bin vlog_reader_cache_footprint`
//!      `cargo run -p forst-rs-bench --release --bin vlog_reader_cache_footprint -- --smoke`
//!      `... -- --segments 50000 --cap 2048`

use std::path::Path;
use std::sync::Arc;

use forst_rs_io::{FileSystem, MemoryFileSystem};
use forst_rs_storage::vlog::{
    ValuePointer, VlogReader, VlogReaderCache, VlogWriter, DEFAULT_VLOG_READER_CACHE_CAP,
    VLOG_READER_CHARGE_BYTES,
};

const CHUNK_BYTES: usize = 64 * 1024; // VlogReader's read-ahead granule.

/// Resident process RSS in bytes (best-effort; 0 if unavailable on this OS).
#[cfg(target_os = "linux")]
fn rss_bytes() -> usize {
    // /proc/self/statm: field 2 (resident) is in pages.
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    let resident_pages: usize = s
        .split_whitespace()
        .nth(1)
        .and_then(|p| p.parse().ok())
        .unwrap_or(0);
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    resident_pages.saturating_mul(page)
}

#[cfg(target_os = "macos")]
fn rss_bytes() -> usize {
    // mach_task_basic_info.resident_size (bytes).
    use std::mem;
    #[repr(C)]
    struct MachTaskBasicInfo {
        virtual_size: u64,
        resident_size: u64,
        resident_size_max: u64,
        user_time: [i32; 2],
        system_time: [i32; 2],
        policy: i32,
        suspend_count: i32,
    }
    const MACH_TASK_BASIC_INFO: u32 = 20;
    let count = (mem::size_of::<MachTaskBasicInfo>() / mem::size_of::<i32>()) as u32;
    let mut info: MachTaskBasicInfo = unsafe { mem::zeroed() };
    let mut out_count = count;
    extern "C" {
        fn mach_task_self() -> u32;
        fn task_info(task: u32, flavor: u32, info: *mut i32, count: *mut u32) -> i32;
    }
    let kr = unsafe {
        task_info(
            mach_task_self(),
            MACH_TASK_BASIC_INFO,
            &mut info as *mut _ as *mut i32,
            &mut out_count,
        )
    };
    if kr == 0 {
        info.resident_size as usize
    } else {
        0
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rss_bytes() -> usize {
    0
}

fn human(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * 1024;
    const GB: usize = 1024 * 1024 * 1024;
    if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MiB", bytes as f64 / MB as f64)
    } else {
        format!("{:.1} KiB", bytes as f64 / KB as f64)
    }
}

/// Writes `n` single-value segments and returns their pointers (one value per
/// segment, sized so the deref materialises a full 64 KiB chunk read).
fn write_segments(fs: &dyn FileSystem, dir: &Path, n: u64) -> Vec<(u64, ValuePointer)> {
    let mut out = Vec::with_capacity(n as usize);
    // A value comfortably under one chunk so the deref's miss path fills the
    // 64 KiB buffer (the resident cost we are bounding).
    let value = vec![0x5Au8; 1024];
    for seg in 1..=n {
        let mut w = VlogWriter::create(fs, dir, seg).expect("create segment");
        let p = w.append(&value).expect("append");
        w.sync().expect("sync");
        out.push((seg, p));
    }
    out
}

struct ArmResult {
    label: &'static str,
    cap: usize,
    byte_budget: usize,
    resident_readers: usize,
    resident_chunk_bytes: usize,
    rss_delta: usize,
}

/// Touches every segment once through the cache (the q9 wide-live-set deref
/// pattern), materialising each resident reader's chunk buffer. Bounded by a
/// count `cap` (0 = uncapped) AND a charged `byte_budget` (usize::MAX = none).
fn run_arm(
    label: &'static str,
    cap: usize,
    byte_budget: usize,
    fs: &dyn FileSystem,
    dir: &Path,
    segments: &[(u64, ValuePointer)],
) -> ArmResult {
    let cache = VlogReaderCache::with_capacity_and_budget(cap, byte_budget);
    let rss_before = rss_bytes();
    for (seg, ptr) in segments {
        let s = *seg;
        let reader: Arc<VlogReader> = cache
            .get_or_open(s, || VlogReader::open(fs, dir, s))
            .expect("open vlog reader");
        // Deref to materialise the 64 KiB chunk buffer (the resident cost).
        let _ = reader.get(ptr).expect("deref value");
    }
    let rss_after = rss_bytes();
    let resident_readers = cache.len();
    ArmResult {
        label,
        cap,
        byte_budget,
        resident_readers,
        resident_chunk_bytes: resident_readers.saturating_mul(CHUNK_BYTES),
        rss_delta: rss_after.saturating_sub(rss_before),
    }
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let smoke = args.iter().any(|a| a == "--smoke");

    let cap = arg_value(&args, "--cap")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_VLOG_READER_CACHE_CAP);

    // Default models q9's wide live-segment set. Smoke keeps the run quick and
    // the uncapped arm's real allocation modest.
    let segments = arg_value(&args, "--segments")
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(if smoke { 4_096 } else { 50_000 });

    println!("=== FRS-WA-V2a-2-LRU vlog-reader-cache footprint mini-bench ===");
    println!(
        "segments = {segments}  cap = {cap}  chunk = {} {}",
        human(CHUNK_BYTES),
        if smoke { "(--smoke)" } else { "" }
    );

    let fs = MemoryFileSystem::new();
    let dir = Path::new("/db");
    fs.create_dir_all(dir).expect("mkdir");
    let segs = write_segments(&fs, dir, segments);

    // FRS-AKV-B1: a byte-budget arm (default 64 MiB) — the never-OOM bound the
    // count cap alone could not give. Sweepable via `--budget-mb`.
    let budget_mb = arg_value(&args, "--budget-mb")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(if smoke { 4 } else { 64 });
    let byte_budget = budget_mb.saturating_mul(1024 * 1024);

    // Uncapped arm (cap = 0): the pre-fix behaviour — keeps every reader.
    let unbounded = run_arm("uncapped  (pre-fix)", 0, usize::MAX, &fs, dir, &segs);
    // Count-bounded arm: O(cap) resident readers (the shipped V2a-2-LRU).
    let bounded = run_arm("count-cap (V2a-2)  ", cap, usize::MAX, &fs, dir, &segs);
    // FRS-AKV-B1 byte-budgeted arm: O(budget) resident bytes (count uncapped
    // so the BYTE bound is what holds — the q9 never-OOM mechanism).
    let budgeted = run_arm("byte-budget (AKV)  ", 0, byte_budget, &fs, dir, &segs);

    let report = |r: &ArmResult| {
        println!(
            "  {}: cap={:<6} budget={:<9} resident_readers={:<7} chunk_bytes={:<10} rss_delta={}",
            r.label,
            if r.cap == 0 {
                "∞".to_string()
            } else {
                r.cap.to_string()
            },
            if r.byte_budget == usize::MAX {
                "∞".to_string()
            } else {
                human(r.byte_budget)
            },
            r.resident_readers,
            human(r.resident_chunk_bytes),
            human(r.rss_delta),
        );
    };
    println!("--- results ({segments} segments dereferenced) ---");
    report(&unbounded);
    report(&bounded);
    report(&budgeted);

    let ratio = if bounded.resident_chunk_bytes == 0 {
        f64::INFINITY
    } else {
        unbounded.resident_chunk_bytes as f64 / bounded.resident_chunk_bytes as f64
    };
    println!(
        "--- VERDICT: resident vlog-reader footprint {} -> {} ({:.1}x smaller); \
         bound is O(cap) not O(segments) ---",
        human(unbounded.resident_chunk_bytes),
        human(bounded.resident_chunk_bytes),
        ratio
    );

    // FRS-AKV-B1 byte-budget verdict.
    let budgeted_resident_bytes = budgeted.resident_readers * VLOG_READER_CHARGE_BYTES;
    println!(
        "--- AKV-B1 byte-budget: resident readers plateau at {} (charge {}), \
         charged resident {} <= budget {} (O(budget), not O(segments)) ---",
        budgeted.resident_readers,
        human(VLOG_READER_CHARGE_BYTES),
        human(budgeted_resident_bytes),
        human(byte_budget),
    );

    // Hard assertions so the bin doubles as a CI gate under --smoke.
    assert_eq!(
        unbounded.resident_readers, segments as usize,
        "uncapped arm must keep every reader resident (O(segments))"
    );
    assert!(
        bounded.resident_readers <= cap,
        "bounded arm resident readers {} must not exceed cap {}",
        bounded.resident_readers,
        cap
    );
    assert!(
        bounded.resident_chunk_bytes < unbounded.resident_chunk_bytes,
        "bounded footprint must be strictly smaller than uncapped"
    );
    // FRS-AKV-B1: the byte budget bounds CHARGED resident bytes regardless of
    // segment count — the q9 never-OOM property.
    assert!(
        budgeted_resident_bytes <= byte_budget,
        "byte-budget arm charged resident {} must not exceed budget {}",
        budgeted_resident_bytes,
        byte_budget
    );
    assert!(
        budgeted.resident_readers < segments as usize,
        "byte budget must cap resident below O(segments) when segments exceed budget/charge"
    );
    println!("OK: bounds hold (count cap O(cap); AKV byte budget O(budget) — q9 never-OOM).");
}

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

//! FRS-PHASE2 UPLOAD-BYTE-BUDGET resident-bound mini-bench
//! (in-process, NO network, NO NEXMark).
//!
//! ## What it proves
//!
//! The in-flight buffered-upload admission control bounds RESIDENT upload
//! memory. The legacy policy is a fixed COUNT cap (`MAX_INFLIGHT_UPLOADS = 8`):
//! it admits up to 8 uploads regardless of size, so under a slow remote a burst
//! of LARGE compaction outputs pins `8 * largest_SST` of RAM — the resident-
//! buffer blowup the byte budget targets. The byte budget
//! (`FRS_UPLOAD_BYTE_BUDGET_MIB`) instead reserves `ceil(bytes / 1 MiB)` permits
//! out of a fixed MiB pool, so the SUM of resident bytes is bounded while MANY
//! small SSTs still proceed concurrently.
//!
//! This bench reproduces the EXACT admission math the engine uses
//! (`opendal_backend::{upload_sem_total_permits, upload_permits_for}` — count =
//! 1 permit/upload over `MAX_INFLIGHT_UPLOADS`; bytes = `ceil(bytes/MiB)` over a
//! MiB budget) driving a realistic MIXED SST-size stream through a tokio
//! `Semaphore`, with a modeled per-MiB upload time (the BOS-class slow remote).
//! Each admitted "upload" holds its full buffer for its transfer; a sampler
//! tracks PEAK concurrent resident bytes. The two arms are compared on:
//!
//!   - **peak resident bytes** — the bound (byte budget should hold it at ~the
//!     configured budget; count cap lets it balloon to `8 * large_SST`);
//!   - **small-SST concurrency** — the byte budget must still admit many small
//!     SSTs at once (it is NOT just a smaller count cap);
//!   - **total wall** — must not regress materially (admission is off the
//!     transfer critical path; both arms move the same bytes).
//!
//! Run: `cargo run -p forst-rs-bench --release --bin upload_byte_budget`
//! Smoke: `... --bin upload_byte_budget -- --smoke`

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

const MIB: u64 = 1024 * 1024;

/// Mirrors `opendal_backend::MAX_INFLIGHT_UPLOADS` (the count-cap default).
const MAX_INFLIGHT_UPLOADS: usize = 8;

/// Mirrors `opendal_backend::UPLOAD_BUDGET_UNIT_BYTES` (1 permit == 1 MiB).
const UNIT_BYTES: u64 = MIB;

/// COUNT regime: always 1 permit per upload (byte-identical to legacy).
fn count_permits_for(_nbytes: u64) -> u32 {
    1
}

/// BYTE regime: `ceil(bytes / 1 MiB)`, clamped to [1, budget] — the exact
/// `byte_budget_permits_for` math.
fn byte_permits_for(nbytes: u64, budget_mib: u64) -> u32 {
    let units = nbytes.div_ceil(UNIT_BYTES).max(1);
    units.min(budget_mib) as u32
}

/// One SST "upload" job: a byte length to keep resident for its transfer.
#[derive(Clone, Copy)]
struct Job {
    bytes: u64,
}

struct ArmResult {
    tag: &'static str,
    peak_resident_bytes: u64,
    /// Max number of jobs concurrently in flight (observed).
    peak_concurrency: usize,
    wall_ms: f64,
    total_bytes: u64,
}

/// Drive `jobs` through a `Semaphore` of `total_permits`, each job reserving
/// `permits_for(bytes)`. The job holds `bytes` resident for a modeled transfer
/// time (`bytes / bw`), then releases. A sampler thread tracks peak resident.
fn run_arm(
    tag: &'static str,
    jobs: &[Job],
    total_permits: usize,
    permits_for: impl Fn(u64) -> u32 + Send + Sync + 'static + Copy,
    bw_mibps: u64,
) -> ArmResult {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_time()
        .build()
        .expect("rt");

    let sem = Arc::new(Semaphore::new(total_permits));
    let resident = Arc::new(AtomicU64::new(0));
    let peak_resident = Arc::new(AtomicU64::new(0));
    let in_flight = Arc::new(AtomicU64::new(0));
    let peak_concurrency = Arc::new(AtomicU64::new(0));
    let total_bytes: u64 = jobs.iter().map(|j| j.bytes).sum();

    // Per-MiB transfer time at the modeled bandwidth.
    let per_mib = Duration::from_secs_f64(1.0 / bw_mibps as f64);

    let start = Instant::now();
    rt.block_on(async {
        let mut handles = Vec::with_capacity(jobs.len());
        for &job in jobs {
            let permits = permits_for(job.bytes).min(total_permits as u32);
            let sem = Arc::clone(&sem);
            let resident = Arc::clone(&resident);
            let peak_resident = Arc::clone(&peak_resident);
            let in_flight = Arc::clone(&in_flight);
            let peak_concurrency = Arc::clone(&peak_concurrency);
            // Acquire admission permit BEFORE allocating the resident buffer —
            // this models the TRUE-bound (FRS_ASYNC_FLUSH_UPLOAD) path: a job
            // that cannot be admitted does NOT hold its buffer resident.
            let permit = sem
                .clone()
                .acquire_many_owned(permits)
                .await
                .expect("permit");
            handles.push(tokio::spawn(async move {
                // Buffer becomes resident now (admitted).
                let r = resident.fetch_add(job.bytes, Ordering::AcqRel) + job.bytes;
                peak_resident.fetch_max(r, Ordering::AcqRel);
                let c = in_flight.fetch_add(1, Ordering::AcqRel) + 1;
                peak_concurrency.fetch_max(c, Ordering::AcqRel);

                // Modeled transfer time proportional to size.
                let mib = (job.bytes as f64 / MIB as f64).max(1.0 / 64.0);
                tokio::time::sleep(per_mib.mul_f64(mib)).await;

                resident.fetch_sub(job.bytes, Ordering::AcqRel);
                in_flight.fetch_sub(1, Ordering::AcqRel);
                drop(permit);
            }));
        }
        for h in handles {
            h.await.expect("join");
        }
    });
    let wall_ms = start.elapsed().as_secs_f64() * 1e3;

    ArmResult {
        tag,
        peak_resident_bytes: peak_resident.load(Ordering::Acquire),
        peak_concurrency: peak_concurrency.load(Ordering::Acquire) as usize,
        wall_ms,
        total_bytes,
    }
}

/// Deterministic mixed SST-size stream: a realistic disagg mix of many small L0
/// flush SSTs (a few MiB) and a handful of LARGE compaction outputs (64–256
/// MiB) — exactly the mix where the count cap over-commits resident memory.
fn build_jobs(scale: usize) -> Vec<Job> {
    // xorshift for reproducibility.
    let mut s: u64 = 0x9E3779B97F4A7C15;
    let mut next = || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    let mut jobs = Vec::new();
    for i in 0..scale {
        // ~1 in 10 jobs is a LARGE compaction output; the rest are small flushes.
        let bytes = if i % 10 == 3 {
            // 64..=256 MiB
            (64 + (next() % 193)) * MIB
        } else {
            // 1..=8 MiB small flush SSTs.
            (1 + (next() % 8)) * MIB
        };
        jobs.push(Job { bytes });
    }
    jobs
}

fn fmt_mib(bytes: u64) -> String {
    format!("{:.1} MiB", bytes as f64 / MIB as f64)
}

fn print_arm(a: &ArmResult) {
    println!(
        "  {:<14} peak_resident={:<12} peak_concurrency={:<3} wall={:.0} ms  ({} moved)",
        a.tag,
        fmt_mib(a.peak_resident_bytes),
        a.peak_concurrency,
        a.wall_ms,
        fmt_mib(a.total_bytes),
    );
}

fn main() {
    let smoke = std::env::args().any(|x| x == "--smoke");
    let scale = if smoke { 60 } else { 400 };
    // Byte budget (MiB). Override with UBB_BUDGET_MIB. The DEFAULT is sized to
    // the count cap's INTENDED resident envelope — `MAX_INFLIGHT_UPLOADS * a
    // typical SST` — so the byte budget delivers the SAME steady-state transfer
    // parallelism while CAPPING the large-SST tail (the count cap's true worst
    // case is `8 * largest`, which a budget never lets happen). A budget set far
    // below that envelope (e.g. 256) trades wall for a tighter bound — useful
    // only when RAM is the hard constraint.
    let budget_mib: u64 = std::env::var("UBB_BUDGET_MIB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024);
    // BOS-class slow remote so admission backpressure actually bites.
    let bw_mibps: u64 = 64;

    let jobs = build_jobs(scale);
    let largest = jobs.iter().map(|j| j.bytes).max().unwrap_or(0);
    let n_large = jobs.iter().filter(|j| j.bytes >= 64 * MIB).count();

    println!("UPLOAD-BYTE-BUDGET resident-bound mini-bench  smoke={smoke}");
    println!(
        "  {} SSTs ({} large ≥64 MiB, largest {}); modeled remote {} MiB/s.",
        jobs.len(),
        n_large,
        fmt_mib(largest),
        bw_mibps,
    );
    println!(
        "  count cap = {} uploads (any size); byte budget = {} MiB (1 permit/MiB).\n",
        MAX_INFLIGHT_UPLOADS, budget_mib,
    );

    // ARM 1 — COUNT cap (legacy default): 8 permits, 1 per upload.
    let count_arm = run_arm(
        "count-cap",
        &jobs,
        MAX_INFLIGHT_UPLOADS,
        count_permits_for,
        bw_mibps,
    );

    // ARM 2 — BYTE budget: `budget_mib` permits, ceil(bytes/MiB) per upload.
    let byte_arm = run_arm(
        "byte-budget",
        &jobs,
        budget_mib as usize,
        move |b| byte_permits_for(b, budget_mib),
        bw_mibps,
    );

    print_arm(&count_arm);
    print_arm(&byte_arm);

    // Worst-case count-cap resident bound: 8 * largest SST (8 large outputs all
    // admitted at once). The byte budget caps resident at ~budget_mib.
    let count_worst = MAX_INFLIGHT_UPLOADS as u64 * largest;
    println!("\n== VERDICT ==");
    println!(
        "  count-cap worst-case bound = 8 × {} = {}  (observed peak {})",
        fmt_mib(largest),
        fmt_mib(count_worst),
        fmt_mib(count_arm.peak_resident_bytes),
    );
    println!(
        "  byte-budget bound          = {} MiB                    (observed peak {})",
        budget_mib,
        fmt_mib(byte_arm.peak_resident_bytes),
    );

    // GATE 1: the byte budget bounds resident at ~the configured budget (allow a
    // one-large-SST clamp overshoot: an oversized SST runs alone reserving the
    // whole budget, so peak ≤ budget + one largest-clamped job's actual bytes is
    // impossible — clamp caps it AT budget — but a small SST may slip in just
    // under the wire, so allow a modest slack).
    let budget_bytes = budget_mib * MIB;
    let byte_ok = byte_arm.peak_resident_bytes <= budget_bytes + largest;
    // GATE 2: the byte budget holds resident materially BELOW the count cap's
    // observed peak (the whole point).
    let bounds_better = byte_arm.peak_resident_bytes < count_arm.peak_resident_bytes;
    // GATE 3: small-SST concurrency is preserved — the byte budget admits MANY
    // jobs at once (not throttled down to a small count).
    let concurrency_ok = byte_arm.peak_concurrency >= MAX_INFLIGHT_UPLOADS;
    // GATE 4: no material wall regression (admission is off the transfer path).
    let wall_ok = byte_arm.wall_ms <= count_arm.wall_ms * 1.25;

    println!(
        "  byte-budget peak ≤ budget+slack: {}  |  bounds below count-cap: {}  |  \
         small-SST concurrency ≥ {}: {} (peak {})  |  wall not regressed: {}",
        byte_ok,
        bounds_better,
        MAX_INFLIGHT_UPLOADS,
        concurrency_ok,
        byte_arm.peak_concurrency,
        wall_ok,
    );

    let marginal = !bounds_better || count_arm.peak_resident_bytes <= budget_bytes + largest;
    if marginal {
        println!(
            "\n  RESULT: MARGINAL — the count cap did not over-commit resident memory at this \
             scale (peak {} ≤ byte budget). The byte budget adds value only when large-SST \
             bursts would pin 8 × large; raise scale/large-mix to see the gap.",
            fmt_mib(count_arm.peak_resident_bytes),
        );
    } else if byte_ok && bounds_better && concurrency_ok && wall_ok {
        println!(
            "\n  RESULT: BYTE BUDGET EFFECTIVE — resident peak {} → {} ({:.1}× lower) while \
             keeping {} concurrent small uploads; wall {:.0}→{:.0} ms.",
            fmt_mib(count_arm.peak_resident_bytes),
            fmt_mib(byte_arm.peak_resident_bytes),
            count_arm.peak_resident_bytes as f64 / byte_arm.peak_resident_bytes.max(1) as f64,
            byte_arm.peak_concurrency,
            count_arm.wall_ms,
            byte_arm.wall_ms,
        );
    } else {
        println!("\n  RESULT: INCONCLUSIVE — see per-gate flags above.");
    }
}

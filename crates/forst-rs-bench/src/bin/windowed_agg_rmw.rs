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

//! WINDOWED-AGG RMW mini-bench (PMC-1 q11/q17 KV-sep-ON read-path, 2026-06-15).
//!
//! Reproduces the windowed-aggregation Reducing/Aggregating state hot path that
//! gates q11/q17: per record, READ the small numeric accumulator, update it in
//! "Java" (here: increment), and WRITE it back. The accumulators are FLUSHED to
//! SST first so that under UNIFORM KV-separation (every value separated) the
//! per-record read pays a vlog deref — the regression this task fixes IN THE
//! READ PATH (the on-disk format stays uniform; we never change separation).
//!
//! It answers the C-vs-B question: WHERE does the deref cost land?
//!   - if the RMW reads HOT (just-written, memtable-resident) accumulators, a
//!     deref there is avoidable indirection => fix C (inline-first).
//!   - if the RMW reads COLD (flushed/SST-resident) accumulators per record,
//!     the deref is structural => fix B (off-heap staging buffer) OR batch+
//!     coalesce+zero-copy (A) can collapse the per-deref cost.
//!
//! Arms (all on a LocalFileSystem; storage-layer probe, NOT NexMark):
//!   1. OFF            — KV-sep OFF (inline values, the target to beat)
//!   2. ON-uniform     — KV-sep ON, min-blob=22 (every accumulator separated)
//!   3. ON+coalesce    — + FRS_VLOG_COALESCE_DEREF (batch the derefs)
//!   4. ON+point       — + FRS_VLOG_POINT_DEREF (right-sized scattered deref)
//!   5. ON+point+coal  — point + coalesce together
//!
//! Each arm reports per-record COLD-RMW latency. The cost lands on COLD/flushed
//! accumulators; `FRS_VLOG_POINT_DEREF` reads exactly the record instead of a
//! 64 KiB chunk, killing the read-amp the regression comes from.
//!
//! Run: `cargo run -p forst-rs-bench --release --bin windowed_agg_rmw`
//!      `... -- --keys 200000 --records 2000000 --acc-size 32 --batch 256`
//!
//! DRAIN-PHASE BREAKDOWN (PMC-1, 2026-06-15): pass `--phase-breakdown` to
//! characterize the ~140s POST-INGEST DRAIN that point-deref does NOT touch
//! (point-deref is a read-path-only lever). q11's windowed agg, after the
//! source finishes, must FLUSH the active accumulator memtable to SST (+vlog
//! under KV-sep) and COMPACT the flushed L0 accumulator SSTs. This mode times
//! the three phases SEPARATELY — RMW-ingest (steady state) vs final-flush vs
//! compaction — for OFF / ON / ON+point, so we can answer "what % of the drain
//! is flush vs compaction vs RMW". The per-phase RATIO is box-robust even
//! though the Mac's ABSOLUTE walls differ from the Linux reference.

use std::sync::Arc;
use std::time::Instant;

use forst_rs_common::config::EngineOptions;
use forst_rs_engine::{
    set_kv_separation_override, set_vlog_coalesce_deref_override, set_vlog_point_deref_override,
    DbImpl,
};

fn parse_arg(args: &[String], flag: &str, default: usize) -> usize {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut s = self.0;
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.0 = s;
        s
    }
}

/// One RMW pass over `records` records: each record picks a key (uniform random
/// over `keys`), reads its accumulator via `batch_get_vectorized` in batches of
/// `batch`, "updates" it (treat first 8 bytes as a u64 count, +1, pad to
/// `acc_size`), and writes it back. Returns (elapsed, total_bytes_read).
fn rmw_pass(
    db: &Arc<DbImpl>,
    keys: usize,
    records: usize,
    acc_size: usize,
    batch: usize,
    seed: u64,
) -> (std::time::Duration, usize) {
    let cf = db.default_cf();
    let mut prng = Rng(seed);
    let mut bytes = 0usize;
    let t = Instant::now();
    let mut processed = 0usize;
    while processed < records {
        let this = batch.min(records - processed);
        // Pick this batch's keys.
        let mut keys_owned: Vec<String> = Vec::with_capacity(this);
        for _ in 0..this {
            let i = prng.next() as usize % keys;
            keys_owned.push(format!("agg|k{i:012}"));
        }
        let key_refs: Vec<&[u8]> = keys_owned.iter().map(|k| k.as_bytes()).collect();
        // Newest visible (matches batch_get_arrow's read_seq=u64::MAX).
        let vals = db
            .batch_get_vectorized(&cf, &key_refs, u64::MAX)
            .expect("rmw read");
        // Update + write back, per record (the Flink sync-operator boundary).
        for (k, v) in key_refs.iter().zip(vals) {
            let mut acc = vec![0u8; acc_size];
            if let Some(cur) = v {
                bytes += cur.len();
                let n = cur.len().min(8);
                acc[..n].copy_from_slice(&cur[..n]);
            }
            let c = u64::from_le_bytes(acc[..8].try_into().unwrap());
            acc[..8].copy_from_slice(&(c + 1).to_le_bytes());
            db.put(&cf, k, &acc).expect("rmw write");
        }
        processed += this;
    }
    (t.elapsed(), bytes)
}

fn build_and_flush(db: &Arc<DbImpl>, keys: usize, acc_size: usize) {
    let cf = db.default_cf();
    let acc = vec![7u8; acc_size];
    for i in 0..keys {
        let key = format!("agg|k{i:012}");
        db.put(&cf, key.as_bytes(), &acc).expect("seed put");
        if (i + 1) % 8192 == 0 {
            db.switch_and_flush(&cf).expect("seed flush");
        }
    }
    db.switch_and_flush(&cf).expect("final flush");
}

#[allow(clippy::too_many_arguments)]
fn run_arm(
    label: &str,
    kvsep: bool,
    coalesce: bool,
    point: bool,
    keys: usize,
    records: usize,
    acc_size: usize,
    batch: usize,
) {
    set_kv_separation_override(Some(kvsep));
    set_vlog_coalesce_deref_override(Some(coalesce));
    set_vlog_point_deref_override(Some(point));

    let tmp = std::env::temp_dir().join(format!(
        "wagg-rmw-{}-{}",
        label.replace([' ', '+'], "_"),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();

    let opts = EngineOptions {
        db_path: tmp.to_string_lossy().to_string(),
        write_buffer_size: 64 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open(opts).expect("open");

    // Seed `keys` accumulators and FLUSH them to SST. Under KV-sep ON +
    // min-blob=22 (set in main) every accumulator (acc_size >= 22) separates into
    // the vlog, so the COLD RMW reads below each pay a deref.
    build_and_flush(&db, keys, acc_size);

    // COLD RMW: accumulators are SST-resident (the regression regime).
    let (cold, cold_bytes) = rmw_pass(&db, keys, records, acc_size, batch, 0x1234_5678_9abc_def0);

    let per_cold = cold.as_secs_f64() * 1e9 / records as f64;
    println!(
        "  {label:<22} COLD(SST)  {per_cold:>8.1} ns/rec   {:>8.3}s total   {:>6.1} MiB read",
        cold.as_secs_f64(),
        cold_bytes as f64 / (1024.0 * 1024.0)
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&tmp);

    set_kv_separation_override(None);
    set_vlog_coalesce_deref_override(None);
    set_vlog_point_deref_override(None);
}

/// Count live SSTs and total live-file bytes across all levels.
fn live_sst_stats(db: &Arc<DbImpl>) -> (usize, u64) {
    let files = db.list_live_files(false).expect("list live files");
    let bytes: u64 = files.iter().map(|f| f.size).sum();
    (files.len(), bytes)
}

/// DRAIN-PHASE BREAKDOWN arm. Ingests `records` RMW updates over `keys`
/// accumulators (auto-flush every `flush_every` records to mimic memtable
/// pressure), then times the POST-INGEST DRAIN in two phases:
///
///   - FLUSH: `flush_all()` lands the final active memtable into SST (+vlog).
///   - COMPACT: `compact_all()` rolls up the flushed L0 accumulator SSTs
///     (under KV-sep this rewrites/relocates vlog-separated values).
///
/// The reported per-phase split (ingest vs flush vs compact) is the box-robust
/// root-cause model of the ~140s q11 drain.
#[allow(clippy::too_many_arguments)]
fn run_drain_arm(
    label: &str,
    kvsep: bool,
    point: bool,
    keys: usize,
    records: usize,
    acc_size: usize,
    batch: usize,
    flush_every: usize,
) {
    set_kv_separation_override(Some(kvsep));
    set_vlog_coalesce_deref_override(Some(false));
    set_vlog_point_deref_override(Some(point));

    let tmp = std::env::temp_dir().join(format!(
        "wagg-drain-{}-{}",
        label.replace([' ', '+'], "_"),
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&tmp);
    std::fs::create_dir_all(&tmp).unwrap();

    let opts = EngineOptions {
        db_path: tmp.to_string_lossy().to_string(),
        write_buffer_size: 64 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let db = DbImpl::open(opts).expect("open");
    let cf = db.default_cf();

    // Phase 1: RMW INGEST (steady state). Per record: read accumulator, +1,
    // write back. Periodic switch_and_flush mimics memtable-fill flushes
    // during the source phase (NOT counted in the drain).
    let mut prng = Rng(0x1234_5678_9abc_def0);
    let t_ingest = Instant::now();
    let mut processed = 0usize;
    while processed < records {
        let this = batch.min(records - processed);
        let mut keys_owned: Vec<String> = Vec::with_capacity(this);
        for _ in 0..this {
            let i = prng.next() as usize % keys;
            keys_owned.push(format!("agg|k{i:012}"));
        }
        let key_refs: Vec<&[u8]> = keys_owned.iter().map(|k| k.as_bytes()).collect();
        let vals = db
            .batch_get_vectorized(&cf, &key_refs, u64::MAX)
            .expect("rmw read");
        for (k, v) in key_refs.iter().zip(vals) {
            let mut acc = vec![0u8; acc_size];
            if let Some(cur) = v {
                let n = cur.len().min(8);
                acc[..n].copy_from_slice(&cur[..n]);
            }
            let c = u64::from_le_bytes(acc[..8].try_into().unwrap());
            acc[..8].copy_from_slice(&(c + 1).to_le_bytes());
            db.put(&cf, k, &acc).expect("rmw write");
        }
        processed += this;
        if flush_every > 0 && processed % flush_every < batch {
            db.switch_and_flush(&cf).expect("mid flush");
        }
    }
    let ingest = t_ingest.elapsed();
    let (sst_pre, bytes_pre) = live_sst_stats(&db);

    // Phase 2: FINAL FLUSH (drain part 1). Land the residual active memtable.
    let t_flush = Instant::now();
    db.switch_and_flush(&cf).ok();
    db.flush_all().expect("drain flush");
    let flush = t_flush.elapsed();
    let (sst_post_flush, bytes_post_flush) = live_sst_stats(&db);

    // Phase 3: COMPACTION (drain part 2). Roll up flushed L0 accumulator SSTs.
    let t_compact = Instant::now();
    db.compact_all().expect("drain compact");
    let compact = t_compact.elapsed();
    let (sst_post, bytes_post) = live_sst_stats(&db);

    let drain = flush + compact;
    let total = ingest + drain;
    let pct = |d: std::time::Duration| d.as_secs_f64() / total.as_secs_f64() * 100.0;
    let drain_pct = |d: std::time::Duration| {
        if drain.as_secs_f64() > 0.0 {
            d.as_secs_f64() / drain.as_secs_f64() * 100.0
        } else {
            0.0
        }
    };
    println!("  {label}");
    println!(
        "    ingest  {:>8.3}s ({:>4.1}% of total)   {processed} RMW recs, {keys} keys",
        ingest.as_secs_f64(),
        pct(ingest)
    );
    println!(
        "    flush   {:>8.3}s ({:>4.1}% of total, {:>4.1}% of DRAIN)   SST {sst_pre}->{sst_post_flush}, {:.1}->{:.1} MiB",
        flush.as_secs_f64(),
        pct(flush),
        drain_pct(flush),
        bytes_pre as f64 / 1048576.0,
        bytes_post_flush as f64 / 1048576.0,
    );
    println!(
        "    compact {:>8.3}s ({:>4.1}% of total, {:>4.1}% of DRAIN)   SST {sst_post_flush}->{sst_post}, {:.1}->{:.1} MiB",
        compact.as_secs_f64(),
        pct(compact),
        drain_pct(compact),
        bytes_post_flush as f64 / 1048576.0,
        bytes_post as f64 / 1048576.0,
    );
    println!(
        "    => DRAIN {:>7.3}s ({:>4.1}% of total)   ingest {:>7.3}s\n",
        drain.as_secs_f64(),
        pct(drain),
        ingest.as_secs_f64(),
    );

    drop(db);
    let _ = std::fs::remove_dir_all(&tmp);

    set_kv_separation_override(None);
    set_vlog_coalesce_deref_override(None);
    set_vlog_point_deref_override(None);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let keys = parse_arg(&args, "--keys", 200_000);
    let records = parse_arg(&args, "--records", 2_000_000);
    let acc_size = parse_arg(&args, "--acc-size", 32);
    let batch = parse_arg(&args, "--batch", 256);

    if args.iter().any(|a| a == "--phase-breakdown") {
        let flush_every = parse_arg(&args, "--flush-every", 100_000);
        std::env::set_var("FRS_KV_MIN_BLOB_SIZE", "22");
        println!("=== windowed-agg DRAIN-PHASE breakdown (q11/q17 KV-sep-ON) ===");
        println!(
            "keys={keys}  records={records}  acc_size={acc_size}B  batch={batch}  flush_every={flush_every}  min_blob=22 (LocalFS)\n"
        );
        run_drain_arm(
            "OFF (inline)",
            false,
            false,
            keys,
            records,
            acc_size,
            batch,
            flush_every,
        );
        run_drain_arm(
            "ON-uniform (KV-sep)",
            true,
            false,
            keys,
            records,
            acc_size,
            batch,
            flush_every,
        );
        run_drain_arm(
            "ON+point (KV-sep + point-deref)",
            true,
            true,
            keys,
            records,
            acc_size,
            batch,
            flush_every,
        );
        return;
    }

    // UNIFORM KV-sep: separate EVERY value (min-blob at the pointer floor). Must
    // be set before any `kv_min_blob_size()` call caches it via OnceLock.
    std::env::set_var("FRS_KV_MIN_BLOB_SIZE", "22");

    println!("=== windowed-agg RMW mini-bench (q11/q17 KV-sep-ON read path) ===");
    println!(
        "keys={keys}  records={records}  acc_size={acc_size}B  batch={batch}  min_blob=22 (LocalFS)\n"
    );

    run_arm("OFF", false, false, false, keys, records, acc_size, batch);
    run_arm(
        "ON-uniform",
        true,
        false,
        false,
        keys,
        records,
        acc_size,
        batch,
    );
    run_arm(
        "ON+coalesce",
        true,
        true,
        false,
        keys,
        records,
        acc_size,
        batch,
    );
    run_arm(
        "ON+point", true, false, true, keys, records, acc_size, batch,
    );
    run_arm(
        "ON+point+coal",
        true,
        true,
        true,
        keys,
        records,
        acc_size,
        batch,
    );
}

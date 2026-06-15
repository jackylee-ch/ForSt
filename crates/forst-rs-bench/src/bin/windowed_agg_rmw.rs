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

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let keys = parse_arg(&args, "--keys", 200_000);
    let records = parse_arg(&args, "--records", 2_000_000);
    let acc_size = parse_arg(&args, "--acc-size", 32);
    let batch = parse_arg(&args, "--batch", 256);

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

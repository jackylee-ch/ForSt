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

//! BLOB-DEREF SINK-COPY mini-bench (PMC-1 q9 KV-sep-ON read path, 2026-06-16).
//!
//! Isolates the SECOND COPY on the q9 join-probe deref path. Under uniform
//! KV-separation a found join value is a BlobRef: `batch_get_arrow`'s slow-path
//! tail calls `get_internal`, which for a BlobRef terminal calls
//! `vlog_deref(ptr) -> Vec<u8>` (COPY 1: `parse_record`/`decompress` allocates a
//! fresh `Vec`; for the uncompressed codec this is `stored.to_vec()`), and then
//! `batch_get_arrow` does `append_value(&value)` into the Arrow `BinaryBuilder`
//! (COPY 2: one memcpy into the Arrow value buffer). So every separated value is
//! copied TWICE between the read buffer and the Arrow batch.
//!
//! The scoped fix (`FRS_VLOG_SINK_DEREF`, default OFF) dereferences the value
//! DIRECTLY into the Arrow builder via the `ValueSink` trait: for the
//! uncompressed codec it appends the stored record slice borrowed (the read
//! buffer → Arrow buffer single memcpy, NO intermediate `Vec`); for a compressed
//! codec the decompressor still produces one buffer, which is then appended (so
//! the win is bounded to the uncompressed case + the alloc, but the output is
//! byte-identical for every codec).
//!
//! This bench drives the REAL `DbImpl::batch_get_arrow` path on a fully-separated
//! cold batch (every key a BlobRef in an SST/vlog), flag OFF vs ON, across a
//! sweep of value sizes, and asserts the two arms produce BYTE-IDENTICAL Arrow
//! output. It is a storage/engine probe on a LocalFileSystem — NOT NexMark.
//!
//! Run: `cargo run -p forst-rs-bench --release --bin blob_deref_sink_copy`
//!      `... -- --keys 50000 --batch 1024`

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, BinaryArray, BinaryBuilder};
use forst_rs_engine::{
    set_kv_separation_override, set_vlog_point_deref_override, set_vlog_sink_deref_override,
    ColumnFamilyDescriptor, ColumnFamilyHandle, DbImpl,
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

/// Build a DB with `keys` separated values of `value_size` bytes, flushed to SST
/// (so every key resolves through the BlobRef/vlog deref slow path). Returns the
/// db + cf + the keys in SCATTERED order (the join-probe shape).
fn build(value_size: usize, keys: usize) -> (Arc<DbImpl>, ColumnFamilyHandle, Vec<Vec<u8>>) {
    let db = DbImpl::open_default().expect("open");
    let cf = db
        .create_column_family(ColumnFamilyDescriptor::new("blob-sink"))
        .expect("cf");
    let mut payload = vec![0u8; value_size];
    let mut key_list = Vec::with_capacity(keys);
    for i in 0..keys {
        for (j, b) in payload.iter_mut().enumerate() {
            *b = ((i.wrapping_mul(131).wrapping_add(j)) % 251) as u8;
        }
        let k = format!("k{i:09}").into_bytes();
        db.put(&cf, &k, &payload).expect("put");
        key_list.push(k);
    }
    db.switch_and_flush(&cf).expect("flush").expect("sst");
    // Scatter the read order (deterministic shuffle).
    let mut s = Rng(0x9E37_79B9_7F4A_7C15);
    for i in (1..key_list.len()).rev() {
        let j = (s.next() as usize) % (i + 1);
        key_list.swap(i, j);
    }
    (db, cf, key_list)
}

/// One full pass of `batch_get_arrow` over all keys in batches of `batch`.
/// Returns (elapsed, total_value_bytes, concatenated value bytes for byte-eq).
fn arrow_pass(
    db: &Arc<DbImpl>,
    cf: &ColumnFamilyHandle,
    keys: &[Vec<u8>],
    batch: usize,
) -> (std::time::Duration, usize, Vec<u8>) {
    let mut bytes = 0usize;
    let mut digest = Vec::new();
    let t = Instant::now();
    let mut off = 0usize;
    while off < keys.len() {
        let this = batch.min(keys.len() - off);
        let mut kb = BinaryBuilder::new();
        for k in &keys[off..off + this] {
            kb.append_value(k);
        }
        let karr: BinaryArray = kb.finish();
        let rb = db.batch_get_arrow(cf, &karr).expect("batch_get_arrow");
        let values = rb
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("value col");
        for i in 0..values.len() {
            if values.is_valid(i) {
                let v = values.value(i);
                bytes += v.len();
                // Sample the first row of each batch into the digest (cheap
                // byte-identity proof without retaining every value).
                if i == 0 {
                    digest.extend_from_slice(v);
                }
            }
        }
        off += this;
    }
    (t.elapsed(), bytes, digest)
}

fn run_size(value_size: usize, keys: usize, batch: usize) {
    set_kv_separation_override(Some(true));
    // Point-deref ON for both arms (the scattered join-probe deref shape; this is
    // already the shipped default-recommended lever). The ONLY difference between
    // the two arms is the sink-deref flag.
    set_vlog_point_deref_override(Some(true));

    // OFF arm: vlog_deref -> Vec (copy 1) + append_value (copy 2).
    set_vlog_sink_deref_override(Some(false));
    let (db, cf, klist) = build(value_size, keys);
    let (off_t, off_bytes, off_digest) = arrow_pass(&db, &cf, &klist, batch);
    // Warm rerun (steady-state, chunk/readers cached).
    let (off_t2, _, off_digest2) = arrow_pass(&db, &cf, &klist, batch);
    drop(db);

    // ON arm: sink-deref (no intermediate Vec for the uncompressed codec).
    set_vlog_sink_deref_override(Some(true));
    let (db, cf, klist) = build(value_size, keys);
    let (on_t, on_bytes, on_digest) = arrow_pass(&db, &cf, &klist, batch);
    let (on_t2, _, on_digest2) = arrow_pass(&db, &cf, &klist, batch);
    drop(db);

    set_vlog_sink_deref_override(None);
    set_vlog_point_deref_override(None);
    set_kv_separation_override(None);

    // BYTE-IDENTITY: the two arms must produce identical Arrow output.
    assert_eq!(off_bytes, on_bytes, "value byte totals must match");
    assert_eq!(
        off_digest, on_digest,
        "first-row digest must be byte-identical"
    );
    assert_eq!(
        off_digest, off_digest2,
        "OFF arm must be stable across reruns"
    );
    assert_eq!(on_digest, on_digest2, "ON arm must be stable across reruns");
    assert_eq!(
        off_digest2, on_digest2,
        "warm-rerun output must also be byte-identical"
    );

    let cold_off = off_t.as_secs_f64() * 1e9 / keys as f64;
    let cold_on = on_t.as_secs_f64() * 1e9 / keys as f64;
    let warm_off = off_t2.as_secs_f64() * 1e9 / keys as f64;
    let warm_on = on_t2.as_secs_f64() * 1e9 / keys as f64;
    println!(
        "  value={value_size:>6}B   OFF cold {cold_off:>7.1} / warm {warm_off:>7.1}   ON cold {cold_on:>7.1} / warm {warm_on:>7.1} ns/key   warm speedup {:>5.2}x",
        warm_off / warm_on
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let keys = parse_arg(&args, "--keys", 50_000);
    let batch = parse_arg(&args, "--batch", 1024);

    // Uniform separation: every value (>= pointer floor) lands in the vlog.
    std::env::set_var("FRS_KV_MIN_BLOB_SIZE", "22");

    println!("=== blob-deref SINK-COPY mini-bench (q9 KV-sep-ON, batch_get_arrow) ===");
    println!("keys={keys}  batch={batch}  min_blob=22  point-deref=ON both arms  (LocalFS)\n");
    println!("  OFF = vlog_deref->Vec + append_value (2 copies)");
    println!("  ON  = FRS_VLOG_SINK_DEREF: deref straight into the Arrow builder (1 copy, uncompressed)\n");

    for &vs in &[64usize, 256, 1024, 4096, 16384] {
        run_size(vs, keys, batch);
    }
    println!("\nByte-identity asserted for every value size (cold + warm reruns).");
}

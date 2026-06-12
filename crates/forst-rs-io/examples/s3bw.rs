// FRS-S3-BW-PROBE (2026-06-01): measure real upload bandwidth from this host to
// the configured S3 endpoint via the EXACT production opendal write path
// (buffered + concurrent multipart on close). Reads S3_* from the environment.
// Run: cargo run --release -p forst-rs-io --example s3bw
//
// FRS-PHASE2-S0 (2026-06-13 design, Stage 0): `--probe` extends the legacy
// upload-only measurement into the full S3-reachability probe required by
// docs/superpowers/specs/2026-06-13-phase2-disaggregated-state-design.md §Stage-0:
//   - metadata-op RTT (stat loop)
//   - PUT / DELETE small-object latency
//   - single-stream upload AND download bandwidth
//   - read-after-write visibility (PUT -> GET loop, feeds design risk R2)
// Output is machine-parseable `PROBE key=value` lines consumed by
// scripts/probe-s3.sh, which applies the design's BOS-vs-emulation decision
// matrix (>=100 MB/s && RTT < 5 ms).
//
// Backend selection (env):
//   S3BW_SCHEME=s3 (default)  — S3_BUCKET/S3_REGION/S3_ENDPOINT/S3_ACCESS_KEY/
//                               S3_SECRET_KEY/S3_PREFIX (same vars as
//                               scripts/measure-sql.sh)
//   S3BW_SCHEME=fs            — fs-emulation rooted at $S3BW_ROOT (self-test of
//                               the probe machinery; full remote code path
//                               minus network)
// Tunables: PROBE_MB (bulk size, default 200), PROBE_OPS (small-op count,
// default 20), PROBE_VIS_TRIALS (visibility trials, default 10).
use forst_rs_io::filesystem::{FileSystem, WriteMode};
use forst_rs_io::OpendalFileSystem;
use std::path::{Path, PathBuf};
use std::time::Instant;

fn build_fs() -> OpendalFileSystem {
    let scheme = std::env::var("S3BW_SCHEME").unwrap_or_else(|_| "s3".to_string());
    match scheme.as_str() {
        "fs" => {
            let root = std::env::var("S3BW_ROOT").expect("S3BW_ROOT required for S3BW_SCHEME=fs");
            OpendalFileSystem::local(Path::new(&root)).expect("build fs-emulation backend")
        }
        "s3" => {
            let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET");
            let region = std::env::var("S3_REGION").unwrap_or_default();
            let endpoint = std::env::var("S3_ENDPOINT").ok();
            let ak = std::env::var("S3_ACCESS_KEY").ok();
            let sk = std::env::var("S3_SECRET_KEY").ok();
            let prefix = std::env::var("S3_PREFIX").unwrap_or_default();
            OpendalFileSystem::s3_with_root(
                &bucket,
                &prefix,
                &region,
                endpoint.as_deref(),
                ak.as_deref(),
                sk.as_deref(),
            )
            .expect("build s3 backend")
        }
        other => panic!("unsupported S3BW_SCHEME: {other}"),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Writes `data` to `path` through the production write path (append + sync +
/// close + await async upload) and returns the wall time for full durability.
fn put_durable(fs: &OpendalFileSystem, path: &Path, data: &[u8]) -> f64 {
    let start = Instant::now();
    {
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(data).expect("append");
        w.sync().expect("sync");
    }
    let _ = fs.await_upload(path);
    start.elapsed().as_secs_f64()
}

/// Reads `path` fully through the sequential-read path; returns (bytes, secs).
fn get_all(fs: &OpendalFileSystem, path: &Path) -> (Vec<u8>, f64) {
    let start = Instant::now();
    let mut r = fs.open_sequential_file(path).expect("open sequential");
    let mut out = Vec::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = r.read(&mut buf).expect("read");
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
    }
    (out, start.elapsed().as_secs_f64())
}

fn median_ms(samples: &mut [f64]) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).expect("non-NaN"));
    samples[samples.len() / 2] * 1000.0
}

fn p90_ms(samples: &mut [f64]) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).expect("non-NaN"));
    samples[(samples.len() * 9) / 10] * 1000.0
}

/// Legacy single-measurement mode (kept verbatim for old callers).
fn legacy_upload(fs: &OpendalFileSystem) {
    let mb = env_usize("PROBE_MB", 200);
    let data = vec![7u8; mb * 1024 * 1024];
    let path = Path::new("frs-s3bw-probe.bin");
    let secs = put_durable(fs, path, &data);
    eprintln!(
        "FRS-S3-BW: uploaded {} MB in {:.2}s = {:.1} MB/s (endpoint hidden)",
        mb,
        secs,
        mb as f64 / secs
    );
    let _ = fs.delete_file(path);
}

fn probe(fs: &OpendalFileSystem) {
    let ops = env_usize("PROBE_OPS", 20).max(1);
    let vis_trials = env_usize("PROBE_VIS_TRIALS", 10).max(1);
    let mb = env_usize("PROBE_MB", 200).max(1);
    let dir = PathBuf::from("frs-probe");
    let mut cleanup: Vec<PathBuf> = Vec::new();

    // --- 1. metadata-op RTT: stat loop on a small existing object ---------
    let rtt_obj = dir.join("rtt.bin");
    put_durable(fs, &rtt_obj, &[1u8; 1024]);
    cleanup.push(rtt_obj.clone());
    let mut rtt: Vec<f64> = Vec::with_capacity(ops);
    for _ in 0..ops {
        let s = Instant::now();
        fs.get_file_metadata(&rtt_obj).expect("stat");
        rtt.push(s.elapsed().as_secs_f64());
    }
    println!(
        "PROBE rtt_ms_median={:.3} rtt_ms_p90={:.3} rtt_samples={}",
        median_ms(&mut rtt),
        p90_ms(&mut rtt),
        ops
    );

    // --- 2. PUT latency: small (4 KiB) objects -----------------------------
    let payload = vec![3u8; 4096];
    let mut put: Vec<f64> = Vec::with_capacity(ops);
    let mut put_keys: Vec<PathBuf> = Vec::with_capacity(ops);
    for i in 0..ops {
        let p = dir.join(format!("put-{i:04}.bin"));
        put.push(put_durable(fs, &p, &payload));
        put_keys.push(p);
    }
    println!(
        "PROBE put_ms_median={:.3} put_ms_p90={:.3} put_bytes=4096 put_samples={}",
        median_ms(&mut put),
        p90_ms(&mut put),
        ops
    );

    // --- 3. read-after-write visibility (design risk R2) -------------------
    // PUT a uniquely-stamped object (fully durable: upload awaited), then GET
    // in a loop until the read returns the just-written bytes. Strong
    // read-after-write => every trial succeeds on attempt 1.
    let mut max_attempts = 0usize;
    let mut vis_ok = true;
    for i in 0..vis_trials {
        let p = dir.join(format!("vis-{i:04}.bin"));
        let body = format!("frs-vis-{i}-{}", std::process::id()).into_bytes();
        put_durable(fs, &p, &body);
        cleanup.push(p.clone());
        let mut attempts = 0usize;
        let mut seen = false;
        while attempts < 50 {
            attempts += 1;
            if fs.file_exists(&p).unwrap_or(false) {
                let (got, _) = get_all(fs, &p);
                if got == body {
                    seen = true;
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        max_attempts = max_attempts.max(attempts);
        vis_ok &= seen;
    }
    println!(
        "PROBE raw_visibility={} raw_max_attempts={} raw_trials={}",
        if !vis_ok {
            "FAILED"
        } else if max_attempts == 1 {
            "strong"
        } else {
            "eventual"
        },
        max_attempts,
        vis_trials
    );

    // --- 4. single-stream upload bandwidth ---------------------------------
    let big = dir.join("bulk.bin");
    let data = vec![7u8; mb * 1024 * 1024];
    let up_secs = put_durable(fs, &big, &data);
    cleanup.push(big.clone());
    println!(
        "PROBE upload_mbps={:.1} upload_mb={} upload_secs={:.2}",
        mb as f64 / up_secs,
        mb,
        up_secs
    );

    // --- 5. single-stream download bandwidth -------------------------------
    let (got, down_secs) = get_all(fs, &big);
    assert_eq!(got.len(), data.len(), "download size mismatch");
    println!(
        "PROBE download_mbps={:.1} download_mb={} download_secs={:.2}",
        mb as f64 / down_secs,
        mb,
        down_secs
    );

    // --- 6. DELETE latency --------------------------------------------------
    let mut del: Vec<f64> = Vec::with_capacity(put_keys.len());
    for p in &put_keys {
        let s = Instant::now();
        fs.delete_file(p).expect("delete");
        del.push(s.elapsed().as_secs_f64());
    }
    println!(
        "PROBE delete_ms_median={:.3} delete_ms_p90={:.3} delete_samples={}",
        median_ms(&mut del),
        p90_ms(&mut del),
        del.len()
    );

    for p in &cleanup {
        let _ = fs.delete_file(p);
    }
    println!("PROBE done=1");
}

fn main() {
    let fs = build_fs();
    if std::env::args().any(|a| a == "--probe") {
        probe(&fs);
    } else {
        legacy_upload(&fs);
    }
}

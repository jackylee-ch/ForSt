// FRS-S3-BW-PROBE (2026-06-01): measure real upload bandwidth from this host to
// the configured S3 endpoint via the EXACT production opendal write path
// (buffered + concurrent multipart on close). Reads S3_* from the environment.
// Run: cargo run --release -p forst-rs-io --example s3bw
use forst_rs_io::filesystem::{FileSystem, WriteMode};
use forst_rs_io::OpendalFileSystem;
use std::time::Instant;

fn main() {
    let bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET");
    let region = std::env::var("S3_REGION").unwrap_or_default();
    let endpoint = std::env::var("S3_ENDPOINT").ok();
    let ak = std::env::var("S3_ACCESS_KEY").ok();
    let sk = std::env::var("S3_SECRET_KEY").ok();
    let prefix = std::env::var("S3_PREFIX").unwrap_or_default();

    let fs = OpendalFileSystem::s3_with_root(
        &bucket,
        &prefix,
        &region,
        endpoint.as_deref(),
        ak.as_deref(),
        sk.as_deref(),
    )
    .expect("build s3 backend");

    let mb = 200usize;
    let data = vec![7u8; mb * 1024 * 1024];
    let path = std::path::Path::new("frs-s3bw-probe.bin");

    let start = Instant::now();
    {
        let mut w = fs
            .open_writable_file(path, WriteMode::CreateOrTruncate)
            .expect("open writable");
        w.append(&data).expect("append");
        w.sync().expect("sync");
    }
    // Ensure the (possibly async) upload is fully durable before stopping the clock.
    let _ = fs.await_upload(path);
    let el = start.elapsed();
    let secs = el.as_secs_f64();
    eprintln!(
        "FRS-S3-BW: uploaded {} MB in {:.2}s = {:.1} MB/s (endpoint hidden)",
        mb,
        secs,
        mb as f64 / secs
    );
    let _ = fs.delete_file(path);
}

// FRS-PHASE2-S1 partial bench (design §5 Stage-1): metadata-op cost of the
// FileMappingManager `link()` vs a physical byte-copy, on fs-emulation —
// the headline mechanism of the link-based checkpoint (paper §5.1/§5.2:
// checkpoint = metadata + file linking, zero data movement).
//
// Run: cargo run --release -p forst-rs-io --example link_vs_copy
// Tunables: LINKBENCH_N (files, default 10000), LINKBENCH_KB (file size KiB,
// default 64). Uses a self-cleaning temp dir (no leftover bench files).
use forst_rs_io::filesystem::{FileSystem, WriteMode};
use forst_rs_io::{FileMappingManager, OpendalFileSystem};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let n = env_usize("LINKBENCH_N", 10_000);
    let kb = env_usize("LINKBENCH_KB", 64);
    let tmp = tempfile::tempdir().expect("tempdir");
    let fs: Arc<dyn FileSystem> =
        Arc::new(OpendalFileSystem::local(tmp.path()).expect("fs-emulation backend"));

    // --- populate the "working dir" with n SSTs ---------------------------
    let payload = vec![42u8; kb * 1024];
    eprintln!("link_vs_copy: creating {n} x {kb} KiB working files ...");
    for i in 0..n {
        let p = PathBuf::from(format!("db/{i:06}.sst"));
        let mut w = fs
            .open_writable_file(&p, WriteMode::CreateOrTruncate)
            .expect("create");
        w.append(&payload).expect("append");
        w.sync().expect("sync");
    }

    // --- cell A: physical copy (today's checkpoint shape) ------------------
    let start = Instant::now();
    let mut buf = vec![0u8; 1024 * 1024];
    for i in 0..n {
        let src = PathBuf::from(format!("db/{i:06}.sst"));
        let dst = PathBuf::from(format!("ckpt-copy/{i:06}.sst"));
        let mut r = fs.open_sequential_file(&src).expect("open src");
        let mut w = fs
            .open_writable_file(&dst, WriteMode::CreateOrTruncate)
            .expect("open dst");
        loop {
            let m = r.read(&mut buf).expect("read");
            if m == 0 {
                break;
            }
            w.append(&buf[..m]).expect("write");
        }
        w.sync().expect("sync");
    }
    let copy_secs = start.elapsed().as_secs_f64();

    // --- cell B: FileMappingManager::link (Stage-2 checkpoint shape) -------
    let mgr = FileMappingManager::new(fs.clone(), PathBuf::from("db/MAPPING.journal"))
        .expect("mapping manager");
    for i in 0..n {
        let p = format!("db/{i:06}.sst");
        mgr.register(Path::new(&p), &p, (kb * 1024) as u64)
            .expect("register");
    }
    let start = Instant::now();
    for i in 0..n {
        let src = PathBuf::from(format!("db/{i:06}.sst"));
        let dst = PathBuf::from(format!("ckpt-link/{i:06}.sst"));
        mgr.link(&src, &dst).expect("link");
    }
    mgr.sync_journal().expect("sync journal");
    let link_secs = start.elapsed().as_secs_f64();

    println!(
        "LINKBENCH n={} size_kb={} copy_total_ms={:.1} link_total_ms={:.1} \
         copy_per_file_us={:.1} link_per_file_us={:.1} speedup={:.0}x",
        n,
        kb,
        copy_secs * 1000.0,
        link_secs * 1000.0,
        copy_secs * 1e6 / n as f64,
        link_secs * 1e6 / n as f64,
        copy_secs / link_secs
    );
}

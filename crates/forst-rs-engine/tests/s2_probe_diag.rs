// S2 stage-3 diagnostic (temporary, gated #[ignore]): replicate the
// join_probe_open ssts_128 shape and attribute the pinned-vs-legacy delta.
use std::sync::{Arc, Mutex};

use forst_rs_common::EngineOptions;
use forst_rs_engine::{DbImpl, FillOutcome, RowSink};
use forst_rs_io::{FileSystem, MemoryFileSystem};

fn ns_prefix(jk: u32) -> [u8; 8] {
    let mut p = [0u8; 8];
    p[..4].copy_from_slice(&jk.to_be_bytes());
    p
}
fn state_key(jk: u32, entry: u32) -> [u8; 8] {
    let mut k = [0u8; 8];
    k[..4].copy_from_slice(&jk.to_be_bytes());
    k[4..].copy_from_slice(&entry.to_be_bytes());
    k
}

struct CountBytes(u64);
impl RowSink for CountBytes {
    fn push(&mut self, key: &[u8], value: &[u8]) -> bool {
        self.0 += (key.len() + value.len()) as u64;
        true
    }
}

#[test]
#[ignore = "diagnostic; run explicitly"]
fn s2_diag_ssts128_shape() {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 64 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    let cf = db.default_cf();
    let val = vec![0xCDu8; 64];
    let num_keys = 4096u32;
    for round in 0..128u32 {
        for jk in 0..num_keys {
            db.put(&cf, &state_key(jk, round), &val).expect("put");
        }
        db.flush_cf(&cf).expect("flush");
    }
    let live = db.list_live_files(false).unwrap();
    let mut by_level = std::collections::BTreeMap::new();
    for f in &live {
        *by_level.entry(f.level).or_insert(0u32) += 1;
    }
    eprintln!("[diag] live files per level: {by_level:?}");

    for (label, pinned) in [("legacy", false), ("pinned", true)] {
        // warm
        for jk in 0..256u32 {
            let prefix = ns_prefix(jk % num_keys);
            if pinned {
                let mut s = db
                    .prefix_scan_stream_with_mode(&cf, &prefix, Arc::new(Mutex::new(None)), true)
                    .unwrap();
                let mut sink = CountBytes(0);
                assert_eq!(s.fill_into(&mut sink).unwrap(), FillOutcome::Exhausted);
            } else {
                let it = db.prefix_scan_iter_owned_arc(&cf, &prefix).unwrap();
                for r in it {
                    r.unwrap();
                }
            }
        }
        let t0 = std::time::Instant::now();
        let mut rows_total = 0u64;
        let mut comps_total = 0u64;
        const PROBES: u32 = 2000;
        for jk in 0..PROBES {
            let prefix = ns_prefix((jk * 7) % num_keys);
            if pinned {
                let mut s = db
                    .prefix_scan_stream_with_mode(&cf, &prefix, Arc::new(Mutex::new(None)), true)
                    .unwrap();
                let mut sink = CountBytes(0);
                assert_eq!(s.fill_into(&mut sink).unwrap(), FillOutcome::Exhausted);
                let (rows, comps, allocs) = s.diag_counters();
                rows_total += rows;
                comps_total += comps;
                assert_eq!(allocs, 0);
            } else {
                let it = db.prefix_scan_iter_owned_arc(&cf, &prefix).unwrap();
                let mut n = 0u64;
                for r in it {
                    r.unwrap();
                    n += 1;
                }
                rows_total += n;
            }
        }
        let el = t0.elapsed();
        eprintln!(
            "[diag] {label}: {:.2} us/probe rows/probe={} comps/probe={}",
            el.as_secs_f64() * 1e6 / f64::from(PROBES),
            rows_total / u64::from(PROBES),
            comps_total / u64::from(PROBES),
        );
    }
}

#[test]
#[ignore = "diagnostic; run explicitly"]
fn s2_diag_churn_shape() {
    let opts = EngineOptions {
        db_path: "/db".to_string(),
        write_buffer_size: 8 * 1024 * 1024,
        ..EngineOptions::default()
    };
    let fs: Arc<dyn FileSystem> = Arc::new(MemoryFileSystem::new());
    let db = DbImpl::open_with_fs(opts, fs).expect("open");
    let cf = db.default_cf();
    const N: u32 = 1000;
    for r in 0..64u32 {
        for i in 0..N {
            let k = format!("p{:08}", i);
            if r > 0 {
                db.delete(&cf, k.as_bytes()).unwrap();
            }
            db.put(&cf, k.as_bytes(), format!("v{:08}-r{}", i, r).as_bytes())
                .unwrap();
        }
        let _ = db.switch_and_flush(&cf);
    }
    // Settle the LSM like the bench's measurement window (maintenance
    // compaction collapses L0 during criterion's 3s+ run).
    for _ in 0..8 {
        if db.compact_l0(&cf).unwrap().is_none() {
            break;
        }
    }
    let live = db.list_live_files(false).unwrap();
    let mut by_level = std::collections::BTreeMap::new();
    for f in &live {
        *by_level.entry(f.level).or_insert(0u32) += 1;
    }
    eprintln!("[churn] live files per level: {by_level:?}");

    for (label, pinned) in [("legacy", false), ("pinned", true), ("legacy2", false), ("pinned2", true)] {
        // warm + measure
        let mut last_counters = (0u64, 0u64, 0u64);
        let mut n_sources = 0usize;
        let t0 = std::time::Instant::now();
        const SCANS: u32 = 100;
        for _ in 0..SCANS {
            let mut s = db
                .range_scan_stream_with_mode(&cf, b"", None, Arc::new(Mutex::new(None)), pinned)
                .unwrap();
            let mut sink = CountBytes(0);
            assert_eq!(s.fill_into(&mut sink).unwrap(), FillOutcome::Exhausted);
            last_counters = s.diag_counters();
            n_sources = s.debug_source_count();
        }
        let el = t0.elapsed();
        eprintln!(
            "[churn] {label}: {:.2} us/scan sources={n_sources} (rows,comps,allocs)={last_counters:?}",
            el.as_secs_f64() * 1e6 / f64::from(SCANS),
        );
    }
}

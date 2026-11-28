//! A/B harness, Rust side. The Go side is benchmarks/rustcore/goab; both take the same
//! arguments and print one JSON object, so benchmarks/rustcore/run.sh can drive either.
use dostoevsky_core::{crc32c, Options, Store};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const CONFIG: &[u8] = br#"{"Node":"n1","Nodes":["n1"],"Topics":{"events":1}}"#;
const TOPIC: &str = "events";

fn status_kb(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|s| {
        s.lines().find(|l| l.starts_with(key)).and_then(|l| l.split_whitespace().nth(1).and_then(|v| v.parse().ok()))
    }).unwrap_or(0)
}

const BUCKETS: usize = 64 + 58 * 32;

fn bucket(v: u64) -> usize {
    if v < 64 { return v as usize; }
    let e = 63 - v.leading_zeros() as usize;
    64 + (e - 6) * 32 + ((v >> (e - 5)) & 31) as usize
}

fn bucket_upper(i: usize) -> u64 {
    if i < 64 { return i as u64; }
    let (e, sub) = ((i - 64) / 32 + 6, ((i - 64) % 32) as u64);
    ((32 + sub + 1) << (e - 5)) - 1
}

fn percentile(h: &[u64], total: u64, q: f64) -> u64 {
    let want = ((total as f64) * q).ceil().max(1.0) as u64;
    let mut seen = 0;
    for (i, &c) in h.iter().enumerate() {
        seen += c;
        if seen >= want { return bucket_upper(i); }
    }
    0
}

fn median(mut v: Vec<u64>) -> u64 {
    if v.is_empty() { return 0; }
    v.sort_unstable();
    v[v.len() / 2]
}

fn value(buf: &mut Vec<u8>, seq: u64, payload: usize) {
    buf.clear();
    buf.extend_from_slice(&seq.to_le_bytes());
    buf.resize(payload.max(8), b'x');
}

fn main() {
    let born = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("");
    let mut opt: HashMap<String, String> = HashMap::new();
    let mut i = 2;
    while i + 1 < args.len() {
        opt.insert(args[i].trim_start_matches("--").to_string(), args[i + 1].clone());
        i += 2;
    }
    let num = |k: &str, d: u64| opt.get(k).map(|v| v.parse::<f64>().unwrap() as u64).unwrap_or(d);
    let dir = opt.get("dir").expect("--dir").clone();
    let dir = Path::new(&dir);
    let payload = num("payload", 100) as usize;
    let open_start = Instant::now();
    let store = Store::open(dir, CONFIG, Options::default()).expect("open");
    let open_ns = open_start.elapsed().as_nanos() as u64;
    let rss_ready = status_kb("VmRSS:");
    match mode {
        "write" => {
            let producers = num("producers", 1) as usize;
            let seconds = num("seconds", 10);
            let store = Arc::new(store);
            let acked = Arc::new(AtomicU64::new(0));
            let stop = Arc::new(AtomicBool::new(false));
            let last_ack = Arc::new(AtomicU64::new(0));
            let start = Instant::now();
            let mut handles = Vec::new();
            for p in 0..producers {
                let (store, acked, stop, last_ack) = (store.clone(), acked.clone(), stop.clone(), last_ack.clone());
                handles.push(std::thread::spawn(move || {
                    let mut hist = vec![0u64; BUCKETS];
                    let (mut n, mut max, mut max_gap, mut gaps10) = (0u64, 0u64, 0u64, 0u64);
                    let mut over = [0u64; 3];
                    let mut buf = Vec::new();
                    while !stop.load(Ordering::Relaxed) {
                        value(&mut buf, (p as u64) << 40 | n, payload);
                        let t0 = Instant::now();
                        store.publish(TOPIC, b"", &buf).expect("publish");
                        let lat = t0.elapsed().as_nanos() as u64;
                        let now = start.elapsed().as_nanos() as u64;
                        let prev = last_ack.swap(now, Ordering::Relaxed);
                        let gap = now.saturating_sub(prev);
                        max_gap = max_gap.max(gap);
                        gaps10 += (gap > 10_000_000) as u64;
                        hist[bucket(lat)] += 1;
                        max = max.max(lat);
                        over[0] += (lat > 10_000_000) as u64;
                        over[1] += (lat > 50_000_000) as u64;
                        over[2] += (lat > 100_000_000) as u64;
                        n += 1;
                        acked.fetch_add(1, Ordering::Relaxed);
                    }
                    (hist, n, max, max_gap, gaps10, over)
                }));
            }
            let (mut per_second, mut rss) = (Vec::new(), Vec::new());
            let mut prev = 0;
            for s in 1..=seconds {
                let target = start + Duration::from_secs(s);
                std::thread::sleep(target.saturating_duration_since(Instant::now()));
                let a = acked.load(Ordering::Relaxed);
                per_second.push(a - prev);
                prev = a;
                rss.push(status_kb("VmRSS:"));
            }
            stop.store(true, Ordering::Relaxed);
            let mut hist = vec![0u64; BUCKETS];
            let (mut total, mut max, mut max_gap, mut gaps10) = (0u64, 0u64, 0u64, 0u64);
            let mut over = [0u64; 3];
            for h in handles {
                let (hh, n, m, g, g10, o) = h.join().unwrap();
                for (a, b) in hist.iter_mut().zip(hh) { *a += b; }
                total += n; max = max.max(m); max_gap = max_gap.max(g); gaps10 += g10;
                for k in 0..3 { over[k] += o[k]; }
            }
            let elapsed = start.elapsed().as_secs_f64();
            let st = store.stats();
            let steady = median(rss[rss.len() * 2 / 3..].to_vec());
            let (sealed, active) = store.segments();
            println!("{{\"engine\":\"rust\",\"mode\":\"write\",\"producers\":{producers},\"payload\":{payload},\"seconds\":{elapsed:.3},\"acked\":{total},\"acked_per_s\":{:.1},\
\"p50_us\":{:.1},\"p99_us\":{:.1},\"p999_us\":{:.1},\"max_us\":{:.1},\"over_10ms\":{},\"over_50ms\":{},\"over_100ms\":{},\"max_ack_gap_ms\":{:.2},\"ack_gaps_over_10ms\":{gaps10},\
\"second_min\":{},\"second_median\":{},\"second_max\":{},\"sync_groups\":{},\"records_per_sync\":{:.2},\"open_ms\":{:.3},\"rss_ready_kb\":{rss_ready},\"rss_steady_kb\":{steady},\"rss_first_kb\":{},\"rss_last_kb\":{},\"rss_peak_kb\":{},\"sealed_segments\":{sealed},\"active_segment\":{active}}}",
                total as f64 / elapsed,
                percentile(&hist, total, 0.50) as f64 / 1e3, percentile(&hist, total, 0.99) as f64 / 1e3, percentile(&hist, total, 0.999) as f64 / 1e3, max as f64 / 1e3,
                over[0], over[1], over[2], max_gap as f64 / 1e6,
                per_second.iter().min().unwrap(), median(per_second.clone()), per_second.iter().max().unwrap(),
                st.groups, st.calls as f64 / st.groups.max(1) as f64, open_ns as f64 / 1e6, rss[0], rss[rss.len() - 1], status_kb("VmHWM:"));
        }
        "fill" => {
            let records = num("records", 1_000_000);
            let batch = num("batch", 1000);
            let start = Instant::now();
            let mut seq = store.next_offset(TOPIC);
            let first = seq;
            let mut bufs: Vec<Vec<u8>> = vec![Vec::new(); batch as usize];
            while seq < first + records {
                let k = batch.min(first + records - seq) as usize;
                for (j, b) in bufs.iter_mut().take(k).enumerate() { value(b, seq + j as u64, payload); }
                let refs: Vec<&[u8]> = bufs[..k].iter().map(|b| &b[..]).collect();
                store.publish_batch(TOPIC, &refs).expect("publish_batch");
                seq += k as u64;
            }
            let el = start.elapsed().as_secs_f64();
            println!("{{\"engine\":\"rust\",\"mode\":\"fill\",\"records\":{records},\"payload\":{payload},\"batch\":{batch},\"seconds\":{el:.3},\"records_per_s\":{:.0},\"rss_peak_kb\":{}}}", records as f64 / el, status_kb("VmHWM:"));
        }
        "reopen" => {
            let offset = num("offset", 0);
            let mut got = 0u64;
            let next = store.read(TOPIC, offset, 1, |o, _, _, _| got = o).expect("read");
            let fetch_ns = born.elapsed().as_nanos() as u64;
            assert!(next == offset + 1 && got == offset, "record {offset} not found");
            let mut publish_us = -1.0;
            if num("publish", 0) == 1 {
                let t0 = Instant::now();
                store.publish(TOPIC, b"", &vec![b'x'; payload]).expect("publish");
                publish_us = t0.elapsed().as_nanos() as f64 / 1e3;
            }
            println!("{{\"engine\":\"rust\",\"mode\":\"reopen\",\"offset\":{offset},\"open_ms\":{:.3},\"start_to_first_fetch_ms\":{:.3},\"first_publish_us\":{publish_us:.1},\"rss_kb\":{},\"truncated_bytes\":{}}}",
                open_ns as f64 / 1e6, fetch_ns as f64 / 1e6, status_kb("VmRSS:"), store.truncated_bytes());
        }
        "read" | "digest" => {
            let start = Instant::now();
            let (mut next, mut count, mut bytes, mut digest) = (num("from", 0), 0u64, 0u64, 0u32);
            let want_digest = mode == "digest";
            loop {
                let before = next;
                next = store.read(TOPIC, next, 1000, |o, t, k, v| {
                    count += 1;
                    bytes += (k.len() + v.len()) as u64;
                    if want_digest {
                        digest = crc32c::update(digest, &o.to_le_bytes());
                        digest = crc32c::update(digest, &t.to_le_bytes());
                        digest = crc32c::update(digest, &(k.len() as u32).to_le_bytes());
                        digest = crc32c::update(digest, k);
                        digest = crc32c::update(digest, v);
                    }
                }).expect("read");
                if next == before { break; }
            }
            let el = start.elapsed().as_secs_f64();
            println!("{{\"engine\":\"rust\",\"mode\":\"{mode}\",\"records\":{count},\"bytes\":{bytes},\"digest\":\"{digest:08x}\",\"seconds\":{el:.3},\"records_per_s\":{:.0},\"mib_per_s\":{:.1},\"rss_peak_kb\":{},\"truncated_bytes\":{}}}",
                count as f64 / el, bytes as f64 / el / 1048576.0, status_kb("VmHWM:"), store.truncated_bytes());
        }
        _ => {
            eprintln!("usage: rsab write|fill|reopen|read|digest --dir D [--producers P --seconds S --payload B --records N --batch K --offset O --publish 1]");
            std::process::exit(2);
        }
    }
}

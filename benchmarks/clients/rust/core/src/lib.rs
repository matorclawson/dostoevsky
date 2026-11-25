//! The workload, histogram, receipt line and command line shared by every engine adapter of the Rust worker.
//! It follows benchmarks/embedded/internal/run and the Python, JavaScript and Java workers: 512-byte records
//! whose first eight bytes are the big-endian message index, concurrent synchronous publishes acknowledged only
//! after the engine's durable acknowledgment, no retries, every acknowledged record read back, and the stop
//! reasons reached / window / failed / verification-failed. Standard library only, so it is tested offline and
//! without any client library.
use std::collections::BTreeMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const RECORD_BYTES: usize = 512;
pub const HIST_BINS: usize = 2400;
/// A longer window overflows nanosecond arithmetic in some languages; every worker refuses it.
pub const MAX_WINDOW_SECONDS: f64 = 24.0 * 3600.0;

pub const REACHED: &str = "reached";
pub const WINDOW: &str = "window";
pub const FAILED: &str = "failed";
pub const VERIFY: &str = "verification-failed";

// ---- the 1% histogram ----

/// 1% wide bins from 1 microsecond up. A quantile is the UPPER EDGE of the bin holding it, in whole nanoseconds,
/// accurate to 1% and never below the true value within the bin domain (1 microsecond up to the last bin's edge).
#[derive(Clone)]
pub struct Hist {
    n: Vec<u64>,
}

impl Default for Hist {
    fn default() -> Self {
        Hist { n: vec![0; HIST_BINS] }
    }
}

impl Hist {
    pub fn index(ns: u64) -> usize {
        let us = ns as f64 / 1e3;
        if us < 1.0 {
            return 0;
        }
        ((us.ln() / 1.01f64.ln()) as usize).min(HIST_BINS - 1)
    }
    pub fn add_ns(&mut self, ns: u64) {
        self.n[Self::index(ns)] += 1;
    }
    pub fn merge(&mut self, other: &Hist) {
        for (a, b) in self.n.iter_mut().zip(&other.n) {
            *a += *b;
        }
    }
    pub fn total(&self) -> u64 {
        self.n.iter().sum()
    }
    pub fn quantile_ns(&self, q: f64) -> u64 {
        let total = self.total();
        if total == 0 {
            return 0;
        }
        let rank = ((q * total as f64).ceil() as u64).max(1);
        let mut seen = 0;
        for (i, c) in self.n.iter().enumerate() {
            seen += c;
            if seen >= rank {
                return (1.01f64.powf((i + 1) as f64) * 1e3) as u64;
            }
        }
        0
    }
    /// As receipts print it: microseconds with three decimals, nothing rounded away.
    pub fn quantile_us_text(&self, q: f64) -> String {
        format!("{:.3}", self.quantile_ns(q) as f64 / 1000.0)
    }
}

pub fn payload(i: u64) -> Vec<u8> {
    let mut b = vec![0u8; RECORD_BYTES];
    b[..8].copy_from_slice(&i.to_be_bytes());
    b
}

// ---- engines ----

/// What an engine call can say about its failure. Only the class name and the kind ever reach a receipt, never the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Timeout,
    /// No answer before the deadline: the publish may or may not have been stored.
    AmbiguousTimeout,
    Connection,
    Protocol,
    Other,
}

#[derive(Debug, Clone)]
pub struct EngineError {
    pub class: String,
    pub kind: Kind,
}

impl EngineError {
    pub fn new(class: &str, kind: Kind) -> Self {
        EngineError { class: class.to_string(), kind }
    }
}

/// `Class:word`: the error class (identifier characters only) and one fixed word.
pub fn reason(e: &EngineError) -> String {
    let class: String = e.class.chars().filter(|c| c.is_ascii_alphanumeric() || *c == '_').take(64).collect();
    let class = if class.is_empty() || class.chars().next().is_some_and(|c| c.is_ascii_digit()) { "Error".to_string() } else { class };
    let word = match e.kind {
        Kind::AmbiguousTimeout => "ambiguous-timeout",
        Kind::Timeout => "timeout",
        Kind::Connection => "connection",
        Kind::Protocol => "protocol",
        Kind::Other => "engine-error",
    };
    format!("{class}:{word}")
}

/// One engine. `publish` returns the offset only after the engine's durable acknowledgment. Never retried.
pub trait Adapter: Send + Sync {
    fn publish(&self, data: &[u8]) -> Result<u64, EngineError>;
    fn read(&self, offset: u64) -> Result<Vec<u8>, EngineError>;
    fn close(&self) {}
    fn client_name(&self) -> String;
    fn client_build(&self) -> String {
        "n/a".to_string()
    }
}

#[derive(Default)]
pub struct Outcome {
    pub acked: u64,
    pub elapsed_s: f64,
    pub stop: String,
    pub detail: String,
    pub hist: Hist,
    /// (message index, engine offset) of every acknowledgment
    pub records: Vec<(u64, u64)>,
    pub ambiguous: bool,
}

struct Shared {
    next: u64,
    stop: Option<(String, String)>,
    ambiguous: bool,
}

/// Publishes with `workers` synchronous producers until `target` messages are acknowledged, the window (seconds)
/// ends, or a publish fails. The failing publish is not counted and nothing is retried. A failure is never hidden by
/// an earlier "reached" or "window".
pub fn run_workload(adapter: Arc<dyn Adapter>, workers: usize, target: u64, window_s: f64) -> Outcome {
    let shared = Arc::new(Mutex::new(Shared { next: 0, stop: None, ambiguous: false }));
    let start = Instant::now();
    let deadline = if window_s > 0.0 { Some(start + Duration::from_secs_f64(window_s)) } else { None };
    let parts: Arc<Mutex<Vec<(Hist, Vec<(u64, u64)>)>>> = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..workers {
        let (shared, adapter, parts) = (shared.clone(), adapter.clone(), parts.clone());
        handles.push(std::thread::spawn(move || {
            let mut hist = Hist::default();
            let mut mine: Vec<(u64, u64)> = Vec::new();
            loop {
                let i = {
                    let mut s = shared.lock().unwrap();
                    if s.stop.is_none() && deadline.is_some_and(|d| Instant::now() >= d) {
                        s.stop = Some((WINDOW.to_string(), String::new()));
                    }
                    if s.stop.is_some() {
                        break;
                    }
                    let i = s.next;
                    if i >= target {
                        s.stop = Some((REACHED.to_string(), String::new()));
                        break;
                    }
                    s.next = i + 1;
                    i
                };
                let begin = Instant::now();
                match adapter.publish(&payload(i)) {
                    Ok(offset) => {
                        hist.add_ns(begin.elapsed().as_nanos() as u64);
                        mine.push((i, offset));
                    }
                    Err(e) => {
                        let mut s = shared.lock().unwrap();
                        if s.stop.as_ref().map_or(true, |(k, _)| k != FAILED) {
                            s.stop = Some((FAILED.to_string(), format!("publish {i}: {}", reason(&e))));
                        }
                        s.ambiguous |= e.kind == Kind::AmbiguousTimeout;
                        break;
                    }
                }
            }
            parts.lock().unwrap().push((hist, mine));
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    let mut out = Outcome { elapsed_s: start.elapsed().as_secs_f64(), ..Default::default() };
    for (h, mine) in parts.lock().unwrap().iter() {
        out.hist.merge(h);
        out.records.extend(mine.iter().copied());
    }
    out.acked = out.records.len() as u64;
    let s = shared.lock().unwrap();
    let (kind, text) = s.stop.clone().unwrap_or((REACHED.to_string(), String::new()));
    out.detail = if kind == FAILED { format!("{text} (after {} acknowledged)", out.acked) } else { text };
    out.stop = kind;
    out.ambiguous = s.ambiguous;
    out
}

/// Reads every acknowledged record back in offset order; returns "" or a plain-words failure.
pub fn verify(adapter: &dyn Adapter, records: &[(u64, u64)]) -> String {
    let mut sorted = records.to_vec();
    sorted.sort_by_key(|r| r.1);
    for (i, offset) in sorted {
        match adapter.read(offset) {
            Err(e) => return format!("record {i} at {offset}: read {}", reason(&e)),
            Ok(got) if got != payload(i) => return format!("record {i} at {offset}: the bytes read back differ from the bytes published"),
            Ok(_) => {}
        }
    }
    String::new()
}

/// A stream ID is "<milliseconds>-<sequence>"; packed into one integer as the other harnesses do.
pub fn pack_id(id: &str) -> Result<u64, String> {
    let (ms, seq) = id.split_once('-').ok_or("stream id has no sequence")?;
    let (ms, seq): (u64, u64) = (ms.parse().map_err(|_| "bad milliseconds")?, seq.parse().map_err(|_| "bad sequence")?);
    if seq >= 1 << 16 {
        return Err("stream id sequence does not fit".into());
    }
    Ok((ms << 16) | seq)
}

pub fn unpack_id(offset: u64) -> String {
    format!("{}-{}", offset >> 16, offset & 0xFFFF)
}

/// A value that stays ONE whitespace-free token, so the line parses as key=value pairs.
fn token(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join("_")
}

/// One line. `result=FAILED` whenever the run failed or its read-back did not verify; `records/s` counts verified
/// messages only.
#[allow(clippy::too_many_arguments)]
pub fn receipt_line(engine: &str, workers: usize, res: &Outcome, verified: u64, client: &str, rss_kib: u64, rss_source: &str, build: &str) -> String {
    let ok = res.stop == REACHED || res.stop == WINDOW;
    let rate = if res.elapsed_s > 0.0 { verified as f64 / res.elapsed_s } else { 0.0 };
    let mut f: Vec<String> = vec![
        format!("engine={engine}"),
        format!("workers={workers}"),
        format!("result={}", if ok { "ok" } else { "FAILED" }),
        format!("acked={}", res.acked),
        format!("verified={verified}"),
        format!("records/s={rate:.0}"),
        format!("elapsed_s={:.2}", res.elapsed_s),
        format!("p50_us={}", res.hist.quantile_us_text(0.50)),
        format!("p95_us={}", res.hist.quantile_us_text(0.95)),
        format!("p99_us={}", res.hist.quantile_us_text(0.99)),
        format!("p99_9_us={}", res.hist.quantile_us_text(0.999)),
        format!("p99_99_us={}", res.hist.quantile_us_text(0.9999)),
        "quantiles=bin-upper-1pct".to_string(),
        format!("client_rss_KiB={rss_kib}"),
        format!("client_rss_source={rss_source}"),
        "lang=rust".to_string(),
        format!("client={}", token(client)),
        format!("client_build={}", token(build)),
    ];
    if res.ambiguous {
        f.push("ambiguous=1".to_string());
    }
    f.push(format!("stop={}", res.stop));
    if !res.detail.is_empty() {
        let d: String = res.detail.chars().map(|c| if c.is_ascii_alphanumeric() || "_.:,()/-".contains(c) { c } else { '_' }).take(200).collect();
        f.push(format!("detail={d}"));
    }
    f.join(" ")
}

// ---- client memory ----

/// This process's peak resident size: the kernel's VmHWM on Linux, else the largest `ps` sample (a thread samples
/// every 200 ms from `start`). The two are different measurements and the receipt names which one it is.
pub struct Rss {
    max: Arc<AtomicU64>,
    stop: Arc<std::sync::atomic::AtomicBool>,
}

fn vm_hwm_kib() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = text.lines().find(|l| l.starts_with("VmHWM:"))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

fn ps_rss_kib() -> Option<u64> {
    let out = std::process::Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output().ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

impl Rss {
    pub fn start() -> Rss {
        let r = Rss { max: Arc::new(AtomicU64::new(0)), stop: Arc::new(std::sync::atomic::AtomicBool::new(false)) };
        if vm_hwm_kib().is_none() {
            let (max, stop) = (r.max.clone(), r.stop.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Some(k) = ps_rss_kib() {
                        max.fetch_max(k, Ordering::Relaxed);
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            });
        }
        r
    }
    pub fn peak_kib(&self) -> u64 {
        vm_hwm_kib().unwrap_or_else(|| self.max.load(Ordering::Relaxed))
    }
    pub fn source(&self) -> &'static str {
        if vm_hwm_kib().is_some() {
            "VmHWM"
        } else {
            "ps-sampled"
        }
    }
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

// ---- the command line ----

/// Creates one adapter from (address, token, workers); it may connect.
pub type Factory = Box<dyn Fn(&str, &str, usize) -> Result<Arc<dyn Adapter>, String> + Send + Sync>;

fn parse_args(argv: &[String]) -> Result<(BTreeMap<String, String>, bool), String> {
    let mut map = BTreeMap::new();
    let mut require_known_build = false;
    let mut i = 0;
    while i < argv.len() {
        let k = &argv[i];
        if k == "--require-known-build" {
            require_known_build = true;
            i += 1;
            continue;
        }
        if !k.starts_with("--") || i + 1 >= argv.len() {
            return Err(format!("usage error near {k}"));
        }
        map.insert(k[2..].to_string(), argv[i + 1].clone());
        i += 2;
    }
    Ok((map, require_known_build))
}

/// One trial; prints one receipt line to `out`. Usage errors return 2. A publish failure is a result, never an exit code.
pub fn run(argv: &[String], env: &BTreeMap<String, String>, out: &mut dyn Write, err: &mut dyn Write, engines: &BTreeMap<String, Factory>) -> i32 {
    let usage = "usage: --engine E --addr host:port [--workers N>=1] [--target N>=1] [--window S, 0 to 86400] [--require-known-build]";
    let (a, require_known_build) = match parse_args(argv) {
        Ok(v) => v,
        Err(e) => {
            let _ = writeln!(err, "{e}\n{usage}");
            return 2;
        }
    };
    let get = |k: &str, d: &str| a.get(k).cloned().unwrap_or_else(|| d.to_string());
    let (workers, target, window): (Result<usize, _>, Result<u64, _>, Result<f64, _>) =
        (get("workers", "1").parse(), get("target", "2000").parse(), get("window", "0").parse());
    let (workers, target, window) = match (workers, target, window) {
        (Ok(w), Ok(t), Ok(s)) if (1..=100_000).contains(&w) && t >= 1 && (0.0..=MAX_WINDOW_SECONDS).contains(&s) => (w, t, s), // NaN fails the range
        _ => {
            let _ = writeln!(err, "--workers and --target must be whole numbers of at least 1 and --window a number from 0 to 86400\n{usage}");
            return 2;
        }
    };
    let (Some(engine), Some(addr)) = (a.get("engine"), a.get("addr")) else {
        let _ = writeln!(err, "{usage}");
        return 2;
    };
    let Some(factory) = engines.get(engine) else {
        let _ = writeln!(err, "unknown engine '{engine}' (have {})", engines.keys().cloned().collect::<Vec<_>>().join(", "));
        return 2;
    };
    let adapter = match factory(addr, env.get("BENCH_TOKEN").map(String::as_str).unwrap_or(""), workers) {
        Ok(ad) => ad,
        Err(_) => {
            // The reason is deliberately not printed: a connection error can carry the address and credentials.
            let _ = writeln!(err, "could not set up the {engine} adapter");
            return 1;
        }
    };
    if require_known_build && adapter.client_build() == "unknown" {
        adapter.close();
        let _ = writeln!(err, "the client build is unknown (no git commit could be read); refusing to run a gated trial");
        return 2;
    }
    let rss = Rss::start();
    let mut res = run_workload(adapter.clone(), workers, target, window);
    let mut verified = 0;
    if res.stop != FAILED || !res.records.is_empty() {
        // also read back what a failed run acknowledged
        let why = verify(adapter.as_ref(), &res.records);
        if why.is_empty() {
            verified = res.acked;
        } else {
            res.stop = VERIFY.to_string();
            res.detail = why;
        }
    }
    adapter.close();
    rss.stop();
    let _ = writeln!(out, "{}", receipt_line(&format!("rust-{engine}"), workers, &res, verified, &adapter.client_name(), rss.peak_kib(), rss.source(), &adapter.client_build()));
    0
}

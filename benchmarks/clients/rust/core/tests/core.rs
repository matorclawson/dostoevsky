use dostoevsky_bench_core::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

const VECTOR: &str = include_str!("../../../hist_vector.json");
const EXAMPLES: &str = include_str!("../../../receipt_examples_rust.txt");

/// An in-memory engine: publish returns the offset; read returns what was stored.
struct Memory {
    log: Mutex<Vec<Vec<u8>>>,
    attempts: AtomicU64,
    fail_at: u64,
    corrupt: bool,
    build: String,
    delay_ms: u64,
}

impl Memory {
    fn new() -> Arc<Memory> {
        Arc::new(Memory { log: Mutex::new(vec![]), attempts: AtomicU64::new(0), fail_at: 0, corrupt: false, build: "n/a".into(), delay_ms: 0 })
    }
    fn with(fail_at: u64, corrupt: bool, build: &str, delay_ms: u64) -> Arc<Memory> {
        Arc::new(Memory { log: Mutex::new(vec![]), attempts: AtomicU64::new(0), fail_at, corrupt, build: build.into(), delay_ms })
    }
}

impl Adapter for Memory {
    fn publish(&self, data: &[u8]) -> Result<u64, EngineError> {
        let n = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        if self.fail_at != 0 && n == self.fail_at {
            return Err(EngineError::new("ConnectionReset", Kind::Connection));
        }
        if self.delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(self.delay_ms));
        }
        let mut l = self.log.lock().unwrap();
        l.push(data.to_vec());
        Ok(l.len() as u64 - 1)
    }
    fn read(&self, offset: u64) -> Result<Vec<u8>, EngineError> {
        let mut b = self.log.lock().unwrap()[offset as usize].clone();
        if self.corrupt {
            *b.last_mut().unwrap() ^= 1;
        }
        Ok(b)
    }
    fn client_name(&self) -> String {
        "memory-0".into()
    }
    fn client_build(&self) -> String {
        self.build.clone()
    }
}

fn numbers_in(block: &str) -> Vec<(String, u64)> {
    // "key": value pairs of one JSON object, without a JSON library
    block
        .split(',')
        .filter_map(|kv| {
            let (k, v) = kv.split_once(':')?;
            Some((k.trim().trim_matches('"').to_string(), v.trim().parse().ok()?))
        })
        .collect()
}

fn block<'a>(text: &'a str, key: &str, open: char, close: char) -> &'a str {
    let at = text.find(&format!("\"{key}\"")).unwrap();
    let start = at + text[at..].find(open).unwrap() + 1;
    &text[start..start + text[start..].find(close).unwrap()]
}

#[test]
fn the_histogram_matches_the_shared_vector() {
    assert!(VECTOR.contains("\"bins\": 2400"));
    let index = numbers_in(block(VECTOR, "index", '{', '}'));
    assert!(index.len() >= 40, "{} index cases", index.len());
    for (ns, want) in &index {
        assert_eq!(Hist::index(ns.parse().unwrap()) as u64, *want, "index of {ns} ns");
    }
    let mut h = Hist::default();
    for ns in block(VECTOR, "durations_ns", '[', ']').split(',') {
        h.add_ns(ns.trim().parse().unwrap());
    }
    let quantiles = numbers_in(block(VECTOR, "quantiles_ns", '{', '}'));
    assert!(quantiles.len() >= 7);
    for (q, want) in quantiles {
        assert_eq!(h.quantile_ns(q.parse().unwrap()), want, "quantile {q}");
    }
    let mut one = Hist::default();
    one.add_ns(1900);
    let got = one.quantile_ns(0.5);
    assert!((1900..=(1900.0 * 1.0101) as u64).contains(&got), "a short sample keeps its sub-microsecond precision: {got}");
    assert_eq!(Hist::default().quantile_ns(0.99), 0);
}

#[test]
fn a_record_is_512_bytes_starting_with_its_index() {
    let p = payload(258);
    assert_eq!((p.len(), &p[..8]), (512, &[0u8, 0, 0, 0, 0, 0, 1, 2][..]));
}

#[test]
fn reached_counts_every_acknowledgment_exactly_once() {
    for workers in [1, 8, 32] {
        let m = Memory::new();
        let r = run_workload(m.clone(), workers, 500, 0.0);
        assert_eq!((r.stop.as_str(), r.acked, m.log.lock().unwrap().len()), (REACHED, 500, 500), "{workers} workers");
        let mut idx: Vec<u64> = r.records.iter().map(|x| x.0).collect();
        idx.sort();
        assert_eq!(idx, (0..500).collect::<Vec<u64>>());
        assert_eq!(verify(m.as_ref(), &r.records), "");
    }
}

#[test]
fn a_failure_is_a_result_never_retried_and_the_failing_publish_is_not_counted() {
    let m = Memory::with(6, false, "n/a", 0);
    let r = run_workload(m.clone(), 1, 100, 0.0);
    assert_eq!(r.stop, FAILED);
    assert_eq!((r.acked, m.attempts.load(Ordering::SeqCst)), (5, 6));
    assert!(r.detail.contains("ConnectionReset:connection") && r.detail.contains("after 5 acknowledged"), "{}", r.detail);
    let many = Memory::with(40, false, "n/a", 0);
    let r = run_workload(many.clone(), 8, 10_000, 0.0);
    assert_eq!(r.stop, FAILED);
    assert_eq!(many.attempts.load(Ordering::SeqCst) - r.acked, 1, "attempts exceed acks by the one failure");
}

#[test]
fn the_window_ends_the_run() {
    let begin = std::time::Instant::now();
    let r = run_workload(Memory::with(0, false, "n/a", 10), 4, u64::MAX, 0.3);
    assert_eq!(r.stop, WINDOW);
    assert!(r.acked > 0 && begin.elapsed().as_secs() < 5);
}

#[test]
fn a_failure_after_the_run_was_already_reached_still_makes_it_failed() {
    // Target 2, two workers: one finishes index 0 and sees the target reached; the other's publish (index 1) is
    // still in flight and then fails.
    struct Late(Arc<Memory>);
    impl Adapter for Late {
        fn publish(&self, data: &[u8]) -> Result<u64, EngineError> {
            if data[7] == 1 {
                std::thread::sleep(std::time::Duration::from_millis(300));
                return Err(EngineError::new("ResetByPeer", Kind::Connection));
            }
            self.0.publish(data)
        }
        fn read(&self, o: u64) -> Result<Vec<u8>, EngineError> {
            self.0.read(o)
        }
        fn client_name(&self) -> String {
            "late".into()
        }
    }
    let r = run_workload(Arc::new(Late(Memory::new())), 2, 2, 0.0);
    assert_eq!((r.stop.as_str(), r.acked), (FAILED, 1));
    assert_eq!(receipt_line("x", 2, &r, 1, "c", 1, "VmHWM", "n/a").split(' ').nth(2), Some("result=FAILED"));
}

#[test]
fn verification_finds_a_changed_byte_and_error_text_never_reaches_the_receipt() {
    let bad = Memory::with(0, true, "n/a", 0);
    let r = run_workload(bad.clone(), 2, 20, 0.0);
    assert!(verify(bad.as_ref(), &r.records).contains("differ"));
    let e = EngineError::new("Auth failed for redis://user:hunter2@10.0.0.9", Kind::Other);
    let why = reason(&e);
    assert_eq!(why, "Error:engine-error", "a class that is not a plain identifier is reported as Error, never kept in part");
    assert_eq!(reason(&EngineError::new("Timeout", Kind::AmbiguousTimeout)), "Timeout:ambiguous-timeout");
    assert_eq!(reason(&EngineError::new("1bad", Kind::Timeout)), "Error:timeout");
    assert_eq!(reason(&EngineError::new("KafkaError", Kind::Connection)), "KafkaError:connection");
    assert_eq!(reason(&EngineError::new("", Kind::Other)), "Error:engine-error");
}

fn fixed(stop: &str, detail: &str, acked: u64, ambiguous: bool) -> Outcome {
    let mut o = Outcome { acked, elapsed_s: 2.0, stop: stop.into(), detail: detail.into(), ambiguous, ..Default::default() };
    for _ in 0..acked {
        o.hist.add_ns(2_000_000);
    }
    o
}

#[test]
fn the_lines_equal_the_shared_examples_byte_for_byte() {
    let want: BTreeMap<&str, &str> = EXAMPLES.lines().filter(|l| !l.trim().is_empty() && !l.starts_with('#')).map(|l| (l.split(' ').next().unwrap(), l)).collect();
    assert_eq!(receipt_line("rust-kafka", 8, &fixed(REACHED, "", 1000, false), 1000, "rdkafka-0.39.0+librdkafka-2.12.1", 51200, "VmHWM", "n/a"), want["engine=rust-kafka"]);
    assert_eq!(receipt_line("rust-dostoevsky", 8, &fixed(REACHED, "", 1000, false), 1000, "dostoevsky-client-0.1.0", 51200, "VmHWM", "0123456789ab"), want["engine=rust-dostoevsky"]);
    let failed = fixed(FAILED, "publish 4: Timeout:ambiguous-timeout (after 4 acknowledged)", 4, true);
    assert_eq!(receipt_line("rust-redis", 8, &failed, 4, "redis-1.8.1", 51200, "ps-sampled", "n/a"), want["engine=rust-redis"]);
    let spaced = receipt_line("rust-x", 8, &fixed(REACHED, "", 1000, false), 1000, "name with spaces (pool)", 1, "VmHWM", "build id");
    assert!(spaced.split(' ').all(|t| t.contains('=')), "{spaced}");
    assert!(receipt_line("x", 1, &fixed(VERIFY, "x", 100, false), 0, "c", 1, "VmHWM", "n/a").contains("records/s=0 "));
}

#[test]
fn stream_ids_pack_and_unpack() {
    assert_eq!(unpack_id(pack_id("1700000000123-7").unwrap()), "1700000000123-7");
    assert!(pack_id("5-65536").is_err());
    assert!(pack_id("nonsense").is_err());
}

fn trial(argv: &[&str], adapter: Arc<dyn Adapter>) -> (i32, String, String) {
    let argv: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
    let mut engines: BTreeMap<String, Factory> = BTreeMap::new();
    engines.insert("memory".into(), Box::new(move |_, _, _| Ok(adapter.clone())));
    let env = BTreeMap::from([("BENCH_TOKEN".to_string(), "secret-token-value".to_string())]);
    let (mut out, mut err) = (Vec::new(), Vec::new());
    let code = run(&argv, &env, &mut out, &mut err, &engines);
    (code, String::from_utf8(out).unwrap(), String::from_utf8(err).unwrap())
}

#[test]
fn one_trial_prints_one_line_and_never_the_token() {
    let (code, out, _) = trial(&["--engine", "memory", "--addr", "x:1", "--workers", "4", "--target", "200"], Memory::new());
    assert_eq!(code, 0);
    assert_eq!(out.trim().lines().count(), 1);
    assert!(out.contains("verified=200") && !out.contains("secret-token-value"), "{out}");
    let (code, out, _) = trial(&["--engine", "memory", "--addr", "x:1", "--target", "50"], Memory::with(11, false, "n/a", 0));
    assert!(code == 0 && out.contains("stop=failed") && out.contains("result=FAILED") && out.contains("acked=10 verified=10"), "{out}");
    let (code, out, _) = trial(&["--engine", "memory", "--addr", "x:1", "--target", "20"], Memory::with(0, true, "n/a", 0));
    assert!(code == 0 && out.contains("stop=verification-failed") && out.contains("verified=0") && out.contains("records/s=0 "), "{out}");
}

#[test]
fn bad_arguments_and_the_window_cap_are_usage_errors() {
    for bad in [["--workers", "1.5"], ["--workers", "0"], ["--target", "abc"], ["--target", "0"], ["--window", "inf"], ["--window", "-1"], ["--window", "NaN"], ["--window", "86401"], ["--window", "1e300"]] {
        let argv = ["--engine", "memory", "--addr", "x:1", bad[0], bad[1]];
        assert_eq!(trial(&argv, Memory::new()).0, 2, "{bad:?}");
    }
    assert_eq!(trial(&["--engine", "nope", "--addr", "x:1"], Memory::new()).0, 2);
    let (code, out, _) = trial(&["--engine", "memory", "--addr", "x:1", "--target", "3", "--window", "86400"], Memory::new());
    assert!(code == 0 && out.contains("stop=reached"), "a window of exactly 24 h is accepted and the run ends at its target");
}

#[test]
fn a_gated_run_refuses_an_unknown_client_build() {
    let unknown = || Memory::with(0, false, "unknown", 0);
    assert_eq!(trial(&["--engine", "memory", "--addr", "x:1", "--target", "5", "--require-known-build"], unknown()).0, 2);
    let (code, out, _) = trial(&["--engine", "memory", "--addr", "x:1", "--target", "5"], unknown());
    assert!(code == 0 && out.contains("client_build=unknown"));
}

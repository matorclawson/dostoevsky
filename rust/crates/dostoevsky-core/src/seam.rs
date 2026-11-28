//! The test seam: an injected clock and an injected random source, and named points at which a test can stop the
//! engine. Mirrors `internal/seam` of the Go engine.
//!
//! Unset, the engine reads the system clock and the system's random source, as it must in production. Fixed, two runs
//! and two implementations given the same requests one at a time write the same bytes:
//!
//! - **Clock:** the k-th reading after [`fix`] (k from 0) is `start + k * step` nanoseconds since the Unix epoch,
//!   plus whatever [`advance`] has added.
//! - **Random bytes:** each request for n bytes takes SHA-256(seed || counter) blocks, seed and counter as 8 bytes
//!   little-endian, the counter starting at 0 after [`fix`] and going up by one per block, and keeps the first n
//!   bytes. What is left of the last block is not used again.
//! - **Same moments, same count:** this engine reads the clock and the random source at the same points in each
//!   operation, and as often, as the Go engine. Each reading site names the Go line it mirrors.
//! - **Write order:** where the Go engine writes partitions and consumer groups it holds in maps, it writes
//!   partitions ordered by topic and then number, and groups by name; checkpoints order partitions by their stored
//!   identifier. This engine writes in the same orders.
//!
//! [`fix`] is not safe while a node or store is open: call it before opening anything.
//!
//! The clock and the random source are checked against vectors written by the Go package (`rust/testdata`). The
//! named points ([`arm`], [`reach`], [`disarm`]) follow the driver requests of the same names; the list of points
//! and the exact meaning of `kill` and `skip` are those of Go's `internal/seam/points.go`, which this file must be
//! checked against.

use dostoevsky_format::sha256;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixed {
    next: i64,
    step: i64,
    seed: u64,
    count: u64,
}

static FIXED: Mutex<Option<Fixed>> = Mutex::new(None);

fn fixed() -> std::sync::MutexGuard<'static, Option<Fixed>> {
    FIXED.lock().unwrap_or_else(|e| e.into_inner())
}

/// Fixes the clock and the random source for this process. Mirrors `seam.Fix`.
pub fn fix(start: i64, step: i64, seed: u64) {
    *fixed() = Some(Fixed {
        next: start,
        step,
        seed,
        count: 0,
    });
}

/// Back to the system clock and the system's random source. Mirrors `seam.Restore`.
pub fn restore() {
    *fixed() = None;
}

pub fn is_fixed() -> bool {
    fixed().is_some()
}

/// Moves a fixed clock by `nanoseconds`. Does nothing when the clock is not fixed. Mirrors `seam.Advance`.
pub fn advance(nanoseconds: i64) {
    if let Some(f) = fixed().as_mut() {
        f.next = f.next.wrapping_add(nanoseconds);
    }
}

/// Nanoseconds since the Unix epoch. Mirrors `seam.Now`.
pub fn now_nanos() -> i64 {
    if let Some(f) = fixed().as_mut() {
        let t = f.next;
        f.next = f.next.wrapping_add(f.step);
        return t;
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as i64)
}

/// Fills `out` with random bytes. Mirrors `seam.Random`.
pub fn random_bytes(out: &mut [u8]) -> std::io::Result<()> {
    if let Some(f) = fixed().as_mut() {
        let mut input = [0u8; 16];
        input[..8].copy_from_slice(&f.seed.to_le_bytes());
        for chunk in out.chunks_mut(32) {
            input[8..].copy_from_slice(&f.count.to_le_bytes());
            f.count += 1;
            chunk.copy_from_slice(&sha256::digest(&input)[..chunk.len()]);
        }
        return Ok(());
    }
    system_random(out)
}

#[cfg(unix)]
fn system_random(out: &mut [u8]) -> std::io::Result<()> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(out)
}

#[cfg(not(unix))]
fn system_random(_out: &mut [u8]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "no system random source on this platform yet",
    ))
}

// ----- named points -----

struct Armed {
    point: String,
    kill: bool,
    skip: u64,
    reached: u64,
}

static ARMED: Mutex<Option<Armed>> = Mutex::new(None);

fn armed() -> std::sync::MutexGuard<'static, Option<Armed>> {
    ARMED.lock().unwrap_or_else(|e| e.into_inner())
}

/// Arms one named point. It fires exactly once, on pass number `skip + 1`: the process kills itself (`kill`) or the
/// operation at the point fails. Passes before that do nothing, and so do passes after it.
pub fn arm(point: &str, kill: bool, skip: u64) {
    *armed() = Some(Armed {
        point: point.to_string(),
        kill,
        skip,
        reached: 0,
    });
}

/// Disarms, and returns how many times the armed point was reached.
pub fn disarm() -> u64 {
    armed().take().map_or(0, |a| a.reached)
}

/// Called by the engine at the point `name`. `Err` means the operation at this point must fail.
pub fn reach(name: &str) -> Result<(), PointFailure> {
    let mut guard = armed();
    let Some(a) = guard.as_mut().filter(|a| a.point == name) else {
        return Ok(());
    };
    a.reached += 1;
    if a.reached != a.skip + 1 {
        return Ok(());
    }
    if a.kill {
        kill_this_process();
    }
    Err(PointFailure)
}

/// The failure an armed point injects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PointFailure;

impl std::fmt::Display for PointFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failure injected at a named point")
    }
}

impl std::error::Error for PointFailure {}

#[cfg(unix)]
fn kill_this_process() -> ! {
    extern "C" {
        fn getpid() -> i32;
        fn kill(pid: i32, signal: i32) -> i32;
    }
    const SIGKILL: i32 = 9;
    // SAFETY: both are async-signal-safe C library calls with plain integer arguments.
    unsafe {
        kill(getpid(), SIGKILL);
    }
    loop {
        std::thread::park();
    }
}

#[cfg(not(unix))]
fn kill_this_process() -> ! {
    std::process::abort()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The seam is one per process, so its tests run as one.
    #[test]
    fn the_contract_as_the_go_package_keeps_it() {
        restore();
        let before = now_nanos();
        assert!(before > 1_700_000_000_000_000_000 && now_nanos() >= before);
        let (mut a, mut b) = ([0u8; 16], [0u8; 16]);
        random_bytes(&mut a).unwrap();
        random_bytes(&mut b).unwrap();
        assert_ne!(a, b);
        advance(5); // nothing to move

        // Every step the Go program took, and what Go returned.
        let steps = include_str!("../../../testdata/seam-go.txt");
        let (mut readings, mut requests) = (0, 0);
        for line in steps.lines().filter(|l| !l.starts_with('#')) {
            let f: Vec<&str> = line.split(' ').collect();
            match f[0] {
                "fix" => fix(f[1].parse().unwrap(), f[2].parse().unwrap(), f[3].parse().unwrap()),
                "advance" => advance(f[1].parse().unwrap()),
                "now" => {
                    assert_eq!(now_nanos(), f[1].parse::<i64>().unwrap(), "{line}");
                    readings += 1;
                }
                "random" => {
                    let mut got = vec![0u8; f[1].parse().unwrap()];
                    random_bytes(&mut got).unwrap();
                    assert_eq!(sha256::hex(&got), *f.get(2).unwrap_or(&""), "{line}");
                    requests += 1;
                }
                other => panic!("unknown step {other}"),
            }
        }
        assert_eq!((readings, requests), (20, 32));
        assert!(is_fixed());
        restore();
        assert!(!is_fixed());

        // Named points.
        assert_eq!(reach("nothing-armed"), Ok(()));
        arm("after-write", false, 2);
        assert_eq!(reach("another-point"), Ok(()));
        let passes = [
            reach("after-write"),
            reach("after-write"),
            reach("after-write"),
            reach("after-write"),
        ];
        assert_eq!(
            passes,
            [Ok(()), Ok(()), Err(PointFailure), Ok(())],
            "it fires once, on pass skip + 1"
        );
        assert_eq!(disarm(), 4);
        assert_eq!(disarm(), 0);
        assert_eq!(reach("after-write"), Ok(()));
    }
}

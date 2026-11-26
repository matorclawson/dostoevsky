//! One trial of the publisher workload from Rust against one engine; prints one receipt line.
//!
//!     BENCH_TOKEN=... worker --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 [--window 600] [--require-known-build]
//!
//! The token comes from the environment, never the command line. A publish failure is a result (stop=failed,
//! result=FAILED), never an exit code, and is never retried; usage errors exit 2. All logic is in the core crate.
mod adapters;

use dostoevsky_bench_core::{run, Factory};
use std::collections::BTreeMap;

fn main() {
    dostoevsky_bench_core::quiet_panics(); // a panic message can carry anything an engine or library said
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let env: BTreeMap<String, String> = std::env::vars().collect();
    let engines: BTreeMap<String, Factory> = adapters::engines();
    let code = run(&argv, &env, &mut std::io::stdout(), &mut std::io::stderr(), &engines);
    std::process::exit(code);
}

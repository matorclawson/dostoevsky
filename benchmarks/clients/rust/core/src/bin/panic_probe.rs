//! Used by core/tests/core.rs: installs the quiet panic hook, then panics with a message that must never be printed.
fn main() {
    dostoevsky_bench_core::quiet_panics();
    panic!("an adapter bug with a secret in its message: hunter2");
}

//! Prototype of the segmented log's write path in Rust, on the Go engine's on-disk format.
//!
//! Scope (what the A/B measures): append, group commit with one sync per group, rollover,
//! reading sealed and active segments, and reopen with torn-tail recovery. A store written here
//! opens with the Go engine's OpenSegmented and the reverse; `rsab digest` and the Go harness's
//! `digest` must agree on every record.
//!
//! Not here (the Go engine has them): idempotency IDs and their index, idx and filter sidecar
//! files (hints the Go engine rebuilds when missing), retention, consumer-group commits through
//! the API (commit frames are replayed and carried through checkpoints), multi-partition topics,
//! the legacy journal, workflows, and the network server.

pub mod crc32c;
pub mod frame;
mod store;

pub use store::{Options, PublishStats, Store};

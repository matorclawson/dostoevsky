# Row 0 receipts

| File | What it shows |
| --- | --- |
| `environment.txt` | machine, OS, toolchains, the commit of `main` underneath |
| `rust-check.txt` | `cargo fmt --check` and `cargo clippy -D warnings` exit 0; 16 tests pass, each named |
| `conformance.txt` | the Go engine alone passes every case; with the Rust driver, every pairing that opens a store with it fails at `open`, naming the row that will bring it |
| `differential-go-go-without-seam.txt` | the Go engine against itself, without the seam: all 6 comparisons differ, and which files differ |
| `differential-go-rust.txt` | Go against Rust with the seam: the Go driver has no `fix_inputs` yet and stops; without the seam every reply differs, as it must before any engine code exists; the Rust driver against itself is identical |

In `conformance.txt`, 28 pairings pass: the 26 of the Go engine with itself, and `incomplete-backup-directory` with Go
writing and Rust reading, in both formats. Those two pass only because the case's last step requires that opening
fails, and the Rust driver refuses every open.

The gates of a row (its conformance cases, a byte-identical differential run, cross-open in both directions) have
nothing to pass here: there is no engine code. These receipts show that the drivers run and report correctly.

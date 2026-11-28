# Receipts for the seam requests

Every path is relative to the repository root. Made with the runner that compares reply bytes and has a deadline on
every read and write.

| File | What it shows |
| --- | --- |
| `environment.txt` | machine, toolchains, which Go driver was used |
| `differential-go-go-with-seam.txt` | the Go engine against itself with the clock and randomness fixed: identical in all 30 comparisons, replies and files, six runs out of six |
| `differential-go-rust.txt` | both drivers through every trace. `seam-requests` and the 22 streams of `request-binding` are identical as bytes. `seam-rearm` differs in three replies: the three refusals of a second `arm`, which this Go driver does not have. The six traces that open a store differ at `open`, as they must before the store is ported |
| `conformance-pairings.txt` | `incomplete-backup-directory` with both drivers: the pairing "Go then Rust" passes because the case has no `switch` step, so only the first engine runs |

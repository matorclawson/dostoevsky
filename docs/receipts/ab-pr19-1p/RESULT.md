# Production-03 Dostoevsky PR 19 A/B at one publisher

Git heads and trees

```text
baseline 1ce4a6d1a514236f9b2a698046ca15fad338ccb3 d8172ab1564cbe2455f1b9d6c7e0801cb6abee68
candidate 6d2b9eaf7ec55e5d17f9f7c73cc65ef5b19120c3 981d86123e2fa75e47931fcb0746e70792ab365f
```

Binary SHA-256 values

```text
cdc12fb97b71ade59f3d4fda2e850e50fe20f017394f79a8cb5f66107bd76cb1  bin/dost-baseline
50256d3aa7c8aa699d566f56abb7cc9992f8944ad768a35ce8e2f5fbd8b6c870  bin/dost-candidate
```

Summary command `go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -expect-producers 1 -trials 15` ran from the candidate checkout.

Each engine ran 15 trials with one publisher and 16,000 records per trial. The first engine alternated by round.

```text
start=2026-11-25T01:03:15Z command=bash one-producer.sh
finish=2026-11-25T01:14:27Z ab_exit=0
summary_command=go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -expect-producers 1 -trials 15
summary_exit=0
pipeline_phase=complete pipeline_exit=0
trial_validation_exit=0
trial_lines=30 failed_trials=0 low_memory_aborts=0 group_issues=0
lines=30 failed=0
engine=baseline workers=1 trials=15 records/s=752 p99_us=5130 total_rss_KiB=9992 reopen_ms=216.23
engine=candidate workers=1 trials=15 records/s=753 p99_us=4835 total_rss_KiB=9996 reopen_ms=216.56
```

[Per-trial throughput and p99](TRIALS.tsv)

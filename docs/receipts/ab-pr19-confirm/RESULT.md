# Production-03 Dostoevsky PR 19 A/B at 32 publishers

Git heads and trees

```text
baseline 6d2b9eaf7ec55e5d17f9f7c73cc65ef5b19120c3 981d86123e2fa75e47931fcb0746e70792ab365f
candidate 59056bfa58fa1b36e221960138b6054e77de9368 5e3f6231658e0d7279c74e2a726c95c9a7bc9d31
```

Binary SHA-256 values

```text
50256d3aa7c8aa699d566f56abb7cc9992f8944ad768a35ce8e2f5fbd8b6c870  bin/dost-baseline
017198d5536e7e6dc4a7a3fd83ce4450166e36a9f30f288f12c6fbb8c1a486b7  bin/dost-candidate
```

Summary command `go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -expect-producers 32 -trials 5` ran from the candidate checkout.

Each engine ran five trials with 32 publishers and 16,000 records per trial. The first engine alternated by round.

```text
start=2026-11-25T01:22:59Z command=bash thirty-two-producers.sh
finish=2026-11-25T01:23:13Z ab_exit=0
summary_command=go run ./cmd/plots -summary -receipt ab.log -expect-engines baseline,candidate -expect-producers 32 -trials 5
summary_exit=0
pipeline_phase=complete pipeline_exit=0
trial_validation_exit=0
trial_lines=10 failed_trials=0 low_memory_aborts=0 group_issues=0
lines=10 failed=0
engine=baseline workers=32 trials=5 records/s=16060 p99_us=6312 total_rss_KiB=10408 reopen_ms=210.65
engine=candidate workers=32 trials=5 records/s=15655 p99_us=6239 total_rss_KiB=10388 reopen_ms=208.7
```

[Per-trial throughput and p99](TRIALS.tsv)

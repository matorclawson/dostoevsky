"""Prints the tables of RESULT.md from a receipts directory. Usage: python report.py RECEIPTS"""
import json
import statistics
import sys

D = sys.argv[1]


def load(name):
    try:
        return [json.loads(line) for line in open(f"{D}/{name}") if line.startswith("{")]
    except FileNotFoundError:
        return []


def rng(xs, fmt="{:.0f}"):
    return f"{fmt.format(min(xs))} to {fmt.format(max(xs))}"


w = load("write.jsonl")
if w:
    print("### Acknowledged throughput and latency (20 s runs, 3 trials, 100-byte payload)\n")
    print("| Producers | Engine | Acknowledged/s by trial | p50 (µs) | p99 (µs) | p99.9 (µs) | Max (ms) | Ready RSS (MiB) | Steady RSS (MiB) | Peak RSS (MiB) |")
    print("| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |")
    for p in (1, 32):
        for e in ("go", "rust"):
            t = sorted((o for o in w if o["producers"] == p and o["engine"] == e), key=lambda o: o["trial"])
            print(f"| {p} | {e} | {', '.join(f'{o['acked_per_s']:,.0f}' for o in t)} | {rng([o['p50_us'] for o in t])} | {rng([o['p99_us'] for o in t])} | {rng([o['p999_us'] for o in t])} | "
                  f"{rng([o['max_us'] / 1e3 for o in t], '{:.1f}')} | {rng([o['rss_ready_kb'] / 1024 for o in t], '{:.1f}')} | {rng([o['rss_steady_kb'] / 1024 for o in t], '{:.1f}')} | {rng([o['rss_peak_kb'] / 1024 for o in t], '{:.1f}')} |")
    print()
    for p in (1, 32):
        g = sorted((o for o in w if o["producers"] == p and o["engine"] == "go"), key=lambda o: o["trial"])
        r = sorted((o for o in w if o["producers"] == p and o["engine"] == "rust"), key=lambda o: o["trial"])
        print(f"{p} producer(s): Rust / Go throughput by trial: {', '.join(f'{b['acked_per_s'] / a['acked_per_s']:.2f}' for a, b in zip(g, r))}. "
              f"Publishes over 10 ms by trial: Go {[o['over_10ms'] for o in g]}, Rust {[o['over_10ms'] for o in r]}.")
    rr = [o for o in w if o["engine"] == "rust" and o["producers"] == 32]
    print(f"\nRust records per sync at 32 producers: {rng([o['records_per_sync'] for o in rr], '{:.1f}')}.")

s = load("sustained.jsonl")
if s:
    print("\n### Sustained load: 32 producers, 30 minutes\n")
    print("| Measure | Go | Rust |")
    print("| --- | ---: | ---: |")
    g = next(o for o in s if o["engine"] == "go")
    r = next(o for o in s if o["engine"] == "rust")
    for label, f in (("Seconds", lambda o: f"{o['seconds']:.0f}"), ("Acknowledged records", lambda o: f"{o['acked']:,}"),
                     ("Acknowledged per second", lambda o: f"{o['acked_per_s']:,.0f}"),
                     ("One-second throughput: lowest / median / highest", lambda o: f"{o['second_min']:,} / {o['second_median']:,} / {o['second_max']:,}"),
                     ("p50 (µs)", lambda o: f"{o['p50_us']:.0f}"), ("p99 (µs)", lambda o: f"{o['p99_us']:.0f}"), ("p99.9 (µs)", lambda o: f"{o['p999_us']:.0f}"),
                     ("Slowest publish (ms)", lambda o: f"{o['max_us'] / 1e3:.1f}"),
                     ("Publishes over 10 ms", lambda o: f"{o['over_10ms']:,}"), ("Publishes over 50 ms", lambda o: f"{o['over_50ms']:,}"), ("Publishes over 100 ms", lambda o: f"{o['over_100ms']:,}"),
                     ("Longest gap between acknowledgments (ms)", lambda o: f"{o['max_ack_gap_ms']:.1f}"), ("Gaps over 10 ms", lambda o: f"{o['ack_gaps_over_10ms']:,}"),
                     ("Ready RSS (MiB)", lambda o: f"{o['rss_ready_kb'] / 1024:.1f}"), ("RSS after 1 s (MiB)", lambda o: f"{o['rss_first_kb'] / 1024:.1f}"),
                     ("Steady RSS, median of last 10 min (MiB)", lambda o: f"{o['rss_steady_kb'] / 1024:.1f}"), ("RSS at the end (MiB)", lambda o: f"{o['rss_last_kb'] / 1024:.1f}"),
                     ("Peak RSS (MiB)", lambda o: f"{o['rss_peak_kb'] / 1024:.1f}"),
                     ("Collector cycles", lambda o: f"{o['gc_cycles']:,}" if "gc_cycles" in o else "n/a"),
                     ("Collector pause, total (ms)", lambda o: f"{o['gc_pause_total_ms']:.1f}" if "gc_cycles" in o else "n/a"),
                     ("Collector pause, longest of last 256 (µs)", lambda o: f"{o['gc_pause_max_recent_us']:.0f}" if "gc_cycles" in o else "n/a")):
        print(f"| {label} | {f(g)} | {f(r)} |")
    print(f"\nPer million publishes over 10 ms: Go {g['over_10ms'] / g['acked'] * 1e6:.2f}, Rust {r['over_10ms'] / r['acked'] * 1e6:.2f}.")

ro = load("reopen.jsonl")
if ro:
    print("\n### Reopen: process start to first successful read (ms; median, range)\n")
    print("| Records | Read | Cache | Runs | Go | Rust |")
    print("| ---: | --- | --- | ---: | --- | --- |")

    def cell(xs):
        return f"{statistics.median(xs):.1f} ({min(xs):.1f} to {max(xs):.1f})" if xs else ""
    for n in (1000000, 10000000):
        for fetch in ("newest", "oldest"):
            for cache in ("cold", "warm"):
                sel = lambda e: [o["start_to_first_fetch_ms"] for o in ro if o["records"] == n and o["fetch"] == fetch and o["cache"] == cache and o["engine"] == e]
                print(f"| {n:,} | {fetch} record | {cache} | {len(sel('go'))} | {cell(sel('go'))} | {cell(sel('rust'))} |")
    print("\n| Records | Measure | Go | Rust |")
    print("| ---: | --- | --- | --- |")
    for n in (1000000, 10000000):
        sel = lambda e, k, f="newest", c="warm": [o[k] for o in ro if o["records"] == n and o["fetch"] == f and o["cache"] == c and o["engine"] == e]
        print(f"| {n:,} | RSS after open and one read, warm (MiB) | {cell([x / 1024 for x in sel('go', 'rss_kb')])} | {cell([x / 1024 for x in sel('rust', 'rss_kb')])} |")
        print(f"| {n:,} | First publish after open (µs), 3 runs | {cell(sel('go', 'first_publish_us', 'newest+publish'))} | {cell(sel('rust', 'first_publish_us', 'newest+publish'))} |")
    f = load("fill.jsonl")
    if f:
        print("\nFilling the stores (batches of 1,000 records, one sync per batch), records per second: " +
              "; ".join(f"{o['engine']} {o['records']:,}: {o['records_per_s']:,.0f}" for o in f) + ".")

rd = load("read.jsonl")
if rd:
    print("\n### Sequential read of the 10M-record store, pages of 1,000 records\n")
    print("| Engine | Cache | Records/s | MiB/s of payload | Peak RSS (MiB) |")
    print("| --- | --- | --- | --- | --- |")
    for e in ("go", "rust"):
        for cache in ("cold", "warm"):
            t = [o for o in rd if o["engine"] == e and o["cache"] == cache]
            print(f"| {e} | {cache} ({len(t)} run{'s' if len(t) > 1 else ''}) | {rng([o['records_per_s'] for o in t], '{:,.0f}')} | {rng([o['mib_per_s'] for o in t], '{:.0f}')} | {rng([o['rss_peak_kb'] / 1024 for o in t], '{:.1f}')} |")

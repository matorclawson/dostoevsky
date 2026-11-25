#!/usr/bin/env python3
"""One trial of the publisher workload from Python against one engine; prints one receipt line.

    BENCH_TOKEN=... python3 worker.py --engine kafka --addr 127.0.0.1:PORT --workers 8 --target 16000 [--window 600]

The token comes from the environment, never from the command line. The server is started and
stopped by the driver (benchmarks/embedded/cmd/serve). A publish failure ends the run as a
result (stop=failed), never an exit code, and is never retried; internal errors exit 1.
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import core  # noqa: E402


def main(argv, env=os.environ, out=sys.stdout, adapters=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--engine", required=True)
    ap.add_argument("--addr", required=True, help="host:port of the server")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--target", type=int, default=2000, help="stop at this many acknowledged messages")
    ap.add_argument("--window", type=float, default=0, help="stop after this many seconds (0: no window)")
    args = ap.parse_args(argv)
    if adapters is None:
        import adapters as _adapters
        adapters = _adapters.ENGINES
    if args.engine not in adapters:
        print(f"unknown engine {args.engine!r} (have {', '.join(sorted(adapters))})", file=sys.stderr)
        return 2
    if args.workers < 1 or args.target < 1:
        print("workers and target must be at least 1", file=sys.stderr)
        return 2
    adapter = adapters[args.engine](args.addr, env.get("BENCH_TOKEN", ""), args.workers)
    try:
        res = core.run_workload(adapter, args.workers, args.target, args.window)
        verified, why = 0, ""
        if res.stop != core.STOP_FAILED or res.records:  # also read back what a failed run acknowledged
            why = core.verify(adapter, res.records)
            verified = 0 if why else res.acked
        if why:
            res.stop, res.detail = core.STOP_VERIFY, why
    finally:
        adapter.close()
    print(core.receipt_line(f"python-{args.engine}", args.workers, res, verified, adapter.client_name, core.peak_rss_kib(), getattr(adapter, "client_build", "n/a")), file=out)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

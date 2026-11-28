"""Compare single acknowledged publishes over loopback and verify all timed receipts after SIGKILL."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import math
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "recovery"))
from kill_peers import ENGINES, restart_command, wait_port


def memory(pid, key):
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith(key + ":"):
            return int(line.split()[1])
    raise ValueError(key)


def kill_and_wait(pid):
    with os.fdopen(os.pidfd_open(pid), "rb", buffering=0) as original:
        os.kill(pid, signal.SIGKILL)
        if not select.select([original], [], [], 20)[0]:
            raise TimeoutError("broker exit after kill")


def kill_while_publishing(adapter, pid, receipts):
    stop = threading.Event()
    cv = threading.Condition()
    active = completed = 0
    if hasattr(adapter, "delivery_timeout"):
        adapter.delivery_timeout = 8
    def publish(worker):
        nonlocal active, completed
        i = worker
        while True:
            digest = hashlib.sha256(f"crash-{i}".encode()).digest()
            data, key = digest * 16, digest[:16].hex()
            with cv:
                if stop.is_set():
                    return
                active += 1
                cv.notify_all()
            try:
                off = adapter.publish(data, key)
            except Exception:
                if stop.is_set():
                    return
                raise
            finally:
                with cv:
                    active -= 1
                    cv.notify_all()
            with cv:
                if off in receipts:
                    raise ValueError("duplicate crash-phase offset")
                receipts[off] = data
                completed += 1
                cv.notify_all()
            i += 8
    with ThreadPoolExecutor(8) as pool:
        jobs = [pool.submit(publish, i) for i in range(8)]
        with cv:
            try:
                if not cv.wait_for(lambda: completed >= 512 and active > 0, timeout=60):
                    raise TimeoutError("crash workload")
                at_kill = active
            finally:
                stop.set()
                kill_and_wait(pid)
        for job in jobs:
            job.result()
    return at_kill, completed


def trial(engine, round_no, count):
    with tempfile.TemporaryDirectory() as tmp:
        env = {**os.environ, "TMPDIR": str(Path(tmp).resolve())}
        serve = subprocess.Popen([env["SERVE_BIN"], "-engine", engine], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env)
        adapter = restarted = None
        try:
            if not select.select([serve.stdout], [], [], 120)[0]:
                raise TimeoutError("startup")
            ready = serve.stdout.readline().split()
            if ready[:1] != ["READY"]:
                raise RuntimeError("broker failed to start")
            fields = dict(item.split("=", 1) for item in ready[1:])
            addr, token, pid = fields["addr"], fields["token"], int(fields["pid"])
            boot = memory(pid, "VmRSS")
            directory, = Path(tmp).glob("embedded-serve-*")
            cmd = ([os.fsdecode(a) for a in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0") if a]
                   if engine == "native" else restart_command(engine, directory))
            if engine == "native" and "-segmented" not in cmd:
                raise RuntimeError("native trial requires a segmented server")
            extra = {"identified": True} if engine == "kafka" else {}
            adapter = ENGINES[engine](addr, token, 32, **extra)
            results, receipts = [], {}
            sequence = 0
            for workers in ([1, 32] if round_no % 2 else [32, 1]):
                payloads = []
                for i in range(count + 64):
                    digest = hashlib.sha256((sequence + i).to_bytes(8, "little")).digest()
                    payloads.append((digest * 16, digest[:16].hex()))
                sequence += len(payloads)
                for data, key in payloads[:64]:
                    off = adapter.publish(data, key)
                    if off in receipts:
                        raise ValueError("duplicate receipt")
                    receipts[off] = data
                gate = threading.Barrier(workers + 1)
                def worker(w):
                    samples = []
                    gate.wait(timeout=15)
                    for i in range(w + 64, len(payloads), workers):
                        data, key = payloads[i]
                        t = time.perf_counter_ns()
                        off = adapter.publish(data, key)
                        samples.append((time.perf_counter_ns() - t, off, data))
                    return samples
                with ThreadPoolExecutor(workers) as pool:
                    jobs = [pool.submit(worker, w) for w in range(workers)]
                    begin = time.perf_counter()
                    gate.wait(timeout=15)
                    samples = [row for job in jobs for row in job.result()]
                    elapsed = time.perf_counter() - begin
                for _, off, data in samples:
                    if off in receipts:
                        raise ValueError("duplicate receipt")
                    receipts[off] = data
                lat = sorted(row[0] / 1000 for row in samples)
                quantiles = [lat[math.ceil(count * q) - 1] for q in (.5, .95, .99)]
                results.append((workers, count / elapsed, quantiles, memory(pid, "VmHWM")))
            outstanding, crash_acks = kill_while_publishing(adapter, pid, receipts)
            adapter.close()
            adapter = None
            # The new broker needs the original token for its native wire protocol.
            restarted = subprocess.Popen(cmd, env={**env, "DOSTOEVSKY_TOKEN": token},
                                         stdout=subprocess.PIPE if engine=="native" else subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if engine=="native":
                if not select.select([restarted.stdout], [], [], 30)[0] or not restarted.stdout.readline().startswith(b"listening "):
                    raise RuntimeError("native restart failed")
            wait_port(addr, restarted)
            adapter = ENGINES[engine](addr, token, 1, **extra)
            for off, data in sorted(receipts.items()):
                if adapter.read(off) != data:
                    raise ValueError(f"acknowledged offset {off} differs after restart")
            for w, rate, (p50, p95, p99), peak in results:
                print(f"engine={engine} round={round_no} workers={w} count={count} rate={rate:.1f} "
                      f"p50_us={p50:.1f} p95_us={p95:.1f} p99_us={p99:.1f} boot_KiB={boot} "
                      f"peak_KiB={peak} restart_verified_per_trial={len(receipts)} outstanding_at_kill={outstanding} crash_acks={crash_acks}", flush=True)
        finally:
            if adapter:
                adapter.close()
            if restarted and restarted.poll() is None:
                restarted.terminate()
                try:
                    restarted.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    restarted.kill()
                    restarted.wait()
            serve.stdin.close()
            try:
                serve.wait(timeout=60)
            except subprocess.TimeoutExpired:
                serve.kill()
                serve.wait()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=2048)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--engines", nargs="+", default=["native", "nats", "kafka"])
    args = parser.parse_args()
    for r in range(args.rounds):
        engines = args.engines[r % len(args.engines):] + args.engines[:r % len(args.engines)]
        for engine in engines:
            trial(engine, r + 1, args.count)

"""Crash-oracle companion to the Linux native protocol benchmark.

Run from the repository root. Uses the benchmark Python environment and the same
SERVE_BIN and BENCH_*_SERVER/HOME variables as benchmarks/clients/run.sh.
"""
import concurrent.futures
import contextlib
import hashlib
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "clients" / "python"))
from adapters import ENGINES


def value(number):
    return hashlib.sha256(number.to_bytes(8, "big")).digest() * 16


def wait_port(addr, process):
    host, port = addr.rsplit(":", 1)
    deadline = time.monotonic() + 60
    while process.poll() is None and time.monotonic() < deadline:
        try:
            with socket.create_connection((host, int(port)), timeout=0.5):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("restarted process failed to open its port")


def restart_command(engine, directory):
    if engine == "kafka":
        return [str(Path(os.environ["BENCH_KAFKA_HOME"]) / "bin/kafka-server-start.sh"),
                str(directory / "server.properties")]
    conf = str(directory / "server.conf")
    return ([os.environ["BENCH_NATS_SERVER"], "-c", conf] if engine == "nats"
            else [os.environ["BENCH_REDIS_SERVER"], conf])


def run(engine):
    with tempfile.TemporaryDirectory() as tmp:
        env = {**os.environ, "TMPDIR": str(Path(tmp).resolve())}
        driver = subprocess.Popen([env["SERVE_BIN"], "-engine", engine], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env)
        restarted = None
        try:
            if not select.select([driver.stdout], [], [], 90)[0]:
                raise TimeoutError("initial readiness")
            ready = driver.stdout.readline().split()
            if ready[:1] != ["READY"]:
                raise RuntimeError("initial server start failed")
            fields = dict(item.split("=", 1) for item in ready[1:])
            addr, token, pid = fields["addr"], fields["token"], int(fields["pid"])
            directory, = Path(tmp).glob("embedded-serve-*")
            adapter = ENGINES[engine](addr, token, 8)
            if engine == "kafka":
                adapter.delivery_timeout = 8
            stopping = threading.Event()
            changed = threading.Condition()
            observed = []
            outstanding = 0

            def publisher(worker):
                nonlocal outstanding
                number = worker
                while True:
                    with changed:
                        if stopping.is_set():
                            return
                        outstanding += 1
                    try:
                        offset = adapter.publish(value(number))
                    except Exception:
                        if stopping.is_set():
                            return
                        raise
                    finally:
                        with changed:
                            outstanding -= 1
                            changed.notify_all()
                    with changed:
                        observed.append((offset, number))
                        changed.notify_all()
                    number += 8

            try:
                with concurrent.futures.ThreadPoolExecutor(8) as pool:
                    jobs = [pool.submit(publisher, n) for n in range(8)]
                    with changed:
                        enough = changed.wait_for(lambda: len(observed) >= 500 and outstanding > 0, timeout=40)
                        stopping.set()
                        at_kill = outstanding
                        os.kill(pid, signal.SIGKILL)
                    for job in jobs:
                        job.result()
                    if not enough:
                        raise RuntimeError("ack threshold or in-flight condition unmet")
            finally:
                stopping.set()
                adapter.close()
            if len({o for o, _ in observed}) != len(observed):
                raise ValueError("duplicate acknowledged offset")

            # Keep serve's stdin open until verification finishes so it keeps the
            # original files. Start the peer with the original volume and config.
            restarted = subprocess.Popen(restart_command(engine, directory), env=env,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            wait_port(addr, restarted)
            with contextlib.closing(ENGINES[engine](addr, token, 1)) as reader:
                for offset, number in sorted(observed):
                    if reader.read(offset) != value(number):
                        raise ValueError(f"{engine}: readback differs for {offset}")
            print(f"engine={engine} signal=SIGKILL producers=8 outstanding_at_kill={at_kill} "
                  f"acknowledged={len(observed)} verified={len(observed)}", flush=True)
        finally:
            if restarted is not None and restarted.poll() is None:
                restarted.terminate()
                try:
                    restarted.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    restarted.kill()
                    restarted.wait()
            driver.stdin.close()
            try:
                driver.wait(timeout=65)
            except subprocess.TimeoutExpired:
                driver.kill()
                driver.wait()


if __name__ == "__main__":
    for peer in sys.argv[1:] or ("nats", "redis", "kafka"):
        if peer not in {"nats", "redis", "kafka"}:
            raise SystemExit("expected nats, redis or kafka")
        run(peer)

"""Linux process-kill durability check. Pass a native server executable; data is disposable."""
import argparse
import concurrent.futures
import hashlib
import os
import pathlib
import queue
import secrets
import subprocess
import tempfile
import threading

from dostoevsky_client.native import NativeClient, ConnectionLost


def payload(i):
    return hashlib.sha256(str(i).encode()).digest() * 16


def serve(binary, directory, token, segmented):
    p = subprocess.Popen([binary, *(["-segmented"] if segmented else []), "-listen", "127.0.0.1:0", "-data", directory],
                         env={**os.environ, "DOSTOEVSKY_TOKEN": token},
                         stdout=subprocess.PIPE, text=True)
    try:
        line = p.stdout.readline()
        if not line.startswith("listening "):
            raise RuntimeError("could not start native server")
        host, port = line.split()[1].rsplit(":", 1)
        return p, (host, int(port))
    except BaseException:
        p.kill()
        p.wait()
        raise


def check(binary, directory, segmented=False, count=2000):
    token = secrets.token_hex(24)
    server, address = serve(binary, directory, token, segmented)
    receipts = queue.Queue()
    stopping = threading.Event()
    def publish(worker):
        with NativeClient(address, token) as client:
            i = worker
            while not stopping.is_set():
                try:
                    receipt = client.publish("events", payload(i), id=str(i))
                except ConnectionLost:
                    if stopping.is_set():
                        return
                    raise
                receipts.put((receipt.offset, i))
                i += 8

    try:
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            jobs = [pool.submit(publish, worker) for worker in range(8)]
            seen = []
            try:
                while len(seen) < count:
                    seen.append(receipts.get(timeout=45))
            finally:
                stopping.set()
                server.kill()
                server.wait()
            for job in jobs:
                job.result()
    finally:
        if server.poll() is None:
            server.kill()
            server.wait()
    while not receipts.empty():
        seen.append(receipts.get_nowait())
    if len({off for off, _ in seen}) != len(seen):
        raise ValueError("duplicate receipt offset")
    server, address = serve(binary, directory, token, segmented)
    try:
        with NativeClient(address, token) as c:
            for off, i in sorted(seen):
                r = c.read("events", 0, off)
                if r is None or r.id != str(i) or r.value != payload(i):
                    raise ValueError(f"record {i} differs after restart")
                if c.publish("events", payload(i), id=str(i)).offset != off:
                    raise ValueError("idempotent retry changed the offset")
    finally:
        server.terminate()
        server.wait(timeout=20)
    print(f"signal=SIGKILL producers=8 acknowledged={len(seen)} verified={len(seen)} retries_verified={len(seen)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=pathlib.Path)
    parser.add_argument("--segmented", action="store_true")
    parser.add_argument("--count", type=int, default=2000)
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    with tempfile.TemporaryDirectory() as tmp:
        check(str(args.binary.resolve()), tmp, args.segmented, args.count)

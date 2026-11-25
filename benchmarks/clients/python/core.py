"""The workload, histogram and receipt line shared by every engine adapter of the Python worker.

It follows benchmarks/embedded/internal/run: 512-byte records whose first eight bytes are the
big-endian message index, synchronous publishes acknowledged only after the engine's durable
acknowledgment, no retries, every acknowledged record read back, and the stop reasons of the
sustained mode (reached, window, failed, verification-failed). Standard library only, so it can
be tested without any engine or client library installed.
"""
import math
import resource
import struct
import sys
import threading
import time
from dataclasses import dataclass, field

RECORD_BYTES = 512
HIST_BINS = 2400
_LOG = math.log(1.01)

STOP_REACHED, STOP_WINDOW, STOP_FAILED, STOP_VERIFY = "reached", "window", "failed", "verification-failed"


class Hist:
    """Latency histogram, 1% resolution from 1 microsecond up, fixed size: the same bins as the
    Go harness (sustained.go), checked against benchmarks/clients/hist_vector.json."""

    def __init__(self):
        self.n = [0] * HIST_BINS

    @staticmethod
    def index(ns: int) -> int:
        us = ns / 1e3
        if us < 1:
            return 0
        return min(int(math.log(us) / _LOG), HIST_BINS - 1)

    def add_ns(self, ns: int) -> None:
        self.n[self.index(ns)] += 1

    def merge(self, other: "Hist") -> None:
        for i, c in enumerate(other.n):
            self.n[i] += c

    def total(self) -> int:
        return sum(self.n)

    def quantile_us(self, q: float) -> int:
        """The upper edge of the bin holding the q-th quantile, in whole microseconds."""
        total = self.total()
        if total == 0:
            return 0
        rank = max(math.ceil(q * total), 1)
        seen = 0
        for i, c in enumerate(self.n):
            seen += c
            if seen >= rank:
                return int(math.pow(1.01, i + 1) * 1e3) // 1000
        return 0


def payload(i: int) -> bytes:
    return struct.pack(">Q", i) + bytes(RECORD_BYTES - 8)


@dataclass
class Result:
    acked: int = 0
    elapsed: float = 0.0
    stop: str = STOP_REACHED
    detail: str = ""
    hist: Hist = field(default_factory=Hist)
    records: list = field(default_factory=list)  # (message index, engine offset) of every acknowledgment


def run_workload(adapter, workers: int, target: int, window: float, clock=time.monotonic) -> Result:
    """Publish with `workers` synchronous producers until `target` messages are acknowledged, the
    `window` (seconds) ends, or a publish fails. The failing publish is not counted and nothing is
    retried. Publishes already in flight when the stop is decided still complete and are counted."""
    lock = threading.Lock()
    state = {"next": 0, "stop": None}
    out = Result()
    parts = []

    def set_stop(kind: str, text: str = "") -> None:
        with lock:
            if state["stop"] is None:
                state["stop"] = (kind, text)

    start = clock()
    deadline = start + window if window > 0 else None

    def producer():
        h, mine = Hist(), []
        parts.append((h, mine))  # list.append is atomic
        while True:
            if deadline is not None and clock() >= deadline:
                set_stop(STOP_WINDOW)
            with lock:
                if state["stop"] is not None:
                    return
                i = state["next"]
                if i >= target:
                    state["stop"] = (STOP_REACHED, "")
                    return
                state["next"] = i + 1
            begin = time.perf_counter_ns()
            try:
                offset = adapter.publish(payload(i))
            except Exception as e:  # noqa: BLE001 - any engine error is a result, never retried
                set_stop(STOP_FAILED, f"publish {i}: {type(e).__name__}: {e}")
                return
            h.add_ns(time.perf_counter_ns() - begin)
            mine.append((i, offset))

    threads = [threading.Thread(target=producer, daemon=True) for _ in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    out.elapsed = clock() - start
    for h, mine in parts:
        out.hist.merge(h)
        out.records.extend(mine)
    out.acked = len(out.records)
    kind, text = state["stop"] or (STOP_REACHED, "")
    out.stop = kind
    out.detail = f"{text} (after {out.acked} acknowledged)" if kind == STOP_FAILED else text
    return out


def verify(adapter, records) -> str:
    """Reads every acknowledged record back, in offset order, and returns "" or a plain-words failure."""
    for i, offset in sorted(records, key=lambda r: r[1]):
        try:
            got = adapter.read(offset)
        except Exception as e:  # noqa: BLE001
            return f"record {i} at {offset}: {type(e).__name__}: {e}"
        if got != payload(i):
            return f"record {i} at {offset}: the bytes read back differ from the bytes published"
    return ""


def peak_rss_kib() -> int:
    """The kernel's peak resident size of this process (ru_maxrss: KiB on Linux, bytes on macOS)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak // 1024 if sys.platform == "darwin" else peak


def receipt_line(engine: str, workers: int, res: Result, verified: int, client: str, rss_kib: int) -> str:
    rate = res.acked / res.elapsed if res.elapsed > 0 else 0
    fields = [
        f"engine={engine}", f"workers={workers}", f"acked={res.acked}", f"verified={verified}",
        f"records/s={rate:.0f}", f"elapsed_s={res.elapsed:.2f}",
        f"p50_us={res.hist.quantile_us(0.50)}", f"p95_us={res.hist.quantile_us(0.95)}",
        f"p99_us={res.hist.quantile_us(0.99)}", f"p99_9_us={res.hist.quantile_us(0.999)}",
        f"p99_99_us={res.hist.quantile_us(0.9999)}",
        f"client_rss_KiB={rss_kib}", "client_rss_source=ru_maxrss",
        f"lang=python", f"client={client}", f"stop={res.stop}",
    ]
    if res.detail:
        fields.append("detail=" + res.detail.replace("\n", " ").replace(" ", "_")[:200])
    return " ".join(fields)

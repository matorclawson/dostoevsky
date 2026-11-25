import io
import json
import os
import sys
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(HERE, ".."))
import adapters  # noqa: E402
import core  # noqa: E402
import worker  # noqa: E402

VECTOR = json.load(open(os.path.join(HERE, "..", "..", "hist_vector.json")))


class Memory:
    """An in-memory engine: publish returns the offset; read returns what was stored."""

    client_name = "memory-0"

    def __init__(self, fail_at=None, delay=0.0, corrupt=False):
        self.lock, self.log, self.fail_at, self.delay, self.corrupt = threading.Lock(), [], fail_at, delay, corrupt
        self.attempts = 0

    def publish(self, data):
        with self.lock:
            self.attempts += 1
            if self.fail_at is not None and self.attempts == self.fail_at:
                raise ConnectionError("the engine went away")
        if self.delay:
            time.sleep(self.delay)
        with self.lock:
            self.log.append(bytes(data))
            return len(self.log) - 1

    def read(self, offset):
        data = self.log[offset]
        return data[:-1] + b"\x01" if self.corrupt else data

    def close(self):
        pass


class HistTest(unittest.TestCase):
    def test_bins_and_quantiles_match_the_shared_vector(self):
        self.assertEqual(VECTOR["bins"], core.HIST_BINS)
        for ns, want in VECTOR["index"].items():
            self.assertEqual(core.Hist.index(int(ns)), want, ns)
        h = core.Hist()
        for ns in VECTOR["durations_ns"]:
            h.add_ns(ns)
        for q, want in VECTOR["quantiles_us"].items():
            self.assertEqual(h.quantile_us(float(q)), want, q)

    def test_an_empty_histogram_has_no_quantiles_and_merging_adds(self):
        a, b = core.Hist(), core.Hist()
        self.assertEqual(a.quantile_us(0.99), 0)
        a.add_ns(5000)
        b.add_ns(5000)
        a.merge(b)
        self.assertEqual(a.total(), 2)


class WorkloadTest(unittest.TestCase):
    def test_a_record_is_512_bytes_starting_with_its_index(self):
        p = core.payload(258)
        self.assertEqual((len(p), p[:8]), (512, b"\x00\x00\x00\x00\x00\x00\x01\x02"))

    def test_reached_counts_every_acknowledgment_exactly_once(self):
        for workers in (1, 8, 32):
            m = Memory()
            res = core.run_workload(m, workers, 500, 0)
            self.assertEqual((res.stop, res.acked, len(m.log)), (core.STOP_REACHED, 500, 500), workers)
            self.assertEqual(sorted(i for i, _ in res.records), list(range(500)))
            self.assertEqual(core.verify(m, res.records), "")

    def test_a_failure_is_a_result_never_retried_and_the_failing_publish_is_not_counted(self):
        m = Memory(fail_at=6)
        res = core.run_workload(m, 1, 100, 0)
        self.assertEqual(res.stop, core.STOP_FAILED)
        self.assertEqual((res.acked, m.attempts), (5, 6))  # five acknowledged, the sixth failed, no seventh
        self.assertIn("the engine went away", res.detail)
        self.assertIn("after 5 acknowledged", res.detail)

    def test_with_many_workers_a_failure_stops_the_others_and_attempts_exceed_acks_by_at_most_the_workers(self):
        m = Memory(fail_at=40)
        res = core.run_workload(m, 8, 10000, 0)
        self.assertEqual(res.stop, core.STOP_FAILED)
        self.assertEqual(res.acked, len(m.log))
        self.assertLessEqual(m.attempts - res.acked, 8)

    def test_the_window_ends_the_run(self):
        m = Memory(delay=0.01)
        begin = time.monotonic()
        res = core.run_workload(m, 4, 10**9, 0.3)
        self.assertEqual(res.stop, core.STOP_WINDOW)
        self.assertGreater(res.acked, 0)
        self.assertLess(time.monotonic() - begin, 5)

    def test_verification_finds_a_changed_byte(self):
        m = Memory(corrupt=True)
        res = core.run_workload(m, 2, 20, 0)
        self.assertIn("differ", core.verify(m, res.records))

    def test_verification_reports_a_read_error_in_words(self):
        m = Memory()
        res = core.run_workload(m, 1, 5, 0)
        res.records.append((99, 12345))
        self.assertIn("IndexError", core.verify(m, res.records))


class ReceiptTest(unittest.TestCase):
    def test_the_line_carries_the_documented_keys_and_no_secret(self):
        res = core.run_workload(Memory(), 2, 100, 0)
        line = core.receipt_line("python-memory", 2, res, 100, "memory-0", 12345)
        keys = dict(kv.split("=", 1) for kv in line.split())
        for k in ("engine", "workers", "acked", "verified", "records/s", "elapsed_s", "p50_us", "p95_us", "p99_us",
                  "p99_9_us", "p99_99_us", "client_rss_KiB", "client_rss_source", "lang", "client", "stop"):
            self.assertIn(k, keys)
        self.assertEqual((keys["engine"], keys["stop"], keys["acked"], keys["verified"]), ("python-memory", "reached", "100", "100"))
        self.assertNotIn("\n", line)

    def test_a_failure_detail_has_no_spaces_so_the_line_stays_key_value(self):
        res = core.Result(acked=3, elapsed=1.0, stop=core.STOP_FAILED, detail="publish 4: boom happened")
        line = core.receipt_line("x", 1, res, 3, "c", 1)
        self.assertTrue(all("=" in kv for kv in line.split()), line)


class WorkerTest(unittest.TestCase):
    def run_main(self, argv, adapter):
        out = io.StringIO()
        code = worker.main(argv, env={"BENCH_TOKEN": "secret-token-value"}, out=out, adapters={"memory": lambda a, t, w: adapter})
        return code, out.getvalue()

    def test_one_trial_prints_one_receipt_line_and_never_the_token(self):
        code, text = self.run_main(["--engine", "memory", "--addr", "x:1", "--workers", "4", "--target", "200"], Memory())
        self.assertEqual(code, 0)
        self.assertEqual(len(text.strip().splitlines()), 1)
        self.assertIn("verified=200", text)
        self.assertNotIn("secret-token-value", text)

    def test_a_failed_run_is_exit_zero_with_stop_failed_and_its_acknowledgments_are_verified(self):
        code, text = self.run_main(["--engine", "memory", "--addr", "x:1", "--target", "50"], Memory(fail_at=11))
        self.assertEqual(code, 0)
        self.assertIn("stop=failed", text)
        self.assertIn("acked=10", text)
        self.assertIn("verified=10", text)

    def test_corrupt_read_back_is_verification_failed(self):
        code, text = self.run_main(["--engine", "memory", "--addr", "x:1", "--target", "20"], Memory(corrupt=True))
        self.assertEqual(code, 0)
        self.assertIn("stop=verification-failed", text)
        self.assertIn("verified=0", text)

    def test_unknown_engine_and_bad_numbers_are_usage_errors(self):
        self.assertEqual(self.run_main(["--engine", "nope", "--addr", "x:1"], Memory())[0], 2)
        self.assertEqual(self.run_main(["--engine", "memory", "--addr", "x:1", "--workers", "0"], Memory())[0], 2)


class RedisIdTest(unittest.TestCase):
    def test_stream_ids_pack_and_unpack(self):
        self.assertEqual(adapters.unpack_id(adapters.pack_id("1700000000123-7")), "1700000000123-7")
        self.assertEqual(adapters.pack_id(b"5-65535"), (5 << 16) | 65535)
        with self.assertRaises(ValueError):
            adapters.pack_id("5-65536")


if __name__ == "__main__":
    unittest.main()

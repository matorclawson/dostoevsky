"""Tests of the interop script's own plumbing (not the clients). From the repository root: make test-scripts"""
import os
import subprocess
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import client_interop  # noqa: E402


def start(code):
    return subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)


class StartupLineTest(unittest.TestCase):
    def test_a_gateway_that_prints_nothing_fails_the_run_instead_of_hanging_it(self):
        with start("import time; time.sleep(60)") as proc:
            try:
                begin = time.monotonic()
                with self.assertRaises(TimeoutError):
                    client_interop.startup_line(proc, timeout=0.5)
                self.assertLess(time.monotonic() - begin, 10)
            finally:
                proc.kill()

    def test_half_a_line_does_not_hang_the_run(self):
        with start("import sys, time; sys.stdout.write('listening 127.0'); sys.stdout.flush(); time.sleep(60)") as proc:
            try:
                begin = time.monotonic()
                with self.assertRaises(TimeoutError) as e:
                    client_interop.startup_line(proc, timeout=0.5)
                self.assertLess(time.monotonic() - begin, 10)
                self.assertIn("listening 127.0", str(e.exception))
            finally:
                proc.kill()

    def test_a_line_that_arrives_in_two_pieces_is_returned_whole(self):
        code = "import sys, time; sys.stdout.write('listening 127.'); sys.stdout.flush(); time.sleep(0.3); print('0.0.1:9', flush=True)"
        with start(code) as proc:
            self.assertEqual(client_interop.startup_line(proc, timeout=10), "listening 127.0.0.1:9")

    def test_a_gateway_that_exits_without_a_line_is_reported_promptly(self):
        with start("pass") as proc:
            self.assertEqual(client_interop.startup_line(proc, timeout=10), "")

    def test_the_first_line_is_returned(self):
        with start("print('listening 127.0.0.1:1', flush=True)") as proc:
            self.assertEqual(client_interop.startup_line(proc, timeout=10), "listening 127.0.0.1:1")


if __name__ == "__main__":
    unittest.main()

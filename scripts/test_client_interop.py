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

    def test_the_first_line_is_returned(self):
        with start("print('listening 127.0.0.1:1', flush=True)") as proc:
            self.assertEqual(client_interop.startup_line(proc, timeout=10), "listening 127.0.0.1:1")


if __name__ == "__main__":
    unittest.main()

import os, sys, threading, time, unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dostoevsky_client import Client, ClientError  # noqa: E402

TOKEN = "a-test-token-with-entropy"
LOG = []  # (path, query, body) of every request the stub saw
CONNECTIONS = []  # one entry per accepted connection
DROP_NEXT = []  # when non-empty, the next request is read and then the connection is closed unanswered


class Stub(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # keep-alive

    def setup(self):
        super().setup()
        CONNECTIONS.append(self.client_address)

    def log_message(self, *a):
        pass

    def _go(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        LOG.append((u.path, q, body, self.headers.get("Authorization")))
        if DROP_NEXT:
            DROP_NEXT.pop()
            self.close_connection = True
            return
        if q.get("topic") == "closing":
            self.close_connection = True
        op = u.path.rsplit("/", 1)[1]
        if q.get("topic") in ("short", "unterminated"):
            return self._truncated(q["topic"])
        if self.headers.get("Authorization") != "Bearer " + TOKEN:
            return self._send(401, b"unauthorized\n")
        if op == "publish":
            if q.get("id") == "clash":
                return self._send(409, b"conflict\n")
            return self._send(200, b"", {"Dostoevsky-Partition": "0", "Dostoevsky-Offset": "7"})
        if op == "read":
            if q["offset"] == "99":
                return self._send(204, b"")
            if q["offset"] == "77":
                return self._send(200, b"x" * ((1 << 20) + 1), {
                    "Dostoevsky-Partition": "0", "Dostoevsky-Offset": "77", "Dostoevsky-Key": "", "Dostoevsky-ID": "",
                    "Dostoevsky-Time": "t"})
            return self._send(200, b"\x00\xff payload", {
                "Dostoevsky-Partition": "0", "Dostoevsky-Offset": q["offset"],
                "Dostoevsky-Key": quote("+/λ", safe=""), "Dostoevsky-ID": "x",
                "Dostoevsky-Time": "2026-01-01T00:00:00Z"})
        if op == "position":
            return self._send(200, b"\x01\x02", {"Dostoevsky-Next": "3"})
        if op == "commit":
            return self._send(204, b"")
        self._send(404, b"not found\n")

    def _truncated(self, kind):
        """A reply that announces more than it delivers, then the connection ends."""
        self.send_response(200)
        for k, v in {"Dostoevsky-Partition": "0", "Dostoevsky-Offset": "1", "Dostoevsky-Key": "", "Dostoevsky-ID": "",
                     "Dostoevsky-Time": "t"}.items():
            self.send_header(k, v)
        if kind == "short":
            self.send_header("Content-Length", "10")
            self.end_headers()
            self.wfile.write(b"abc")
            self.close_connection = True  # keep-alive server: the client must see the stream end
        else:
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(b"3\r\nabc\r\n")  # no terminating zero-length chunk
            self.close_connection = True

    def _send(self, code, body, headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        if self.close_connection:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = _go


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), Stub)  # keep-alive needs a thread per connection
        cls.srv.daemon_threads = True
        cls.srv.block_on_close = False
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d/" % cls.srv.server_port

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def setUp(self):
        LOG.clear()
        CONNECTIONS.clear()
        DROP_NEXT.clear()
        self.c = Client(self.url, TOKEN)
        self.addCleanup(self.c.close)

    def test_publish_sends_binary_body_token_and_escaped_params(self):
        ack = self.c.publish("jobs", b"\x00\xff", key="+/λ", id="once")
        self.assertEqual((ack.partition, ack.offset), (0, 7))
        path, q, body, auth = LOG[0]
        self.assertEqual(path, "/dostoevsky/client/v1/publish")
        self.assertEqual((q["topic"], q["key"], q["id"]), ("jobs", "+/λ", "once"))
        self.assertEqual(body, b"\x00\xff")
        self.assertEqual(auth, "Bearer " + TOKEN)

    def test_read_returns_binary_value_and_decoded_key(self):
        r = self.c.read("jobs", 0, 5, wait_ms=100)
        self.assertEqual((r.offset, r.key, r.id, r.value), (5, "+/λ", "x", b"\x00\xff payload"))
        self.assertEqual(LOG[0][1]["wait_ms"], "100")

    def test_read_at_the_end_is_none(self):
        self.assertIsNone(self.c.read("jobs", 0, 99))

    def test_position_and_commit(self):
        p = self.c.position("jobs", 0, "g")
        self.assertEqual((p.next, p.state), (3, b"\x01\x02"))
        self.c.commit("jobs", 0, "g", 3, b"s")
        self.assertEqual(LOG[-1][1]["expected"], "3")
        self.assertEqual(LOG[-1][2], b"s")

    def test_the_token_only_travels_over_https_or_to_loopback(self):
        for ok in ("http://127.0.0.1:8787", "http://localhost:1", "http://[::1]:1", "https://example.com", "https://10.0.0.5:8787"):
            Client(ok, TOKEN)
        for bad in ("http://example.com", "http://10.0.0.5:8787", "http://0.0.0.0:1", "ftp://127.0.0.1", "127.0.0.1:8787", "http://localhost.example.com"):
            with self.assertRaises(ValueError, msg=bad):
                Client(bad, TOKEN)

    def test_an_oversized_response_is_refused(self):
        with self.assertRaises(ClientError) as e:
            self.c.read("jobs", 0, 77)
        self.assertEqual(e.exception.code, "oversized_response")

    def test_one_connection_serves_many_calls(self):
        for i in range(20):
            self.c.publish("jobs", b"x", id="i%d" % i)
            self.c.read("jobs", 0, 5)
        self.assertEqual(len(CONNECTIONS), 1)

    def test_threads_share_a_small_pool(self):
        def work():
            for _ in range(20):
                self.c.read("jobs", 0, 5)
        threads = [threading.Thread(target=work) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        self.assertLessEqual(len(CONNECTIONS), 8)
        self.assertLessEqual(len(self.c._idle), 16)

    def test_a_server_that_closes_the_connection_is_followed_by_a_new_one(self):
        self.c.publish("closing", b"x", id="a")
        self.c.publish("closing", b"x", id="b")
        self.assertEqual(len(CONNECTIONS), 2)

    def test_an_idle_connection_past_the_limit_is_replaced(self):
        self.c._idle_limit = 0.0
        self.c.read("jobs", 0, 5)
        time.sleep(0.01)
        self.c.read("jobs", 0, 5)
        self.assertEqual(len(CONNECTIONS), 2)

    def test_a_stale_reused_connection_is_retried_for_safe_calls_only(self):
        self.c.read("jobs", 0, 5)            # the connection is now pooled
        DROP_NEXT.append(1)
        self.assertEqual(self.c.read("jobs", 0, 5).offset, 5)   # retried on a fresh connection
        self.assertEqual(len(CONNECTIONS), 2)
        self.c.publish("jobs", b"x", id="safe")
        DROP_NEXT.append(1)
        self.assertEqual(self.c.publish("jobs", b"x", id="safe").offset, 7)  # carries an id: retried
        before = len([1 for entry in LOG if entry[0].endswith("publish")])
        DROP_NEXT.append(1)
        with self.assertRaises(ConnectionError):                 # no id: never repeated automatically
            self.c.publish("jobs", b"x")
        after = len([1 for entry in LOG if entry[0].endswith("publish")])
        self.assertEqual(after - before, 1)
        self.c.read("jobs", 0, 5)
        DROP_NEXT.append(1)
        with self.assertRaises(ConnectionError):                 # commit is never repeated
            self.c.commit("jobs", 0, "g", 3)

    def test_an_incomplete_body_is_an_error_never_a_truncated_success(self):
        for topic in ("short", "unterminated"):
            with self.assertRaises(ClientError, msg=topic) as e:
                self.c.read(topic, 0, 1)
            self.assertEqual(e.exception.code, "incomplete_response", topic)

    def test_a_failure_after_the_reply_began_is_never_replayed(self):
        self.c.read("jobs", 0, 5)  # pool a connection: the next call reuses it
        before = len([e for e in LOG if e[1].get("topic") == "short"])
        with self.assertRaises(ClientError):  # a read is safe to repeat, but only before any reply byte
            self.c.read("short", 0, 1)
        self.assertEqual(len([e for e in LOG if e[1].get("topic") == "short"]) - before, 1)

    def test_errors_carry_status_and_word(self):
        with self.assertRaises(ClientError) as e:
            self.c.publish("jobs", b"", id="clash")
        self.assertEqual((e.exception.status, e.exception.code), (409, "conflict"))
        with self.assertRaises(ClientError) as e:
            with Client(self.url, "wrong") as wrong:  # closed, so no socket is left for the GC to warn about
                wrong.position("jobs", 0, "g")
        self.assertEqual(e.exception.status, 401)


if __name__ == "__main__":
    unittest.main()

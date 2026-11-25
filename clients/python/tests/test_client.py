import os, sys, threading, unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, quote, urlparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dostoevsky import Client, ClientError  # noqa: E402

TOKEN = "a-test-token-with-entropy"
LOG = []  # (path, query, body) of every request the stub saw


class Stub(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _go(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query, keep_blank_values=True).items()}
        LOG.append((u.path, q, body, self.headers.get("Authorization")))
        op = u.path.rsplit("/", 1)[1]
        if self.headers.get("Authorization") != "Bearer " + TOKEN:
            return self._send(401, b"unauthorized\n")
        if op == "publish":
            if q.get("id") == "clash":
                return self._send(409, b"conflict\n")
            return self._send(200, b"", {"Dostoevsky-Partition": "0", "Dostoevsky-Offset": "7"})
        if op == "read":
            if q["offset"] == "99":
                return self._send(204, b"")
            return self._send(200, b"\x00\xff payload", {
                "Dostoevsky-Partition": "0", "Dostoevsky-Offset": q["offset"],
                "Dostoevsky-Key": quote("+/λ", safe=""), "Dostoevsky-ID": "x",
                "Dostoevsky-Time": "2026-01-01T00:00:00Z"})
        if op == "position":
            return self._send(200, b"\x01\x02", {"Dostoevsky-Next": "3"})
        if op == "commit":
            return self._send(204, b"")
        self._send(404, b"not found\n")

    def _send(self, code, body, headers=None):
        self.send_response(code)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_POST = _go


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.srv = HTTPServer(("127.0.0.1", 0), Stub)
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()
        cls.url = "http://127.0.0.1:%d/" % cls.srv.server_port

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def setUp(self):
        LOG.clear()
        self.c = Client(self.url, TOKEN)

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

    def test_errors_carry_status_and_word(self):
        with self.assertRaises(ClientError) as e:
            self.c.publish("jobs", b"", id="clash")
        self.assertEqual((e.exception.status, e.exception.code), (409, "conflict"))
        with self.assertRaises(ClientError) as e:
            Client(self.url, "wrong").position("jobs", 0, "g")
        self.assertEqual(e.exception.status, 401)


if __name__ == "__main__":
    unittest.main()

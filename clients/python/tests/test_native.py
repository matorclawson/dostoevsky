import asyncio, json, os, socket, struct, sys, threading, time, unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dostoevsky_client import ClientError, Position, Receipt, Record, _wire  # noqa: E402
from dostoevsky_client.native import AsyncNativeClient, ConnectionLost, NativeClient, Overloaded  # noqa: E402

with open(os.path.join(os.path.dirname(__file__), "..", "..", "..", "wire", "testdata", "vectors.json"), encoding="utf-8") as f:
    VECTORS = json.load(f)
TOKEN = "example-token-not-a-secret"


class Vectors(unittest.TestCase):
    def test_every_client_message_is_encoded_byte_for_byte(self):
        seen = 0
        for v in VECTORS["valid"]:
            if v["sender"] != "client":
                continue
            f = v["fields"]
            h = bytes.fromhex
            if v["name"] == "hello":
                got = _wire.hello(f["token"], f["max_version"])
            else:
                build = {
                    "PUBLISH": lambda: _wire.publish(f["topic"], h(f["value_hex"]), h(f["key_hex"]).decode(), h(f["id_hex"]).decode()),
                    "READ": lambda: _wire.read(f["topic"], f["partition"], int(f["offset"]), f["wait_ms"]),
                    "POSITION": lambda: _wire.position(f["topic"], f["partition"], f["group"]),
                    "COMMIT": lambda: _wire.commit(f["topic"], f["partition"], f["group"], int(f["expected"]), h(f["state_hex"])),
                    "PING": _wire.ping,
                }[f["type"]]
                got = _wire.set_id(build(), f["request_id"])
            self.assertEqual(bytes(got).hex(), v["hex"], v["name"])
            seen += 1
        self.assertEqual(seen, 7)

    def test_every_server_message_is_decoded(self):
        for v in VECTORS["valid"]:
            if v["sender"] != "server":
                continue
            data, f = bytes.fromhex(v["hex"]), v["fields"]
            if v["name"].startswith("welcome"):
                self.assertEqual(_wire.welcome(data), (f["status"], f["version"], f["max_body"], f["window"], f["window_bytes"]), v["name"])
                continue
            length, kind, request_id = _wire.header(data)
            self.assertEqual((length, request_id), (len(data) - _wire.HEADER, f["request_id"]), v["name"])
            got = _wire.reply(kind, data[_wire.HEADER:])
            name = f["type"]
            if name == "PUBLISH_OK":
                want = (f["partition"], int(f["offset"]))
            elif name == "READ_OK" and f["found"]:
                want = (f["partition"], int(f["offset"]), bytes.fromhex(f["key_hex"]).decode(), bytes.fromhex(f["id_hex"]).decode(),
                        "2025-11-25T22:00:00.123456789Z", bytes.fromhex(f["value_hex"]))
            elif name == "POSITION_OK":
                want = (int(f["next"]), bytes.fromhex(f["state_hex"]))
            elif name == "ERROR":
                want = f["code"]
            else:
                want = None
            self.assertEqual(got, want, v["name"])

    def test_every_malformed_server_message_is_rejected(self):
        for v in VECTORS["invalid"]:
            if v["sender"] != "server":
                continue
            data = bytes.fromhex(v["hex"])
            with self.assertRaises(_wire.Malformed, msg=v["name"]):
                _, kind, _ = _wire.header(data)
                _wire.reply(kind, data[_wire.HEADER:])


class ReplyBounds(unittest.TestCase):
    def test_reply_values_outside_the_value_table_are_malformed(self):
        def record(key=0, ident=0, value=0, offset=0):
            return (struct.pack("<BHQq", 1, 0, offset, 0) + struct.pack("<H", key) + b"a" * key
                    + struct.pack("<H", ident) + b"a" * ident + bytes(value))

        high = struct.pack("<Q", 1 << 63)
        for kind, body in ((0x81, bytes(2) + high), (0x82, record(offset=1 << 63)), (0x82, record(key=4097)),
                           (0x82, record(ident=257)), (0x82, record(value=(1 << 20) + 1)), (0x83, high),
                           (0x83, bytes(8 + (1 << 20) + 1)), (0xFF, b"\0"), (0xFF, b"\x0d")):
            with self.assertRaises(_wire.Malformed, msg=f"{kind:#x}, {len(body)} bytes"):
                _wire.reply(kind, body)
        self.assertIsNotNone(_wire.reply(0x82, record(4096, 256, 1 << 20)))

    def test_a_refusing_welcome_with_anything_else_set_is_malformed(self):
        def welcome(status, window):
            return b"DSTV" + struct.pack("<BHIHI", status, 0, 0, window, 0)

        self.assertEqual(_wire.welcome(welcome(2, 0))[0], 2)
        for data in (welcome(2, 1), welcome(99, 0)):
            with self.assertRaises(_wire.Malformed):
                _wire.welcome(data)

    def test_tls_with_a_unix_socket_is_refused(self):
        with self.assertRaisesRegex(ValueError, "unix socket"):
            NativeClient("/nonexistent.sock", TOKEN, tls=True)


def two_replies_at_once(fake, conn, kind, request_id, body):
    conn.sendall(frame(0x85, request_id) + frame(0x85, request_id + 1))


def exactly(conn, n):
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def frame(kind, request_id, body=b""):
    return struct.pack("<IBBI", len(body), kind, 0, request_id) + body


def publish_ok(offset):
    return struct.pack("<HQ", 0, offset)


class Fake:
    """A scripted server for one connection: reads HELLO, answers WELCOME, hands each request to on_frame."""

    def __init__(self, test, on_frame=lambda *a: None, status=0, window=256, window_bytes=4 << 20):
        self.on_frame, self.frames, self.conns = on_frame, [], []
        self.welcome = b"DSTV" + (struct.pack("<BHIHI", 0, 1, _wire.MAX_BODY, window, window_bytes) if status == 0
                                  else struct.pack("<BHIHI", status, 0, 0, 0, 0))
        self.listener = socket.create_server(("127.0.0.1", 0))
        self.address = ("127.0.0.1", self.listener.getsockname()[1])
        threading.Thread(target=self._serve, daemon=True).start()
        test.addCleanup(self.close)

    def _serve(self):
        try:
            conn, _ = self.listener.accept()
            self.conns.append(conn)
            exactly(conn, struct.unpack_from("<H", exactly(conn, 10), 8)[0])
            conn.sendall(self.welcome)
            while True:
                length, kind, _, request_id = struct.unpack("<IBBI", exactly(conn, 10))
                f = (kind, request_id, exactly(conn, length))
                self.frames.append(f)
                self.on_frame(self, conn, *f)
        except (OSError, EOFError):
            pass

    def close(self):
        for c in self.conns:
            c.close()
        self.listener.close()


def until(condition):
    deadline = time.monotonic() + 5
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError("timed out")
        time.sleep(0.005)


def reverse_three(fake, conn, kind, request_id, body):
    if len(fake.frames) == 3:
        for _, rid, b in reversed(fake.frames):
            conn.sendall(frame(0x81, rid, publish_ok(b[-1])))


class Sync(unittest.TestCase):
    def test_replies_out_of_order_reach_the_call_that_asked(self):
        fake = Fake(self, reverse_three)
        with NativeClient(fake.address, TOKEN) as c:
            pending = [c.publish_nowait("events", bytes([n])) for n in (7, 8, 9)]
            self.assertEqual([p.result().offset for p in pending], [7, 8, 9])

    def test_the_window_holds_requests_back_in_call_order(self):
        def answer(fake, conn, kind, request_id, body):
            time.sleep(0.05)
            conn.sendall(frame(0x81, request_id, publish_ok(len(fake.frames))))

        fake = Fake(self, answer, window=2)
        with NativeClient(fake.address, TOKEN) as c:
            pending = [c.publish_nowait("events", bytes([n])) for n in (1, 2, 3, 4)]
            self.assertIsNotNone(pending[0].result())
            c.drain()
            self.assertEqual([p.result().offset for p in pending], [1, 2, 3, 4])
        self.assertEqual([f[2][-1] for f in fake.frames], [1, 2, 3, 4])

    def test_a_lost_connection_is_not_retried(self):
        fake = Fake(self, lambda fake, conn, *a: conn.shutdown(socket.SHUT_RDWR))
        c = NativeClient(fake.address, TOKEN)
        with self.assertRaises(ConnectionLost) as lost:
            c.publish("events", b"x")
        self.assertTrue(lost.exception.sent)
        with self.assertRaises(ConnectionLost) as after:
            c.ping()
        self.assertFalse(after.exception.sent)
        self.assertEqual(len(fake.frames), 1)

    def test_a_request_error_keeps_the_connection(self):
        def answer(fake, conn, kind, request_id, body):
            conn.sendall(frame(0xFF, request_id, b"\x03") if len(fake.frames) == 1 else frame(0x81, request_id, publish_ok(5)))

        fake = Fake(self, answer)
        with NativeClient(fake.address, TOKEN) as c:
            with self.assertRaises(ClientError) as refused:
                c.publish("events", b"a")
            self.assertEqual((refused.exception.status, refused.exception.code), (409, "conflict"))
            self.assertEqual(c.publish("events", b"b"), Receipt(0, 5))

    def test_a_bad_reply_ends_the_connection(self):
        for name, answer in (("unknown id", lambda r: frame(0x85, r + 1)), ("wrong type", lambda r: frame(0x84, r)),
                             ("bytes left over", lambda r: frame(0x85, r, b"\0")), ("connection error", lambda r: frame(0xFF, 0, b"\x0b"))):
            with self.subTest(name):
                fake = Fake(self, lambda fake, conn, kind, request_id, body, a=answer: conn.sendall(a(request_id)))
                c = NativeClient(fake.address, TOKEN)
                with self.assertRaises(ConnectionLost):
                    c.ping()
                with self.assertRaises(ConnectionLost):
                    c.ping()

    def test_a_refused_or_unusable_welcome(self):
        for options, code in (({"status": 2}, "unauthorized"), ({"window": 0}, "protocol"),
                              ({"window_bytes": _wire.MAX_BODY + _wire.HEADER - 1}, "protocol")):
            with self.subTest(options=options):
                fake = Fake(self, **options)
                with self.assertRaises(ClientError) as refused:
                    NativeClient(fake.address, TOKEN)
                self.assertEqual(refused.exception.code, code)
                self.assertNotIn(TOKEN, str(refused.exception))
                self.assertEqual(fake.frames, [])

    def test_forbidden_values_are_refused_before_anything_is_sent(self):
        fake = Fake(self)
        with NativeClient(fake.address, TOKEN) as c:
            for call, code in (
                (lambda: c.publish("events", bytes((1 << 20) + 1)), "limit"),
                (lambda: c.publish("events", b"", id="i" * 257), "limit"),
                (lambda: c.publish("events", b"", key="\ud800"), "invalid"),
                (lambda: c.read("events", 0, -1), "invalid"),
                (lambda: c.read("events", 0, 0, wait_ms=30001), "invalid"),
                (lambda: c.commit("events", 70000, "g", 0), "invalid"),
            ):
                with self.assertRaises(ClientError) as refused:
                    call()
                self.assertEqual(refused.exception.code, code)
        self.assertEqual(fake.frames, [])

    def test_a_buffered_reply_cannot_answer_a_request_sent_after_it_arrived(self):
        fake = Fake(self, two_replies_at_once, window=1)
        with NativeClient(fake.address, TOKEN) as c:
            c.ping()
            # The second reply came before the second ping existed: the connection is already lost or is now.
            with self.assertRaises(ConnectionLost):
                c.ping()

    def test_no_plaintext_off_loopback(self):
        for host in ("192.0.2.1", "example.com"):
            with self.assertRaisesRegex(ValueError, "without TLS"):
                NativeClient((host, 8788), TOKEN)


class Async(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_publishes_share_the_connection(self):
        fake = Fake(self, reverse_three)
        async with await AsyncNativeClient.connect(fake.address, TOKEN) as c:
            got = await asyncio.gather(*(c.publish("events", bytes([n])) for n in (7, 8, 9)))
        self.assertEqual([r.offset for r in got], [7, 8, 9])
        self.assertEqual([f[2][-1] for f in fake.frames], [7, 8, 9])

    async def test_a_cancelled_caller_still_frees_its_place(self):
        fake = Fake(self, window=1)
        async with await AsyncNativeClient.connect(fake.address, TOKEN) as c:
            first = asyncio.create_task(c.publish("events", b"1"))
            await asyncio.get_running_loop().run_in_executor(None, until, lambda: len(fake.frames) == 1)
            first.cancel()
            second = asyncio.create_task(c.publish("events", b"2"))
            await asyncio.sleep(0.05)
            self.assertEqual(len(fake.frames), 1)
            fake.conns[0].sendall(frame(0x81, fake.frames[0][1], publish_ok(0)))
            await asyncio.get_running_loop().run_in_executor(None, until, lambda: len(fake.frames) == 2)
            fake.conns[0].sendall(frame(0x81, fake.frames[1][1], publish_ok(1)))
            self.assertEqual((await second).offset, 1)

    async def test_the_clients_own_ceiling_refuses_without_sending(self):
        fake = Fake(self, window=1)
        c = await AsyncNativeClient.connect(fake.address, TOKEN, max_requests=2)
        held = [asyncio.create_task(c.ping()) for _ in range(2)]
        await asyncio.sleep(0.05)
        with self.assertRaises(Overloaded):
            await c.ping()
        self.assertEqual(len(fake.frames), 1)
        await c.close()
        await asyncio.gather(*held, return_exceptions=True)
        with self.assertRaisesRegex(ValueError, "max_bytes"):
            await AsyncNativeClient.connect(fake.address, TOKEN, max_bytes=1000)

    async def test_a_buffered_reply_cannot_answer_a_later_request(self):
        fake = Fake(self, two_replies_at_once, window=1)
        c = await AsyncNativeClient.connect(fake.address, TOKEN)
        first, second = await asyncio.gather(c.ping(), c.ping(), return_exceptions=True)
        self.assertIsNone(first)
        self.assertIsInstance(second, ConnectionLost)
        await c.close()

    async def test_a_lost_connection(self):
        fake = Fake(self, lambda fake, conn, *a: conn.shutdown(socket.SHUT_RDWR))
        c = await AsyncNativeClient.connect(fake.address, TOKEN)
        with self.assertRaises(ConnectionLost) as lost:
            await c.publish("events", b"x")
        self.assertTrue(lost.exception.sent)
        with self.assertRaises(ConnectionLost) as after:
            await c.ping()
        self.assertFalse(after.exception.sent)
        await c.close()


if __name__ == "__main__":
    unittest.main()

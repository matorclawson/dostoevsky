"""Native clients for the Dostoevsky wire protocol, version 1 (docs/wire-v1.md): one connection, many requests at once.

PREVIEW: no released Dostoevsky server speaks this protocol yet.

`NativeClient` is synchronous and starts no thread: its calls do the socket I/O themselves. `AsyncNativeClient` is
for asyncio. Both have the methods and results of the HTTP `Client`. Nothing is retried and there is no reconnect:
after the connection is lost every call fails, and a request that may have reached the server has an unknown outcome
(`ConnectionLost.sent`).

An `address` is `(host, port)` or the path of a unix socket. The token is sent only over verified TLS (`tls=True`,
`cafile` for a private CA), to a loopback address, or to a unix socket.
"""
import asyncio
import ipaddress
import selectors
import socket
import ssl
import threading
import time

from . import ClientError, Position, Receipt, Record
from . import _wire

__all__ = ["NativeClient", "AsyncNativeClient", "ClientError", "ConnectionLost", "Overloaded"]

# The client's own ceilings. The server's window can only lower what is in flight, never raise it above these.
_MAX_IN_FLIGHT = 256
_MAX_IN_FLIGHT_BYTES = 4 << 20
_HANDSHAKE_SECONDS = 5.0
_REQUEST_SECONDS = 40.0
_CHUNK = 65536


class ConnectionLost(ClientError):
    """The connection ended. `sent` True: the request may have taken effect."""

    def __init__(self, sent: bool):
        super().__init__(0, "connection_lost")
        self.sent = sent


class Overloaded(ClientError):
    """The client's own queue is full: the request was refused here and never sent."""

    def __init__(self):
        super().__init__(0, "client_overloaded")


def _failure(code: int) -> ClientError:
    name, status = _wire.CODES.get(code, ("protocol", 400))
    return ClientError(status, name)


def _build(make, *args):
    try:
        return make(*args)
    except _wire.Refused as error:
        raise ClientError(413 if error.code == "limit" else 400, error.code) from None


def _is_loopback_address(text: str) -> bool:
    try:
        ip = ipaddress.ip_address(text.split("%")[0])
    except ValueError:
        return False
    return (getattr(ip, "ipv4_mapped", None) or ip).is_loopback


def _check_target(address, tls: bool, cafile=None) -> None:
    if isinstance(address, str) and (tls or cafile is not None):
        raise ValueError("tls does not apply to a unix socket: remove one of the two")
    if not isinstance(address, str) and not tls and address[0] != "localhost" and not _is_loopback_address(address[0]):
        raise ValueError("refusing to send the token without TLS to a non-loopback address; use tls=True")


def _context(cafile) -> ssl.SSLContext:
    context = ssl.create_default_context(cafile=cafile)  # verifies the chain and the host name
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    return context


def _accept(data: bytes):
    """(window, window_bytes) from a WELCOME, or the reason to give up."""
    try:
        status, version, max_body, window, window_bytes = _wire.welcome(data)
    except _wire.Malformed:
        raise _failure(11) from None
    if status:
        raise _failure(status)
    if not _wire.acceptable(version, max_body, window, window_bytes):
        raise _failure(11)
    return min(window, _MAX_IN_FLIGHT), min(window_bytes, _MAX_IN_FLIGHT_BYTES)


def _result(kind: int, value):
    if kind == _wire.PUBLISH:
        return Receipt(*value)
    if kind == _wire.READ:
        return None if value is None else Record(*value)
    if kind == _wire.POSITION:
        return Position(*value)
    return None


class _Ids:
    def __init__(self):
        self.last = 0

    def next(self, used) -> int:
        while True:
            self.last = 1 if self.last == 0xFFFFFFFF else self.last + 1
            if self.last not in used:
                return self.last


class Pending:
    """A request that has been handed to a `NativeClient`; `result()` waits for its answer."""

    def __init__(self, client, kind, size, start, deadline):
        self._client, self._kind, self._size = client, kind, size
        self._start, self._deadline = start, deadline
        self._done, self._value, self._error = False, None, None

    def _finish(self, value=None, error=None):
        self._done, self._value, self._error = True, value, error

    def result(self):
        with self._client._lock:
            while not self._done:
                self._client._pump()
        if self._error is not None:
            raise self._error
        return self._value


class NativeClient:
    """Synchronous. One lock serializes its use: a thread waiting in `read` holds up the others, so give each thread
    its own client, or use `AsyncNativeClient`."""

    def __init__(self, address, token: str, *, tls: bool = False, cafile: str | None = None):
        first = _build(_wire.hello, token)
        _check_target(address, tls, cafile)
        deadline = time.monotonic() + _HANDSHAKE_SECONDS
        sock = self._open(address, tls, cafile)
        try:
            sock.settimeout(max(deadline - time.monotonic(), 0.001))
            sock.sendall(first)
            data = b""
            while len(data) < _wire.WELCOME_BYTES:
                sock.settimeout(max(deadline - time.monotonic(), 0.001))
                chunk = sock.recv(_wire.WELCOME_BYTES - len(data))
                if not chunk:
                    raise ConnectionLost(False)
                data += chunk
            self._window, self._window_bytes = _accept(data)
            sock.setblocking(False)
        except OSError:
            sock.close()
            raise ConnectionLost(False) from None
        except BaseException:
            sock.close()
            raise
        self._sock = sock
        self._selector = selectors.DefaultSelector()
        self._selector.register(sock, selectors.EVENT_READ)
        self._lock = threading.RLock()
        self._in, self._out = bytearray(), bytearray()
        self._queued = self._written = self._unanswered = 0  # byte counts
        self._pending = {}  # request id -> Pending, oldest first
        self._ids = _Ids()
        self._carried = 0  # bytes written when the first byte of a partly received reply arrived
        self._dead = False

    @staticmethod
    def _open(address, tls, cafile):
        sock = None
        try:
            if isinstance(address, str):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(_HANDSHAKE_SECONDS)
                sock.connect(address)
                return sock
            sock = socket.create_connection(address, timeout=_HANDSHAKE_SECONDS)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if tls:
                return _context(cafile).wrap_socket(sock, server_hostname=address[0])
        except OSError:
            if sock is not None:
                sock.close()
            raise ConnectionLost(False) from None
        # Without TLS it is the address actually connected to that must be loopback, whatever the name resolved to.
        if not _is_loopback_address(sock.getpeername()[0]):
            sock.close()
            raise ValueError("refusing to send the token: the connected address is not loopback")
        return sock

    # -- I/O, always under the lock

    def _receive(self) -> bool:
        progressed = False
        while not self._dead:
            try:
                chunk = self._sock.recv(_CHUNK)
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                break
            except OSError:
                self._fail()
                break
            if not chunk:
                self._fail()
                break
            progressed = True
            arrived = self._carried if self._in else self._written
            self._in += chunk
            self._parse(arrived)
        return progressed

    def _send(self) -> bool:
        progressed = False
        while self._out and not self._dead:
            try:
                n = self._sock.send(bytes(self._out[:_CHUNK]))
            except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                break
            except OSError:
                self._fail()
                break
            del self._out[:n]
            self._written += n
            progressed = True
        return progressed

    def _parse(self, arrived):
        try:
            while not self._dead and len(self._in) >= _wire.HEADER:
                length, kind, request_id = _wire.header(self._in)
                if len(self._in) < _wire.HEADER + length:
                    break
                body = bytes(self._in[_wire.HEADER:_wire.HEADER + length])
                del self._in[:_wire.HEADER + length]
                self._answer(kind, request_id, body, arrived)
                arrived = self._written  # nothing is written while a batch is parsed
            self._carried = arrived
        except _wire.Malformed:
            self._fail()

    def _answer(self, kind, request_id, body, arrived):
        if request_id == 0:  # a connection error: the server closes after it
            if kind != _wire.ERROR:
                raise _wire.Malformed("request id 0")
            _wire.reply(kind, body)
            return self._fail()
        request = self._pending.get(request_id)
        if request is None or request._start + request._size > arrived or kind not in (_wire.ERROR, _wire.REPLY | request._kind):
            raise _wire.Malformed("a reply that answers nothing")
        value = _wire.reply(kind, body)
        del self._pending[request_id]
        self._unanswered -= request._size
        if kind == _wire.ERROR:
            request._finish(error=_failure(value))
        else:
            request._finish(_result(request._kind, value))

    def _pump(self):
        """Moves bytes both ways, waiting if there is nothing to do. Replies are read while requests are written."""
        if self._dead:
            return
        received = self._receive()
        sent = self._send()
        if received or sent or self._dead:
            return
        wait = None
        if self._pending:
            wait = next(iter(self._pending.values()))._deadline - time.monotonic()
            if wait <= 0:
                return self._fail()
        events = selectors.EVENT_READ | (selectors.EVENT_WRITE if self._out else 0)
        self._selector.modify(self._sock, events)
        self._selector.select(wait)

    def _fail(self):
        if self._dead:
            return
        self._dead = True
        self._selector.close()
        self._sock.close()
        for request in self._pending.values():
            request._finish(error=ConnectionLost(self._written > request._start))
        self._pending.clear()
        self._out.clear()
        self._in.clear()

    def _submit(self, kind, frame) -> Pending:
        with self._lock:
            while not self._dead and (len(self._pending) >= self._window or self._unanswered + len(frame) > self._window_bytes):
                self._pump()
            if self._dead:
                raise ConnectionLost(False)
            request_id = self._ids.next(self._pending)
            request = Pending(self, kind, len(frame), self._queued, time.monotonic() + _REQUEST_SECONDS)
            self._out += _wire.set_id(frame, request_id)
            self._queued += len(frame)
            self._unanswered += len(frame)
            self._pending[request_id] = request
            self._receive()
            self._send()
            return request

    # -- the API

    def publish_nowait(self, topic: str, value: bytes, *, key: str = "", id: str = "") -> Pending:
        """Hands a publish to the connection without waiting for its answer (it waits only for room in the server's
        window). Publishes are appended in call order. Bytes move only during this client's calls: use `result()` or
        `drain()`."""
        return self._submit(_wire.PUBLISH, _build(_wire.publish, topic, value, key, id))

    def drain(self) -> None:
        """Waits until nothing is unanswered. Errors are reported by each request's `result()`."""
        with self._lock:
            while self._pending and not self._dead:
                self._pump()

    def publish(self, topic: str, value: bytes, *, key: str = "", id: str = "") -> Receipt:
        return self.publish_nowait(topic, value, key=key, id=id).result()

    def read(self, topic: str, partition: int, offset: int, *, wait_ms: int = 0) -> Record | None:
        return self._submit(_wire.READ, _build(_wire.read, topic, partition, offset, wait_ms)).result()

    def position(self, topic: str, partition: int, group: str) -> Position:
        return self._submit(_wire.POSITION, _build(_wire.position, topic, partition, group)).result()

    def commit(self, topic: str, partition: int, group: str, expected: int, state: bytes = b"") -> None:
        self._submit(_wire.COMMIT, _build(_wire.commit, topic, partition, group, expected, state)).result()

    def ping(self) -> None:
        self._submit(_wire.PING, _wire.ping()).result()

    def close(self) -> None:
        with self._lock:
            self._fail()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class AsyncNativeClient:
    """asyncio. Make one with `await AsyncNativeClient.connect(address, token)`; use it from one event loop."""

    def __init__(self, reader, writer, window, window_bytes, max_requests, max_bytes):
        self._reader, self._writer = reader, writer
        self._max_requests, self._max_bytes = max_requests, max_bytes
        self._calls = self._held = 0  # unanswered requests and their frame bytes, waiting ones included
        self._sent = 0  # requests written
        self._window, self._window_bytes = window, window_bytes
        self._pending = {}  # request id -> (type, size, future, sequence)
        self._unanswered = 0
        self._ids = _Ids()
        self._dead = False
        self._turn = asyncio.Lock()  # first come, first served: requests leave in call order
        self._room = asyncio.Event()
        self._task = asyncio.get_running_loop().create_task(self._receive())

    @classmethod
    async def connect(cls, address, token: str, *, tls: bool = False, cafile: str | None = None,
                      max_requests: int = 1024, max_bytes: int = 8 << 20):
        """`max_requests` and `max_bytes` bound what is unanswered here, calls waiting for the window included; a call
        beyond them raises `Overloaded`. `max_bytes` is at least one maximum frame."""
        if max_requests < 1 or max_bytes < _wire.MAX_BODY + _wire.HEADER:
            raise ValueError("max_requests must be at least 1 and max_bytes at least one maximum frame")
        first = _build(_wire.hello, token)
        _check_target(address, tls, cafile)
        writer = None

        async def handshake():
            nonlocal writer
            if isinstance(address, str):
                reader, writer = await asyncio.open_unix_connection(address)
            else:
                reader, writer = await asyncio.open_connection(*address, ssl=_context(cafile) if tls else None)
                if not tls and not _is_loopback_address(writer.get_extra_info("peername")[0]):
                    raise ValueError("refusing to send the token: the connected address is not loopback")
            writer.write(first)
            return reader, await reader.readexactly(_wire.WELCOME_BYTES)

        try:
            reader, data = await asyncio.wait_for(handshake(), _HANDSHAKE_SECONDS)
            window, window_bytes = _accept(data)
        except (OSError, EOFError, asyncio.TimeoutError):
            if writer is not None:
                writer.close()
            raise ConnectionLost(False) from None
        except BaseException:
            if writer is not None:
                writer.close()
            raise
        return cls(reader, writer, window, window_bytes, max_requests, max_bytes)

    async def _receive(self):
        buffer = bytearray()
        carried = 0  # self._sent when the first byte of a partly received reply arrived
        try:
            while True:
                chunk = await self._reader.read(_CHUNK)
                if not chunk:
                    return
                # A reply may only answer a request written before its first byte arrived. There is no await while a
                # batch is parsed, so no request can be sent between two replies of it.
                arrived = carried if buffer else self._sent
                buffer += chunk
                while len(buffer) >= _wire.HEADER:
                    length, kind, request_id = _wire.header(buffer)
                    if len(buffer) < _wire.HEADER + length:
                        break
                    body = bytes(buffer[_wire.HEADER:_wire.HEADER + length])
                    del buffer[:_wire.HEADER + length]
                    if request_id == 0:
                        if kind != _wire.ERROR:
                            raise _wire.Malformed("request id 0")
                        _wire.reply(kind, body)
                        return
                    self._answer(kind, request_id, body, arrived)
                    arrived = self._sent
                carried = arrived
        except (OSError, EOFError, _wire.Malformed):
            pass
        finally:
            self._fail()

    def _answer(self, kind, request_id, body, arrived):
        request = self._pending.get(request_id)
        if request is None or request[3] > arrived or kind not in (_wire.ERROR, _wire.REPLY | request[0]):
            raise _wire.Malformed("a reply that answers nothing")
        value = _wire.reply(kind, body)
        del self._pending[request_id]
        wanted, size, future, _ = request
        self._unanswered -= size
        self._room.set()
        if future.done():
            return
        if kind == _wire.ERROR:
            future.set_exception(_failure(value))
        else:
            future.set_result(_result(wanted, value))

    def _fail(self):
        if self._dead:
            return
        self._dead = True
        self._writer.close()
        for _, _, future, _ in self._pending.values():
            if not future.done():
                future.set_exception(ConnectionLost(True))
        self._pending.clear()
        self._room.set()

    async def _call(self, kind, frame):
        size = len(frame)
        if self._dead:
            raise ConnectionLost(False)
        if self._calls >= self._max_requests or self._held + size > self._max_bytes:
            raise Overloaded()
        self._calls += 1
        self._held += size
        try:
            future = await self._write(kind, frame)
        except BaseException:
            self._release(size)
            raise
        # The place is held until the reply (or the loss of the connection), not until the caller stops waiting.
        future.add_done_callback(lambda _: self._release(size))
        return await self._wait(future)

    def _release(self, size):
        self._calls -= 1
        self._held -= size

    async def _write(self, kind, frame):
        async with self._turn:
            while not self._dead and (len(self._pending) >= self._window or self._unanswered + len(frame) > self._window_bytes):
                self._room.clear()
                await self._room.wait()
            if self._dead:
                raise ConnectionLost(False)
            request_id = self._ids.next(self._pending)
            future = asyncio.get_running_loop().create_future()
            # A caller that gives up must not leave an "exception was never retrieved" warning behind.
            future.add_done_callback(lambda f: f.cancelled() or f.exception())
            self._sent += 1
            self._pending[request_id] = (kind, len(frame), future, self._sent)
            self._unanswered += len(frame)
            self._writer.write(_wire.set_id(frame, request_id))
        return future

    async def _wait(self, future):
        # Shielded: cancelling the caller does not unsend the request, whose reply still frees its place in the window.
        try:
            await asyncio.wait_for(self._writer.drain(), _REQUEST_SECONDS)
            return await asyncio.wait_for(asyncio.shield(future), _REQUEST_SECONDS)
        except (OSError, asyncio.TimeoutError):
            # No answer in time, or the connection broke while writing. A reply that did arrive still stands.
            if not future.done():
                self._fail()
            return future.result()

    async def publish(self, topic: str, value: bytes, *, key: str = "", id: str = "") -> Receipt:
        return await self._call(_wire.PUBLISH, _build(_wire.publish, topic, value, key, id))

    async def read(self, topic: str, partition: int, offset: int, *, wait_ms: int = 0) -> Record | None:
        return await self._call(_wire.READ, _build(_wire.read, topic, partition, offset, wait_ms))

    async def position(self, topic: str, partition: int, group: str) -> Position:
        return await self._call(_wire.POSITION, _build(_wire.position, topic, partition, group))

    async def commit(self, topic: str, partition: int, group: str, expected: int, state: bytes = b"") -> None:
        await self._call(_wire.COMMIT, _build(_wire.commit, topic, partition, group, expected, state))

    async def ping(self) -> None:
        await self._call(_wire.PING, _wire.ping())

    async def close(self) -> None:
        self._fail()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

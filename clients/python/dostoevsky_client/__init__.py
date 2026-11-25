"""Synchronous Python client for the Dostoevsky v1 HTTP API.

Connections are reused (HTTP/1.1 keep-alive) from a small pool, so a Client is safe to
share between threads. Redirects are never followed.
"""
import http.client
import ipaddress
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlencode, unquote, urlsplit


class ClientError(Exception):
    def __init__(self, status, code):
        self.status, self.code = status, code
        super().__init__(f"{status} {code}")


@dataclass(frozen=True)
class Receipt:
    partition: int
    offset: int


@dataclass(frozen=True)
class Record(Receipt):
    key: str
    id: str
    time: str
    value: bytes


@dataclass(frozen=True)
class Position:
    next: int
    state: bytes


MAX_BODY = 1 << 20  # the protocol's largest value, key or checkpoint state


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _check_url(url: str) -> None:
    """The bearer token grants the whole store: only https, or http to loopback, may carry it."""
    parts = urlsplit(url)
    if parts.scheme == "https" and parts.hostname:
        return
    if parts.scheme == "http" and parts.hostname and _is_loopback(parts.hostname):
        return
    raise ValueError("refusing to send the token over plain http to a non-loopback address; use https://")


def _bounded(response, limit: int) -> bytes:
    data = response.read(limit + 1)
    if len(data) > limit:
        raise ClientError(response.status, "oversized_response")
    return data


_MAX_IDLE = 16        # idle connections kept per Client
_IDLE_LIMIT = 30.0    # seconds; the gateway closes idle connections at 60


class Client:
    def __init__(self, url: str, token: str):
        _check_url(url)
        parts = urlsplit(url)
        self._https = parts.scheme == "https"
        self._host, self._port = parts.hostname, parts.port
        self._prefix = parts.path.rstrip("/") + "/dostoevsky/client/v1/"
        self._auth = {"Authorization": "Bearer " + token, "Content-Type": "application/octet-stream"}
        self._idle = []  # (connection, time it went idle), most recent last
        self._lock = threading.Lock()
        self._idle_limit = _IDLE_LIMIT

    def close(self) -> None:
        with self._lock:
            idle, self._idle = self._idle, []
        for conn, _ in idle:
            conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _connect(self):
        make = http.client.HTTPSConnection if self._https else http.client.HTTPConnection
        return make(self._host, self._port, timeout=40)

    def _take(self):
        """A pooled connection that has not idled too long, else None."""
        now = time.monotonic()
        with self._lock:
            while self._idle:
                conn, since = self._idle.pop()
                if now - since <= self._idle_limit:
                    return conn
                conn.close()
        return None

    def _give(self, conn) -> None:
        with self._lock:
            if len(self._idle) < _MAX_IDLE:
                self._idle.append((conn, time.monotonic()))
                return
        conn.close()

    def _once(self, conn, method, path, data):
        try:
            conn.request(method, path, body=data, headers=self._auth)
            response = conn.getresponse()
            body = response.read(MAX_BODY + 1)
        except (http.client.HTTPException, OSError) as error:
            conn.close()
            if isinstance(error, OSError):
                raise
            raise ConnectionError(str(error)) from error
        if len(body) > MAX_BODY:
            conn.close()
            raise ClientError(response.status, "oversized_response")
        if response.will_close:
            conn.close()
        else:
            self._give(conn)
        if not 200 <= response.status < 300:  # includes redirects, which are never followed
            raise ClientError(response.status, body[:256].decode(errors="replace").strip())
        return response.status, response.headers, body

    def _call(self, op, params, data=None, retry_safe=True):
        """One call. A request that fails on a REUSED connection before any response is
        retried once on a fresh one, but only when repeating it is safe (reads, positions and
        publishes that carry an id); anything else surfaces the error rather than risk a
        duplicate."""
        path = self._prefix + op + "?" + urlencode(params)
        method = "GET" if data is None else "POST"
        conn = self._take()
        if conn is not None:
            try:
                return self._once(conn, method, path, data)
            except ConnectionError:  # not a timeout: a long poll must not wait twice
                if not retry_safe:
                    raise
        return self._once(self._connect(), method, path, data)

    def publish(self, topic: str, value: bytes, *, key: str = "", id: str = "") -> Receipt:
        _, h, _ = self._call("publish", dict(topic=topic, key=key, id=id), value, retry_safe=bool(id))
        return Receipt(int(h["Dostoevsky-Partition"]), int(h["Dostoevsky-Offset"]))

    def read(self, topic: str, partition: int, offset: int, *, wait_ms: int = 0) -> Record | None:
        status, h, data = self._call("read", dict(topic=topic, partition=partition, offset=offset, wait_ms=wait_ms))
        if status == 204:
            return None
        return Record(int(h["Dostoevsky-Partition"]), int(h["Dostoevsky-Offset"]),
                      unquote(h["Dostoevsky-Key"]), unquote(h["Dostoevsky-ID"]), h["Dostoevsky-Time"], data)

    def position(self, topic: str, partition: int, group: str) -> Position:
        _, h, data = self._call("position", dict(topic=topic, partition=partition, group=group))
        return Position(int(h["Dostoevsky-Next"]), data)

    def commit(self, topic: str, partition: int, group: str, expected: int, state: bytes = b"") -> None:
        # Never retried automatically: a repeat after a lost reply would answer 409.
        self._call("commit", dict(topic=topic, partition=partition, group=group, expected=expected), state, retry_safe=False)

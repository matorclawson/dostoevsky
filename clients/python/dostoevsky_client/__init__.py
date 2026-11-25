"""Synchronous Python client for the Dostoevsky v1 HTTP API."""
import http.client
import ipaddress
from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.parse import urlencode, unquote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


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
    """The whole body, at most `limit` bytes. A body that ends before the announced length
    (Content-Length short, or chunked without its terminating chunk) is an error, never a
    success with a truncated value."""
    try:
        data = response.read(limit + 1)
    except http.client.IncompleteRead as error:
        raise ClientError(response.status, "incomplete_response") from error
    if len(data) > limit:
        raise ClientError(response.status, "oversized_response")
    if response.length:  # bytes still owed by a Content-Length body
        raise ClientError(response.status, "incomplete_response")
    return data


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


class Client:
    def __init__(self, url: str, token: str):
        _check_url(url)
        self._url = url.rstrip("/") + "/dostoevsky/client/v1/"
        self._auth = {"Authorization": "Bearer " + token, "Content-Type": "application/octet-stream"}
        self._http = build_opener(_NoRedirect())

    def _call(self, op, params, data=None):
        request = Request(self._url + op + "?" + urlencode(params), data=data, headers=self._auth)
        try:
            with self._http.open(request, timeout=40) as response:
                return response.status, response.headers, _bounded(response, MAX_BODY)
        except HTTPError as error:
            with error:
                raise ClientError(error.code, error.read(256).decode(errors="replace").strip()) from error

    def publish(self, topic: str, value: bytes, *, key: str = "", id: str = "") -> Receipt:
        _, h, _ = self._call("publish", dict(topic=topic, key=key, id=id), value)
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
        self._call("commit", dict(topic=topic, partition=partition, group=group, expected=expected), state)

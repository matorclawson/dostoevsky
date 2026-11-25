# dostoevsky-client (Python)

Install name `dostoevsky-client`, import name `dostoevsky_client`. (The bare name `dostoevsky` on PyPI is an unrelated sentiment-analysis package; this client never uses it, so both can be installed together.)

A standard-library-only client for Dostoevsky's HTTP API. Python 3.10 or newer.

```sh
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes go run ./cmd/dostoevsky-gateway &   # from the repository root
pip install ./clients/python                                                      # or: PYTHONPATH=clients/python
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes python3 examples/clients/python/worker.py
```

```python
from dostoevsky_client import Client
c = Client("http://127.0.0.1:8787", token)
ack = c.publish("jobs", b"payload", key="k", id="once")      # same id + payload again returns the same offset
rec = c.read("jobs", ack.partition, ack.offset, wait_ms=1000)  # None when nothing arrives in time
c.commit("jobs", ack.partition, "workers", expected=ack.offset, state=b"")
```

Connections are reused (HTTP/1.1 keep-alive, a small pool, thread-safe); use `with Client(...) as c:` or `c.close()` to release them. A call that fails on a reused connection before any reply is retried once on a fresh one only when repeating it is safe: reads, positions, and publishes that carry an `id`. An id-less publish or a commit is never repeated for you.

Errors raise `ClientError(status, code)` where `code` is the gateway's word (`conflict`, `expired`, `limit`, ...).

## Native clients (preview)

`dostoevsky_client.native` speaks the binary [wire protocol](https://github.com/rodriveracom/dostoevsky/blob/main/docs/wire-v1.md). **No released Dostoevsky server speaks it yet**, so it is tested only against the protocol's test vectors and a scripted server. No speed or memory claim is made.

```python
from dostoevsky_client.native import NativeClient, AsyncNativeClient
with NativeClient(("127.0.0.1", 8788), token) as c:
    pending = [c.publish_nowait("jobs", job) for job in jobs]   # in flight together, appended in this order
    acks = [p.result() for p in pending]

async with await AsyncNativeClient.connect(("127.0.0.1", 8788), token) as c:
    acks = await asyncio.gather(*(c.publish("jobs", job) for job in jobs))
```

The methods are those of `Client`. `NativeClient` starts no thread, so bytes move only during its calls. Nothing is retried and there is no reconnect: after `ConnectionLost`, connect again; if `sent` is true the request may have taken effect.

Test (no server needed, uses a local stub): `python3 -m unittest discover -s clients/python/tests`

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

Errors raise `ClientError(status, code)` where `code` is the gateway's word (`conflict`, `expired`, `limit`, ...).

Test (no server needed, uses a local stub): `python3 -m unittest discover -s clients/python/tests`

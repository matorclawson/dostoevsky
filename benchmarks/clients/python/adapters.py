"""One adapter per engine. Each takes the server's address and token and offers
publish(bytes) -> offset (returned only after the engine's durable acknowledgment, never retried),
read(offset) -> bytes, and close(). Third-party libraries are imported only by the adapter that
needs them, so the rest of the worker runs with the standard library alone."""
import asyncio
import importlib.metadata
import os
import threading

TOPIC = "events"


def _version(dist: str) -> str:
    return f"{dist}-{importlib.metadata.version(dist)}"


class Dostoevsky:
    """Our client through the gateway (HTTP/1.1 keep-alive, a small connection pool)."""

    def __init__(self, addr, token, workers):
        from dostoevsky_client import Client  # PYTHONPATH=clients/python
        self.client_name = "dostoevsky-client-0.1.0(keep-alive)"
        self._c = Client("http://" + addr, token)

    def publish(self, data):
        return self._c.publish(TOPIC, data).offset

    def read(self, offset):
        rec = self._c.read(TOPIC, 0, offset)
        if rec is None:
            raise LookupError(f"offset {offset} is past the end")
        return rec.value

    def close(self):
        self._c.close()


class Kafka:
    """confluent-kafka (librdkafka): acks=all, no retries, no idempotence, one message per batch."""

    def __init__(self, addr, token, workers):
        from confluent_kafka import Consumer, Producer, TopicPartition  # noqa: F401
        self.client_name = _version("confluent-kafka")
        self._addr = addr
        self._p = Producer({
            "bootstrap.servers": addr, "acks": "all", "enable.idempotence": False, "retries": 0,
            "linger.ms": 0, "batch.num.messages": 1, "queue.buffering.max.messages": 100000,
        })
        self._stop = threading.Event()
        self._poller = threading.Thread(target=self._poll, daemon=True)
        self._poller.start()
        self._consumer = None
        self._loaded = {}

    def _poll(self):
        while not self._stop.is_set():
            self._p.poll(0.02)

    def publish(self, data):
        done, box = threading.Event(), []

        def delivered(err, msg):
            box.append((err, msg))
            done.set()

        self._p.produce(TOPIC, value=data, on_delivery=delivered)
        if not done.wait(60):
            raise TimeoutError("no delivery report within 60 s")
        err, msg = box[0]
        if err is not None:
            raise RuntimeError(str(err))
        return msg.offset()

    def read(self, offset):
        import time
        from confluent_kafka import Consumer, TopicPartition
        if self._consumer is None:
            self._consumer = Consumer({"bootstrap.servers": self._addr, "group.id": "verify", "enable.auto.commit": False,
                                       "auto.offset.reset": "earliest"})
            self._consumer.assign([TopicPartition(TOPIC, 0, 0)])
        deadline = time.monotonic() + 60
        while offset not in self._loaded:
            for m in self._consumer.consume(500, 1.0):
                if m.error():
                    raise RuntimeError(str(m.error()))
                self._loaded[m.offset()] = m.value()
            if time.monotonic() > deadline:
                raise TimeoutError(f"offset {offset} not found within the verification deadline")
        return self._loaded.pop(offset)

    def close(self):
        self._stop.set()
        if self._consumer is not None:
            self._consumer.close()
        self._p.flush(10)


class Nats:
    """nats-py (asyncio) driven from threads: each producer thread blocks on its own JetStream publish."""

    def __init__(self, addr, token, workers):
        import nats
        from nats.js import api
        self.client_name = _version("nats-py")
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True).start()
        self._nc = self._run(nats.connect(f"nats://{addr}", token=token, allow_reconnect=False, max_reconnect_attempts=0))
        self._js = self._nc.jetstream()
        self._run(self._js.add_stream(api.StreamConfig(name="EVENTS", subjects=[TOPIC], storage=api.StorageType.FILE, num_replicas=1)))

    def _run(self, coro, timeout=90):
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def publish(self, data):
        return self._run(self._js.publish(TOPIC, data, timeout=60)).seq

    def read(self, offset):
        return self._run(self._js.get_msg("EVENTS", seq=offset)).data

    def close(self):
        self._run(self._nc.close())
        self._loop.call_soon_threadsafe(self._loop.stop)


def pack_id(stream_id) -> int:
    """A stream ID is '<milliseconds>-<sequence>'; pack it into one integer offset as the Go harness does."""
    if isinstance(stream_id, bytes):
        stream_id = stream_id.decode()
    ms, seq = stream_id.split("-")
    if int(seq) >= 1 << 16:
        raise ValueError(f"stream id {stream_id}: sequence does not fit")
    return (int(ms) << 16) | int(seq)


def unpack_id(offset: int) -> str:
    return f"{offset >> 16}-{offset & 0xFFFF}"


class Redis:
    """redis-py: XADD on a stream, appendfsync always on the server, no client retries."""

    def __init__(self, addr, token, workers):
        import redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry
        self.client_name = _version("redis")
        host, port = addr.rsplit(":", 1)
        pool = redis.ConnectionPool(host=host, port=int(port), password=token, max_connections=workers + 8,
                                    socket_timeout=60, socket_connect_timeout=10, retry=Retry(NoBackoff(), 0))
        self._r = redis.Redis(connection_pool=pool)

    def publish(self, data):
        return pack_id(self._r.xadd(TOPIC, {"d": data}))

    def read(self, offset):
        rows = self._r.xrange(TOPIC, unpack_id(offset), unpack_id(offset))
        if len(rows) != 1:
            raise LookupError(f"stream id {unpack_id(offset)}: {len(rows)} entries")
        return rows[0][1][b"d"]

    def close(self):
        self._r.close()


ENGINES = {"dostoevsky": Dostoevsky, "kafka": Kafka, "nats": Nats, "redis": Redis}

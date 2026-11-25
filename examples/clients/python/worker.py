import os
from dostoevsky_client import Client

client = Client(os.getenv("DOSTOEVSKY_URL", "http://127.0.0.1:8787"), os.environ["DOSTOEVSKY_TOKEN"])
ack = client.publish("jobs", b"task from python", key="jobs+λ", id="python-demo")
group = "python-worker"
cursor = client.position("jobs", ack.partition, group).next
while cursor <= ack.offset:
    job = client.read("jobs", ack.partition, cursor, wait_ms=1000)
    if job is None:
        raise RuntimeError("missing job")
    print(job.id, job.value)
    client.commit("jobs", ack.partition, group, cursor, job.id.encode())
    cursor += 1

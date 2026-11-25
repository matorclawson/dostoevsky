"""Integration test: seed binary data, run each client, restart, verify and replay."""
import os
import secrets
import subprocess
import sys
import tempfile
from contextlib import contextmanager

from dostoevsky_client import Client, ClientError


@contextmanager
def gateway(directory, token):
    env = dict(os.environ, DOSTOEVSKY_TOKEN=token)
    with subprocess.Popen(
        [os.environ.get("DOSTOEVSKY_TEST_GATEWAY", ".dev/bin/dostoevsky-gateway"), "-listen", "127.0.0.1:0", "-data", directory],
        stdout=subprocess.PIPE, text=True, env=env,
    ) as proc:
        try:
            line = proc.stdout.readline().strip()
            if not line.startswith("listening "):
                raise RuntimeError("gateway failed to start: " + line)
            url = "http://" + line.split()[1]
            yield Client(url, token), dict(env, DOSTOEVSKY_URL=url)
        finally:
            proc.terminate()
            proc.wait(timeout=10)


def workers(languages, env):
    for lang in languages:
        subprocess.run(["make", "dev-client-" + lang], check=True, env=env, timeout=300)


def check(client, languages):
    for index, lang in enumerate(languages, start=1):
        record = client.read("jobs", 0, index)
        assert record and record.value == ("task from " + lang).encode()
        assert record.id == lang + "-demo" and record.key == "jobs+λ"
        pos = client.position("jobs", 0, lang + "-worker")
        assert pos.next == index + 1 and pos.state == record.id.encode()
    assert client.read("jobs", 0, len(languages) + 1, wait_ms=10) is None
    sentinel = client.read("jobs", 0, 0)
    assert sentinel.value == bytes(range(256)) and sentinel.key == "key+/ λ"


def refuses_plaintext_off_loopback(token):
    """The gateway must not serve the whole-store token in plaintext beyond loopback."""
    with tempfile.TemporaryDirectory() as directory:  # removed when the check ends
        done = subprocess.run(
            [os.environ.get("DOSTOEVSKY_TEST_GATEWAY", ".dev/bin/dostoevsky-gateway"), "-listen", "0.0.0.0:0", "-data", directory],
            env=dict(os.environ, DOSTOEVSKY_TOKEN=token), capture_output=True, text=True, timeout=20,
        )
    assert done.returncode != 0 and "plaintext" in done.stderr, done


def main(languages):
    allowed = {"python", "javascript", "typescript", "java", "rust"}
    if not languages or len(set(languages)) != len(languages) or set(languages) - allowed:
        raise ValueError("select one or more distinct client languages")
    token = secrets.token_urlsafe(32)
    refuses_plaintext_off_loopback(token)
    with tempfile.TemporaryDirectory() as directory:
        with gateway(directory, token) as (client, env):
            client.publish("jobs", bytes(range(256)), key="key+/ λ", id="seed")
            workers(languages, env)
            check(client, languages)
        with gateway(directory, token) as (client, env):
            check(client, languages)
            workers(languages, env)
            check(client, languages)
            try:
                client.commit("jobs", 0, languages[0] + "-worker", 0)
            except ClientError as err:
                assert (err.status, err.code) == (409, "conflict")
            else:
                raise AssertionError("stale commit succeeded")
    print("PASS cross-language publication, replay, checkpoints and restart:", ", ".join(languages))


if __name__ == "__main__":
    main(sys.argv[1:])

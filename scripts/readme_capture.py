#!/usr/bin/env python3
"""Run the README quick start for real and draw what the terminal showed.

Run from the root of a fresh clone on Linux or macOS (Go, make and Python 3.10+ on
PATH):

    python3 scripts/readme_capture.py docs/img/quickstart.svg

It starts the gateway with `make dev-gateway`, runs the client commands of the
README, kills the gateway with SIGKILL, starts it again and repeats the publish.
Every output line in the picture is what the command printed. The script fails,
and writes nothing, if a command fails, if the duplicate publish gets a new offset,
or if the output names this machine, this user or a local path.
Standard library only.
"""

import getpass
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
from xml.sax.saxutils import escape

PORT = 8787
WIDTH = 92  # terminal columns; longer lines wrap as a terminal would

TOKEN_CMD = "export DOSTOEVSKY_TOKEN=$(go run ./cmd/dostoevsky-gateway -gen-token)"
SETUP = [
    "export PYTHONPATH=clients/python",
    "PY='import os; from dostoevsky_client import Client; "
    'c = Client("http://127.0.0.1:8787", os.environ["DOSTOEVSKY_TOKEN"])\'',
]
PUBLISH = """python3 -c "$PY; print(c.publish('jobs', b'paid', id='order-1'))\""""
READ = """python3 -c "$PY; print(c.read('jobs', 0, 1).value)\""""
WORKER = "make dev-client-python"


def listening():
    with socket.socket() as s:
        s.settimeout(0.2)
        return s.connect_ex(("127.0.0.1", PORT)) == 0


class Gateway:
    def __init__(self, env):
        self.log = tempfile.TemporaryFile("w+")
        self.proc = subprocess.Popen(
            ["make", "dev-gateway"], env=env, stdout=self.log,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        deadline = time.monotonic() + 180
        while not listening():
            if self.proc.poll() is not None or time.monotonic() > deadline:
                self.log.seek(0)
                said = self.log.read()
                self.kill()
                sys.exit("the gateway did not start:\n" + said)
            time.sleep(0.2)

    def kill(self):
        """SIGKILL the whole group: make, go run and the gateway itself."""
        try:
            os.killpg(self.proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.proc.wait()
        deadline = time.monotonic() + 30
        while listening():
            if time.monotonic() > deadline:
                sys.exit("the port is still open after SIGKILL")
            time.sleep(0.1)


def run(command, env):
    done = subprocess.run(
        ["sh", "-c", command], env=env, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, timeout=300,
    )
    if done.returncode != 0:
        sys.exit("%s: exit %d\n%s" % (command, done.returncode, done.stdout))
    return done.stdout.rstrip("\n").split("\n") if done.stdout.strip() else []


def capture():
    """Return the transcript as (kind, text) pairs: "cmd", "out" or "note"."""
    if listening():
        sys.exit("port %d is already in use: refusing to talk to another server" % PORT)
    if os.path.exists(".dev/client-data"):
        sys.exit(".dev/client-data exists: run this in a fresh clone")
    env = {k: v for k, v in os.environ.items() if not k.startswith("DOSTOEVSKY_")}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["MAKEFLAGS"] = "--no-print-directory"
    token = run("go run ./cmd/dostoevsky-gateway -gen-token", env)[-1]
    env["DOSTOEVSKY_TOKEN"] = token
    env["PYTHONPATH"] = "clients/python"
    py = SETUP[1][len("PY='"):-1]
    env["PY"] = py

    lines = [("cmd", TOKEN_CMD)] + [("cmd", c) for c in SETUP]

    def step(command):
        out = run(command, env)
        lines.append(("cmd", command))
        lines.extend(("out", line) for line in out)
        return out

    gateway = Gateway(env)
    try:
        lines.append(("note", "# `make dev-gateway &` is running in the background (started by the capture script)"))
        step(WORKER)
        first = step(PUBLISH)
        gateway.kill()
        lines.append(("note", "# the gateway is force-killed (SIGKILL), then started again"))
        gateway = Gateway(env)
        again = step(PUBLISH)
        value = step(READ)
    finally:
        gateway.kill()
    if first != again:
        sys.exit("the duplicate publish was not recognised: %r then %r" % (first, again))
    if value != ["b'paid'"]:
        sys.exit("unexpected read after restart: %r" % value)

    private = {token, os.getcwd(), os.path.realpath(os.getcwd()), os.path.expanduser("~"),
               socket.gethostname(), socket.gethostname().split(".")[0], getpass.getuser()}
    for _, text in lines:
        for word in private:
            if len(word) > 2 and word != "/root" and word != "root" and word in text:
                sys.exit("the transcript names a private value; nothing written")
        if any(p in text for p in ("/home/", "/Users/", "/Volumes/", "/private/", "/var/folders/")):
            sys.exit("the transcript holds a local path; nothing written")
    return lines


def wrap(kind, text):
    if kind == "cmd":
        text = "$ " + text
    rows = [text[:WIDTH]]
    rest = text[WIDTH:]
    while rest:
        rows.append("    " + rest[:WIDTH - 4])
        rest = rest[WIDTH - 4:]
    return [(kind, row) for row in rows]


def svg(lines):
    rows = [row for kind, text in lines for row in wrap(kind, text)]
    line_height, top, width = 20, 52, 800
    height = top + line_height * len(rows) + 14
    color = {"cmd": "#e6edf3", "out": "#7ee787", "note": "#9198a1"}
    said = " ".join(text for kind, text in lines if kind == "out")
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" '
        'role="img" aria-labelledby="t d">' % (width, height, width, height),
        '<title id="t">The Dostoevsky quick start in a terminal</title>',
        '<desc id="d">%s</desc>' % escape(
            "A job is published and read, the gateway is killed and restarted, and publishing "
            "the same ID again returns the same offset. Output: " + said),
        '<rect width="100%" height="100%" rx="8" fill="#0d1117" stroke="#3d444d"/>',
        '<circle cx="20" cy="20" r="6" fill="#ff5f57"/><circle cx="40" cy="20" r="6" fill="#febc2e"/>'
        '<circle cx="60" cy="20" r="6" fill="#28c840"/>',
        '<g font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" font-size="13">',
    ]
    for i, (kind, text) in enumerate(rows):
        # textLength keeps every row inside the frame whatever monospace font draws it.
        parts.append(
            '<text x="16" y="%d" fill="%s" xml:space="preserve" textLength="%.1f" '
            'lengthAdjust="spacingAndGlyphs">%s</text>'
            % (top + i * line_height, color[kind], len(text) * 8.0, escape(text)))
    parts.append("</g></svg>")
    return "\n".join(parts) + "\n"


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: python3 scripts/readme_capture.py OUT.svg")
    lines = capture()
    picture = svg(lines)
    with open(sys.argv[1], "w", encoding="utf-8") as f:
        f.write(picture)
    for _, row in (r for kind, text in lines for r in wrap(kind, text)):
        print(row)


if __name__ == "__main__":
    main()

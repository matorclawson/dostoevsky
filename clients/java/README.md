# dostoevsky-client (Java)

Maven coordinates `io.github.rodriveracom:dostoevsky-client` (not yet published); package `io.github.rodriveracom.dostoevsky`. No dependencies, Java 17 or newer (`java.net.http`). Thread-safe: reuse one `Client` per endpoint.

```sh
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes go run ./cmd/dostoevsky-gateway &   # from the repository root
mkdir -p /tmp/dj && javac -d /tmp/dj clients/java/src/main/java/io/github/rodriveracom/dostoevsky/Client.java examples/clients/java/Worker.java
DOSTOEVSKY_TOKEN=a-token-of-at-least-16-bytes java -cp /tmp/dj Worker
```

```java
var c = new Client("http://127.0.0.1:8787", token);
var ack = c.publish("jobs", "payload".getBytes(UTF_8), "k", "once");
var rec = c.read("jobs", ack.partition(), ack.offset(), 1000);   // Optional.empty() when nothing arrives in time
c.commit("jobs", ack.partition(), "workers", ack.offset(), new byte[0]);
```

Errors throw `Client.ClientException` with `status` and `code` (`conflict`, `expired`, `limit`, ...).

## Native protocol client (preview)

`NativeClient` speaks the binary [wire protocol](https://github.com/rodriveracom/dostoevsky/blob/main/docs/wire-v1.md) on one connection with many requests in flight. **No released Dostoevsky server speaks it yet.** Plaintext only, to a loopback address or a unix socket. One instance can be shared between threads; one I/O thread owns the socket.

```java
try (var c = NativeClient.connect("127.0.0.1", 8788, token)) {          // or NativeClient.connectUnix(path, token)
    var pending = c.publishNowait("jobs", payload, "k", "once");        // pipelined
    var ack = pending.get();
    var rec = c.read("jobs", ack.partition(), ack.offset(), 1000);
}
```

Errors are `Client.ClientException`, or `NativeClient.ConnectionLost` whose `sent` true means the request may have taken effect. Nothing is retried and there is no reconnect. A reply is accepted only within 40 s of its request.

**Reply matching.** A reply is accepted only for a request that had been written in full before the reply was read, and request ids are random, so a server is very unlikely to guess the id of a request it has not received. A server that is broken or hostile holds the token and can return any answer.

Test with only a JDK, no Maven: `clients/java/test.sh`. The `pom.xml` is for a later, separate publishing decision.

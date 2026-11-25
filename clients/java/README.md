# dostoevsky-client (Java)

Maven coordinates `io.github.rodriveracom:dostoevsky-client` (not yet published); package `io.github.rodriveracom.dostoevsky`. One source file, no dependencies, Java 17 or newer (`java.net.http`). Thread-safe: reuse one `Client` per endpoint.

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

Test with only a JDK, no Maven: `clients/java/test.sh`. The `pom.xml` is for a later, separate publishing decision.

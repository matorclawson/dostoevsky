package io.github.rodriveracom.dostoevsky;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Optional;
import java.util.StringJoiner;

/** A thread-safe, synchronous Dostoevsky client. Reuse one instance per endpoint. */
public final class Client {
    public record Receipt(int partition, long offset) {}
    public record StreamRecord(Receipt receipt, String key, String id, String time, byte[] value) {}
    public record Position(long next, byte[] state) {}
    public static final class ClientException extends IOException {
        public final int status;
        public final String code;
        ClientException(int status, String code) {
            super(status + " " + code);
            this.status = status;
            this.code = code;
        }
    }

    /** The protocol's largest value, key or checkpoint state. */
    static final int MAX_BODY = 1 << 20;

    private record Reply(int statusCode, HttpHeaders headers, byte[] body) {}

    /** Closes a response stream whose body read outlives its deadline; one daemon thread serves every client. */
    private static final ScheduledExecutorService WATCHDOG = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "dostoevsky-client-watchdog");
        t.setDaemon(true);
        return t;
    });

    /** Bound on connecting, on waiting for the reply headers, and on reading the whole body. */
    volatile Duration timeout = Duration.ofSeconds(40);

    private final String base;
    private final String token;
    private final HttpClient http = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(10)).build();

    /** The bearer token grants the whole store: only https, or http to loopback, may carry it. */
    static void checkUrl(String url) {
        URI u = URI.create(url);
        String scheme = u.getScheme() == null ? "" : u.getScheme().toLowerCase();
        String host = u.getHost() == null ? "" : u.getHost().toLowerCase();
        boolean loopback = host.equals("localhost") || host.equals("[::1]") || host.matches("127(\\.\\d{1,3}){3}");
        if (scheme.equals("https") && !host.isEmpty() || scheme.equals("http") && loopback) return;
        throw new IllegalArgumentException(
            "refusing to send the token over plain http to a non-loopback address; use https://");
    }

    public Client(String url, String token) {
        checkUrl(url);
        this.base = url.replaceAll("/+$", "") + "/dostoevsky/client/v1/";
        this.token = token;
    }

    private Reply call(String op, byte[] body, String... pairs)
            throws IOException, InterruptedException {
        var query = new StringJoiner("&");
        for (int i = 0; i < pairs.length; i += 2) {
            query.add(pairs[i] + "=" + URLEncoder.encode(pairs[i + 1], StandardCharsets.UTF_8));
        }
        var request = HttpRequest.newBuilder(URI.create(base + op + "?" + query))
            .timeout(timeout).header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/octet-stream");
        if (body == null) request.GET();
        else request.POST(HttpRequest.BodyPublishers.ofByteArray(body));
        var res = http.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
        try (var in = res.body()) {
            int code = res.statusCode();
            // The request timeout covers connecting and the reply headers only. The body read has
            // its own deadline: a watchdog closes the stream, which aborts a read blocked on a
            // server that sent headers and then went quiet.
            var timedOut = new AtomicBoolean();
            var guard = WATCHDOG.schedule(() -> {
                timedOut.set(true);
                try { in.close(); } catch (IOException ignored) { }
            }, timeout.toMillis(), TimeUnit.MILLISECONDS);
            try {
                if (code / 100 != 2) {
                    byte[] text = in.readNBytes(256);
                    throw new ClientException(code, new String(text, StandardCharsets.UTF_8).strip());
                }
                byte[] responseBytes = in.readNBytes(MAX_BODY + 1);
                if (responseBytes.length > MAX_BODY) throw new ClientException(code, "oversized_response");
                var announced = res.headers().firstValueAsLong("Content-Length");
                if (announced.isPresent() && announced.getAsLong() != responseBytes.length) {
                    throw new ClientException(code, "incomplete_response");
                }
                return new Reply(code, res.headers(), responseBytes);
            } catch (IOException e) {
                if (timedOut.get() && !(e instanceof ClientException)) {
                    throw new IOException("timed out reading the response body", e);
                }
                throw e;
            } finally {
                guard.cancel(false);
            }
        }
    }

    private static String header(Reply res, String name) throws IOException {
        return res.headers().firstValue("Dostoevsky-" + name)
            .orElseThrow(() -> new IOException("Missing response header: " + name));
    }

    private static String metadata(Reply res, String name) throws IOException {
        return URLDecoder.decode(header(res, name).replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    private static Receipt receipt(Reply res) throws IOException {
        return new Receipt(Integer.parseInt(header(res, "Partition")), Long.parseLong(header(res, "Offset")));
    }

    public Receipt publish(String topic, byte[] value, String key, String id) throws IOException, InterruptedException {
        return receipt(call("publish", value, "topic", topic, "key", key, "id", id));
    }

    public Optional<StreamRecord> read(String topic, int partition, long offset, int waitMs)
            throws IOException, InterruptedException {
        var res = call("read", null, "topic", topic, "partition", Integer.toString(partition),
            "offset", Long.toString(offset), "wait_ms", Integer.toString(waitMs));
        if (res.statusCode() == 204) return Optional.empty();
        return Optional.of(new StreamRecord(receipt(res), metadata(res, "Key"),
            metadata(res, "ID"), header(res, "Time"), res.body()));
    }

    public Position position(String topic, int partition, String group) throws IOException, InterruptedException {
        var res = call("position", null, "topic", topic, "partition", Integer.toString(partition), "group", group);
        return new Position(Long.parseLong(header(res, "Next")), res.body());
    }

    public void commit(String topic, int partition, String group, long expected, byte[] state)
            throws IOException, InterruptedException {
        call("commit", state, "topic", topic, "partition", Integer.toString(partition),
            "group", group, "expected", Long.toString(expected));
    }
}

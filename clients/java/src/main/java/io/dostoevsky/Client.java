package io.dostoevsky;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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

    private final String base;
    private final String token;
    private final HttpClient http = HttpClient.newBuilder()
        .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(10)).build();

    public Client(String url, String token) {
        this.base = url.replaceAll("/+$", "") + "/dostoevsky/client/v1/";
        this.token = token;
    }

    private HttpResponse<byte[]> call(String op, byte[] body, String... pairs)
            throws IOException, InterruptedException {
        var query = new StringJoiner("&");
        for (int i = 0; i < pairs.length; i += 2) {
            query.add(pairs[i] + "=" + URLEncoder.encode(pairs[i + 1], StandardCharsets.UTF_8));
        }
        var request = HttpRequest.newBuilder(URI.create(base + op + "?" + query))
            .timeout(Duration.ofSeconds(40)).header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/octet-stream");
        if (body == null) request.GET();
        else request.POST(HttpRequest.BodyPublishers.ofByteArray(body));
        var res = http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        if (res.statusCode() / 100 != 2) {
            throw new ClientException(res.statusCode(), new String(res.body(), StandardCharsets.UTF_8).strip());
        }
        return res;
    }

    private static String header(HttpResponse<?> res, String name) throws IOException {
        return res.headers().firstValue("Dostoevsky-" + name)
            .orElseThrow(() -> new IOException("Missing response header: " + name));
    }

    private static String metadata(HttpResponse<?> res, String name) throws IOException {
        return URLDecoder.decode(header(res, name).replace("+", "%2B"), StandardCharsets.UTF_8);
    }

    private static Receipt receipt(HttpResponse<?> res) throws IOException {
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

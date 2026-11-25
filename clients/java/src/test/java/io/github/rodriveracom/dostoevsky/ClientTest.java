package io.github.rodriveracom.dostoevsky;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** Tests against a local stub of the gateway. Run with test.sh; needs -ea. */
public final class ClientTest {
    static final String TOKEN = "a-test-token-with-entropy";
    record Seen(String method, String path, Map<String, String> q, byte[] body, String auth) {}
    static final CopyOnWriteArrayList<Seen> seen = new CopyOnWriteArrayList<>();

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static Map<String, String> query(String raw) {
        var m = new HashMap<String, String>();
        if (raw == null) return m;
        for (String kv : raw.split("&")) {
            int i = kv.indexOf('=');
            m.put(URLDecoder.decode(kv.substring(0, i), StandardCharsets.UTF_8), URLDecoder.decode(kv.substring(i + 1), StandardCharsets.UTF_8));
        }
        return m;
    }

    public static void main(String[] args) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            var q = query(ex.getRequestURI().getRawQuery());
            var body = ex.getRequestBody().readAllBytes();
            var auth = ex.getRequestHeaders().getFirst("Authorization");
            var path = ex.getRequestURI().getPath();
            seen.add(new Seen(ex.getRequestMethod(), path, q, body, auth));
            var h = ex.getResponseHeaders();
            int code = 200;
            byte[] out = new byte[0];
            String op = path.substring(path.lastIndexOf('/') + 1);
            if ("stall".equals(q.get("topic"))) {  // headers, ten bytes, then silence
                ex.sendResponseHeaders(200, 100);
                ex.getResponseBody().write(new byte[10]);
                ex.getResponseBody().flush();
                try { Thread.sleep(4000); } catch (InterruptedException ignored) { }
                ex.close();
                return;
            }
            if ("short".equals(q.get("topic"))) {  // announces 10 bytes, delivers 3, ends
                ex.sendResponseHeaders(200, 10);
                ex.getResponseBody().write(new byte[3]);
                ex.close();
                return;
            }
            if (!("Bearer " + TOKEN).equals(auth)) { code = 401; out = "unauthorized\n".getBytes(StandardCharsets.UTF_8); }
            else if (op.equals("publish")) {
                if ("clash".equals(q.get("id"))) { code = 409; out = "conflict\n".getBytes(StandardCharsets.UTF_8); }
                else { h.set("Dostoevsky-Partition", "0"); h.set("Dostoevsky-Offset", "7"); }
            } else if (op.equals("read")) {
                if (q.get("offset").equals("99")) code = 204;
                else if (q.get("offset").equals("77")) {
                    h.set("Dostoevsky-Partition", "0"); h.set("Dostoevsky-Offset", "77"); h.set("Dostoevsky-Key", "");
                    h.set("Dostoevsky-ID", ""); h.set("Dostoevsky-Time", "t");
                    out = new byte[(1 << 20) + 1];
                }
                else {
                    h.set("Dostoevsky-Partition", "0"); h.set("Dostoevsky-Offset", q.get("offset"));
                    // The server uses PathEscape: a space is %20 and a plus sign stays a plus sign.
                    h.set("Dostoevsky-Key", "+/%CE%BB%20x"); h.set("Dostoevsky-ID", "x");
                    h.set("Dostoevsky-Time", "2026-01-01T00:00:00Z");
                    out = new byte[] {0, (byte) 255, 65};
                }
            } else if (op.equals("position")) { h.set("Dostoevsky-Next", "3"); out = new byte[] {1, 2}; }
            else if (op.equals("commit")) code = 204;
            else { code = 404; out = "not found\n".getBytes(StandardCharsets.UTF_8); }
            if (code == 204) { ex.sendResponseHeaders(204, -1); }
            else { ex.sendResponseHeaders(code, out.length); ex.getResponseBody().write(out); }
            ex.close();
        });
        server.start();
        try {
            var c = new Client("http://127.0.0.1:" + server.getAddress().getPort() + "/", TOKEN);

            var ack = c.publish("jobs", new byte[] {0, (byte) 255}, "+/λ", "once");
            check(ack.partition() == 0 && ack.offset() == 7, "receipt");
            var s = seen.get(0);
            check(s.method().equals("POST") && s.path().equals("/dostoevsky/client/v1/publish"), "publish route");
            check(s.q().get("topic").equals("jobs") && s.q().get("key").equals("+/λ") && s.q().get("id").equals("once"), "escaped params " + s.q());
            check(Arrays.equals(s.body(), new byte[] {0, (byte) 255}), "binary body");
            check(s.auth().equals("Bearer " + TOKEN), "token");

            var r = c.read("jobs", 0, 5, 100).orElseThrow();
            check(r.receipt().offset() == 5 && r.id().equals("x"), "record");
            check(r.key().equals("+/λ x"), "a plus sign stays a plus sign and %20 is a space: " + r.key());
            check(Arrays.equals(r.value(), new byte[] {0, (byte) 255, 65}), "binary value");
            check(c.read("jobs", 0, 99, 0).isEmpty(), "204 is empty");

            var p = c.position("jobs", 0, "g");
            check(p.next() == 3 && Arrays.equals(p.state(), new byte[] {1, 2}), "position");
            seen.clear();
            c.commit("jobs", 0, "g", 3, new byte[] {7});
            check(seen.get(0).q().get("expected").equals("3") && Arrays.equals(seen.get(0).body(), new byte[] {7}), "commit");

            try { c.publish("jobs", new byte[0], "", "clash"); check(false, "409 not thrown"); }
            catch (Client.ClientException e) { check(e.status == 409 && e.code.equals("conflict"), "409 word"); }
            try { new Client("http://127.0.0.1:" + server.getAddress().getPort(), "wrong").position("jobs", 0, "g"); check(false, "401 not thrown"); }
            catch (Client.ClientException e) { check(e.status == 401, "401"); }
            try { c.read("jobs", 0, 77, 0); check(false, "oversized not refused"); }
            catch (Client.ClientException e) { check(e.code.equals("oversized_response"), "oversized word"); }

            for (var ok : new String[] {"http://127.0.0.1:8787", "http://localhost:1", "http://[::1]:1", "https://example.com", "https://10.0.0.5:8787"}) Client.checkUrl(ok);
            for (var bad : new String[] {"http://example.com", "http://10.0.0.5:8787", "http://0.0.0.0:1", "http://localhost.example.com", "ftp://127.0.0.1", "127.0.0.1:8787"}) {
                try { Client.checkUrl(bad); check(false, "accepted " + bad); } catch (IllegalArgumentException expected) { }
            }
            try { c.read("short", 0, 1, 0); check(false, "a short body was accepted"); }
            catch (java.io.IOException expected) { }

            // The body deadline: headers arrive, the body stalls. Without a deadline on the read this
            // would wait for the server (4 s here, 40 s by default) instead of one second.
            c.timeout = java.time.Duration.ofSeconds(1);
            long begin = System.nanoTime();
            try { c.read("stall", 0, 1, 0); check(false, "a stalled body was accepted"); }
            catch (java.io.IOException e) { check(e.getMessage() != null, "message"); }
            long ms = (System.nanoTime() - begin) / 1_000_000;
            check(ms < 3000, "the body read ignored its deadline: " + ms + " ms");
            System.out.println("ok");
        } finally {
            server.stop(0);
        }
    }
}

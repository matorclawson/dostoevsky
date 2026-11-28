package io.github.rodriveracom.dostoevsky;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * The native client against a real dostoevsky-server. Skipped unless DOSTOEVSKY_SERVER (the binary) and
 * DOSTOEVSKY_WORK (a new empty directory with a short path) are set.
 */
public final class NativeInteropTest {
    static final String TOKEN = "interop-token-0123456789abcdef";

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static byte[] text(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    record Server(Process process, String address) implements AutoCloseable {
        // A kill, not a shutdown: what was acknowledged must survive it.
        @Override
        public void close() {
            process.destroyForcibly();
            try {
                process.waitFor();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static Server serve(String binary, Path data, String flag, String where) throws Exception {
        var builder = new ProcessBuilder(binary, "-data", data.toString(), flag, where)
            .redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.environment().put("DOSTOEVSKY_TOKEN", TOKEN);
        Process process = builder.start();
        var out = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        String line = out.readLine();
        check(line != null && line.startsWith("listening "), "server did not start: " + line);
        return new Server(process, line.split(" ")[1]);
    }

    interface Connect {
        NativeClient to(String address) throws Exception;
    }

    static void failsWith(String code, NativeClientTest.Call call) {
        NativeClientTest.code(call, code);
    }

    static void checks(NativeClient client, String topic) throws Exception {
        client.ping();
        var first = client.publish(topic, text("one"), "k", "a");
        check(first.partition() == 0 && first.offset() == 0, "first");
        check(client.publish(topic, text("one"), "k", "a").offset() == 0, "dedup");
        failsWith("conflict", () -> client.publish(topic, text("other"), "k", "a"));

        var record = client.read(topic, 0, 0, 0).orElseThrow();
        check(record.key().equals("k") && record.id().equals("a"), "metadata");
        check(Arrays.equals(record.value(), text("one")), "value");
        check(record.time().endsWith("Z") && record.time().charAt(10) == 'T', record.time());
        check(client.read(topic, 0, 1, 0).isEmpty(), "empty read");
        check(client.read(topic, 0, 1, 50).isEmpty(), "timed out read");
        failsWith("invalid", () -> client.read(topic, 7, 0, 0));

        // A waiting read does not hold up a publish on the same client.
        var waited = new byte[1][];
        var reader = new Thread(() -> {
            try {
                waited[0] = client.read(topic, 0, 1, 20_000).orElseThrow().value();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        reader.start();
        Thread.sleep(500);
        client.publish(topic, text("two"), "", "");
        reader.join(15_000);
        check(Arrays.equals(waited[0], text("two")), "long poll");

        check(client.position(topic, 0, "g").next() == 0, "initial position");
        client.commit(topic, 0, "g", 0, text("state"));
        failsWith("conflict", () -> client.commit(topic, 0, "g", 0, new byte[0]));
        var position = client.position(topic, 0, "g");
        check(position.next() == 1 && Arrays.equals(position.state(), text("state")), "position");

        var pending = new ArrayList<NativeClient.PendingPublish>();
        for (int i = 0; i < 1000; i++) pending.add(client.publishNowait(topic + "-p", text("" + i), "", ""));
        for (int i = 0; i < 1000; i++) check(pending.get(i).get().offset() == i, "order at " + i);

        var value = new byte[1 << 20];
        Arrays.fill(value, (byte) 7);
        var big = new ArrayList<NativeClient.PendingPublish>();
        for (int i = 0; i < 3; i++) big.add(client.publishNowait(topic + "-big", value, "", ""));
        for (int i = 0; i < 3; i++) check(big.get(i).get().offset() == i, "large order");
        for (int i = 0; i < 3; i++) {
            check(Arrays.equals(client.read(topic + "-big", 0, i, 0).orElseThrow().value(), value), "large value");
        }

        var offsets = new ConcurrentSkipListSet<Long>();
        var threads = new ArrayList<Thread>();
        for (int t = 0; t < 8; t++) {
            var thread = new Thread(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        offsets.add(client.publish(topic + "-threads", text("x"), "", "").offset());
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (var thread : threads) thread.join(60_000);
        check(offsets.size() == 800 && offsets.first() == 0 && offsets.last() == 799, "shared client");
    }

    static void run(String binary, Path data, String flag, String where, String topic, Connect connect)
            throws Exception {
        NativeClient old;
        try (var server = serve(binary, data, flag, where)) {
            old = connect.to(server.address());
            checks(old, topic);
        }
        var lost = NativeClientTest.thrown(old::ping);
        check(lost instanceof NativeClient.ConnectionLost, "after the kill: " + lost);
        old.close();
        try (var server = serve(binary, data, flag, where); var client = connect.to(server.address())) {
            check(Arrays.equals(client.read(topic, 0, 0, 0).orElseThrow().value(), text("one")), "record");
            check(client.position(topic, 0, "g").next() == 1, "position");
            check(client.publish(topic, text("one"), "k", "a").offset() == 0, "dedup");
        }
    }

    public static void main(String[] args) throws Exception {
        String binary = System.getenv("DOSTOEVSKY_SERVER"), work = System.getenv("DOSTOEVSKY_WORK");
        if (binary == null || work == null) {
            System.out.println("interop skipped: set DOSTOEVSKY_SERVER and DOSTOEVSKY_WORK");
            return;
        }
        Connect tcp = address -> {
            int port = Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
            var wrong = NativeClientTest.thrown(
                () -> NativeClient.connect("127.0.0.1", port, "wrong-token-wrong-token"));
            check(wrong instanceof Client.ClientException e && e.code.equals("unauthorized"), "wrong token: " + wrong);
            return NativeClient.connect("127.0.0.1", port, TOKEN);
        };
        run(binary, Path.of(work, "tcp-data"), "-listen", "127.0.0.1:0", "t-tcp", tcp);
        System.out.println("passed over tcp");
        Path socket = Path.of(work, "run", "s");
        run(binary, Path.of(work, "unix-data"), "-unix", socket.toString(), "t-unix",
            address -> NativeClient.connectUnix(socket, TOKEN));
        System.out.println("passed over a unix socket");
    }
}

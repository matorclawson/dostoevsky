package io.github.rodriveracom.dostoevsky;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;

/** Tests against the spec vectors and a scripted server. Run with test.sh; needs -ea. */
public final class NativeClientTest {
    static final String TOKEN = "example-token-not-a-secret";
    static final String WELCOME = "4453545600010000201000000100004000";
    static final String WINDOW_ONE = "4453545600010000201000010000004000";

    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
    }

    static byte[] hex(String text) {
        return HexFormat.of().parseHex(text);
    }

    static byte[] bytes(ByteBuffer b, int id) {
        byte[] out = Arrays.copyOf(b.array(), b.limit());
        ByteBuffer.wrap(out).order(ByteOrder.LITTLE_ENDIAN).putInt(6, id);
        return out;
    }

    interface Call {
        void run() throws Exception;
    }

    static Exception thrown(Call call) {
        try {
            call.run();
        } catch (Exception e) {
            return e;
        }
        throw new AssertionError("no exception");
    }

    static void code(Call call, String code) {
        Exception e = thrown(call);
        check(e instanceof Client.ClientException c && c.code.equals(code), "wanted " + code + ", got " + e);
    }

    static void lost(Call call, boolean sent) {
        Exception e = thrown(call);
        check(e instanceof NativeClient.ConnectionLost c && c.sent == sent, "wanted lost sent=" + sent + ", got " + e);
    }

    // The byte strings are from wire/testdata/vectors.json.
    static void vectors() throws Exception {
        check(Arrays.equals(NativeClient.hello(TOKEN),
            hex("44535456010000001a006578616d706c652d746f6b656e2d6e6f742d612d736563726574")), "hello");
        check(Arrays.equals(bytes(NativeClient.publishFrame("events", "hi".getBytes(StandardCharsets.UTF_8), "", ""), 1),
            hex("0d000000010001000000066576656e7473000000006869")), "publish");
        check(Arrays.equals(bytes(NativeClient.publishFrame("jobs", new byte[0], "ké", "once"), 0x01020304),
            hex("10000000010004030201046a6f627303006bc3a904006f6e6365")), "publish with key and id");
        check(Arrays.equals(bytes(NativeClient.readFrame("events", 0, 41, 1000), 2),
            hex("13000000020002000000066576656e747300002900000000000000e803")), "read");
        check(Arrays.equals(bytes(NativeClient.positionFrame("events", 0, "workers"), 4),
            hex("11000000030004000000066576656e7473000007776f726b657273")), "position");
        check(Arrays.equals(bytes(NativeClient.commitFrame("events", 0, "workers", 42,
            "{\"n\":2}".getBytes(StandardCharsets.UTF_8)), 5),
            hex("20000000040005000000066576656e7473000007776f726b6572732a000000000000007b226e223a327d")), "commit");
        check(Arrays.equals(bytes(NativeClient.pingFrame(), 6), hex("00000000050006000000")), "ping");

        var receipt = (Client.Receipt) NativeClient.reply(0x81, ByteBuffer.wrap(hex("03000100000000002000")));
        check(receipt.partition() == 3 && receipt.offset() == 9007199254740993L, "publish ok");
        var found = (Optional<?>) NativeClient.reply(0x82,
            ByteBuffer.wrap(hex("0100002900000000000000158dc824e15e7b1801006b000000ff0a0d")));
        var record = (Client.StreamRecord) found.orElseThrow();
        check(record.receipt().offset() == 41 && record.key().equals("k") && record.id().isEmpty(), "record");
        check(Arrays.equals(record.value(), hex("00ff0a0d")), "value");
        check(record.time().equals("2025-11-25T22:00:00.123456789Z"), record.time());
        check(((Optional<?>) NativeClient.reply(0x82, ByteBuffer.wrap(new byte[1]))).isEmpty(), "end");
        for (var bad : new Object[][] {{0x82, "02"}, {0x81, "0000"}, {0x85, "00"}, {0xFF, "0d"}, {0x42, ""}}) {
            thrown(() -> NativeClient.reply((int) bad[0], ByteBuffer.wrap(hex((String) bad[1]))));
        }
        check(NativeClient.rfc3339(0).equals("1970-01-01T00:00:00Z"), "epoch");
        check(NativeClient.rfc3339(-1).equals("1969-12-31T23:59:59.999999999Z"), "before the epoch");
        check(NativeClient.rfc3339(951_827_696_500_000_000L).equals("2000-02-29T12:34:56.5Z"), "leap day");

        int[] limits = NativeClient.welcome(hex(WELCOME));
        check(limits[0] == 256 && limits[1] == 4 << 20, "welcome");
        code(() -> NativeClient.welcome(hex("4453545602000000000000000000000000")), "unauthorized");
        for (String bad : new String[] {"4453545602010000000000000000000000", "445354560d000000000000000000000000",
                "4453545600010000201000000000004000", "4453545600020000201000000100004000"}) {
            code(() -> NativeClient.welcome(hex(bad)), "protocol");
        }
        code(() -> NativeClient.publishFrame("t", new byte[(1 << 20) + 1], "", ""), "limit");
        code(() -> NativeClient.publishFrame("t", new byte[0], "", "i".repeat(257)), "limit");
        code(() -> NativeClient.publishFrame("t", new byte[0], "\ud800", ""), "invalid");
        code(() -> NativeClient.readFrame("t", 0, -1, 0), "invalid");
        code(() -> NativeClient.readFrame("t", 0, 0, 30001), "invalid");
        code(() -> NativeClient.commitFrame("t", 70000, "g", 0, new byte[0]), "invalid");
    }

    interface Script {
        void run(DataInputStream in, OutputStream out, Socket socket) throws Exception;
    }

    record Frame(int type, int id) {}

    /** A server for one connection: reads HELLO, answers {@code welcome}, then runs the script. */
    static final class Fake {
        final ServerSocket listener;
        final Thread thread;
        volatile Throwable failure;

        Fake(String welcome, Script script) throws IOException {
            listener = new ServerSocket();
            listener.setReceiveBufferSize(4096); // so a large request cannot be absorbed unread
            listener.bind(new java.net.InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
            thread = new Thread(() -> {
                try (Socket socket = listener.accept()) {
                    socket.setSoTimeout(10_000);
                    var in = new DataInputStream(socket.getInputStream());
                    in.readFully(new byte[10 + TOKEN.length()]);
                    socket.getOutputStream().write(hex(welcome));
                    script.run(in, socket.getOutputStream(), socket);
                } catch (Throwable t) {
                    failure = t;
                }
            });
            thread.start();
        }

        NativeClient client() throws IOException {
            return NativeClient.connect("127.0.0.1", listener.getLocalPort(), TOKEN, 4096);
        }

        void done() throws Exception {
            thread.join(15_000);
            listener.close();
            check(!thread.isAlive(), "the script did not finish");
            if (failure != null) throw new AssertionError("script failed", failure);
        }
    }

    static Frame header(DataInputStream in) throws IOException {
        byte[] head = new byte[10];
        in.readFully(head);
        var b = ByteBuffer.wrap(head).order(ByteOrder.LITTLE_ENDIAN);
        return new Frame(head[4], b.getInt(6));
    }

    static Frame next(DataInputStream in) throws IOException {
        byte[] head = new byte[10];
        in.readFully(head);
        var b = ByteBuffer.wrap(head).order(ByteOrder.LITTLE_ENDIAN);
        in.readFully(new byte[b.getInt(0)]);
        return new Frame(head[4], b.getInt(6));
    }

    static byte[] published(int id, long offset) {
        var b = ByteBuffer.allocate(20).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(10).put((byte) 0x81).put((byte) 0).putInt(id).putShort((short) 0).putLong(offset);
        return b.array();
    }

    /** Reads until the client ends the connection; a read that times out fails the script. */
    static void drain(DataInputStream in) throws IOException {
        byte[] sink = new byte[CHUNK];
        try {
            while (in.read(sink) >= 0) { }
        } catch (java.net.SocketException reset) {
            // also an end of the connection
        }
    }

    static final int CHUNK = 65536;
    static final byte[] A = {'a'};

    static void repliesInAnyOrderAndSplitAnywhere() throws Exception {
        var fake = new Fake(WELCOME, (in, out, socket) -> {
            Frame a = next(in), b = next(in);
            var both = new ArrayList<Byte>();
            for (byte x : published(b.id(), 1)) both.add(x);
            for (byte x : published(a.id(), 0)) both.add(x);
            for (byte x : both) {
                out.write(x);
                out.flush();
            }
            drain(in);
        });
        try (var client = fake.client()) {
            var a = client.publishNowait("t", A, "", "");
            var b = client.publishNowait("t", A, "", "");
            check(a.get().offset() == 0 && b.get().offset() == 1, "order");
        }
        fake.done();
    }

    static void aReplyForAnIdNotYetSentEndsTheConnection() throws Exception {
        var fake = new Fake(WELCOME, (in, out, socket) -> {
            out.write(published(1, 0));
            drain(in); // the client ends it without sending anything
        });
        try (var client = fake.client()) {
            fake.done();
            lost(client::ping, false);
        }
    }

    static void aReplyToARequestNotYetWrittenInFullIsRefused() throws Exception {
        var fake = new Fake(WELCOME, (in, out, socket) -> {
            Frame first = header(in); // the body is left unread, so the client cannot finish writing it
            out.write(published(first.id(), 0));
            Thread.sleep(500);
            drain(in);
        });
        try (var client = fake.client()) {
            var pending = client.publishNowait("t", new byte[1 << 20], "", "");
            lost(pending::get, true);
        }
        fake.done();
    }

    static void aBadReplyEndsTheConnection() throws Exception {
        for (String wrong : new String[] {"id", "type", "connection error"}) {
            var fake = new Fake(WELCOME, (in, out, socket) -> {
                Frame f = next(in);
                switch (wrong) {
                    case "id" -> out.write(published(f.id() + 1, 0));
                    case "type" -> out.write(bytes(NativeClient.pingFrame(), f.id()));
                    default -> out.write(hex("01000000ff00000000000b"));
                }
                drain(in);
            });
            try (var client = fake.client()) {
                lost(() -> client.publish("t", A, "", ""), true);
                lost(client::ping, false);
            }
            fake.done();
        }
    }

    static void noMoreThanTheWindowIsUnanswered() throws Exception {
        var fake = new Fake(WINDOW_ONE, (in, out, socket) -> {
            Frame first = next(in);
            socket.setSoTimeout(300);
            try {
                in.read();
                throw new AssertionError("a second request was sent");
            } catch (java.net.SocketTimeoutException expected) {
                socket.setSoTimeout(10_000);
            }
            out.write(published(first.id(), 0));
            Frame second = next(in);
            out.write(published(second.id(), 1));
            drain(in);
        });
        try (var client = fake.client()) {
            var a = client.publishNowait("t", A, "", "");
            var b = client.publishNowait("t", A, "", "");
            check(a.get().offset() == 0 && b.get().offset() == 1, "window");
        }
        fake.done();
    }

    static void aRequestNobodyWaitsForStillExpires() throws Exception {
        var fake = new Fake(WELCOME, (in, out, socket) -> drain(in));
        try (var client = fake.client()) {
            client.requestNanos = 200_000_000L;
            client.publishNowait("t", A, "", "");
            fake.done(); // the client, still open, has ended the connection
            lost(client::ping, false);
        }
    }

    static void aRefusalAndOtherHosts() throws Exception {
        var fake = new Fake("4453545602000000000000000000000000", (in, out, socket) -> drain(in));
        code(fake::client, "unauthorized");
        fake.done();
        check(thrown(() -> NativeClient.connect("192.0.2.1", 8788, TOKEN)) instanceof IllegalArgumentException,
            "a non-loopback address was tried");
    }

    public static void main(String[] args) throws Exception {
        vectors();
        repliesInAnyOrderAndSplitAnywhere();
        aReplyForAnIdNotYetSentEndsTheConnection();
        aReplyToARequestNotYetWrittenInFullIsRefused();
        aBadReplyEndsTheConnection();
        noMoreThanTheWindowIsUnanswered();
        aRequestNobodyWaitsForStillExpires();
        aRefusalAndOtherHosts();
        System.out.println("native client tests passed");
    }
}

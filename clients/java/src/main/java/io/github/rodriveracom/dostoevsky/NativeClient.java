package io.github.rodriveracom.dostoevsky;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Native client for the Dostoevsky wire protocol, version 1 (docs/wire-v1.md): one connection, many requests at once.
 *
 * <p>PREVIEW: no released Dostoevsky server speaks this protocol yet.
 *
 * <p>Plaintext only, to a loopback address or a unix socket. Safe to share between threads: one I/O thread owns the
 * socket. Nothing is retried and there is no reconnect: after the connection is lost every call fails, and a request
 * that may have reached the server has an unknown outcome ({@link ConnectionLost#sent}).
 *
 * <p>A reply is accepted only for a request that had been written in full before the reply was read. Request ids are
 * random, so a server is very unlikely to guess the id of a request it has not received. A server that has received
 * only part of a request can still answer it early; the answer is refused if it is read before the rest is written,
 * and believed if it is read afterwards. Only a broken or hostile server does this, and it holds the token and can
 * return any answer anyway.
 *
 * <p>The ceilings of 256 requests and 4 MiB cover requests admitted to the connection. Each caller waiting for room
 * also holds the one frame it is waiting to send.
 */
public final class NativeClient implements AutoCloseable {
    /** The connection ended. {@code sent} true: the request may have taken effect. */
    public static final class ConnectionLost extends IOException {
        public final boolean sent;

        ConnectionLost(boolean sent) {
            super(sent ? "connection lost; outcome unknown" : "connection lost; not sent");
            this.sent = sent;
        }
    }

    /** A publish handed to the connection. */
    public static final class PendingPublish {
        private final CompletableFuture<Object> answer;

        private PendingPublish(CompletableFuture<Object> answer) {
            this.answer = answer;
        }

        public Client.Receipt get() throws IOException, InterruptedException {
            return (Client.Receipt) await(answer);
        }
    }

    static final int MAX_BODY = 1_056_768;
    private static final int MAX_VALUE = 1 << 20;
    private static final int MAX_KEY = 4096;
    private static final int MAX_ID = 256;
    private static final int MAX_TOKEN = 4096;
    private static final int MAX_WAIT_MS = 30_000;
    static final int HEADER = 10;
    private static final int WELCOME_BYTES = 17;

    static final int PUBLISH = 0x01;
    static final int READ = 0x02;
    static final int POSITION = 0x03;
    static final int COMMIT = 0x04;
    static final int PING = 0x05;
    static final int ERROR = 0xFF;
    static final int REPLY = 0x80;

    // The client's own ceilings. The server's window can only lower what is in flight.
    private static final int MAX_IN_FLIGHT = 256;
    private static final int MAX_IN_FLIGHT_BYTES = 4 << 20;
    private static final long HANDSHAKE_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final long REQUEST_NANOS = TimeUnit.SECONDS.toNanos(40);
    private static final int CHUNK = 65536;
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final DateTimeFormatter SECONDS = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss");
    private static final Object NOTHING = new Object();

    private static final class Request {
        final int type;
        final int size;
        final long end; // bytes queued on this connection up to and including this request
        final long deadline;
        final CompletableFuture<Object> answer = new CompletableFuture<>();

        Request(int type, int size, long end, long deadline) {
            this.type = type;
            this.size = size;
            this.end = end;
            this.deadline = deadline;
        }
    }

    private static final class Malformed extends Exception {
        Malformed(String what) {
            super(what);
        }
    }

    private final SocketChannel channel;
    private final Selector selector;
    private final SelectionKey key;
    private final int window;
    private final int windowBytes;
    private final Thread io;

    // Guarded by lock. Insertion order of `pending` is deadline order.
    private final Object lock = new Object();
    private final LinkedHashMap<Integer, Request> pending = new LinkedHashMap<>();
    private final ArrayDeque<ByteBuffer> out = new ArrayDeque<>();
    private int unanswered;
    private long queued;
    private boolean closing;
    private boolean dead;

    // Owned by the I/O thread.
    private long written;
    private long carried; // `written` when the first byte of the frame being received was read
    private final ByteBuffer in = ByteBuffer.allocate(CHUNK);
    private final ByteBuffer head = ByteBuffer.allocate(HEADER).order(ByteOrder.LITTLE_ENDIAN);
    private ByteBuffer body;

    /** A test shortens it. */
    volatile long requestNanos = REQUEST_NANOS;

    /** Connects over TCP. Only loopback addresses: the token travels in clear text. */
    public static NativeClient connect(String host, int port, String token) throws IOException {
        return connect(host, port, token, 0);
    }

    static NativeClient connect(String host, int port, String token, int sendBuffer) throws IOException {
        byte[] hello = hello(token);
        InetAddress[] addresses = InetAddress.getAllByName(host); // name resolution is not bounded
        for (InetAddress address : addresses) {
            if (!address.isLoopbackAddress()) {
                throw new IllegalArgumentException(
                    "refusing to send the token: this client speaks plaintext to loopback only");
            }
        }
        long deadline = System.nanoTime() + HANDSHAKE_NANOS;
        IOException last = new ConnectionLost(false);
        for (InetAddress address : addresses) {
            SocketChannel channel = SocketChannel.open();
            NativeClient client = null;
            try {
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                if (sendBuffer > 0) channel.setOption(StandardSocketOptions.SO_SNDBUF, sendBuffer);
                client = start(channel, new InetSocketAddress(address, port), hello, deadline);
                return client;
            } catch (Client.ClientException e) {
                throw e;
            } catch (IOException e) {
                last = new ConnectionLost(false);
            } finally {
                if (client == null) channel.close();
            }
        }
        throw last;
    }

    /** Connects to a unix socket. */
    public static NativeClient connectUnix(Path path, String token) throws IOException {
        byte[] hello = hello(token);
        SocketAddress address = UnixDomainSocketAddress.of(path); // a bad path fails before anything is opened
        SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX);
        NativeClient client = null;
        try {
            client = start(channel, address, hello, System.nanoTime() + HANDSHAKE_NANOS);
            return client;
        } catch (Client.ClientException e) {
            throw e;
        } catch (IOException e) {
            throw new ConnectionLost(false);
        } finally {
            if (client == null) channel.close();
        }
    }

    // Connect, HELLO and WELCOME within one absolute deadline.
    private static NativeClient start(SocketChannel channel, SocketAddress target, byte[] hello, long deadline)
            throws IOException {
        channel.configureBlocking(false);
        Selector selector = Selector.open();
        boolean started = false;
        try {
            SelectionKey key = channel.register(selector, 0);
            if (!channel.connect(target)) {
                do {
                    await(selector, key, SelectionKey.OP_CONNECT, deadline);
                } while (!channel.finishConnect());
            }
            ByteBuffer first = ByteBuffer.wrap(hello);
            while (first.hasRemaining()) {
                if (channel.write(first) == 0) await(selector, key, SelectionKey.OP_WRITE, deadline);
            }
            ByteBuffer welcome = ByteBuffer.allocate(WELCOME_BYTES).order(ByteOrder.LITTLE_ENDIAN);
            while (welcome.hasRemaining()) {
                int n = channel.read(welcome);
                if (n < 0) throw new IOException("closed during the handshake");
                if (n == 0) await(selector, key, SelectionKey.OP_READ, deadline);
            }
            if (deadline - System.nanoTime() <= 0) throw new IOException("the handshake took too long");
            int[] limits = welcome(welcome.array());
            NativeClient client = new NativeClient(channel, selector, key, limits[0], limits[1]);
            client.io.start();
            started = true;
            return client;
        } finally {
            if (!started) selector.close();
        }
    }

    private static void await(Selector selector, SelectionKey key, int op, long deadline) throws IOException {
        long left = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (left <= 0) throw new IOException("the handshake took too long");
        key.interestOps(op);
        selector.select(left);
        selector.selectedKeys().clear();
    }

    private NativeClient(SocketChannel channel, Selector selector, SelectionKey key, int window, int windowBytes) {
        this.channel = channel;
        this.selector = selector;
        this.key = key;
        this.window = window;
        this.windowBytes = windowBytes;
        this.io = new Thread(this::run, "dostoevsky-native");
        this.io.setDaemon(true);
    }

    // -- the I/O thread

    private void run() {
        try {
            while (true) {
                long wait = 0; // until woken
                int ops = SelectionKey.OP_READ;
                synchronized (lock) {
                    if (closing) break;
                    if (!pending.isEmpty()) {
                        long left = pending.values().iterator().next().deadline - System.nanoTime();
                        if (left <= 0) break; // the connection owns every deadline
                        wait = Math.max(1, TimeUnit.NANOSECONDS.toMillis(left));
                    }
                    if (!out.isEmpty()) ops |= SelectionKey.OP_WRITE;
                }
                key.interestOps(ops);
                selector.select(wait);
                selector.selectedKeys().clear();
                // Reads come first: what is read now is judged against what had been written before.
                if (!receive()) break;
                send();
            }
        } catch (IOException | Malformed e) {
            // The error text is not passed on.
        } finally {
            fail();
        }
    }

    private boolean receive() throws IOException, Malformed {
        for (int reads = 0; reads < 16; reads++) {
            in.clear();
            int n = channel.read(in);
            if (n < 0) return false;
            if (n == 0) return true;
            in.flip();
            while (in.hasRemaining()) {
                if (body == null) {
                    if (head.position() == 0) carried = written;
                    move(in, head);
                    if (head.hasRemaining()) break;
                    int length = head.getInt(0);
                    // The length is checked from the header, before the body is allocated.
                    if (length < 0 || length > MAX_BODY || head.get(5) != 0) throw new Malformed("bad header");
                    body = ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN);
                }
                move(in, body);
                if (body.hasRemaining()) break;
                body.flip();
                if (!answer(head.get(4) & 0xFF, head.getInt(6), body, carried)) return false;
                body = null;
                head.clear();
            }
        }
        return true;
    }

    private static void move(ByteBuffer from, ByteBuffer to) {
        int n = Math.min(from.remaining(), to.remaining());
        to.put(from.array(), from.position(), n);
        from.position(from.position() + n);
    }

    /** Delivers one server frame. False ends the connection. */
    private boolean answer(int type, int id, ByteBuffer data, long arrived) throws Malformed {
        if (id == 0) return false; // a connection error: the server closes after it
        Object result = reply(type, data);
        Request request;
        synchronized (lock) {
            request = pending.get(id);
            if (request == null || request.end > arrived || type != ERROR && type != (REPLY | request.type)
                    || request.deadline - System.nanoTime() <= 0) {
                return false;
            }
            pending.remove(id);
            unanswered -= request.size;
            lock.notifyAll();
        }
        if (result instanceof Client.ClientException error) request.answer.completeExceptionally(error);
        else request.answer.complete(result);
        return true;
    }

    private void send() throws IOException {
        while (true) {
            ByteBuffer next;
            synchronized (lock) {
                next = out.peek();
            }
            if (next == null) return;
            written += channel.write(next);
            if (next.hasRemaining()) return;
            synchronized (lock) {
                out.poll();
            }
        }
    }

    private void fail() {
        Request[] lost;
        synchronized (lock) {
            dead = true;
            lost = pending.values().toArray(new Request[0]);
            pending.clear();
            out.clear();
            unanswered = 0;
            lock.notifyAll();
        }
        try {
            channel.close();
        } catch (IOException ignored) {
        }
        try {
            selector.close();
        } catch (IOException ignored) {
        }
        for (Request request : lost) {
            // Sent if any of its bytes were written.
            request.answer.completeExceptionally(new ConnectionLost(request.end - request.size < written));
        }
    }

    // -- calls

    /** Queues one frame, first waiting for room in the server's window. */
    private CompletableFuture<Object> submit(ByteBuffer frame) throws IOException, InterruptedException {
        int type = frame.get(4) & 0xFF;
        int size = frame.remaining();
        Request request;
        synchronized (lock) {
            while (!dead && !closing && (pending.size() >= window || unanswered + size > windowBytes)) lock.wait();
            if (dead || closing) throw new ConnectionLost(false);
            int id;
            do {
                id = RANDOM.nextInt();
            } while (id == 0 || pending.containsKey(id));
            frame.putInt(6, id);
            queued += size;
            request = new Request(type, size, queued, System.nanoTime() + requestNanos);
            pending.put(id, request);
            unanswered += size;
            out.add(frame);
        }
        selector.wakeup();
        return request.answer;
    }

    private static Object await(CompletableFuture<Object> answer) throws IOException, InterruptedException {
        try {
            return answer.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException cause) throw cause;
            throw new IOException(e.getCause());
        }
    }

    /**
     * Hands a publish to the connection without waiting for its receipt (it waits only for room in the server's
     * window). Publishes of one thread are appended in call order.
     */
    public PendingPublish publishNowait(String topic, byte[] value, String key, String id)
            throws IOException, InterruptedException {
        return new PendingPublish(submit(publishFrame(topic, value, key, id)));
    }

    /** Appends one record. An empty {@code key} or {@code id} means none. */
    public Client.Receipt publish(String topic, byte[] value, String key, String id)
            throws IOException, InterruptedException {
        return publishNowait(topic, value, key, id).get();
    }

    /** One record, or empty if the end was reached, after waiting at most {@code waitMs} (30000 or less). */
    @SuppressWarnings("unchecked")
    public Optional<Client.StreamRecord> read(String topic, int partition, long offset, int waitMs)
            throws IOException, InterruptedException {
        return (Optional<Client.StreamRecord>) await(submit(readFrame(topic, partition, offset, waitMs)));
    }

    public Client.Position position(String topic, int partition, String group)
            throws IOException, InterruptedException {
        return (Client.Position) await(submit(positionFrame(topic, partition, group)));
    }

    /** Moves the group's cursor from {@code expected} to {@code expected + 1} and stores {@code state} with it. */
    public void commit(String topic, int partition, String group, long expected, byte[] state)
            throws IOException, InterruptedException {
        await(submit(commitFrame(topic, partition, group, expected, state)));
    }

    public void ping() throws IOException, InterruptedException {
        await(submit(pingFrame()));
    }

    /** Ends the connection. Unanswered requests fail. */
    @Override
    public void close() {
        synchronized (lock) {
            closing = true;
            lock.notifyAll();
        }
        if (!dead()) selector.wakeup();
        boolean interrupted = false;
        while (true) {
            try {
                io.join();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) Thread.currentThread().interrupt();
    }

    private boolean dead() {
        synchronized (lock) {
            return dead;
        }
    }

    // -- encoding

    private static Client.ClientException invalid() {
        return new Client.ClientException(400, "invalid");
    }

    private static Client.ClientException limit() {
        return new Client.ClientException(413, "limit");
    }

    private static byte[] utf8(String text, int max) throws Client.ClientException {
        if (text == null) throw invalid();
        ByteBuffer encoded;
        try {
            encoded = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(text)); // a lone surrogate fails
        } catch (CharacterCodingException e) {
            throw invalid();
        }
        if (encoded.remaining() > max) throw limit();
        byte[] data = new byte[encoded.remaining()];
        encoded.get(data);
        return data;
    }

    static byte[] hello(String token) throws Client.ClientException {
        byte[] data = utf8(token, MAX_TOKEN);
        ByteBuffer b = ByteBuffer.allocate(10 + data.length).order(ByteOrder.LITTLE_ENDIAN);
        b.put(new byte[] {'D', 'S', 'T', 'V'}).putShort((short) 1).putShort((short) 0).putShort((short) data.length);
        b.put(data);
        return b.array();
    }

    /** A frame with request id 0, ready to be written from its start. */
    private static ByteBuffer frame(int type, int bodySize) throws Client.ClientException {
        if (bodySize > MAX_BODY) throw limit();
        ByteBuffer b = ByteBuffer.allocate(HEADER + bodySize).order(ByteOrder.LITTLE_ENDIAN);
        b.putInt(bodySize).put((byte) type).put((byte) 0).putInt(0);
        return b;
    }

    private static int partition(int partition) throws Client.ClientException {
        if (partition < 0 || partition > 0xFFFF) throw invalid();
        return partition;
    }

    private static long offset(long offset) throws Client.ClientException {
        if (offset < 0) throw invalid();
        return offset;
    }

    static ByteBuffer publishFrame(String topic, byte[] value, String key, String id) throws Client.ClientException {
        byte[] t = utf8(topic, 255), k = utf8(key, MAX_KEY), i = utf8(id, MAX_ID);
        if (value == null) throw invalid();
        if (value.length > MAX_VALUE) throw limit();
        ByteBuffer b = frame(PUBLISH, 5 + t.length + k.length + i.length + value.length);
        b.put((byte) t.length).put(t).putShort((short) k.length).put(k).putShort((short) i.length).put(i).put(value);
        return b.flip();
    }

    static ByteBuffer readFrame(String topic, int partition, long offset, int waitMs) throws Client.ClientException {
        byte[] t = utf8(topic, 255);
        if (waitMs < 0 || waitMs > MAX_WAIT_MS) throw invalid();
        ByteBuffer b = frame(READ, 13 + t.length);
        b.put((byte) t.length).put(t).putShort((short) partition(partition)).putLong(offset(offset))
            .putShort((short) waitMs);
        return b.flip();
    }

    static ByteBuffer positionFrame(String topic, int partition, String group) throws Client.ClientException {
        byte[] t = utf8(topic, 255), g = utf8(group, 255);
        ByteBuffer b = frame(POSITION, 4 + t.length + g.length);
        b.put((byte) t.length).put(t).putShort((short) partition(partition)).put((byte) g.length).put(g);
        return b.flip();
    }

    static ByteBuffer commitFrame(String topic, int partition, String group, long expected, byte[] state)
            throws Client.ClientException {
        byte[] t = utf8(topic, 255), g = utf8(group, 255);
        if (state == null) throw invalid();
        if (state.length > MAX_VALUE) throw limit();
        ByteBuffer b = frame(COMMIT, 12 + t.length + g.length + state.length);
        b.put((byte) t.length).put(t).putShort((short) partition(partition)).put((byte) g.length).put(g)
            .putLong(offset(expected)).put(state);
        return b.flip();
    }

    static ByteBuffer pingFrame() throws Client.ClientException {
        return frame(PING, 0).flip();
    }

    // -- decoding

    private static Client.ClientException failure(int code) {
        return switch (code) {
            case 1 -> new Client.ClientException(400, "invalid");
            case 2 -> new Client.ClientException(401, "unauthorized");
            case 3 -> new Client.ClientException(409, "conflict");
            case 4 -> new Client.ClientException(409, "wrong_owner");
            case 5 -> new Client.ClientException(410, "expired");
            case 6 -> new Client.ClientException(413, "limit");
            case 7 -> new Client.ClientException(503, "closed");
            case 8 -> new Client.ClientException(504, "timeout");
            case 9 -> new Client.ClientException(408, "canceled");
            case 10 -> new Client.ClientException(500, "storage_failure");
            case 11 -> new Client.ClientException(400, "protocol");
            case 12 -> new Client.ClientException(400, "unsupported_version");
            default -> null;
        };
    }

    private static Client.ClientException protocol() {
        return new Client.ClientException(400, "protocol");
    }

    /** {window, windowBytes} within this client's ceilings. */
    static int[] welcome(byte[] data) throws Client.ClientException {
        ByteBuffer b = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        if (data.length != WELCOME_BYTES || data[0] != 'D' || data[1] != 'S' || data[2] != 'T' || data[3] != 'V') {
            throw protocol();
        }
        int status = data[4] & 0xFF;
        if (status != 0) {
            // A refusal is a known status with every later field zero.
            for (int i = 5; i < WELCOME_BYTES; i++) {
                if (data[i] != 0) throw protocol();
            }
            Client.ClientException error = failure(status);
            throw error != null ? error : protocol();
        }
        int version = b.getShort(5) & 0xFFFF;
        int maxBody = b.getInt(7);
        int window = b.getShort(11) & 0xFFFF;
        long windowBytes = b.getInt(13) & 0xFFFFFFFFL;
        // Every legal frame must fit the byte window, or nothing more is sent.
        if (version != 1 || maxBody != MAX_BODY || window < 1 || windowBytes < MAX_BODY + HEADER) throw protocol();
        return new int[] {Math.min(window, MAX_IN_FLIGHT), (int) Math.min(windowBytes, MAX_IN_FLIGHT_BYTES)};
    }

    private static void need(ByteBuffer b, int n) throws Malformed {
        if (b.remaining() < n) throw new Malformed("body too short");
    }

    private static long number(ByteBuffer b) throws Malformed {
        need(b, 8);
        long value = b.getLong();
        if (value < 0) throw new Malformed("offset out of range");
        return value;
    }

    private static int u16(ByteBuffer b) throws Malformed {
        need(b, 2);
        return b.getShort() & 0xFFFF;
    }

    private static String string(ByteBuffer b, int max) throws Malformed {
        int n = u16(b);
        if (n > max) throw new Malformed("text too long");
        need(b, n);
        ByteBuffer slice = b.slice(b.position(), n);
        b.position(b.position() + n);
        try {
            return StandardCharsets.UTF_8.newDecoder().decode(slice).toString();
        } catch (CharacterCodingException e) {
            throw new Malformed("text is not UTF-8");
        }
    }

    private static byte[] rest(ByteBuffer b, int max) throws Malformed {
        if (b.remaining() > max) throw new Malformed("value too long");
        byte[] data = new byte[b.remaining()];
        b.get(data);
        return data;
    }

    /**
     * The body of a server frame: a Receipt, an Optional record, a Position, NOTHING for COMMIT and PING, or the
     * ClientException an ERROR names.
     */
    static Object reply(int type, ByteBuffer b) throws Malformed {
        b.order(ByteOrder.LITTLE_ENDIAN);
        Object result;
        if (type == (REPLY | PUBLISH)) {
            int partition = u16(b);
            result = new Client.Receipt(partition, number(b));
        } else if (type == (REPLY | READ)) {
            need(b, 1);
            int found = b.get();
            if (found == 0) {
                result = Optional.empty();
            } else if (found == 1) {
                int partition = u16(b);
                long offset = number(b);
                need(b, 8);
                String time = rfc3339(b.getLong());
                String key = string(b, MAX_KEY);
                String id = string(b, MAX_ID);
                result = Optional.of(new Client.StreamRecord(
                    new Client.Receipt(partition, offset), key, id, time, rest(b, MAX_VALUE)));
            } else {
                throw new Malformed("found is neither 0 nor 1");
            }
        } else if (type == (REPLY | POSITION)) {
            long next = number(b);
            result = new Client.Position(next, rest(b, MAX_VALUE));
        } else if (type == (REPLY | COMMIT) || type == (REPLY | PING)) {
            result = NOTHING;
        } else if (type == ERROR) {
            need(b, 1);
            result = failure(b.get() & 0xFF);
            if (result == null) throw new Malformed("unknown error code");
        } else {
            throw new Malformed("unknown reply type");
        }
        if (b.hasRemaining()) throw new Malformed("bytes left over");
        return result;
    }

    /** UTC with nanoseconds and no trailing zeros: the text the HTTP API puts in Dostoevsky-Time. */
    static String rfc3339(long nanos) {
        long seconds = Math.floorDiv(nanos, 1_000_000_000L);
        int rest = (int) Math.floorMod(nanos, 1_000_000_000L);
        var text = new StringBuilder(SECONDS.format(LocalDateTime.ofEpochSecond(seconds, 0, ZoneOffset.UTC)));
        if (rest != 0) {
            String fraction = Integer.toString(1_000_000_000 + rest).substring(1);
            int end = fraction.length();
            while (fraction.charAt(end - 1) == '0') end--;
            text.append('.').append(fraction, 0, end);
        }
        return text.append('Z').toString();
    }
}

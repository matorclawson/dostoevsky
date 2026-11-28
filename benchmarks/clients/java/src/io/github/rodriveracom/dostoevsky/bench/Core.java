package io.github.rodriveracom.dostoevsky.bench;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The workload, histogram and receipt line shared by every engine adapter of the Java worker. It follows
 * benchmarks/embedded/internal/run and the Python and JavaScript workers: 512-byte records whose first eight
 * bytes are the big-endian message index, concurrent synchronous publishes acknowledged only after the
 * engine's durable acknowledgment, no retries, every acknowledged record read back, and the stop reasons
 * reached / window / failed / verification-failed. JDK only, so it is tested without any client library.
 */
public final class Core {
    private Core() {}

    public static final int RECORD_BYTES = 512;
    public static final int HIST_BINS = 2400;
    public static final String REACHED = "reached", WINDOW = "window", FAILED = "failed", VERIFY = "verification-failed";

    /** One engine. publish returns the offset only after the engine's durable acknowledgment. Never retried. */
    public interface Adapter {
        long publish(byte[] data) throws Exception;

        byte[] read(long offset) throws Exception;

        void close() throws Exception;

        String clientName();

        default String clientBuild() { return "n/a"; }
    }

    /** The engine gave no answer before the deadline: the publish may or may not have been stored. */
    public static final class AmbiguousOutcome extends RuntimeException {
        public AmbiguousOutcome(String message, Throwable cause) { super(message, cause); }
    }

    // ---- the 1% histogram (StrictMath: the same fdlibm results as the other languages) ----
    private static final double LOG = StrictMath.log(1.01);

    public static final class Hist {
        final long[] n = new long[HIST_BINS];

        public static int index(long ns) {
            double us = ns / 1e3;
            if (us < 1) return 0;
            return Math.min((int) (StrictMath.log(us) / LOG), HIST_BINS - 1);
        }

        public void addNs(long ns) { n[index(ns)]++; }

        public void merge(Hist o) { for (int i = 0; i < n.length; i++) n[i] += o.n[i]; }

        public long total() { long t = 0; for (long c : n) t += c; return t; }

        /** The upper edge of the bin holding the q-th quantile, in whole nanoseconds. */
        public long quantileNs(double q) {
            long total = total();
            if (total == 0) return 0;
            long rank = Math.max((long) Math.ceil(q * total), 1);
            long seen = 0;
            for (int i = 0; i < n.length; i++) {
                seen += n[i];
                if (seen >= rank) return (long) (StrictMath.pow(1.01, i + 1) * 1e3);
            }
            return 0;
        }

        /** As receipts print it: microseconds with three decimals, nothing rounded away. */
        public String quantileUsText(double q) { return String.format(Locale.ROOT, "%.3f", quantileNs(q) / 1000.0); }
    }

    public static byte[] payload(long i) {
        return ByteBuffer.allocate(RECORD_BYTES).putLong(i).array();
    }

    /** What may go into a receipt about an engine error: the class and one fixed word, never the message. */
    public static String reason(Throwable e) {
        String name = e.getClass().getSimpleName();
        if (!name.matches("[A-Za-z_][A-Za-z0-9_]{0,63}")) name = "Exception";
        String word = "engine-error";
        if (e instanceof AmbiguousOutcome) word = "ambiguous-timeout";
        else if (e instanceof java.util.concurrent.TimeoutException || e instanceof java.net.SocketTimeoutException
                || e instanceof java.net.http.HttpTimeoutException) word = "timeout";
        else if (e instanceof java.net.ConnectException || e instanceof java.net.SocketException
                || e instanceof java.nio.channels.ClosedChannelException) word = "connection";
        else if (e instanceof IllegalArgumentException || e instanceof NumberFormatException || e instanceof ClassCastException) word = "protocol";
        return name + ":" + word;
    }

    public static final class Result {
        public long acked;
        public double elapsedSeconds;
        public String stop = REACHED;
        public String detail = "";
        public Hist hist = new Hist();
        public final List<long[]> records = new ArrayList<>(); // {message index, engine offset}
        public boolean ambiguous;
    }

    private static final class Stop {
        final String kind, text;
        Stop(String kind, String text) { this.kind = kind; this.text = text; }
    }

    /**
     * Publishes with {@code workers} synchronous producers until {@code target} messages are acknowledged, the
     * window (seconds) ends, or a publish fails. The failing publish is not counted and nothing is retried. A
     * failure is never hidden by an earlier "reached" or "window".
     */
    public static Result runWorkload(Adapter adapter, int workers, long target, double windowSeconds) throws InterruptedException {
        final Object lock = new Object();
        final long[] state = {0};            // next message index
        final Stop[] stop = {null};
        final boolean[] ambiguous = {false};
        final AtomicLong clockStart = new AtomicLong(System.nanoTime());
        final long start = clockStart.get();
        final long deadline = windowSeconds > 0 ? start + (long) (windowSeconds * 1e9) : Long.MAX_VALUE;
        final List<Hist> hists = new ArrayList<>();
        final List<List<long[]>> parts = new ArrayList<>();

        Runnable producer = () -> {
            Hist h = new Hist();
            List<long[]> mine = new ArrayList<>();
            synchronized (lock) { hists.add(h); parts.add(mine); }
            for (;;) {
                long i;
                synchronized (lock) {
                    if (stop[0] == null && System.nanoTime() >= deadline) stop[0] = new Stop(WINDOW, "");
                    if (stop[0] != null) return;
                    i = state[0];
                    if (i >= target) { stop[0] = new Stop(REACHED, ""); return; }
                    state[0] = i + 1;
                }
                long begin = System.nanoTime();
                long offset;
                try {
                    offset = adapter.publish(payload(i));
                } catch (Throwable e) {
                    synchronized (lock) {
                        if (stop[0] == null || !FAILED.equals(stop[0].kind)) stop[0] = new Stop(FAILED, "publish " + i + ": " + reason(e));
                        ambiguous[0] |= e instanceof AmbiguousOutcome;
                    }
                    return;
                }
                h.addNs(System.nanoTime() - begin);
                mine.add(new long[] {i, offset});
            }
        };
        List<Thread> threads = new ArrayList<>();
        for (int w = 0; w < workers; w++) {
            Thread t = new Thread(producer, "producer-" + w);
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) t.join();
        Result out = new Result();
        out.elapsedSeconds = (System.nanoTime() - start) / 1e9;
        for (Hist h : hists) out.hist.merge(h);
        for (List<long[]> p : parts) out.records.addAll(p);
        out.acked = out.records.size();
        Stop s = stop[0] == null ? new Stop(REACHED, "") : stop[0];
        out.stop = s.kind;
        out.detail = FAILED.equals(s.kind) ? s.text + " (after " + out.acked + " acknowledged)" : s.text;
        out.ambiguous = ambiguous[0];
        return out;
    }

    /** Reads every acknowledged record back in offset order; returns "" or a plain-words failure. */
    public static String verify(Adapter adapter, List<long[]> records) {
        List<long[]> sorted = new ArrayList<>(records);
        sorted.sort((a, b) -> Long.compare(a[1], b[1]));
        for (long[] r : sorted) {
            byte[] got;
            try {
                got = adapter.read(r[1]);
            } catch (Throwable e) {
                return "record " + r[0] + " at " + r[1] + ": read " + reason(e);
            }
            if (!java.util.Arrays.equals(got, payload(r[0]))) {
                return "record " + r[0] + " at " + r[1] + ": the bytes read back differ from the bytes published";
            }
        }
        return "";
    }

    /** A stream ID is "&lt;milliseconds&gt;-&lt;sequence&gt;"; packed into one long as the other harnesses do. */
    public static long packId(String id) {
        String[] p = id.split("-");
        long seq = Long.parseLong(p[1]);
        if (seq >= 1 << 16) throw new IllegalArgumentException("stream id sequence does not fit");
        return (Long.parseLong(p[0]) << 16) | seq;
    }

    public static String unpackId(long offset) { return (offset >>> 16) + "-" + (offset & 0xFFFF); }

    /** A value that stays ONE whitespace-free token, so the line parses as key=value pairs. */
    static String token(String text) { return text.replaceAll("\\s+", "_"); }

    /** One line. result=FAILED whenever the run failed or its read-back did not verify; records/s counts verified messages only. */
    public static String receiptLine(String engine, int workers, Result res, long verified, String client, long rssKiB, String rssSource, String build) {
        boolean ok = REACHED.equals(res.stop) || WINDOW.equals(res.stop);
        double rate = res.elapsedSeconds > 0 ? verified / res.elapsedSeconds : 0;
        List<String> f = new ArrayList<>(List.of(
            "engine=" + engine, "workers=" + workers, "result=" + (ok ? "ok" : "FAILED"),
            "acked=" + res.acked, "verified=" + verified,
            String.format(Locale.ROOT, "records/s=%.0f", rate), String.format(Locale.ROOT, "elapsed_s=%.2f", res.elapsedSeconds),
            "p50_us=" + res.hist.quantileUsText(0.50), "p95_us=" + res.hist.quantileUsText(0.95), "p99_us=" + res.hist.quantileUsText(0.99),
            "p99_9_us=" + res.hist.quantileUsText(0.999), "p99_99_us=" + res.hist.quantileUsText(0.9999), "quantiles=bin-upper-1pct",
            "client_rss_KiB=" + rssKiB, "client_rss_source=" + rssSource, "lang=java",
            "client=" + token(client), "client_build=" + token(build)));
        if (res.ambiguous) f.add("ambiguous=1");
        f.add("stop=" + res.stop);
        if (!res.detail.isEmpty()) {
            String d = res.detail.replaceAll("[^A-Za-z0-9_.:,()/-]", "_");
            f.add("detail=" + d.substring(0, Math.min(200, d.length())));
        }
        return String.join(" ", f);
    }

    /** This JVM's peak resident size in KiB where the kernel keeps one (Linux VmHWM), else -1. */
    public static long peakRssKiB() {
        try {
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("VmHWM:")) return Long.parseLong(line.replaceAll("[^0-9]", ""));
            }
        } catch (Exception ignored) { }
        return -1;
    }
}

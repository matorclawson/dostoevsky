package io.github.rodriveracom.dostoevsky.bench;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Tests of the worker with an in-memory engine; JDK only. Run with test.sh (java -ea). */
public final class CoreTest {
    static int checks;

    static void check(boolean ok, String what) {
        checks++;
        if (!ok) throw new AssertionError(what);
    }

    /** An in-memory engine: publish returns the offset; read returns what was stored. */
    static class Memory implements Core.Adapter {
        final List<byte[]> log = new ArrayList<>();
        final long failAt;
        final boolean corrupt;
        String build = "n/a";
        long attempts;

        Memory(long failAt, boolean corrupt) { this.failAt = failAt; this.corrupt = corrupt; }

        Memory() { this(-1, false); }

        public synchronized long publish(byte[] data) {
            attempts++;
            if (failAt >= 0 && attempts == failAt) throw new RuntimeException("the engine went away");
            log.add(data.clone());
            return log.size() - 1;
        }

        public synchronized byte[] read(long offset) {
            byte[] b = log.get((int) offset).clone();
            if (corrupt) b[b.length - 1] ^= 1;
            return b;
        }

        public void close() {}

        public String clientName() { return "memory-0"; }

        public String clientBuild() { return build; }
    }

    static String json(String file) throws Exception { return Files.readString(Path.of(file)); }

    static void histogram(String vectorFile) throws Exception {
        String v = json(vectorFile);
        check(v.contains("\"bins\": 2400"), "the vector's bin count");
        Matcher idx = Pattern.compile("\"index\": \\{(.*?)\\}", Pattern.DOTALL).matcher(v);
        check(idx.find(), "index block");
        Matcher e = Pattern.compile("\"(\\d+)\": (\\d+)").matcher(idx.group(1));
        int n = 0;
        while (e.find()) {
            check(Core.Hist.index(Long.parseLong(e.group(1))) == Integer.parseInt(e.group(2)), "index of " + e.group(1));
            n++;
        }
        check(n >= 40, "the vector has " + n + " index cases, edge-adjacent ones included");
        Matcher dur = Pattern.compile("\"durations_ns\": \\[(.*?)\\]", Pattern.DOTALL).matcher(v);
        check(dur.find(), "durations block");
        Core.Hist h = new Core.Hist();
        for (String s : dur.group(1).split(",")) h.addNs(Long.parseLong(s.trim()));
        Matcher qs = Pattern.compile("\"quantiles_ns\": \\{(.*?)\\}", Pattern.DOTALL).matcher(v);
        check(qs.find(), "quantiles block");
        Matcher q = Pattern.compile("\"([0-9.]+)\": (\\d+)").matcher(qs.group(1));
        int m = 0;
        while (q.find()) {
            check(h.quantileNs(Double.parseDouble(q.group(1))) == Long.parseLong(q.group(2)), "quantile " + q.group(1));
            m++;
        }
        check(m >= 7, "quantiles checked: " + m);
        Core.Hist one = new Core.Hist();
        one.addNs(1900);
        long got = one.quantileNs(0.5);
        check(got >= 1900 && got <= 1900 * 1.0101, "a short sample keeps its sub-microsecond precision: " + got);
        check(new Core.Hist().quantileNs(0.99) == 0, "an empty histogram");
    }

    static void workload() throws Exception {
        byte[] p = Core.payload(258);
        check(p.length == 512 && p[6] == 1 && p[7] == 2, "a record is 512 bytes starting with its index");
        for (int workers : new int[] {1, 8, 32}) {
            Memory m = new Memory();
            Core.Result r = Core.runWorkload(m, workers, 500, 0);
            check(Core.REACHED.equals(r.stop) && r.acked == 500 && m.log.size() == 500, "reached at " + workers + " workers");
            long distinct = r.records.stream().map(x -> x[0]).distinct().count();
            check(distinct == 500, "every index acknowledged exactly once");
            check(Core.verify(m, r.records).isEmpty(), "verification");
        }
        Memory f = new Memory(6, false);
        Core.Result r = Core.runWorkload(f, 1, 100, 0);
        check(Core.FAILED.equals(r.stop) && r.acked == 5 && f.attempts == 6, "a failure is a result and is never retried: " + r.acked + " " + f.attempts);
        check(r.detail.contains("RuntimeException:engine-error") && !r.detail.contains("the engine went away") && r.detail.contains("after 5 acknowledged"), r.detail);
        Memory many = new Memory(40, false);
        Core.Result mr = Core.runWorkload(many, 8, 10000, 0);
        check(Core.FAILED.equals(mr.stop) && mr.acked == many.log.size() && many.attempts - mr.acked == 1, "many workers: attempts exceed acks by one");
        Memory slow = new Memory() {
            public synchronized long publish(byte[] d) {
                try { Thread.sleep(10); } catch (InterruptedException ignored) { }
                return super.publish(d);
            }
        };
        long begin = System.nanoTime();
        Core.Result w = Core.runWorkload(slow, 4, Long.MAX_VALUE, 0.3);
        check(Core.WINDOW.equals(w.stop) && w.acked > 0 && System.nanoTime() - begin < 5_000_000_000L, "the window ends the run");
        Memory bad = new Memory(-1, true);
        Core.Result br = Core.runWorkload(bad, 2, 20, 0);
        check(Core.verify(bad, br.records).contains("differ"), "verification finds a changed byte");
    }

    static void failureAfterReached() throws Exception {
        // Target 2, two workers: one finishes index 0 and sees the target reached; the other's publish (index 1) is
        // still in flight and then fails. The run must be a failed run, not "reached".
        CountDownLatch go = new CountDownLatch(1);
        Memory m = new Memory() {
            public long publish(byte[] data) {
                if (data[7] == 1) {
                    try { go.await(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) { }
                    throw new RuntimeException("reset by peer");
                }
                return super.publish(data);
            }
        };
        new Thread(() -> { try { Thread.sleep(300); } catch (InterruptedException ignored) { } go.countDown(); }).start();
        Core.Result r = Core.runWorkload(m, 2, 2, 0);
        check(Core.FAILED.equals(r.stop) && r.acked == 1, "a late failure still makes the run failed: " + r.stop);
        check(Core.receiptLine("x", 2, r, 1, "c", 1, "VmHWM", "n/a").split(" ")[2].equals("result=FAILED"), "result=FAILED");
    }

    static void sanitising() throws Exception {
        Memory leaky = new Memory() {
            public synchronized long publish(byte[] d) { throw new RuntimeException("AUTH failed for redis://user:hunter2@10.0.0.9:6379 payload=abab"); }
        };
        Core.Result r = Core.runWorkload(leaky, 1, 5, 0);
        String line = Core.receiptLine("x", 1, r, 0, "c", 1, "VmHWM", "n/a");
        for (String leak : new String[] {"hunter2", "10.0.0.9", "redis://", "payload", "AUTH"}) check(!line.contains(leak), leak);
        check(line.contains("RuntimeException:engine-error"), "the class and a fixed word");
        check(Core.reason(new Core.AmbiguousOutcome("x", null)).equals("AmbiguousOutcome:ambiguous-timeout"), "ambiguous reason");
        check(Core.reason(new java.util.concurrent.TimeoutException("x")).equals("TimeoutException:timeout"), "timeout reason");
        check(Core.reason(new java.net.ConnectException("x")).equals("ConnectException:connection"), "connection reason");
        Memory m = new Memory();
        Core.Result ok = Core.runWorkload(m, 1, 3, 0);
        Core.Adapter readLeak = new Memory() {
            public synchronized byte[] read(long o) { throw new RuntimeException("secret-in-read hunter2"); }
        };
        String why = Core.verify(readLeak, ok.records);
        check(!why.contains("hunter2") && why.contains("RuntimeException:engine-error"), why);
    }

    static Core.Result fixed(String stop, String detail, long acked, boolean ambiguous) {
        Core.Result r = new Core.Result();
        for (long i = 0; i < acked; i++) r.hist.addNs(2_000_000);
        r.acked = acked;
        r.elapsedSeconds = 2.0;
        r.stop = stop;
        r.detail = detail;
        r.ambiguous = ambiguous;
        return r;
    }

    static void receipts(String examplesFile) throws Exception {
        List<String> lines = new ArrayList<>();
        for (String l : Files.readAllLines(Path.of(examplesFile))) if (!l.isBlank() && !l.startsWith("#")) lines.add(l.trim());
        Map<String, String> want = new java.util.HashMap<>();
        for (String l : lines) want.put(l.split(" ")[0], l);
        check(Core.receiptLine("java-kafka", 8, fixed(Core.REACHED, "", 1000, false), 1000, "kafka-clients-4.4.0", 51200, "VmHWM", "n/a").equals(want.get("engine=java-kafka")), "the kafka line equals the shared example");
        check(Core.receiptLine("java-dostoevsky", 8, fixed(Core.REACHED, "", 1000, false), 1000, "dostoevsky-client-0.1.0", 51200, "VmHWM", "0123456789ab").equals(want.get("engine=java-dostoevsky")), "the dostoevsky line");
        check(Core.receiptLine("java-redis", 8, fixed(Core.FAILED, "publish 4: AmbiguousOutcome:ambiguous-timeout (after 4 acknowledged)", 4, true), 4, "jedis-8.0.2", 51200, "ps-sampled", "n/a").equals(want.get("engine=java-redis")), "the failed redis line");
        String spaced = Core.receiptLine("java-x", 8, fixed(Core.REACHED, "", 1000, false), 1000, "name with spaces (pool)", 1, "VmHWM", "build id");
        for (String t : spaced.split(" ")) check(t.contains("="), "stray token " + t);
        check(Core.receiptLine("x", 1, fixed(Core.VERIFY, "x", 100, false), 0, "c", 1, "VmHWM", "n/a").contains("records/s=0 "), "verified-only throughput");
    }

    static String[] run(String[] args, Core.Adapter adapter, Map<String, String> env) throws Exception {
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        int code = Worker.run(args, env, new PrintStream(o, true, StandardCharsets.UTF_8), new PrintStream(e, true, StandardCharsets.UTF_8),
            Map.of("memory", (Worker.Factory) (addr, token, workers) -> adapter));
        return new String[] {Integer.toString(code), o.toString(StandardCharsets.UTF_8), e.toString(StandardCharsets.UTF_8)};
    }

    static void worker() throws Exception {
        String[] r = run(new String[] {"--engine", "memory", "--addr", "x:1", "--workers", "4", "--target", "200"}, new Memory(), Map.of("BENCH_TOKEN", "secret-token-value"));
        check(r[0].equals("0") && r[1].trim().split("\n").length == 1 && r[1].contains("verified=200") && !r[1].contains("secret-token-value"), "one trial, one line, no token: " + r[1]);
        r = run(new String[] {"--engine", "memory", "--addr", "x:1", "--target", "50"}, new Memory(11, false), Map.of());
        check(r[0].equals("0") && r[1].contains("stop=failed") && r[1].contains("result=FAILED") && r[1].contains("acked=10 verified=10"), r[1]);
        r = run(new String[] {"--engine", "memory", "--addr", "x:1", "--target", "20"}, new Memory(-1, true), Map.of());
        check(r[0].equals("0") && r[1].contains("stop=verification-failed") && r[1].contains("verified=0") && r[1].contains("records/s=0 "), r[1]);
        String[][] bad = {{"--workers", "1.5"}, {"--workers", "0"}, {"--target", "abc"}, {"--target", "0"}, {"--window", "Infinity"}, {"--window", "-1"}, {"--window", "NaN"}, {"--window", "86401"}, {"--window", "1e300"}};
        for (String[] b : bad) {
            String[] args = {"--engine", "memory", "--addr", "x:1", b[0], b[1]};
            check(run(args, new Memory(), Map.of())[0].equals("2"), String.join(" ", b));
        }
        check(run(new String[] {"--engine", "nope", "--addr", "x:1"}, new Memory(), Map.of())[0].equals("2"), "unknown engine");
        r = run(new String[] {"--engine", "memory", "--addr", "x:1", "--target", "3", "--window", "86400"}, new Memory(), Map.of());
        check(r[0].equals("0") && r[1].contains("stop=reached"), "a window of exactly 24 h is accepted and the run ends at its target");
        Memory unknown = new Memory();
        unknown.build = "unknown";
        check(run(new String[] {"--engine", "memory", "--addr", "x:1", "--target", "5", "--require-known-build"}, unknown, Map.of())[0].equals("2"), "a gated run refuses an unknown build");
        r = run(new String[] {"--engine", "memory", "--addr", "x:1", "--target", "5"}, unknown, Map.of());
        check(r[0].equals("0") && r[1].contains("client_build=unknown"), "an ungated run records it");
    }

    static void ids() throws Exception {
        check(Core.unpackId(Core.packId("1700000000123-7")).equals("1700000000123-7"), "stream ids round trip");
        boolean threw = false;
        try { Core.packId("5-65536"); } catch (IllegalArgumentException e) { threw = true; }
        check(threw, "a sequence that does not fit is refused");
    }

    static void git(Path dir, String... args) throws Exception {
        List<String> cmd = new ArrayList<>(List.of("git", "-C", dir.toString(), "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"));
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        p.getInputStream().readAllBytes();
        check(p.waitFor() == 0, "git " + String.join(" ", args));
    }

    static void build() throws Exception {
        Path repo = Files.createTempDirectory("buildid");
        git(repo, "init", "-q");
        Files.createDirectories(repo.resolve("clients/java"));
        Files.writeString(repo.resolve("clients/java/Client.java"), "class Client {}\n");
        git(repo, "add", ".");
        git(repo, "commit", "-q", "-m", "one");
        Path dir = repo.resolve("clients/java"); // a RELATIVE-looking subdirectory: the old code resolved its pathspec twice
        String clean = Build.id(dir);
        check(clean.matches("[0-9a-f]{12}"), "a clean tree reports a bare commit, not -dirty: " + clean);
        Files.writeString(dir.resolve("UntrackedBuildOutput.class"), "junk\n"); // untracked build output must not count
        check(Build.id(dir).equals(clean), "an untracked file does not make the tree dirty: " + Build.id(dir));
        Files.writeString(dir.resolve("Client.java"), "class Client { int x; }\n");
        check(Build.id(dir).equals(clean + "-dirty"), "a modified file is -dirty: " + Build.id(dir));
        git(repo, "commit", "-q", "-am", "two");
        check(Build.id(dir).matches("[0-9a-f]{12}") && !Build.id(dir).equals(clean), "committed again: a new clean id");
        Path none = Files.createTempDirectory("notarepo");
        PrintStream saved = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        String unknown;
        try { unknown = Build.id(none); } finally { System.setErr(saved); }
        check(unknown.equals("unknown") && captured.size() == 0, "outside a repository: unknown, and git's complaint is not echoed");
    }

    public static void main(String[] args) throws Exception {
        String root = args.length > 0 ? args[0] : "../..";
        histogram(root + "/hist_vector.json");
        workload();
        failureAfterReached();
        sanitising();
        receipts(root + "/receipt_examples_java.txt");
        worker();
        ids();
        build();
        System.out.println("ok " + checks + " checks");
    }
}

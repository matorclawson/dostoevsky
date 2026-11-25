package io.github.rodriveracom.dostoevsky.bench;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

/**
 * One trial of the publisher workload from Java against one engine; prints one receipt line.
 *
 * <pre>BENCH_TOKEN=... java -cp ... io.github.rodriveracom.dostoevsky.bench.Main --engine kafka --addr 127.0.0.1:PORT
 *     --workers 8 --target 16000 [--window 600] [--require-known-build]</pre>
 *
 * The token comes from the environment, never from the command line. A publish failure is a result
 * (stop=failed, result=FAILED), never an exit code, and is never retried; usage errors exit 2.
 */
public final class Worker {
    private Worker() {}

    /** Creates one adapter; it may connect. */
    public interface Factory {
        Core.Adapter create(String addr, String token, int workers) throws Exception;
    }

    /** Peak client memory: the kernel's VmHWM where there is one, else the largest `ps` sample. */
    static final class Rss {
        private volatile long max = -1;
        private volatile boolean stop;
        private Thread sampler;

        void start() {
            if (Core.peakRssKiB() >= 0) return;
            sampler = new Thread(() -> {
                while (!stop) {
                    try {
                        Process p = new ProcessBuilder("ps", "-o", "rss=", "-p", Long.toString(ProcessHandle.current().pid())).redirectErrorStream(true).start();
                        String s = new String(p.getInputStream().readAllBytes()).trim();
                        p.waitFor();
                        if (!s.isEmpty()) max = Math.max(max, Long.parseLong(s));
                        Thread.sleep(200);
                    } catch (Exception e) { return; }
                }
            }, "rss-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }

        long peakKiB() {
            long k = Core.peakRssKiB();
            return k >= 0 ? k : Math.max(max, 0);
        }

        String source() { return Core.peakRssKiB() >= 0 ? "VmHWM" : "ps-sampled"; }

        void stop() { stop = true; }
    }

    public static int run(String[] argv, Map<String, String> env, PrintStream out, PrintStream err, Map<String, Factory> engines) throws Exception {
        Map<String, String> a = new TreeMap<>();
        boolean requireKnownBuild = false;
        for (int i = 0; i < argv.length; i++) {
            String k = argv[i];
            if (k.equals("--require-known-build")) { requireKnownBuild = true; continue; }
            if (!k.startsWith("--") || i + 1 >= argv.length) { err.println("usage error near " + k); return 2; }
            a.put(k.substring(2), argv[++i]);
        }
        long workers, target;
        double window;
        try {
            workers = Long.parseLong(a.getOrDefault("workers", "1"));
            target = Long.parseLong(a.getOrDefault("target", "2000"));
            window = Double.parseDouble(a.getOrDefault("window", "0"));
        } catch (NumberFormatException e) {
            err.println("--workers and --target must be whole numbers and --window a number");
            return 2;
        }
        if (!a.containsKey("engine") || !a.containsKey("addr") || workers < 1 || workers > 100000 || target < 1
                || Double.isNaN(window) || Double.isInfinite(window) || window < 0) {
            err.println("usage: --engine E --addr host:port [--workers N>=1] [--target N>=1] [--window S>=0, finite] [--require-known-build]");
            return 2;
        }
        Factory factory = engines.get(a.get("engine"));
        if (factory == null) {
            err.println("unknown engine '" + a.get("engine") + "' (have " + String.join(", ", new TreeMap<>(engines).keySet()) + ")");
            return 2;
        }
        Core.Adapter adapter = factory.create(a.get("addr"), env.getOrDefault("BENCH_TOKEN", ""), (int) workers);
        if (requireKnownBuild && "unknown".equals(adapter.clientBuild())) {
            adapter.close();
            err.println("the client build is unknown (no git commit could be read); refusing to run a gated trial");
            return 2;
        }
        Rss rss = new Rss();
        rss.start();
        Core.Result res;
        long verified = 0;
        try {
            res = Core.runWorkload(adapter, (int) workers, target, window);
            if (!Core.FAILED.equals(res.stop) || !res.records.isEmpty()) { // also read back what a failed run acknowledged
                String why = Core.verify(adapter, res.records);
                if (!why.isEmpty()) { res.stop = Core.VERIFY; res.detail = why; } else verified = res.acked;
            }
        } finally {
            adapter.close();
            rss.stop();
        }
        out.println(Core.receiptLine("java-" + a.get("engine"), (int) workers, res, verified, adapter.clientName(), rss.peakKiB(), rss.source(), adapter.clientBuild()));
        return 0;
    }
}

package io.github.rodriveracom.dostoevsky.bench;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

/** The git identity of a source directory, JDK only so it is tested without any client library. */
public final class Build {
    private Build() {}

    /**
     * "&lt;12 hex&gt;" of the commit the checkout holding {@code dir} is at, plus "-dirty" when that directory has
     * uncommitted changes to TRACKED files (untracked build output such as node_modules does not count), or "unknown" when git cannot say. The status is asked about "." relative to {@code dir}
     * (not a path that would be resolved against {@code dir} a second time), and git's own error text is
     * discarded: it never reaches the status or the receipt.
     */
    public static String id(Path dir) {
        try {
            String sha = git(dir, "rev-parse", "--short=12", "HEAD");
            String dirty = git(dir, "status", "--porcelain", "--untracked-files=no", "--", ".");
            return sha + (dirty.isEmpty() ? "" : "-dirty");
        } catch (Exception e) {
            return "unknown";
        }
    }

    private static String git(Path dir, String... args) throws Exception {
        String[] cmd = new String[args.length + 3];
        cmd[0] = "git"; cmd[1] = "-C"; cmd[2] = dir.toString();
        System.arraycopy(args, 0, cmd, 3, args.length);
        Process p = new ProcessBuilder(cmd).redirectError(ProcessBuilder.Redirect.DISCARD).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        if (!p.waitFor(10, TimeUnit.SECONDS) || p.exitValue() != 0) throw new IllegalStateException("git failed");
        return out;
    }
}

package io.github.rodriveracom.dostoevsky.bench;

import java.util.Map;

/** Wires the real engine adapters into the worker. The tests drive {@link Worker} with an in-memory engine instead. */
public final class Main {
    private Main() {}

    public static void main(String[] args) {
        int code;
        try {
            code = Worker.run(args, System.getenv(), System.out, System.err, Adapters.ENGINES);
        } catch (Throwable t) {
            // One deliberate final line (class only: a message can carry anything), not a stack trace the driver would cut.
            System.err.println("worker error: " + Core.reason(t));
            code = 1;
        }
        System.exit(code);
    }
}

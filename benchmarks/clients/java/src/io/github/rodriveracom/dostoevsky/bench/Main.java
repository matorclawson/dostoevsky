package io.github.rodriveracom.dostoevsky.bench;

import java.util.Map;

/** Wires the real engine adapters into the worker. The tests drive {@link Worker} with an in-memory engine instead. */
public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        System.exit(Worker.run(args, System.getenv(), System.out, System.err, Adapters.ENGINES));
    }
}

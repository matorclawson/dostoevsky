import io.github.rodriveracom.dostoevsky.Client;
import java.nio.charset.StandardCharsets;

public class Worker {
    public static void main(String[] args) throws Exception {
        var client = new Client(System.getenv().getOrDefault("DOSTOEVSKY_URL", "http://127.0.0.1:8787"),
                                System.getenv("DOSTOEVSKY_TOKEN"));
        var ack = client.publish("jobs", "task from java".getBytes(StandardCharsets.UTF_8), "jobs+λ", "java-demo");
        String group = "java-worker";
        long next = client.position("jobs", ack.partition(), group).next();
        while (next <= ack.offset()) {
            var job = client.read("jobs", ack.partition(), next, 1000).orElseThrow();
            System.out.println(job.id() + ": " + new String(job.value(), StandardCharsets.UTF_8));
            client.commit("jobs", ack.partition(), group, next, job.id().getBytes(StandardCharsets.UTF_8));
            next++;
        }
    }
}

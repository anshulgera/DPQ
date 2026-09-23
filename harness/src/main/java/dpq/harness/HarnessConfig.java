package dpq.harness;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Harness settings from {@code --key=value} flags (D14b).
 *
 * @param rate      enqueues per second in open-loop mode, or 0 for closed-loop
 * @param duration  seconds of open-loop load; ignored in closed-loop mode
 * @param messages  total messages in closed-loop mode; open-loop sends {@code rate × duration}
 * @param crashRate fraction of deliveries a consumer "crashes" on (never acks), forcing a redelivery
 */
record HarnessConfig(String baseUrl, String queue, int producers, int consumers, int messages, double crashRate,
        int duration, int rate) {

    static final String USAGE = """
            usage: harness [--base-url=http://localhost:8080] [--queue=harness-<random>] [--producers=4]
                           [--consumers=4] [--messages=10000] [--crash-rate=0.05] [--rate=0] [--duration=30]
              --rate=N      open-loop: N enqueues/s for --duration seconds, latency measured from each
                            request's scheduled start (no coordinated omission); 0 = closed-loop
              --messages=N  closed-loop: total messages, split across producers
            The queue is created with a 2s visibility timeout so crashed deliveries come back within the run.
            Exits 0 when p95 < 100ms for enqueue and dequeue and every message is accounted for.""";

    boolean openLoop() {
        return rate > 0;
    }

    int totalMessages() {
        return openLoop() ? rate * duration : messages;
    }

    static HarnessConfig parse(String... args) {
        Map<String, String> flags = new HashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (!arg.startsWith("--") || eq < 0) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg + "\n" + USAGE);
            }
            flags.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        HarnessConfig config = new HarnessConfig(
                flags.getOrDefault("base-url", "http://localhost:8080"),
                flags.getOrDefault("queue", "harness-" + UUID.randomUUID().toString().substring(0, 8)),
                Integer.parseInt(flags.getOrDefault("producers", "4")),
                Integer.parseInt(flags.getOrDefault("consumers", "4")),
                Integer.parseInt(flags.getOrDefault("messages", "10000")),
                Double.parseDouble(flags.getOrDefault("crash-rate", "0.05")),
                Integer.parseInt(flags.getOrDefault("duration", "30")),
                Integer.parseInt(flags.getOrDefault("rate", "0")));
        flags.keySet().removeAll(java.util.Set.of("base-url", "queue", "producers", "consumers", "messages",
                "crash-rate", "duration", "rate"));
        if (!flags.isEmpty()) {
            throw new IllegalArgumentException("unknown flags " + flags.keySet() + "\n" + USAGE);
        }
        if (config.producers() < 1 || config.consumers() < 1 || config.totalMessages() < 1
                || config.crashRate() < 0 || config.crashRate() >= 1) {
            throw new IllegalArgumentException("invalid settings: " + config + "\n" + USAGE);
        }
        return config;
    }
}

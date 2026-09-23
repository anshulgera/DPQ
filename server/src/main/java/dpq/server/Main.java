package dpq.server;

import dpq.core.Clock;
import dpq.core.QueueService;
import dpq.core.RandomIdGenerator;
import dpq.core.RandomReceiptGenerator;
import dpq.core.SystemClock;
import io.javalin.Javalin;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Starts the service. Settings come from {@code --key=value} arguments, falling back to environment variables:
 * {@code --port} / {@code DPQ_PORT} (default 8080) and {@code --reaper-interval-ms} / {@code DPQ_REAPER_INTERVAL_MS}
 * (default 100). Stops the HTTP server and then the reaper on shutdown (SIGTERM, Ctrl-C).
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) {
        Map<String, String> flags = parseFlags(args);
        int port = setting(flags, "port", "DPQ_PORT", 8080);
        int reaperMillis = setting(flags, "reaper-interval-ms", "DPQ_REAPER_INTERVAL_MS", 100);

        Clock clock = new SystemClock();
        QueueService service = new QueueService(clock, new RandomIdGenerator(clock, new SecureRandom()),
                new RandomReceiptGenerator(),
                QueueService.Options.defaults().withReaperInterval(Duration.ofMillis(reaperMillis)));
        Javalin app = DpqApp.create(service).start(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop(); // stop taking requests first, then stop the reaper
            service.close();
        }, "dpq-shutdown"));
    }

    private static Map<String, String> parseFlags(String[] args) {
        Map<String, String> flags = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--") || !arg.contains("=")) {
                throw new IllegalArgumentException("expected --key=value, got: " + arg);
            }
            int eq = arg.indexOf('=');
            flags.put(arg.substring(2, eq), arg.substring(eq + 1));
        }
        return flags;
    }

    private static int setting(Map<String, String> flags, String flag, String env, int fallback) {
        String value = flags.getOrDefault(flag, System.getenv(env));
        return value == null ? fallback : Integer.parseInt(value);
    }
}

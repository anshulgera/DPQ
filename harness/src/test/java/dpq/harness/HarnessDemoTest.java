package dpq.harness;

import static org.assertj.core.api.Assertions.assertThat;

import dpq.core.Clock;
import dpq.core.QueueService;
import dpq.core.RandomIdGenerator;
import dpq.core.RandomReceiptGenerator;
import dpq.core.SystemClock;
import dpq.server.DpqApp;
import io.javalin.Javalin;
import java.security.SecureRandom;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The enqueue → dequeue → ack flow under concurrency, end to end (D14b): a real server in-process on a random
 * port, and the harness driving it over HTTP with crashing consumers. Latency isn't asserted here (CI machines
 * vary); conservation is.
 */
class HarnessDemoTest {

    @Test
    @Timeout(60)
    void everyMessageIsAccountedForWithCrashingConsumers() throws Exception {
        Report closed = runAgainstServer("--producers=3", "--consumers=3", "--messages=600", "--crash-rate=0.1");

        assertThat(closed.errors()).isZero();
        assertThat(closed.enqueued()).hasSize(600);
        assertThat(closed.crashed()).isPositive(); // redelivery really happened
        assertThat(closed.conservationOk()).as("missing %s", closed.missing()).isTrue();
    }

    @Test
    @Timeout(60)
    void openLoopSendsRateTimesDurationMessages() throws Exception {
        Report open = runAgainstServer("--consumers=2", "--rate=200", "--duration=2", "--crash-rate=0");

        assertThat(open.errors()).isZero();
        assertThat(open.enqueued()).hasSize(400);
        assertThat(open.enqueueLatency().getTotalCount()).isEqualTo(400);
        assertThat(open.conservationOk()).isTrue();
    }

    private static Report runAgainstServer(String... flags) throws Exception {
        Clock clock = new SystemClock();
        try (QueueService service = new QueueService(clock, new RandomIdGenerator(clock, new SecureRandom()),
                new RandomReceiptGenerator(),
                QueueService.Options.defaults().withReaperInterval(Duration.ofMillis(20)))) {
            Javalin app = DpqApp.create(service).start(0);
            try {
                String[] args = new String[flags.length + 1];
                args[0] = "--base-url=http://localhost:" + app.port();
                System.arraycopy(flags, 0, args, 1, flags.length);
                Report report = new Harness(HarnessConfig.parse(args)).run();
                report.print(System.out);
                return report;
            } finally {
                app.stop();
            }
        }
    }
}

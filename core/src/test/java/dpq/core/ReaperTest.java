package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.time.FakeClock;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** The reaper on a real scheduler; queue time still comes from a {@link FakeClock} (D6). */
class ReaperTest {

    @Test
    void theScheduledReaperExpiresLeasesOnAnIdleQueue() {
        FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
        try (QueueService service = new QueueService(clock, new SequentialIdGenerator(),
                new SequentialReceiptGenerator(),
                QueueService.Options.defaults().withReaperInterval(Duration.ofMillis(10)))) {
            service.createQueue("foo", QueueConfig.of(30L, null, null));
            service.enqueue("foo", "m", Priority.HIGH, null);
            service.dequeue("foo").orElseThrow();

            clock.advance(Duration.ofSeconds(30));

            await().atMost(Duration.ofSeconds(5))
                    .until(() -> service.partition("foo").inFlightCount() == 0);
            assertThat(service.partition("foo").readyCount(Priority.HIGH)).isEqualTo(1);
        }
    }

    @Test
    void closeStopsTheSweeps() {
        AtomicInteger sweeps = new AtomicInteger();
        Reaper reaper = new Reaper(sweeps::incrementAndGet, Duration.ofMillis(5));
        await().atMost(Duration.ofSeconds(5)).until(() -> sweeps.get() >= 2);

        reaper.close();

        assertThat(reaper.isTerminated()).isTrue();
        int afterClose = sweeps.get();
        await().during(Duration.ofMillis(100)).atMost(Duration.ofSeconds(1))
                .until(() -> sweeps.get() == afterClose);
    }

    @Test
    void aFailingSweepDoesNotStopLaterSweeps() {
        AtomicInteger sweeps = new AtomicInteger();
        try (Reaper reaper = new Reaper(() -> {
            if (sweeps.incrementAndGet() == 1) {
                throw new IllegalStateException("boom");
            }
        }, Duration.ofMillis(5))) {
            await().atMost(Duration.ofSeconds(5)).until(() -> sweeps.get() >= 3);
            assertThat(reaper.isTerminated()).isFalse();
        }
    }
}

package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.DeliveredMessage;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.model.QueueMetricsSnapshot;
import dpq.core.time.FakeClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Regressions found by the model-based tests (PR 8): the D8d rules for an expired lease are judged at the lease
 * deadline, not at whenever a drain happens to run, so the outcome doesn't depend on reaper timing.
 */
class LeaseExpiryTimingTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private final FakeClock clock = new FakeClock(START);
    private final QueueService service = new QueueService(clock, new SequentialIdGenerator(),
            new SequentialReceiptGenerator(), QueueService.Options.defaults().withoutReaper());

    @AfterEach
    void close() {
        service.close();
    }

    @Test
    void aLeaseThatEndedBeforeTheTtlIsDeadLetteredEvenIfDrainedAfterTheTtl() {
        service.createQueue("q", QueueConfig.of(30L, 1, null));
        service.enqueue("q", "m", Priority.HIGH, Duration.ofSeconds(50));
        service.dequeue("q").orElseThrow(); // lease ends at 30s, TTL at 50s

        clock.advance(Duration.ofSeconds(100)); // first drain happens long after both

        QueueMetricsSnapshot source = service.metrics("q");
        assertThat(source.deadLettered()).isEqualTo(1);
        assertThat(source.expired().get(Priority.HIGH)).isZero();
        assertThat(service.metrics("q.dlq").ready().get(Priority.HIGH)).isEqualTo(1);
    }

    @Test
    void aLeaseThatEndedBeforeTheTtlIsRedeliveredAndThenExpiresWhenDrainedLate() {
        service.createQueue("q", QueueConfig.of(30L, 5, null));
        service.enqueue("q", "m", Priority.HIGH, Duration.ofSeconds(50));
        service.dequeue("q").orElseThrow();

        clock.advance(Duration.ofSeconds(100));

        QueueMetricsSnapshot m = service.metrics("q");
        assertThat(m.redelivered()).isEqualTo(1); // back to ready at 30s ...
        assertThat(m.expired().get(Priority.HIGH)).isEqualTo(1); // ... then expired at 50s
    }

    @Test
    void theDeadLetterTimeIsTheLeaseDeadlineNotTheDrainTime() {
        service.createQueue("q", QueueConfig.of(30L, 1, null));
        service.enqueue("q", "m", Priority.LOW, null);
        service.dequeue("q").orElseThrow(); // lease ends at 30s

        clock.advance(Duration.ofSeconds(100));
        service.sweep(); // the reaper's drain moves it from the idle source queue

        DeliveredMessage dead = service.dequeue("q.dlq").orElseThrow();
        assertThat(dead.deadLetter().deadLetteredAt()).isEqualTo(START.plusSeconds(30));
        assertThat(dead.enqueuedAt()).isEqualTo(START.plusSeconds(30));
    }

    @Test
    void theDlqAgeCountsFromTheLeaseDeadline() {
        service.createQueue("q", QueueConfig.of(30L, 1, null));
        service.enqueue("q", "m", Priority.LOW, null);
        service.dequeue("q").orElseThrow();

        clock.advance(Duration.ofSeconds(100));
        service.sweep();

        assertThat(service.metrics("q.dlq").oldestAgeSeconds()).isEqualTo(70.0);
    }
}

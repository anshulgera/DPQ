package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dpq.core.error.QueueFullException;
import dpq.core.error.QueueNotFoundException;
import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.DeliveredMessage;
import dpq.core.model.MessageId;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.model.QueueMetricsSnapshot;
import dpq.core.time.FakeClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class QueueMetricsTest {

    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final QueueService service = new QueueService(clock, new SequentialIdGenerator(),
            new SequentialReceiptGenerator(), QueueService.Options.defaults().withoutReaper());

    @AfterEach
    void close() {
        service.close();
    }

    @Test
    void readyCountsArePerPriority() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        enqueue(Priority.HIGH, Priority.LOW, Priority.LOW, Priority.MEDIUM, Priority.LOW);

        QueueMetricsSnapshot m = service.metrics("q");

        assertThat(m.ready()).containsEntry(Priority.HIGH, 1L).containsEntry(Priority.MEDIUM, 1L)
                .containsEntry(Priority.LOW, 3L);
        assertThat(m.totalReady()).isEqualTo(5);
    }

    @Test
    void inFlightCountsDequeuedButUnackedMessages() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        enqueue(Priority.HIGH, Priority.HIGH, Priority.HIGH);
        DeliveredMessage first = service.dequeue("q").orElseThrow();
        service.dequeue("q").orElseThrow();
        service.ack("q", first.id(), first.receipt());

        assertThat(service.metrics("q").inFlight()).isEqualTo(1);
    }

    @Test
    void oldestAgeIsTimeSinceTheOldestReadyMessageWasEnqueued() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        enqueue(Priority.LOW);
        clock.advance(Duration.ofSeconds(4));
        enqueue(Priority.HIGH);
        clock.advance(Duration.ofSeconds(3));

        QueueMetricsSnapshot m = service.metrics("q");

        assertThat(m.oldestAgeSeconds()).isEqualTo(7.0);
        assertThat(m.oldestAgeSecondsByPriority()).containsEntry(Priority.LOW, 7.0)
                .containsEntry(Priority.HIGH, 3.0).containsEntry(Priority.MEDIUM, 0.0);
    }

    @Test
    void oldestAgeIgnoresInFlightAndExpiredMessages() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        service.enqueue("q", "leased", Priority.HIGH, null);
        service.enqueue("q", "expiring", Priority.MEDIUM, Duration.ofSeconds(5));
        clock.advance(Duration.ofSeconds(2));
        service.enqueue("q", "young", Priority.LOW, null);
        service.dequeue("q").orElseThrow(); // "leased" is now in flight
        clock.advance(Duration.ofSeconds(5)); // "expiring" passes its TTL

        assertThat(service.metrics("q").oldestAgeSeconds()).isEqualTo(5.0); // only "young" is ready
    }

    @Test
    void aRedeliveredMessageKeepsItsOriginalEnqueueTime() {
        service.createQueue("q", QueueConfig.of(10L, null, null));
        enqueue(Priority.HIGH);
        service.dequeue("q").orElseThrow();
        clock.advance(Duration.ofSeconds(10)); // lease expires; redelivered on the next drain

        assertThat(service.metrics("q").oldestAgeSeconds()).isEqualTo(10.0);
    }

    @Test
    void oldestAgeIsZeroWhenNothingIsReady() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        clock.advance(Duration.ofSeconds(5));

        assertThat(service.metrics("q").oldestAgeSeconds()).isZero();
    }

    @Test
    void aStarvedLowPriorityGrowsOlderWhileHighStaysYoung() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        enqueue(Priority.LOW);
        for (int second = 0; second < 30; second++) {
            enqueue(Priority.HIGH);
            DeliveredMessage d = service.dequeue("q").orElseThrow();
            assertThat(d.priority()).isEqualTo(Priority.HIGH); // LOW is never served while HIGH is ready
            service.ack("q", d.id(), d.receipt());
            clock.advance(Duration.ofSeconds(1));
        }
        enqueue(Priority.HIGH);

        QueueMetricsSnapshot m = service.metrics("q");

        assertThat(m.oldestAgeSecondsByPriority().get(Priority.LOW)).isEqualTo(30.0);
        assertThat(m.oldestAgeSecondsByPriority().get(Priority.HIGH)).isZero();
        assertThat(m.oldestAgeSeconds()).isEqualTo(30.0);
    }

    @Test
    void eachCounterIncrementsExactlyOncePerEvent() {
        service.createQueue("q", QueueConfig.of(10L, 2, 3));
        MessageId acked = service.enqueue("q", "acked", Priority.HIGH, null);
        service.enqueue("q", "poison", Priority.MEDIUM, null);
        service.enqueue("q", "expires", Priority.LOW, Duration.ofSeconds(1));
        assertThatThrownBy(() -> service.enqueue("q", "rejected", Priority.LOW, null))
                .isInstanceOf(QueueFullException.class);

        DeliveredMessage d = service.dequeue("q").orElseThrow();
        service.ack("q", acked, d.receipt());
        service.dequeue("q").orElseThrow(); // poison, delivery 1
        clock.advance(Duration.ofSeconds(10)); // poison redelivered; "expires" expired
        service.dequeue("q").orElseThrow(); // poison, delivery 2
        clock.advance(Duration.ofSeconds(10)); // poison dead-lettered
        assertThat(service.dequeue("q")).isEmpty();

        QueueMetricsSnapshot m = service.metrics("q");
        assertThat(m.enqueued()).containsEntry(Priority.HIGH, 1L).containsEntry(Priority.MEDIUM, 1L)
                .containsEntry(Priority.LOW, 1L);
        assertThat(m.delivered()).containsEntry(Priority.HIGH, 1L).containsEntry(Priority.MEDIUM, 2L)
                .containsEntry(Priority.LOW, 0L);
        assertThat(m.dequeueEmpty()).isEqualTo(1);
        assertThat(m.acked()).isEqualTo(1);
        assertThat(m.redelivered()).isEqualTo(1);
        assertThat(m.deadLettered()).isEqualTo(1);
        assertThat(m.expired()).containsEntry(Priority.LOW, 1L).containsEntry(Priority.HIGH, 0L);
        assertThat(m.enqueueRejected()).isEqualTo(1);

        QueueMetricsSnapshot dlq = service.metrics("q.dlq");
        assertThat(dlq.enqueued()).containsEntry(Priority.MEDIUM, 1L);
        assertThat(dlq.ready()).containsEntry(Priority.MEDIUM, 1L);
    }

    @Test
    void ratesCoverTheLastSixtyCompleteSeconds() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        for (int second = 0; second < 60; second++) {
            enqueue(Priority.LOW);
            enqueue(Priority.LOW);
            DeliveredMessage d = service.dequeue("q").orElseThrow();
            service.ack("q", d.id(), d.receipt());
            clock.advance(Duration.ofSeconds(1));
        }

        QueueMetricsSnapshot m = service.metrics("q");
        assertThat(m.enqueueRatePerSec()).isEqualTo(2.0);
        assertThat(m.ackRatePerSec()).isEqualTo(1.0);

        clock.advance(Duration.ofSeconds(60));
        assertThat(service.metrics("q").enqueueRatePerSec()).isZero();
    }

    @Test
    void metricsAllCoversEveryQueueIncludingDlqs() {
        service.createQueue("a", QueueConfig.of(null, null, null));
        service.createQueue("b", QueueConfig.of(null, null, null));

        assertThat(service.metricsAll()).extracting(QueueMetricsSnapshot::queue)
                .containsExactlyInAnyOrder("a", "a.dlq", "b", "b.dlq");
        assertThatThrownBy(() -> service.metrics("nope")).isInstanceOf(QueueNotFoundException.class);
    }

    @Test
    void snapshotsTakenDuringConcurrentTrafficAreInternallyConsistent() throws Exception {
        service.createQueue("q", QueueConfig.of(null, null, 100_000));
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> workers = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
            for (int i = 0; i < 2; i++) {
                workers.add(pool.submit(() -> {
                    start.await();
                    for (int n = 0; n < 5_000; n++) {
                        service.enqueue("q", "m", Priority.values()[n % 3], null);
                    }
                    return null;
                }));
                workers.add(pool.submit(() -> {
                    start.await();
                    while (running.get()) {
                        service.dequeue("q").ifPresent(d -> service.ack("q", d.id(), d.receipt()));
                    }
                    return null;
                }));
            }
            start.countDown();
            try {
                for (int s = 0; s < 2_000; s++) {
                    QueueMetricsSnapshot m = service.metrics("q");
                    long enqueued = m.enqueued().values().stream().mapToLong(Long::longValue).sum();
                    long delivered = m.delivered().values().stream().mapToLong(Long::longValue).sum();
                    // Nothing expires here, so every enqueued message is ready, in flight or acked.
                    assertThat(enqueued).isEqualTo(m.totalReady() + m.inFlight() + m.acked());
                    assertThat(delivered).isEqualTo(m.inFlight() + m.acked());
                }
                workers.get(0).get();
                workers.get(2).get();
            } finally {
                running.set(false); // consumers must stop even if an assertion failed, or close() never returns
            }
            for (Future<?> w : workers) {
                w.get();
            }
        }
    }

    private void enqueue(Priority... priorities) {
        for (Priority p : priorities) {
            service.enqueue("q", "m", p, null);
        }
    }
}

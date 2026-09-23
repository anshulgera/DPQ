package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import dpq.core.error.MessageNotFoundException;
import dpq.core.error.StaleReceiptException;
import dpq.core.id.RandomIdGenerator;
import dpq.core.id.RandomReceiptGenerator;
import dpq.core.model.DeliveredMessage;
import dpq.core.model.MessageId;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.model.QueueMetricsSnapshot;
import dpq.core.time.FakeClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

/**
 * Invariant stress test (D13a): concurrent producers and consumers, abandoned deliveries, TTLs, the real reaper
 * and a clock that keeps moving. Every message must end in exactly one terminal state, and no two consumers may
 * ever hold overlapping leases on the same message.
 */
@Tag("stress")
class ConservationStressTest {

    private static final int PRODUCERS = 8;
    private static final int CONSUMERS = 8;
    private static final int PER_PRODUCER = 5_000;
    private static final Duration VISIBILITY = Duration.ofSeconds(1);
    private static final String QUEUE = "q";

    private record Delivery(DeliveredMessage message) {}

    @RepeatedTest(20)
    @Timeout(30) // liveness
    void everyMessageEndsInExactlyOneTerminalState() throws Exception {
        FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
        Set<MessageId> enqueued = ConcurrentHashMap.newKeySet();
        Set<MessageId> acked = ConcurrentHashMap.newKeySet();
        AtomicInteger successfulAcks = new AtomicInteger();
        ConcurrentLinkedQueue<Delivery> deliveries = new ConcurrentLinkedQueue<>();
        AtomicBoolean producersDone = new AtomicBoolean();
        AtomicBoolean stopClock = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);

        try (QueueService service = new QueueService(clock, new RandomIdGenerator(clock, new Random()),
                        new RandomReceiptGenerator(),
                        QueueService.Options.defaults().withReaperInterval(Duration.ofMillis(5)));
                ExecutorService pool = Executors.newFixedThreadPool(PRODUCERS + CONSUMERS + 1)) {
            service.createQueue(QUEUE, QueueConfig.of(VISIBILITY.toSeconds(), 3, 100_000));

            List<Future<?>> producers = new ArrayList<>();
            for (int p = 0; p < PRODUCERS; p++) {
                producers.add(pool.submit(() -> {
                    start.await();
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    for (int i = 0; i < PER_PRODUCER; i++) {
                        Duration ttl = rnd.nextInt(5) == 0 ? Duration.ofSeconds(2) : null;
                        Priority priority = Priority.values()[rnd.nextInt(3)];
                        enqueued.add(service.enqueue(QUEUE, "payload-" + i, priority, ttl));
                    }
                    return null;
                }));
            }
            List<Future<?>> consumers = new ArrayList<>();
            for (int c = 0; c < CONSUMERS; c++) {
                consumers.add(pool.submit(() -> {
                    start.await();
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    while (true) {
                        Optional<DeliveredMessage> d = service.dequeue(QUEUE);
                        if (d.isEmpty()) {
                            if (producersDone.get() && settled(service)) {
                                return null;
                            }
                            Thread.onSpinWait();
                            continue;
                        }
                        deliveries.add(new Delivery(d.get()));
                        if (rnd.nextInt(10) == 0) {
                            continue; // abandoned: the lease must expire and the message be redelivered
                        }
                        try {
                            service.ack(QUEUE, d.get().id(), d.get().receipt());
                            successfulAcks.incrementAndGet();
                            acked.add(d.get().id());
                        } catch (StaleReceiptException | MessageNotFoundException e) {
                            // The lease ended before the ack (the clock kept moving): a legitimate outcome.
                        }
                    }
                }));
            }
            Future<?> clockAdvancer = pool.submit(() -> {
                start.await();
                while (!stopClock.get()) {
                    clock.advance(Duration.ofMillis(1));
                    LockSupport.parkNanos(20_000);
                }
                return null;
            });

            start.countDown();
            for (Future<?> f : producers) {
                f.get(); // rethrows any producer exception
            }
            producersDone.set(true);
            for (Future<?> f : consumers) {
                f.get();
            }
            stopClock.set(true);
            clockAdvancer.get();

            Set<MessageId> deadLettered = drainDlq(service);
            QueueMetricsSnapshot m = service.metrics(QUEUE);

            // Conservation: every enqueued message is acked, dead-lettered or expired, exactly once.
            assertThat(enqueued).hasSize(PRODUCERS * PER_PRODUCER);
            assertThat(successfulAcks.get()).as("no message acked twice").isEqualTo(acked.size());
            assertThat(acked).as("acked and dead-lettered are disjoint").doesNotContainAnyElementsOf(deadLettered);
            assertThat(enqueued).containsAll(acked).containsAll(deadLettered);
            long expired = enqueued.size() - acked.size() - deadLettered.size();

            // Counters equal the ground truth, and nothing is left behind.
            assertThat(sum(m.enqueued())).isEqualTo(enqueued.size());
            assertThat(m.acked()).isEqualTo(acked.size());
            assertThat(m.deadLettered()).isEqualTo(deadLettered.size());
            assertThat(sum(m.expired())).isEqualTo(expired);
            assertThat(sum(m.delivered())).isEqualTo(deliveries.size());
            assertThat(m.totalReady()).isZero();
            assertThat(m.inFlight()).isZero();

            // The run really exercised redelivery, dead-lettering and TTL expiry, not just the happy path.
            assertThat(m.redelivered()).isPositive();
            assertThat(deadLettered).isNotEmpty();
            assertThat(expired).isPositive();

            assertLeasesNeverOverlap(deliveries);
        }
    }

    /** Deliveries of one message, in delivery order, never overlap: each lease starts after the last ended. */
    private static void assertLeasesNeverOverlap(ConcurrentLinkedQueue<Delivery> deliveries) {
        Map<MessageId, List<DeliveredMessage>> byId = deliveries.stream()
                .map(Delivery::message)
                .collect(Collectors.groupingBy(DeliveredMessage::id));
        for (List<DeliveredMessage> leases : byId.values()) {
            leases.sort((a, b) -> Long.compare(a.deliverySeq(), b.deliverySeq()));
            for (int i = 1; i < leases.size(); i++) {
                Instant previousEnd = leases.get(i - 1).visibleUntil();
                Instant start = leases.get(i).visibleUntil().minus(VISIBILITY);
                assertThat(start).as("lease %d of %s", i, leases.get(i).id()).isAfterOrEqualTo(previousEnd);
            }
        }
    }

    private static boolean settled(QueueService service) {
        QueueMetricsSnapshot m = service.metrics(QUEUE);
        return m.totalReady() == 0 && m.inFlight() == 0;
    }

    private static Set<MessageId> drainDlq(QueueService service) {
        Set<MessageId> ids = new HashSet<>();
        for (Optional<DeliveredMessage> d = service.dequeue(QUEUE + ".dlq"); d.isPresent();
                d = service.dequeue(QUEUE + ".dlq")) {
            assertThat(ids.add(d.get().id())).as("dead-lettered once: %s", d.get().id()).isTrue();
            service.ack(QUEUE + ".dlq", d.get().id(), d.get().receipt());
        }
        return ids;
    }

    private static long sum(Map<Priority, Long> counts) {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }
}

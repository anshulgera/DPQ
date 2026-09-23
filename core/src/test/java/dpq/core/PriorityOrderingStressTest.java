package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;

/**
 * Concurrent ordering tests for the "priority inversion" hard case (D13a2), using the partition's delivery
 * sequence as the true delivery order. No lease ever expires here, so "ready" simply means "not yet delivered".
 */
@Tag("stress")
class PriorityOrderingStressTest {

    private static final String QUEUE = "q";
    private static final QueueConfig NO_EXPIRY = QueueConfig.of(12 * 3600L, null, 100_000);

    private record Delivered(long deliverySeq, MessageId id, Priority priority, long startTick) {}

    private record Enqueued(MessageId id, Priority priority, int producer, int producerSeq, long returnedTick) {}

    @RepeatedTest(20)
    @Timeout(30)
    void concurrentConsumersDrainAPreloadedQueueInPriorityThenFifoOrder() throws Exception {
        try (QueueService service = service()) {
            int total = 30_000;
            Map<MessageId, Integer> enqueueOrder = new HashMap<>();
            Random rnd = new Random();
            for (int i = 0; i < total; i++) {
                enqueueOrder.put(service.enqueue(QUEUE, "m", Priority.values()[rnd.nextInt(3)], null), i);
            }

            ConcurrentLinkedQueue<Delivered> delivered = new ConcurrentLinkedQueue<>();
            runConcurrently(16, () -> {
                for (Optional<DeliveredMessage> d = service.dequeue(QUEUE); d.isPresent(); d = service.dequeue(QUEUE)) {
                    delivered.add(new Delivered(d.get().deliverySeq(), d.get().id(), d.get().priority(), 0));
                    service.ack(QUEUE, d.get().id(), d.get().receipt());
                }
            });

            List<Delivered> inOrder = sortedBySeq(delivered);
            assertThat(inOrder).hasSize(total);
            assertThat(inOrder.stream().map(Delivered::id).distinct().count()).isEqualTo(total);
            for (int i = 1; i < inOrder.size(); i++) {
                assertThat(inOrder.get(i).priority()).as("priority never goes up, at delivery %d", i)
                        .isGreaterThanOrEqualTo(inOrder.get(i - 1).priority());
            }
            Map<Priority, Integer> lastOrder = new EnumMap<>(Priority.class);
            for (Delivered d : inOrder) {
                int order = enqueueOrder.get(d.id());
                assertThat(order).as("FIFO within %s", d.priority())
                        .isGreaterThan(lastOrder.getOrDefault(d.priority(), -1));
                lastOrder.put(d.priority(), order);
            }
        }
    }

    @RepeatedTest(20)
    @Timeout(30)
    void noLowerPriorityIsDeliveredWhileAnEarlierEnqueuedHigherPriorityIsReady() throws Exception {
        int producers = 8;
        int consumers = 8;
        int perProducer = 5_000;
        AtomicLong tick = new AtomicLong();
        ConcurrentLinkedQueue<Enqueued> enqueued = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Delivered> delivered = new ConcurrentLinkedQueue<>();
        AtomicBoolean producersDone = new AtomicBoolean();
        CountDownLatch start = new CountDownLatch(1);

        try (QueueService service = service();
                ExecutorService pool = Executors.newFixedThreadPool(producers + consumers)) {
            List<Future<?>> producerFutures = new ArrayList<>();
            for (int p = 0; p < producers; p++) {
                int producer = p;
                producerFutures.add(pool.submit(() -> {
                    start.await();
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();
                    for (int i = 0; i < perProducer; i++) {
                        Priority priority = Priority.values()[rnd.nextInt(3)];
                        MessageId id = service.enqueue(QUEUE, "m", priority, null);
                        // Taken after enqueue returned, so the message was in the queue before this tick.
                        enqueued.add(new Enqueued(id, priority, producer, i, tick.incrementAndGet()));
                    }
                    return null;
                }));
            }
            List<Future<?>> consumerFutures = new ArrayList<>();
            for (int c = 0; c < consumers; c++) {
                consumerFutures.add(pool.submit(() -> {
                    start.await();
                    while (true) {
                        // Taken before dequeue starts, so anything enqueued before this tick was visible to it.
                        long startTick = tick.incrementAndGet();
                        Optional<DeliveredMessage> d = service.dequeue(QUEUE);
                        if (d.isEmpty()) {
                            if (producersDone.get() && service.metrics(QUEUE).totalReady() == 0) {
                                return null;
                            }
                            Thread.onSpinWait();
                            continue;
                        }
                        delivered.add(new Delivered(d.get().deliverySeq(), d.get().id(), d.get().priority(), startTick));
                        service.ack(QUEUE, d.get().id(), d.get().receipt());
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : producerFutures) {
                f.get();
            }
            producersDone.set(true);
            for (Future<?> f : consumerFutures) {
                f.get();
            }
        }

        assertThat(delivered).hasSize(producers * perProducer);
        Map<MessageId, Long> seqOf = new HashMap<>();
        delivered.forEach(d -> seqOf.put(d.id(), d.deliverySeq()));
        assertNoPriorityInversion(enqueued, delivered, seqOf);
        assertPerProducerFifoWithinPriority(enqueued, seqOf);
    }

    /**
     * For each delivery D and each priority H above D's: among H messages whose enqueue returned before D's dequeue
     * started, none may have been delivered after D, since it was ready when D was chosen. Checked in O(n log n)
     * with a prefix maximum of delivery seqs over H messages sorted by enqueue tick.
     */
    private static void assertNoPriorityInversion(ConcurrentLinkedQueue<Enqueued> enqueued,
            ConcurrentLinkedQueue<Delivered> delivered, Map<MessageId, Long> seqOf) {
        for (Priority higher : List.of(Priority.HIGH, Priority.MEDIUM)) {
            List<Enqueued> ofHigher = enqueued.stream().filter(e -> e.priority() == higher)
                    .sorted(Comparator.comparingLong(Enqueued::returnedTick)).toList();
            long[] ticks = ofHigher.stream().mapToLong(Enqueued::returnedTick).toArray();
            long[] maxSeqUpTo = new long[ofHigher.size()];
            long max = Long.MIN_VALUE;
            for (int i = 0; i < ofHigher.size(); i++) {
                max = Math.max(max, seqOf.get(ofHigher.get(i).id()));
                maxSeqUpTo[i] = max;
            }
            for (Delivered d : delivered) {
                if (d.priority().compareTo(higher) <= 0) {
                    continue; // only deliveries of a lower priority than `higher`
                }
                int before = insertionPoint(ticks, d.startTick()); // H messages enqueued before D started
                if (before > 0) {
                    assertThat(maxSeqUpTo[before - 1])
                            .as("%s delivered at seq %d while an earlier %s was ready", d.priority(),
                                    d.deliverySeq(), higher)
                            .isLessThan(d.deliverySeq());
                }
            }
        }
    }

    private static void assertPerProducerFifoWithinPriority(ConcurrentLinkedQueue<Enqueued> enqueued,
            Map<MessageId, Long> seqOf) {
        Map<String, List<Enqueued>> streams = new ConcurrentHashMap<>();
        enqueued.forEach(e -> streams.computeIfAbsent(e.producer() + "/" + e.priority(), k -> new ArrayList<>())
                .add(e));
        for (List<Enqueued> stream : streams.values()) {
            stream.sort(Comparator.comparingInt(Enqueued::producerSeq));
            for (int i = 1; i < stream.size(); i++) {
                assertThat(seqOf.get(stream.get(i).id())).isGreaterThan(seqOf.get(stream.get(i - 1).id()));
            }
        }
    }

    /** Number of ticks strictly below {@code tick}. */
    private static int insertionPoint(long[] ticks, long tick) {
        int i = Arrays.binarySearch(ticks, tick);
        return i >= 0 ? i : -i - 1; // ticks are unique, so a hit can't happen for another thread's tick
    }

    private static List<Delivered> sortedBySeq(ConcurrentLinkedQueue<Delivered> delivered) {
        return delivered.stream().sorted(Comparator.comparingLong(Delivered::deliverySeq)).toList();
    }

    private static QueueService service() {
        FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
        QueueService service = new QueueService(clock, new RandomIdGenerator(clock, new Random()),
                new RandomReceiptGenerator(), QueueService.Options.defaults().withoutReaper());
        service.createQueue(QUEUE, NO_EXPIRY);
        return service;
    }

    private static void runConcurrently(int threads, Runnable body) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    body.run();
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }
    }
}

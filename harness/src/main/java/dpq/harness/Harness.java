package dpq.harness;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.HdrHistogram.ConcurrentHistogram;
import org.HdrHistogram.Histogram;

/**
 * Producer/consumer load harness over HTTP (D14b). Producers enqueue random priorities; consumers dequeue, ack,
 * and "crash" on a fraction of deliveries (no ack) so the visibility timeout and redelivery are exercised. At the
 * end it checks that every enqueued message was acked or dead-lettered exactly once, and reports throughput and
 * p50/p95/p99 latency against the p95 < 100ms target.
 */
public final class Harness {

    static final int VISIBILITY_TIMEOUT_SECONDS = 2;
    private static final long DRAIN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);
    private static final long MAX_LATENCY_MICROS = TimeUnit.SECONDS.toMicros(60);
    private static final String[] PRIORITIES = {"HIGH", "MEDIUM", "LOW"};

    private final HarnessConfig config;
    private final DpqClient client;
    private final Set<String> enqueued = ConcurrentHashMap.newKeySet();
    private final Set<String> acked = ConcurrentHashMap.newKeySet();
    private final AtomicLong duplicateAcks = new AtomicLong();
    private final AtomicLong deliveries = new AtomicLong();
    private final AtomicLong crashed = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicBoolean producersDone = new AtomicBoolean();
    private final AtomicLong drainDeadline = new AtomicLong(Long.MAX_VALUE);
    private final Histogram enqueueLatency = new ConcurrentHistogram(MAX_LATENCY_MICROS, 3);
    private final Histogram dequeueLatency = new ConcurrentHistogram(MAX_LATENCY_MICROS, 3);

    Harness(HarnessConfig config) {
        this.config = config;
        this.client = new DpqClient(config.baseUrl());
    }

    public static void main(String[] args) throws Exception {
        HarnessConfig config;
        try {
            config = HarnessConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(2);
            return;
        }
        Report report = new Harness(config).run();
        report.print(System.out);
        System.exit(report.passed() ? 0 : 1);
    }

    Report run() throws Exception {
        client.createQueue(config.queue(), VISIBILITY_TIMEOUT_SECONDS);
        long start = System.nanoTime();
        long producedAt;
        try (ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Future<?>> consumers = new ArrayList<>();
            for (int c = 0; c < config.consumers(); c++) {
                consumers.add(threads.submit(this::consume));
            }
            List<Future<?>> producers = config.openLoop() ? produceOpenLoop(threads) : produceClosedLoop(threads);
            for (Future<?> f : producers) {
                f.get();
            }
            producedAt = System.nanoTime();
            producersDone.set(true);
            drainDeadline.set(System.nanoTime() + DRAIN_TIMEOUT_NANOS);
            for (Future<?> f : consumers) {
                f.get();
            }
        }
        double elapsedSeconds = (System.nanoTime() - start) / 1e9;
        Set<String> deadLettered = drainDlq();
        return new Report(config, (producedAt - start) / 1e9, elapsedSeconds, Set.copyOf(enqueued),
                Set.copyOf(acked), deadLettered, duplicateAcks.get(), deliveries.get(), crashed.get(), errors.get(),
                enqueueLatency, dequeueLatency);
    }

    private List<Future<?>> produceClosedLoop(ExecutorService threads) {
        List<Future<?>> producers = new ArrayList<>();
        int total = config.totalMessages();
        for (int p = 0; p < config.producers(); p++) {
            int count = total / config.producers() + (p < total % config.producers() ? 1 : 0);
            producers.add(threads.submit(() -> {
                for (int i = 0; i < count; i++) {
                    enqueueOne(System.nanoTime());
                }
            }));
        }
        return producers;
    }

    /**
     * Schedules enqueues at a fixed arrival rate. Each request's latency runs from its scheduled start, so time
     * spent waiting behind a slow server counts (no coordinated omission).
     */
    private List<Future<?>> produceOpenLoop(ExecutorService threads) {
        List<Future<?>> requests = new ArrayList<>();
        long interval = TimeUnit.SECONDS.toNanos(1) / config.rate();
        long first = System.nanoTime();
        for (int i = 0; i < config.totalMessages(); i++) {
            long scheduled = first + i * interval;
            LockSupport.parkNanos(scheduled - System.nanoTime());
            requests.add(threads.submit(() -> enqueueOne(scheduled)));
        }
        return requests;
    }

    private void enqueueOne(long scheduledNanos) {
        try {
            String priority = PRIORITIES[ThreadLocalRandom.current().nextInt(PRIORITIES.length)];
            enqueued.add(client.enqueue(config.queue(), "payload", priority));
            record(enqueueLatency, scheduledNanos);
        } catch (Exception e) {
            errors.incrementAndGet();
        }
    }

    private void consume() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (System.nanoTime() < drainDeadline.get()) {
            try {
                long start = System.nanoTime();
                Optional<DpqClient.Delivery> d = client.dequeue(config.queue());
                record(dequeueLatency, start);
                if (d.isEmpty()) {
                    if (producersDone.get()
                            && acked.size() + client.deadLettered(config.queue()) >= enqueued.size()) {
                        return;
                    }
                    Thread.sleep(1 + rnd.nextInt(10)); // back off with jitter on an empty queue (D10)
                    continue;
                }
                deliveries.incrementAndGet();
                if (rnd.nextDouble() < config.crashRate()) {
                    crashed.incrementAndGet(); // never ack: the lease expires and the message is redelivered
                    continue;
                }
                if (client.ack(config.queue(), d.get()) && !acked.add(d.get().messageId())) {
                    duplicateAcks.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                errors.incrementAndGet();
            }
        }
    }

    private Set<String> drainDlq() throws Exception {
        Set<String> ids = new HashSet<>();
        String dlq = config.queue() + ".dlq";
        for (Optional<DpqClient.Delivery> d = client.dequeue(dlq); d.isPresent(); d = client.dequeue(dlq)) {
            if (!ids.add(d.get().messageId())) {
                duplicateAcks.incrementAndGet();
            }
            client.ack(dlq, d.get());
        }
        return ids;
    }

    private static void record(Histogram histogram, long startNanos) {
        long micros = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - startNanos);
        histogram.recordValue(Math.min(Math.max(micros, 0), MAX_LATENCY_MICROS));
    }
}

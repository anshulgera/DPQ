package dpq.core;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The queue engine's entry point (D2, D5): a registry of queues, each with one partition (D5b) and a companion
 * dead-letter queue {@code {name}.dlq} (D9a). Thread-safe; close it to stop the reaper.
 */
public final class QueueService implements AutoCloseable {

    /**
     * Engine settings.
     *
     * @param maxQueues      maximum user queues per node; DLQs don't count (D11b)
     * @param drainLimit     expired entries each operation drains first (D6)
     * @param reaperInterval how often the reaper drains every partition, or {@code null} for no reaper
     */
    public record Options(int maxQueues, int drainLimit, Duration reaperInterval) {

        public static Options defaults() {
            return new Options(1000, Partition.DEFAULT_DRAIN_LIMIT, Duration.ofMillis(100));
        }

        public Options withoutReaper() {
            return new Options(maxQueues, drainLimit, null);
        }

        public Options withReaperInterval(Duration interval) {
            return new Options(maxQueues, drainLimit, interval);
        }

        public Options withMaxQueues(int max) {
            return new Options(max, drainLimit, reaperInterval);
        }
    }

    private record Queue(QueueDescription description, List<Partition> partitions) {}

    static final String DLQ_SUFFIX = ".dlq";
    private static final QueueConfig DLQ_CONFIG = QueueConfig.of(null, null, null);

    private final Clock clock;
    private final IdGenerator ids;
    private final ReceiptGenerator receipts;
    private final Options options;
    private final ConcurrentHashMap<String, Queue> queues = new ConcurrentHashMap<>();
    private final AtomicInteger userQueues = new AtomicInteger();
    private final Reaper reaper;

    public QueueService(Clock clock, IdGenerator ids, ReceiptGenerator receipts, Options options) {
        this.clock = clock;
        this.ids = ids;
        this.receipts = receipts;
        this.options = options;
        this.reaper = options.reaperInterval() == null ? null : new Reaper(this::sweep, options.reaperInterval());
    }

    /**
     * Creates a queue and its DLQ. Idempotent: returns {@code true} if it was created, {@code false} if it
     * already existed with the same resolved config, and throws {@link QueueAlreadyExistsException} if the config
     * differs (D11c).
     */
    public boolean createQueue(String name, QueueConfig config) {
        QueueNames.requireValidUserName(name);
        Objects.requireNonNull(config, "config");
        Queue existing = queues.get(name);
        if (existing != null) {
            return requireSameConfig(existing, config);
        }
        // Reserve a slot first so concurrent creates of different queues can't pass the cap (D18f).
        if (userQueues.incrementAndGet() > options.maxQueues()) {
            userQueues.decrementAndGet();
            throw new QueueLimitExceededException(options.maxQueues());
        }
        // The DLQ is registered before the queue is published, so a drain never finds it missing (D9a). Its
        // config is fixed, so concurrent creators all register the same DLQ.
        Queue dlq = queues.computeIfAbsent(name + DLQ_SUFFIX, n -> newQueue(n, DLQ_CONFIG, null));
        boolean[] created = {false};
        Queue queue = queues.computeIfAbsent(name, n -> {
            created[0] = true;
            return newQueue(n, config, dlq);
        });
        if (!created[0]) {
            userQueues.decrementAndGet();
            return requireSameConfig(queue, config);
        }
        return true;
    }

    public QueueDescription getQueue(String name) {
        return require(name).description();
    }

    public Set<String> queueNames() {
        return Set.copyOf(queues.keySet());
    }

    /** Enqueues into a user queue; {@code ttl} may be {@code null}. Producers can't enqueue into a DLQ (D9a). */
    public MessageId enqueue(String queue, String payload, Priority priority, Duration ttl) {
        Queue q = require(queue);
        if (q.description().deadLetterQueue()) {
            throw new ValidationException("cannot enqueue into a dead-letter queue: " + queue);
        }
        return q.partitions().get(0).enqueue(payload, priority, ttl);
    }

    public Optional<DeliveredMessage> dequeue(String queue) {
        return require(queue).partitions().get(0).dequeue();
    }

    /** Acks on the partition named by the ID's prefix (D11a). */
    public void ack(String queue, MessageId id, ReceiptHandle receipt) {
        Queue q = require(queue);
        if (id.partition() >= q.partitions().size()) {
            throw new MessageNotFoundException(id);
        }
        q.partitions().get(id.partition()).ack(id, receipt);
    }

    public QueueMetricsSnapshot metrics(String queue) {
        throw new UnsupportedOperationException("not implemented");
    }

    public java.util.List<QueueMetricsSnapshot> metricsAll() {
        throw new UnsupportedOperationException("not implemented");
    }

    /** Fully drains every partition; the reaper's task (D6). */
    void sweep() {
        for (Queue queue : queues.values()) {
            for (Partition partition : queue.partitions()) {
                partition.drainExpired(Integer.MAX_VALUE);
            }
        }
    }

    Partition partition(String queue) {
        return require(queue).partitions().get(0);
    }

    @Override
    public void close() {
        if (reaper != null) {
            reaper.close();
        }
    }

    private Queue newQueue(String name, QueueConfig config, Queue dlq) {
        boolean isDlq = dlq == null;
        // Lock order source → DLQ: the sink runs under the source partition's lock and takes the DLQ's (D9a).
        DeadLetterSink sink = isDlq
                ? (message, info) -> {
                    throw new IllegalStateException("a dead-letter queue has no dead-letter queue");
                }
                : (message, info) -> dlq.partitions().get(message.id().partition()).acceptDeadLetter(message, info);
        Partition partition = new Partition(name, 0, config, isDlq, clock, ids, receipts,
                new StrictPriorityPolicy(), sink, options.drainLimit());
        return new Queue(new QueueDescription(name, config, isDlq), List.of(partition));
    }

    private Queue require(String name) {
        Queue queue = name == null ? null : queues.get(name);
        if (queue == null) {
            throw new QueueNotFoundException(name);
        }
        return queue;
    }

    private static boolean requireSameConfig(Queue queue, QueueConfig config) {
        if (!queue.description().config().equals(config)) {
            throw new QueueAlreadyExistsException(queue.description().name());
        }
        return false;
    }
}

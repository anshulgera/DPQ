package dpq.core;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One partition of a queue (D4, D5, D5b). A single {@link ReentrantLock} guards all of its mutable state, so
 * the lanes, the message index, the lease map and the deadline set always change together.
 *
 * <p>Every operation first drains up to {@code drainLimit} expired leases, so correctness never depends on a
 * timer; the reaper calls {@link #drainExpired} with no limit to keep idle partitions current (D6).
 *
 * <p>Lock order: a source partition calls its {@link DeadLetterSink} while holding its own lock, and the sink
 * takes the DLQ partition's lock. A DLQ never has a DLQ of its own, so the order is acyclic (D9a).
 */
final class Partition {

    static final int MAX_PAYLOAD_BYTES = 256 * 1024;
    static final int DEFAULT_DRAIN_LIMIT = 256;

    /** A monotonic deadline; {@code seq} (the message's, unique per partition) breaks ties. */
    private record Deadline(long at, long seq, MessageId id) {}

    private record Lease(ReceiptHandle receipt, Deadline deadline) {}

    private final String queueName;
    private final int index;
    private final QueueConfig config;
    private final Clock clock;
    private final IdGenerator ids;
    private final ReceiptGenerator receipts;
    private final SelectionPolicy policy;
    private final DeadLetterSink deadLetters;
    private final int drainLimit;

    private final ReentrantLock lock = new ReentrantLock();

    // Guarded by lock.
    private final EnumMap<Priority, Lane> lanes = new EnumMap<>(Priority.class);
    private final Map<MessageId, Message> messages = new HashMap<>(); // every live message, ready or in flight
    private final Map<MessageId, Lease> inFlight = new HashMap<>();
    // Holds exactly one entry per lease; removed on ack or expiry, so it never holds stale deadlines (D4).
    private final TreeSet<Deadline> visibilityDeadlines =
            new TreeSet<>(Comparator.comparingLong(Deadline::at).thenComparingLong(Deadline::seq));
    private long nextSeq;
    private long nextDeliverySeq;

    Partition(String queueName, int index, QueueConfig config, Clock clock, IdGenerator ids,
            ReceiptGenerator receipts, SelectionPolicy policy, DeadLetterSink deadLetters, int drainLimit) {
        this.queueName = queueName;
        this.index = index;
        this.config = config;
        this.clock = clock;
        this.ids = ids;
        this.receipts = receipts;
        this.policy = policy;
        this.deadLetters = deadLetters;
        this.drainLimit = drainLimit;
        for (Priority p : Priority.values()) {
            lanes.put(p, new Lane());
        }
    }

    MessageId enqueue(String payload, Priority priority, java.time.Duration ttl) {
        return enqueue(payload, priority);
    }

    int expiredCount() {
        return 0;
    }

    int ttlDeadlineCount() {
        return 0;
    }

    MessageId enqueue(String payload, Priority priority) {
        validate(payload, priority);
        lock.lock();
        try {
            drainLocked(drainLimit);
            if (messages.size() >= config.maxDepth()) {
                throw new QueueFullException(queueName, config.maxDepth());
            }
            MessageId id = ids.next(index);
            long seq = nextSeq++;
            messages.put(id, new Message(id, payload, priority, seq, clock.monotonicMillis(), clock.wallTime(),
                    Message.NO_EXPIRY, 0, null));
            lanes.get(priority).offer(new Lane.Entry(seq, id));
            return id;
        } finally {
            lock.unlock();
        }
    }

    Optional<DeliveredMessage> dequeue() {
        lock.lock();
        try {
            drainLocked(drainLimit);
            Optional<Lane> lane = policy.select(lanes);
            if (lane.isEmpty()) {
                return Optional.empty();
            }
            Lane.Entry entry = lane.get().poll().orElseThrow();
            Message message = messages.get(entry.id()).delivered();
            messages.put(message.id(), message);

            ReceiptHandle receipt = receipts.next();
            long timeoutMillis = config.visibilityTimeout().toMillis();
            Deadline deadline = new Deadline(clock.monotonicMillis() + timeoutMillis, message.seq(), message.id());
            visibilityDeadlines.add(deadline);
            inFlight.put(message.id(), new Lease(receipt, deadline));
            return Optional.of(new DeliveredMessage(message.id(), receipt, message.payload(), message.priority(),
                    message.deliveryCount(), message.enqueuedAt(), clock.wallTime().plusMillis(timeoutMillis),
                    nextDeliverySeq++, message.deadLetter()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes an in-flight message. Throws {@link MessageNotFoundException} if it is gone (or never existed) and
     * {@link StaleReceiptException} if the receipt isn't its current lease's, or the lease deadline has passed
     * whether or not a drain has processed it yet (D8a, D8d).
     */
    void ack(MessageId id, ReceiptHandle receipt) {
        lock.lock();
        try {
            drainLocked(drainLimit);
            if (!messages.containsKey(id)) {
                throw new MessageNotFoundException(id);
            }
            Lease lease = inFlight.get(id);
            if (lease == null
                    || !lease.receipt().equals(receipt)
                    || clock.monotonicMillis() >= lease.deadline().at()) {
                throw new StaleReceiptException(id);
            }
            inFlight.remove(id);
            visibilityDeadlines.remove(lease.deadline());
            messages.remove(id);
        } finally {
            lock.unlock();
        }
    }

    /** Processes up to {@code limit} expired leases; returns how many it processed. */
    int drainExpired(int limit) {
        lock.lock();
        try {
            return drainLocked(limit);
        } finally {
            lock.unlock();
        }
    }

    int visibilityDeadlineCount() {
        lock.lock();
        try {
            return visibilityDeadlines.size();
        } finally {
            lock.unlock();
        }
    }

    int readyCount(Priority priority) {
        lock.lock();
        try {
            return lanes.get(priority).size();
        } finally {
            lock.unlock();
        }
    }

    int inFlightCount() {
        lock.lock();
        try {
            return inFlight.size();
        } finally {
            lock.unlock();
        }
    }

    private int drainLocked(int limit) {
        long now = clock.monotonicMillis();
        int drained = 0;
        while (drained < limit && !visibilityDeadlines.isEmpty() && visibilityDeadlines.first().at() <= now) {
            expireLease(visibilityDeadlines.pollFirst().id());
            drained++;
        }
        return drained;
    }

    /** Applies the D8d lease-expiry rows: dead-letter once deliveries are used up, else redeliver in place. */
    private void expireLease(MessageId id) {
        inFlight.remove(id);
        Message message = messages.get(id);
        if (message.deliveryCount() >= config.maxDeliveries()) {
            messages.remove(id);
            deadLetters.deadLetter(message, new DeadLetterInfo(
                    queueName, message.deliveryCount(), DeadLetterReason.MAX_DELIVERIES, clock.wallTime()));
        } else {
            lanes.get(message.priority()).offerRetry(new Lane.Entry(message.seq(), id));
        }
    }

    private static void validate(String payload, Priority priority) {
        if (payload == null) {
            throw new ValidationException("payload is required");
        }
        if (priority == null) {
            throw new ValidationException("priority is required");
        }
        // Cheap bound first: UTF-8 uses at most 3 bytes per UTF-16 char.
        if (payload.length() * 3L > MAX_PAYLOAD_BYTES
                && payload.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES) {
            throw new ValidationException("payload exceeds " + MAX_PAYLOAD_BYTES + " bytes (UTF-8)");
        }
    }
}

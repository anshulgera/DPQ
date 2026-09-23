package dpq.core;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One partition of a queue (D4, D5, D5b). A single {@link ReentrantLock} guards all of its mutable state, so
 * the lanes, the message index and the lease map always change together.
 */
final class Partition {

    static final int MAX_PAYLOAD_BYTES = 256 * 1024;

    private record Lease(ReceiptHandle receipt, long deadlineMono) {}

    private final String queueName;
    private final int index;
    private final QueueConfig config;
    private final Clock clock;
    private final IdGenerator ids;
    private final ReceiptGenerator receipts;
    private final SelectionPolicy policy;

    private final ReentrantLock lock = new ReentrantLock();

    // Guarded by lock.
    private final EnumMap<Priority, Lane> lanes = new EnumMap<>(Priority.class);
    private final Map<MessageId, Message> messages = new HashMap<>(); // every live message, ready or in flight
    private final Map<MessageId, Lease> inFlight = new HashMap<>();
    private long nextSeq;
    private long nextDeliverySeq;

    Partition(String queueName, int index, QueueConfig config, Clock clock, IdGenerator ids,
            ReceiptGenerator receipts, SelectionPolicy policy) {
        this.queueName = queueName;
        this.index = index;
        this.config = config;
        this.clock = clock;
        this.ids = ids;
        this.receipts = receipts;
        this.policy = policy;
        for (Priority p : Priority.values()) {
            lanes.put(p, new Lane());
        }
    }

    MessageId enqueue(String payload, Priority priority) {
        validate(payload, priority);
        lock.lock();
        try {
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
            Optional<Lane> lane = policy.select(lanes);
            if (lane.isEmpty()) {
                return Optional.empty();
            }
            Lane.Entry entry = lane.get().poll().orElseThrow();
            Message message = messages.get(entry.id()).delivered();
            messages.put(message.id(), message);

            ReceiptHandle receipt = receipts.next();
            long timeoutMillis = config.visibilityTimeout().toMillis();
            inFlight.put(message.id(), new Lease(receipt, clock.monotonicMillis() + timeoutMillis));
            return Optional.of(new DeliveredMessage(message.id(), receipt, message.payload(), message.priority(),
                    message.deliveryCount(), message.enqueuedAt(), clock.wallTime().plusMillis(timeoutMillis),
                    nextDeliverySeq++, message.deadLetter()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes an in-flight message. Throws {@link MessageNotFoundException} if it is gone (or never existed) and
     * {@link StaleReceiptException} if the receipt isn't its current lease's (D8a, D8d).
     */
    void ack(MessageId id, ReceiptHandle receipt) {
        lock.lock();
        try {
            if (!messages.containsKey(id)) {
                throw new MessageNotFoundException(id);
            }
            Lease lease = inFlight.get(id);
            if (lease == null || !lease.receipt().equals(receipt)) {
                throw new StaleReceiptException(id);
            }
            inFlight.remove(id);
            messages.remove(id);
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

package dpq.core;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One partition of a queue (D4, D5, D5b). A single {@link ReentrantLock} guards all of its mutable state, so
 * the lanes, the message index, the lease map and the deadline sets always change together.
 *
 * <p>Every operation first drains up to {@code drainLimit} expired leases and TTLs, so correctness never
 * depends on a timer; the reaper calls {@link #drainExpired} with no limit to keep idle partitions current (D6).
 *
 * <p>Lock order: a source partition calls its {@link DeadLetterSink} while holding its own lock, and the sink
 * takes the DLQ partition's lock. A DLQ never has a DLQ of its own, so the order is acyclic (D9a).
 *
 * <p>A dead-letter partition ({@code deadLetterQueue}, D18b) has no depth or delivery limit, so it never drops
 * or forwards a message, and only {@link #acceptDeadLetter} adds to it.
 */
final class Partition {

    static final int MAX_PAYLOAD_BYTES = 256 * 1024;
    static final int DEFAULT_DRAIN_LIMIT = 256;
    static final Duration MIN_TTL = Duration.ofSeconds(1);
    static final Duration MAX_TTL = Duration.ofDays(14);

    /** A monotonic deadline; {@code seq} (the message's, unique per partition) breaks ties. */
    private record Deadline(long at, long seq, MessageId id) {}

    private record Lease(ReceiptHandle receipt, Deadline deadline) {}

    private static final Comparator<Deadline> DEADLINE_ORDER =
            Comparator.comparingLong(Deadline::at).thenComparingLong(Deadline::seq);

    private final String queueName;
    private final int index;
    private final QueueConfig config;
    private final boolean deadLetterQueue;
    private final int maxDepth;
    private final int maxDeliveries;
    private final Clock clock;
    private final IdGenerator ids;
    private final ReceiptGenerator receipts;
    private final SelectionPolicy policy;
    private final DeadLetterSink deadLetters;
    private final int drainLimit;

    private final ReentrantLock lock = new ReentrantLock();

    // Guarded by lock.
    private final EnumMap<Priority, Lane> lanes = new EnumMap<>(Priority.class);
    // Every live message, ready or in flight, in arrival order (a replaced value keeps its place).
    private final Map<MessageId, Message> messages = new LinkedHashMap<>();
    private final Map<MessageId, Lane.Entry> ready = new HashMap<>(); // lane entry of each ready message
    private final Map<MessageId, Lease> inFlight = new HashMap<>();
    // Only live deadlines (D4): a lease's entry is removed on ack or expiry; a TTL entry exists only while the
    // message is ready, so it is removed on dequeue and re-added on redelivery (D9c re-checks TTL at expiry).
    private final TreeSet<Deadline> visibilityDeadlines = new TreeSet<>(DEADLINE_ORDER);
    private final TreeSet<Deadline> ttlDeadlines = new TreeSet<>(DEADLINE_ORDER);
    private long nextSeq;
    private long nextDeliverySeq;
    private final QueueCounters counters = new QueueCounters();

    Partition(String queueName, int index, QueueConfig config, boolean deadLetterQueue, Clock clock, IdGenerator ids,
            ReceiptGenerator receipts, SelectionPolicy policy, DeadLetterSink deadLetters, int drainLimit) {
        this.queueName = queueName;
        this.index = index;
        this.config = config;
        this.deadLetterQueue = deadLetterQueue;
        this.maxDepth = deadLetterQueue ? Integer.MAX_VALUE : config.maxDepth();
        this.maxDeliveries = deadLetterQueue ? Integer.MAX_VALUE : config.maxDeliveries();
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

    MessageId enqueue(String payload, Priority priority) {
        return enqueue(payload, priority, null);
    }

    /** Adds a message; {@code ttl} may be {@code null} for a message that never expires. */
    MessageId enqueue(String payload, Priority priority, Duration ttl) {
        validate(payload, priority, ttl);
        lock.lock();
        try {
            long now = clock.monotonicMillis();
            drainLocked(drainLimit, now);
            if (messages.size() >= maxDepth) {
                counters.enqueueRejected++;
                throw new QueueFullException(queueName, maxDepth);
            }
            MessageId id = ids.next(index);
            long expiresAt = ttl == null ? Message.NO_EXPIRY : now + ttl.toMillis();
            Message message =
                    new Message(id, payload, priority, nextSeq++, now, clock.wallTimeAt(now), expiresAt, 0, null);
            messages.put(id, message);
            makeReady(message, false);
            counters.enqueued(priority, now);
            return id;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds a message dead-lettered by the source queue (D9a, D18c): same ID, payload and priority; no TTL; a
     * fresh delivery count; {@code enqueuedAt} is the dead-letter time. Called under the source partition's lock.
     */
    void acceptDeadLetter(Message source, DeadLetterInfo info, long deadLetteredAtMono) {
        if (!deadLetterQueue) {
            throw new IllegalStateException(queueName + " is not a dead-letter queue");
        }
        lock.lock();
        try {
            Message message = new Message(source.id(), source.payload(), source.priority(), nextSeq++,
                    deadLetteredAtMono, info.deadLetteredAt(), Message.NO_EXPIRY, 0, info);
            messages.put(message.id(), message);
            makeReady(message, false);
            counters.enqueued(message.priority(), deadLetteredAtMono);
        } finally {
            lock.unlock();
        }
    }

    Optional<DeliveredMessage> dequeue() {
        lock.lock();
        try {
            long now = clock.monotonicMillis();
            drainLocked(drainLimit, now);
            Optional<Lane> lane = policy.select(lanes);
            if (lane.isEmpty()) {
                counters.dequeueEmpty++;
                return Optional.empty();
            }
            Lane.Entry entry = lane.get().poll().orElseThrow();
            ready.remove(entry.id());
            Message message = messages.get(entry.id()).delivered();
            messages.put(message.id(), message);
            if (message.expiresAtMono() != Message.NO_EXPIRY) {
                ttlDeadlines.remove(ttlDeadline(message));
            }

            ReceiptHandle receipt = receipts.next();
            long timeoutMillis = config.visibilityTimeout().toMillis();
            Deadline deadline = new Deadline(now + timeoutMillis, message.seq(), message.id());
            visibilityDeadlines.add(deadline);
            inFlight.put(message.id(), new Lease(receipt, deadline));
            counters.delivered[message.priority().ordinal()]++;
            return Optional.of(new DeliveredMessage(message.id(), receipt, message.payload(), message.priority(),
                    message.deliveryCount(), message.enqueuedAt(), clock.wallTimeAt(deadline.at()),
                    nextDeliverySeq++, message.deadLetter()));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes an in-flight message. Throws {@link MessageNotFoundException} if it is gone (or never existed) and
     * {@link StaleReceiptException} if the receipt isn't its current lease's, or the lease deadline has passed
     * whether or not a drain has processed it yet (D8a, D8d). A passed TTL doesn't matter here: the lease wins
     * (D9c).
     */
    void ack(MessageId id, ReceiptHandle receipt) {
        lock.lock();
        try {
            long now = clock.monotonicMillis();
            drainLocked(drainLimit, now);
            if (!messages.containsKey(id)) {
                throw new MessageNotFoundException(id);
            }
            Lease lease = inFlight.get(id);
            if (lease == null
                    || !lease.receipt().equals(receipt)
                    || now >= lease.deadline().at()) {
                throw new StaleReceiptException(id);
            }
            inFlight.remove(id);
            visibilityDeadlines.remove(lease.deadline());
            messages.remove(id);
            counters.acked(now);
        } finally {
            lock.unlock();
        }
    }

    /** Processes up to {@code limit} expired leases and TTLs, earliest first; returns how many it processed. */
    int drainExpired(int limit) {
        lock.lock();
        try {
            return drainLocked(limit, clock.monotonicMillis());
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

    /**
     * Reads a consistent snapshot of this partition (D12a). Like any operation, it first drains up to the drain
     * limit, so expired messages don't count as ready or old.
     */
    QueueMetricsSnapshot snapshot() {
        lock.lock();
        try {
            long now = clock.monotonicMillis();
            drainLocked(drainLimit, now);
            EnumMap<Priority, Long> readyCounts = new EnumMap<>(Priority.class);
            EnumMap<Priority, Double> oldestAge = new EnumMap<>(Priority.class);
            for (Priority p : Priority.values()) {
                Lane lane = lanes.get(p);
                readyCounts.put(p, (long) lane.size());
                // O(1): the lane's oldest live entry; a redelivered message keeps its original enqueue time.
                oldestAge.put(p, lane.peekOldest()
                        .map(e -> (now - messages.get(e.id()).enqueuedAtMono()) / 1000.0)
                        .orElse(0.0));
            }
            return new QueueMetricsSnapshot(queueName, Collections.unmodifiableMap(readyCounts), inFlight.size(),
                    Collections.unmodifiableMap(oldestAge), byPriority(counters.enqueued),
                    byPriority(counters.delivered), counters.dequeueEmpty, counters.acked, counters.redelivered,
                    counters.deadLettered, byPriority(counters.expired), counters.enqueueRejected,
                    counters.enqueueRate.perSecond(now), counters.ackRate.perSecond(now));
        } finally {
            lock.unlock();
        }
    }

    /** Up to {@code limit} live messages in arrival order, without leasing or changing any (D18b). O(limit). */
    List<MessageView> list(int limit) {
        lock.lock();
        try {
            drainLocked(drainLimit, clock.monotonicMillis());
            List<MessageView> views = new ArrayList<>(Math.min(limit, messages.size()));
            for (Message m : messages.values()) {
                if (views.size() == limit) {
                    break;
                }
                MessageState state = inFlight.containsKey(m.id()) ? MessageState.IN_FLIGHT : MessageState.READY;
                views.add(new MessageView(m.id(), m.payload(), m.priority(), state, m.enqueuedAt(),
                        m.deliveryCount(), m.deadLetter()));
            }
            return views;
        } finally {
            lock.unlock();
        }
    }

    long expiredCount() {
        lock.lock();
        try {
            return counters.totalExpired();
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

    int ttlDeadlineCount() {
        lock.lock();
        try {
            return ttlDeadlines.size();
        } finally {
            lock.unlock();
        }
    }

    /** Drains as of {@code now}: the single clock reading the calling operation uses throughout. */
    private int drainLocked(int limit, long now) {
        int drained = 0;
        while (drained < limit) {
            Deadline lease = firstDue(visibilityDeadlines, now);
            Deadline ttl = firstDue(ttlDeadlines, now);
            if (lease == null && ttl == null) {
                break;
            }
            if (ttl == null || (lease != null && DEADLINE_ORDER.compare(lease, ttl) <= 0)) {
                visibilityDeadlines.pollFirst();
                expireLease(lease.id(), lease.at(), now);
            } else {
                ttlDeadlines.pollFirst();
                expireReady(ttl.id());
            }
            drained++;
        }
        return drained;
    }

    private static Deadline firstDue(TreeSet<Deadline> deadlines, long now) {
        return deadlines.isEmpty() || deadlines.first().at() > now ? null : deadlines.first();
    }

    /**
     * Applies the D8d lease-expiry rows in order: TTL passed → dropped as expired; deliveries used up →
     * dead-lettered; otherwise → redelivered in its original position. The rows are judged at the lease
     * deadline {@code at}, not at the (possibly later) drain time {@code now}, so the outcome doesn't depend on
     * when a drain happens to run. A redelivered message whose TTL passed between {@code at} and {@code now}
     * expires later in the same drain.
     */
    private void expireLease(MessageId id, long at, long now) {
        inFlight.remove(id);
        Message message = messages.get(id);
        if (message.expiresAtMono() <= at) {
            messages.remove(id);
            counters.expired[message.priority().ordinal()]++;
        } else if (message.deliveryCount() >= maxDeliveries) {
            messages.remove(id);
            counters.deadLettered++;
            Instant deadLetteredAt = clock.wallTimeAt(at);
            deadLetters.deadLetter(message, new DeadLetterInfo(
                    queueName, message.deliveryCount(), DeadLetterReason.MAX_DELIVERIES, deadLetteredAt), at);
        } else {
            makeReady(message, true);
            counters.redelivered++;
        }
    }

    /** A ready message's TTL passed: tombstone its lane entry and drop it (D9b). */
    private void expireReady(MessageId id) {
        Message message = messages.remove(id);
        lanes.get(message.priority()).markDead(ready.remove(id));
        counters.expired[message.priority().ordinal()]++;
    }

    private void makeReady(Message message, boolean redelivery) {
        Lane.Entry entry = new Lane.Entry(message.seq(), message.id());
        Lane lane = lanes.get(message.priority());
        if (redelivery) {
            lane.offerRetry(entry);
        } else {
            lane.offer(entry);
        }
        ready.put(message.id(), entry);
        if (message.expiresAtMono() != Message.NO_EXPIRY) {
            ttlDeadlines.add(ttlDeadline(message));
        }
    }

    private static Map<Priority, Long> byPriority(long[] counts) {
        EnumMap<Priority, Long> map = new EnumMap<>(Priority.class);
        for (Priority p : Priority.values()) {
            map.put(p, counts[p.ordinal()]);
        }
        return Collections.unmodifiableMap(map);
    }

    private static Deadline ttlDeadline(Message message) {
        return new Deadline(message.expiresAtMono(), message.seq(), message.id());
    }

    private static void validate(String payload, Priority priority, Duration ttl) {
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
        if (ttl != null && (ttl.compareTo(MIN_TTL) < 0 || ttl.compareTo(MAX_TTL) > 0)) {
            throw new ValidationException("ttl must be between 1s and 14d, was " + ttl);
        }
    }
}

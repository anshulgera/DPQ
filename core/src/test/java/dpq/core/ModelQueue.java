package dpq.core;

import dpq.core.error.MessageNotFoundException;
import dpq.core.error.QueueFullException;
import dpq.core.error.StaleReceiptException;
import dpq.core.error.ValidationException;
import dpq.core.id.IdGenerator;
import dpq.core.id.ReceiptGenerator;
import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.DeliveredMessage;
import dpq.core.model.MessageId;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.model.QueueMetricsSnapshot;
import dpq.core.model.ReceiptHandle;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A deliberately naive reference model of one queue {@code q} and its DLQ {@code q.dlq} (D13). It is written from
 * Requirements.txt and the D8d table, not from the implementation: one list scanned on every operation, time as
 * a plain {@code long}, and every rule applied at the instant its deadline passes. Single-threaded.
 */
final class ModelQueue {

    static final String NAME = "q";
    static final String DLQ = "q.dlq";

    private static final class Msg {
        final MessageId id;
        final String payload;
        final Priority priority;
        final long seq;
        final long enqueuedAt;
        final Long expiresAt; // null: never expires
        int deliveryCount;
        boolean inFlight;
        ReceiptHandle receipt;
        long leaseDeadline;

        Msg(MessageId id, String payload, Priority priority, long seq, long enqueuedAt, Long expiresAt) {
            this.id = id;
            this.payload = payload;
            this.priority = priority;
            this.seq = seq;
            this.enqueuedAt = enqueuedAt;
            this.expiresAt = expiresAt;
        }
    }

    private record DeadMsg(Priority priority, long deadLetteredAt) {}

    private final Instant start;
    private final long visibilityMillis;
    private final int maxDeliveries;
    private final int maxDepth;
    private final IdGenerator ids = new SequentialIdGenerator();
    private final ReceiptGenerator receipts = new SequentialReceiptGenerator();

    private final List<Msg> live = new ArrayList<>(); // ready and in flight
    private final List<DeadMsg> dlq = new ArrayList<>();
    private long now;
    private long nextSeq;
    private long nextDeliverySeq;

    private final long[] enqueued = new long[3];
    private final long[] delivered = new long[3];
    private final long[] expired = new long[3];
    private final long[] dlqEnqueued = new long[3];
    private long dequeueEmpty;
    private long acked;
    private long redelivered;
    private long deadLettered;
    private long rejected;
    private final List<Long> enqueueTimes = new ArrayList<>();
    private final List<Long> ackTimes = new ArrayList<>();
    private final List<Long> dlqEnqueueTimes = new ArrayList<>();

    ModelQueue(Instant start, QueueConfig config) {
        this.start = start;
        this.visibilityMillis = config.visibilityTimeout().toMillis();
        this.maxDeliveries = config.maxDeliveries();
        this.maxDepth = config.maxDepth();
    }

    void advance(long millis) {
        now += millis;
    }

    MessageId enqueue(String payload, Priority priority, Duration ttl) {
        if (ttl != null && (ttl.compareTo(Duration.ofSeconds(1)) < 0 || ttl.compareTo(Duration.ofDays(14)) > 0)) {
            throw new ValidationException("ttl");
        }
        applyDueRules();
        if (live.size() >= maxDepth) {
            rejected++;
            throw new QueueFullException(NAME, maxDepth);
        }
        MessageId id = ids.next(0);
        live.add(new Msg(id, payload, priority, nextSeq++, now, ttl == null ? null : now + ttl.toMillis()));
        enqueued[priority.ordinal()]++;
        enqueueTimes.add(now);
        return id;
    }

    /** Highest priority first; FIFO (original enqueue order) within a priority, redeliveries included. */
    Optional<DeliveredMessage> dequeue() {
        applyDueRules();
        Msg best = null;
        for (Msg m : live) {
            if (!m.inFlight && (best == null
                    || m.priority.ordinal() < best.priority.ordinal()
                    || (m.priority == best.priority && m.seq < best.seq))) {
                best = m;
            }
        }
        if (best == null) {
            dequeueEmpty++;
            return Optional.empty();
        }
        best.deliveryCount++;
        best.inFlight = true;
        best.receipt = receipts.next();
        best.leaseDeadline = now + visibilityMillis;
        delivered[best.priority.ordinal()]++;
        return Optional.of(new DeliveredMessage(best.id, best.receipt, best.payload, best.priority,
                best.deliveryCount, wall(best.enqueuedAt), wall(best.leaseDeadline), nextDeliverySeq++, null));
    }

    void ack(MessageId id, ReceiptHandle receipt) {
        applyDueRules();
        Msg m = live.stream().filter(x -> x.id.equals(id)).findFirst().orElse(null);
        if (m == null) {
            throw new MessageNotFoundException(id); // acked, dead-lettered, expired or never existed
        }
        if (!m.inFlight || !m.receipt.equals(receipt) || now >= m.leaseDeadline) {
            throw new StaleReceiptException(id);
        }
        live.remove(m);
        acked++;
        ackTimes.add(now);
    }

    QueueMetricsSnapshot metrics() {
        applyDueRules();
        EnumMap<Priority, Long> ready = new EnumMap<>(Priority.class);
        EnumMap<Priority, Double> age = new EnumMap<>(Priority.class);
        for (Priority p : Priority.values()) {
            List<Msg> readyOfP = live.stream().filter(m -> !m.inFlight && m.priority == p).toList();
            ready.put(p, (long) readyOfP.size());
            age.put(p, readyOfP.stream().mapToLong(m -> m.enqueuedAt).min().stream()
                    .mapToDouble(t -> (now - t) / 1000.0).findFirst().orElse(0.0));
        }
        long inFlight = live.stream().filter(m -> m.inFlight).count();
        return new QueueMetricsSnapshot(NAME, ready, inFlight, age, map(enqueued), map(delivered), dequeueEmpty,
                acked, redelivered, deadLettered, map(expired), rejected, rate(enqueueTimes), rate(ackTimes));
    }

    QueueMetricsSnapshot dlqMetrics() {
        applyDueRules();
        EnumMap<Priority, Long> ready = new EnumMap<>(Priority.class);
        EnumMap<Priority, Double> age = new EnumMap<>(Priority.class);
        for (Priority p : Priority.values()) {
            List<DeadMsg> ofP = dlq.stream().filter(m -> m.priority() == p).toList();
            ready.put(p, (long) ofP.size());
            age.put(p, ofP.stream().mapToLong(DeadMsg::deadLetteredAt).min().stream()
                    .mapToDouble(t -> (now - t) / 1000.0).findFirst().orElse(0.0));
        }
        long[] zero = new long[3];
        return new QueueMetricsSnapshot(DLQ, ready, 0, age, map(dlqEnqueued), map(zero), 0, 0, 0, 0, map(zero), 0,
                rate(dlqEnqueueTimes), 0.0);
    }

    /**
     * Applies every rule whose deadline has passed, earliest first, each judged at its own deadline (D8d):
     * a lease that ends after the TTL → expired; otherwise deliveries used up → DLQ; otherwise → ready again.
     * A ready message whose TTL passes → expired (D9b).
     */
    private void applyDueRules() {
        while (true) {
            Msg due = null;
            long dueAt = Long.MAX_VALUE;
            for (Msg m : live) {
                long at = m.inFlight ? m.leaseDeadline : (m.expiresAt == null ? Long.MAX_VALUE : m.expiresAt);
                if (at <= now && (at < dueAt || (at == dueAt && m.seq < due.seq))) {
                    due = m;
                    dueAt = at;
                }
            }
            if (due == null) {
                return;
            }
            if (!due.inFlight) {
                live.remove(due);
                expired[due.priority.ordinal()]++;
            } else if (due.expiresAt != null && due.expiresAt <= dueAt) {
                live.remove(due);
                expired[due.priority.ordinal()]++;
            } else if (due.deliveryCount >= maxDeliveries) {
                live.remove(due);
                deadLettered++;
                dlq.add(new DeadMsg(due.priority, dueAt));
                dlqEnqueued[due.priority.ordinal()]++;
                dlqEnqueueTimes.add(dueAt);
            } else {
                due.inFlight = false;
                due.receipt = null;
                redelivered++;
            }
        }
    }

    private double rate(List<Long> times) {
        long current = Math.floorDiv(now, 1000);
        long count = times.stream().filter(t -> {
            long age = current - Math.floorDiv(t, 1000);
            return age >= 1 && age <= 60;
        }).count();
        return count / 60.0;
    }

    private Instant wall(long millis) {
        return start.plusMillis(millis);
    }

    private static Map<Priority, Long> map(long[] counts) {
        EnumMap<Priority, Long> m = new EnumMap<>(Priority.class);
        for (Priority p : Priority.values()) {
            m.put(p, counts[p.ordinal()]);
        }
        return Collections.unmodifiableMap(m);
    }
}

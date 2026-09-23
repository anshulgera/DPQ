package dpq.core.model;

import java.time.Instant;

/**
 * One delivery of a message to a consumer.
 *
 * @param receipt      proves this delivery when acking (D8a)
 * @param enqueuedAt   wall-clock enqueue time; for a DLQ message, the dead-letter time (D18c)
 * @param visibleUntil wall-clock time the lease ends, for display
 * @param deliverySeq  partition-wide delivery order, for ordering tests; not exposed over HTTP
 * @param deadLetter   dead-letter metadata, or {@code null} unless delivered from a DLQ (D18c)
 */
public record DeliveredMessage(
        MessageId id,
        ReceiptHandle receipt,
        String payload,
        Priority priority,
        int deliveryCount,
        Instant enqueuedAt,
        Instant visibleUntil,
        long deliverySeq,
        DeadLetterInfo deadLetter) {}

package dpq.core;

import java.time.Instant;

/**
 * An enqueued message. Immutable: the partition replaces it when its delivery state changes.
 *
 * @param seq            enqueue order within the partition; the FIFO key, kept across redelivery (D4)
 * @param enqueuedAtMono monotonic enqueue time, used for oldest-message age (D18d)
 * @param enqueuedAt     wall-clock enqueue time, for display only (D18d)
 * @param expiresAtMono  monotonic TTL deadline, or {@link #NO_EXPIRY}
 * @param deliveryCount  deliveries so far, including the current one (D8b)
 * @param deadLetter     dead-letter metadata, or {@code null} unless the message is in a DLQ (D18c)
 */
public record Message(
        MessageId id,
        String payload,
        Priority priority,
        long seq,
        long enqueuedAtMono,
        Instant enqueuedAt,
        long expiresAtMono,
        int deliveryCount,
        DeadLetterInfo deadLetter) {

    public static final long NO_EXPIRY = Long.MAX_VALUE;
}

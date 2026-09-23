package dpq.core;

import java.time.Instant;

/**
 * A read-only view of a live message, for listing a queue's contents (D18b).
 *
 * @param deliveryCount deliveries so far in this queue
 * @param deadLetter    dead-letter metadata, or {@code null} unless the message is in a DLQ
 */
public record MessageView(
        MessageId id,
        String payload,
        Priority priority,
        MessageState state,
        Instant enqueuedAt,
        int deliveryCount,
        DeadLetterInfo deadLetter) {}

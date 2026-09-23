package dpq.core;

import java.time.Instant;

/** Metadata attached to a message when it moves to a DLQ (D9a, D18c). */
public record DeadLetterInfo(
        String sourceQueue, int sourceDeliveryCount, DeadLetterReason reason, Instant deadLetteredAt) {}

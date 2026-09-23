package dpq.core.id;

import dpq.core.time.Clock;
import java.util.UUID;
import java.util.random.RandomGenerator;

/**
 * UUIDv7 generator (RFC 9562): a 48-bit Unix-millis timestamp followed by 74 random bits. When called again
 * within the same millisecond (or after the wall clock steps back), it increments the previous value's
 * random field instead of drawing a new one (RFC 9562 §6.2, method 2), so its IDs are strictly increasing.
 */
public final class UuidV7 {

    private static final long RAND_A_MAX = (1L << 12) - 1;
    private static final long RAND_B_MAX = (1L << 62) - 1;

    private final Clock clock;
    private final RandomGenerator random;

    // Guarded by this.
    private long lastMillis = -1;
    private long randA;
    private long randB;

    public UuidV7(Clock clock, RandomGenerator random) {
        this.clock = clock;
        this.random = random;
    }

    public synchronized UUID next() {
        long millis = clock.wallTime().toEpochMilli();
        if (millis > lastMillis) {
            lastMillis = millis;
            randA = random.nextLong() & RAND_A_MAX;
            randB = random.nextLong() & RAND_B_MAX;
        } else if (++randB > RAND_B_MAX) {
            randB = 0;
            if (++randA > RAND_A_MAX) {
                // All 74 bits used up within one millisecond: borrow the next millisecond.
                randA = 0;
                lastMillis++;
            }
        }
        long msb = (lastMillis << 16) | (0x7L << 12) | randA; // version 7
        long lsb = (0b10L << 62) | randB; // RFC 9562 variant
        return new UUID(msb, lsb);
    }
}

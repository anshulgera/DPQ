package dpq.core;

import java.time.Instant;

/**
 * The only source of time in the engine (D6). Deadlines and ages use {@link #monotonicMillis()}, so an
 * NTP step can't shorten or extend a lease; {@link #wallTime()} is for displayed timestamps only.
 */
public interface Clock {

    /** Milliseconds from an arbitrary fixed origin; never goes backwards. */
    long monotonicMillis();

    /** Current wall-clock time, for display. */
    Instant wallTime();
}

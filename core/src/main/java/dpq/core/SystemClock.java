package dpq.core;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/** Production {@link Clock}: the only place the engine reads system time. */
public final class SystemClock implements Clock {

    @Override
    public long monotonicMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }

    @Override
    public Instant wallTime() {
        return Instant.now();
    }

    @Override
    public Instant wallTimeAt(long monotonicMillis) {
        // Offsets from "now" on both scales, so wall-clock steps apply to the displayed time too.
        return Instant.now().minusMillis(monotonicMillis() - monotonicMillis);
    }
}

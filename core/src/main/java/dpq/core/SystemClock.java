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
}

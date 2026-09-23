package com.keychain.dpq.core;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/** Manual clock for tests (D6). Monotonic and wall time move together, only through {@link #advance}. */
public final class FakeClock implements Clock {

    private final Instant start;
    private final AtomicLong elapsedMillis = new AtomicLong();

    public FakeClock(Instant start) {
        this.start = start;
    }

    /** Moves time forward; safe to call from any thread. */
    public void advance(Duration amount) {
        if (amount.isNegative()) {
            throw new IllegalArgumentException("a clock can't go backwards: " + amount);
        }
        elapsedMillis.addAndGet(amount.toMillis());
    }

    @Override
    public long monotonicMillis() {
        return elapsedMillis.get();
    }

    @Override
    public Instant wallTime() {
        return start.plusMillis(elapsedMillis.get());
    }
}

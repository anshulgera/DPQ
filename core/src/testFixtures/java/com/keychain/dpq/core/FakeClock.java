package com.keychain.dpq.core;

import java.time.Duration;
import java.time.Instant;

public final class FakeClock implements Clock {

    public FakeClock(Instant start) {}

    public void advance(Duration amount) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public long monotonicMillis() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Instant wallTime() {
        throw new UnsupportedOperationException("not implemented");
    }
}

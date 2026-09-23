package dpq.core;

import java.time.Duration;

public final class Reaper implements AutoCloseable {

    public Reaper(Runnable sweep, Duration interval) {}

    public boolean isTerminated() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() {}
}

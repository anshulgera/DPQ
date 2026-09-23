package dpq.core;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Runs a sweep on a fixed delay from one daemon thread, so idle queues still expire leases and TTLs and move
 * messages to their DLQ (D6). A failing sweep is logged and doesn't stop later ones.
 */
final class Reaper implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(Reaper.class.getName());

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "dpq-reaper");
        thread.setDaemon(true);
        return thread;
    });

    Reaper(Runnable sweep, Duration interval) {
        long millis = interval.toMillis();
        if (millis <= 0) {
            throw new IllegalArgumentException("reaper interval must be at least 1ms, was " + interval);
        }
        executor.scheduleWithFixedDelay(() -> {
            try {
                sweep.run();
            } catch (RuntimeException e) {
                // An exception would cancel all later runs of a scheduled task.
                LOG.log(System.Logger.Level.WARNING, "reaper sweep failed", e);
            }
        }, millis, millis, TimeUnit.MILLISECONDS);
    }

    boolean isTerminated() {
        return executor.isTerminated();
    }

    @Override
    public void close() {
        executor.shutdownNow();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

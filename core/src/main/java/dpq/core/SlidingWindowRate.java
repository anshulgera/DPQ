package dpq.core;

/**
 * Events per second over the 60 complete seconds before the current one (D12b), from a ring of one-second
 * buckets keyed by monotonic second. It has 61 slots: 60 complete seconds plus the current one, which would
 * otherwise overwrite the oldest complete second.
 *
 * <p>Not thread-safe: guarded by the owning partition's lock (D5).
 */
final class SlidingWindowRate {

    private static final int WINDOW_SECONDS = 60;
    private static final int SLOTS = WINDOW_SECONDS + 1;

    private final long[] counts = new long[SLOTS];
    private final long[] seconds = new long[SLOTS];

    SlidingWindowRate() {
        java.util.Arrays.fill(seconds, Long.MIN_VALUE);
    }

    void record(long nowMillis) {
        long second = Math.floorDiv(nowMillis, 1000);
        int slot = Math.floorMod(second, SLOTS);
        if (seconds[slot] != second) {
            seconds[slot] = second;
            counts[slot] = 0;
        }
        counts[slot]++;
    }

    double perSecond(long nowMillis) {
        long current = Math.floorDiv(nowMillis, 1000);
        long sum = 0;
        for (int slot = 0; slot < SLOTS; slot++) {
            long age = current - seconds[slot];
            if (age >= 1 && age <= WINDOW_SECONDS) {
                sum += counts[slot];
            }
        }
        return (double) sum / WINDOW_SECONDS;
    }
}

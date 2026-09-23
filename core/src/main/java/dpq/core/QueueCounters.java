package dpq.core;

import dpq.core.model.Priority;

/**
 * A partition's event counters and rates (D12). Plain fields, because every update already happens under the
 * partition lock (D5). Not thread-safe on its own.
 */
final class QueueCounters {

    final long[] enqueued = new long[Priority.values().length];
    final long[] delivered = new long[Priority.values().length];
    final long[] expired = new long[Priority.values().length];
    long dequeueEmpty;
    long acked;
    long redelivered;
    long deadLettered;
    long enqueueRejected;
    final SlidingWindowRate enqueueRate = new SlidingWindowRate();
    final SlidingWindowRate ackRate = new SlidingWindowRate();

    void enqueued(Priority priority, long nowMillis) {
        enqueued[priority.ordinal()]++;
        enqueueRate.record(nowMillis);
    }

    void acked(long nowMillis) {
        acked++;
        ackRate.record(nowMillis);
    }

    long totalExpired() {
        long total = 0;
        for (long n : expired) {
            total += n;
        }
        return total;
    }
}

package dpq.core;

import java.util.Map;

/**
 * One queue's metrics at one instant, read under the partition lock so the values are mutually consistent
 * (D12a). Both the Prometheus and the JSON endpoints are built from it.
 *
 * @param ready                      ready messages by priority
 * @param inFlight                   delivered but not yet acked or expired
 * @param oldestAgeSecondsByPriority age of the oldest ready message per priority; 0 when none is ready
 * @param enqueued                   total accepted enqueues by priority; for a DLQ, dead-lettered arrivals
 * @param delivered                  total deliveries by priority, redeliveries included
 * @param dequeueEmpty               total dequeues that found nothing ready
 * @param redelivered                total leases that expired and returned the message to its lane
 * @param deadLettered               total messages moved to this queue's DLQ
 * @param expired                    total messages dropped by TTL, by priority
 * @param enqueueRejected            total enqueues rejected because the queue was full
 * @param enqueueRatePerSec          enqueues per second over the last 60 complete seconds (D12b)
 * @param ackRatePerSec              acks per second over the last 60 complete seconds (D12b)
 */
public record QueueMetricsSnapshot(
        String queue,
        Map<Priority, Long> ready,
        long inFlight,
        Map<Priority, Double> oldestAgeSecondsByPriority,
        Map<Priority, Long> enqueued,
        Map<Priority, Long> delivered,
        long dequeueEmpty,
        long acked,
        long redelivered,
        long deadLettered,
        Map<Priority, Long> expired,
        long enqueueRejected,
        double enqueueRatePerSec,
        double ackRatePerSec) {

    public long totalReady() {
        return ready.values().stream().mapToLong(Long::longValue).sum();
    }

    /** The spec's per-queue value: the oldest ready message of any priority. */
    public double oldestAgeSeconds() {
        return oldestAgeSecondsByPriority.values().stream().mapToDouble(Double::doubleValue).max().orElse(0);
    }
}

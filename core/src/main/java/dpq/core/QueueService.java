package dpq.core;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public final class QueueService implements AutoCloseable {

    public record Options(int maxQueues, int drainLimit, Duration reaperInterval) {

        public static Options defaults() {
            return new Options(1000, Partition.DEFAULT_DRAIN_LIMIT, Duration.ofMillis(100));
        }

        public Options withoutReaper() {
            return new Options(maxQueues, drainLimit, null);
        }

        public Options withReaperInterval(Duration interval) {
            return new Options(maxQueues, drainLimit, interval);
        }

        public Options withMaxQueues(int max) {
            return new Options(max, drainLimit, reaperInterval);
        }
    }

    public QueueService(Clock clock, IdGenerator ids, ReceiptGenerator receipts, Options options) {}

    public boolean createQueue(String name, QueueConfig config) {
        throw new UnsupportedOperationException("not implemented");
    }

    public QueueDescription getQueue(String name) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Set<String> queueNames() {
        throw new UnsupportedOperationException("not implemented");
    }

    public MessageId enqueue(String queue, String payload, Priority priority, Duration ttl) {
        throw new UnsupportedOperationException("not implemented");
    }

    public Optional<DeliveredMessage> dequeue(String queue) {
        throw new UnsupportedOperationException("not implemented");
    }

    public void ack(String queue, MessageId id, ReceiptHandle receipt) {
        throw new UnsupportedOperationException("not implemented");
    }

    void sweep() {
        throw new UnsupportedOperationException("not implemented");
    }

    Partition partition(String queue) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() {}
}

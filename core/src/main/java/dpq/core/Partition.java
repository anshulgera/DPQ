package dpq.core;

import java.util.Optional;

final class Partition {

    Partition(String queueName, int index, QueueConfig config, Clock clock, IdGenerator ids,
            ReceiptGenerator receipts, SelectionPolicy policy) {}

    MessageId enqueue(String payload, Priority priority) {
        throw new UnsupportedOperationException("not implemented");
    }

    Optional<DeliveredMessage> dequeue() {
        throw new UnsupportedOperationException("not implemented");
    }

    void ack(MessageId id, ReceiptHandle receipt) {
        throw new UnsupportedOperationException("not implemented");
    }

    int readyCount(Priority priority) {
        throw new UnsupportedOperationException("not implemented");
    }

    int inFlightCount() {
        throw new UnsupportedOperationException("not implemented");
    }
}

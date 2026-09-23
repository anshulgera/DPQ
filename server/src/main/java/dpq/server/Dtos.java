package dpq.server;

import dpq.core.DeadLetterInfo;
import dpq.core.DeliveredMessage;
import dpq.core.MessageState;
import dpq.core.MessageView;
import dpq.core.Priority;
import dpq.core.QueueDescription;
import dpq.core.QueueMetricsSnapshot;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** JSON request and response bodies (plan.md §5). Null fields are omitted from responses. */
final class Dtos {

    private Dtos() {}

    /** PUT /queues/{name}; omitted fields take the defaults (D11c). */
    record CreateQueueRequest(Long visibilityTimeoutSeconds, Integer maxDeliveries, Integer maxDepth) {}

    record QueueResponse(String name, long visibilityTimeoutSeconds, int maxDeliveries, int maxDepth) {
        static QueueResponse of(QueueDescription queue) {
            return new QueueResponse(queue.name(), queue.config().visibilityTimeout().toSeconds(),
                    queue.config().maxDeliveries(), queue.config().maxDepth());
        }
    }

    record EnqueueRequest(String payload, Priority priority, Long ttlSeconds) {}

    record EnqueueResponse(String messageId) {}

    /** The receipt travels in the body, never the URL, so it stays out of access logs (D8a). */
    record AckRequest(String receiptHandle) {}

    record DeadLetterJson(String sourceQueue, int deliveryCount, String reason, Instant deadLetteredAt) {
        static DeadLetterJson of(DeadLetterInfo info) {
            return info == null ? null : new DeadLetterJson(info.sourceQueue(), info.sourceDeliveryCount(),
                    info.reason().name(), info.deadLetteredAt());
        }
    }

    record DequeueResponse(String messageId, String receiptHandle, String payload, Priority priority,
            int deliveryCount, Instant enqueuedAt, Instant visibleUntil, DeadLetterJson deadLetter) {
        static DequeueResponse of(DeliveredMessage m) {
            return new DequeueResponse(m.id().toString(), m.receipt().value(), m.payload(), m.priority(),
                    m.deliveryCount(), m.enqueuedAt(), m.visibleUntil(), DeadLetterJson.of(m.deadLetter()));
        }
    }

    record MessageJson(String messageId, String payload, Priority priority, MessageState state, Instant enqueuedAt,
            int deliveryCount, DeadLetterJson deadLetter) {
        static MessageJson of(MessageView m) {
            return new MessageJson(m.id().toString(), m.payload(), m.priority(), m.state(), m.enqueuedAt(),
                    m.deliveryCount(), DeadLetterJson.of(m.deadLetter()));
        }
    }

    /** GET /queues/{name}.dlq (D18b). */
    record MessageListResponse(List<MessageJson> messages) {}

    /** GET /queues/{name}/metrics: the spec's "Get Metrics", from the same snapshot as /metrics (D12a). */
    record MetricsResponse(String queue, Map<Priority, Long> ready, long totalReady, long inFlight,
            double oldestMessageAgeSeconds, Map<Priority, Double> oldestAgeSecondsByPriority,
            Map<Priority, Long> enqueued, Map<Priority, Long> delivered, long dequeueEmpty, long acked,
            long redelivered, long deadLettered, Map<Priority, Long> expired, long enqueueRejected,
            double enqueueRatePerSec, double ackRatePerSec) {
        static MetricsResponse of(QueueMetricsSnapshot m) {
            return new MetricsResponse(m.queue(), m.ready(), m.totalReady(), m.inFlight(), m.oldestAgeSeconds(),
                    m.oldestAgeSecondsByPriority(), m.enqueued(), m.delivered(), m.dequeueEmpty(), m.acked(),
                    m.redelivered(), m.deadLettered(), m.expired(), m.enqueueRejected(), m.enqueueRatePerSec(),
                    m.ackRatePerSec());
        }
    }

    record ErrorResponse(String error, String message) {}

    record HealthResponse(String status) {}
}

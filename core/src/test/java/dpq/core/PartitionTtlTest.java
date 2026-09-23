package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** TTL expiry and its precedence over redelivery and dead-lettering (D8d, D9b, D9c). */
class PartitionTtlTest {

    private static final Duration VISIBILITY = Duration.ofSeconds(30);
    private static final Duration TTL = Duration.ofSeconds(60);

    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final List<Message> deadLettered = new ArrayList<>();
    private Partition partition = partition(5, Partition.DEFAULT_DRAIN_LIMIT);

    @Test
    void aReadyMessageIsDequeueableUntilItsTtlPasses() {
        partition.enqueue("m", Priority.HIGH, TTL);
        partition.enqueue("n", Priority.HIGH, TTL);

        clock.advance(TTL.minusMillis(1));
        assertThat(partition.dequeue()).map(DeliveredMessage::payload).contains("m");

        clock.advance(Duration.ofMillis(1));
        assertThat(partition.dequeue()).isEmpty();
    }

    @Test
    void aReadyMessagePastItsTtlIsNeverDequeuedAndCountsAsExpired() {
        partition.enqueue("short", Priority.HIGH, TTL);
        partition.enqueue("forever", Priority.LOW);

        clock.advance(TTL);

        assertThat(partition.dequeue()).map(DeliveredMessage::payload).contains("forever");
        assertThat(partition.dequeue()).isEmpty();
        assertThat(partition.expiredCount()).isEqualTo(1);
        assertThat(deadLettered).isEmpty();
    }

    @Test
    void anExpiredEntryDeepInTheLaneLeavesTheCountsAtOnce() {
        partition.enqueue("head", Priority.MEDIUM);
        partition.enqueue("middle", Priority.MEDIUM, TTL);
        partition.enqueue("tail", Priority.MEDIUM);

        clock.advance(TTL);
        partition.drainExpired(Integer.MAX_VALUE);

        assertThat(partition.readyCount(Priority.MEDIUM)).isEqualTo(2);
        assertThat(partition.dequeue()).map(DeliveredMessage::payload).contains("head");
        assertThat(partition.dequeue()).map(DeliveredMessage::payload).contains("tail");
    }

    @Test
    void expiryFreesDepth() {
        partition = new Partition("orders", 0, QueueConfig.of(null, null, 1), clock, new SequentialIdGenerator(),
                new SequentialReceiptGenerator(), new StrictPriorityPolicy(), (m, info) -> deadLettered.add(m),
                Partition.DEFAULT_DRAIN_LIMIT);
        partition.enqueue("a", Priority.LOW, TTL);
        clock.advance(TTL);

        assertThat(partition.enqueue("b", Priority.LOW)).isNotNull();
    }

    @Test
    void aTtlPassingWhileInFlightStillAllowsTheAck() {
        MessageId id = partition.enqueue("m", Priority.HIGH, Duration.ofSeconds(10));
        ReceiptHandle receipt = partition.dequeue().orElseThrow().receipt();

        clock.advance(Duration.ofSeconds(20)); // TTL passed, lease still valid

        partition.ack(id, receipt);
        assertThat(partition.inFlightCount()).isZero();
        assertThat(partition.expiredCount()).isZero();
    }

    @Test
    void aTtlPassingWhileInFlightDropsTheMessageWhenTheLeaseExpires() {
        MessageId id = partition.enqueue("m", Priority.HIGH, Duration.ofSeconds(10));
        ReceiptHandle receipt = partition.dequeue().orElseThrow().receipt();

        clock.advance(VISIBILITY);

        assertThat(partition.dequeue()).isEmpty(); // not redelivered
        assertThat(deadLettered).isEmpty();
        assertThat(partition.expiredCount()).isEqualTo(1);
        assertThatThrownBy(() -> partition.ack(id, receipt)).isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    void ttlBeatsDeadLetteringWhenBothApplyAtLeaseExpiry() {
        partition = partition(1, Partition.DEFAULT_DRAIN_LIMIT);
        partition.enqueue("m", Priority.HIGH, Duration.ofSeconds(10));
        partition.dequeue().orElseThrow(); // deliveryCount 1 == maxDeliveries

        clock.advance(VISIBILITY);
        partition.drainExpired(Integer.MAX_VALUE);

        assertThat(deadLettered).isEmpty();
        assertThat(partition.expiredCount()).isEqualTo(1);
    }

    @Test
    void aRedeliveredMessageKeepsItsTtl() {
        partition.enqueue("m", Priority.HIGH, TTL); // expires at 60s
        partition.dequeue().orElseThrow();
        clock.advance(VISIBILITY); // lease expires at 30s, TTL not passed: redelivered

        assertThat(partition.readyCount(Priority.HIGH)).isEqualTo(1);
        clock.advance(TTL.minus(VISIBILITY)); // 60s: TTL passes while ready again

        assertThat(partition.dequeue()).isEmpty();
        assertThat(partition.expiredCount()).isEqualTo(1);
    }

    @Test
    void ackOfAnExpiredMessageIsNotFound() {
        MessageId id = partition.enqueue("m", Priority.HIGH, TTL);
        clock.advance(TTL);

        assertThatThrownBy(() -> partition.ack(id, new ReceiptHandle("any")))
                .isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    void bothDeadlineSetsAreEmptyAfterManyTtlMessagesAreAcked() {
        for (int i = 0; i < 100; i++) {
            MessageId id = partition.enqueue("m" + i, Priority.values()[i % 3], TTL);
            partition.ack(id, partition.dequeue().orElseThrow().receipt());
        }

        assertThat(partition.visibilityDeadlineCount()).isZero();
        assertThat(partition.ttlDeadlineCount()).isZero();
    }

    @Test
    void leaseAndTtlExpiriesShareOneDrainLimit() {
        partition = partition(5, 2);
        partition.enqueue("leased", Priority.HIGH);
        partition.dequeue().orElseThrow();
        partition.enqueue("ttl-1", Priority.LOW, VISIBILITY);
        partition.enqueue("ttl-2", Priority.LOW, VISIBILITY);
        clock.advance(VISIBILITY); // one lease and two TTLs expire together

        assertThat(partition.drainExpired(2)).isEqualTo(2);
        assertThat(partition.drainExpired(2)).isEqualTo(1);
        assertThat(partition.drainExpired(2)).isZero();
    }

    @Test
    void rejectsTtlOutsideOneSecondToFourteenDays() {
        assertThatThrownBy(() -> partition.enqueue("m", Priority.LOW, Duration.ofMillis(999)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("ttl");
        assertThatThrownBy(() -> partition.enqueue("m", Priority.LOW, Duration.ofDays(14).plusMillis(1)))
                .isInstanceOf(ValidationException.class);
        assertThat(partition.enqueue("m", Priority.LOW, Duration.ofSeconds(1))).isNotNull();
        assertThat(partition.enqueue("m", Priority.LOW, Duration.ofDays(14))).isNotNull();
    }

    private Partition partition(int maxDeliveries, int drainLimit) {
        return new Partition("orders", 0, QueueConfig.of(VISIBILITY.toSeconds(), maxDeliveries, null), clock,
                new SequentialIdGenerator(), new SequentialReceiptGenerator(), new StrictPriorityPolicy(),
                (message, info) -> deadLettered.add(message), drainLimit);
    }
}

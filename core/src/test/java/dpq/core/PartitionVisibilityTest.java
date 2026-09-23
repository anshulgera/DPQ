package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Visibility timeout, redelivery and the dead-letter hand-off (D6, D8b, D8d). */
class PartitionVisibilityTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final Duration VISIBILITY = Duration.ofSeconds(30);

    private record DeadLettered(Message message, DeadLetterInfo info) {}

    private final FakeClock clock = new FakeClock(START);
    private final List<DeadLettered> sink = new ArrayList<>();
    private Partition partition = partition(5, Partition.DEFAULT_DRAIN_LIMIT);

    @Test
    void aLeasedMessageStaysInvisibleUntilItsDeadline() {
        MessageId id = partition.enqueue("m", Priority.HIGH);
        partition.dequeue().orElseThrow();

        clock.advance(VISIBILITY.minusMillis(1));
        assertThat(partition.dequeue()).isEmpty();

        clock.advance(Duration.ofMillis(1));
        assertThat(partition.dequeue()).map(DeliveredMessage::id).contains(id);
    }

    @Test
    void aRedeliveredMessageComesBeforeANewerMessageOfTheSamePriority() {
        MessageId first = partition.enqueue("first", Priority.MEDIUM);
        partition.dequeue().orElseThrow();
        partition.enqueue("second", Priority.MEDIUM);

        clock.advance(VISIBILITY);

        assertThat(partition.dequeue()).map(DeliveredMessage::id).contains(first);
        assertThat(partition.dequeue()).map(DeliveredMessage::payload).contains("second");
    }

    @Test
    void leasesExpiringOutOfOrderAreRedeliveredInOriginalOrder() {
        MessageId a = partition.enqueue("a", Priority.LOW);
        MessageId b = partition.enqueue("b", Priority.LOW);
        partition.dequeue().orElseThrow(); // a
        clock.advance(Duration.ofSeconds(10));
        partition.dequeue().orElseThrow(); // b, leased 10s later than a
        partition.enqueue("c", Priority.LOW);

        clock.advance(VISIBILITY.plusSeconds(10)); // both leases have expired

        assertThat(partition.dequeue()).map(DeliveredMessage::id).contains(a);
        assertThat(partition.dequeue()).map(DeliveredMessage::id).contains(b);
        assertThat(partition.dequeue()).map(DeliveredMessage::payload).contains("c");
    }

    @Test
    void deliveryCountIncrementsOnEachRedelivery() {
        partition.enqueue("m", Priority.HIGH);

        for (int expected = 1; expected <= 3; expected++) {
            assertThat(partition.dequeue().orElseThrow().deliveryCount()).isEqualTo(expected);
            clock.advance(VISIBILITY);
        }
    }

    @Test
    void anOldReceiptAfterRedeliveryIsStale() {
        MessageId id = partition.enqueue("m", Priority.HIGH);
        ReceiptHandle old = partition.dequeue().orElseThrow().receipt();
        clock.advance(VISIBILITY);
        ReceiptHandle current = partition.dequeue().orElseThrow().receipt();

        assertThatThrownBy(() -> partition.ack(id, old)).isInstanceOf(StaleReceiptException.class);
        partition.ack(id, current);
    }

    @Test
    void withTwoMaxDeliveriesTheSecondExpiryDeadLettersTheMessage() {
        partition = partition(2, Partition.DEFAULT_DRAIN_LIMIT);
        MessageId id = partition.enqueue("poison", Priority.MEDIUM);

        partition.dequeue().orElseThrow();
        clock.advance(VISIBILITY);
        assertThat(partition.dequeue()).map(DeliveredMessage::deliveryCount).contains(2);
        assertThat(sink).isEmpty();

        clock.advance(VISIBILITY);
        assertThat(partition.dequeue()).isEmpty();

        assertThat(sink).singleElement().satisfies(d -> {
            assertThat(d.message().id()).isEqualTo(id);
            assertThat(d.message().payload()).isEqualTo("poison");
            assertThat(d.message().priority()).isEqualTo(Priority.MEDIUM);
            assertThat(d.info()).isEqualTo(new DeadLetterInfo(
                    "orders", 2, DeadLetterReason.MAX_DELIVERIES, START.plus(VISIBILITY.multipliedBy(2))));
        });
        assertThat(partition.readyCount(Priority.MEDIUM)).isZero();
        assertThat(partition.inFlightCount()).isZero();
    }

    @Test
    void withOneMaxDeliveryTheFirstExpiryDeadLettersTheMessage() {
        partition = partition(1, Partition.DEFAULT_DRAIN_LIMIT);
        MessageId id = partition.enqueue("m", Priority.LOW);
        partition.dequeue().orElseThrow();

        clock.advance(VISIBILITY);
        assertThat(partition.dequeue()).isEmpty();

        assertThat(sink).singleElement().satisfies(d -> assertThat(d.message().id()).isEqualTo(id));
    }

    @Test
    void deadLetteringFreesDepth() {
        partition = new Partition("orders", 0, QueueConfig.of(30L, 1, 1), false, clock, new SequentialIdGenerator(),
                new SequentialReceiptGenerator(), new StrictPriorityPolicy(),
                (m, info) -> sink.add(new DeadLettered(m, info)), Partition.DEFAULT_DRAIN_LIMIT);
        partition.enqueue("a", Priority.LOW);
        partition.dequeue().orElseThrow();
        clock.advance(VISIBILITY);

        assertThat(partition.enqueue("b", Priority.LOW)).isNotNull();
    }

    @Test
    void ackAtExactlyTheDeadlineIsStaleEvenBeforeAnyDrain() {
        partition = partition(5, 0); // operations never drain, so only the ack's own deadline check applies
        MessageId id = partition.enqueue("m", Priority.HIGH);
        ReceiptHandle receipt = partition.dequeue().orElseThrow().receipt();

        clock.advance(VISIBILITY);

        assertThatThrownBy(() -> partition.ack(id, receipt)).isInstanceOf(StaleReceiptException.class);
        assertThat(partition.inFlightCount()).isEqualTo(1); // still undrained, not removed by the failed ack
    }

    @Test
    void ackOneMillisecondBeforeTheDeadlineSucceeds() {
        MessageId id = partition.enqueue("m", Priority.HIGH);
        ReceiptHandle receipt = partition.dequeue().orElseThrow().receipt();

        clock.advance(VISIBILITY.minusMillis(1));

        partition.ack(id, receipt);
        assertThat(partition.inFlightCount()).isZero();
    }

    @Test
    void ackOfADeadLetteredMessageIsNotFound() {
        partition = partition(1, Partition.DEFAULT_DRAIN_LIMIT);
        MessageId id = partition.enqueue("m", Priority.HIGH);
        ReceiptHandle receipt = partition.dequeue().orElseThrow().receipt();
        clock.advance(VISIBILITY);
        partition.drainExpired(Integer.MAX_VALUE);

        assertThatThrownBy(() -> partition.ack(id, receipt)).isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    void ackingBeforeTheDeadlineLeavesNothingToExpire() {
        MessageId id = partition.enqueue("m", Priority.HIGH);
        partition.ack(id, partition.dequeue().orElseThrow().receipt());

        clock.advance(VISIBILITY.multipliedBy(2));

        assertThat(partition.drainExpired(Integer.MAX_VALUE)).isZero();
        assertThat(partition.dequeue()).isEmpty();
        assertThat(partition.visibilityDeadlineCount()).isZero();
    }

    @Test
    void oneOperationDrainsAtMostTheLimitAndAFullDrainHandlesTheRest() {
        int limit = 4;
        partition = partition(5, limit);
        for (int i = 0; i <= limit; i++) {
            partition.enqueue("m" + i, Priority.HIGH);
            partition.dequeue().orElseThrow();
        }
        clock.advance(VISIBILITY); // limit + 1 leases expire at the same instant

        partition.enqueue("trigger", Priority.LOW); // any operation drains first

        assertThat(partition.inFlightCount()).isEqualTo(1);
        assertThat(partition.readyCount(Priority.HIGH)).isEqualTo(limit);

        assertThat(partition.drainExpired(Integer.MAX_VALUE)).isEqualTo(1);
        assertThat(partition.inFlightCount()).isZero();
        assertThat(partition.readyCount(Priority.HIGH)).isEqualTo(limit + 1);
    }

    private Partition partition(int maxDeliveries, int drainLimit) {
        return new Partition("orders", 0, QueueConfig.of(VISIBILITY.toSeconds(), maxDeliveries, null), false, clock,
                new SequentialIdGenerator(), new SequentialReceiptGenerator(), new StrictPriorityPolicy(),
                (message, info) -> sink.add(new DeadLettered(message, info)), drainLimit);
    }
}

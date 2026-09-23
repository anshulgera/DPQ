package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class PartitionTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private final FakeClock clock = new FakeClock(START);
    private Partition partition = partition(QueueConfig.of(null, null, null));

    @Test
    void dequeueServesHigherPriorityBeforeOlderLowerPriority() {
        partition.enqueue("low", Priority.LOW);
        partition.enqueue("medium", Priority.MEDIUM);
        partition.enqueue("high", Priority.HIGH);

        assertThat(drainPayloads()).containsExactly("high", "medium", "low");
    }

    @Test
    void samePriorityComesOutFifo() {
        partition.enqueue("a", Priority.MEDIUM);
        partition.enqueue("b", Priority.MEDIUM);
        partition.enqueue("c", Priority.MEDIUM);

        assertThat(drainPayloads()).containsExactly("a", "b", "c");
    }

    @Test
    void dequeueOnAnEmptyPartitionReturnsEmpty() {
        assertThat(partition.dequeue()).isEmpty();
    }

    @Test
    void aDequeuedMessageIsInvisibleToASecondDequeue() {
        partition.enqueue("only", Priority.HIGH);

        assertThat(partition.dequeue()).isPresent();
        assertThat(partition.dequeue()).isEmpty();
    }

    @Test
    void dequeueReturnsTheMessageWithItsLeaseDetails() {
        MessageId id = partition.enqueue("payload", Priority.HIGH);
        clock.advance(Duration.ofSeconds(2));

        DeliveredMessage delivered = partition.dequeue().orElseThrow();

        assertThat(delivered.id()).isEqualTo(id);
        assertThat(delivered.payload()).isEqualTo("payload");
        assertThat(delivered.priority()).isEqualTo(Priority.HIGH);
        assertThat(delivered.deliveryCount()).isEqualTo(1);
        assertThat(delivered.enqueuedAt()).isEqualTo(START);
        assertThat(delivered.visibleUntil()).isEqualTo(START.plusSeconds(2 + 30));
        assertThat(delivered.receipt()).isNotNull();
        assertThat(delivered.deadLetter()).isNull();
    }

    @Test
    void ackWithTheCurrentReceiptRemovesTheMessage() {
        MessageId id = partition.enqueue("m", Priority.LOW);
        DeliveredMessage delivered = partition.dequeue().orElseThrow();

        partition.ack(id, delivered.receipt());

        assertThat(partition.inFlightCount()).isZero();
        assertThat(partition.dequeue()).isEmpty();
        assertThatThrownBy(() -> partition.ack(id, delivered.receipt()))
                .isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    void ackWithTheWrongReceiptIsStale() {
        MessageId id = partition.enqueue("m", Priority.LOW);
        partition.dequeue().orElseThrow();

        assertThatThrownBy(() -> partition.ack(id, new ReceiptHandle("not-the-receipt")))
                .isInstanceOf(StaleReceiptException.class);
        assertThat(partition.inFlightCount()).isEqualTo(1);
    }

    @Test
    void ackOfAReadyMessageThatWasNeverDeliveredIsStale() {
        MessageId id = partition.enqueue("m", Priority.LOW);

        assertThatThrownBy(() -> partition.ack(id, new ReceiptHandle("guess")))
                .isInstanceOf(StaleReceiptException.class);
        assertThat(partition.readyCount(Priority.LOW)).isEqualTo(1);
    }

    @Test
    void ackOfAnUnknownIdIsNotFound() {
        MessageId unknown = new SequentialIdGenerator().next(0);

        assertThatThrownBy(() -> partition.ack(unknown, new ReceiptHandle("r")))
                .isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    void enqueueAtMaxDepthIsRejectedAndInFlightMessagesCount() {
        partition = partition(QueueConfig.of(null, null, 2));
        partition.enqueue("a", Priority.HIGH);
        partition.enqueue("b", Priority.HIGH);
        partition.dequeue().orElseThrow(); // 1 ready + 1 in flight = 2

        assertThatThrownBy(() -> partition.enqueue("c", Priority.HIGH)).isInstanceOf(QueueFullException.class);
        assertThat(partition.readyCount(Priority.HIGH)).isEqualTo(1);
    }

    @Test
    void enqueueSucceedsAgainAfterAnAckFreesSpace() {
        partition = partition(QueueConfig.of(null, null, 1));
        MessageId id = partition.enqueue("a", Priority.HIGH);
        DeliveredMessage delivered = partition.dequeue().orElseThrow();
        assertThatThrownBy(() -> partition.enqueue("b", Priority.HIGH)).isInstanceOf(QueueFullException.class);

        partition.ack(id, delivered.receipt());

        assertThat(partition.enqueue("b", Priority.HIGH)).isNotNull();
    }

    @Test
    void countsTrackEveryOperation() {
        MessageId high = partition.enqueue("h", Priority.HIGH);
        partition.enqueue("m1", Priority.MEDIUM);
        partition.enqueue("m2", Priority.MEDIUM);
        assertCounts(1, 2, 0, 0);

        DeliveredMessage delivered = partition.dequeue().orElseThrow();
        assertThat(delivered.id()).isEqualTo(high);
        assertCounts(0, 2, 0, 1);

        partition.dequeue().orElseThrow();
        assertCounts(0, 1, 0, 2);

        partition.ack(high, delivered.receipt());
        assertCounts(0, 1, 0, 1);

        partition.enqueue("l", Priority.LOW);
        assertCounts(0, 1, 1, 1);
    }

    @Test
    void deliverySeqIncreasesStrictlyAcrossDeliveries() {
        for (int i = 0; i < 5; i++) {
            partition.enqueue("m" + i, Priority.values()[i % 3]);
        }

        List<Long> seqs = new ArrayList<>();
        for (Optional<DeliveredMessage> d = partition.dequeue(); d.isPresent(); d = partition.dequeue()) {
            seqs.add(d.get().deliverySeq());
        }

        assertThat(seqs).hasSize(5).isSorted().doesNotHaveDuplicates();
    }

    @Test
    void eachDeliveryGetsAFreshReceipt() {
        partition.enqueue("a", Priority.HIGH);
        partition.enqueue("b", Priority.HIGH);

        ReceiptHandle first = partition.dequeue().orElseThrow().receipt();
        ReceiptHandle second = partition.dequeue().orElseThrow().receipt();

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void rejectsPayloadsOver256KiB() {
        String max = "x".repeat(256 * 1024);
        String tooBig = "é".repeat(128 * 1024 + 1); // 2 bytes each in UTF-8: one byte over

        assertThat(partition.enqueue(max, Priority.LOW)).isNotNull();
        assertThatThrownBy(() -> partition.enqueue(tooBig, Priority.LOW))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void rejectsMissingPayloadOrPriority() {
        assertThatThrownBy(() -> partition.enqueue(null, Priority.LOW)).isInstanceOf(ValidationException.class);
        assertThatThrownBy(() -> partition.enqueue("p", null)).isInstanceOf(ValidationException.class);
    }

    private Partition partition(QueueConfig config) {
        return new Partition("orders", 0, config, false, clock, new SequentialIdGenerator(), new SequentialReceiptGenerator(),
                new StrictPriorityPolicy(), (message, info, at) -> {}, Partition.DEFAULT_DRAIN_LIMIT);
    }

    private List<String> drainPayloads() {
        List<String> payloads = new ArrayList<>();
        for (Optional<DeliveredMessage> d = partition.dequeue(); d.isPresent(); d = partition.dequeue()) {
            payloads.add(d.get().payload());
        }
        return payloads;
    }

    private void assertCounts(int high, int medium, int low, int inFlight) {
        assertThat(partition.readyCount(Priority.HIGH)).as("ready HIGH").isEqualTo(high);
        assertThat(partition.readyCount(Priority.MEDIUM)).as("ready MEDIUM").isEqualTo(medium);
        assertThat(partition.readyCount(Priority.LOW)).as("ready LOW").isEqualTo(low);
        assertThat(partition.inFlightCount()).as("in flight").isEqualTo(inFlight);
    }
}

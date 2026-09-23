package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** The read-only message listing behind {@code GET /queues/X.dlq} (D18b). */
class ListMessagesTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    private final FakeClock clock = new FakeClock(START);
    private final QueueService service = new QueueService(clock, new SequentialIdGenerator(),
            new SequentialReceiptGenerator(), QueueService.Options.defaults().withoutReaper());

    @AfterEach
    void close() {
        service.close();
    }

    @Test
    void listsDeadLettersInArrivalOrderWithStateAndMetadata() {
        service.createQueue("q", QueueConfig.of(30L, 1, null));
        MessageId first = service.enqueue("q", "first", Priority.LOW, null);
        MessageId second = service.enqueue("q", "second", Priority.HIGH, null);
        service.dequeue("q").orElseThrow(); // second (HIGH)
        clock.advance(Duration.ofSeconds(10));
        service.dequeue("q").orElseThrow(); // first
        clock.advance(Duration.ofSeconds(30)); // both leases have ended: second at 30s, first at 40s
        service.sweep();
        service.dequeue("q.dlq").orElseThrow(); // second is now in flight in the DLQ

        List<MessageView> listed = service.listMessages("q.dlq", 100);

        assertThat(listed).extracting(MessageView::id).containsExactly(second, first);
        assertThat(listed.get(0).state()).isEqualTo(MessageState.IN_FLIGHT);
        assertThat(listed.get(1)).isEqualTo(new MessageView(first, "first", Priority.LOW, MessageState.READY,
                START.plusSeconds(40), 0,
                new DeadLetterInfo("q", 1, DeadLetterReason.MAX_DELIVERIES, START.plusSeconds(40))));
    }

    @Test
    void respectsTheLimit() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        for (int i = 0; i < 5; i++) {
            service.enqueue("q", "m" + i, Priority.MEDIUM, null);
        }

        assertThat(service.listMessages("q", 2)).extracting(MessageView::payload).containsExactly("m0", "m1");
    }

    @Test
    void listingDoesNotLeaseOrChangeAnything() {
        service.createQueue("q", QueueConfig.of(null, null, null));
        MessageId id = service.enqueue("q", "m", Priority.MEDIUM, null);

        service.listMessages("q", 100);

        assertThat(service.dequeue("q")).map(DeliveredMessage::id).contains(id);
        assertThat(service.metrics("q").delivered().get(Priority.MEDIUM)).isEqualTo(1);
    }

    @Test
    void rejectsANonPositiveLimitAndUnknownQueues() {
        service.createQueue("q", QueueConfig.of(null, null, null));

        assertThatThrownBy(() -> service.listMessages("q", 0)).isInstanceOf(ValidationException.class);
        assertThatThrownBy(() -> service.listMessages("nope", 10)).isInstanceOf(QueueNotFoundException.class);
    }
}

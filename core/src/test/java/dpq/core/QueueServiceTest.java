package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class QueueServiceTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final QueueConfig ONE_DELIVERY = QueueConfig.of(30L, 1, null);

    private final FakeClock clock = new FakeClock(START);
    private QueueService service = service(QueueService.Options.defaults().withoutReaper());

    @AfterEach
    void close() {
        service.close();
    }

    @Test
    void creatingTheSameQueueTwiceIsIdempotentButADifferentConfigConflicts() {
        assertThat(service.createQueue("foo", QueueConfig.of(null, null, null))).isTrue();
        assertThat(service.createQueue("foo", QueueConfig.of(30L, 5, 10_000))).isFalse();

        assertThatThrownBy(() -> service.createQueue("foo", QueueConfig.of(60L, null, null)))
                .isInstanceOf(QueueAlreadyExistsException.class);
        assertThat(service.getQueue("foo").config()).isEqualTo(QueueConfig.of(null, null, null));
    }

    @Test
    void creatingAQueueAlsoCreatesItsDeadLetterQueue() {
        service.createQueue("foo", QueueConfig.of(120L, 3, 50));

        QueueDescription dlq = service.getQueue("foo.dlq");
        assertThat(service.hasQueue("foo")).isTrue();
        assertThat(service.hasQueue("foo.dlq")).isTrue();
        assertThat(service.hasQueue("bar")).isFalse();

        assertThat(dlq.deadLetterQueue()).isTrue();
        assertThat(dlq.config()).isEqualTo(QueueConfig.of(null, null, null)); // fixed, not the source's (D18b)
        assertThat(service.getQueue("foo").deadLetterQueue()).isFalse();
    }

    @Test
    void userQueueNamesCannotUseTheReservedDlqForm() {
        assertThatThrownBy(() -> service.createQueue("x.dlq", QueueConfig.of(null, null, null)))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void producersCannotEnqueueIntoADeadLetterQueue() {
        service.createQueue("foo", QueueConfig.of(null, null, null));

        assertThatThrownBy(() -> service.enqueue("foo.dlq", "m", Priority.HIGH, null))
                .isInstanceOf(ValidationException.class);
        assertThat(service.dequeue("foo.dlq")).isEmpty();
    }

    @Test
    void concurrentCreatesOfTheSameQueueAllSucceedAndCreateItOnce() throws Exception {
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return service.createQueue("foo", QueueConfig.of(null, null, null));
                }));
            }
            start.countDown();
            int created = 0;
            for (Future<Boolean> r : results) {
                created += r.get() ? 1 : 0;
            }
            assertThat(created).isEqualTo(1);
        }

        assertThat(service.queueNames()).containsExactlyInAnyOrder("foo", "foo.dlq");
    }

    @Test
    void creatingOneQueueOverTheCapIsRejectedAndDlqsDoNotCount() {
        service = service(QueueService.Options.defaults().withoutReaper().withMaxQueues(2));
        service.createQueue("a", QueueConfig.of(null, null, null));
        service.createQueue("b", QueueConfig.of(null, null, null));

        assertThatThrownBy(() -> service.createQueue("c", QueueConfig.of(null, null, null)))
                .isInstanceOf(QueueLimitExceededException.class);
        assertThat(service.createQueue("a", QueueConfig.of(null, null, null))).isFalse(); // repeats still fine
        assertThat(service.queueNames()).containsExactlyInAnyOrder("a", "a.dlq", "b", "b.dlq");
    }

    @Test
    void operationsOnAnUnknownQueueAreNotFound() {
        MessageId id = new MessageId(0, new UUID(0, 1));

        assertThatThrownBy(() -> service.getQueue("nope")).isInstanceOf(QueueNotFoundException.class);
        assertThatThrownBy(() -> service.enqueue("nope", "m", Priority.LOW, null))
                .isInstanceOf(QueueNotFoundException.class);
        assertThatThrownBy(() -> service.dequeue("nope")).isInstanceOf(QueueNotFoundException.class);
        assertThatThrownBy(() -> service.ack("nope", id, new ReceiptHandle("r")))
                .isInstanceOf(QueueNotFoundException.class);
    }

    @Test
    void ackOfAnIdForAPartitionTheQueueDoesNotHaveIsNotFound() {
        service.createQueue("foo", QueueConfig.of(null, null, null));

        assertThatThrownBy(() -> service.ack("foo", new MessageId(7, new UUID(0, 1)), new ReceiptHandle("r")))
                .isInstanceOf(MessageNotFoundException.class);
    }

    @Test
    void aMessageThatExhaustsItsDeliveriesLandsInTheDlqWithMetadata() {
        service.createQueue("foo", ONE_DELIVERY);
        MessageId id = service.enqueue("foo", "poison", Priority.MEDIUM, Duration.ofHours(1));
        service.dequeue("foo").orElseThrow();
        clock.advance(Duration.ofSeconds(30));

        assertThat(service.dequeue("foo")).isEmpty(); // drains the expired lease into the DLQ

        DeliveredMessage dead = service.dequeue("foo.dlq").orElseThrow();
        assertThat(dead.id()).isEqualTo(id); // keeps its ID (D18c)
        assertThat(dead.payload()).isEqualTo("poison");
        assertThat(dead.priority()).isEqualTo(Priority.MEDIUM);
        assertThat(dead.deliveryCount()).isEqualTo(1); // the DLQ's own count
        assertThat(dead.enqueuedAt()).isEqualTo(START.plusSeconds(30)); // dead-letter time (D18c)
        assertThat(dead.deadLetter()).isEqualTo(
                new DeadLetterInfo("foo", 1, DeadLetterReason.MAX_DELIVERIES, START.plusSeconds(30)));

        service.ack("foo.dlq", dead.id(), dead.receipt());
        assertThat(service.dequeue("foo.dlq")).isEmpty();
    }

    @Test
    void aDeadLetteredMessageLosesItsTtl() {
        service.createQueue("foo", ONE_DELIVERY);
        service.enqueue("foo", "m", Priority.HIGH, Duration.ofSeconds(40));
        service.dequeue("foo").orElseThrow();
        clock.advance(Duration.ofSeconds(30)); // lease expires before the TTL: dead-lettered
        service.dequeue("foo");

        clock.advance(Duration.ofDays(1));

        assertThat(service.dequeue("foo.dlq")).isPresent();
    }

    @Test
    void theDlqAcceptsMessagesPastTheSourceMaxDepthAndHasNoDlqOfItsOwn() {
        service.createQueue("foo", QueueConfig.of(30L, 1, 1));
        for (int i = 0; i < 3; i++) {
            service.enqueue("foo", "m" + i, Priority.LOW, null);
            service.dequeue("foo").orElseThrow();
            clock.advance(Duration.ofSeconds(30));
        }
        service.dequeue("foo");

        for (int i = 0; i < 3; i++) {
            assertThat(service.dequeue("foo.dlq")).isPresent();
        }
        assertThatThrownBy(() -> service.getQueue("foo.dlq.dlq")).isInstanceOf(QueueNotFoundException.class);
    }

    @Test
    void dlqMessagesAreRedeliveredWithoutALimit() {
        service.createQueue("foo", ONE_DELIVERY);
        service.enqueue("foo", "m", Priority.LOW, null);
        service.dequeue("foo").orElseThrow();
        clock.advance(Duration.ofSeconds(30));
        service.dequeue("foo");

        for (int attempt = 1; attempt <= 10; attempt++) {
            assertThat(service.dequeue("foo.dlq")).map(DeliveredMessage::deliveryCount).contains(attempt);
            clock.advance(QueueConfig.DEFAULT_VISIBILITY_TIMEOUT);
        }
    }

    @Test
    void aSweepExpiresLeasesOnAnIdleQueue() {
        service.createQueue("foo", ONE_DELIVERY);
        service.enqueue("foo", "m", Priority.LOW, null);
        service.dequeue("foo").orElseThrow();
        clock.advance(Duration.ofSeconds(30));

        service.sweep(); // what the reaper runs; no queue operation is called

        assertThat(service.partition("foo").inFlightCount()).isZero();
        assertThat(service.partition("foo.dlq").readyCount(Priority.LOW)).isEqualTo(1);
    }

    private QueueService service(QueueService.Options options) {
        return new QueueService(clock, new SequentialIdGenerator(), new SequentialReceiptGenerator(), options);
    }
}

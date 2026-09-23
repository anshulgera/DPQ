package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import dpq.core.error.DpqException;
import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.DeliveredMessage;
import dpq.core.model.MessageId;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.model.ReceiptHandle;
import dpq.core.time.FakeClock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.state.Action;
import net.jqwik.api.state.ActionChain;
import net.jqwik.api.state.Transformer;

/**
 * Model-based tests (D13b): random action sequences run against the real {@link QueueService} and the naive
 * {@link ModelQueue}, whose results must match after every step. The real service runs with no reaper and an
 * unlimited drain, and both sides use the same deterministic IDs and receipts, so outcomes compare exactly.
 */
class QueueModelProperties {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    // Small limits so that depth, redelivery, DLQ and TTL boundaries come up often.
    private static final QueueConfig CONFIG = QueueConfig.of(10L, 2, 6);

    enum ReceiptChoice { CURRENT, FIRST_EVER, BOGUS }

    /** Both implementations plus the IDs and receipts seen so far, which acks pick from. */
    static final class Harness {
        final FakeClock clock = new FakeClock(START);
        final QueueService real = new QueueService(clock, new SequentialIdGenerator(),
                new SequentialReceiptGenerator(),
                QueueService.Options.defaults().withoutReaper().withDrainLimit(Integer.MAX_VALUE));
        final ModelQueue model = new ModelQueue(START, CONFIG);
        final List<MessageId> issued = new ArrayList<>();
        final Map<MessageId, List<ReceiptHandle>> receipts = new HashMap<>();

        Harness() {
            real.createQueue(ModelQueue.NAME, CONFIG);
        }

        void enqueue(Priority priority, Duration ttl) {
            Object real = outcome(() -> this.real.enqueue(ModelQueue.NAME, "payload", priority, ttl));
            Object model = outcome(() -> this.model.enqueue("payload", priority, ttl));
            assertThat(real).isEqualTo(model);
            if (model instanceof MessageId id) {
                issued.add(id);
            }
        }

        void dequeue() {
            Object real = outcome(() -> this.real.dequeue(ModelQueue.NAME));
            Object model = outcome(this.model::dequeue);
            assertThat(real).isEqualTo(model);
            if (model instanceof java.util.Optional<?> delivered && delivered.isPresent()) {
                DeliveredMessage d = (DeliveredMessage) delivered.get();
                receipts.computeIfAbsent(d.id(), k -> new ArrayList<>()).add(d.receipt());
            }
        }

        void ack(int idChoice, ReceiptChoice receiptChoice) {
            MessageId id = issued.isEmpty() || idChoice >= issued.size()
                    ? new MessageId(0, new UUID(1, idChoice)) // never issued
                    : issued.get(idChoice);
            List<ReceiptHandle> seen = receipts.getOrDefault(id, List.of());
            ReceiptHandle receipt = switch (receiptChoice) {
                case CURRENT -> seen.isEmpty() ? new ReceiptHandle("bogus") : seen.get(seen.size() - 1);
                case FIRST_EVER -> seen.isEmpty() ? new ReceiptHandle("bogus") : seen.get(0);
                case BOGUS -> new ReceiptHandle("bogus");
            };
            Object real = outcome(() -> {
                this.real.ack(ModelQueue.NAME, id, receipt);
                return "acked";
            });
            Object model = outcome(() -> {
                this.model.ack(id, receipt);
                return "acked";
            });
            assertThat(real).isEqualTo(model);
        }

        void advance(long millis) {
            clock.advance(Duration.ofMillis(millis));
            model.advance(millis);
        }

        void checkMetrics() {
            assertThat(real.metrics(ModelQueue.NAME)).isEqualTo(model.metrics());
            assertThat(real.metrics(ModelQueue.DLQ)).isEqualTo(model.dlqMetrics());
        }

        @Override
        public String toString() {
            return "Harness";
        }
    }

    @Property(tries = 1000)
    void theServiceBehavesLikeTheModel(@ForAll("chains") ActionChain<Harness> chain) {
        chain.run();
    }

    @Provide
    Arbitrary<ActionChain<Harness>> chains() {
        return ActionChain.startWith(Harness::new)
                .withAction(4, enqueue())
                .withAction(4, Action.just(Transformer.mutate("dequeue", Harness::dequeue)))
                .withAction(3, ack())
                .withAction(3, advance())
                .withAction(1, Action.just(Transformer.mutate("checkMetrics", Harness::checkMetrics)))
                .withMaxTransformations(60);
    }

    private static Action.Independent<Harness> enqueue() {
        Arbitrary<Duration> ttl = Arbitraries.frequencyOf(
                        net.jqwik.api.Tuple.of(1, Arbitraries.just(Duration.ZERO)), // invalid: rejected by both
                        net.jqwik.api.Tuple.of(6, Arbitraries.integers().between(1, 30).map(Duration::ofSeconds)))
                .injectNull(0.4);
        return () -> Combinators.combine(Arbitraries.of(Priority.class), ttl)
                .as((priority, t) -> Transformer.mutate(
                        "enqueue " + priority + " ttl=" + t, (Harness h) -> h.enqueue(priority, t)));
    }

    private static Action.Independent<Harness> ack() {
        return () -> Combinators.combine(Arbitraries.integers().between(0, 30), Arbitraries.of(ReceiptChoice.class))
                .as((idChoice, receipt) -> Transformer.mutate(
                        "ack #" + idChoice + " " + receipt, (Harness h) -> h.ack(idChoice, receipt)));
    }

    private static Action.Independent<Harness> advance() {
        Arbitrary<Long> millis = Arbitraries.oneOf(
                Arbitraries.of(0L, 1L, 999L, 1_000L, 9_999L, 10_000L), // lease and second boundaries
                Arbitraries.longs().between(0, 60_000));
        return () -> millis.map(ms -> Transformer.mutate("advance " + ms + "ms", (Harness h) -> h.advance(ms)));
    }

    private static Object outcome(Supplier<?> operation) {
        try {
            return operation.get();
        } catch (DpqException e) {
            return e.getClass();
        }
    }
}

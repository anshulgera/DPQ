package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import dpq.core.id.SequentialIdGenerator;
import dpq.core.id.SequentialReceiptGenerator;
import dpq.core.model.DeliveredMessage;
import dpq.core.model.MessageId;
import dpq.core.model.Priority;
import dpq.core.model.QueueConfig;
import dpq.core.time.Clock;
import dpq.core.time.FakeClock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Regression found by the conservation stress test (PR 9): displayed times must be the wall-clock form of the
 * monotonic instants the logic used, even if the clock moves while an operation runs. Otherwise
 * {@code visibleUntil} can promise a lease that ends later than it really does.
 */
class ClockConsistencyTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    /** A clock that moves forward 1ms every time wall time is read, as if another thread advanced it. */
    private static final class MovingOnWallReadClock implements Clock {
        private long millis;

        @Override
        public long monotonicMillis() {
            return millis;
        }

        @Override
        public Instant wallTime() {
            millis++;
            return START.plusMillis(millis);
        }

        @Override
        public Instant wallTimeAt(long monotonicMillis) {
            return START.plusMillis(monotonicMillis);
        }

        void set(long value) {
            millis = value;
        }
    }

    private final MovingOnWallReadClock clock = new MovingOnWallReadClock();
    private final Partition partition = new Partition("q", 0, QueueConfig.of(30L, null, null), false, clock,
            new SequentialIdGenerator(), new SequentialReceiptGenerator(), new StrictPriorityPolicy(),
            (m, info, at) -> {}, Partition.DEFAULT_DRAIN_LIMIT);

    @Test
    void visibleUntilIsExactlyWhenTheLeaseEnds() {
        MessageId id = partition.enqueue("m", Priority.HIGH);
        clock.set(100);

        DeliveredMessage d = partition.dequeue().orElseThrow();

        assertThat(d.visibleUntil()).isEqualTo(START.plusMillis(100 + 30_000));
        clock.set(100 + 30_000 - 1);
        partition.ack(id, d.receipt()); // still valid 1ms before visibleUntil
    }

    @Test
    void enqueuedAtIsTheWallTimeOfTheEnqueue() {
        clock.set(42);
        partition.enqueue("m", Priority.HIGH);

        assertThat(partition.dequeue().orElseThrow().enqueuedAt()).isEqualTo(START.plusMillis(42));
    }

    @Test
    void fakeClockConvertsMonotonicInstantsExactly() {
        FakeClock fake = new FakeClock(START);
        fake.advance(Duration.ofSeconds(5));

        assertThat(fake.wallTimeAt(fake.monotonicMillis())).isEqualTo(fake.wallTime());
        assertThat(fake.wallTimeAt(1_000)).isEqualTo(START.plusSeconds(1));
    }
}

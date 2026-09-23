package dpq.core.id;

import dpq.core.model.MessageId;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Deterministic {@link IdGenerator} for tests: the n-th ID is {@code p{partition}-00000000-…-{n}}. */
public final class SequentialIdGenerator implements IdGenerator {

    private final AtomicLong counter = new AtomicLong();

    @Override
    public MessageId next(int partition) {
        return new MessageId(partition, new UUID(0, counter.incrementAndGet()));
    }
}

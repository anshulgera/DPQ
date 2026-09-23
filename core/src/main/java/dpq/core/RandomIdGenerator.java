package dpq.core;

import java.util.random.RandomGenerator;

/** Production {@link IdGenerator}: UUIDv7 IDs, unique across nodes without coordination (D11a). */
public final class RandomIdGenerator implements IdGenerator {

    private final UuidV7 uuids;

    public RandomIdGenerator(Clock clock, RandomGenerator random) {
        this.uuids = new UuidV7(clock, random);
    }

    @Override
    public MessageId next(int partition) {
        return new MessageId(partition, uuids.next());
    }
}

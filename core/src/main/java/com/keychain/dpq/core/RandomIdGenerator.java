package com.keychain.dpq.core;

import java.util.random.RandomGenerator;

public final class RandomIdGenerator implements IdGenerator {

    public RandomIdGenerator(Clock clock, RandomGenerator random) {}

    @Override
    public MessageId next(int partition) {
        throw new UnsupportedOperationException("not implemented");
    }
}

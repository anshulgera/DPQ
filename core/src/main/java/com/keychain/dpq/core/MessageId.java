package com.keychain.dpq.core;

import java.util.UUID;

public record MessageId(int partition, UUID uuid) {

    public static MessageId parse(String text) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException("not implemented");
    }
}

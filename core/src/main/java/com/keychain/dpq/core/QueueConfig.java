package com.keychain.dpq.core;

import java.time.Duration;

public record QueueConfig(Duration visibilityTimeout, int maxDeliveries, int maxDepth) {

    public static QueueConfig of(Long visibilityTimeoutSeconds, Integer maxDeliveries, Integer maxDepth) {
        throw new UnsupportedOperationException("not implemented");
    }
}

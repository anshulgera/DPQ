package dpq.core.model;

import dpq.core.error.ValidationException;
import java.time.Duration;

/**
 * Per-queue settings (D11b, D11c). Build with {@link #of} so omitted fields take their defaults; configs are
 * compared after defaults are applied, so omitting a field equals sending its default.
 */
public record QueueConfig(Duration visibilityTimeout, int maxDeliveries, int maxDepth) {

    public static final Duration DEFAULT_VISIBILITY_TIMEOUT = Duration.ofSeconds(30);
    public static final int DEFAULT_MAX_DELIVERIES = 5;
    public static final int DEFAULT_MAX_DEPTH = 10_000;

    private static final Duration MIN_VISIBILITY_TIMEOUT = Duration.ofSeconds(1);
    private static final Duration MAX_VISIBILITY_TIMEOUT = Duration.ofHours(12);
    private static final int MAX_MAX_DELIVERIES = 1000;

    public QueueConfig {
        if (visibilityTimeout == null
                || visibilityTimeout.compareTo(MIN_VISIBILITY_TIMEOUT) < 0
                || visibilityTimeout.compareTo(MAX_VISIBILITY_TIMEOUT) > 0) {
            throw new ValidationException("visibilityTimeout must be between 1s and 12h, was " + visibilityTimeout);
        }
        if (maxDeliveries < 1 || maxDeliveries > MAX_MAX_DELIVERIES) {
            throw new ValidationException("maxDeliveries must be between 1 and 1000, was " + maxDeliveries);
        }
        if (maxDepth < 1) {
            throw new ValidationException("maxDepth must be at least 1, was " + maxDepth);
        }
    }

    /** Builds a config from optional fields; {@code null} means "use the default". */
    public static QueueConfig of(Long visibilityTimeoutSeconds, Integer maxDeliveries, Integer maxDepth) {
        return new QueueConfig(
                visibilityTimeoutSeconds == null
                        ? DEFAULT_VISIBILITY_TIMEOUT
                        : Duration.ofSeconds(visibilityTimeoutSeconds),
                maxDeliveries == null ? DEFAULT_MAX_DELIVERIES : maxDeliveries,
                maxDepth == null ? DEFAULT_MAX_DEPTH : maxDepth);
    }
}

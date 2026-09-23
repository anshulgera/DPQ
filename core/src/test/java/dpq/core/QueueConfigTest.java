package com.keychain.dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class QueueConfigTest {

    @Test
    void omittedFieldsTakeTheDefaults() {
        QueueConfig config = QueueConfig.of(null, null, null);

        assertThat(config.visibilityTimeout()).isEqualTo(Duration.ofSeconds(30));
        assertThat(config.maxDeliveries()).isEqualTo(5);
        assertThat(config.maxDepth()).isEqualTo(10_000);
    }

    @Test
    void omittedFieldsEqualExplicitDefaults() {
        assertThat(QueueConfig.of(null, null, null)).isEqualTo(QueueConfig.of(30L, 5, 10_000));
        assertThat(QueueConfig.of(60L, null, null)).isEqualTo(QueueConfig.of(60L, 5, 10_000));
        assertThat(QueueConfig.of(60L, null, null)).isNotEqualTo(QueueConfig.of(null, null, null));
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 12 * 3600 + 1})
    void rejectsVisibilityTimeoutOutsideOneSecondToTwelveHours(long seconds) {
        assertThatThrownBy(() -> QueueConfig.of(seconds, null, null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("visibilityTimeout");
    }

    @Test
    void rejectsSubSecondVisibilityTimeout() {
        assertThatThrownBy(() -> new QueueConfig(Duration.ofMillis(999), 5, 10_000))
                .isInstanceOf(ValidationException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 1001})
    void rejectsMaxDeliveriesOutsideOneToAThousand(int maxDeliveries) {
        assertThatThrownBy(() -> QueueConfig.of(null, maxDeliveries, null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("maxDeliveries");
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void rejectsMaxDepthBelowOne(int maxDepth) {
        assertThatThrownBy(() -> QueueConfig.of(null, null, maxDepth))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("maxDepth");
    }

    @Test
    void acceptsTheBoundaryValues() {
        assertThat(QueueConfig.of(1L, 1, 1).visibilityTimeout()).isEqualTo(Duration.ofSeconds(1));
        assertThat(QueueConfig.of(12 * 3600L, 1000, 1).maxDeliveries()).isEqualTo(1000);
    }
}

package com.keychain.dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class QueueNamesTest {

    @ParameterizedTest
    @ValueSource(strings = {"orders", "A", "order-events_v2", "0123456789"})
    void acceptsValidUserNames(String name) {
        assertThat(QueueNames.requireValidUserName(name)).isEqualTo(name);
    }

    @Test
    void acceptsEightyCharacters() {
        String name = "a".repeat(80);
        assertThat(QueueNames.requireValidUserName(name)).isEqualTo(name);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "x.dlq", "a.b", "has space", "slash/name", "ümlaut", "tab\t"})
    void rejectsEmptyNamesAndBadCharacters(String name) {
        assertThatThrownBy(() -> QueueNames.requireValidUserName(name)).isInstanceOf(ValidationException.class);
    }

    @Test
    void rejectsEightyOneCharacters() {
        assertThatThrownBy(() -> QueueNames.requireValidUserName("a".repeat(81)))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(() -> QueueNames.requireValidUserName(null)).isInstanceOf(ValidationException.class);
    }
}

package com.keychain.dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CoreSmokeTest {

    @Test
    void testsRunOnTheJava21Toolchain() {
        assertThat(Runtime.version().feature()).isEqualTo(21);
    }
}

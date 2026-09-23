package com.keychain.dpq.harness;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class HarnessSmokeTest {

    @Test
    void testsRunOnTheJava21Toolchain() {
        assertThat(Runtime.version().feature()).isEqualTo(21);
    }
}

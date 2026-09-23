package dpq.server;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ServerSmokeTest {

    @Test
    void testsRunOnTheJava21Toolchain() {
        assertThat(Runtime.version().feature()).isEqualTo(21);
    }
}

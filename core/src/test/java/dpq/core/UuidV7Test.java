package com.keychain.dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

class UuidV7Test {

    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T00:00:00Z"));
    private final UuidV7 generator = new UuidV7(clock, new Random(42));

    @Test
    void setsVersionSevenAndTheRfcVariant() {
        UUID uuid = generator.next();

        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2);
    }

    @Test
    void embedsTheWallClockMillis() {
        assertThat(timestampOf(generator.next())).isEqualTo(clock.wallTime().toEpochMilli());

        clock.advance(Duration.ofMillis(5));

        assertThat(timestampOf(generator.next())).isEqualTo(clock.wallTime().toEpochMilli());
    }

    @Test
    void isStrictlyIncreasingWithinTheSameMillisecond() {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            ids.add(generator.next());
        }

        // The canonical lowercase-hex form sorts in the same order as the unsigned 128-bit value.
        for (int i = 1; i < ids.size(); i++) {
            assertThat(ids.get(i).toString()).isGreaterThan(ids.get(i - 1).toString());
        }
        assertThat(ids).allSatisfy(id -> {
            assertThat(id.version()).isEqualTo(7);
            assertThat(id.variant()).isEqualTo(2);
            assertThat(timestampOf(id)).isEqualTo(clock.wallTime().toEpochMilli());
        });
    }

    @Test
    void staysIncreasingAcrossMilliseconds() {
        UUID first = generator.next();
        clock.advance(Duration.ofMillis(1));
        UUID second = generator.next();

        assertThat(second.toString()).isGreaterThan(first.toString());
    }

    @Test
    void borrowsTheNextMillisecondWhenTheRandomFieldIsExhausted() {
        RandomGenerator allOnes = () -> -1L; // starts the 74-bit random field at its maximum
        UuidV7 saturated = new UuidV7(clock, allOnes);
        long now = clock.wallTime().toEpochMilli();

        UUID first = saturated.next();
        UUID second = saturated.next();

        assertThat(timestampOf(first)).isEqualTo(now);
        assertThat(timestampOf(second)).isEqualTo(now + 1);
        assertThat(second.toString()).isGreaterThan(first.toString());
        assertThat(second.version()).isEqualTo(7);
        assertThat(second.variant()).isEqualTo(2);
    }

    private static long timestampOf(UUID uuid) {
        return uuid.getMostSignificantBits() >>> 16;
    }
}

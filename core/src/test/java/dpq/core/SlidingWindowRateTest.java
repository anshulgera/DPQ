package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SlidingWindowRateTest {

    private final SlidingWindowRate rate = new SlidingWindowRate();

    @Test
    void twoEventsASecondForSixtySecondsIsTwoPerSecond() {
        long now = 0;
        for (int second = 0; second < 60; second++) {
            rate.record(now);
            rate.record(now + 500);
            now += 1000;
        }

        assertThat(rate.perSecond(now)).isEqualTo(2.0);
    }

    @Test
    void theCurrentIncompleteSecondIsNotCounted() {
        rate.record(5_000);

        assertThat(rate.perSecond(5_999)).isZero();
        assertThat(rate.perSecond(6_000)).isEqualTo(1.0 / 60);
    }

    @Test
    void eventsOlderThanSixtySecondsAreEvicted() {
        rate.record(0); // second 0
        rate.record(30_000); // second 30

        assertThat(rate.perSecond(60_000)).isEqualTo(2.0 / 60); // window covers seconds 0..59
        assertThat(rate.perSecond(61_000)).isEqualTo(1.0 / 60); // second 0 has left the window
        assertThat(rate.perSecond(91_000)).isZero(); // after 60 idle seconds
    }

    @Test
    void aSlotReusedAfterWrappingStartsFromZero() {
        rate.record(1_000); // second 1
        rate.record(62_000); // second 62 reuses second 1's slot

        assertThat(rate.perSecond(63_000)).isEqualTo(1.0 / 60);
    }

    @Test
    void worksWithNegativeMonotonicTimes() {
        rate.record(-1_500); // System.nanoTime() may be negative

        assertThat(rate.perSecond(-500)).isEqualTo(1.0 / 60);
    }
}

package com.keychain.dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class FakeClockTest {

    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void advanceMovesMonotonicAndWallTimeForwardExactly() {
        FakeClock clock = new FakeClock(START);
        long before = clock.monotonicMillis();

        clock.advance(Duration.ofMillis(1234));

        assertThat(clock.monotonicMillis()).isEqualTo(before + 1234);
        assertThat(clock.wallTime()).isEqualTo(START.plusMillis(1234));
    }

    @Test
    void concurrentAdvancesFromEightThreadsSumExactly() throws Exception {
        FakeClock clock = new FakeClock(START);
        long before = clock.monotonicMillis();
        int threads = 8;
        int advancesPerThread = 10_000;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();

        try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                long step = t + 1; // threads advance by different amounts
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < advancesPerThread; i++) {
                        clock.advance(Duration.ofMillis(step));
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        }

        long expected = (long) advancesPerThread * (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8);
        assertThat(clock.monotonicMillis()).isEqualTo(before + expected);
        assertThat(clock.wallTime()).isEqualTo(START.plusMillis(expected));
    }
}

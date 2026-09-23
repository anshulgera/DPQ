package dpq.core;

import java.util.concurrent.atomic.AtomicLong;

/** Deterministic {@link ReceiptGenerator} for tests: the n-th receipt is n as 32 hex digits. */
public final class SequentialReceiptGenerator implements ReceiptGenerator {

    private final AtomicLong counter = new AtomicLong();

    @Override
    public ReceiptHandle next() {
        return new ReceiptHandle("%032x".formatted(counter.incrementAndGet()));
    }
}

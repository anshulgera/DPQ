package dpq.core;

import java.security.SecureRandom;
import java.util.HexFormat;

/** Production {@link ReceiptGenerator}: 128 random bits as lowercase hex, so receipts can't be guessed. */
public final class RandomReceiptGenerator implements ReceiptGenerator {

    private final SecureRandom random = new SecureRandom();

    @Override
    public ReceiptHandle next() {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        return new ReceiptHandle(HexFormat.of().formatHex(bytes));
    }
}

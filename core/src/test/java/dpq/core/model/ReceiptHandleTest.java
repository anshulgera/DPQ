package dpq.core.model;

import static org.assertj.core.api.Assertions.assertThat;

import dpq.core.id.RandomReceiptGenerator;
import dpq.core.id.ReceiptGenerator;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReceiptHandleTest {

    @Test
    void randomReceiptsAre128BitLowercaseHex() {
        ReceiptHandle receipt = new RandomReceiptGenerator().next();

        assertThat(receipt.value()).matches("[0-9a-f]{32}");
    }

    @Test
    void randomReceiptsAreUniqueAcross100kGenerations() {
        ReceiptGenerator receipts = new RandomReceiptGenerator();
        Set<ReceiptHandle> seen = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            seen.add(receipts.next());
        }

        assertThat(seen).hasSize(100_000);
    }
}

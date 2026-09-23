package dpq.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MessageIdTest {

    private static final String UUID_TEXT = "0190a2b3-c4d5-7e6f-8a9b-0c1d2e3f4a5b";

    @Test
    void formatsAsPartitionPrefixAndUuid() {
        MessageId id = new MessageId(3, UUID.fromString(UUID_TEXT));

        assertThat(id.toString()).isEqualTo("p3-" + UUID_TEXT);
    }

    @Test
    void roundTripsGeneratedIds() {
        IdGenerator ids = new RandomIdGenerator(new FakeClock(Instant.parse("2026-01-01T00:00:00Z")), new Random(7));
        for (int partition : new int[] {0, 1, 42, Integer.MAX_VALUE}) {
            MessageId id = ids.next(partition);

            assertThat(id.partition()).isEqualTo(partition);
            assertThat(MessageId.parse(id.toString())).isEqualTo(id);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        UUID_TEXT,                                  // no prefix
        "3-" + UUID_TEXT,                           // missing 'p'
        "p-" + UUID_TEXT,                           // missing partition
        "px-" + UUID_TEXT,                          // non-numeric partition
        "p-1-" + UUID_TEXT,                         // negative partition
        "p03-" + UUID_TEXT,                         // leading zero would not round-trip
        "p99999999999-" + UUID_TEXT,                // partition overflows int
        "p3_" + UUID_TEXT,                          // wrong separator
        "p3-1-1-1-1-1",                             // short-form UUID that UUID.fromString accepts
        "p3-0190A2B3-C4D5-7E6F-8A9B-0C1D2E3F4A5B",  // uppercase would not round-trip
        "p3-" + UUID_TEXT + "x"                     // trailing garbage
    })
    void rejectsMalformedIds(String text) {
        assertThatThrownBy(() -> MessageId.parse(text)).isInstanceOf(ValidationException.class);
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(() -> MessageId.parse(null)).isInstanceOf(ValidationException.class);
    }
}

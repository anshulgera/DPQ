package com.keychain.dpq.core;

import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Message ID of the form {@code p{partition}-{uuid}} (D11a); the prefix routes an ack to its partition. */
public record MessageId(int partition, UUID uuid) {

    // Canonical forms only (no leading zeros, lowercase hex), so parse and toString round-trip exactly.
    private static final Pattern FORMAT = Pattern.compile(
            "p(0|[1-9][0-9]{0,9})-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})");

    public MessageId {
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be non-negative, was " + partition);
        }
        Objects.requireNonNull(uuid, "uuid");
    }

    public static MessageId parse(String text) {
        Matcher m = text == null ? null : FORMAT.matcher(text);
        if (m == null || !m.matches()) {
            throw new ValidationException("malformed message ID");
        }
        long partition = Long.parseLong(m.group(1));
        if (partition > Integer.MAX_VALUE) {
            throw new ValidationException("malformed message ID");
        }
        return new MessageId((int) partition, UUID.fromString(m.group(2)));
    }

    @Override
    public String toString() {
        return "p" + partition + "-" + uuid;
    }
}

package com.keychain.dpq.core;

/** The receipt doesn't match a current lease: an older delivery's receipt, or the lease deadline has passed (HTTP 409, D8d). */
public final class StaleReceiptException extends DpqException {

    public StaleReceiptException(MessageId id) {
        super("receipt is stale or the lease is over: " + id);
    }
}

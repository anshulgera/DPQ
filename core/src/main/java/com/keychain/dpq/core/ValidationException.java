package com.keychain.dpq.core;

/** Invalid input: a bad queue name, config value, payload, TTL or message ID (HTTP 400). */
public final class ValidationException extends DpqException {

    public ValidationException(String message) {
        super(message);
    }
}

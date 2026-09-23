package com.keychain.dpq.core;

/** Opaque token identifying one delivery of a message (D8a). */
public record ReceiptHandle(String value) {}

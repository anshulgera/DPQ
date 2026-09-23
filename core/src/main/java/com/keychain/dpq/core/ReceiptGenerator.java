package com.keychain.dpq.core;

/** Creates a fresh receipt handle per delivery; injectable for deterministic tests (D11a). */
public interface ReceiptGenerator {

    ReceiptHandle next();
}

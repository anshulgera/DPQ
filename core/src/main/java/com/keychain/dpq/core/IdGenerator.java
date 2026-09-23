package com.keychain.dpq.core;

/** Creates message IDs; injectable so tests can use deterministic IDs (D11a). */
public interface IdGenerator {

    MessageId next(int partition);
}

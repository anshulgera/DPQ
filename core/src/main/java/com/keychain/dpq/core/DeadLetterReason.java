package com.keychain.dpq.core;

/** Why a message was dead-lettered. TTL expiry is deliberately not a reason (D9b). */
public enum DeadLetterReason {
    MAX_DELIVERIES
}

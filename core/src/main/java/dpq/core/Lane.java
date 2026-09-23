package com.keychain.dpq.core;

import java.util.Optional;

public final class Lane {

    public static final class Entry {

        public Entry(long seq, MessageId id) {}

        public long seq() {
            throw new UnsupportedOperationException("not implemented");
        }

        public MessageId id() {
            throw new UnsupportedOperationException("not implemented");
        }
    }

    public void offer(Entry entry) {}

    public void offerRetry(Entry entry) {}

    public Optional<Entry> poll() {
        throw new UnsupportedOperationException("not implemented");
    }

    public Optional<Entry> peekOldest() {
        throw new UnsupportedOperationException("not implemented");
    }

    public void markDead(Entry entry) {}

    public boolean isEmpty() {
        throw new UnsupportedOperationException("not implemented");
    }

    public int size() {
        throw new UnsupportedOperationException("not implemented");
    }

    int physicalSize() {
        throw new UnsupportedOperationException("not implemented");
    }
}

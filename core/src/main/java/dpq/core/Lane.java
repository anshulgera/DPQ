package dpq.core;

import dpq.core.model.MessageId;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Optional;
import java.util.PriorityQueue;

/**
 * The messages of one priority in one partition (D4): a retry min-heap by seq, served before a FIFO deque.
 *
 * <p>Under strict FIFO, every message already dequeued has a lower seq than every message still in the deque,
 * so serving redelivered messages from the heap first puts each back in its original position.
 *
 * <p>Removed messages are tombstoned with {@link #markDead} rather than searched for. Dead entries are
 * discarded when they reach a head, and the lane is compacted once they exceed half its physical size, so a
 * starved lane that is never polled can't fill up with them (D18e).
 *
 * <p>Not thread-safe: guarded by the owning partition's lock.
 */
final class Lane {

    /** One message's place in the lane. Only {@code dead} is mutable. */
    static final class Entry {

        private final long seq;
        private final MessageId id;
        private boolean dead;

        Entry(long seq, MessageId id) {
            this.seq = seq;
            this.id = id;
        }

        long seq() {
            return seq;
        }

        MessageId id() {
            return id;
        }
    }

    private final PriorityQueue<Entry> retry = new PriorityQueue<>(Comparator.comparingLong(Entry::seq));
    private final ArrayDeque<Entry> main = new ArrayDeque<>();
    private int deadCount;

    void offer(Entry entry) {
        main.addLast(entry);
    }

    /** Re-adds a redelivered message; it keeps its original seq, so it is served in its original order. */
    void offerRetry(Entry entry) {
        retry.add(entry);
    }

    /** Removes and returns the next live entry: the retry heap's head first, then the deque's. */
    Optional<Entry> poll() {
        purgeDeadHeads();
        Entry next = retry.isEmpty() ? main.pollFirst() : retry.poll();
        return Optional.ofNullable(next);
    }

    /** The entry {@link #poll} would return next, without removing it. */
    Optional<Entry> peek() {
        purgeDeadHeads();
        return Optional.ofNullable(retry.isEmpty() ? main.peekFirst() : retry.peek());
    }

    /** The live entry with the lowest seq, without removing it; used for oldest-message age. */
    Optional<Entry> peekOldest() {
        purgeDeadHeads();
        Entry r = retry.peek();
        Entry m = main.peekFirst();
        if (r == null || m == null) {
            return Optional.ofNullable(r != null ? r : m);
        }
        return Optional.of(r.seq < m.seq ? r : m);
    }

    /** Tombstones an entry still in this lane. Idempotent. */
    void markDead(Entry entry) {
        if (entry.dead) {
            return;
        }
        entry.dead = true;
        deadCount++;
        if (deadCount * 2 > physicalSize()) {
            compact();
        }
    }

    boolean isEmpty() {
        return size() == 0;
    }

    /** Number of live entries. */
    int size() {
        return physicalSize() - deadCount;
    }

    /** Number of stored entries, live or dead. */
    int physicalSize() {
        return retry.size() + main.size();
    }

    private void purgeDeadHeads() {
        while (!retry.isEmpty() && retry.peek().dead) {
            retry.poll();
            deadCount--;
        }
        while (!main.isEmpty() && main.peekFirst().dead) {
            main.pollFirst();
            deadCount--;
        }
    }

    private void compact() {
        retry.removeIf(e -> e.dead);
        main.removeIf(e -> e.dead);
        deadCount = 0;
    }
}

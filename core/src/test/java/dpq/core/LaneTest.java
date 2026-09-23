package com.keychain.dpq.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class LaneTest {

    private final Lane lane = new Lane();

    @Test
    void pollingAnEmptyLaneReturnsEmpty() {
        assertThat(lane.poll()).isEmpty();
        assertThat(lane.peekOldest()).isEmpty();
        assertThat(lane.isEmpty()).isTrue();
    }

    @Test
    void mainEntriesComeOutFifo() {
        offerAll(1, 2, 3);

        assertThat(drainSeqs()).containsExactly(1L, 2L, 3L);
        assertThat(lane.isEmpty()).isTrue();
    }

    @Test
    void retryEntriesAreServedBeforeMainEntries() {
        offerAll(10, 11);
        lane.offerRetry(entry(4));

        assertThat(drainSeqs()).containsExactly(4L, 10L, 11L);
    }

    @Test
    void retriesComeOutInSeqOrderEvenWhenOfferedOutOfOrder() {
        offerAll(10);
        lane.offerRetry(entry(5));
        lane.offerRetry(entry(3));
        lane.offerRetry(entry(7));

        assertThat(drainSeqs()).containsExactly(3L, 5L, 7L, 10L);
    }

    @Test
    void tombstonedEntriesAreSkippedByPollAndPeek() {
        Lane.Entry first = entry(1);
        Lane.Entry retried = entry(0);
        lane.offer(first);
        offerAll(2);
        lane.offerRetry(retried);

        lane.markDead(retried);
        lane.markDead(first);

        assertThat(lane.peekOldest()).map(Lane.Entry::seq).contains(2L);
        assertThat(lane.size()).isEqualTo(1);
        assertThat(drainSeqs()).containsExactly(2L);
    }

    @Test
    void peekOldestIsTheMinimumOverRetryHeapAndDeque() {
        offerAll(1);
        lane.offerRetry(entry(5));

        assertThat(lane.peekOldest()).map(Lane.Entry::seq).contains(1L);
        // The retry heap is still served first, even though its head is not the oldest here.
        assertThat(lane.poll()).map(Lane.Entry::seq).contains(5L);
    }

    @Test
    void peekOldestDoesNotRemoveTheEntry() {
        offerAll(1);

        lane.peekOldest();

        assertThat(lane.poll()).map(Lane.Entry::seq).contains(1L);
    }

    @Test
    void markingAnEntryDeadTwiceCountsOnce() {
        Lane.Entry e = entry(1);
        lane.offer(e);
        offerAll(2, 3, 4);

        lane.markDead(e);
        lane.markDead(e);

        assertThat(lane.size()).isEqualTo(3);
        assertThat(lane.physicalSize()).isEqualTo(4); // 1 of 4 dead: no compaction yet
    }

    @Test
    void tombstoningMostOfTheMiddleCompactsTheLaneAndKeepsFifo() {
        List<Lane.Entry> entries = new ArrayList<>();
        for (long seq = 1; seq <= 10; seq++) {
            Lane.Entry e = entry(seq);
            entries.add(e);
            lane.offer(e);
        }

        for (int i = 2; i <= 7; i++) { // seqs 3..8: 60% of the lane, none at the head
            lane.markDead(entries.get(i));
        }

        assertThat(lane.physicalSize()).isEqualTo(4);
        assertThat(lane.size()).isEqualTo(4);
        assertThat(drainSeqs()).containsExactly(1L, 2L, 9L, 10L);
    }

    @Test
    void compactionAlsoRemovesDeadRetryEntries() {
        List<Lane.Entry> retries = new ArrayList<>();
        for (long seq = 1; seq <= 4; seq++) {
            Lane.Entry e = entry(seq);
            retries.add(e);
            lane.offerRetry(e);
        }
        offerAll(5, 6);

        lane.markDead(retries.get(1));
        lane.markDead(retries.get(2));
        lane.markDead(retries.get(3)); // 3 of 6 dead: not yet over half
        assertThat(lane.physicalSize()).isEqualTo(6);
        lane.markDead(retries.get(0)); // 4 of 6: compacts

        assertThat(lane.physicalSize()).isEqualTo(2);
        assertThat(drainSeqs()).containsExactly(5L, 6L);
    }

    @Test
    void deadHeadsDiscardedByPollNoLongerCountTowardCompaction() {
        List<Lane.Entry> entries = new ArrayList<>();
        for (long seq = 1; seq <= 4; seq++) {
            Lane.Entry e = entry(seq);
            entries.add(e);
            lane.offer(e);
        }
        lane.markDead(entries.get(0));
        lane.markDead(entries.get(1)); // 2 of 4 dead: not over half

        assertThat(lane.poll()).map(Lane.Entry::seq).contains(3L); // discards dead 1 and 2

        assertThat(lane.physicalSize()).isEqualTo(1);
        assertThat(lane.size()).isEqualTo(1);
    }

    private void offerAll(long... seqs) {
        for (long seq : seqs) {
            lane.offer(entry(seq));
        }
    }

    private List<Long> drainSeqs() {
        List<Long> seqs = new ArrayList<>();
        for (var e = lane.poll(); e.isPresent(); e = lane.poll()) {
            seqs.add(e.get().seq());
        }
        return seqs;
    }

    private static Lane.Entry entry(long seq) {
        return new Lane.Entry(seq, new MessageId(0, new UUID(0, seq)));
    }
}

package dpq.core;

/**
 * Receives messages that exhausted their deliveries (D8b, D9a). Called while the source partition's lock is
 * held, so an implementation may take the DLQ partition's lock but no other: lock order is source → DLQ.
 */
interface DeadLetterSink {

    /**
     * @param deadLetteredAtMono monotonic time the lease ended, which is when the message was dead-lettered,
     *                           whenever the drain that noticed it ran
     */
    void deadLetter(Message message, DeadLetterInfo info, long deadLetteredAtMono);
}

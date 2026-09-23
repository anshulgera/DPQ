package dpq.core;

/**
 * Receives messages that exhausted their deliveries (D8b, D9a). Called while the source partition's lock is
 * held, so an implementation may take the DLQ partition's lock but no other: lock order is source → DLQ.
 */
interface DeadLetterSink {

    void deadLetter(Message message, DeadLetterInfo info);
}

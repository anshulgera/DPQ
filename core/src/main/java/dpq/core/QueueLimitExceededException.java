package dpq.core;

/** The node already holds its maximum number of user queues (D11b, D18h). */
public final class QueueLimitExceededException extends DpqException {

    public QueueLimitExceededException(int maxQueues) {
        super("queue limit reached (" + maxQueues + " user queues)");
    }
}

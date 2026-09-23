package dpq.core;

/** The queue holds {@code maxDepth} ready + in-flight messages; the producer should back off (HTTP 429, D11b). */
public final class QueueFullException extends DpqException {

    public QueueFullException(String queue, int maxDepth) {
        super("queue is full (maxDepth " + maxDepth + "): " + queue);
    }
}

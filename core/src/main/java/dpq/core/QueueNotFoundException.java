package dpq.core;

/** Unknown queue (HTTP 404). */
public final class QueueNotFoundException extends DpqException {

    public QueueNotFoundException(String queue) {
        super("queue not found: " + queue);
    }
}

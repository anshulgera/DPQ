package dpq.core.error;

/** The queue exists with a different resolved config (HTTP 409, D11c). */
public final class QueueAlreadyExistsException extends DpqException {

    public QueueAlreadyExistsException(String queue) {
        super("queue already exists with a different config: " + queue);
    }
}

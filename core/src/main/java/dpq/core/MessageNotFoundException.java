package dpq.core;

/** The message was acked, dead-lettered, expired or never existed (HTTP 404, D8d). */
public final class MessageNotFoundException extends DpqException {

    public MessageNotFoundException(MessageId id) {
        super("message not found: " + id);
    }
}

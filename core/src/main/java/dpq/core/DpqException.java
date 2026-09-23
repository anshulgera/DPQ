package dpq.core;

/** Base type for every error the queue engine reports to callers. */
public abstract class DpqException extends RuntimeException {

    protected DpqException(String message) {
        super(message);
    }
}

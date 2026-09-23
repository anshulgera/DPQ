package dpq.core;

import java.util.regex.Pattern;

/**
 * Queue-name rules (D9a, D11b). User names can't contain '.', so a user queue can never collide with the
 * reserved {@code {name}.dlq} form of a dead-letter queue.
 */
public final class QueueNames {

    private static final Pattern USER_NAME = Pattern.compile("[A-Za-z0-9_-]{1,80}");

    private QueueNames() {}

    /** Returns {@code name} if it is a valid user queue name, else throws {@link ValidationException}. */
    public static String requireValidUserName(String name) {
        if (name == null || !USER_NAME.matcher(name).matches()) {
            throw new ValidationException("queue name must match [A-Za-z0-9_-]{1,80}");
        }
        return name;
    }
}

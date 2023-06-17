package net.oneandone.kafka.jobs.api.exceptions;

/**
 * @author aschoerk
 */
public class Recoverable extends RuntimeException {
    public Recoverable(final String message) {
        super(message);
    }

    public Recoverable(final String message, final Throwable cause) {
        super(message, cause);
    }

    public Recoverable(final Throwable cause) {
        super(cause);
    }
}

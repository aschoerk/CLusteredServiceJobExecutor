package net.oneandone.kafka.jobs.api.exceptions;

/**
 * @author aschoerk
 */
public class UnRecoverable extends RuntimeException {
    public UnRecoverable(final String message) {
        super(message);
    }

    public UnRecoverable(final String message, final Throwable cause) {
        super(message, cause);
    }

    public UnRecoverable(final Throwable cause) {
        super(cause);
    }
}

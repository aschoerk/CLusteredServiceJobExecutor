package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public class KjeException extends RuntimeException {

    private static final long serialVersionUID = -754306613693066379L;

    public KjeException(final String message) {
        super(message);
    }

    public KjeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public KjeException(final Throwable cause) {
        super(cause);
    }

    public KjeException(final String message, final Throwable cause, final boolean enableSuppression, final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}

package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface Transaction {
    default void begin() {}

    default void commit() {}

    default void rollback() {}

    default boolean isTransactionRunning() { return false; }

    default void setRollbackOnly() {}

    default boolean evaluateException(Exception e) {
        return false;
    }
}

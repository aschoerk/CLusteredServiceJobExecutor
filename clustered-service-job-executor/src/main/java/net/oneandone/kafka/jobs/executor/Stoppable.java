package net.oneandone.kafka.jobs.executor;

/**
 * @author aschoerk
 */
public interface Stoppable {

    void setRunning();

    boolean isRunning();

    void setShutDown();

    boolean doShutDown();
}

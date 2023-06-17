package net.oneandone.kafka.jobs.beans;

/**
 * @author aschoerk
 */
public interface Stoppable {

    void setRunning();

    boolean isRunning();

    void setShutDown();

    boolean doShutDown();
}

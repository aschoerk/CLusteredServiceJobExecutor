package net.oneandone.kafka.jobs.executor;

/**
 * @author aschoerk
 */
class StoppableBase implements Stoppable {
    boolean running = false;
    boolean shutdown = false;

    @Override
    public void setRunning() {
        this.running = true;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void setShutDown() {
        this.shutdown = true;
    }

    @Override
    public boolean doShutDown() {
        return shutdown;
    }
}

package net.oneandone.kafka.jobs.beans;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
class StoppableBase implements Stoppable {

    Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final Beans beans;
    boolean running = false;
    boolean shutdown = false;

    public StoppableBase(final Beans beans) {
        this.beans = beans;
    }

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

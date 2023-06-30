package net.oneandone.kafka.jobs.beans;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
class StoppableBase implements Stoppable {
    private static AtomicInteger threadIx = new AtomicInteger();
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

    protected void initThreadName(final String name) {
        String threadName = String.format("KCTM_%010d_%05d_%s_%7d",
                Thread.currentThread().getContextClassLoader().hashCode(),
                Thread.currentThread().getId(),
                String.format("%-12s",name).substring(0,12), threadIx.incrementAndGet());
        Thread.currentThread().setName(threadName);
        logger.trace("Initialized Name of Thread with Id: {}", Thread.currentThread().getId());
    }
}

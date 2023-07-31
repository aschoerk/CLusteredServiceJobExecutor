package net.oneandone.kafka.jobs.beans;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
class StoppableBase implements Stoppable {
    private static final AtomicInteger threadIx = new AtomicInteger();

    protected Beans beans;
    Logger logger = LoggerFactory.getLogger(this.getClass());
    boolean running = false;
    boolean shutdown = false;

    public StoppableBase(final Beans beans) {
        this.beans = beans;
    }

    @Override
    public void setRunning() {setRunning(true);}

    @Override
    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    @Override
    public void setShutDown() {
        this.shutdown = true;
    }

    @Override
    public boolean doShutDown() {
        return shutdown;
    }

    protected void waitForThreads(Future ... threads) {
        beans.getContainer().submitInThread(() -> {
            initThreadName("WaitForThreads", true);
            Arrays.stream(threads).forEach(t ->
            {
                if(t != null) {
                    logger.info("Waiting for Thread to end {}", t);
                }
            });
            try {
                Instant startTime = beans.getContainer().getClock().instant();
                while (Arrays.stream(threads).anyMatch(t -> (t != null) && !t.isDone() && !t.isCancelled()) && startTime.plus(Duration.ofMillis(5000)).isAfter(beans.getContainer().getClock().instant())) {
                    Thread.sleep(10);
                }
                Arrays.stream(threads).filter(t -> (t != null) && !t.isDone() && !t.isCancelled()).forEach(
                        t -> t.cancel(true)
                );
                while (Arrays.stream(threads).anyMatch(t -> (t != null) && !t.isDone() && !t.isCancelled()) && startTime.plus(Duration.ofMillis(5000)).isAfter(beans.getContainer().getClock().instant())) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException i) {
                Thread.interrupted();
            }
            setRunning(false);
        });
    }

    protected void waitForStoppables(Stoppable... stoppables) {
        try {
            while (Arrays.stream(stoppables).anyMatch(t -> t.isRunning())) {
                Thread.sleep(10);
            }
        } catch (InterruptedException i) {
            Thread.interrupted();
        }
    }

    protected void stopStoppables(Stoppable... stoppables) {
        Arrays.stream(stoppables).forEach(s -> {
            while (s.isRunning()) {
                s.setShutDown();
            }
        });
    }

    protected void initThreadName(final String name) {
        initThreadName(name, false);
    }

    protected void initThreadName(final String name, boolean ignoreStoppable) {
        if (!ignoreStoppable && doShutDown()) {
            logger.error("Initializing Threadname {} but doShutdown is true", name);
        }
        String threadName = String.format("KCTM_%05d_%05d_%07d_%s",
                beans.getCount(),
                Thread.currentThread().getId(),
                 threadIx.incrementAndGet(), name);
        Thread.currentThread().setName(threadName);
        logger.trace("Initialized Name {} of Thread with Id: {}", name, Thread.currentThread().getId());
    }

    public Beans getBeans() {
        return beans;
    }
}

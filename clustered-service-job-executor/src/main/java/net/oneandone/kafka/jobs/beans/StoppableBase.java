package net.oneandone.kafka.jobs.beans;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author aschoerk
 */
class StoppableBase implements Stoppable {
    private static AtomicInteger threadIx = new AtomicInteger();
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

    protected void waitForThreads(Thread ... threads) {
        beans.getContainer().createThread(new Thread(() -> {
            try {
                Instant startTime = beans.getContainer().getClock().instant();
                while (Arrays.stream(threads).anyMatch(t -> t.isAlive()) && startTime.plus(Duration.ofMillis(5000)).isAfter(beans.getContainer().getClock().instant())) {
                    Thread.sleep(10);
                }
                Arrays.stream(threads).filter(t -> t.isAlive()).forEach(
                        t -> t.interrupt()
                );
                while (Arrays.stream(threads).anyMatch(t -> t.isAlive()) && startTime.plus(Duration.ofMillis(5000)).isAfter(beans.getContainer().getClock().instant())) {
                    Thread.sleep(10);
                }
            } catch (InterruptedException i) {
                Thread.interrupted();
            }
            setRunning(false);
        })).start();
    }

    protected void waitForStoppables(Stoppable ... stoppables) {
        try {
            while (Arrays.stream(stoppables).anyMatch(t -> t.isRunning())) {
                Thread.sleep(10);
            }
        } catch (InterruptedException i) {
            Thread.interrupted();
        }
    }

    protected void stopStoppables(Stoppable ... stoppables) {
        Arrays.stream(stoppables).forEach(s -> {
            while (s.isRunning()) s.setShutDown();
        } );
    }

    protected void initThreadName(final String name) {
        String threadName = String.format("KCTM_%010d_%05d_%s_%7d",
                Thread.currentThread().getContextClassLoader().hashCode(),
                Thread.currentThread().getId(),
                String.format("%-12s", name).substring(0, 12), threadIx.incrementAndGet());
        Thread.currentThread().setName(threadName);
        logger.trace("Initialized Name of Thread with Id: {}", Thread.currentThread().getId());
    }
}

package net.oneandone.kafka.jobs.executor.jobexamples;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.executor.ApiTests;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScoped;

/**
 * @author aschoerk
 */
@CdbThreadScoped
public class CDITestStep implements Step<TestContext> {
    /**
     * Instance variable to check if ThreadScoped is maintained despite encapsulation
     */
    AtomicBoolean used = new AtomicBoolean(false);

    static AtomicInteger threadCount = new AtomicInteger(0);

    Random random = new Random();

    @Override
    public StepResult handle(final TestContext context) {
        int threads = threadCount.incrementAndGet();
        try {
            Thread.sleep(random.nextInt(10));
            ApiTests.logger.info("Handle was called Threads: {} ", threads);
            if(!used.compareAndSet(false, true)) {
                throw new KjeException("Collision in threadscoped Step");
            }
            Thread.sleep(random.nextInt(10));
            context.i++;
            return StepResult.DONE;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            threadCount.decrementAndGet();
            if(!used.compareAndSet(true, false)) {
                throw new KjeException("Collision in threadscoped Step");
            }
        }

    }
}

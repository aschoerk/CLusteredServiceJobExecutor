package net.oneandone.kafka.jobs.executor.jobexamples;

import java.util.concurrent.atomic.AtomicInteger;

import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.executor.ApiTests;

/**
 * @author aschoerk
 */
public class TestStep implements Step<TestContext> {

    static AtomicInteger counter = new AtomicInteger();

    @Override
    public StepResult handle(final TestContext context) {
        ApiTests.logger.info("Handle was called");
        counter.incrementAndGet();
        context.i++;
        return StepResult.DONE;
    }
}

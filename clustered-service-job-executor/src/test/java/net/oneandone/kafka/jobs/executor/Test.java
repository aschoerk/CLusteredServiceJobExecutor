package net.oneandone.kafka.jobs.executor;


import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.api.Executor;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Providers;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

public class Test {

    static Logger logger = LoggerFactory.getLogger("Test");

    static class TestContext {
        int i;
    }

    static class TestJob implements Job<TestContext> {

        @Override
        public String name() {
            return "TestJobName";
        }

        @Override
        public String signature() {
            return name() + "|" + "|" + Arrays.stream(steps()).map(Step::name).collect(Collectors.joining("|"));
        }

        @Override
        public Step<TestContext>[] steps() {
            return new Step[] {
                    new TestStep()
            };
        }
    }

    static class TestStep implements Step<TestContext> {

        static AtomicInteger counter = new AtomicInteger();

        @Override
        public StepResult handle(final TestContext context) {
            logger.info("Handle was called");
            counter.incrementAndGet();
            return StepResult.DONE;
        }
    }

    @org.junit.jupiter.api.Test
    void test() throws InterruptedException {
        Executor executor = Providers.get().createExecutor();
        TestJob jobTemplate = new TestJob();
        Job<TestContext> job = executor.register(jobTemplate, TestContext.class);
        Context<TestContext> context = executor.create(job, new TestContext());
        while (TestStep.counter.get() < 1) {
            Thread.sleep(200);
        }
    }
}

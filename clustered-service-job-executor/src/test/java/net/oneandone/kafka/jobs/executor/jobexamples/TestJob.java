package net.oneandone.kafka.jobs.executor.jobexamples;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.beans.EngineImpl;

/**
 * @author aschoerk
 */
public class TestJob implements Job<TestContext> {

    EngineImpl engine;

    static AtomicInteger ids = new AtomicInteger();

    public TestJob(final Engine engine) {
        this.engine = (EngineImpl) engine;
    }

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
        return new Step[]{
                new TestStep()
        };
    }

    @Override
    public Supplier<String> getIdCreator() {
        return () -> engine.getBeans().getNodeId() + "_" + ids.incrementAndGet();
    }
}

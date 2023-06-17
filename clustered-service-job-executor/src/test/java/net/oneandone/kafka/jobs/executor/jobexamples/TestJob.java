package net.oneandone.kafka.jobs.executor.jobexamples;

import java.util.Arrays;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;

/**
 * @author aschoerk
 */
public class TestJob implements Job<TestContext> {

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
}

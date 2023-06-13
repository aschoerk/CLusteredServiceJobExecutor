package net.oneandone.kafka.jobs.executor;

import java.util.Arrays;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;

/**
 * @author aschoerk
 */
public  class JobImpl<T> implements Job<T> {

    Step<T> steps[];

    Class<T> clazz;

    public JobImpl(Job<T> job, Class<T> clazz) {
        this.clazz = clazz;
        steps = new Step[job.steps().length];
        for (int i = 0; i < job.steps().length; i++) {
            steps[i] = new StepImpl<>(this, job.steps()[i]);
        }

    }

    @Override
    public String signature() {
        return name() + "|" + "|" + Arrays.stream(steps()).map(Step::name).collect(Collectors.joining("|"));
    }

    @Override
    public Step<T>[] steps() {
        return steps;
    }

}

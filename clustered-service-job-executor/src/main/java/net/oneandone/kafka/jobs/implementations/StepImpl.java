package net.oneandone.kafka.jobs.implementations;

import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

/**
 * @author aschoerk
 */
public class StepImpl<T> implements Step<T> {

    Step<T> step;

    JobImpl<T> job;

    ThreadLocal<T> context = new ThreadLocal<>();

    public StepImpl(JobImpl job, Step<T> step) {
        this.job = job;
        this.step = step;
    }

    @Override
    public String name() {
        return Step.super.name();
    }

    @Override
    public StepResult handle(final T context) {
        this.context.set(context);
        return this.step.handle(context);
    }

    @Override
    public T getContext() {
        return context.get();
    }

    @Override
    public Job getJob() {
        return Step.super.getJob();
    }
}

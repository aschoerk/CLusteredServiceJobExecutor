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

    ThreadLocal<Transport> context = new ThreadLocal<>();

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
        return this.step.handle(context);
    }

    @Override
    public T getContext() {
        return Step.super.getContext();
    }

    @Override
    public Job getJob() {
        return Step.super.getJob();
    }
}

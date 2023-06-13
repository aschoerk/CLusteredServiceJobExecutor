package net.oneandone.kafka.jobs.executor;

import java.util.concurrent.atomic.AtomicReference;

import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

/**
 * @author aschoerk
 */
public class StepImpl<T> implements Step<T> {

    Step<T> step;

    JobImpl<T> job;

    ThreadLocal<Context<T>> context = new ThreadLocal<>();

    public StepImpl(JobImpl<T> job, Step<T> step) {
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
    public Context<T> getContext() {
        return Step.super.getContext();
    }

    @Override
    public Job<T> getJob() {
        return Step.super.getJob();
    }
}

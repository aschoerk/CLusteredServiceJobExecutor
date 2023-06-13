package net.oneandone.kafka.jobs.executor;

import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.api.JobData;

/**
 * @author aschoerk
 */
public class ContextImpl<T> implements Context<T> {

    JobDataImpl jobData;

    T context;

    public ContextImpl(final JobDataImpl jobData, final T context) {
        this.jobData = jobData;
        this.context = context;
    }

    @Override
    public JobData jobData() {
        return jobData;
    }

    public JobDataImpl jobDataImpl() {
        return jobData;
    }

    @Override
    public T context() {
        return context;
    }
}

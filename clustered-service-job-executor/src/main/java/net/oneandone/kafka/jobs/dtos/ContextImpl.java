package net.oneandone.kafka.jobs.dtos;

import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;

/**
 * @author aschoerk
 */
public class ContextImpl<T> implements Context<T> {

    private JobDataImpl jobData;

    private T context;

    public ContextImpl(final JobDataImpl jobData, final T context) {
        this.jobData = jobData;
        this.context = context;
    }

    @Override
    public JobDataImpl jobData() {
        return jobData;
    }


    @Override
    public T context() {
        return context;
    }
}

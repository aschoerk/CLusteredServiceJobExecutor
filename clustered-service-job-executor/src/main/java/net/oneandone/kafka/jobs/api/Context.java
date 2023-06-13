package net.oneandone.kafka.jobs.api;

/**
 * the context which glues the steps together.
 */
public interface Context<T> {

    /**
     * describes current state of job
     * @return current state of job
     */
    JobData jobData();

    /**
     * a job specific context. All steps of a job need to support the same context.
     * @return a job specific context. All steps of a job need to support the same context.
     */
    T context();
}

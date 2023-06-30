package net.oneandone.kafka.jobs.api;

/**
 * the context which glues the steps together.
 */
public interface Transport {

    /**
     * describes current state of job
     * @return current state of job
     */
    JobData jobData();

    /**
     * a job specific context. All steps of a job need to support the same context.
     * needs to stay of the same class during complete execution of a job-instance
     * needs to be equal, an instance of or a subclass of the T-Parameter of the job and the job-steps
     * will be marshalled using GSon if container marshall or unmarshall returns null
     * @return a job specific context. All steps of a job need to support the same context.
     */
    String context();

    String resumeData();
}

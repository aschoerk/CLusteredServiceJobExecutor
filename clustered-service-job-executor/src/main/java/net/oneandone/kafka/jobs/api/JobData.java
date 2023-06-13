package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface JobData {
    /**
     * the unique id of the job instance
     * @return the unique id of the job instance
     */
    String id();

    /**
     * the signature of the job which originally started to handle the context
     * @return the signature of the job which originally started to handle the context
     */
    String jobSignature();

    /**
     * the current state the job is in.
     * @return the current state the job is in.
     */
    State state();

    /**
     * the errors occurred during execution of the job
     * @return the errors occurred during execution of the job
     */
    Remark[] errors();

    /**
     * comments added to a job instance during the execution
     * @return comments added to a job instance during the execution
     */
    Remark[] comments();

    /**
     * index of the current step
     * @return index of the current step
     */
    int step();
}

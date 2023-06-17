package net.oneandone.kafka.jobs.api;

import java.time.Instant;

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

    /**
     * The current class of the context
     * @return The current class of the context
     */
    Class contextClass();

    /**
     * Number of sequential attempts to execute the current step
     *
     * @return vNumber of sequential attempts to execute the current step
     */
    Integer retries();

    /**
     * The next time the step should get started, if != null, don't start before that time.
     * @return The next time the step should get started, if != null, don't start before that time.
     */
    Instant date();
}

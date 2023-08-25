package net.oneandone.kafka.jobs.api;

import java.time.Instant;

/**
 * Interface describing the maintenance data for a instance of a job to be run.
 */
public interface JobData {
    /**
     * the unique id of the job instance
     * @return the unique id of the job instance
     */
    String getId();

    /**
     * the signature of the job which originally started to handle the context
     * @return the signature of the job which originally started to handle the context
     */
    String getSignature();

    /**
     * the current state the job is in.
     * @return the current state the job is in.
     */
    State getState();

    /**
     * signifying the time this Job-Instance was created. Used to order the starting of jobs in groups.
     * @return the time this Job-Instance was created. Used to order the starting of jobs in groups.
     */
    Instant getCreatedAt();

    /**
     * the errors occurred during execution of the job
     * @return the errors occurred during execution of the job
     */
    Remark[] getErrors();

    /**
     * comments added to a job instance during the execution
     * @return comments added to a job instance during the execution
     */
    Remark[] getComments();

    /**
     * index of the current step
     * @return index of the current step
     */
    int getStep();

    /**
     * number of steps executed
     * @return number of steps executed
     */
    int getStepCount();

    /**
     * The name of the current class of the context, this is set during Engine#create
     * @return The current class of the context
     */
    String getContextClass();

    /**
     * The name of the current class of the data sent additionally to allow resume of suspended jobs. This is sent via resume and
     * must match the parameter of the resumed Step.
     * @return The class of the date accompanied by the resume call. (if there is any).
     */
    String getResumeDataClass();

    /**
     * Number of sequential attempts to execute the current step
     *
     * @return vNumber of sequential attempts to execute the current step
     */
    int getRetries();

    /**
     * The next time the step should get started, if != null, don't start before that time.
     * @return The next time the step should get started, if != null, don't start before that time.
     */
    Instant getDate();

    /**
     * used to identify duplicate idempotent requests. Per job-name only one JobEntity may be executed per correlationId.
     * @return the correlationId  used to identify duplicate, idempotent requests.
     */
    String getCorrelationId();

    /**
     * optional id klassifying job-instances so that only the oldest Job which is not DONE or in state ERROR will be started.
     * @return an optional id klassifying job-instances so that only the oldest Job which is not DONE or in state ERROR will be started.
     */
    String getGroupId();
}

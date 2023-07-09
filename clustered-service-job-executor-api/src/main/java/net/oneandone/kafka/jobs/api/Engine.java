package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface Engine {

    /**
     * registers a job and its steps so that they can be executed in principle
     *
     * @param job the job containing steps which can be executed.
     * @return job and steps adapted to the executor jar.
     */
    <T> void register(Job<T> job, Class<T> clazz);

    /**
     * register "remote" accessible RemoteJobs
     * @param jobs information about remote accessible jobs
     */
    void register(RemoteExecutor remoteExecutor);

    /**
     * creates a job instance to be executed
     *
     * @param job     the job containing the sequence of steps to be executed.
     * @param context the context shared between the steps executed
     * @return The context after JobData has been initialized.
     */
    <T> Transport create(Job<T> job, T context);

    <T> Transport create(Job<T> job, T context, String correlationId);

    <T> Transport create(Job<T> job, String groupId, T context);

    /**
     *  create job only if correlationId is not used by any job in the recent past (configuration)
     * @param job     the job containing the sequence of steps to be executed.
     * @param context the context shared between the steps executed
     * @param correlationId identifies uniquely to be executed jobs. If the jobname or signature is relevant, it needs to be included beforehand.
     * @param groupId signifies jobs which are to be executed exclusively. The most early defined before later.
     * @return The context after JobData has been initialized. Or the context of the previous job with same correlationId if available
     * @param <T>
     */
    <T> Transport create(Job<T> job, String groupId, T context, String correlationId);

    /**
     * resume a suspended Job. Multiple parallel calls of resume at the same time may occur.
     *
     * @param jobID      the id of the jobData
     * @param resumeData additional Data to be sent to the step
     * @param correlationId optional id to recognize multiple idempotently repeated callbacks. This is checked in pool with all other job-correlationIds.
     * @param <R>        additional Data to be made available to the step.
     */
    <R> void resume(String jobID, R resumeData, String correlationId);

    /**
     * resume a suspended Job. Multiple parallel calls of resume at the same time may occur.
     *
     * @param jobID      the id of the jobData
     * @param resumeData additional Data to be sent to the step
     * @param <R>        additional Data to be made available to the step.
     */
    <R> void resume(String jobID, R resumeData);

    void stop();

}

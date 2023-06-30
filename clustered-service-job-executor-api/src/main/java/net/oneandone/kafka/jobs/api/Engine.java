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

    /**
     * resume a suspended Job. Multiple parallel calls of resume at the same time may occur.
     *
     * @param jobID      the id of the jobData
     * @param resumeData additional Data to be sent to the step
     * @param correlationId optional id to recognize multiple idempotently repeaated callbacks
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

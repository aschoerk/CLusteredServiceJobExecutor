package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface Executor {

    /**
     * registers a job and its steps so that they can be executed in principle
     *
     * @param job the job containing steps which can be executed.
     * @return job and steps adapted to the executor jar.
     */
    <T> Job<T> register(Job<T> job, Class<T> clazz);

    /**
     * creates a job instance to be executed
     * @param job the job containing the sequence of steps to be executed.
     * @param context the context shared between the steps executed
     * @return The context after JobData has been initialized.
     */
    <T> Context<T> create(Job<T> job, T context);




}

package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface Step<T> {

    /**
     * A name of the step used to identify comparable steps
     * @return name of the step used to identify comparable steps
     */
    default String name() { return this.getClass().getName(); }

    /**
     * the function to execute as part of the job
     * @param context the data describing completely the current job state. Can be changed during handle.
     *                further steps will see the results of those changes.
     * @return the result which controls how the job is executed further
     */
    StepResult handle(T context);

    default Context<T> getContext() { throw new KjeException("Using unregistered job/step"); }

    default Job<T> getJob() { throw new KjeException("Using unregistered job/step"); }


}

package net.oneandone.kafka.jobs.api;

/**
 * @param <Context> see Job
 */
public interface Step<Context> {

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
    StepResult handle(Context context);

    default <R> StepResult handle(Context context, R remoteData) {
        return handle(context);
    }

    default Context getContext() { throw new KjeException("Using unregistered job/step"); }

    default Job<Context> getJob() { throw new KjeException("Using unregistered job/step"); }


}

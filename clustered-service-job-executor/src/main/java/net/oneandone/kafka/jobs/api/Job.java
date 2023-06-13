package net.oneandone.kafka.jobs.api;

public interface Job<T> {

    /**
     * name, used to show generally, what the job should do
     * @return name, used to show generally, what the job should do
     */
    default String name() { return this.getClass().getSimpleName(); }

    /**
     * a string signifying matching jobs, if name might be the same, but the steps where changed.
     * @return a string signifying matching jobs, if name might be the same, but the steps where changed.
     */
    String signature();

    /**
     * steps to be executed by job
     * @return steps to be executed by job
     */
    Step<T>[] steps();


}
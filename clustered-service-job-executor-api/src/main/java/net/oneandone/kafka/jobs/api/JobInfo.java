package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface JobInfo<Context> {
    String getName();

    default String getVersion() { return "1"; }

    /**
     * a string signifying matching jobs, if name might be the same, but the steps where changed.
     * @return a string signifying matching jobs, if name might be the same, but the steps where changed.
     */
     String getSignature();

     int getStepCount();

    /**
     * return the context class the steps are using
     *
     * @return the context class the steps are using
     */
    String getContextClass();
}

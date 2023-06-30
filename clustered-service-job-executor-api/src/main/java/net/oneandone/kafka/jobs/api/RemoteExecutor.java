package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface RemoteExecutor {
    JobInfo[] supportedJobs();

    StepResult handle(Transport transport);
}

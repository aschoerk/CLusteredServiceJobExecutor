package net.oneandone.kafka.jobs.api;

import net.oneandone.kafka.jobs.api.dto.TransportDto;

/**
 * @author aschoerk
 */
public interface RemoteExecutor {
    JobInfo[] supportedJobs();

    StepResult handle(TransportDto transport);
}

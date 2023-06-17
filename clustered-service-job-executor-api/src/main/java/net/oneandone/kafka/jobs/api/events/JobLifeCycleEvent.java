package net.oneandone.kafka.jobs.api.events;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.State;

/**
 * event sent in case of Statechanges
 */
public interface JobLifeCycleEvent extends Event {

    /**
     * The new state of jobData as it will be sent to the Topic
     * @return the new state of jobData as it will be sent to the Topic
     */
    JobData jobData();

    /**
     * the previous state of the job
     * @return the previous state of the job
     */
    State previousState();
}

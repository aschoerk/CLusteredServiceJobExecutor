package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.time.Instant;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.api.events.JobLifeCycleEvent;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;

/**
 * @author aschoerk
 */
public class JobTools extends StoppableBase {

    JobTools(Beans beans) {
        super(beans);
    }

    public void prepareJobDataForRunning(JobDataImpl jobData) {
        jobData.setDate(Instant.now(beans.container.getClock()));
        if (jobData.state() == ERROR) {
            jobData.setRetries(0);
        }
        changeStateTo(jobData, RUNNING);
    }

    void changeStateTo(final JobDataImpl jobData, final State newState) {
        State previousState = jobData.state();
        jobData.setState(newState);
        beans.container.fire(new JobLifeCycleEvent() {
            @Override
            public JobData jobData() {
                return jobData;
            }

            @Override
            public State previousState() {
                return previousState;
            }
        });
    }
}

package net.oneandone.kafka.jobs.tools;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;

/**
 * @author aschoerk
 */
public class ResumeStep implements Step<ResumeJobData> {
    private final Beans beans;

    public ResumeStep(final Beans beans) {
        this.beans = beans;
    }

    @Override
    public StepResult handle(final ResumeJobData context) {
        String jobId = context.resumedJobId;
        JobDataState state = beans.getJobDataStates().get(jobId);
        if (state == null) {
            return StepResult.DELAY;
        } else {

            switch (state.getState()) {
                case DONE:
                    return StepResult.DONE;
                case SUSPENDED:
                    TransportImpl suspendedJob = beans.getReceiver().readJob(state);
                    suspendedJob.jobData().setResumeDataClass(context.resumeDataClass);
                    beans.getJobTools().prepareJobDataForRunning(suspendedJob.jobData());
                    beans.getSender().send(suspendedJob);
                    break;
            }
        }


        if (state.getState() != State.SUSPENDED) {
            return StepResult.DELAY;
        }


        return StepResult.DONE;
    }
}

package net.oneandone.kafka.jobs.beans;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.dto.TransportDto;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.implementations.JobImpl;
import net.oneandone.kafka.jobs.implementations.StepImpl;

/**
 * @author aschoerk
 */
public class LocalRemoteExecutor implements RemoteExecutor {

    Beans beans;

    public LocalRemoteExecutor(final Beans beans) {
        this.beans = beans;
    }

    @Override
    public JobInfo[] supportedJobs() {
        return beans.getInternalJobs().values().toArray(new JobInfo[0]);
    }

    @Override
    public StepResult handle(TransportDto transport) {
        try {
            final JobData jobData = transport.jobData();
            String signature = jobData.getSignature();
            JobImpl job = beans.getInternalJobs().get(signature);
            TransportImpl transportImpl = null;
            if(transport instanceof TransportImpl) {
                transportImpl = (TransportImpl) transport;
            }
            else {
                transportImpl = new TransportImpl(beans, transport);
            }
            if((jobData.getStep() >= 0) && (jobData.getStep() < job.steps().length)) {
                if(transport.resumeData() != null) {
                    return ((StepImpl) job.steps()[jobData.getStep()]).callHandle(beans,
                            transportImpl.jobData(), transportImpl.getContext(Class.forName(jobData.getContextClass())),
                            transportImpl.getResumeData(Class.forName(jobData.getResumeDataClass())));
                }
                else {
                    return ((StepImpl) job.steps()[jobData.getStep()]).callHandle(beans,
                            transportImpl.jobData(),transportImpl.getContext(Class.forName(jobData.getContextClass())));
                }
            }
            else {
                return StepResult.errorResult("Invalid JobData " + jobData);
            }
        } catch (ClassNotFoundException cne) {
            return StepResult.errorResult("Class not found " + cne);
        }
    }
}

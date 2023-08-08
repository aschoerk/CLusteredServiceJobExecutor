package net.oneandone.kafka.jobs.beans;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.Transport;
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
        return beans.getJobs().values().toArray(new JobInfo[0]);
    }

    @Override
    public StepResult handle(Transport transport) {
        try {
            final JobData jobData = transport.jobData();
            String signature = jobData.jobSignature();
            JobImpl job = beans.getJobs().get(signature);
            TransportImpl transportImpl = null;
            if(transport instanceof TransportImpl) {
                transportImpl = (TransportImpl) transport;
            }
            else {
                transportImpl = new TransportImpl(beans, transport);
            }
            if((jobData.step() >= 0) && (jobData.step() < job.steps().length)) {
                if(transport.resumeData() != null) {
                    return ((StepImpl) job.steps()[jobData.step()]).callHandle(beans,
                            transportImpl.jobData(), transportImpl.getContext(Class.forName(jobData.contextClass())),
                            transportImpl.getResumeData(Class.forName(jobData.resumeDataClass())));
                }
                else {
                    return ((StepImpl) job.steps()[jobData.step()]).callHandle(beans,
                            transportImpl.jobData(),transportImpl.getContext(Class.forName(jobData.contextClass())));
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

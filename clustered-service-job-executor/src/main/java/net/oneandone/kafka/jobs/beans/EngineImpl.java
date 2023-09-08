package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.GROUP;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.implementations.JobImpl;
import net.oneandone.kafka.jobs.api.tools.JsonMarshaller;
import net.oneandone.kafka.jobs.tools.ResumeJob;
import net.oneandone.kafka.jobs.tools.ResumeJobData;

/**
 * @author aschoerk
 */
public class EngineImpl extends StoppableBase implements Engine {

    static AtomicInteger engineCount = new AtomicInteger();

    String name;

    Instant startupTime;

    public EngineImpl(Beans beans) {
        super(beans);
        this.name = beans.getContainer().getConfiguration().getNodeName() + engineCount.incrementAndGet();
        this.startupTime = beans.getContainer().getClock().instant();
    }


    @Override
    public <T> void register(final Job<T> job) {
        JobImpl<T> result = new JobImpl<>(job, beans);
        beans.getInternalJobs().put(result.getSignature(), result);
    }

    @Override
    public void register(final RemoteExecutor remoteExecutor) {
        beans.getRemoteExecutors().addExecutor(remoteExecutor);
    }

    @Override
    public <T> Transport create(final String jobName, final String context, String groupId, String correlationId) {
        JobInfo jobInfo = beans.getRemoteExecutors().findRemoteJob(jobName);
        if (jobInfo == null) {
            throw new KjeException("Job " + jobName + " not found");
        }
        return createJob(jobInfo, context, groupId, correlationId);
    }

    @Override
    public <T> Transport create(final String jobName, final String context) {
        return create(jobName, context, null, null);
    }

    @Override
    public <T> Transport create(final Job<T> job, final T context) {
        return create(job, context, null);
    }

    @Override
    public <T> Transport create(final Job<T> job, final String groupId, final T context) {
        return create(job, groupId, context, null);
    }

    @Override
    public <T> Transport create(final Job<T> job, final T context, String correlationId) {
        return create(job, null, context, correlationId);
    }

    private Transport createJob(final JobInfo jobInfo, final String context, final String groupId, final String correlationId) {
        if(correlationId != null) {
            JobDataState state = beans.getJobDataCorrelationIds().get(correlationId);
            if(state != null) {
                TransportImpl existing = beans.getReceiver().readJob(state);
                return existing;
            }
        }

        JobDataImpl jobData = new JobDataImpl(jobInfo, correlationId, groupId, beans);

        if (jobData.getGroupId() != null) {
            jobData.setState(GROUP);
        } else {
            beans.getJobTools().prepareJobDataForRunning(jobData);
        }

        TransportImpl contextImpl = new TransportImpl(jobData, context, beans);

        beans.getSender().send(contextImpl);

        return contextImpl;
    }


    @Override
    public <T> Transport create(final Job<T> job, final String groupId, final T context, String correlationId) {

        JobImpl<T> jobImpl = (JobImpl<T>) beans.getInternalJobs().get(job.getSignature());

        if(jobImpl == null) {
            throw new KjeException("expected job first to be registered with executor");
        }

        String contextString = beans.getContainer().marshal(context);
        if (contextString == null) {
            contextString = JsonMarshaller.gson.toJson(context);
        }

        return createJob(job, contextString, groupId, correlationId);

    }

    @Override
    public <R> void resume(final String jobID, final R resumeData, String correlationID) {
        String data = beans.getContainer().marshal(resumeData);
        if(data == null) {
            data = JsonMarshaller.gson.toJson(resumeData);
        }
        if(correlationID != null) {
            create(new ResumeJob(beans), new ResumeJobData(jobID, correlationID, data, resumeData.getClass()), jobID + "__" + correlationID);
        }
        else {
            create(new ResumeJob(beans), new ResumeJobData(jobID, correlationID, data, resumeData.getClass()));
        }
    }

    @Override
    public <R> void resume(final String jobID, final R resumeData) {
        resume(jobID, resumeData, null);
    }


    @Override
    public void stop() {
        beans.setShutDown();
        waitForStoppables(beans);
    }

    public String getName() {
        return name;
    }

    public String createId() {
        return UUID.randomUUID().toString();
    }

    public Instant getStartupTime() {
        return startupTime;
    }
}

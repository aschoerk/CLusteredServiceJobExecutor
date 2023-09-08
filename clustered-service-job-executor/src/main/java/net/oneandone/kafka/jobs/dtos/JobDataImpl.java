package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.api.dto.JobDataDto;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class JobDataImpl extends JobDataDto {

    private transient int partition = -1;

    private transient long offset = -1;

    public <T> JobDataImpl(JobImpl<T> job, final Class<T> contextClass, String correlationId, String groupId, Beans beans) {
        this(createJobDataId(job, beans),
                correlationId, State.RUNNING, job.getSignature(), 0, 0, beans);

        this.setContextClass(contextClass.getCanonicalName());
        this.setCreatedAt(beans.getContainer().getClock().instant());
        this.setGroupId(groupId);
    }

    public JobDataImpl(JobInfo job, String correlationId, String groupId, Beans beans) {
        this(beans.getEngine().createId(),
                correlationId, State.RUNNING, job.getSignature(), 0, 0, beans);

        this.setContextClass(job.getContextClass());
        this.setCreatedAt(beans.getContainer().getClock().instant());
        this.setGroupId(groupId);
    }

    public JobDataImpl(final String id, final String correlationId, final State state, final String signature,
                       final int step, final int stepCount, Beans beans) {
        this.setId(id);
        this.setCorrelationId(correlationId);
        this.setState(state);
        this.setJobSignature(signature);
        this.setStep(step);
        this.setStepCount(stepCount);
        this.setCreatedAt(beans.getContainer().getClock().instant());
    }

    private static <T> String createJobDataId(final JobImpl<T> job, final Beans beans) {
        return beans.getEngine().createId();
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(final int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(final long offset) {
        this.offset = offset;
    }

    @Override
    public int getRetries() {
        if(retries == null) {
            setRetries(0);
        }
        return retries;
    }


    public void addError(Instant instant, String errorId, String error) {
        if(getErrors() == null) {
            setErrors(new RemarkImpl[1]);
        }
        else {
            setErrors(Arrays.copyOf(getErrors(), getErrors().length + 1));
        }
        getErrors()[getErrors().length - 1] = new RemarkImpl(instant, errorId, error);
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        JobDataImpl jobData = (JobDataImpl) o;
        return (getStep() == jobData.getStep()) && getId().equals(jobData.getId()) && getJobSignature().equals(jobData.getJobSignature()) && (getState() == jobData.getState()) && Arrays.equals(getErrors(), jobData.getErrors()) && Arrays.equals(getComments(), jobData.getComments());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }

    public void incStepCount() {
        setStepCount(getStepCount() + 1);
    }

    @Override
    public String toString() {
        return "JobDataImpl{" +
               "id='" + getId() + '\'' +
               ", state=" + getState() +
               ", step=" + getStep() +
               ", retries=" + getRetries() +
               ", date=" + getDate() +
               ", groupId='" + getGroupId() + '\'' +
               ", jobSignature='" + getJobSignature() + '\'' +
               ", createdAt=" + getCreatedAt() +
               ", stepCount=" + getStepCount() +
               ", contextClass='" + getContextClass() + '\'' +
               ", resumeDataClass='" + getResumeDataClass() + '\'' +
               ", correlationId='" + getCorrelationId() + '\'' +
               ", errors=" + Arrays.toString(getErrors()) +
               ", comments=" + Arrays.toString(getComments()) +
               '}';
    }


}

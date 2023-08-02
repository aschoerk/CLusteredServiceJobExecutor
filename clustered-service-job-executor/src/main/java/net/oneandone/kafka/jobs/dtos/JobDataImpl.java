package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Remark;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class JobDataImpl implements JobData {
    private final String id;

    private final String jobSignature;

    private Instant createdAt;

    private State state;

    private int step;

    private int stepCount;

    private Integer retries = null;

    private Instant date = null;

    private String contextClass;

    private String resumeDataClass;

    private String correlationId;

    private RemarkImpl[] errors = null;

    private RemarkImpl[] comments = null;
    private String groupId = null;

    public <T> JobDataImpl(JobImpl<T> job, final Class<T> contextClass, String correlationId, String groupId, Beans beans) {
        this(createJobDataId(job, beans),
                correlationId, State.RUNNING, job.signature(), 0, 0);

        this.contextClass = contextClass.getCanonicalName();
        this.createdAt = beans.getContainer().getClock().instant();
        this.groupId = groupId;
    }

    public JobDataImpl(final String id, final String correlationId, final State state, final String signature,
                       final int step, final int stepCount) {
        this.id = id;
        this.correlationId = correlationId;
        this.state = state;
        this.jobSignature = signature;
        this.step = step;
        this.stepCount = stepCount;
    }

    private static <T> String createJobDataId(final JobImpl<T> job, final Beans beans) {
        return (job.getIdCreator() != null) ? job.getIdCreator().get() : beans.getEngine().createId();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String jobSignature() {
        return jobSignature;
    }

    @Override
    public Instant createdAt() {
        return createdAt;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public Remark[] errors() {
        return errors;
    }

    @Override
    public Remark[] comments() {
        return comments;
    }

    @Override
    public int step() {
        return step;
    }

    @Override
    public int stepCount() {return stepCount;}

    @Override
    public String contextClass() {
        return contextClass;
    }


    @Override
    public String resumeDataClass() {
        return resumeDataClass;
    }

    @Override
    public String groupId() {
        return groupId;
    }

    public void setErrors(final RemarkImpl[] remarks) {
        this.errors = remarks;
    }

    public void setComments(final RemarkImpl[] remarks) {
        this.comments = remarks;
    }

    public void setContextClass(final String contextClass) {
        this.contextClass = contextClass;
    }


    @Override
    public int retries() {
        if(retries == null) {
            retries = 0;
        }
        return retries;
    }

    public void setState(final State state) {
        this.state = state;
    }

    public void setStep(final int step) {
        this.step = step;
    }

    public void setRetries(final int retries) {
        this.retries = retries;
    }

    public void setDate(final Instant date) {
        this.date = date;
    }

    @Override
    public Instant date() {
        return date;
    }

    public void addError(Instant instant, String errorId, String error) {
        if(errors == null) {
            errors = new RemarkImpl[1];
        }
        else {
            errors = Arrays.copyOf(errors, errors.length + 1);
        }
        errors[errors.length - 1] = new RemarkImpl(instant, errorId, error);
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
        return (step == jobData.step) && id.equals(jobData.id) && jobSignature.equals(jobData.jobSignature) && (state == jobData.state) && Arrays.equals(errors, jobData.errors) && Arrays.equals(comments, jobData.comments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public void incStepCount() {
        stepCount++;
    }

    @Override
    public String toString() {
        return "JobDataImpl{" +
               "id='" + id + '\'' +
               ", state=" + state +
               ", step=" + step +
               ", retries=" + retries +
               ", date=" + date +
               ", groupId='" + groupId + '\'' +
               ", jobSignature='" + jobSignature + '\'' +
               ", createdAt=" + createdAt +
               ", stepCount=" + stepCount +
               ", contextClass='" + contextClass + '\'' +
               ", resumeDataClass='" + resumeDataClass + '\'' +
               ", correlationId='" + correlationId + '\'' +
               ", errors=" + Arrays.toString(errors) +
               ", comments=" + Arrays.toString(comments) +
               '}';
    }

    public String getResumeDataClass() {
        return resumeDataClass;
    }

    public void setResumeDataClass(final String resumeDataClassP) {
        this.resumeDataClass = resumeDataClassP;
    }

    @Override
    public String correlationId() {
        return correlationId;
    }

    public void setCorrelationId(final String correlationId) {
        this.correlationId = correlationId;
    }
}

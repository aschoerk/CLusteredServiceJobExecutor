package net.oneandone.kafka.jobs.api.dto;

import java.time.Instant;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Remark;
import net.oneandone.kafka.jobs.api.State;

/**
 * @author aschoerk
 */
public class JobDataDto implements JobData {
    private String id = null;
    private String jobSignature = null;
    private State state = null;
    private Instant createdAt = null;
    private Remark[] errors = new Remark[0];
    private Remark[] comments = new Remark[0];
    private int step = -1;
    private int stepCount = 0;
    private String contextClass = null;
    private String resumeDataClass = null;
    protected Integer retries = null;
    private Instant date = null;
    private String correlationId = null;
    private String groupId = null;

    public void setId(final String id) {
        this.id = id;
    }

    public String getJobSignature() {
        return jobSignature;
    }

    public void setJobSignature(final String jobSignature) {
        this.jobSignature = jobSignature;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getSignature() {
        return jobSignature;
    }

    public State getState() {
        return state;
    }

    public void setState(final State state) {
        this.state = state;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(final Instant createdAt) {
        this.createdAt = createdAt;
    }

    public Remark[] getErrors() {
        return errors;
    }

    public void setErrors(Remark[] errors) {
        this.errors = errors;
    }

    public Remark[] getComments() {
        return comments;
    }

    public void setComments(Remark[] comments) {
        this.comments = comments;
    }

    public int getStep() {
        return step;
    }

    public void setStep(final int step) {
        this.step = step;
    }

    public int getStepCount() {
        return stepCount;
    }

    public void setStepCount(final int stepCount) {
        this.stepCount = stepCount;
    }

    public String getContextClass() {
        return contextClass;
    }

    public void setContextClass(final String contextClass) {
        this.contextClass = contextClass;
    }

    public String getResumeDataClass() {
        return resumeDataClass;
    }

    public void setResumeDataClass(final String resumeDataClass) {
        this.resumeDataClass = resumeDataClass;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(final int retries) {
        this.retries = retries;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public Instant getDate() {
        return date;
    }

    public void setDate(final Instant date) {
        this.date = date;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(final String correlationId) {
        this.correlationId = correlationId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }
}

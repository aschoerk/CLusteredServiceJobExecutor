package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Remark;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class JobDataImpl implements JobData {
    private String id;

    private String jobSignature;

    private State state;

    private int step;

    private Integer retries = null;

    private Instant date = null;

    private Class contextClass;

    private RemarkImpl[] errors = null;

    private RemarkImpl[] comments = null;

    public <T> JobDataImpl(JobImpl<T> job) {
        this.jobSignature = job.signature();
        this.state = State.RUNNING;
        this.step = 0;
        this.id = UUID.randomUUID().toString();
    }

    public JobDataImpl(final String id, final State state, final String signature, final int step) {
        this.id = id;
        this.state = state;
        this.jobSignature = signature;
        this.step = step;
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
    public Class contextClass() {
        return contextClass;
    }

    public void setErrors(final RemarkImpl[] remarks) {
        this.errors = remarks;
    }
    public void setComments(final RemarkImpl[] remarks) {
        this.comments = remarks;
    }

    public void setContextClass(final Class contextClass) {
        this.contextClass = contextClass;
    }

    @Override
    public Integer retries() {
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
        if (errors == null) {
            errors = new RemarkImpl[1];
        } else {
            errors = Arrays.copyOf(errors, errors.length + 1);
        }
        errors[errors.length - 1] = new RemarkImpl(instant, errorId, error );
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
}

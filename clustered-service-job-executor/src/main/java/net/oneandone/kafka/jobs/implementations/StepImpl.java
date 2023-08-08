package net.oneandone.kafka.jobs.implementations;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;

/**
 * @author aschoerk
 */
public class StepImpl<T> implements Step<T> {

    Step<T> step;

    JobImpl<T> job;

    ThreadLocal<T> context = new ThreadLocal<>();

    static ThreadLocal<JobDataImpl> jobData = new ThreadLocal<>();

    static ThreadLocal<Beans> beans = new ThreadLocal<>();

    public StepImpl(JobImpl job, Step<T> step) {
        this.job = job;
        this.step = step;
    }

    @Override
    public String name() {
        return Step.super.name();
    }

    public StepResult callHandle(Beans beansP, JobDataImpl jobDataP, final T contextP) {
        this.context.set(contextP);
        JobDataImpl currentJobData = getJobData();
        Beans currentBeans = beansP;
        setBeans(beansP);
        setJobData(jobDataP);
        try {
            return this.step.handle(contextP);
        } finally {
            setJobData(currentJobData);
            setBeans(currentBeans);
        }
    }

    public <R> StepResult callHandle(Beans beansP, JobDataImpl jobDataP, final T contextP, final R releaseData) {
        this.context.set(contextP);
        JobDataImpl currentJobData = getJobData();
        Beans currentBeans = getBeans();
        setJobData(jobDataP);
        setBeans(beansP);
        try {
            return this.step.handle(contextP, releaseData);
        } finally {
            setJobData(currentJobData);
            setBeans(currentBeans);
        }
    }

    @Override
    public StepResult handle(final T t) {
        throw new KjeException("StepImpl handle(T) should not be called");
    }

    @Override
    public <R> StepResult handle(final T t, final R remoteData) {
        throw new KjeException("StepImpl handle(T,R) should not be called");
    }

    @Override
    public T getContext() {
        return context.get();
    }

    @Override
    public Job getJob() {
        return Step.super.getJob();
    }

    public static void setJobData(JobDataImpl jobData) {
        StepImpl.jobData.set(jobData);
    }

    public static JobDataImpl getJobData() {
        return jobData.get();
    }

    public static void setBeans(Beans beans) {
        StepImpl.beans.set(beans);
    }

    public static Beans getBeans() {
        return beans.get();
    }
}

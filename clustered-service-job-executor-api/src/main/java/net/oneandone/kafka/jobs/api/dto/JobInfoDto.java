package net.oneandone.kafka.jobs.api.dto;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.JobInfo;

/**
 * @author aschoerk
 */
public class JobInfoDto implements JobInfo {
    private String name;
    private String version;
    private String signature;
    private int stepNumber;
    private String contextClass;

    public JobInfoDto() {

    }

    public JobInfoDto(Job job) {

        this.name = job.getName();
        this.signature = job.getSignature();
        this.version = job.getVersion();
        this.stepNumber = job.getStepCount();
        this.contextClass = job.getContextClass();
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setVersion(final String version) {
        this.version = version;
    }

    public void setSignature(final String signature) {
        this.signature = signature;
    }

    public void setStepNumber(final int stepNumber) {
        this.stepNumber = stepNumber;
    }

    public void setContextClass(final String contextClass) {
        this.contextClass = contextClass;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public String getSignature() {
        return signature;
    }

    @Override
    public int getStepCount() {
        return stepNumber;
    }

    @Override
    public String getContextClass() {
        return contextClass;
    }
}

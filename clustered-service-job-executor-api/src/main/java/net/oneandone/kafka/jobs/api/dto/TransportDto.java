package net.oneandone.kafka.jobs.api.dto;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Transport;

public class TransportDto implements Transport {
    private JobDataDto jobData = null;

    private String context = null;

    private String resumeData = null;

    @Override
    public JobData jobData() {
        return jobData;
    }

    @Override
    public String context() {
        return context;
    }

    @Override
    public String resumeData() {
        return resumeData;
    }

    public JobDataDto getJobData() {
        return jobData;
    }

    public void setJobData(final JobDataDto jobData) {
        this.jobData = jobData;
    }

    public String getContext() {
        return context;
    }

    public void setContext(final String context) {
        this.context = context;
    }

    public String getResumeData() {
        return resumeData;
    }

    public void setResumeData(final String resumeData) {
        this.resumeData = resumeData;
    }
}

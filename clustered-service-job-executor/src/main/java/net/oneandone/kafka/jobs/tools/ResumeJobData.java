package net.oneandone.kafka.jobs.tools;

/**
 * used as Data for a Job to resume a suspended Job.
 */
public class ResumeJobData {

    public ResumeJobData(final String resumedJobId, final String correlationId, final String resumeData, Class resumeDataClass) {
        this.resumeData = resumeData;
        this.resumedJobId = resumedJobId;
        this.resumeDataClass = resumeDataClass.getCanonicalName();
        this.correlationId = correlationId;
    }

    String resumeData;

    String resumeDataClass;

    String resumedJobId;

    String correlationId;
}

package net.oneandone.kafka.jobs.api.dto;

/**
 * @author aschoerk
 */
public class NewJobDto {
    private String jobName;

    private String groupId;

    private String correlationId;

    private String context;

    public NewJobDto() {

    }

    public NewJobDto(final String jobName, final String context) {
        this(jobName, null, null, context);
    }

    public NewJobDto(final String jobName, final String groupId, final String correlationId, final String context) {
        this.jobName = jobName;
        this.groupId = groupId;
        this.correlationId = correlationId;
        this.context = context;
    }

    public String getJobName() {
        return jobName;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getContext() {
        return context;
    }
}

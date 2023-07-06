package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Objects;

/**
 * @author aschoerk
 */
public class CorrelationId {
    private String correlationId;
    private String jobName;

    public CorrelationId(final String correlationId, final String jobName) {
        this.correlationId = correlationId;
        this.jobName = jobName;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public String getJobName() {
        return jobName;
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        CorrelationId that = (CorrelationId) o;
        return correlationId.equals(that.correlationId) && jobName.equals(that.jobName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlationId, jobName);
    }
}

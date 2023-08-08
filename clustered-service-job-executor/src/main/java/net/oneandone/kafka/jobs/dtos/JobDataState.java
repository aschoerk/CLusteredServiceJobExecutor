package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Objects;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.State;

/**
 * @author aschoerk
 */
public class JobDataState {

    /**
     * optional correlationId used together with Job to identify idempotent requests.
     */
    private final String correlationId;

    private final String groupId;

    /**
     * The id of the currently running job instance
     */
    private final String id;
    /**
     * the state of the job
     */
    private final State state;
    /**
     * generated each time a step-execution has been started.
     */
    private final int step;
    /**
     * dependent on the state:
     * DELAYED: the expected timestamp when the job should get rescheduled
     * SUSPENDED: the expected timestamp when the suspended job should get rescheduled if no resume occurred.
     */
    private Instant date;

    /**
     * the timestamp, the job was created at.
     */
    private final Instant createdAt;
    /**
     * the partition where the instance of the job is to be found on JobDataTopic
     */
    private final int partition;
    /**
     * the offset in the partition where the instance of the job is to be found on JobDataTopic.
     */
    private final long offset;

    public JobDataState(JobDataImpl jobData) {
        this(jobData.id(), jobData.state(),
                jobData.getPartition(), jobData.getOffset(),
                jobData.date(), jobData.createdAt(), jobData.step(), jobData.correlationId(), jobData.groupId());
    }

    public JobDataState(final String id, final State state, final int partition, final long offset, final Instant date,
                        final Instant createdAt, final int step, final String correlationId, final String groupId) {
        this.id = id;
        this.state = state;
        this.date = date;
        this.createdAt = createdAt;
        this.partition = partition;
        this.offset = offset;
        this.step = step;
        this.correlationId = correlationId;
        this.groupId = groupId;
    }

    public JobDataState(final String id, final State state, final int partition, final long offset, final Instant date,
                        final Instant createdAt, final int step) {
        this(id, state, partition, offset, date, createdAt, step, null, null);
    }



    public String getCorrelationId() {
        return correlationId;
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public Instant getDate() {
        return date;
    }

    public int getStep() {
        return step;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public String getGroupId() {
        return groupId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        JobDataState that = (JobDataState) o;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "JobDataState{" +
               "correlationId='" + correlationId + '\'' +
               ", id='" + id + '\'' +
               ", state=" + state +
               ", step=" + step +
               ", date=" + date +
               ", groupId='" + groupId + '\'' +
               ", createdAt=" + createdAt +
               ", partition=" + partition +
               ", offset=" + offset +
               '}';
    }

    public void setDate(final Instant instant) {
        this.date = instant;
    }
}

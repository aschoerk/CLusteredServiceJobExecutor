package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;

import net.oneandone.kafka.jobs.api.State;

/**
 * @author aschoerk
 */
public class JobDataState {

    public JobDataState(final String id, final State state, final int partition, final long offset, final Instant date) {
        this.id = id;
        this.state = state;
        this.date = date;
        this.partition = partition;
        this.offset = offset;
    }

    /**
     * The id of the currently running job instance
     */
    private String id;

    /**
     * the state of the job
     */
    private State state;

    /**
     * dependent on the state:
     * DELAYED: the expected timestamp when the job should get rescheduled
     * SUSPENDED: the expected timestamp when the suspended job should get rescheduled if no resume occurred.
     */
    private Instant date;

    /**
     * the partition where the instance of the job is to be found on JobDataTopic
     */
    private int partition;

    /**
     * the offset in the partition where the instance of the job is to be found on JobDataTopic.
     */
    private long offset;

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    public Instant getDate() {
        return date;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}

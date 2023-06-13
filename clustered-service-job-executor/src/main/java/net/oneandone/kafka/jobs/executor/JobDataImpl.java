package net.oneandone.kafka.jobs.executor;

import java.util.UUID;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Remark;
import net.oneandone.kafka.jobs.api.State;

/**
 * @author aschoerk
 */
public class JobDataImpl implements JobData {
    String id;

    String jobSignature;

    State state;

    int step;

    Remark[] errors = null;

    Remark[] comments = null;

    public <T> JobDataImpl(Job<T> job) {
        this.jobSignature = job.signature();
        this.state = State.RUNNING;
        this.step = 0;
        this.id = UUID.randomUUID().toString();
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
}

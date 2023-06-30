package net.oneandone.kafka.jobs.beans;

import java.util.Map;
import java.util.concurrent.BlockingDeque;

import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class Beans {

    private final Container container;
    private final EngineImpl engine;

    private final Executor executor;

    private final Sender sender;

    private final Receiver receiver;

    private final PendingHandler pendingHandler;

    private final BlockingDeque<TransportImpl> queue;

    private final Map<String, JobDataState> jobDataStates;

    private final Map<String, JobImpl> jobs;

    private final Map<String, Map<String, JobDataState>> jobDataCorrelationIds;
    private final JobTools jobTools;

    private final MetricCounts metricCounts;
    private RemoteExecutors remoteExecutors;

    public Beans(Container container, BeansFactory beansFactory) {
        this.container = container;

        jobs = beansFactory.createJobsMap();
        queue = beansFactory.createQueue();
        jobDataStates = beansFactory.createJobDataStates();
        jobDataCorrelationIds = beansFactory.createJobDataCorrelationIds();
        this.engine = beansFactory.createEngine(this);
        this.executor = beansFactory.createExecutor(this);
        this.sender = beansFactory.createSender(this);
        this.receiver = beansFactory.createReceiver(this);
        this.jobTools = beansFactory.createJobTools(this);
        this.pendingHandler = beansFactory.createPendingHandler(this);
        this.metricCounts = beansFactory.createMetricCounts(this);
        this.remoteExecutors = beansFactory.createRemoteExecutors(this);
    }

    public Map<String, Map<String, JobDataState>> getJobDataCorrelationIds() {
        return jobDataCorrelationIds;
    }

    public Container getContainer() {
        return container;
    }

    public EngineImpl getEngine() {
        return engine;
    }

    public Executor getExecutor() {
        return executor;
    }

    public Sender getSender() {
        return sender;
    }

    public Receiver getReceiver() {
        return receiver;
    }

    public PendingHandler getPendingHandler() {
        return pendingHandler;
    }

    public BlockingDeque<TransportImpl> getQueue() {
        return queue;
    }

    public Map<String, JobImpl> getJobs() {
        return jobs;
    }

    public Map<String, JobDataState> getJobDataStates() {
        return jobDataStates;
    }

    public JobTools getJobTools() {
        return jobTools;
    }

    public MetricCounts getMetricCounts() {
        return metricCounts;
    }

    public RemoteExecutors getRemoteExecutors() { return remoteExecutors; }

}

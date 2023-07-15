package net.oneandone.kafka.jobs.beans;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class Beans extends StoppableBase {

    static AtomicInteger beanCounter = new AtomicInteger(0);

    int count;

    private final Container container;
    private final EngineImpl engine;

    private final Executor executor;

    private final Sender sender;

    private final Receiver receiver;

    private final PendingHandler pendingHandler;

    private final BlockingDeque<TransportImpl> queue;

    private final Map<String, JobDataState> jobDataStates;

    private final Map<String, JobImpl> jobs;

    private final Map<String, JobDataState> jobDataCorrelationIds;
    private final JobTools jobTools;

    private final MetricCounts metricCounts;
    private final Reviver reviver;
    private RemoteExecutors remoteExecutors;
    private Map<String, Queue<JobDataState>> statesByGroup;
    private Set<String> jobsCreatedByThisNodeForGroup;

    public Beans(Container container, BeansFactory beansFactory) {
        super(null);
        this.container = container;
        this.count = beanCounter.incrementAndGet();

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
        this.reviver = beansFactory.createResurrection(this);
        this.statesByGroup = beansFactory.creatStatesByGroup();
        this.jobsCreatedByThisNodeForGroup = beansFactory.createJobsCreatedByThisNodeForGroup();
        setRunning();
    }

    public Map<String, JobDataState> getJobDataCorrelationIds() {
        return jobDataCorrelationIds;
    }

    public Map<String, Queue<JobDataState>> getStatesByGroup() {
        return statesByGroup;
    }

    public Set<String> getGroupJobsResponsibleFor() {
        return jobsCreatedByThisNodeForGroup;
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

    /**
     * registered jobs by signature
     * @return the registered jobs by signature
     */
    public Map<String, JobImpl> getJobs() {
        return jobs;
    }

    /**
     * the currently last state of all jobs as
     * @return the currently last state of all jobs
     */
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

    public int getCount() {
        return count;
    }

    @Override
    public void setShutDown() {
        this.beans = this;
        super.setShutDown();
        reviver.setShutDown();
        receiver.setShutDown();
        Thread queueWaiter = container.createThread (() -> {
                    while (!queue.isEmpty()) {
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                });
        queueWaiter.start();
        waitForThreads(queueWaiter);
        executor.setShutDown();
        waitForStoppables(receiver, executor, reviver);
        pendingHandler.setShutDown();
        this.stopStoppables(engine, jobTools, sender, metricCounts, remoteExecutors);
    }

    public Reviver getReviver() {
        return reviver;
    }
}

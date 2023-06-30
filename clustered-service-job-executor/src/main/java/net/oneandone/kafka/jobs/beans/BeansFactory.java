package net.oneandone.kafka.jobs.beans;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class BeansFactory {

    public EngineImpl createEngine(Beans beans) {
        return new EngineImpl(beans);
    }

    public Executor createExecutor(Beans beans) {
        return new Executor(beans);
    }

    public Sender createSender(Beans beans) {
        return new Sender(beans);
    }

    public Receiver createReceiver(Beans beans) {
        return new Receiver(beans);
    }

    public PendingHandler createPendingHandler(Beans beans) {
        return new PendingHandler(beans);
    }

    public BlockingDeque<TransportImpl> createQueue() {
        return new LinkedBlockingDeque<>(1000);
    }

    public Map<String, JobImpl> createJobsMap() {
        return new ConcurrentHashMap<>();
    }
    public JobTools createJobTools(Beans beans) {
        return new JobTools(beans);
    }

    public MetricCounts createMetricCounts(Beans beans) {
        return new MetricCounts(beans);
    }

    Map<String, JobDataState> createJobDataStates() {return new ConcurrentHashMap<>(); }

    Map<String, Map<String, JobDataState>> createJobDataCorrelationIds() {return new ConcurrentHashMap<>(); }

    public RemoteExecutors createRemoteExecutors(final Beans beans) {
        return new RemoteExecutors(beans);
    }
}

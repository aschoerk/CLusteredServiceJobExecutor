package net.oneandone.kafka.jobs.beans;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import net.oneandone.kafka.clusteredjobs.NodeFactory;
import net.oneandone.kafka.clusteredjobs.NodeFactoryImpl;
import net.oneandone.kafka.clusteredjobs.NodeImpl;
import net.oneandone.kafka.clusteredjobs.api.Container;
import net.oneandone.kafka.clusteredjobs.api.Node;
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

    public JobsSender createSender(Beans beans) {
        return new JobsSender(beans);
    }

    public Receiver createReceiver(Beans beans) {
        return new Receiver(beans);
    }

    public JobsPendingHandler createPendingHandler(Beans beans) {
        return new JobsPendingHandler(beans);
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

    Map<String, JobDataState> createJobDataCorrelationIds() {return new ConcurrentHashMap<>(); }

    public RemoteExecutors createRemoteExecutors(final Beans beans) {
        return new RemoteExecutors(beans);
    }

    public ClusteredJobReviver createReviver(final Beans beans) { return new ClusteredJobReviver(beans); }

    public Map<String, Queue<JobDataState>> creatStatesByGroup() {
        return new ConcurrentHashMap<>();
    }

    public Set<String> createJobsCreatedByThisNodeForGroup() {
        return ConcurrentHashMap.newKeySet();
    }

    public NodeFactory createNodeFactory() {
        return new NodeFactoryImpl();
    }

    public Node createNode(Container container, NodeFactory nodeFactory) {
        return new NodeImpl(container, nodeFactory);
    }
}

package net.oneandone.kafka.jobs.beans;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.Transport;

/**
 * @author aschoerk
 */
public class RemoteExecutors extends StoppableBase implements RemoteExecutor {

    public RemoteExecutors(final Beans beans) {
        super(beans);
    }

    Map<String, List<RemoteExecutor>> executors = new ConcurrentHashMap<>();
    @Override
    public JobInfo[] supportedJobs() {
        throw new KjeException("Not expected call to supported Jobs");
    }

    @Override
    public StepResult handle(final Transport transport) {
        List<RemoteExecutor> candidates = executors.get(transport.jobData().jobSignature());
        if (candidates == null || candidates.isEmpty()) {
            throw new KjeException("Did not find Executor for " + transport.jobData());
        }
        return candidates.iterator().next().handle(transport);
    }

    boolean thereIsRemoteExecutor(String signature) {
        return executors.get(signature) != null && executors.get(signature).size() > 0;
    }

    public void addExecutor(final RemoteExecutor remoteExecutor) {
        Arrays.stream(remoteExecutor.supportedJobs()).forEach(j -> {
            final String signature = j.signature();
            if (executors.get(signature) == null) {
                synchronized (this) {
                    if (executors.get(signature) == null) {
                        executors.put(signature, new ArrayList<>());
                    }

                }
            }
            executors.get(signature).add(remoteExecutor);
        });
    }
}

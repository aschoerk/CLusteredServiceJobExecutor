package net.oneandone.kafka.jobs.beans;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.dto.TransportDto;

/**
 * @author aschoerk
 */
public class RemoteExecutors extends StoppableBase implements RemoteExecutor {

    private Random random = new Random(Instant.now().toEpochMilli());

    public RemoteExecutors(final Beans beans) {
        super(beans);
        beans.getContainer().submitLongRunning(() -> {
            while (true) {
                RemoteExecutor[] remoteExecutors = beans.getContainer().getRemoteExecutors();
                synchronized (this) {
                    executors.clear();
                    for (RemoteExecutor r : remoteExecutors) {
                        addExecutor(r);
                    }
                }
                try {
                    Thread.sleep(Duration.ofSeconds(60).toMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    Map<String, List<RemoteExecutor>> executors = new ConcurrentHashMap<>();
    @Override
    public JobInfo[] supportedJobs() {
        throw new KjeException("Not expected call to supported Jobs");
    }

    @Override
    public StepResult handle(final TransportDto transport) {
        List<RemoteExecutor> candidates = executors.get(transport.jobData().getSignature());
        int index = 0;
        if ((candidates == null) || candidates.isEmpty()) {
            throw new KjeException("Did not find Executor for " + transport.jobData());
        } else {
            index = random.nextInt(candidates.size());
            return candidates.get(index).handle(transport);
        }
    }

    RemoteExecutor thereIsRemoteExecutor(String signature) {
        synchronized (this) {
            List<RemoteExecutor> candidates = executors.get(signature);
            int index = 0;
            if ((candidates == null) || candidates.isEmpty()) {
                return null;
            } else {
                index = random.nextInt(candidates.size());
                return candidates.get(index);
            }
        }
    }

    public void addExecutor(final RemoteExecutor remoteExecutor) {
        Arrays.stream(remoteExecutor.supportedJobs()).forEach(j -> {
            final String signature = j.getSignature();
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

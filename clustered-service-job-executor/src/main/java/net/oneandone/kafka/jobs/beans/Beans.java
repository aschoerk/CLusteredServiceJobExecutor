package net.oneandone.kafka.jobs.beans;

import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.dtos.ContextImpl;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class Beans {

    final Container container;
    final ExecutorImpl executor;

    final Sender sender;

    final Receiver receiver;

    final BlockingDeque<ContextImpl> queue = new LinkedBlockingDeque<>(1000);

    final Map<String, JobImpl> jobs = new ConcurrentHashMap<>();
    final JobTools jobTools;

    final MetricCounts metricCounts;

    public Beans(Container container)  {
        this.container = container;
        this.executor = new ExecutorImpl(this);
        this.sender = new Sender(this);
        this.receiver = new Receiver(this);
        this.jobTools = new JobTools(this);
        this.metricCounts = new MetricCounts(this);
    }

}

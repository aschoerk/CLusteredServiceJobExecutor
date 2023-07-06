package net.oneandone.kafka.jobs.executor.support;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.Sender;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;

/**
 * @author aschoerk
 */
public class TestSender extends Sender {

    public Map<State, AtomicInteger> stateCounts = new HashMap<>();

    public Map<String, State> jobStates = new ConcurrentHashMap<>();

    public Map<Pair<String, String>, TransportImpl> lastContexts = new ConcurrentHashMap<>();

    public Map<Pair<String, String>, ConsumerRecord> lastConsumerRecordsUsedForState = new ConcurrentHashMap<>();

    public TestSender(Beans beans) {
        super(beans);
        for (State state: State.values()) {
            stateCounts.put(state, new AtomicInteger(0));
        }
    }

    @Override
    public <T> void send(final TransportImpl context) {
        synchronized (lastContexts) {
            final JobDataImpl jobData = context.jobData();
            lastContexts.put(Pair.of(jobData.contextClass(), jobData.id()), context );
        }
        super.send(context);
    }

    @Override
    public void sendState(final JobDataImpl jobData, final ConsumerRecord r) {
        synchronized (lastConsumerRecordsUsedForState) {
            lastConsumerRecordsUsedForState.put(Pair.of(jobData.contextClass(), jobData.id()), r );
            stateCounts.get(jobData.state()).incrementAndGet();
            jobStates.put(jobData.id(), jobData.state());
        }
        super.sendState(jobData, r);
    }

    public void addStates(final TestSender currentTestsender) {
        synchronized (lastContexts) {
            currentTestsender.lastContexts.entrySet().forEach(
                    e -> {
                        if (!lastContexts.containsKey(e.getKey())) {
                            lastContexts.put(e.getKey(), e.getValue());
                        }
                    }
            );
        }
        synchronized (lastConsumerRecordsUsedForState) {
            currentTestsender.lastConsumerRecordsUsedForState.entrySet().forEach(
                    e -> {
                        if (!lastConsumerRecordsUsedForState.containsKey(e.getKey())) {
                            lastConsumerRecordsUsedForState.put(e.getKey(), e.getValue());
                        }
                    }
            );
            currentTestsender.stateCounts.entrySet().forEach(
                    e -> {
                        if (stateCounts.containsKey(e.getKey())) {
                            stateCounts.get(e.getKey()).addAndGet(e.getValue().get());
                        }
                    }
            );
            currentTestsender.jobStates.entrySet().forEach(
                    e -> {
                        if (!jobStates.containsKey(e.getKey())) {
                            jobStates.put(e.getKey(), e.getValue());
                        }
                    }
            );
        }
    }
}

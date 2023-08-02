package net.oneandone.kafka.jobs.executor.support;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class TestSenderData {
    public Map<State, AtomicInteger> stateCounts = new HashMap<>();

    public Map<String, String> jobStates = new ConcurrentHashMap<>();

    public Map<Pair<String, String>, TransportImpl> lastContexts = new ConcurrentHashMap<>();

    public Map<Pair<String, String>, ConsumerRecord> lastConsumerRecordsUsedForState = new ConcurrentHashMap<>();

    public Set<String> stateSendingEngines = ConcurrentHashMap.newKeySet();

    public TestSenderData() {
        for (State state: State.values()) {
            stateCounts.put(state, new AtomicInteger(0));
        }
    }
}

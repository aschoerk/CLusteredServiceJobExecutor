package net.oneandone.kafka.jobs.beans;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;


/**
 * @author aschoerk
 */
public class Receiver extends StoppableBase {

    static final String GROUP_ID = "ClusteredServiceJobExecutor";
    private Future stateInitWaitingThread;

    private Future jobDataReceiverThread;
    private boolean stateInitCompleted = false;


    private AtomicReferenceArray<JobDataState> lastOfPartition;
    private final Map<String, TransportImpl> sentRunning = new ConcurrentHashMap<>();
    private KafkaConsumer<String, String> jobReaderConsumer = null;
    private KafkaConsumer<String, String> jobMultiReaderConsumer = null;

    public Receiver(Beans beans) {
        super(beans);

        final Map<String, Object> consumerConfig = getConsumerConfig(beans);
        // jobStates must be received by all nodes.
        consumerConfig.put(GROUP_ID_CONFIG, beans.getEngine().getName());

        this.jobDataReceiverThread = submitLongRunning(() -> {
            initThreadName("Data-Receiver");
            receiveJobData(getConsumerConfig(beans));
        });
        logger.info("Receiver job Data receiving started");

        consumerConfig.put(GROUP_ID_CONFIG, beans.getEngine().getName());
    }

    static Map<String, Object> getConsumerConfig(final Beans beans) {
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, beans.getContainer().getBootstrapServers());
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG,
                (int) beans.getContainer().getConfiguration().getConsumerPollInterval().toMillis());
        consumerConfig.put(GROUP_ID_CONFIG, GROUP_ID);
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG,
                (int) beans.getContainer().getConfiguration().getMaxPollJobDataRecords());
        return consumerConfig;
    }

    static JobDataState getLatestChange(final Beans beans) {
        JobDataState latestGroupChange = null;
        AtomicReferenceArray<JobDataState> latestStates = beans.getReceiver().getLastOfPartion();
        for (int i = 0; i < latestStates.length(); i++) {
            JobDataState act = latestStates.get(i);
            if(latestGroupChange == null) {
                latestGroupChange = act;
            }
            else {
                if((act != null) && act.getCreatedAt().isAfter(latestGroupChange.getCreatedAt())) {
                    latestGroupChange = act;
                }
            }
        }
        return latestGroupChange;
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(jobDataReceiverThread, stateInitWaitingThread);
        if(jobReaderConsumer != null) {
            jobReaderConsumer.close();
            jobReaderConsumer = null;
        }
    }

    private void receiveJobData(final Map<String, Object> syncingConsumerConfig) {
        logger.info("Starting JobData Receiver for Group: {}", syncingConsumerConfig.get(GROUP_ID_CONFIG));
        try (KafkaConsumer<String, String> consumer = beans.createConsumer(syncingConsumerConfig)) {
            consumer.subscribe(Collections.singleton(beans.getContainer().getJobDataTopicName()));
            while (!doShutDown()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    TransportImpl transport = beans.getJobTools().evaluatePackage(r);
                    handleSingleRecord(transport);
                }
            }
        }
    }

    protected void handleSingleRecord(final TransportImpl transport) {
        final JobDataImpl jobData = transport.jobData();
        logger.trace("E: {} Received jobData for {} id: {} partition: {} offset: {} state: {} step: {} stepCount: {}",
                beans.getEngine().getName(), jobData.getSignature(), jobData.getId(),
                jobData.getPartition(), jobData.getOffset(),
                jobData.getState(), jobData.getStep(), jobData.getStepCount());

        switch (jobData.getState()) {
            case RUNNING:
                if(!beans.getExecutor().executeJob( transport)) {
                    beans.getExecutor().delayJob(transport, "Not scheduled in internal queue");
                    beans.getSender().send(transport);
                }
                break;
            case DELAYED:
                beans.getPendingHandler().schedulePending(transport);
                // this node got the task to reschedule the job as soon as date is
                break;
            case ERROR:
                break;
        }
    }


    public TransportImpl readJob(final JobDataState state) {
        if(jobReaderConsumer == null) {
            Map<String, Object> consumerConfig = getConsumerConfig(beans);
            consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
            consumerConfig.put(GROUP_ID_CONFIG, beans.getNodeId());
            jobReaderConsumer = beans.createConsumer(consumerConfig);
        }
        TopicPartition topicPartition = new TopicPartition(beans.getContainer().getJobDataTopicName(), state.getPartition());
        jobReaderConsumer.assign(Collections.singletonList(topicPartition));
        jobReaderConsumer.seek(topicPartition, state.getOffset());
        boolean found = false;
        do {
            ConsumerRecords<String, String> records = jobReaderConsumer.poll(Duration.ofMillis(1000));
            found = !records.isEmpty();
            Optional<ConsumerRecord<String, String>> optionalR = records.records(topicPartition).stream().filter(r -> r.offset() == state.getOffset()).findAny();
            if(optionalR.isPresent()) {
                TransportImpl context = beans.getJobTools().evaluatePackage(optionalR.get());
                state.setDate(beans.getContainer().getClock().instant());
                return context;
            }
        } while (found && !doShutDown());
        return null;
}


    public AtomicReferenceArray<JobDataState> getLastOfPartion() {
        return lastOfPartition;
    }

    public void setStateInitCompleted() {
        stateInitCompleted = true;
    }

    public List<TransportImpl> readJobs(final List<Triple<Integer, Long, Long>> minMaxTriples,
                                        final Map<Integer, List<Pair<Integer, Long>>> statesToRevive) {
        if(jobMultiReaderConsumer == null) {
            Map<String, Object> consumerConfig = getConsumerConfig(beans);
            consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1000);
            consumerConfig.put(GROUP_ID_CONFIG, beans.getNodeId());
            jobMultiReaderConsumer = beans.createConsumer(consumerConfig);
        }
        Map<Integer, Set<Long>> offsetMap = statesToRevive.entrySet().stream()
                .map(e -> Pair.of(e.getKey(), e.getValue().stream().map(Pair::getRight).collect(Collectors.toSet())))
                .collect(Collectors.toList())
                .stream()
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
        ArrayList<TransportImpl> result = new ArrayList<>();
        for (Triple<Integer, Long, Long> t: minMaxTriples) {
            Integer partition = t.getLeft();
            final Set<Long> offsets = offsetMap.get(partition);
            Long minOffset = t.getMiddle();
            Long maxOffset = t.getRight();
            TopicPartition topicPartition = new TopicPartition(beans.getContainer().getJobDataTopicName(), partition);
            jobMultiReaderConsumer.assign(Collections.singletonList(topicPartition));
            jobMultiReaderConsumer.seek(topicPartition, minOffset);
            MutableBoolean partitionReady = new MutableBoolean(false);
            do {
                ConsumerRecords<String, String> records = jobMultiReaderConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records.records(topicPartition)) {
                    if(offsets.contains(r.offset())) {
                        result.add(beans.getJobTools().evaluatePackage(r));
                    }
                    if((r.offset() > maxOffset)) {
                        partitionReady.setTrue();
                        break;
                    }
                }
            } while (partitionReady.isFalse() && !doShutDown());
        }
        return result;
    }
}

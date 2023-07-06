package net.oneandone.kafka.jobs.beans;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import net.oneandone.kafka.jobs.dtos.CorrelationId;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class Receiver extends StoppableBase {

    static final String GROUP_ID = "ClusteredServiceJobExecutor";
    private final Thread jobStateReceiverThread;
    private Thread stateInitWaitingThread;

    private Thread jobDataReceiverThread;
    private boolean stateInitCompleted = false;

    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(jobDataReceiverThread, jobStateReceiverThread, stateInitWaitingThread);
    }

    public Receiver(Beans beans) {
        super(beans);



        final Map<String, Object> consumerConfig = getConsumerConfig(beans);
        // jobStates must be received by all nodes.
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
        this.jobStateReceiverThread = beans.getContainer().createThread(() -> {
            initThreadName("State-Receiver");
            consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
            receiveJobState(consumerConfig);
        });
        this.jobStateReceiverThread.start();
        stateInitWaitingThread = beans.getContainer().createThread(() -> {
                try {
                    while(!stateInitCompleted) {
                        Thread.sleep(100);
                    }
                    Receiver.this.stateInitCompleted = true;
                    Receiver.this.stateInitWaitingThread = null;
                    this.jobDataReceiverThread = beans.getContainer().createThread(() -> {
                        initThreadName("Data-Receiver");
                        receiveJobData( getConsumerConfig(beans));
                    });
                    this.jobDataReceiverThread.start();
                    beans.getResurrection().initResurrection();
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        stateInitWaitingThread.start();

        Map<String, Object> singleJobConsumerConfig = getConsumerConfig(beans);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
    }

    private static Map<String, Object> getConsumerConfig(final Beans beans) {
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, beans.getContainer().getBootstrapServers());
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 100);
        consumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        consumerConfig.put(GROUP_ID_CONFIG, GROUP_ID);
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerConfig;
    }

    private void receiveJobState(final Map<String, Object> consumerConfig) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            final String jobStateTopicName = beans.getContainer().getJobStateTopicName();
            List<PartitionInfo> partitions = consumer.partitionsFor(jobStateTopicName);
            List<TopicPartition> topicPartitions = partitions
                    .stream()
                    .map(p -> new TopicPartition(jobStateTopicName, p.partition()))
                    .collect(Collectors.toList());
            Map<Integer, Long> currentEndOffsets =
                    consumer
                            .endOffsets(topicPartitions)
                            .entrySet()
                            .stream()
                            .collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue()));
            consumer.beginningOffsets(topicPartitions).entrySet().stream().forEach(e -> {
                if (currentEndOffsets.get(e.getKey().partition()) <= e.getValue()) {
                    currentEndOffsets.remove(e.getKey().partition());
                }
            });

            consumer.subscribe(Collections.singleton(jobStateTopicName));
            while (!doShutDown()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (currentEndOffsets.isEmpty() || records.isEmpty()) {
                    this.stateInitCompleted = true;
                }
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    if (currentEndOffsets.containsKey(r.partition())) {
                        if (currentEndOffsets.get(r.partition()) <= r.offset()) {
                            currentEndOffsets.remove(r.partition());
                        }
                    }
                    JobDataState jobDataState = JsonMarshaller.gson.fromJson(json, JobDataState.class);
                    beans.getJobDataStates().put(jobDataState.getId(), jobDataState);
                    if (jobDataState.getCorrelationId() != null) {
                        CorrelationId correlationId = new CorrelationId(jobDataState.getCorrelationId(), jobDataState.getJobName());
                        JobDataState existingJobDataState = beans.getJobDataCorrelationIds().get(correlationId);
                        if (!existingJobDataState.getId().equals(jobDataState.getId())) {
                            logger.error("different Jobs {}, {} with the same correlationId {}",jobDataState, existingJobDataState, correlationId);
                        }
                        beans.getJobDataCorrelationIds().put(correlationId, jobDataState);
                    }
                }
            }
        }
    }

    private void receiveJobData(final Map<String, Object> syncingConsumerConfig) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(syncingConsumerConfig)) {
            consumer.subscribe(Collections.singleton(beans.getContainer().getJobDataTopicName()));
            while (!doShutDown()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {

                    TransportImpl context = evaluatePackage(r);

                    beans.getSender().sendState(context.jobData(), r);
                    switch (context.jobData().state()) {
                        case RUNNING:
                            boolean res = false;
                            try {
                                res = beans.getQueue().offer(context, 1, TimeUnit.SECONDS);
                            } catch (InterruptedException e) {

                            }
                            if(!res) {
                                beans.getExecutor().delayJob(context, "Not scheduled in internal queue");
                                beans.getSender().send(context);
                            }
                            break;
                        case DELAYED:
                            beans.getPendingHandler().schedulePending(context);
                            // this node got the task to reschedule the job as soon as date is
                            break;
                        case ERROR:
                            break;
                    }
                }
            }
        }
    }

    private TransportImpl evaluatePackage(final ConsumerRecord<String, String> r) {
        String json = r.value();
        String[] resultString = json.split(Sender.SEPARATORREGEX);
        JobDataImpl jobData = JsonMarshaller.gson.fromJson(resultString[0], JobDataImpl.class);
        TransportImpl context = new TransportImpl(jobData, resultString[1], beans);
        if (resultString.length == 3) {
            context.setResumeData(resultString[2]);
        }
        return context;
    }

    public TransportImpl readJob(final JobDataState state) {
        Map<String, Object> consumerConfig = getConsumerConfig(beans);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig)) {
            TopicPartition topicPartition = new TopicPartition(beans.getContainer().getJobDataTopicName(), state.getPartition());
            consumer.seek(topicPartition, state.getOffset());
            boolean found = false;
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                found = !records.isEmpty();
                Optional<ConsumerRecord<String, String>> optionalR = records.records(topicPartition).stream().filter(r -> r.offset() == state.getOffset()).findAny();
                if (optionalR.isPresent()) {
                    TransportImpl context = evaluatePackage(optionalR.get());
                    return context;
                }
            } while (found && !doShutDown());
        }
        return null;
    }


}

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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

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

    private Thread jobDataReceiverThread;

    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(jobDataReceiverThread, jobStateReceiverThread);
    }

    public Receiver(Beans beans) {
        super(beans);

        final Map<String, Object> consumerConfig = getConsumerConfig(beans);
        this.jobDataReceiverThread = beans.getContainer().createThread(() -> {
            initThreadName("Data-Receiver");
            receiveJobData(consumerConfig);
        });
        this.jobDataReceiverThread.start();
        // jobStates must be received by all nodes.
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
        this.jobStateReceiverThread = beans.getContainer().createThread(() -> {
            initThreadName("State-Receiver");
            consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
            receiveJobState(consumerConfig);
        });
        this.jobStateReceiverThread.start();
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
            consumer.subscribe(Collections.singleton(beans.getContainer().getJobStateTopicName()));
            while (!doShutDown()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    JobDataState jobDataState = JsonMarshaller.gson.fromJson(json, JobDataState.class);
                    beans.getJobDataStates().put(jobDataState.getId(), jobDataState);
                    if (jobDataState.getCorrelationId() != null) {
                        Map<String, JobDataState> perJob = beans.getJobDataCorrelationIds().get(jobDataState.getJobName());
                        if (perJob == null) synchronized (this) {
                            if (beans.getJobDataCorrelationIds().get(jobDataState.getJobName()) == null) {
                                beans.getJobDataCorrelationIds().put(jobDataState.getJobName(), new ConcurrentHashMap<>());
                            }
                            perJob = beans.getJobDataCorrelationIds().get(jobDataState.getJobName());
                        }
                        JobDataState existing = perJob.get(jobDataState.getCorrelationId());
                        if (!existing.getId().equals(jobDataState.getId())) {
                            // TODO: there is a second different job instance with the same correlationId. Set to ERROR?
                        }
                        perJob.put(jobDataState.getCorrelationId(), jobDataState);
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

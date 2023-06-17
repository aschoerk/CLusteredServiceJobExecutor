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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.ContextImpl;
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

    Map<String, JobDataState> jobDataStates = new HashMap<>();


    public Receiver(Beans beans) {
        super(beans);
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, beans.container.getBootstrapServers());
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 100);
        consumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        consumerConfig.put(GROUP_ID_CONFIG, GROUP_ID);
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.jobDataReceiverThread = beans.container.createThread(() -> {
            receiveJobData(beans, consumerConfig);
        });
        this.jobDataReceiverThread.start();
        // jobStates must be received by all nodes.
        consumerConfig.put(GROUP_ID_CONFIG, beans.container.getConfiguration().getNodeName());
        this.jobStateReceiverThread = beans.container.createThread(() -> {
            receiveJobState(beans, consumerConfig);
        });
        this.jobStateReceiverThread.start();
    }

    private void receiveJobState(final Beans beans, final Map<String, Object> consumerConfig) {
        try ( KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            consumer.subscribe(Collections.singleton(beans.container.getJobStateTopicName()));
            while (!doShutDown()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    JobDataState jobDataState = JsonMarshaller.gson.fromJson(json, JobDataState.class);
                    if (jobDataState.getState() != State.DONE) {
                        jobDataStates.put(jobDataState.getId(), jobDataState);
                    } else {
                        jobDataStates.remove(jobDataState.getId());
                    }
                }
            }

        }
    }

    private void receiveJobData(final Beans beans, final Map<String, Object> syncingConsumerConfig) {
        try ( KafkaConsumer<String, String> consumer = new KafkaConsumer<>(syncingConsumerConfig)) {
            consumer.subscribe(Collections.singleton(beans.container.getJobDataTopicName()));
            while (!doShutDown()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    String[] resultString = json.split(Sender.SEPARATORREGEX);
                    JobDataImpl jobData = JsonMarshaller.gson.fromJson(resultString[0], JobDataImpl.class);
                    Object o = beans.container.unmarshal(resultString[1], jobData.contextClass());
                    if(o == null) {
                        o = JsonMarshaller.gson.fromJson(resultString[1], jobData.contextClass());
                    }
                    ContextImpl context = new ContextImpl(jobData, o);
                    boolean res = false;
                    try {
                        res = beans.queue.offer(context, 1, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {

                    }
                    if(!res) {
                        context.jobData().setState(State.DELAYED);
                        beans.sender.send(context);
                    } else {
                        beans.sender.sendState(jobData, r);
                    }
                }
            }
        }
    }
}

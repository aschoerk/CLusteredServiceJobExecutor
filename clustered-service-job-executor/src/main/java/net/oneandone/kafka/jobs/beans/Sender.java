package net.oneandone.kafka.jobs.beans;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class Sender extends StoppableBase {

    static final String SEPARATOR = "\n|||SEPARATOR|||\n";
    static final String SEPARATORREGEX = "\n\\|\\|\\|SEPARATOR\\|\\|\\|\n";

    private final Map<String, Object> config;

    private KafkaProducer jobDataProducer;

    protected Sender(Beans beans) {
        super(beans);
        this.config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, beans.getContainer().getBootstrapServers());
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    }

    private KafkaProducer getJobDataProducer() {
        if(jobDataProducer == null) {
            this.jobDataProducer = new KafkaProducer(config);
        }
        return jobDataProducer;
    }

    public <T> void send(TransportImpl context) {

        String jobDataJson = JsonMarshaller.gson.toJson(context.jobData());

        String contextJson = context.context();

        if(jobDataJson.contains(SEPARATOR) || ((contextJson != null) && contextJson.contains(SEPARATOR))) {
            throw new KjeException("Could not send separator " + SEPARATOR + " containing strings" + jobDataJson + " and " + contextJson);
        }
        String toSend = String.format("%s%s%s", jobDataJson, SEPARATOR, context.context());
        final String resumeData = context.resumeData();

        if(resumeData != null) {
            if(resumeData.contains(SEPARATOR) || ((contextJson != null) && resumeData.contains(SEPARATOR))) {
                throw new KjeException("Could not send separator " + SEPARATOR + " containing strings" + jobDataJson + " and " + resumeData);
            }
            toSend = toSend + SEPARATOR + resumeData;
        }

        logger.info("Sending: {}", context.jobData());
        getJobDataProducer().send(new ProducerRecord(beans.getContainer().getJobDataTopicName(), context.jobData().id(), toSend));
    }

    public void sendState(JobDataImpl jobData, ConsumerRecord r) {

        JobDataState jobDataState = new JobDataState(jobData.id(), jobData.state(), r.partition(), r.offset(), jobData.date(), jobData.stepCount());
        logger.info("Sending: {}", jobDataState);
        String toSend = JsonMarshaller.gson.toJson(jobDataState);
        getJobDataProducer().send(new ProducerRecord(beans.getContainer().getJobStateTopicName(), jobData.id(), toSend));
    }

}

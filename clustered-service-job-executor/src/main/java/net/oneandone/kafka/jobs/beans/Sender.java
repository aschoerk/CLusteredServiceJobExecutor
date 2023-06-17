package net.oneandone.kafka.jobs.beans;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.dtos.ContextImpl;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class Sender extends StoppableBase {

    static final String SEPARATOR = "\n|||SEPARATOR|||\n";
    static final String SEPARATORREGEX = "\n\\|\\|\\|SEPARATOR\\|\\|\\|\n";

    private Map<String, Object> config;

    private KafkaProducer jobDataProducer;

    Sender(Beans beans) {
        super(beans);
        this.config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, beans.container.getBootstrapServers());
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    }

    private KafkaProducer getJobDataProducer() {
        if (jobDataProducer == null) {
            this.jobDataProducer = new KafkaProducer(config);
        }
        return jobDataProducer;
    }

    public <T> void send(ContextImpl<T> context) {

        context.jobData().setContextClass(context.context().getClass());
        String jobDataJson = JsonMarshaller.gson.toJson(context.jobData());
        String contextJson = beans.container.marshal(context.context());

        if (contextJson == null) {
            contextJson = JsonMarshaller.gson.toJson(context.context());
        }

        if (jobDataJson.contains(SEPARATOR) || contextJson != null && contextJson.contains(SEPARATOR)) {
            throw new KjeException("Could not send separator " + SEPARATOR +" containing strings" + jobDataJson + " and " + contextJson);
        }
        String toSend = String.format("%s%s%s", jobDataJson, SEPARATOR,contextJson);

        getJobDataProducer().send(new ProducerRecord(beans.container.getJobDataTopicName(), context.jobData().id(), toSend));
    }

    public void sendState(JobDataImpl jobData, ConsumerRecord r) {
        JobDataState jobDataState = new JobDataState(jobData.id(), jobData.state(), r.partition(), r.offset(), jobData.date());
        String toSend = JsonMarshaller.gson.toJson(jobDataState);
        getJobDataProducer().send(new ProducerRecord(beans.container.getJobStateTopicName(), jobData.id(), toSend));
    }

}

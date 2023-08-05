package net.oneandone.kafka.jobs.beans;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class JobsSender extends StoppableBase {

    static final String SEPARATOR = "\n|||SEPARATOR|||\n";
    static final String SEPARATORREGEX = "\n\\|\\|\\|SEPARATOR\\|\\|\\|\n";

    private final Map<String, Object> config;

    private KafkaProducer jobDataProducer;

    private final Deque<Future<RecordMetadata>> futures = new ConcurrentLinkedDeque<>();

    protected JobsSender(Beans beans) {
        super(beans);
        this.config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, beans.getContainer().getBootstrapServers());
        config.put(ProducerConfig.ACKS_CONFIG, "1");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 20000000 );

        setRunning();
    }

    private KafkaProducer getJobDataProducer() {
        if(jobDataProducer == null) {
            this.jobDataProducer = new KafkaProducer(config);
        }
        return jobDataProducer;
    }

    public <T> void send(TransportImpl context) {

        final JobDataImpl jobData = context.jobData();
        logger.info("E: {} Sending jobData for {} id: {} state: {} step: {} stepCount: {}", beans.getEngine().getName(), jobData.jobSignature(), jobData.id(), jobData.state(), jobData.step(), jobData.stepCount());
        String jobDataJson = JsonMarshaller.gson.toJson(jobData);

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

        futures.removeIf(f -> f.isDone());


        futures.add(doSend(new ProducerRecord(beans.getContainer().getJobDataTopicName(), jobData.id(), toSend)));
        if(futures.size() > 100) {
            getJobDataProducer().flush();
        }

    }

    public void sendState(JobDataImpl jobData, ConsumerRecord r) {
        logger.info("E: {} Sending state for {} ", beans.getEngine().getName(), jobData);

        futures.removeIf(f -> f.isDone());
        JobDataState jobDataState = new JobDataState(jobData.id(), jobData.state(),
                r.partition(), r.offset(), jobData.date(), jobData.createdAt(), jobData.step(), jobData.correlationId(), jobData.groupId());
        logger.info("Beans: {} Sending state record: {}", beans.getEngine().getName(), jobDataState);
        jobDataState.setSent(beans.getContainer().getClock().instant());
        jobDataState.setSender(beans.getNode().getUniqueNodeId());
        String toSend = JsonMarshaller.gson.toJson(jobDataState);
        futures.removeIf(f -> f.isDone());
        futures.add(doSend(new ProducerRecord(beans.getContainer().getJobStateTopicName(), jobData.id(), toSend)));
    }

    Future<?> doSend(ProducerRecord<String, String> toSend) {
        try {
            return getJobDataProducer().send(toSend);
        } catch (IllegalStateException e) {
            jobDataProducer = null;
            return getJobDataProducer().send(toSend);
        }
    }

    @Override
    public void setShutDown() {
        try {
            super.setShutDown();
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
                futures.removeIf(f -> f.isDone());
            } while (futures.size() > 0);
            getJobDataProducer().close();
        } catch (Exception e) {
            logger.error("Exception during shutdown", e);
        } finally {
            setRunning(false);
        }
    }
}

package net.oneandone.kafka.jobs.beans;

import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
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



    private final Map<String, Object> config;

    private KafkaProducer jobDataProducer;

    private final Deque<Future<RecordMetadata>> futures = new ConcurrentLinkedDeque<>();

    protected JobsSender(Beans beans) {
        super(beans);
        this.config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, beans.getContainer().getBootstrapServers());
        config.put(ProducerConfig.ACKS_CONFIG, "-1");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // config.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 20000000 );

        setRunning();
    }

    private KafkaProducer getJobDataProducer() {
        if(jobDataProducer == null) {
            this.jobDataProducer = beans.createProducer(config);
        }
        return jobDataProducer;
    }

    public <T> void send(TransportImpl context) {
        final JobDataImpl jobData = context.jobData();
        logger.trace("E: {} Sending jobData for {} id: {} state: {} step: {} stepCount: {}", beans.getEngine().getName(),
                jobData.jobSignature(), jobData.id(), jobData.state(), jobData.step(), jobData.stepCount());

        Pair<String, String> payload = beans.getJobTools().prepareKafkaKeyValue(context);

        futures.removeAll(futures.stream().filter(f -> f.isDone() || f.isCancelled()).collect(Collectors.toList()));

        futures.add(doSend(new ProducerRecord(beans.getContainer().getJobDataTopicName(), payload.getKey(), payload.getValue())));
        if(futures.size() > 100) {
            getJobDataProducer().flush();
        }

    }



    Future<?> doSend(ProducerRecord<String, String> toSend) {
        Future result;
        try {
            return getJobDataProducer().send(toSend, new Callback() {
                @Override
                public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                    if (exception == null) {
                        logger.info("Message with key: {}, value:sent {} successfully to partition {} woth offset {}",
                                toSend.key(), toSend.value(), metadata.partition(),metadata.offset());
                    } else {
                        logger.error("exception onCompletion", exception);
                    }
                }
            });
        } catch (IllegalStateException e) {
            logger.error("Trying to send record", e);
            jobDataProducer = null;
            // result = getJobDataProducer().send(toSend);
        }
        return null;
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
                futures.removeAll(futures.stream().filter(f -> f.isDone() || f.isCancelled()).collect(Collectors.toList()));
            } while (futures.size() > 0);
            getJobDataProducer().close();
        } catch (Exception e) {
            logger.error("Exception during shutdown", e);
        } finally {
            setRunning(false);
        }
    }
}

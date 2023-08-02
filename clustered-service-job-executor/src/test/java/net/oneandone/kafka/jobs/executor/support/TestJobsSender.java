package net.oneandone.kafka.jobs.executor.support;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.JobsSender;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class TestJobsSender extends JobsSender {

    Logger logger = LoggerFactory.getLogger("ApiTests");

    private final TestSenderData data;

    public TestJobsSender(Beans beans, TestSenderData data) {
        super(beans);
        this.data = data;

    }

    @Override
    public <T> void send(final TransportImpl context) {
        final JobDataImpl jobData = context.jobData();
        data.lastContexts.put(Pair.of(jobData.jobSignature(), jobData.id()), context);
        super.send(context);
    }

    @Override
    public void sendState(final JobDataImpl jobData, final ConsumerRecord r) {
        data.lastConsumerRecordsUsedForState.put(Pair.of(jobData.contextClass(), jobData.id()), r);
        data.stateCounts.get(jobData.state()).incrementAndGet();
        if (!data.stateSendingEngines.contains(beans.getEngine().getName())) {
            data.stateSendingEngines.add(beans.getEngine().getName());
            logger.error("new state sending engine: {}",beans.getEngine().getName());
        }
        data.jobStates.put(jobData.id(), beans.getEngine().getName() + "_" + jobData.state());
        super.sendState(jobData, r);
    }

}

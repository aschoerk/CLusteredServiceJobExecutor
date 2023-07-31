package net.oneandone.kafka.jobs.executor.support;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.JobsSender;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class TestJobsSender extends JobsSender {


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
        data.jobStates.put(jobData.id(), jobData.state());
        super.sendState(jobData, r);
    }

}

package net.oneandone.kafka.jobs.executor.support;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.BeansFactory;
import net.oneandone.kafka.jobs.beans.Sender;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class TestBeansFactory extends BeansFactory {

    TestSender testSender = null;

    @Override
    public Sender createSender(final Beans beans) {
        testSender = new TestSender(beans);
        return testSender;
    }

    @Override
    public BlockingDeque<TransportImpl> createQueue() {
        return new LinkedBlockingDeque<>(1000);
    }

    public TestSender getTestSender() {
        return testSender;
    }
}

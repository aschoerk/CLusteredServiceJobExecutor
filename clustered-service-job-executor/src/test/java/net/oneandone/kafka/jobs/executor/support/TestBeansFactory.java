package net.oneandone.kafka.jobs.executor.support;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.BeansFactory;
import net.oneandone.kafka.jobs.beans.ClusteredJobReviver;
import net.oneandone.kafka.jobs.beans.EngineImpl;
import net.oneandone.kafka.jobs.beans.JobsPendingHandler;
import net.oneandone.kafka.jobs.beans.JobsSender;
import net.oneandone.kafka.jobs.beans.Receiver;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class TestBeansFactory extends BeansFactory {

    static Logger logger = LoggerFactory.getLogger(TestBeansFactory.class);

    TestJobsSender testSender = null;

    TestSenderData testSenderData = new TestSenderData();

    @Override
    public JobsSender createSender(final Beans beans) {
        testSender = new TestJobsSender(beans, testSenderData);
        return testSender;
    }

    @Override
    public EngineImpl createEngine(final Beans beans) {
        AtomicInteger idCounter = new AtomicInteger();
        return new EngineImpl(beans) {
            @Override
            public String createId() {
                return super.getName() + "_" + idCounter.incrementAndGet();
            }
        };
    }

    static class TestClusteredJobsReviver extends ClusteredJobReviver {

        static AtomicBoolean entered = new AtomicBoolean(false);
        public TestClusteredJobsReviver(final Beans beans) {
            super(beans);
            logger.info("Reviver created: Engine: {}", super.beans.getEngine().getName());
        }

        @Override
        public void call() {
            logger.trace("Entering call: Thread: {} Engine: {}", Thread.currentThread().getName(), super.beans.getEngine().getName());
            if (!entered.compareAndSet(false, true)) {
                logger.error("Entering call: ClusteredJobsReviver running multiple times {} ", super.beans.getEngine().getName());
            }
            try {
                super.call();
            } finally {
                if (!entered.compareAndSet(true, false)) {
                    logger.error("Entering call: ClusteredJobsReviver running multiple times {} ", super.beans.getEngine().getName());
                }
                logger.trace("Leaving call: Thread: {} Engine: {}", Thread.currentThread().getName(), super.beans.getEngine().getName());
            }
        }
    }

    @Override
    public ClusteredJobReviver createReviver(final Beans beans) {

        return new TestClusteredJobsReviver(beans);
    }

    @Override
    public JobsPendingHandler createPendingHandler(final Beans beans) {
        return new JobsPendingHandler(beans) {
            @Override
            public void setShutDown() {
                super.setShutDown();
                super.sortedPending.stream().forEach(sp -> {
                    logger.info("E: {} rescued: {}", sp.jobData());
                });
            }
        };
    }

    @Override
    public Receiver createReceiver(final Beans beans) {
        return new Receiver(beans) {
            @Override
            protected void handleSingleRecord(final TransportImpl transport) {
                JobDataImpl jobData = transport.jobData();
                testSenderData.stateCounts.get(jobData.state()).incrementAndGet();
                if (!testSenderData.stateSendingEngines.contains(beans.getEngine().getName())) {
                    testSenderData.stateSendingEngines.add(beans.getEngine().getName());
                    logger.error("new state sending engine: {}",beans.getEngine().getName());
                }
                testSenderData.jobStates.put(jobData.id(), beans.getEngine().getName() + "_" + jobData.state());
                super.handleSingleRecord(transport);
            }
        };
    }

    public TestSenderData getTestSenderData() {
        return testSenderData;
    }
}

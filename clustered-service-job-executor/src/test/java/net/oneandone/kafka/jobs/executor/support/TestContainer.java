package net.oneandone.kafka.jobs.executor.support;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.Transaction;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScopedContext;

public class TestContainer implements Container {

    private final Clock clock;
    private String bootstrapServers;

    private CdbThreadScopedContext cdbThreadScopedContext;
    BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<>();


    ExecutorService executorService;

    public TestContainer(final String bootstrapServers,
                         CdbThreadScopedContext cdbThreadScopedContext,
                         Clock clock) {
        this.bootstrapServers = bootstrapServers;
        this.cdbThreadScopedContext = cdbThreadScopedContext;
        this.clock = clock;
        executorService = new ThreadPoolExecutor(20, 50, 10000,
                TimeUnit.MILLISECONDS, workQueue);
    }

    @Override
    public String getSyncTopicName() {
        return "SyncTopic";
    }

    @Override
    public String getJobDataTopicName() {
        return "JobDataTopic";
    }

    @Override
    public String getBootstrapServers() {
        return bootstrapServers;
    }

    @Override
    public Thread createThread(Runnable runnable) {
        return new Thread(runnable);
    }

    @Override
    public Future<?> submitInThread(Runnable runnable) {
        return executorService.submit(runnable);
    }

    @Override
    public void startThreadUsage() {
        cdbThreadScopedContext.activate();
    }

    @Override
    public void stopThreadUsage() {
        cdbThreadScopedContext.deactivate();
    }

    @Override
    public Clock getClock() {
        return clock;
    }

    @Override
    public Transaction getTransaction() {
        return new Transaction() {

        };
    }

    final AtomicLong ids = new AtomicLong();

    @Override
    public Supplier<String> getIdCreator() {
        return () -> getConfiguration().getNodeName() + "_" + ids.incrementAndGet();
    }

    Configuration configuration = new Configuration() {
        @Override
        public Duration getInitialWaitTimePhase1() {
            return Duration.ofSeconds(1);
        }

        @Override
        public Duration getInitialWaitTimePhase2() {
            return Duration.ofSeconds(1);
        }

        @Override
        public Duration getReviverPeriod() {
            return Duration.ofSeconds(5);
        }

        @Override
        public Duration getMaxDelayOfStateMessages() {
            return Duration.ofSeconds(3);
        }

        @Override
        public String getNodeName() {
            return "N" + this.hashCode() + "_";
        }
    };

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}

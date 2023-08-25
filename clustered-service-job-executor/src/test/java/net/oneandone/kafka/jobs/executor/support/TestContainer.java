package net.oneandone.kafka.jobs.executor.support;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.Transaction;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScopedContext;

public class TestContainer implements Container {

    private final Clock clock;
    private String bootstrapServers;

    private final CdbThreadScopedContext cdbThreadScopedContext;
    public BlockingQueue<Runnable> workerThreadQueue = new ArrayBlockingQueue<>(1000);
    public BlockingQueue<Runnable> shortRunningThreadQueue = new ArrayBlockingQueue<>(100);
    BlockingQueue<Runnable> longRunningWorkQueue = new LinkedBlockingQueue<>();


    ExecutorService workerThreadExecutorService;

    ExecutorService longRunningThreadExecutorService;

    ExecutorService shortRunningThreadExecutorService;
    private int threadPoolSize;

    public TestContainer(final String bootstrapServers,
                         CdbThreadScopedContext cdbThreadScopedContext,
                         Clock clock) {
        this.bootstrapServers = bootstrapServers;
        this.cdbThreadScopedContext = cdbThreadScopedContext;
        this.clock = clock;
        workerThreadExecutorService = new ThreadPoolExecutor(200, 200, 1,
                TimeUnit.MILLISECONDS, workerThreadQueue);
        shortRunningThreadExecutorService = new ThreadPoolExecutor(20, 500, 1,
                TimeUnit.MILLISECONDS, shortRunningThreadQueue);
        longRunningThreadExecutorService = new ThreadPoolExecutor(100, 300, 100,
                TimeUnit.MILLISECONDS, longRunningWorkQueue);

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
    public Future<?> submitInWorkerThread(final Runnable runnable) {
        return workerThreadExecutorService.submit(runnable);
    }

    @Override
    public Future<?> submitShortRunning(final Runnable runnable) {
        return shortRunningThreadExecutorService.submit(runnable);
    }

    @Override
    public Future<?> submitLongRunning(final Runnable runnable) {
        return longRunningThreadExecutorService.submit(runnable);
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
            return Duration.ofSeconds(30);
        }

        @Override
        public String getNodeName() {
            return "N" + this.hashCode() + "_";
        }

        @Override
        public int getMaxPollJobDataRecords() {
            return 20;
        }

        @Override
        public Duration getConsumerPollInterval() {
            return Duration.ofSeconds(60);
        }
    };

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void setWorkerThreadPoolSize(final int i) {
        this.threadPoolSize = i;
        workerThreadQueue = new LinkedBlockingQueue<>(i * 10);
        workerThreadExecutorService = new ThreadPoolExecutor(i, i * 2, 1,
                TimeUnit.MILLISECONDS, workerThreadQueue);
    }
}

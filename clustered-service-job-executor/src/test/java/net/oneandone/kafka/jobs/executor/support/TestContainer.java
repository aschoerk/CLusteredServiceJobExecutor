package net.oneandone.kafka.jobs.executor.support;

import java.time.Clock;

import jakarta.enterprise.context.ApplicationScoped;
import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.Transaction;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScopedContext;

public class TestContainer implements Container {

    private final Clock clock;
    private String bootstrapServers;

    private CdbThreadScopedContext cdbThreadScopedContext;

    public TestContainer(final String bootstrapServers,
                         CdbThreadScopedContext cdbThreadScopedContext,
                         Clock clock) {
        this.bootstrapServers = bootstrapServers;
        this.cdbThreadScopedContext = cdbThreadScopedContext;
        this.clock = clock;
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

    public void setBootstrapServers(final String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}

package net.oneandone.kafka.jobs.executor;

import net.oneandone.kafka.jobs.api.Executor;
import net.oneandone.kafka.jobs.api.Providers;

/**
 * @author aschoerk
 */
public class ProvidersImpl implements Providers  {
    @Override
    public Executor createExecutor() {
        return new ExecutorImpl();
    }
}

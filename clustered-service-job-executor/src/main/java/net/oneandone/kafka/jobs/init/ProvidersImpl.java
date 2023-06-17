package net.oneandone.kafka.jobs.init;

import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.Executor;
import net.oneandone.kafka.jobs.api.Providers;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.ExecutorImpl;

/**
 * @author aschoerk
 */
public class ProvidersImpl implements Providers  {
    @Override
    public Executor createExecutor(Container container) {
        Beans beans = new Beans(container);
        return new ExecutorImpl(beans);
    }
}

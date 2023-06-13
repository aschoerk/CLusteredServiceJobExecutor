package net.oneandone.kafka.jobs.api;

import java.util.ServiceLoader;

/**
 * @author aschoerk
 */
public interface Providers {
    Executor createExecutor();

    static Providers get() {
        ServiceLoader<Providers> loader = ServiceLoader.load(Providers.class);
        return loader.findFirst().get();
    }

}

package net.oneandone.kafka.jobs.api;

import java.util.ServiceLoader;

/**
 * @author aschoerk
 */
public interface Providers {
    Engine createEngine(Container container);

    Engine createTestEngine(Container container, Object beansFactory);

    static Providers get() {
        ServiceLoader<Providers> loader = ServiceLoader.load(Providers.class);
        return loader.findFirst().get();
    }

}

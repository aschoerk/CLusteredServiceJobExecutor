package net.oneandone.kafka.jobs.init;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.Providers;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.BeansFactory;

/**
 * @author aschoerk
 */
public class ProvidersImpl implements Providers {

    Map<String, Beans> nodes = new HashMap<>();

    @Override
    public synchronized Engine createEngine(Container container) {
        final String nodeName = container.getConfiguration().getNodeName();
        if (nodes.containsKey(nodeName)) {
            return nodes.get(nodeName).getEngine();
        } else {
            Beans beans = new Beans(container, new BeansFactory());
            nodes.put(nodeName, beans);
            return beans.getEngine();
        }
    }

    @Override
    public synchronized Engine createTestEngine(Container container, Object beansFactory) {
        final String nodeName = container.getConfiguration().getNodeName();
        if (nodes.containsKey(nodeName)) {
            return nodes.get(nodeName).getEngine();
        } else {
            Beans beans = new Beans(container, (BeansFactory) beansFactory);
            nodes.put(nodeName, beans);
            return beans.getEngine();
        }
    }
}

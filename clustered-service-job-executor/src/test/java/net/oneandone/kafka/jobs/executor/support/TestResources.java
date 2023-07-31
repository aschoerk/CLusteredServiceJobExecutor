package net.oneandone.kafka.jobs.executor.support;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import kafka.server.KafkaConfig;
import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScopedContext;


@ApplicationScoped
public class TestResources {

    @Inject
    CdbThreadScopedContext cdbThreadScopedContext;

    public TestBeansFactory getTestBeansFactory() {
        return testBeansFactory;
    }

    @Inject
    TestBeansFactory testBeansFactory;

    @Produces
    public Clock produceClock() {
        return Clock.systemUTC();
    }
    private TestCluster cluster;

    private TestContainer container;

    public void startKafka() throws Exception {
        Properties brokerConfig = new Properties();

        brokerConfig.setProperty(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:0");
        cluster = new TestCluster(1, brokerConfig);
        cluster.start();

        getContainer().setBootstrapServers(cluster.bootstrapServers());
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("retention.ms", Integer.toString(12 * 3600 * 1000));
        cluster.deleteTopicsAndWait(10000, getContainer().getJobDataTopicName(), getContainer().getJobStateTopicName(), getContainer().getSyncTopicName());
        cluster.createTopic(getContainer().getJobDataTopicName(), 2, 1, topicConfig);
        cluster.createTopic(getContainer().getJobStateTopicName(), 2, 1, topicConfig);
        cluster.createTopic(getContainer().getSyncTopicName(), 1, 1, topicConfig);
    }



    public void stopKafkaCluster() {
        if (cluster != null) {
            cluster.shutdown();
        }
        cluster = null;
    }

    public TestCluster getCluster() {
        return cluster;
    }

    public static class TestCluster extends EmbeddedKafkaCluster {
        public TestCluster(int numBrokers, Properties config) {
            super(numBrokers, config);
        }

        public void shutdown() {
            stop();
        }
    }

    public TestContainer getContainer() {
        if (this.container == null) {
            this.container = new TestContainer("dummyNodes", cdbThreadScopedContext, produceClock());
        }
        return this.container;
    }

}

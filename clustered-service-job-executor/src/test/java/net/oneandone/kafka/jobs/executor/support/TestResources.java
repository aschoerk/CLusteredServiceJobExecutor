package net.oneandone.kafka.jobs.executor.support;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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

        brokerConfig.setProperty(KafkaConfig.ListenersProp(), "PLAINTEXT://localhost:9092");
        // cluster = new TestCluster(1, brokerConfig);
        // cluster.start();
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put("retention.ms", Integer.toString(12 * 3600 * 1000));

        if (cluster != null) {
            getContainer().setBootstrapServers(cluster.bootstrapServers());
            cluster.deleteTopicsAndWait(10000, getContainer().getJobDataTopicName(), getContainer().getSyncTopicName());
            cluster.createTopic(getContainer().getJobDataTopicName(), 2, 1, topicConfig);
            cluster.createTopic(getContainer().getSyncTopicName(), 1, 1, topicConfig);
        } else {
            getContainer().setBootstrapServers("localhost:9092");
            Properties props = new Properties();
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            try (AdminClient adminClient = AdminClient.create(props)) {
                adminClient.deleteTopics(Arrays.asList(getContainer().getJobDataTopicName(), getContainer().getSyncTopicName()));
                adminClient.createTopics(
                        Arrays.asList(new NewTopic(getContainer().getJobDataTopicName(), 2, (short)1),
                        new NewTopic(getContainer().getSyncTopicName(), 1, (short)1)) );
            }

        }

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

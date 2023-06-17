package net.oneandone.kafka.jobs.executor;


import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.SutClasses;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;
import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.api.Executor;
import net.oneandone.kafka.jobs.api.Providers;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScopedExtension;
import net.oneandone.kafka.jobs.executor.jobexamples.CDITestJob;
import net.oneandone.kafka.jobs.executor.jobexamples.TestContext;
import net.oneandone.kafka.jobs.executor.jobexamples.TestJob;
import net.oneandone.kafka.jobs.executor.support.TestContainer;
import net.oneandone.kafka.jobs.executor.support.TestResources;

@ExtendWith(IocJUnit5Extension.class)
@SutClasses({CdbThreadScopedExtension.class})
@TestClasses({TestResources.class, TestContainer.class})
public class ApiTests {

    public static Logger logger = LoggerFactory.getLogger("ApiTests");

    @Inject
    TestResources testResources;

    @BeforeEach
    public void initTests() throws Exception {
        testResources.startKafka();
    }

    @AfterEach
    public void exitTests() {
        testResources.stopKafkaCluster();
    }

    @Test
    void test() throws InterruptedException {
        Executor executor = Providers.get().createExecutor(testResources.getContainer());
        TestJob jobTemplate = new TestJob();
        executor.register(jobTemplate, TestContext.class);
        Context<TestContext> context = executor.create(jobTemplate, new TestContext());
        while (context.context().getI() < 1) {
            Thread.sleep(200);
        }
    }

    @Inject
    CDITestJob cdiTestJob;

    @Test
    void cdiTest() throws InterruptedException {
        Executor executor = Providers.get().createExecutor(testResources.getContainer());
        executor.register(cdiTestJob, TestContext.class);
        List<Context<TestContext>> contexts = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            contexts.add(executor.create(cdiTestJob, new TestContext()));
        }
        while (contexts.stream().anyMatch(c -> c.context().getI() < 2)) {
            Thread.sleep(200);
        }
    }
}

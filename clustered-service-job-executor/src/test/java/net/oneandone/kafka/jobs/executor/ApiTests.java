package net.oneandone.kafka.jobs.executor;


import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.SutClasses;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Providers;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.beans.LocalRemoteExecutor;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScopedExtension;
import net.oneandone.kafka.jobs.executor.jobexamples.CDITestJob;
import net.oneandone.kafka.jobs.executor.jobexamples.TestContext;
import net.oneandone.kafka.jobs.executor.jobexamples.TestJob;
import net.oneandone.kafka.jobs.executor.support.TestBeansFactory;
import net.oneandone.kafka.jobs.executor.support.TestContainer;
import net.oneandone.kafka.jobs.executor.support.TestResources;
import net.oneandone.kafka.jobs.executor.support.TestSender;
import net.oneandone.kafka.jobs.implementations.JobImpl;

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
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        TestJob jobTemplate = new TestJob();
        engine.register(jobTemplate, TestContext.class);
        Transport transport = engine.create(jobTemplate, new TestContext());
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSender().lastContexts.entrySet();
        while (contexts.size() == 0 || ((TestContext)contexts.iterator().next().getValue().getContext(TestContext.class)).getI() < 1) {
            Thread.sleep(200);
        }
    }


    @Test
    void jobsTest() {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        TestJob jobTemplate = new TestJob();
    }

    @Inject
    CDITestJob cdiTestJob;

    RemoteExecutor createRemoteTestExecutor(Job job) {
        Beans beans = Mockito.mock(Beans.class);
        HashMap<String, Job> tmpMap = new HashMap<>();
        tmpMap.put(job.signature(), new JobImpl(job, TestContext.class));
        Mockito.doReturn(tmpMap).when(beans).getJobs();
        return new LocalRemoteExecutor(beans);
    }

    @ParameterizedTest
    @CsvSource({
            "true,1,1,2,0,0,0",
            "false,2,2,4,0,0,0",
            "false,3,3,7,1,0,0",
            "false,4,4,9,1,0,0",
            "false,5,5,12,2,0,0",
            "false,10,10,24,4,0,0",
            // 200 / 5 = 40, 40 / 5 = 8, 8 / 5 = 1 -- 49 times delayed
            "true,100,100,249,49,0,0",
            "false,100,100,249,49,0,0"
    }
    )
    void cdiTest(boolean doremote, int loops, int expectedDone, int expectedRunning, int expectedDelayed, int expectedSuspended, int expectedError) throws InterruptedException {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        engine.register(cdiTestJob, TestContext.class);
        if (doremote) {
            engine.register(createRemoteTestExecutor(cdiTestJob));
        }
        for (int i = 0; i < loops; i++) {
            engine.create(cdiTestJob, new TestContext());
        }
        waitForTest(testBeansFactory);
        checkTests(expectedDone, expectedRunning, expectedDelayed, expectedSuspended, expectedError, testBeansFactory);

        engine.stop();
    }


    @ParameterizedTest
    @ValueSource(ints = {10,1,2,4,6,10})
    void reviverTest(int revivals) throws InterruptedException {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        engine.register(cdiTestJob, TestContext.class);
        for (int i = 0; i < 500; i++) {
            engine.create(cdiTestJob, new TestContext());
        }
        for (int i = 0; i < revivals; i++) {
            Thread.sleep(2000);
            TestSender currentTestsender = testBeansFactory.getTestSender();
            engine.stop();
            engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            engine.register(cdiTestJob, TestContext.class);
            engine.create(cdiTestJob, new TestContext());
            TestSender newTestsender = testBeansFactory.getTestSender();
            newTestsender.addStates(currentTestsender);
        }
        waitForTest(testBeansFactory);
    }




    private void waitForTest(final TestBeansFactory testBeansFactory) throws InterruptedException {
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSender().lastContexts.entrySet();
        Set<Map.Entry<String, State>> states = testBeansFactory.getTestSender().jobStates.entrySet();
        logger.info("Waiting for {} jobs to complete", states.size());
        while (contexts.stream().anyMatch(c -> c.getValue().jobData().state() != DONE && c.getValue().jobData().state() != ERROR)
        || states.stream().anyMatch(c -> !(c.getValue().equals(DONE) || c.getValue().equals(ERROR)))) {
            Thread.sleep(200);
            logger.info("running: {} delayed: {} done: {} error: {}",
                    contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING).count(),
                    contexts.stream().filter(c -> c.getValue().jobData().state() == DELAYED).count(),
                    contexts.stream().filter(c -> c.getValue().jobData().state() == DONE).count(),
                    contexts.stream().filter(c -> c.getValue().jobData().state() == ERROR).count()
            );
        }
    }

    private static void checkTests(final int expectedDone, final int expectedRunning, final int expectedDelayed, final int expectedSuspended, final int expectedError, final TestBeansFactory testBeansFactory) {
        Map<State, AtomicInteger> stateCounts = testBeansFactory.getTestSender().stateCounts;
        Assertions.assertEquals(expectedDone, stateCounts.get(DONE).get());
        Assertions.assertEquals(expectedRunning, stateCounts.get(State.RUNNING).get());
        Assertions.assertEquals(expectedDelayed, stateCounts.get(State.DELAYED).get());
        Assertions.assertEquals(expectedSuspended, stateCounts.get(State.SUSPENDED).get());
        Assertions.assertEquals(expectedError, stateCounts.get(ERROR).get());
    }
}

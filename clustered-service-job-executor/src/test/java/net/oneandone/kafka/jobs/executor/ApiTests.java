package net.oneandone.kafka.jobs.executor;


import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oneandone.iocunit.IocJUnit5Extension;
import com.oneandone.iocunit.analyzer.annotations.SutClasses;
import com.oneandone.iocunit.analyzer.annotations.TestClasses;

import jakarta.inject.Inject;
import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Providers;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.api.Transport;
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
import net.oneandone.kafka.jobs.implementations.JobImpl;

@ExtendWith(IocJUnit5Extension.class)
@SutClasses({CdbThreadScopedExtension.class})
@TestClasses({TestResources.class, TestContainer.class})
public class ApiTests {

    public static Logger logger = LoggerFactory.getLogger("ApiTests");

    @Inject
    TestResources testResources;
    @Inject
    CDITestJob cdiTestJob;

    private static void checkTests(final int expectedDone, final int expectedRunning, final int expectedDelayed, final int expectedSuspended, final int expectedError, final TestBeansFactory testBeansFactory) {
        Map<State, AtomicInteger> stateCounts = testBeansFactory.getTestSenderData().stateCounts;
        Assertions.assertEquals(expectedDone, stateCounts.get(DONE).get());
        Assertions.assertEquals(expectedRunning, stateCounts.get(State.RUNNING).get());
        Assertions.assertEquals(expectedDelayed, stateCounts.get(State.DELAYED).get());
        Assertions.assertEquals(expectedSuspended, stateCounts.get(State.SUSPENDED).get());
        Assertions.assertEquals(expectedError, stateCounts.get(ERROR).get());
    }

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
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        while ((contexts.size() == 0) || (contexts.iterator().next().getValue().getContext(TestContext.class).getI() < 1)) {
            Thread.sleep(200);
        }
    }

    @Test
    void simpleGroupTest() throws InterruptedException {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        engine.register(cdiTestJob, TestContext.class);
        Transport transport = engine.create(cdiTestJob, "group", new TestContext("group"));
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        while ((contexts.size() == 0) || (contexts.iterator().next().getValue().getContext(TestContext.class).getI() < 1)) {
            Thread.sleep(200);
        }
    }

    // @Test
    void testStopKafka() throws Exception {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        TestJob jobTemplate = new TestJob();
        engine.register(jobTemplate, TestContext.class);
        engine.create(jobTemplate, new TestContext());
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        while ((contexts.size() == 0) || (contexts.iterator().next().getValue().getContext(TestContext.class).getI() < 1)) {
            Thread.sleep(200);
        }

        testResources.getCluster().shutdown();
        testResources.stopKafkaCluster();
        testBeansFactory.getTestSenderData().lastContexts.clear();

        testResources.startKafka();
        engine.create(jobTemplate, new TestContext());
        contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        while ((contexts.size() == 0) || (contexts.iterator().next().getValue().getContext(TestContext.class).getI() < 1)) {
            Thread.sleep(200);
        }

    }

    @Test
    void jobsTest() {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        TestJob jobTemplate = new TestJob();
    }

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
        if(doremote) {
            engine.register(createRemoteTestExecutor(cdiTestJob));
        }
        for (int i = 0; i < loops; i++) {
            engine.create(cdiTestJob, new TestContext());
        }
        waitForTest(testBeansFactory, null, loops, null);
        checkTests(expectedDone, expectedRunning, expectedDelayed, expectedSuspended, expectedError, testBeansFactory);

        engine.stop();
    }

    @ParameterizedTest
    @CsvSource({
            "100,1,5000,10000",
            "100,1,3000,1",  // one engine a few revivals after creation
            "100,1,300000,1",  // one engine no revival
            "100,2,300000,1",  // 2 engines no revival
            "100,10,300000,1",  // 10 engines no revival
            "100,1,3000,1",  // one engine a few revivals after creation
            "100,1,5000,10000",
            "100,2,5000,10000",
            "100,10,5000,10000",
            "10000,2,3000,10000",
            "10000,5,3000,10000",
            "10000,2,2000,10000",
            "10000,5,5000,10000",
    })
    void reviverTest(int jobnumber, int engineNumber, int revivalAfter, int creationTime) throws InterruptedException {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine[] engines = new Engine[engineNumber];
        for (int i = 0; i < engineNumber; i++) {
            engines[i] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            engines[i].register(cdiTestJob, TestContext.class);
        }
        final Instant[] start = {Instant.now()};
        final int[] currentEngine = {0};
        final int[] createdCount = {0};
        for (int i = 0; i < jobnumber; i++) {
            engines[currentEngine[0]].create(cdiTestJob, new TestContext());
            handleRevival(testBeansFactory, engines, start, currentEngine[0], revivalAfter, createdCount);
            Thread.sleep(creationTime / jobnumber);
            currentEngine[0] = (currentEngine[0] + 1) % engineNumber;
        }
        waitForTest(testBeansFactory, () -> {
            handleRevival(testBeansFactory, engines, start, currentEngine[0], revivalAfter, createdCount);
            currentEngine[0] = (currentEngine[0] + 1) % engineNumber;
        }, jobnumber, createdCount);
        Arrays.stream(engines).forEach(e -> e.stop());
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Assertions.assertEquals(jobnumber + createdCount[0], contexts.stream().filter(c -> c.getValue().jobData().state() == DONE).count());
        Assertions.assertEquals(jobnumber + createdCount[0], testBeansFactory.getTestSenderData().stateCounts.get(DONE).get());
        Assertions.assertEquals(0, contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING).count());
        Assertions.assertEquals(0, contexts.stream().filter(c -> c.getValue().jobData().state() == ERROR).count());
    }


    private void handleRevival(final TestBeansFactory testBeansFactory,
                               final Engine[] engines, Instant[] start, final int currentEngine, int revivalAfter,
                               final int[] createdCount) {
        if(Instant.now().isAfter(start[0].plus(Duration.ofMillis(revivalAfter)))) {
            engines[currentEngine].stop();
            engines[currentEngine] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            engines[currentEngine].register(cdiTestJob, TestContext.class);
            engines[currentEngine].create(cdiTestJob, new TestContext());
            createdCount[0]++;
            start[0] = Instant.now();
        }
    }

    private void waitForTest(final TestBeansFactory testBeansFactory, Runnable doInLoop, int createdJobs, final int[] createdCount) throws InterruptedException {
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Set<Map.Entry<String, State>> states = testBeansFactory.getTestSenderData().jobStates.entrySet();
        logger.info("Waiting for {} jobs to complete", states.size());
        while (contexts.stream().anyMatch(c -> (c.getValue().jobData().state() != DONE) && (c.getValue().jobData().state() != ERROR))
               || states.stream().anyMatch(c -> !(c.getValue() == DONE || c.getValue() == ERROR))) {
            Thread.sleep(200);
            if(doInLoop != null) {
                doInLoop.run();
            }
            final long running = contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING).count();
            final long delayed = contexts.stream().filter(c -> c.getValue().jobData().state() == DELAYED).count();
            final long done = contexts.stream().filter(c -> c.getValue().jobData().state() == DONE).count();
            final long errors = contexts.stream().filter(c -> c.getValue().jobData().state() == ERROR).count();
            final long elapsedRunning = contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING)
                    .filter(c -> c.getValue().jobData().date().isBefore(Instant.now())).count();
            final long elapsedDelayed = contexts.stream().filter(c -> c.getValue().jobData().state() == DELAYED)
                    .filter(c -> c.getValue().jobData().date().isBefore(Instant.now())).count();

            final long revivableRunning = contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING)
                    .filter(c -> c.getValue().jobData().date().plus(testResources.getContainer().getConfiguration().getMaxDelayOfStateMessages()).isBefore(Instant.now())).count();
            final long revivableDelayed = contexts.stream().filter(c -> c.getValue().jobData().state() == DELAYED)
                    .filter(c -> c.getValue().jobData().date().plus(testResources.getContainer().getConfiguration().getMaxDelayOfStateMessages()).isBefore(Instant.now())).count();
            logger.info("running: {} delayed: {} done: {} error: {} elapsedRunning: {} elapsedDelayed: {} revivableRunning: {} revivableDelayed: {}",
                    running, delayed, done, errors, elapsedRunning, elapsedDelayed, revivableRunning, revivableDelayed
            );
            if((running + delayed + done + errors) > (createdJobs + ((createdCount != null) ? createdCount[0] : 0))) {
                logger.error("Too many jobs");
                testBeansFactory.getTestSenderData().lastContexts.keySet().stream().sorted()
                        .forEach(k -> logger.error("Context having key: {}", k));
            }
        }
    }
}

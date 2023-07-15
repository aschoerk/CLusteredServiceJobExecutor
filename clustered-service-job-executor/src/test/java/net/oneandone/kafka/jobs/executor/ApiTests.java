package net.oneandone.kafka.jobs.executor;


import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.GROUP;
import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import net.oneandone.kafka.jobs.executor.jobexamples.CDITestStep;
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

    private static void checkTests(final int expectedDone, final int expectedRunning, final int expectedDelayed,
                                   final int expectedSuspended, final int expectedError, final int expectedGroup,
                                   final TestBeansFactory testBeansFactory) {
        Map<State, AtomicInteger> stateCounts = testBeansFactory.getTestSenderData().stateCounts;
        Assertions.assertEquals(expectedDone, stateCounts.get(DONE).get(),"done");
        Assertions.assertEquals(expectedRunning, stateCounts.get(State.RUNNING).get(),"running");
        Assertions.assertEquals(expectedDelayed, stateCounts.get(State.DELAYED).get(),"delayed");
        Assertions.assertEquals(expectedSuspended, stateCounts.get(State.SUSPENDED).get(),"suspended");

        Assertions.assertEquals(expectedError, stateCounts.get(ERROR).get(),"errors");
        Assertions.assertEquals(expectedGroup, stateCounts.get(State.GROUP).get(),"groups");
        Assertions.assertEquals(0, CDITestStep.collisionsDetected.get(),"collisions in threadscoped step");
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
        tmpMap.put(job.signature(), new JobImpl(job, TestContext.class, beans));
        Mockito.doReturn(tmpMap).when(beans).getJobs();
        return new LocalRemoteExecutor(beans);
    }

    @ParameterizedTest
    @CsvSource({
            "true,1,1,2,0,0,0,1,1",
            "false,2,2,4,0,0,0,2,2",
            "false,3,3,7,1,0,0,3,3",
            "false,4,4,9,1,0,0,4,4",
            "false,5,5,12,2,0,0,2,5",
            "true,1,1,2,0,0,0,0,0",
            "false,2,2,4,0,0,0,0,0",
            "false,3,3,7,1,0,0,0,0",
            "false,4,4,9,1,0,0,0,0",
            "false,5,5,12,2,0,0,0,0",
            "false,10,10,24,4,0,0,0,0",
            // 200 / 5 = 40, 40 / 5 = 8, 8 / 5 = 1 -- 49 times delayed
            "true,100,100,249,49,0,0,0,0",
            "false,100,100,249,49,0,0,0,0",
            "false,10,10,24,4,0,0,7,10",
            "false,10,10,24,4,0,0,4,10",
            // 200 / 5 = 40, 40 / 5 = 8, 8 / 5 = 1 -- 49 times delayed
            "true,100,100,249,49,0,0,3,100",
            "true,100,100,249,49,0,0,30,100",
            "true,100,100,249,49,0,0,10,100",
            "false,100,100,249,49,0,0,20,100",
            "false,100,100,249,49,0,0,5,100"    }
    )
    void cdiTest(boolean doremote, int loops, int expectedDone, int expectedRunning, int expectedDelayed,
                 int expectedSuspended, int expectedError, int groupNo, int expectedGroup) throws InterruptedException {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        String[] groups = generateGroupNames(groupNo);
        engine.register(cdiTestJob, TestContext.class);
        if(doremote) {
            engine.register(createRemoteTestExecutor(cdiTestJob));
        }
        for (int i = 0; i < loops; i++) {
            Thread.sleep(10);
            if (groupNo > 0) {
                engine.create(cdiTestJob, groups[i % groupNo], new TestContext(groups[i % groupNo]));
            } else {
                engine.create(cdiTestJob, new TestContext());
            }
        }
        waitForTest(testBeansFactory, null, loops, null);
        checkTests(expectedDone, expectedRunning, expectedDelayed, expectedSuspended, expectedError, expectedGroup, testBeansFactory);

        engine.stop();
    }

    @Test
    void submitTest() throws InterruptedException {
        final int[] ints = new int[1];

        Future<?> future = testResources.getContainer().submitInThread(
                () -> ints[0]++
        );
        while (!future.isDone()) {
            Thread.sleep(100);
        }
        Assertions.assertEquals(1, ints[0]);
    }

    private static String[] generateGroupNames(final int groupNo) {
        String[] groups = new String[groupNo];
        for (int i = 0; i < groupNo; i++) {
            groups[i] = "group" + i;
        }
        return groups;
    }

    @ParameterizedTest
    @CsvSource({
            "100,1,2,5000000,1000,1",
            "20,1,2,5000,10000,1",
            "100,1,10,5000,10000,10",
            "100,1,10,5000,10000,100",
            "100,1,2,5000,10000,100",
            "100,1,2,5000,10000,10",
            "10000,1,5,30000,10000,1000",
            "10000,1,5,3000,10000,0",
            "100,1,2,3000,1,0",  // one engine a few revivals after creation
            "100,1,2,300000,1,0",  // one engine no revival
            "100,1,2,300000,1,0",  // 2 engines no revival
            "100,1,1,5000,10000,0",
            "100,1,10,300000,1,0",  // 10 engines no revival
            "100,1,1,3000,1,0",  // one engine a few revivals after creation
            "100,1,1,5000,10000,0",
            "100,1,2,5000,10000,0",
            "100,1,10,5000,10000,0",
            "10000,1,2,3000,10000,0",
            "10000,1,5,3000,10000,0",
            "10000,1,2,2000,10000,0",
            "10000,1,5,5000,10000,0",
    })
    void reviverTest(int jobnumber, int fixedEngineNumber, int engineNumber, int revivalAfter, int creationTime, int groupNo) throws InterruptedException {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine[] fixedEngines = new Engine[fixedEngineNumber];
        for (int i = 0; i < fixedEngineNumber; i++) {
            fixedEngines[i] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            fixedEngines[i].register(cdiTestJob, TestContext.class);
        }
        Engine[] engines = new Engine[engineNumber];
        for (int i = 0; i < engineNumber; i++) {
            engines[i] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            engines[i].register(cdiTestJob, TestContext.class);
        }
        String[] groupIds = generateGroupNames(groupNo);
        final Instant[] start = {Instant.now()};
        final int[] currentEngine = {0};
        final int[] createdCount = {0};
        for (int i = 0; i < jobnumber; i++) {
            if (groupIds.length == 0) {
                engines[currentEngine[0]].create(cdiTestJob, new TestContext());
            } else {
                final String groupId = groupIds[i % groupNo];
                engines[currentEngine[0]].create(cdiTestJob, groupId,new TestContext(groupId));
            }
            handleRevival(testBeansFactory, engines, start, currentEngine[0], revivalAfter, createdCount, groupIds, true);
            Thread.sleep(creationTime / jobnumber);
            currentEngine[0] = (currentEngine[0] + 1) % engineNumber;
        }
        waitForTest(testBeansFactory, () -> {
            handleRevival(testBeansFactory, engines, start, currentEngine[0], revivalAfter, createdCount, groupIds, false);
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
                               final int[] createdCount, String[] groupIds, boolean createJobs) {
        if(Instant.now().isAfter(start[0].plus(Duration.ofMillis(revivalAfter)))) {
            engines[currentEngine].stop();
            engines[currentEngine] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            engines[currentEngine].register(cdiTestJob, TestContext.class);
            if (createJobs) {
                if(groupIds.length != 0) {
                    final String groupId = groupIds[createdCount[0] % groupIds.length];
                    engines[currentEngine].create(cdiTestJob, groupId, new TestContext(groupId));
                }
                else {
                    engines[currentEngine].create(cdiTestJob, new TestContext());
                }
                createdCount[0]++;
            }
            start[0] = Instant.now();
        }
    }

    private void waitForTest(final TestBeansFactory testBeansFactory, Runnable doInLoop, int createdJobs, final int[] createdCount) throws InterruptedException {
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Set<Map.Entry<String, State>> states = testBeansFactory.getTestSenderData().jobStates.entrySet();
        logger.info("Waiting for {} jobs to complete", states.size());
        Instant outputTimestamp = Instant.now();
        while (contexts.stream().anyMatch(c -> (c.getValue().jobData().state() != DONE) && (c.getValue().jobData().state() != ERROR))
               || states.stream().anyMatch(c -> !(c.getValue() == DONE || c.getValue() == ERROR))) {
            Thread.sleep(200);
            if(doInLoop != null) {
                doInLoop.run();
            }
            if (logger.isInfoEnabled() && Instant.now().isAfter(outputTimestamp.plus(Duration.ofSeconds(3)))) {
                outputTimestamp = Instant.now();
                final long running = contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING).count();
                final long ingroup = contexts.stream().filter(c -> c.getValue().jobData().state() == GROUP).count();
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

                Map<State, AtomicInteger> stateCounts = testBeansFactory.getTestSenderData().stateCounts;
                logger.info("rn: {} dlyd: {} dn: {} er: {} grp: {} rnSnt: {} dlydSnt: {} grpSnt: {} elpsdR: {} elpsedDd: {} rvivR: {} rvivD: {} coll: {}",
                        running, delayed, done, errors, ingroup, stateCounts.get(State.RUNNING).get(), stateCounts.get(State.DELAYED).get(),
                        stateCounts.get(State.GROUP).get(),
                        elapsedRunning, elapsedDelayed, revivableRunning, revivableDelayed, CDITestStep.collisionsDetected
                );
                if (ingroup > 0) {
                    logger.info("GroupRecords: {}", contexts.stream().filter(c -> c.getValue().jobData().state() == GROUP)
                            .map(c -> c.getValue().jobData().groupId() + "|" + c.getValue().jobData().id())
                            .collect(Collectors.joining(",")));
                }
                if((running + delayed + done + errors) > (createdJobs + ((createdCount != null) ? createdCount[0] : 0))) {
                    logger.error("Too many jobs");
                    testBeansFactory.getTestSenderData().lastContexts.keySet().stream().sorted()
                            .forEach(k -> logger.error("Context having key: {}", k));
                }
            }

        }
    }
}

package net.oneandone.kafka.jobs.executor;


import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.GROUP;
import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import net.oneandone.kafka.jobs.beans.EngineImpl;
import net.oneandone.kafka.jobs.beans.LocalRemoteExecutor;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
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
        Assertions.assertEquals(expectedDone, stateCounts.get(DONE).get(), "done");
        Assertions.assertEquals(expectedRunning, stateCounts.get(State.RUNNING).get(), "running");
        Assertions.assertEquals(expectedDelayed, stateCounts.get(State.DELAYED).get(), "delayed");
        Assertions.assertEquals(expectedSuspended, stateCounts.get(State.SUSPENDED).get(), "suspended");

        Assertions.assertEquals(expectedError, stateCounts.get(ERROR).get(), "errors");
        Assertions.assertEquals(expectedGroup, stateCounts.get(State.GROUP).get(), "groups");
        Assertions.assertEquals(0, CDITestStep.collisionsDetected.get(), "collisions in threadscoped step");
    }

    private static String[] generateGroupNames(final int groupNo) {
        String[] groups = new String[groupNo];
        for (int i = 0; i < groupNo; i++) {
            groups[i] = "group" + i;
        }
        return groups;
    }

    @BeforeEach
    public void initTests() throws Exception {
        CDITestStep.initStatics();
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
        TestJob jobTemplate = new TestJob(engine);
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
        TestJob jobTemplate = new TestJob(engine);
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
        while ((contexts.isEmpty()) || (contexts.iterator().next().getValue().getContext(TestContext.class).getI() < 1)) {
            Thread.sleep(200);
        }

    }

    @Test
    void jobsTest() {
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        TestJob jobTemplate = new TestJob(engine);
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
            "false,10000,10000,20000,0,0,0,0,0,0",
            "false,1,1,2,0,0,0,0,0,0",
            "false,2,2,4,0,0,0,0,0,0",

            "false,1000,1000,2000,0,0,0,0,0,0",
            "true,1,1,2,0,0,0,0,0,2",
            "true,1,1,2,0,0,0,1,1,4",
            "false,2,2,4,0,0,0,2,2,4",
            "false,3,3,7,1,0,0,3,3,4",
            "false,4,4,9,1,0,0,4,4,4",
            "false,5,5,12,2,0,0,2,5,4",
            "true,1,1,2,0,0,0,0,0,4",
            "false,2,2,4,0,0,0,0,0,4",
            "false,3,3,7,1,0,0,0,0,4",
            "false,4,4,9,1,0,0,0,0,4",
            "false,5,5,12,2,0,0,0,0,4",
            "false,10,10,24,4,0,0,0,0,4",
            // 200 / 5 = 40, 40 / 5 = 8, 8 / 5 = 1 -- 49 times delayed
            "true,100,100,249,49,0,0,0,0,4",
            "false,100,100,249,49,0,0,0,0,4",
            "false,10,10,24,4,0,0,7,10,4",
            "false,10,10,24,4,0,0,4,10,4",
            // 200 / 5 = 40, 40 / 5 = 8, 8 / 5 = 1 -- 49 times delayed
            "true,100,100,249,49,0,0,3,100,4",
            "true,100,100,249,49,0,0,30,100,4",
            "true,100,100,249,49,0,0,10,100,4",
            "false,100,100,249,49,0,0,20,100,4",
            "false,100,100,249,49,0,0,5,100,4"}
    )
    void cdiTest(boolean doremote, int loops, int expectedDone, int expectedRunning, int expectedDelayed,
                 int expectedSuspended, int expectedError, int groupNo, int expectedGroup, int successesInSequence) throws InterruptedException {
        CDITestStep.successesInSequence = successesInSequence;
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        String[] groups = generateGroupNames(groupNo);
        engine.register(cdiTestJob, TestContext.class);
        if(doremote) {
            engine.register(createRemoteTestExecutor(cdiTestJob));
        }
        for (int i = 0; i < loops; i++) {
            Thread.sleep(10);
            if(groupNo > 0) {
                engine.create(cdiTestJob, groups[i % groupNo], new TestContext(groups[i % groupNo]));
            }
            else {
                engine.create(cdiTestJob, new TestContext());
            }
        }
        waitForTest(testBeansFactory, null, loops, null, Collections.emptySet());
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

    @ParameterizedTest
    @CsvSource({
            "500000,0,10,1000,100,0,50",

            /*"10000,1,10,10000,10000,0,2",
            "10000,1,2,1000,10000,0,3000",
            "10000,1,2,1000,10000,0,0",
            "10000,1,2,3000,10000,0,4",
            "100,1,2,3000,1,0,4",  // one engine a few revivals after creation
            "100,1,2,300000,1,0,4",  // one engine no revival
            "100,1,2,300000,1,0,4",  // 2 engines no revival
            "100,1,1,5000,10000,0,4",
            "100,1,10,300000,1,0,4",  // 10 engines no revival
            "100,1,1,3000,1,0,4",  // one engine a few revivals after creation
            "100,1,1,5000,10000,0,4",
            "100,1,2,5000,10000,0,4",
            "100,1,10,5000,10000,0,4",
            "10000,1,5,3000,10000,0,4",
            "10000,1,2,2000,10000,0,4",
            "10000,1,5,5000,10000,0,4",
            "10000,1,5,30000,10000,5000,4",
            "10000,1,5,30000,10000,1000,4",
            "10000,1,5,3000,10000,0,4",
            "100,1,2,5000000,1000,1,4",
            "20,1,2,5000,10000,1,4",
            "100,1,10,5000,10000,10,4",
            "100,1,10,5000,10000,100,4",
            "100,1,2,5000,10000,100,4",
            "100,1,2,5000,10000,10,4",*/

    })
    void reviverTest(int jobnumber, int fixedEngineNumber, int engineNumber, int revivalAfter, int creationTime,
                     int groupNo, int successesInSequence) throws InterruptedException {
        testResources.getContainer().setThreadPoolSize((fixedEngineNumber + engineNumber) * 20);
        CDITestStep.successesInSequence = successesInSequence;
        Set<String> revivedEngines = new HashSet<>();
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine[] fixedEngines = new Engine[fixedEngineNumber];
        for (int i = 0; i < fixedEngineNumber; i++) {
            fixedEngines[i] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            fixedEngines[i].register(cdiTestJob, TestContext.class);
        }
        List<Engine> engines = new ArrayList<>();
        for (int i = 0; i < engineNumber; i++) {
            engines.add(Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory));
            engines.get(i).register(cdiTestJob, TestContext.class);
        }
        Collections.shuffle(engines);
        String[] groupIds = generateGroupNames(groupNo);
        final Instant testStart = Instant.now();
        final Instant[] start = {Instant.now()};
        final int[] currentEngine = {0};
        final int[] createdCount = {0};
        for (int i = 0; i < jobnumber; i++) {
            final Engine engine = engines.get(currentEngine[0]);
            if(groupIds.length == 0) {
                engine.create(cdiTestJob, new TestContext());
            }
            else {
                final String groupId = groupIds[i % groupNo];
                engine.create(cdiTestJob, groupId, new TestContext(groupId));
            }
            if(Instant.now().isAfter(start[0].plus(Duration.ofMillis(revivalAfter)))) {
                handleRevival(testBeansFactory, engines, currentEngine[0], createdCount, groupIds,
                        false, revivedEngines);
                incEngine(engines, currentEngine);
                start[0] = Instant.now();
            }
            if(i % 1000 == 99) {
                Thread.sleep(creationTime / (jobnumber / 1000));
                logger.info("Sent {} records", i);
            }
        }
        waitForTest(testBeansFactory, () -> {
            if(Instant.now().isAfter(start[0].plus(Duration.ofMillis(revivalAfter)))) {
                handleRevival(testBeansFactory, engines, currentEngine[0], createdCount, groupIds,
                        true && testStart.plus(Duration.ofSeconds(120)).isAfter(Instant.now()), revivedEngines);
                incEngine(engines, currentEngine);
                start[0] = Instant.now();
            }
        }, jobnumber, createdCount, revivedEngines);
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Assertions.assertEquals(jobnumber + createdCount[0], contexts.stream().filter(c -> c.getValue().jobData().state() == DONE).count());
        Assertions.assertEquals(jobnumber + createdCount[0], testBeansFactory.getTestSenderData().stateCounts.get(DONE).get());
        Assertions.assertEquals(0, contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING).count());
        Assertions.assertEquals(0, contexts.stream().filter(c -> c.getValue().jobData().state() == ERROR).count());
        engines.forEach(e -> e.stop());
        Arrays.stream(fixedEngines).forEach(e -> e.stop());
    }

    private static void incEngine(final List<Engine> engines, final int[] currentEngine) {
        do {
            currentEngine[0] =  (currentEngine[0] + 1) % engines.size();
        } while (engines.get(currentEngine[0]) == null);
    }


    private void handleRevival(final TestBeansFactory testBeansFactory,
                               final List<Engine> engines, final int currentEngine,
                               final int[] createdCount, String[] groupIds, boolean createJobs, final Set<String> revivedEngines) {
        final EngineImpl toRevive = (EngineImpl) engines.get(currentEngine);
        final String engineName = toRevive.getName();

        logger.trace("handleRevival for engine: {} index is: {} doing", engineName, currentEngine);
        if(toRevive.getStartupTime().plus(Duration.ofSeconds(20)).isBefore(Instant.now())) {
            logger.info("Reviving Engine: {} {}", currentEngine, engineName);
            if(revivedEngines.contains(engineName)) {
                logger.error("Engine {} already revived once!!!", engineName);
            }
            else {
                engines.set(currentEngine, null);
                testResources.getContainer().submitInThread(
                        () -> {
                            toRevive.stop();
                            final EngineImpl newEngine = (EngineImpl) Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
                            engines.set(currentEngine, newEngine);
                            engines.get(currentEngine).register(cdiTestJob, TestContext.class);
                            logger.info("Did revive now {} is at index {} new name: {}", toRevive.getName(),
                                    currentEngine, newEngine.getName());
                            revivedEngines.add(engineName);
                        }
                );
            }
        } else {
            logger.trace("did not revive engine {}, was to young", engineName);
        }
        if(createJobs) {
            int nextEngine = currentEngine;
            Engine creatingEngine;
            do {
                nextEngine = (nextEngine + engines.size() - 1) % engines.size();
                creatingEngine = engines.get(nextEngine);
            } while (creatingEngine == null);
            for (int i = 0; i < 10; i++) {
                if(groupIds.length != 0) {
                    final String groupId = groupIds[createdCount[0] % groupIds.length];
                    creatingEngine.create(cdiTestJob, groupId, new TestContext(groupId));
                }
                else {
                    creatingEngine.create(cdiTestJob, new TestContext());
                }
                createdCount[0]++;
            }
        }

    }

    private void waitForTest(final TestBeansFactory testBeansFactory, Runnable doInLoop, int createdJobs, final int[] createdCount, final Set<String> revivedEngines) throws InterruptedException {
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Set<Map.Entry<String, String>> states = testBeansFactory.getTestSenderData().jobStates.entrySet();
        logger.info("Waiting for {} jobs to complete", states.size());
        Instant outputTimestamp = Instant.now();
        while (contexts.stream().anyMatch(c -> (c.getValue().jobData().state() != DONE) && (c.getValue().jobData().state() != ERROR))
               || states.stream().anyMatch(c -> !((c.getValue().endsWith("DONE") || (c.getValue().endsWith("ERROR")))))) {
            Thread.sleep(200);
            if(doInLoop != null) {
                doInLoop.run();
            }
            final Duration maxDelay = testResources.getContainer().getConfiguration().getMaxDelayOfStateMessages();
            if(logger.isInfoEnabled() && Instant.now().isAfter(outputTimestamp.plus(Duration.ofSeconds(3)))) {
                outputTimestamp = Instant.now();

                long running = contexts.stream().filter(c -> c.getValue().jobData().state() == RUNNING).count();
                long ingroup = contexts.stream().filter(c -> c.getValue().jobData().state() == GROUP).count();
                long delayed = contexts.stream().filter(c -> c.getValue().jobData().state() == DELAYED).count();
                long done = contexts.stream().filter(c -> c.getValue().jobData().state() == DONE).count();
                long errors = contexts.stream().filter(c -> c.getValue().jobData().state() == ERROR).count();

                HashMap<State, Long> stateMap = new HashMap<>();
                for (State s : State.values()) {
                    stateMap.put(s, 0L);
                }
                long[] revivableRunning = {0L};
                long[] revivableDelayed = {0L};
                long[] elapsedRunning = {0L};
                long[] elapsedDelayed = {0L};
                long[] runningOrDelayed = {0L};
                contexts.stream().forEach(c -> {
                    final State state = c.getValue().jobData().state();
                    stateMap.put(state, stateMap.get(state) + 1L);
                    if(state == RUNNING || state == DELAYED) {
                        runningOrDelayed[0]++;
                        final Instant date = c.getValue().jobData().date();
                        if(date.isBefore(Instant.now())) {
                            switch (state) {
                                case RUNNING:
                                    elapsedRunning[0]++;
                                    break;
                                case DELAYED:
                                    elapsedDelayed[0]++;
                                    break;
                            }
                        }
                        else {
                            if(date.plus(maxDelay).isBefore(Instant.now())) {
                                switch (state) {
                                    case RUNNING:
                                        revivableRunning[0]++;
                                        break;
                                    case DELAYED:
                                        revivableDelayed[0]++;
                                        break;
                                }
                            }
                        }
                    }

                });

                Map<State, AtomicInteger> stateCounts = testBeansFactory.getTestSenderData().stateCounts;
                logger.info("rn: {} dlyd: {} dn: {} er: {} grp: {} rnSnt: {} dlydSnt: {} grpSnt: {} elpsdR: {} elpsedDd: {} rvivR: {} rvivD: {} coll: {}",
                        stateMap.get(RUNNING), stateMap.get(DELAYED), stateMap.get(DONE), stateMap.get(ERROR), stateMap.get(GROUP),
                        stateCounts.get(State.RUNNING).get(), stateCounts.get(State.DELAYED).get(), stateCounts.get(State.GROUP).get(),
                        elapsedRunning[0], elapsedDelayed[0], revivableRunning[0], revivableDelayed[0], CDITestStep.collisionsDetected
                );
                if(ingroup > 0) {
                    logger.info("GroupRecords: {}", contexts.stream().filter(c -> c.getValue().jobData().state() == GROUP)
                            .map(c -> c.getValue().jobData().groupId() + "|" + c.getValue().jobData().id())
                            .collect(Collectors.joining(",")));
                }
                if(delayed > 0 || errors > 0) {
                    logger.error("Delayed or error jobs d/e/r/dn:sum {}/{}/{}/{}:{}",
                            delayed, errors, running, done, running + delayed + done + errors);
                    logger.info("id;step;state;date;group;correlationId;correlationId,nodeid");
                    Map<String, Long> elapsedOnLiveEngines = new HashMap<>();
                    contexts.stream()
                            .filter(e -> (e.getValue().jobData().state() == DELAYED || e.getValue().jobData().state() == RUNNING)
                                         && (e.getValue().jobData().date()/*.plus(maxDelay)*/.isBefore(Instant.now())
                                             || e.getValue().jobData().date()/*.plus(maxDelay)*/.isAfter(Instant.now().plus(Duration.ofSeconds(10))))
                                         && runningOrDelayed[0] < 100
                                         || e.getValue().jobData().state() == ERROR ||
                                         runningOrDelayed[0] < 20 && (e.getValue().jobData().state() == DELAYED || e.getValue().jobData().state() == RUNNING))
                            .sorted((c1, c2) -> c1.getValue().jobData().date().compareTo(c2.getValue().jobData().date()))
                            .forEach(
                                    e -> {
                                        JobDataImpl j = e.getValue().jobData();
                                        final String jobState = testBeansFactory.getTestSenderData().jobStates.get(j.id());
                                        if(jobState == null) {
                                            logger.error("No Jobstate found for id: {}", j.id());
                                        }
                                        else {
                                            final String engine = jobState.substring(0, jobState.lastIndexOf('_'));
                                            if(revivedEngines.contains(engine) || runningOrDelayed[0] < 20) {
                                                logger.info("{};{};{};{};{};{};{};{}", j.id(), j.step(),
                                                        j.state(), j.date(), j.groupId(), j.correlationId(),
                                                        jobState, revivedEngines.contains(engine) ? "rvv: " + j.date().plus(maxDelay) : "");
                                            }
                                            else {
                                                Long elapsed = elapsedOnLiveEngines.get(engine);
                                                if(elapsed == null) {
                                                    elapsedOnLiveEngines.put(engine, 1L);
                                                }
                                                else {
                                                    elapsedOnLiveEngines.put(engine, elapsed + 1);
                                                }
                                            }
                                        }
                                    }
                            );
                    elapsedOnLiveEngines.entrySet().forEach(e -> {
                        logger.error("Elapsed on live engine {}: {}records", e.getKey(), e.getValue());
                    });
                }
                logger.info("e: {} l: {} q: {} rn: {} dlyd: {} dn: {} er: {} grp: {} rnSnt: {} dlydSnt: {} grpSnt: {} elpsdR: {} elpsedDd: {} rvivR: {} rvivD: {} coll: {}",
                        CDITestStep.stepEntered.get(), CDITestStep.stepLeft.get(), this.testResources.getContainer().workQueue.size(),
                        stateMap.get(RUNNING), stateMap.get(DELAYED), stateMap.get(DONE), stateMap.get(ERROR), stateMap.get(GROUP),
                        stateCounts.get(State.RUNNING).get(), stateCounts.get(State.DELAYED).get(), stateCounts.get(State.GROUP).get(),
                        elapsedRunning[0], elapsedDelayed[0], revivableRunning[0], revivableDelayed[0], CDITestStep.collisionsDetected
                );
            }

        }
    }
}

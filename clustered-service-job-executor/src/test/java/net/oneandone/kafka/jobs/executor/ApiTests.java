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
        engine.register(jobTemplate);
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
        engine.register(cdiTestJob);
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
        engine.register(jobTemplate);
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
        tmpMap.put(job.getSignature(), new JobImpl(job, beans));
        Mockito.doReturn(tmpMap).when(beans).getJobs();
        Mockito.doReturn("RemoteNode" + Thread.currentThread().getName()).when(beans).getNodeId();
        return new LocalRemoteExecutor(beans);
    }

    @ParameterizedTest
    @CsvSource({
            "false,100,100,249,49,0,0,3,100,4",
            "false,100,100,249,49,0,0,30,100,4",
            "false,100,100,249,49,0,0,10,100,4",

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
            "false,100,100,249,49,0,0,5,100,4",
            "false,10000,10000,20000,0,0,0,0,0,0",}
    )
    void cdiTest(boolean doremote, int loops, int expectedDone, int expectedRunning, int expectedDelayed,
                 int expectedSuspended, int expectedError, int groupNo, int expectedGroup, int successesInSequence) throws InterruptedException {
        CDITestStep.successesInSequence = successesInSequence;
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine engine = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
        String[] groups = generateGroupNames(groupNo);
        engine.register(cdiTestJob);
        if(doremote) {
            engine.register(createRemoteTestExecutor(cdiTestJob));
        }
        for (int i = 0; i < loops; i++) {
            Thread.sleep(10);
            createJob( groups, i, engine);
        }
        waitForTest(testBeansFactory, null,  Collections.emptySet(), 0);
        checkTests(expectedDone, expectedRunning, expectedDelayed, expectedSuspended, expectedError, expectedGroup, testBeansFactory);

        engine.stop();
    }

    @Test
    void submitTest() throws InterruptedException {
        final int[] ints = new int[1];

        Future<?> future = testResources.getContainer().submitInWorkerThread(
                () -> ints[0]++
        );
        while (!future.isDone()) {
            Thread.sleep(100);
        }
        Assertions.assertEquals(1, ints[0]);
    }

    @ParameterizedTest
    @CsvSource({
            "10000,1,10,30000,1000,10000,1000000,5000,180",
            "100000,1,10,100000000,10000,100,1000,1000,1800",
            "10000,1,10,100000000,10000,100,100,0,180",

            "10000,1,10,10000,10000,0,2,0,180",
            "10000,1,2,1000,10000,0,3000,0,180",
            "10000,1,2,1000,10000,0,0,0,180",
            "10000,1,2,3000,10000,0,4,0,180",
            "100,1,2,3000,1,0,4,0,180",  // one engine a few revivals after creation
            "100,1,2,300000,1,0,4,0,180",  // one engine no revival
            "100,1,2,300000,1,0,4,0,180",  // 2 engines no revival
            "100,1,1,5000,10000,0,4,0,180",
            "100,1,10,300000,1,0,4,0,180",  // 10 engines no revival
            "100,1,1,3000,1,0,4,0,180",  // one engine a few revivals after creation
            "100,1,1,5000,10000,0,4,0,180",
            "100,1,2,5000,10000,0,4,0,180",
            "100,1,10,5000,10000,0,4,0,180",
            "10000,1,5,3000,10000,0,4,0,180",
            "10000,1,2,2000,10000,0,4,0,180",
            "10000,1,5,5000,10000,0,4,0,180",
            "10000,1,5,30000,10000,5000,4,0,180",
            "10000,1,5,30000,10000,1000,4,0,180",
            "10000,1,5,3000,10000,0,4,0,180",
            "100,1,2,5000000,1000,1,4,0,180",
            "20,1,2,5000,10000,1,4,0,180",
            "100,1,10,5000,10000,10,4,0,180",
            "100,1,10,5000,10000,100,4,0,180",
            "100,1,2,5000,10000,100,4,0,180",
            "100,1,2,5000,10000,10,4,0,180",

    })
    void reviverTest(int jobnumber, int fixedEngineNumber, int engineNumber, int revivalAfter, int creationTime,
                     int groupNo, int successesInSequence, int periodicAdding, int createTimeSeconds) throws InterruptedException {
        testResources.getContainer().setWorkerThreadPoolSize((fixedEngineNumber + engineNumber) * 20);
        CDITestStep.successesInSequence = successesInSequence;
        Set<String> revivedEngines = new HashSet<>();
        final TestBeansFactory testBeansFactory = testResources.getTestBeansFactory();
        Engine[] fixedEngines = new Engine[fixedEngineNumber];
        for (int i = 0; i < fixedEngineNumber; i++) {
            fixedEngines[i] = Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
            fixedEngines[i].register(cdiTestJob);
        }
        List<Engine> engines = new ArrayList<>();
        for (int i = 0; i < engineNumber; i++) {
            engines.add(Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory));
            engines.get(i).register(cdiTestJob);
        }
        Collections.shuffle(engines);
        String[] groupIds = generateGroupNames(groupNo);
        final Instant testStart = Instant.now();
        final Instant[] lastRevival = {Instant.now()};
        final Instant[] lastCreation = {Instant.now()};
        final int[] currentEngine = {0};
        final int[] createdCount = {0};
        createInitialJobs(jobnumber, revivalAfter, creationTime, groupNo,
                revivedEngines, testBeansFactory, engines, groupIds, lastRevival, currentEngine, createdCount);
        waitForTest(testBeansFactory, () -> {
            if(Instant.now().isAfter(lastRevival[0].plus(Duration.ofMillis(revivalAfter)))) {
                handleRevival(testBeansFactory, engines, currentEngine[0], revivedEngines);

                incEngine(engines, currentEngine);
                lastRevival[0] = Instant.now();
            }
            if(Instant.now().isAfter(lastCreation[0].plus(Duration.ofMillis(creationTime)))) {
                jobCreation(engines, currentEngine[0], createdCount, groupIds,
                        testStart.plus(Duration.ofSeconds(createTimeSeconds)).isAfter(Instant.now()),
                        periodicAdding);
                lastCreation[0] = Instant.now();
            }
        },  revivedEngines, createTimeSeconds);
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Assertions.assertEquals(jobnumber + createdCount[0], contexts.stream().filter(c -> c.getValue().jobData().getState() == DONE).count());
        Assertions.assertEquals(jobnumber + createdCount[0], testBeansFactory.getTestSenderData().stateCounts.get(DONE).get());
        Assertions.assertEquals(0, contexts.stream().filter(c -> c.getValue().jobData().getState() == RUNNING).count());
        Assertions.assertEquals(0, contexts.stream().filter(c -> c.getValue().jobData().getState() == ERROR).count());
        engines.forEach(e -> e.stop());
        Arrays.stream(fixedEngines).forEach(e -> e.stop());
    }

    private void createInitialJobs(final int jobnumber, final int revivalAfter, final int creationTime, final int groupNo,
                                   final Set<String> revivedEngines,
                                   final TestBeansFactory testBeansFactory, final List<Engine> engines,
                                   final String[] groupIds, final Instant[] start, final int[] currentEngine,
                                   final int[] createdCount) throws InterruptedException {
        for (int i = 0; i < jobnumber; i++) {
            final Engine engine = engines.get(currentEngine[0]);
            createJob(groupIds, i, engine);
            if(Instant.now().isAfter(start[0].plus(Duration.ofMillis(revivalAfter)))) {
                handleRevival(testBeansFactory, engines, currentEngine[0],
                         revivedEngines);
                incEngine(engines, currentEngine);
                start[0] = Instant.now();
            }
            if(i % 1000 == 99) {
                Thread.sleep(creationTime / (jobnumber / 1000));
                logger.info("Sent {} records", i);
            }
        }
    }
    private void createJob(final String[] groupIds, final int createdCount, final Engine creatingEngine) {
        if(groupIds.length > 0) {
            final String groupId = groupIds[createdCount % groupIds.length];
            creatingEngine.create(cdiTestJob, groupId, new TestContext(groupId));
        }
        else {
            creatingEngine.create(cdiTestJob, new TestContext());
        }
    }

    private static void incEngine(final List<Engine> engines, final int[] currentEngine) {
        do {
            currentEngine[0] =  (currentEngine[0] + 1) % engines.size();
        } while (engines.get(currentEngine[0]) == null);
    }


    private void handleRevival(final TestBeansFactory testBeansFactory,
                               final List<Engine> engines, final int currentEngine,
                               final Set<String> revivedEngines) {
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
                testResources.getContainer().submitInWorkerThread(
                        () -> {
                            toRevive.stop();
                            final EngineImpl newEngine = (EngineImpl) Providers.get().createTestEngine(testResources.getContainer(), testBeansFactory);
                            engines.set(currentEngine, newEngine);
                            engines.get(currentEngine).register(cdiTestJob);
                            logger.info("Did revive now {} is at index {} new name: {}", toRevive.getName(),
                                    currentEngine, newEngine.getName());
                            revivedEngines.add(engineName);
                        }
                );
            }
        } else {
            logger.trace("did not revive engine {}, was to young", engineName);
        }


    }

    private void jobCreation(final List<Engine> engines, final int currentEngine, final int[] createdCount, final String[] groupIds, final boolean createJobs, final int periodicAdding) {
        if(createJobs) {
            int nextEngine = currentEngine;
            Engine creatingEngine;
            do {
                nextEngine = (nextEngine + engines.size() - 1) % engines.size();
                creatingEngine = engines.get(nextEngine);
            } while (creatingEngine == null);
            for (int i = 0; i < periodicAdding; i++) {
                createJob(groupIds, createdCount[0], creatingEngine);
                createdCount[0]++;
            }
        }
    }


    private void waitForTest(final TestBeansFactory testBeansFactory, Runnable doInLoop, final Set<String> revivedEngines, final int createTimeSeconds) throws InterruptedException {
        Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts = testBeansFactory.getTestSenderData().lastContexts.entrySet();
        Set<Map.Entry<String, String>> states = testBeansFactory.getTestSenderData().jobStates.entrySet();
        logger.info("Waiting for {} jobs to complete", states.size());
        Instant startTime = Instant.now();
        Instant outputTimestamp = Instant.now();
        while (contexts.stream().anyMatch(c -> (c.getValue().jobData().getState() != DONE) && (c.getValue().jobData().getState() != ERROR))
               || states.stream().anyMatch(c -> !((c.getValue().endsWith("DONE") || (c.getValue().endsWith("ERROR")))))
                || startTime.plus(Duration.ofSeconds(createTimeSeconds)).isAfter(Instant.now())
        ) {
            Thread.sleep(200);
            if(doInLoop != null) {
                doInLoop.run();
            }
            outputTimestamp = outputInfo(testBeansFactory, revivedEngines, contexts, outputTimestamp);

        }
        logger.info("Ready waiting for test");
    }

    private Instant outputInfo(final TestBeansFactory testBeansFactory, final Set<String> revivedEngines, final Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts, Instant outputTimestamp) {
        final Duration maxDelay = testResources.getContainer().getConfiguration().getMaxDelayOfStateMessages();
        if(logger.isInfoEnabled() && Instant.now().isAfter(outputTimestamp.plus(Duration.ofSeconds(3)))) {
            outputTimestamp = Instant.now();

            long running = contexts.stream().filter(c -> c.getValue().jobData().getState() == RUNNING).count();
            long ingroup = contexts.stream().filter(c -> c.getValue().jobData().getState() == GROUP).count();
            long delayed = contexts.stream().filter(c -> c.getValue().jobData().getState() == DELAYED).count();
            long done = contexts.stream().filter(c -> c.getValue().jobData().getState() == DONE).count();
            long errors = contexts.stream().filter(c -> c.getValue().jobData().getState() == ERROR).count();

            HashMap<State, Long> stateMap = new HashMap<>();
            for (State s : State.values()) {
                stateMap.put(s, 0L);
            }
            long[] revivableRunning = {0L};
            long[] revivableDelayed = {0L};
            long[] elapsedRunning = {0L};
            long[] elapsedDelayed = {0L};
            long[] runningOrDelayed = {0L};
            countStates(contexts, maxDelay, stateMap, revivableRunning, revivableDelayed, elapsedRunning, elapsedDelayed, runningOrDelayed);

            Map<State, AtomicInteger> stateCounts = testBeansFactory.getTestSenderData().stateCounts;
            logger.info("rn: {} dlyd: {} dn: {} er: {} grp: {} rnSnt: {} dlydSnt: {} grpSnt: {} elpsdR: {} elpsedDd: {} rvivR: {} rvivD: {} calld: {} coll: {} ",
                    stateMap.get(RUNNING), stateMap.get(DELAYED), stateMap.get(DONE), stateMap.get(ERROR), stateMap.get(GROUP),
                    stateCounts.get(State.RUNNING).get(), stateCounts.get(State.DELAYED).get(), stateCounts.get(State.GROUP).get(),
                    elapsedRunning[0], elapsedDelayed[0], revivableRunning[0], revivableDelayed[0], CDITestStep.stepEntered,
                    CDITestStep.collisionsDetected
            );
            if(ingroup > 0 && ingroup < 30) {
                logger.info("GroupRecords: {}", contexts.stream().filter(c -> c.getValue().jobData().getState() == GROUP)
                        .map(c -> c.getValue().jobData().getGroupId() + "|" + c.getValue().jobData().getId())
                        .collect(Collectors.joining(",")));
            }
            if(delayed > 0 || errors > 0) {
                logger.warn("Delayed or error jobs d/e/r/dn:sum {}/{}/{}/{}:{}",
                        delayed, errors, running, done, running + delayed + done + errors);
                logger.info("id;step;state;date;group;correlationId;correlationId,nodeid");
                outputData(testBeansFactory, revivedEngines, contexts, maxDelay, runningOrDelayed);
            }
            logger.info("t: {} e: {} l: {} q: {} rn: {} dlyd: {} dn: {} er: {} grp: {} rnSnt: {} dlydSnt: {} grpSnt: {} elpsdR: {} elpsedDd: {} rvivR: {} rvivD: {} calld: {} coll: {}",
                    CDITestStep.staticThreadCount, CDITestStep.stepEntered.get(), CDITestStep.stepLeft.get(), this.testResources.getContainer().workerThreadQueue.size(),
                    stateMap.get(RUNNING), stateMap.get(DELAYED), stateMap.get(DONE), stateMap.get(ERROR), stateMap.get(GROUP),
                    stateCounts.get(State.RUNNING).get(), stateCounts.get(State.DELAYED).get(), stateCounts.get(State.GROUP).get(),
                    elapsedRunning[0], elapsedDelayed[0], revivableRunning[0], revivableDelayed[0], CDITestStep.stepEntered,
                    CDITestStep.collisionsDetected
            );
        }
        return outputTimestamp;
    }

    private static void outputData(final TestBeansFactory testBeansFactory, final Set<String> revivedEngines, final Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts, final Duration maxDelay, final long[] runningOrDelayed) {
        Map<String, Long> elapsedOnLiveEngines = new HashMap<>();
        contexts.stream()
                .filter(e -> (e.getValue().jobData().getState() == DELAYED || e.getValue().jobData().getState() == RUNNING)
                             && (e.getValue().jobData().getDate()/*.plus(maxDelay)*/.isBefore(Instant.now())
                                 || e.getValue().jobData().getDate()/*.plus(maxDelay)*/.isAfter(Instant.now().plus(Duration.ofSeconds(10))))
                             && runningOrDelayed[0] < 100
                             || e.getValue().jobData().getState() == ERROR ||
                             runningOrDelayed[0] < 20 && (e.getValue().jobData().getState() == DELAYED || e.getValue().jobData().getState() == RUNNING))
                .sorted((c1, c2) -> c1.getValue().jobData().getDate().compareTo(c2.getValue().jobData().getDate()))
                .forEach(
                        e -> {
                            JobDataImpl j = e.getValue().jobData();
                            final String jobState = testBeansFactory.getTestSenderData().jobStates.get(j.getId());
                            if(jobState == null) {
                                logger.error("No Jobstate found for id: {}", j.getId());
                            }
                            else {
                                final String engine = jobState.substring(0, jobState.lastIndexOf('_'));
                                if(revivedEngines.contains(engine) || runningOrDelayed[0] < 20) {
                                    logger.info("{};{};{};{};{};{};{};{}", j.getId(), j.getStep(),
                                            j.getState(), j.getDate(), j.getGroupId(), j.getCorrelationId(),
                                            jobState, revivedEngines.contains(engine) ? "rvv: " + j.getDate().plus(maxDelay) : "");
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

    private static void countStates(final Set<Map.Entry<Pair<String, String>, TransportImpl>> contexts, final Duration maxDelay, final HashMap<State, Long> stateMap, final long[] revivableRunning, final long[] revivableDelayed, final long[] elapsedRunning, final long[] elapsedDelayed, final long[] runningOrDelayed) {
        contexts.stream().forEach(c -> {
            final State state = c.getValue().jobData().getState();
            stateMap.put(state, stateMap.get(state) + 1L);
            if(state == RUNNING || state == DELAYED) {
                runningOrDelayed[0]++;
                final Instant date = c.getValue().jobData().getDate();
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
    }
}

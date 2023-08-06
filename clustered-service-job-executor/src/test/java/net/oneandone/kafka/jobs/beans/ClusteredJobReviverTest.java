package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.DONE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class ClusteredJobReviverTest {


    private static void prepareReceivedGroupRecord(final BeansMocks mocks, int index, int group) {
        prepareReceiveRecord(mocks, index, mocks.now, 1, State.GROUP, group);
    }
    private static void prepareReceivedGroupRecord(final BeansMocks mocks, int index, int group, State state) {
        prepareReceiveRecord(mocks, index, mocks.now, 1, state, group);
    }


    private static void prepareReceiveRecord(final BeansMocks mocks, int index, Instant recordDate) {
        prepareReceiveRecord(mocks, index, recordDate, 1, State.RUNNING, null);
    }

    private static void prepareReceiveRecord(final BeansMocks mocks, int index, Instant recordDate, int partition) {
        prepareReceiveRecord(mocks, index, recordDate, partition, State.RUNNING, null);
    }

    private static TransportImpl prepareReceiveRecord(final BeansMocks mocks, int index, Instant recordDate, int partition, State state, Integer group) {
        JobDataImpl jobData = new JobDataImpl(Integer.toString(index), null, state, "sig", 0, 0, mocks.beans);
        if(group != null) {
            jobData.setGroup(Integer.toString(group));
        }
        jobData.setDate(recordDate);
        TransportImpl transport = new TransportImpl(jobData, "", mocks.beans);
        Pair<String, String> pair = JobTools.prepareKafkaKeyValue(transport);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", partition, index, pair.getKey(), pair.getValue());
        mocks.sut.receiveRecord(Collections.emptyMap(), record);
        mocks.addTransport(partition + " " + index, transport);
        return transport;
    }

    @Test
    void canReviveOneRecord() {
        BeansMocks mocks = new BeansMocks();
        mocks.sut.setStateInitCompleted(true);
        prepareReceiveRecord(mocks, 1, mocks.toRevive());
        Assertions.assertEquals(1, mocks.beans.getJobDataStates().size());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(1)).readJob(any(JobDataState.class));
        Mockito.verify(mocks.sender, times(1)).send(any(TransportImpl.class));
    }

    @Test
    void canReviveTwoRecords() {
        BeansMocks mocks = new BeansMocks();
        doReturn(2).when(mocks.configuration).getMaxRecordsToReadByRandomAccess();
        mocks.sut.setStateInitCompleted(true);
        prepareReceiveRecord(mocks, 1, mocks.toRevive());
        prepareReceiveRecord(mocks, 2, mocks.toRevive());

        Assertions.assertEquals(2, mocks.beans.getJobDataStates().size());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(2)).readJob(any(JobDataState.class));
        Mockito.verify(mocks.sender, times(2)).send(any(TransportImpl.class));
    }

    @Test
    void canNotReviveOneRecord() {
        BeansMocks mocks = new BeansMocks();
        doReturn(2).when(mocks.configuration).getMaxRecordsToReadByRandomAccess();
        prepareReceiveRecord(mocks, 1, mocks.now);
        prepareReceiveRecord(mocks, 2, mocks.toRevive());

        Assertions.assertEquals(2, mocks.beans.getJobDataStates().size());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(1)).readJob(any(JobDataState.class));
        Mockito.verify(mocks.sender, times(1)).send(any(TransportImpl.class));
    }

    @Test
    void canNotReviveIfNotNecessaryRecord() {
        BeansMocks mocks = new BeansMocks();
        doReturn(2).when(mocks.configuration).getMaxRecordsToReadByRandomAccess();

        prepareReceiveRecord(mocks, 1, mocks.now);
        prepareReceiveRecord(mocks, 2, mocks.now);

        Assertions.assertEquals(2, mocks.beans.getJobDataStates().size());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(0)).readJob(any(JobDataState.class));
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
    }

    @Test
    void canReviveByReadingRanges() {
        BeansMocks mocks = new BeansMocks();

        prepareReceiveRecord(mocks, 1, mocks.toRevive());
        prepareReceiveRecord(mocks, 3, mocks.toRevive());

        Assertions.assertEquals(2, mocks.beans.getJobDataStates().size());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(1)).readJobs(
                eq(Arrays.asList(Triple.of(1, 1L, 3L))),
                anyMap());
    }

    @Test
    void canReviveByReadingRangesStartingAfterOffset() {
        BeansMocks mocks = new BeansMocks();

        prepareReceiveRecord(mocks, 1, mocks.now);
        prepareReceiveRecord(mocks, 2, mocks.toRevive());
        prepareReceiveRecord(mocks, 4, mocks.toRevive());
        prepareReceiveRecord(mocks, 5, mocks.now);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));

        Assertions.assertEquals(4, mocks.beans.getJobDataStates().size());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(1)).readJobs(
                eq(Arrays.asList(Triple.of(1, 2L, 4L))),
                anyMap());
    }

    @Test
    void canReviveByReadingRangesStartingAfterOffsetInMultiplePartitions() {
        BeansMocks mocks = new BeansMocks();

        prepareReceiveRecord(mocks, 1, mocks.now);
        prepareReceiveRecord(mocks, 2, mocks.toRevive());
        prepareReceiveRecord(mocks, 3, mocks.now);
        prepareReceiveRecord(mocks, 4, mocks.toRevive());
        prepareReceiveRecord(mocks, 5, mocks.now);
        prepareReceiveRecord(mocks, 1001, mocks.toRevive(), 3);
        prepareReceiveRecord(mocks, 1002, mocks.toRevive(), 3);
        prepareReceiveRecord(mocks, 1004, mocks.toRevive(), 3);
        prepareReceiveRecord(mocks, 1005, mocks.toRevive(), 3);

        Assertions.assertEquals(9, mocks.beans.getJobDataStates().size());
        List<TransportImpl> jobs = mocks.transports
                .entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("3 ") || e.getKey().equals("1 2") || e.getKey().equals("1 4"))
                .map(e -> e.getValue()).collect(Collectors.toList());

        doReturn(jobs).when(mocks.receiver).readJobs(eq(Arrays.asList(Triple.of(1, 2L, 4L), Triple.of(3, 1001L, 1005L))),
                anyMap());
        mocks.sut.call();
        Mockito.verify(mocks.receiver, times(1)).readJobs(
                eq(Arrays.asList(Triple.of(1, 2L, 4L), Triple.of(3, 1001L, 1005L))),
                anyMap());

        Mockito.verify(mocks.sender, times(6)).send(any(TransportImpl.class));
    }

    @Test
    void canMakeGroupCallToRunning() {
        BeansMocks mocks = new BeansMocks();
        prepareReceivedGroupRecord(mocks, 1, 1);
        Mockito.verify(mocks.sender, times(1)).send(any(TransportImpl.class));
        // same group must not start job
        prepareReceivedGroupRecord(mocks, 2, 1);
        Mockito.verify(mocks.sender, times(1)).send(any(TransportImpl.class));
        // other group may start job
        prepareReceivedGroupRecord(mocks, 3, 2);
        Mockito.verify(mocks.sender, times(2)).send(any(TransportImpl.class));
        // first job of group receives RUNNING from self
        prepareReceivedGroupRecord(mocks, 1, 1, State.RUNNING);
        Mockito.verify(mocks.sender, times(2)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 1, 1, DONE);
        // first job ob group ended, so the second one must be started
        Mockito.verify(mocks.sender, times(3)).send(any(TransportImpl.class));
        // second job of group sends RUNNING to self
        prepareReceivedGroupRecord(mocks, 2, 1, State.RUNNING);
        Mockito.verify(mocks.sender, times(3)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 4, 1);
        Mockito.verify(mocks.sender, times(3)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 2, 1, State.ERROR);
        Mockito.verify(mocks.sender, times(4)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 5, 2);
        Mockito.verify(mocks.sender, times(4)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 3, 2, DONE);
        Mockito.verify(mocks.sender, times(5)).send(any(TransportImpl.class));
    }

    @Test
    void canReInitGroups() {
        BeansMocks mocks = new BeansMocks();
        mocks.sut.setStateInitCompleted(false);
        prepareReceivedGroupRecord(mocks, 1, 1);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
        // same group must not start job
        prepareReceivedGroupRecord(mocks, 2, 1);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
        // other group may start job
        prepareReceivedGroupRecord(mocks, 3, 2);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
        // first job of group receives RUNNING from self
        prepareReceivedGroupRecord(mocks, 1, 1, State.RUNNING);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 1, 1, DONE);
        // first job ob group ended, so the second one must be started
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));

        mocks.sut.setStateInitCompleted(true);
        // second job of group sends RUNNING to self
        prepareReceivedGroupRecord(mocks, 2, 1, State.RUNNING);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 4, 1);
        Mockito.verify(mocks.sender, times(0)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 2, 1, State.ERROR);
        Mockito.verify(mocks.sender, times(1)).send(any(TransportImpl.class));
        // second job in group 2 can not be started as long as first is not done
        prepareReceivedGroupRecord(mocks, 5, 2);
        Mockito.verify(mocks.sender, times(1)).send(any(TransportImpl.class));
        // call should revive Group 2
        doReturn(Duration.ofSeconds(10)).when(mocks.configuration).getReviverPeriod();
        doReturn(Clock.fixed(Instant.now().plus(Duration.ofSeconds(11)), ZoneId.of("CET"))).when(mocks.container).getClock();
        // needs to revive 4 and 3
        mocks.sut.call();
        Mockito.verify(mocks.sender, times(3)).send(any(TransportImpl.class));
        prepareReceivedGroupRecord(mocks, 3, 2, DONE);
        // now 5 may start
        Mockito.verify(mocks.sender, times(4)).send(any(TransportImpl.class));
    }

    @Test
    void mayNotStartGroupDuringInit() {

    }

    @Test
    void canReviveGroups() {

    }

    static class BeansMocks {
        final Container container;
        final Configuration configuration;
        final EngineImpl engine;
        final Receiver receiver;
        final JobTools jobTools;
        final JobsSender sender;
        final Instant now;
        private final ClusteredJobReviver sut;
        Beans beans;
        private Map<String, TransportImpl> transports = new HashMap<>();

        public BeansMocks() {
            now = Instant.now();
            beans = mock(Beans.class);
            doReturn(new HashMap<>()).when(beans).getJobDataStates();
            doReturn(new HashMap<>()).when(beans).getStatesByGroup();
            container = mock(Container.class);
            configuration = mock(Configuration.class);
            doReturn(configuration).when(container).getConfiguration();
            doReturn(Duration.ofSeconds(1)).when(configuration).getMaxDelayOfStateMessages();
            doReturn(1).when(configuration).getMaxRecordsToReadByRandomAccess();
            doReturn(container).when(beans).getContainer();
            doReturn(Clock.fixed(now, ZoneId.of("CET"))).when(container).getClock();
            engine = mock(EngineImpl.class);
            doReturn(engine).when(beans).getEngine();
            receiver = mock(Receiver.class);
            doReturn(receiver).when(beans).getReceiver();
            jobTools = new JobTools(beans);
            doReturn(jobTools).when(beans).getJobTools();
            sender = mock(JobsSender.class);
            doReturn(sender).when(beans).getSender();
            sut = new ClusteredJobReviver(beans);
            sut.setStateInitCompleted(true);
            doAnswer(new Answer<TransportImpl>() {
                public TransportImpl answer(InvocationOnMock invocation) {
                    JobDataState jobDataState = invocation.getArgument(0);
                    String searchString = jobDataState.getPartition() + " " + jobDataState.getOffset();
                    return transports.get(searchString);
                }
            }).when(receiver).readJob(any(JobDataState.class));
        }

        Instant toRevive() {
            return now.minus(beans.getContainer().getConfiguration().getMaxDelayOfStateMessages()).minus(Duration.ofSeconds(1));
        }

        public void addTransport(final String s, final TransportImpl transport) {
            this.transports.put(s, transport);
        }
    }
}

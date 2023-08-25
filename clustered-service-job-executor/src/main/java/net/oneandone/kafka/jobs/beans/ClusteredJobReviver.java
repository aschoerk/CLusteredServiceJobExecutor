package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.GROUP;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import net.oneandone.kafka.clusteredjobs.api.ClusterTask;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

public class ClusteredJobReviver extends StoppableBase implements ClusterTask {
    private Future<?> jobStateReceiverThread;
    private boolean reviverClaimed = false;

    private boolean stateInitCompleted;

    public ClusteredJobReviver(Beans beans) {
        super(beans);
    }


    /**
     * start to consume all existing state-records
     */
    @Override
    public void startup() {
        logger.info("E: {} startup", beans.getEngine().getName());
        final Map<String, Object> consumerConfig = Receiver.getConsumerConfig(beans);
        // jobStates must be received by all nodes.
        stateInitCompleted = false;
        consumerConfig.put(GROUP_ID_CONFIG, beans.getNodeId());
        this.jobStateReceiverThread = submitLongRunning(() -> {
            initThreadName("State-Receiver");
            consumerConfig.put(GROUP_ID_CONFIG, beans.getNodeId());
            consumerConfig.put("max.poll.records", beans.getContainer().getConfiguration().getMaxPollAdminJobDataRecords());
            receiveJobState(consumerConfig);
        });

        reviverClaimed = true;
        logger.info("E: {} startup done", beans.getEngine().getName());
    }

    private void receiveJobState(final Map<String, Object> consumerConfig) {
        logger.info("Starting to receive Jobstates using group {}", consumerConfig.get(GROUP_ID_CONFIG));
        try (KafkaConsumer<String, String> consumer = beans.createConsumer(consumerConfig)) {
            final String jobStateTopicName = beans.getContainer().getJobDataTopicName();
            List<PartitionInfo> partitions = consumer.partitionsFor(jobStateTopicName);
            List<TopicPartition> topicPartitions = partitions.stream().map(p -> new TopicPartition(jobStateTopicName, p.partition())).collect(Collectors.toList());
            Map<Integer, Long> currentEndOffsets = consumer.endOffsets(topicPartitions).entrySet().stream().collect(Collectors.toMap(e -> e.getKey().partition(), e -> e.getValue()));
            consumer.beginningOffsets(topicPartitions).entrySet().stream().forEach(e -> {
                if(currentEndOffsets.get(e.getKey().partition()) <= e.getValue()) {
                    currentEndOffsets.remove(e.getKey().partition());
                }
            });
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);

            while (reviverClaimed && !doShutDown()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if(currentEndOffsets.isEmpty() || records.isEmpty()) {
                    if (!stateInitCompleted) {
                        logger.info("Ready receiving initial Jobstates");
                        this.stateInitCompleted = true;
                        beans.getReceiver().setStateInitCompleted();
                        startGroupsAfterInit();
                    }
                }
                for (ConsumerRecord<String, String> r : records) {
                    receiveRecord(currentEndOffsets, r);
                }
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
    }

    private void startGroupsAfterInit() {
        beans.getStatesByGroup().entrySet().forEach(e -> {
            Queue<String> ids = e.getValue();
            JobDataState nextOneState = beans.getJobDataStates().get(ids.element());
            if (nextOneState.getState() == GROUP) {
                TransportImpl nextOne = beans.getReceiver().readJob(beans.getJobDataStates().get(nextOneState.getId()));
                logger.info("After Init, Starting grouped Job: {} GroupId: {}", nextOne.jobData().getId(), nextOne.jobData().getGroupId());
                beans.getJobTools().prepareJobDataForRunning(nextOne.jobData());
                beans.getSender().send(nextOne);
            }
        });
    }

    void receiveRecord(final Map<Integer, Long> currentEndOffsets, final ConsumerRecord<String, String> r) {
        if(currentEndOffsets.containsKey(r.partition())) {
            if(currentEndOffsets.get(r.partition()) <= r.offset()) {
                currentEndOffsets.remove(r.partition());
            }
        }
        TransportImpl transport = beans.getJobTools().evaluatePackage(r);
        JobDataImpl jobData = transport.jobData();
        JobDataState jobDataState = new JobDataState(jobData);

        logger.trace("Received state {} for job: {} groupId: {}", jobDataState.getState(),
                jobDataState.getId(), jobDataState.getGroupId());
        beans.getJobDataStates().put(jobDataState.getId(), jobDataState);
        if(jobDataState.getGroupId() != null) {
            try {
                beans.getContainer().submitShortRunning(() -> {
                    handleGroupJob(transport);
                });
            } catch (RejectedExecutionException e) {
                handleGroupJob(transport);
            }
        }
    }

    private void handleGroupJob(final TransportImpl transport) {
        final State state = transport.jobData().getState();
        if(state == GROUP) {
            handleNewGroupedJob(transport);
        }
        else {
            if((state == DONE) || (state == ERROR)) {
                handleEndOfGroupedJob(transport);
            }
        }
    }

    public int compareGroupJobs(String j, String k) {
        final JobDataState stateJ = beans.getJobDataStates().get(j);
        final JobDataState stateK = beans.getJobDataStates().get(k);
        if (stateJ.getCreatedAt().equals(stateK.getCreatedAt())) {
            return String.CASE_INSENSITIVE_ORDER.compare(stateJ.getId(), stateK.getId());
        } else {
            return stateJ.getCreatedAt().compareTo(stateK.getCreatedAt());
        }
    }

    private void handleEndOfGroupedJob(final TransportImpl transport) {
        JobDataImpl jobData = transport.jobData();
        logger.trace("E: {} done group {} record {}", beans.getEngine().getName(), jobData.getGroupId(), jobData.getId());
        Queue<String> statesForThisGroup = beans.getStatesByGroup().get(jobData.getGroupId());
        if((statesForThisGroup != null)) {
            if (statesForThisGroup.contains(jobData.getId())) {
                synchronized (beans) {
                    logger.info("removing grouped record {} group: {}",
                            jobData.getId(), jobData.getGroupId());
                    if(!statesForThisGroup.remove(jobData.getId())) {
                        logger.error("but not foundrecord {} group: {}",
                                jobData.getId(), jobData.getGroupId());
                    }
                    if(statesForThisGroup.isEmpty()) { // remove collection from common group map
                        beans.getStatesByGroup().remove(jobData.getGroupId());
                    }
                    else {
                        Optional<String> nextOneState = statesForThisGroup.stream()
                                .min((i, j) -> compareGroupJobs(i, j));
                        if(stateInitCompleted && nextOneState.isPresent()) {
                            TransportImpl nextOne = beans.getReceiver().readJob(beans.getJobDataStates().get(nextOneState.get()));
                            logger.info("After End, Starting grouped Job: {} GroupId: {}", nextOne.jobData().getId(), nextOne.jobData().getGroupId());
                            beans.getJobTools().prepareJobDataForRunning(nextOne.jobData());
                            beans.getSender().send(nextOne);
                        }
                    }
                }
            }  else {
                logger.error("Ignored End Of Group Job {}, group {} statesForThisGroup: {}",jobData.getId(),
                        jobData.getGroupId(),statesForThisGroup);
            }
        } else {
            logger.error("Ignored End Of Group Job {}, group {} statesForThisGroup: {}",jobData.getId(),
                    jobData.getGroupId(),statesForThisGroup);
        }
    }

    private void handleNewGroupedJob(final TransportImpl transport) {
        JobDataImpl jobData = transport.jobData();
        logger.trace("Adding group job {} groupId: {}", jobData.getId(), jobData.getGroupId());
        Queue<String> statesForThisGroup = beans.getStatesByGroup().get(jobData.getGroupId());
        synchronized (beans) {
            statesForThisGroup = beans.getStatesByGroup().get(jobData.getGroupId());
            if(statesForThisGroup == null) {
                statesForThisGroup = new ConcurrentLinkedQueue<>();
                statesForThisGroup.add(jobData.getId());
                beans.getStatesByGroup().put(jobData.getGroupId(), statesForThisGroup);
            }
            else {
                statesForThisGroup.add(jobData.getId());
            }
        }

        if(stateInitCompleted && statesForThisGroup.size() == 1) {
            if (!statesForThisGroup.element().equals(jobData.getId())) {
                logger.error("Logical error when starting group job {} groupId: {}", jobData.getId(), jobData.getGroupId());
            }
            beans.getJobTools().prepareJobDataForRunning(transport.jobData());
            logger.info("Starting grouped: {} GroupId: {}", jobData.getId(), jobData.getGroupId());
            beans.getSender().send(transport);
        } else {
            logger.info("Enqueued grouped: {} GroupId: {} stateInit {}", jobData.getId(), jobData.getGroupId(), stateInitCompleted);
        }
    }

    /**
     * identify and restart stale jobs
     */
    @Override
    public void call() {
        logger.warn("E: {} call", beans.getEngine().getName());
        if (stateInitCompleted) {
            // only check for revive after init of Clustered Task
            final Duration maxDelayOfStateMessages = beans.getContainer().getConfiguration().getMaxDelayOfStateMessages();
            final Instant instant = beans.getContainer().getClock().instant();
            List<JobDataState> statesToRevive = beans.getJobDataStates()
                    .entrySet().stream()
                    .filter(e -> (
                                         e.getValue().getState() == State.RUNNING)
                                 || (e.getValue().getState() == State.DELAYED)
                                 || (e.getValue().getState() == State.SUSPENDED))
                    .filter(e -> e.getValue()
                            .getDate()
                            .plus(maxDelayOfStateMessages)
                            .isBefore(instant))
                    .map(e -> e.getValue())
                    .collect(Collectors.toList());
            long[] reviving = { 0L};
            if (statesToRevive.size() <= beans.getContainer().getConfiguration().getMaxRecordsToReadByRandomAccess()) {
                statesToRevive.stream().forEach(s -> {
                    logger.info("E: {} Reviver Reading job for state: {}", beans.getEngine().getName(), s);
                    TransportImpl transport = beans.getReceiver().readJob(s);
                    if(transport != null) {
                        logger.info("E: {} Reviving: {}", beans.getEngine().getName(), transport.jobData());
                        beans.getSender().send(transport);
                        reviving[0]++;
                    }
                    else {
                        logger.error("E: {} No job found anymore for jobstate  {}", beans.getEngine().getName(), s);
                        beans.getJobDataStates().remove(s.getId());
                    }
                });
            } else {

                Map<Integer, List<Pair<Integer, Long>>> coordinates = statesToRevive
                        .stream()
                        .map(s -> Pair.of(s.getPartition(), s.getOffset()))
                        .collect(Collectors.groupingBy(Pair::getLeft));
                List<Triple<Integer, Long, Long>> minMaxPairs =
                        coordinates
                        .entrySet()
                        .stream()
                        .map(e -> Triple.of(e.getKey(),
                                e.getValue().stream().map(Pair::getRight).min(Long::compare).get(),
                                e.getValue().stream().map(Pair::getRight).max(Long::compare).get()))
                        .collect(Collectors.toList());
                List<TransportImpl> jobs = beans.getReceiver().readJobs(minMaxPairs, coordinates);
                jobs.forEach(j -> {
                    logger.info("E: {} Reviving: {}", beans.getEngine().getName(), j.jobData());
                    beans.getSender().send(j);
                    beans.getJobDataStates().remove(j.jobData().getId());
                    reviving[0]++;
                });
            }
            detectAndReviveGroups();
            if (reviving[0] != 0) {
                logger.warn("Revived {} records", reviving[0]);
            }
        }
        logger.info("E: {} call end", beans.getEngine().getName());
    }

    private void detectAndReviveGroups() {
        beans.getStatesByGroup().entrySet().forEach(e -> {
            if(doShutDown()) {
                Thread.currentThread().interrupt();
            }
            Queue<String> l = e.getValue();
            if(l != null) {
                Optional<String> j = l.stream()
                        .min((j1, j2) -> compareGroupJobs(j1, j2));
                if (j.isPresent()) {
                    JobDataState s = beans.getJobDataStates().get(j.get());
                 if (s.getState() == GROUP
                    && s.getDate().plus(beans.getContainer()
                                 .getConfiguration().getMaxDelayOfStateMessages())
                             .isBefore(beans.getContainer().getClock().instant())) {
                        // state of highest prior job is yet GROUP, so now start it
                        logger.warn("Reviving group {} using job: {}", s.getGroupId(), s.getId());
                        TransportImpl job = beans.getReceiver().readJob(s);
                        if(job != null) {
                            beans.getJobTools().prepareJobDataForRunning(job.jobData());
                            beans.getSender().send(job);
                        }
                    }
                }
            }
        });
    }

    public void setStateInitCompleted(final boolean stateInitCompleted) {
        this.stateInitCompleted = stateInitCompleted;
    }

    @Override
    public void shutdown() {
        super.setShutDown();
        logger.info("E: {} shutdown", beans.getEngine().getName());
        reviverClaimed = false;
        waitForThreads(jobStateReceiverThread);
        logger.info("E: {} end of shutdown", beans.getEngine().getName());
    }


    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(jobStateReceiverThread);
    }


}

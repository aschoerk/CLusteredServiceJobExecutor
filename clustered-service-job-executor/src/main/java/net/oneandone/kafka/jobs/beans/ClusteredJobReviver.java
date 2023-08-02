package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.GROUP;
import static net.oneandone.kafka.jobs.api.State.RUNNING;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
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
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

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
        consumerConfig.put(GROUP_ID_CONFIG, beans.getNode().getUniqueNodeId());
        this.jobStateReceiverThread = beans.getContainer().submitInLongRunningThread(() -> {
            initThreadName("State-Receiver");
            consumerConfig.put(GROUP_ID_CONFIG, beans.getNode().getUniqueNodeId());
            consumerConfig.put("max.poll.records", beans.getContainer().getConfiguration().getMaxPollJobDataRecords());
            receiveJobState(consumerConfig);
        });

        reviverClaimed = true;
        logger.info("E: {} startup done", beans.getEngine().getName());
    }

    private void receiveJobState(final Map<String, Object> consumerConfig) {
        logger.info("Starting to receive Jobstates using group {}", consumerConfig.get(GROUP_ID_CONFIG));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            final String jobStateTopicName = beans.getContainer().getJobStateTopicName();
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
                    }
                }
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    if(currentEndOffsets.containsKey(r.partition())) {
                        if(currentEndOffsets.get(r.partition()) <= r.offset()) {
                            currentEndOffsets.remove(r.partition());
                        }
                    }
                    JobDataState jobDataState = JsonMarshaller.gson.fromJson(json, JobDataState.class);
                    logger.trace("Received state {} for job: {} groupId: {} from {}", jobDataState.getState(),
                            jobDataState.getId(), jobDataState.getGroupId(), jobDataState.getSender());
                    beans.getJobDataStates().put(jobDataState.getId(), jobDataState);
                    if((jobDataState.getGroupId() != null) && isGroupRelevantState(jobDataState.getState())) {
                        handleGroupJob(jobDataState);
                    }
                }
                synchronized (this) {
                    this.notifyAll();
                }
            }
        }
    }

    private boolean isGroupRelevantState(final State state) {
        return (state == GROUP) || (state == ERROR) || (state == DONE);
    }

    private void handleGroupJob(final JobDataState jobDataState) {
        if(jobDataState.getState() == GROUP) {
            handleNewGroupedJob(jobDataState);
        }
        else {
            if((jobDataState.getState() == DONE) || (jobDataState.getState() == ERROR)) {
                handleEndOfGroupedJob(jobDataState);
            }
        }
    }

    private void handleEndOfGroupedJob(final JobDataState jobDataState) {
        logger.trace("E: {} done group {} record {}", beans.getEngine().getName(), jobDataState.getGroupId(), jobDataState.getId());
        Queue<JobDataState> statesForThisGroup = beans.getStatesByGroup().get(jobDataState.getGroupId());
        if((statesForThisGroup != null) && statesForThisGroup.contains(jobDataState)) {
            synchronized (beans) {
                logger.trace("removing group {} record {}", jobDataState.getGroupId(),
                        jobDataState.getId());
                if(!statesForThisGroup.remove(jobDataState)) {
                    logger.error("but not found");
                }
                if(statesForThisGroup.isEmpty()) { // remove collection from common group map
                    beans.getStatesByGroup().remove(jobDataState.getGroupId());
                } else {
                    if(beans.getGroupJobsResponsibleFor().contains(jobDataState.getId())) {
                        beans.getGroupJobsResponsibleFor().remove(jobDataState.getId());
                        Optional<JobDataState> nextOneState = statesForThisGroup.stream()
                                .min((i, j) -> i.getCreatedAt().compareTo(j.getCreatedAt()));
                        if(nextOneState.isPresent()) {
                            TransportImpl nextOne = beans.getReceiver().readJob(nextOneState.get());
                            beans.getJobTools().prepareJobDataForRunning(nextOne.jobData());
                            beans.getSender().send(nextOne);
                        }
                    }
                }
            }
        }
    }

    private void handleNewGroupedJob(final JobDataState jobDataState) {
        logger.trace("Adding group job {} groupId: {}", jobDataState.getId(), jobDataState.getGroupId());
        Queue<JobDataState> statesForThisGroup = beans.getStatesByGroup().get(jobDataState.getGroupId());
        synchronized (beans) {
            statesForThisGroup = beans.getStatesByGroup().get(jobDataState.getGroupId());
            if(statesForThisGroup == null) {
                statesForThisGroup = new ConcurrentLinkedQueue<>();
                statesForThisGroup.add(jobDataState);
                beans.getStatesByGroup().put(jobDataState.getGroupId(), statesForThisGroup);
            }
            else {
                statesForThisGroup.add(jobDataState);
            }
        }
        if(beans.getGroupJobsResponsibleFor().contains(jobDataState.getId())) {
            beans.getGroupJobsResponsibleFor().remove(jobDataState.getId());
            if(statesForThisGroup.size() == 1) {
                if (!statesForThisGroup.element().getId().equals(jobDataState.getId())) {
                    logger.error("Logical error when starting group job {} groupId: {}", jobDataState.getId(), jobDataState.getGroupId());
                }
                TransportImpl context = beans.getReceiver().readJob(jobDataState);
                beans.getJobTools().prepareJobDataForRunning(context.jobData());
                logger.trace("Starting grouped Job: {} GroupId: {}", jobDataState.getId(), jobDataState.getGroupId());
                beans.getSender().send(context);
            }
        }
    }

    /**
     * identify and restart stale jobs
     */
    @Override
    public void call() {
        logger.warn("E: {} call", beans.getEngine().getName());
        if (stateInitCompleted) {
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
            if (statesToRevive.size() < 10) {
                statesToRevive.stream().forEach(s -> {
                    logger.info("E: {} Reviver Reading job for state: {}", beans.getEngine().getName(), s);
                    TransportImpl jobData = beans.getReceiver().readJob(s);
                    if(jobData != null) {
                        logger.info("E: {} Reviving: {}", beans.getEngine().getName(), jobData.jobData());
                        beans.getSender().send(jobData);
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
                    beans.getJobDataStates().remove(j.jobData().id());
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
            Queue<JobDataState> l = e.getValue();
            if(l != null) {
                Optional<JobDataState> j = l.stream().min((j1, j2) -> j1.getCreatedAt().compareTo(j2.getCreatedAt()));
                if(j.isPresent()
                   && j.get().getCreatedAt().plus(beans.getContainer()
                                .getConfiguration().getReviverPeriod())
                           .isBefore(beans.getContainer().getClock().instant())) {
                    JobDataState currentStateOfGroupedJob = beans.getJobDataStates().get(j.get().getId());
                    if((currentStateOfGroupedJob != null) && (currentStateOfGroupedJob.getState() == GROUP)) {
                        // make sure you get all eleated information of this group from all partitions
                        JobDataState currentStateOfGroupedJob2 = beans.getJobDataStates().get(j.get().getId());
                        if((currentStateOfGroupedJob2 != null) && (currentStateOfGroupedJob2.getState() == GROUP)) {
                            logger.info("Reviving group {} using job: {}", currentStateOfGroupedJob2.getGroupId(), currentStateOfGroupedJob2.getId());
                            TransportImpl job = beans.getReceiver().readJob(currentStateOfGroupedJob2);
                            if(job != null) {
                                beans.getJobTools().prepareJobDataForRunning(job.jobData());
                                beans.getSender().send(job);
                            }
                        }
                    }
                }
            }
        });
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

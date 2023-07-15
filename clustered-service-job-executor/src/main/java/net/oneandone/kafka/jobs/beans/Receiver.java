package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.GROUP;
import static net.oneandone.kafka.jobs.api.State.RUNNING;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;


/**
 * @author aschoerk
 */
public class Receiver extends StoppableBase {

    static final String GROUP_ID = "ClusteredServiceJobExecutor";
    private final Thread jobStateReceiverThread;
    private Thread stateInitWaitingThread;

    private Thread jobDataReceiverThread;
    private boolean stateInitCompleted = false;
    private final Queue<Future<?>> groupWaitingFutures = new LinkedList<>();

    private AtomicReferenceArray<JobDataState> lastOfPartition;
    private Map<String, TransportImpl> sentRunning = new ConcurrentHashMap<>();

    public Receiver(Beans beans) {
        super(beans);

        final Map<String, Object> consumerConfig = getConsumerConfig(beans);
        // jobStates must be received by all nodes.
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
        this.jobStateReceiverThread = beans.getContainer().createThread(() -> {
            initThreadName("State-Receiver");
            consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
            receiveJobState(consumerConfig);
        });
        this.jobStateReceiverThread.start();
        stateInitWaitingThread = beans.getContainer().createThread(() -> {
            initThreadName("StateInitWaiter");
            try {
                while (!stateInitCompleted) {
                    Thread.sleep(100);
                }
                logger.info("Receiver stateInitCompleted");
                Receiver.this.stateInitWaitingThread = null;
                this.jobDataReceiverThread = beans.getContainer().createThread(() -> {
                    initThreadName("Data-Receiver");
                    receiveJobData(getConsumerConfig(beans));
                });
                this.jobDataReceiverThread.start();
                logger.info("Receiver job Data receiving started");
                beans.getReviver().initRevival();
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        });
        stateInitWaitingThread.start();

        Map<String, Object> singleJobConsumerConfig = getConsumerConfig(beans);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
    }

    private static Map<String, Object> getConsumerConfig(final Beans beans) {
        final Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, beans.getContainer().getBootstrapServers());
        consumerConfig.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 100);
        consumerConfig.put(MAX_POLL_INTERVAL_MS_CONFIG, 5000);
        consumerConfig.put(GROUP_ID_CONFIG, GROUP_ID);
        consumerConfig.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return consumerConfig;
    }

    static JobDataState getLatestChange(final Beans beans) {
        JobDataState latestGroupChange = null;
        AtomicReferenceArray<JobDataState> latestStates = beans.getReceiver().getLastOfPartion();
        for (int i = 0; i < latestStates.length(); i++) {
            JobDataState act = latestStates.get(i);
            if (latestGroupChange == null) {
                latestGroupChange = act;
            } else {
                if (act != null && act.getSent().isAfter(latestGroupChange.getSent())) {
                    latestGroupChange = act;
                }
            }
        }
        return latestGroupChange;
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(jobDataReceiverThread, jobStateReceiverThread, stateInitWaitingThread);
    }

    private void receiveJobState(final Map<String, Object> consumerConfig) {
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

            this.lastOfPartition = new AtomicReferenceArray<>(topicPartitions.size());

            // consumer.subscribe(Collections.singleton(jobStateTopicName));
            while (!doShutDown()) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if(currentEndOffsets.isEmpty() || records.isEmpty()) {
                    this.stateInitCompleted = true;
                }
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    if(currentEndOffsets.containsKey(r.partition())) {
                        if(currentEndOffsets.get(r.partition()) <= r.offset()) {
                            currentEndOffsets.remove(r.partition());
                        }
                    }
                    JobDataState jobDataState = JsonMarshaller.gson.fromJson(json, JobDataState.class);
                    lastOfPartition.set(jobDataState.getPartition(), jobDataState);
                    logger.trace("Received state {} for job: {} groupId: {} from {}", jobDataState.getState(),
                            jobDataState.getId(), jobDataState.getGroupId(), jobDataState.getSender());
                    beans.getJobDataStates().put(jobDataState.getId(), jobDataState);
                    if (jobDataState.getState() == RUNNING && sentRunning.containsKey(jobDataState.getId())) {
                        TransportImpl runningJob = sentRunning.remove(jobDataState.getId());
                        if (jobDataState.getSender().equals(beans.getContainer().getConfiguration().getNodeName())) {
                            boolean res = false;
                            try {
                                res = beans.getQueue().offer(runningJob, 100, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {

                            }
                            if(!res) {
                                beans.getExecutor().delayJob(runningJob, "Not scheduled in internal queue");
                                beans.getSender().send(runningJob);
                            }
                        } else  {
                            logger.warn("Ignored own state RUNNING group: {} id: {} because another node was first", jobDataState.getGroupId(), jobDataState.getId());
                        }
                    }
                    if(jobDataState.getCorrelationId() != null) {
                        JobDataState existingJobDataState = beans.getJobDataCorrelationIds().get(jobDataState.getCorrelationId());
                        if(!existingJobDataState.getId().equals(jobDataState.getId())) {
                            logger.error("different Jobs {}, {} with the same correlationId {}", jobDataState, existingJobDataState, jobDataState.getCorrelationId());
                        }
                        beans.getJobDataCorrelationIds().put(jobDataState.getCorrelationId(), jobDataState);
                    }
                    if((jobDataState.getGroupId() != null) && isGroupRelevantState(jobDataState.getState())) {
                        handleGroupedJobs(jobDataState);
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

    private void handleGroupedJobs(final JobDataState jobDataState) {
        groupWaitingFutures.removeIf(f -> f.isDone());
        if (beans.getGroupJobsResponsibleFor().contains(jobDataState.getId())) {
            groupWaitingFutures.add(beans.getContainer().submitInThread(() -> {
                initThreadName("PartitionWaiter");
                waitForStatePartitions(jobDataState);
                handleGroupJob(jobDataState);
            }));
        } else {
            handleGroupJob(jobDataState);
        }

    }

    public void waitForStatePartitions(final JobDataState jobDataState) {

        int loops = 0;
        boolean enoughInfoFromOther;
        do {
            logger.trace("wait for other partitions for {}", jobDataState);
            enoughInfoFromOther = true;
            for (int i = 0; (i < lastOfPartition.length()) && enoughInfoFromOther; i++) {
                enoughInfoFromOther =
                        (lastOfPartition.get(i) != null)
                        && !lastOfPartition.get(i).getSent().isBefore(jobDataState.getSent());
            }
            synchronized (this) {
                try {
                    this.wait(1000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                    break;
                }
            }
            loops++;
        } while(!enoughInfoFromOther && (loops < (beans.getContainer().getConfiguration().getMaxTimeToWaitForOtherPartitions().toMillis() / 1000)));

        logger.trace("handleGroupedJobs looped {} waiting for other partition id: {} group: {}", loops,
                jobDataState.getId(), jobDataState.getGroupId());
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
        logger.trace("done group {} record {}", jobDataState.getGroupId(), jobDataState.getId());
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
                TransportImpl context = readJob(jobDataState);
                beans.getJobTools().prepareJobDataForRunning(context.jobData());
                logger.trace("Starting grouped Job: {} GroupId: {}", jobDataState.getId(), jobDataState.getGroupId());
                beans.getSender().send(context);
            }
        }
    }

    private void receiveJobData(final Map<String, Object> syncingConsumerConfig) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(syncingConsumerConfig)) {
            consumer.subscribe(Collections.singleton(beans.getContainer().getJobDataTopicName()));
            while (!doShutDown()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> r : records) {
                    TransportImpl context = evaluatePackage(r);

                    beans.getSender().sendState(context.jobData(), r);
                    switch (context.jobData().state()) {
                        case RUNNING:
                            this.sentRunning.put(context.jobData().id(), context);

                            break;
                        case DELAYED:
                            beans.getPendingHandler().schedulePending(context);
                            // this node got the task to reschedule the job as soon as date is
                            break;
                        case ERROR:
                            break;
                    }

                }
            }
        }
    }

    private boolean preventRunningWithGroup(final TransportImpl context) {
        if((context.jobData().state() == RUNNING) && (context.jobData().groupId() != null)) {
            if(beans.getStatesByGroup().containsKey(context.jobData().groupId())) {
                Collection<JobDataState> statesByGroup = beans.getStatesByGroup().get(context.jobData().groupId());
                if((statesByGroup.size() > 0) && !statesByGroup.stream().anyMatch(s -> s.getId().equals(context.jobData().id()))) {
                    if(statesByGroup.stream().anyMatch(s -> s.getCreatedAt().isBefore(context.jobData().createdAt()))) {
                        context.jobData().setState(GROUP);
                        beans.getSender().send(context);
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private TransportImpl evaluatePackage(final ConsumerRecord<String, String> r) {
        String json = r.value();
        String[] resultString = json.split(Sender.SEPARATORREGEX);
        JobDataImpl jobData = JsonMarshaller.gson.fromJson(resultString[0], JobDataImpl.class);
        TransportImpl context = new TransportImpl(jobData, resultString[1], beans);
        if(resultString.length == 3) {
            context.setResumeData(resultString[2]);
        }
        return context;
    }

    public TransportImpl readJob(final JobDataState state) {
        Map<String, Object> consumerConfig = getConsumerConfig(beans);
        consumerConfig.put(MAX_POLL_RECORDS_CONFIG, 1);
        consumerConfig.put(GROUP_ID_CONFIG, beans.getContainer().getConfiguration().getNodeName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerConfig)) {
            TopicPartition topicPartition = new TopicPartition(beans.getContainer().getJobDataTopicName(), state.getPartition());
            consumer.assign(Collections.singletonList(topicPartition));
            consumer.seek(topicPartition, state.getOffset());
            boolean found = false;
            do {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                found = !records.isEmpty();
                Optional<ConsumerRecord<String, String>> optionalR = records.records(topicPartition).stream().filter(r -> r.offset() == state.getOffset()).findAny();
                if(optionalR.isPresent()) {
                    TransportImpl context = evaluatePackage(optionalR.get());
                    return context;
                }
            } while (found && !doShutDown());
        }
        return null;
    }


    public AtomicReferenceArray<JobDataState> getLastOfPartion() {
        return lastOfPartition;
    }
}

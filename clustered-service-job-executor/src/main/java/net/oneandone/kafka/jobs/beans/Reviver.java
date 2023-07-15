package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.GROUP;

import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class Reviver extends StoppableBase {

    private final Random random = new Random(System.nanoTime());
    private Thread reviverThread;
    private Thread groupsReviverThread;

    public Reviver(final Beans beans) {
        super(beans);
    }

    public void initRevival() {
        reviverThread = beans.getContainer().createThread(() -> run());
        reviverThread.start();
        logger.info("Revival init completed");
        setRunning(true);
    }

    void run() {
        try {
            initThreadName("Revival");
            while (!doShutDown()) {
                logger.info("check states size: {}", beans.getJobDataStates().size());
                List<JobDataState> statesToRevive = beans.getJobDataStates()
                        .entrySet().stream()
                        .filter(e -> (
                                             e.getValue().getState() == State.RUNNING)
                                     || (e.getValue().getState() == State.DELAYED)
                                     || (e.getValue().getState() == State.SUSPENDED))
                        .filter(e -> e.getValue()
                                .getDate()
                                .plus(
                                        beans.getContainer().getConfiguration().getMaxDelayOfStateMessages())
                                .isBefore(beans.getContainer().getClock().instant()))
                        .map(e -> e.getValue())
                        .collect(Collectors.toList());
                statesToRevive.stream().forEach(s -> {
                    logger.info("Reading job for state: {}", s);
                    TransportImpl jobData = beans.getReceiver().readJob(s);
                    if(jobData != null) {
                        logger.info("Reviving: {}", jobData.jobData());
                        beans.getSender().send(jobData);
                    }
                    else {
                        logger.error("No job found anymore for jobstate  {}", s);
                        beans.getJobDataStates().remove(s.getId());
                    }
                });
                try {
                    long minimalPeriod = beans.getContainer().getConfiguration().getReviverPeriod().toMillis();
                    minimalPeriod += beans.getExecutor().randomizedPeriod(minimalPeriod);
                    logger.info("Wait for {} milliseconds", minimalPeriod);
                    synchronized (Reviver.this) {
                        this.wait(minimalPeriod);
                    }
                    logger.info("Ready Wait for {} milliseconds", minimalPeriod);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
                detectAndReviveGroups();
            }
        } finally {
            setRunning(false);
        }
        logger.info("Ready");
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
        this.waitForThreads(groupsReviverThread);
        while (isRunning()) {
            synchronized (this) {
                this.notify();
            }
        }
        logger.info("Reviver shutdown");
    }

    public void initGroups(final JobDataState[] lastOfPartition) {
        groupsReviverThread = beans.getContainer().createThread(() -> {
            initThreadName("GroupReviver");
            try {
                Thread.sleep(1000 + (random.nextLong() % 1000));
            } catch (InterruptedException e) {
                logger.info("GroupReviver interrupted");
            }
            if(!Thread.interrupted()) {
                detectAndReviveGroups();
            }
            this.groupsReviverThread = null;
        });
        groupsReviverThread.start();
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
                        JobDataState latestChange = Receiver.getLatestChange(beans);
                        // the job is not handled yet

                        if(latestChange != null) {
                            beans.getReceiver().waitForStatePartitions(latestChange);
                        }
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

}

package net.oneandone.kafka.jobs.beans;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataState;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 * @author aschoerk
 */
public class Reviver extends StoppableBase {

    private Thread reviverThread;

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
                List<JobDataState> statesToResurrect = beans
                        .getJobDataStates()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toList())
                        .stream()
                        .filter(e -> e.getValue().getState() == State.RUNNING || e.getValue().getState() == State.DELAYED || e.getValue().getState() == State.SUSPENDED)
                        .filter(e ->
                                e.getValue()
                                        .getDate()
                                        .plus(beans.getContainer().getConfiguration().getMaxDelayOfStateMessages())
                                        .isBefore(beans.getContainer().getClock().instant())
                        ).map(e -> e.getValue()).collect(Collectors.toList());
                statesToResurrect.stream().forEach(s -> {
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
                        }
                );
                try {
                    long minimalPeriod = beans.getContainer().getConfiguration().getReviverPeriod().toMillis();
                    minimalPeriod = minimalPeriod
                                    + beans.getExecutor().randomizedPeriod(minimalPeriod);
                    logger.info("Wait for {} milliseconds", minimalPeriod);
                    synchronized (Reviver.this) {
                        this.wait(minimalPeriod);
                    }
                    logger.info("Ready Wait for {} milliseconds", minimalPeriod);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            }
        } finally {
            setRunning(false);
        }
        logger.info("Ready");
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
        while (isRunning()) {
            synchronized (this) {
                this.notify();
            }
        }
        logger.info("Reviver shutdown");
    }
}

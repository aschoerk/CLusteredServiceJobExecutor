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

    static Random random = new Random();

    private Thread reviverThread;

    public Reviver(final Beans beans) {
        super(beans);
    }

    public void initResurrection() {
        reviverThread = beans.getContainer().createThread(() -> run());
        reviverThread.start();
    }

    void run() {
        while (!doShutDown()) {
            List<JobDataState> statesToResurrect = beans
                    .getJobDataStates()
                    .entrySet()
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
                        if (jobData != null) {
                            logger.info("Reviving: {}", jobData.jobData());
                            beans.getSender().send(jobData);
                        } else {
                            logger.error("No job found anymore for jobstate  {}",s);
                        }
                    }
            );
            try {
                final long minimalPeriod = beans.getContainer().getConfiguration().getReviverPeriod().toMillis();
                Thread.sleep(minimalPeriod
                             + beans.getExecutor().randomizedPeriod(minimalPeriod));
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
        }
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
        super.waitForThreads(reviverThread);
    }
}

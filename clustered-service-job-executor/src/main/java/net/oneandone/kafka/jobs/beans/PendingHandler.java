package net.oneandone.kafka.jobs.beans;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataState;

/**
 * @author aschoerk
 */
public class PendingHandler extends StoppableBase {
    private final SortedSet<TransportImpl> sortedPending = Collections.synchronizedSortedSet(new TreeSet<>(new TimestampComparator()));

    private final Map<String, TransportImpl> pendingByIdentifier = Collections.synchronizedMap(new HashMap<>());

    private int defaultWaitMillis = 10000;

    final Thread pendingHandlerThread;

    public static class TimestampComparator implements Comparator<TransportImpl> {
        @Override
        public int compare(final TransportImpl o1, final TransportImpl o2) {
            int result = o1.jobData().date().compareTo(o2.jobData().date());
            return result != 0 ? result : o1.jobData().id().compareTo(o2.jobData().id());
        }
    }

    public PendingHandler(final Beans beans) {
        super(beans);
        pendingHandlerThread = beans.getContainer().createThread(
                () -> run()
        );
        pendingHandlerThread.start();
    }

    /**
     * <b>The</b> scheduling function
     * @param e the pendingEntry to be scheduled
     */
    public void schedulePending(final TransportImpl e) {
        logger.info("Node: {} Scheduling JobData: {} in {} milliseconds",
                beans.getContainer().getConfiguration().getNodeName(),
                e.jobData(),
                Duration.between(Instant.now(),e.jobData().date()).toMillis());
        removePending(e.jobData().id(), false);
        pendingByIdentifier.put(e.jobData().id(), e);
        sortedPending.add(e);
        synchronized (this) {
            if(sortedPending.first().equals(e)) {
                this.notify();
            }
        }
    }

    /**
     * Remove a pendingEntry, if enfore is true log an error if it is gnerally found but no entry is currently scheduled
     * @param jobDataId the identifier of the entry to be removed
     * @param enforce if true log an error if it is gnerally found but no entry is currently scheduled
     */
    private void removePending(final String jobDataId, boolean enforce) {
        logger.info("Removing pending {}", jobDataId);
        TransportImpl e = pendingByIdentifier.get(jobDataId);
        pendingByIdentifier.remove(jobDataId);
        if(e != null) {
            boolean result = sortedPending.remove(e);
            if (!result && enforce) {
                logger.error("Could not remove pending {} ", e.jobData().id());
            }
        }
    }

    public void run() {
        initThreadName("PendingHandler");
        setRunning();
        try {
            while (!doShutDown()) {
                loopBody();
            }
        } finally {
            setRunning(false);
        }
    }

    void loopBody() {
        selectAndExecute();
        Duration toWait;
        toWait = determineWaitTime();
        waitOrAcceptNotify(toWait);
    }

    private void waitOrAcceptNotify(final Duration toWait) {
        if(!toWait.isNegative()) {
            try {
                long toWaitTime = toWait.toMillis();
                if (toWaitTime > 500) {
                    toWaitTime = 500;
                }
                if (sortedPending.size() > 0) {
                    logger.info("Waiting for notify or {} milliseconds next entry {} jobdata: {}", toWaitTime, sortedPending.first().context(), sortedPending.first().jobData());
                }
                if (toWaitTime > 0) {
                    synchronized (this) {
                        this.wait(toWaitTime);
                    }
                }
            } catch (InterruptedException e) {
                if (!doShutDown())
                    logger.error("PendingHandler N: {} got interrupted {}", beans.getContainer().getConfiguration().getNodeName(), e);
                else
                    logger.info("PendingHandler N: {} got interrupted {}", beans.getContainer().getConfiguration().getNodeName(), e);
            }
        }
    }

    private Duration determineWaitTime() {
        Duration toWait;
        if(sortedPending.size() > 0) {
            TransportImpl nextTask = sortedPending.first();
            toWait = Duration.between(beans.getContainer().getClock().instant(), nextTask.jobData().date()).plusMillis(1);
        }
        else {
            toWait = Duration.ofMillis(defaultWaitMillis);
        }
        return toWait;
    }

    private void selectAndExecute() {
        while (sortedPending.size() > 0 && !sortedPending.first().jobData().date().isAfter(beans.getContainer().getClock().instant())) {
            TransportImpl pendingTask = sortedPending.first();
            sortedPending.remove(pendingTask);
            logger.info("Found Pending: {}", pendingTask.jobData());
            try {
                switch (pendingTask.jobData().state()) {
                    case DELAYED:
                        beans.getMetricCounts().incWokenUpDelayed();
                        if (beans.getJobDataStates().containsKey(pendingTask.jobData().id())) {
                            JobDataState jobDataState = beans.getJobDataStates().get(pendingTask.jobData().id());
                            if (jobDataState.getState() == State.DELAYED && jobDataState.getDate().equals(pendingTask.jobData().date())) {
                                beans.getJobTools().prepareJobDataForRunning(pendingTask.jobData());
                                beans.getSender().send(pendingTask);
                            } else {
                                logger.warn("Woken up Delayed {} but state not fitting {}",pendingTask.jobData(), jobDataState);
                            }
                        }
                        break;

                }
            } catch (Throwable t) {
                logger.error(String.format("Executing PendingTask: %s Exception:", pendingTask.jobData().id()), t);
            }
        }
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(pendingHandlerThread);
        waitForStoppables(this);
        sortedPending.stream().collect(Collectors.toList()).forEach(p -> beans.getSender().send(p));
    }
}

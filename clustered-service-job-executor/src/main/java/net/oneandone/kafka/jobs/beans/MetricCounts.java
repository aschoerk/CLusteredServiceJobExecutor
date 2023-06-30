package net.oneandone.kafka.jobs.beans;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import net.oneandone.kafka.jobs.api.State;

public class MetricCounts extends StoppableBase {

    public MetricCounts(Beans beans) {
        super(beans);
    }

    public static final String CDBJOBS_METRIC_UNIT = "cdbjobs";
    private final AtomicLong running = new AtomicLong(0);
    private final AtomicLong startup = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong stopped = new AtomicLong(0);
    private final AtomicLong createdBatches = new AtomicLong(0);
    private final AtomicLong errorBatches = new AtomicLong(0);
    private final AtomicLong resumedBatches = new AtomicLong(0);
    private final AtomicLong doneBatches = new AtomicLong(0);
    private final AtomicLong suspendedBatches = new AtomicLong();
    private final AtomicLong lastSearchJobsDelayed = new AtomicLong(0);
    private final AtomicLong delayedFromQueue = new AtomicLong(0);
    private final AtomicLong stateChangeError = new AtomicLong();
    private final AtomicLong stateChangeDelay = new AtomicLong();
    private final AtomicLong stateChangeWaiting = new AtomicLong();
    private final AtomicLong wokenUpRunning = new AtomicLong();
    private final AtomicLong wokenUpDelayed = new AtomicLong();
    private final AtomicLong wokenUpWaiting = new AtomicLong();
    private final List<Integer> batchSizes = new ArrayList<>();

    private Map<State, Long> jobCountsPerState = new ConcurrentHashMap<>();

    public Map<State, Long> getJobCountsPerState() {
        return jobCountsPerState;
    }

    private Long getCountForState(final State state) {
        final Long res = jobCountsPerState.get(state);
        return res != null ? res : 0L;
    }

    public void addBatchSize(int batchSize) {
        synchronized (batchSizes) {
            batchSizes.add(batchSize);
        }
    }

    /**
     * number of currently running jobs
     */
    public Long incRunning(int by) {
        return running.addAndGet(by);
    }

    public Long decRunning(int by) {
        return running.addAndGet(-by);
    }

    /**
     * timestamp of startup of this engine
     */
    public Long getStartup() {
        return startup.get();
    }

    /**
     * number of steps handled
     */ public Long incStopped() {
        return stopped.incrementAndGet();
    }

    /**
     * number of continuous stephandlings done (as long as steps return DONE, there is no rescheduling)
     */ public Long incCreated() {
        return createdBatches.incrementAndGet();
    }


    /**
     * number of times stephandlings resulted in ERROR
     */ public Long incInError() {
        return errorBatches.incrementAndGet();
    }

    /**
     * number of times stephandlings resulted in DELAY
     */ public Long incResumed() {
        return resumedBatches.incrementAndGet();
    }

    /**
     * number of times stephandlings resulted in STOP
     */ public Long incDone() {
        return doneBatches.incrementAndGet();
    }

    /**
     * number of times stephandlings resulted in STOP
     */ public Long incWokenUpRunning() {
        return this.wokenUpRunning.incrementAndGet();
    }

    /**
     * number of times stephandlings resulted in STOP
     */
    public Long incWokenUpDelayed() {
        return this.wokenUpDelayed.incrementAndGet();
    }
    /**
     * number of times stephandlings resulted in STOP
     */
    public Long incWokenUpWaiting() {
        return this.wokenUpWaiting.incrementAndGet();
    }

    /**
     * last time delayed RemoteJobs where searched.
     */
    public AtomicLong getLastSearchJobsDelayed() {
        return lastSearchJobsDelayed;
    }

    public void addDelayedFromQueue(int toAdd) {
        delayedFromQueue.addAndGet((long) toAdd);
    }

    public void addStateChangeDelay(int toAdd) {
        stateChangeDelay.addAndGet((long) toAdd);
    }

    public void addStateChangeWaiting(int toAdd) {
        stateChangeWaiting.addAndGet((long) toAdd);
    }

    public void addStateChangeError(int toAdd) {
        stateChangeError.addAndGet((long) toAdd);
    }

    public void incSuspended() {
        suspendedBatches.incrementAndGet();
    }

    public Long getCreated() {
        return createdBatches.get();
    }

    public void countJobResult(State jobState, String jobName, String stepName) {

    }
}

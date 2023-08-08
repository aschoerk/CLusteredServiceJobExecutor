package net.oneandone.kafka.jobs.beans;

import static java.time.temporal.ChronoUnit.SECONDS;
import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;
import static net.oneandone.kafka.jobs.api.State.ERROR;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.exceptions.UnRecoverable;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.implementations.JobImpl;
import net.oneandone.kafka.jobs.implementations.StepImpl;

/**
 * @author aschoerk
 */
public class Executor extends StoppableBase {

    static Random random = new Random();
    private final List<Future<?>> currentFutures = new LinkedList<>();

    public Executor(final Beans beans) {
        super(beans);
    }

    public boolean executeJob(final TransportImpl element) {
        if (!usedByThread.compareAndSet(false, true)) {
            throw new KjeException("not expected in multipleThreads");
        }
        try {
            final List<Future<?>> toRemove = currentFutures
                    .stream().filter(f -> f.isDone() || f.isCancelled()).collect(Collectors.toList());
            currentFutures.removeAll(toRemove);
            if(currentFutures.size() > beans.getContainer().getConfiguration().maxPendingJobsPerNode()) {
                return false;
            }
            else {
                try {
                    Future<?> future = beans.getContainer().submitInThread(
                            () -> processStep(element)
                    );
                    if(future.isCancelled()) {
                        logger.error("Future is cancelled");
                    }
                    currentFutures.add(future);
                    return true;
                } catch (RejectedExecutionException e) {
                    return false;
                }
            }
        } finally {
            if (!usedByThread.compareAndSet(true, false)) {
                throw new KjeException("Not expected in multiple threads");
            }
        }
    }

    @Override
    public void setShutDown() {
        super.setShutDown();
    }

    public long randomizedPeriod(long maxWaitTime) {
        if(maxWaitTime != 0) {
            return random.nextLong() % maxWaitTime;
        }
        else {
            return 0;
        }
    }

    boolean thereIsANewStep(JobData jobData, int stepInc) {
        JobImpl<?> job = beans.getJobs().get(jobData.jobSignature());
        final int newStep = jobData.step() + stepInc;
        if((newStep < 0) || (newStep > job.steps().length)) {
            throw new UnRecoverable(String.format("Trying to execute invalid step %d in Job %s by increment: %d", newStep, job.name(), stepInc));
        }
        return (newStep >= 0) && (newStep <= (job.steps().length - 1));
    }

    TransportImpl stopJob(TransportImpl context) {
        logger.trace("Stopping {}", context.jobData());
        JobDataImpl jobData = context.jobData();
        logger.info("Stop Job({}): {}/{}", jobData.id(), beans.getJobs().get(jobData.jobSignature()).name(), jobData.step());
        jobData.setDate(beans.getContainer().getClock().instant());
        beans.getJobTools().changeStateTo(jobData, DONE);
        return null;
    }

    void errorJob(TransportImpl context, String error) {
        logger.error("Error cause: {} job: {}", error, context.jobData());
        final JobDataImpl jobData = context.jobData();
        jobData.addError(beans.getContainer().getClock().instant(), null, error);
        beans.getJobTools().changeStateTo(jobData, ERROR);
    }

    void delayJob(TransportImpl context, String error) {
        logger.info("Delaying cause: {} job: {}", error, context.jobData());
        final JobDataImpl jobData = context.jobData();
        int retries = jobData.retries();
        final Configuration configuration = beans.getContainer().getConfiguration();
        if(retries < (configuration.getMaxPhase1Tries() + configuration.getMaxPhase2Tries())) {
            retries++;
            beans.getJobTools().changeStateTo(jobData, DELAYED);
            long time;
            long minimalWaitTimePhase1;
            if(retries < configuration.getMaxPhase1Tries()) {
                minimalWaitTimePhase1 = configuration.getInitialWaitTimePhase1().multipliedBy((long) (Math.pow(2, retries - 1))).toSeconds();
                time = minimalWaitTimePhase1 + randomizedPeriod(minimalWaitTimePhase1);
            }
            else {
                minimalWaitTimePhase1 = configuration
                        .getInitialWaitTimePhase1()
                        .multipliedBy((long) (Math.pow(2, configuration.getMaxPhase1Tries()))).toSeconds();
                final long minimalWaitTimePhase2 =
                        configuration
                                .getInitialWaitTimePhase2()
                                .multipliedBy((long) (Math.pow(2, (retries - configuration.getMaxPhase1Tries())))).toSeconds();
                time = minimalWaitTimePhase1 + minimalWaitTimePhase2 + randomizedPeriod(minimalWaitTimePhase2);
            }
            jobData.setDate(Instant.now(beans.getContainer().getClock()).plus(time, SECONDS));
            jobData.setRetries(retries);
        }
    }

    private <Context> void processStep(final TransportImpl element) {
        initThreadName("Step");
        beans.getContainer().startThreadUsage();
        try {
            final JobDataImpl jobData = element.jobData();
            String signature = jobData.jobSignature();
            StepResult result;
            final int currentStep = jobData.step();
            JobImpl<Context> job = (JobImpl<Context>) beans.getJobs().get(signature);
            if((job == null) ||
               (beans.getRemoteExecutors().thereIsRemoteExecutor(signature) && beans.getContainer().getConfiguration().preferRemoteExecution())) {
                result = beans.getRemoteExecutors().handle(element);
            }
            else {
                StepImpl<Context> step = (StepImpl<Context>) job.steps()[currentStep];
                jobData.incStepCount();
                try {
                    final Context context = (Context) element.getContext(Class.forName(jobData.contextClass()));
                    if(element.resumeData() != null) {
                        result = step.callHandle(beans, jobData, context, element.getResumeData(Class.forName(jobData.resumeDataClass())));
                    }
                    else {
                        result = step.callHandle(beans, jobData, context);
                    }
                    element.setContext(context);
                } catch (ClassNotFoundException cne) {
                    throw new KjeException("Could not get Class: " + cne);
                }
            }
            TransportImpl nextOne = null;
            switch (result.getStepResultEnum()) {
                case DONE:
                    if(thereIsANewStep(jobData, result.getStepIncrement())) {
                        jobData.setStep(currentStep + result.getStepIncrement());
                        jobData.setRetries(0);
                        beans.getJobTools().prepareJobDataForRunning(jobData);
                        beans.getSender().send(element);
                    }
                    else {
                        beans.getMetricCounts().incDone();
                        nextOne = stopJob(element);
                        beans.getSender().send(element);
                    }
                    break;
                case STOP:
                    beans.getMetricCounts().incStopped();
                    nextOne = stopJob(element);
                    beans.getSender().send(element);
                    break;
                case DELAY:
                    delayJob(element, result.getError());
                    beans.getSender().send(element);
                    break;
                case ERROR:
                    beans.getMetricCounts().incInError();
                    errorJob(element, result.getError());
                    beans.getSender().send(element);
                    break;
                default:
                    logger.error("Unsupported Step Result: {}", result);
            }
            if(nextOne != null) {
                logger.info("Starting grouped Job: {}", nextOne.jobData());
                beans.getJobTools().prepareJobDataForRunning(nextOne.jobData());
                beans.getSender().send(nextOne);
            }
        } finally {
            beans.getContainer().stopThreadUsage();
        }
    }

}

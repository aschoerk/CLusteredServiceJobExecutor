package net.oneandone.kafka.jobs.beans;

import static java.time.temporal.ChronoUnit.SECONDS;
import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.exceptions.UnRecoverable;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class Executor extends StoppableBase {


    static Random random = new Random();

    Thread dequer;

    @Override
    public void setShutDown() {
        super.setShutDown();
        waitForThreads(dequer);
    }

    public Executor(final Beans beans) {
        super(beans);
        dequer = beans.getContainer().createThread(() -> {
            initThreadName("Executor");
            setRunning();
            while (!doShutDown()) {
                try {
                    final TransportImpl element = beans.getQueue().pollLast(500, TimeUnit.MILLISECONDS);
                    if (element != null) {
                        Thread thread = beans.getContainer().createThread(
                                () -> processStep(element)
                        );
                        thread.start();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        dequer.start();
    }

    int randomizedPeriod(int maxWaitTime) {
        if (maxWaitTime != 0) {
            return random.nextInt(maxWaitTime);
        } else {
            return 0;
        }
    }
    boolean thereIsANewStep(JobData jobData, int stepInc) {
        JobImpl job = beans.getJobs().get(jobData.jobSignature());
        final int newStep = jobData.step() + stepInc;
        if (newStep < 0 || newStep > job.steps().length) {
            throw new UnRecoverable(String.format("Trying to execute invalid step %d in Job %s by increment: %d", newStep, job.name(), stepInc));
        }
        return newStep >= 0 &&  newStep <= job.steps().length - 1;
    }

    void stopJob(TransportImpl context) {
        logger.info("Stopping {}", context.jobData());
        JobDataImpl jobData = context.jobData();
        logger.info("Stop Job({}): {}/{}", jobData.id(), beans.getJobs().get(jobData.jobSignature()).name(), jobData.step());
        jobData.setDate(beans.getContainer().getClock().instant());
        beans.getJobTools().changeStateTo(jobData, DONE);
    }

    void delayJob(TransportImpl context, String error) {
        logger.info("Delaying cause: {} job: {}", error, context.jobData());
        final JobDataImpl jobData = context.jobData();
        int currentStep = jobData.step();
        int retries = jobData.retries();
        final Configuration configuration = beans.getContainer().getConfiguration();
        if (retries < configuration.getMaxPhase1Tries() + configuration.getMaxPhase2Tries() ) {
            retries++;
            beans.getJobTools().changeStateTo(jobData, DELAYED);
            int time;
            int minimalWaitTimePhase1;
            if (retries < configuration.getMaxPhase1Tries()) {
                minimalWaitTimePhase1 = retries * configuration.getWaitPhase1Seconds();
                time = minimalWaitTimePhase1 + randomizedPeriod(minimalWaitTimePhase1);
            } else {
                minimalWaitTimePhase1 = (configuration.getMaxPhase1Tries() + 1) * configuration.getWaitPhase1Seconds();
                final int minimalWaitTimePhase2 = ((retries - configuration.getMaxPhase1Tries()) + 1) * configuration.getWaitPhase2Seconds();
                time = minimalWaitTimePhase1 + minimalWaitTimePhase2 + randomizedPeriod(minimalWaitTimePhase2);
            }
            jobData.setDate(Instant.now(beans.getContainer().getClock()).plus(time, SECONDS));
            jobData.setRetries(retries);
        }
    }

    private void processStep(final TransportImpl element) {
        initThreadName("Step");
        beans.getContainer().startThreadUsage();
        try {
            final JobDataImpl jobData = element.jobData();
            String signature = jobData.jobSignature();
            StepResult result;
            final int currentStep = jobData.step();
            JobImpl job = beans.getJobs().get(signature);
            if (job == null ||
                beans.getRemoteExecutors().thereIsRemoteExecutor(signature) && beans.getContainer().getConfiguration().preferRemoteExecution()) {
                result = beans.getRemoteExecutors().handle(element);
            } else {
                Step step = job.steps()[currentStep];
                jobData.incStepCount();
                try {
                    if(element.resumeData() != null) {
                        result =  step.handle(element.getContext(Class.forName(jobData.contextClass())), element.getResumeData(Class.forName(jobData.resumeDataClass())));
                    }
                    else {
                        result =  step.handle(element.getContext(Class.forName(jobData.contextClass())));
                    }
                } catch(ClassNotFoundException cne) {
                    throw new KjeException("Could not get Class: " + cne);
                }
            }
            switch (result.getStepResultEnum()) {
                case DONE:
                    if(thereIsANewStep(jobData, result.getStepIncrement())) {
                        jobData.setStep(currentStep + 1);
                        beans.getJobTools().prepareJobDataForRunning(jobData);
                        beans.getSender().send(element);
                    }
                    else {
                        beans.getMetricCounts().incDone();
                        stopJob(element);
                        beans.getSender().send(element);
                    }
                    break;
                case STOP:
                    beans.getMetricCounts().incStopped();
                    stopJob(element);
                    beans.getSender().send(element);
                    break;
                case DELAY:
                    delayJob(element, result.getError());
                    beans.getSender().send(element);
                    break;
                case ERROR:
                    beans.getMetricCounts().incInError();
                    beans.getSender().send(element);
                    break;
                default:
                    logger.error("Unsupported Step Result: ", result);
            }
        } finally {
            beans.getContainer().stopThreadUsage();
        }
    }

}

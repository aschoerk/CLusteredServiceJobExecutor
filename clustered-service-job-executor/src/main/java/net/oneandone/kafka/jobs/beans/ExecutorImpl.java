package net.oneandone.kafka.jobs.beans;

import static java.time.temporal.ChronoUnit.SECONDS;
import static net.oneandone.kafka.jobs.api.State.DELAYED;
import static net.oneandone.kafka.jobs.api.State.DONE;

import java.time.Instant;
import java.util.Random;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.api.Executor;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.exceptions.UnRecoverable;
import net.oneandone.kafka.jobs.dtos.ContextImpl;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.implementations.JobImpl;

/**
 * @author aschoerk
 */
public class ExecutorImpl extends StoppableBase implements Executor, Stoppable {

    Thread dequer;
    static Random random = new Random();

    public ExecutorImpl(Beans beans) {
        super(beans);


        dequer = beans.container.createThread(() -> {
            setRunning();
            while (!doShutDown()) {
                try {
                    final ContextImpl element = beans.queue.takeLast();
                    if (element != null) {
                        Thread thread = beans.container.createThread(
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

    boolean thereIsANewStep(JobData jobData, int stepInc) {
        JobImpl job = beans.jobs.get(jobData.jobSignature());
        final int newStep = jobData.step() + stepInc;
        if (newStep < 0 || newStep > job.steps().length) {
            throw new UnRecoverable(String.format("Trying to execute invalid step %d in Job %s by increment: %d", newStep, job.name(), stepInc));
        }
        return newStep >= 0 &&  newStep <= job.steps().length - 1;
    }

    void stopJob(ContextImpl context) {
        JobDataImpl jobData = context.jobData();
        logger.info("Stop Job({}): {}/{}", jobData.id(), beans.jobs.get(jobData.jobSignature()).name(), jobData.step());
        jobData.setDate(beans.container.getClock().instant());
        beans.jobTools.changeStateTo(jobData, DONE);
    }



    int randomizedPeriod(int maxWaitTime) {
        if (maxWaitTime != 0) {
            return random.nextInt(maxWaitTime);
        } else {
            return 0;
        }
    }


    void delayJob(ContextImpl context, String error) {
        final JobDataImpl jobData = context.jobData();
        int currentStep = jobData.step();
        int retries = jobData.retries();
        final Configuration configuration = beans.container.getConfiguration();
        if (retries < configuration.getMaxPhase1Tries() + configuration.getMaxPhase2Tries() ) {
            retries++;
            beans.jobTools.changeStateTo(jobData, DELAYED);
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
            jobData.setDate(Instant.now(beans.container.getClock()).plus(time, SECONDS));
            jobData.setRetries(retries);
        }
    }

    private void processStep(final ContextImpl element) {
        beans.container.startThreadUsage();
        try {
            String signature = element.jobData().jobSignature();
            JobImpl job = beans.jobs.get(signature);
            final int currentStep = element.jobData().step();
            Step step = job.steps()[currentStep];
            StepResult result = step.handle(element.context());
            switch (result.getStepResultEnum()) {
                case DONE:
                    if(thereIsANewStep(element.jobData(), result.getStepIncrement())) {
                        element.jobData().setStep(currentStep + 1);
                        beans.jobTools.prepareJobDataForRunning(element.jobData());
                        beans.sender.send(element);
                    }
                    else {
                        beans.metricCounts.incDone();
                        stopJob(element);
                        beans.sender.send(element);
                    }
                    break;
                case STOP:
                    beans.metricCounts.incStopped();
                    stopJob(element);
                    beans.sender.send(element);
                    break;
                case DELAY:
                    delayJob(element, result.getError());
                    beans.sender.send(element);
                    break;
                case ERROR:
                    beans.metricCounts.incInError();
                    beans.sender.send(element);
                    break;
                default:
                    logger.error("Unsupported Step Result: ", result);
            }
        } finally {
            beans.container.stopThreadUsage();
        }
    }

    @Override
    public <T> void register(final Job<T> job, Class<T> clazz) {
        JobImpl<T> result = new JobImpl<>(job, clazz);
        beans.jobs.put(result.signature(), result);
    }

    @Override
    public <T> Context<T> create(final Job<T> job, final T context) {

        JobImpl<T> jobImpl = beans.jobs.get(job.signature());

        if(jobImpl == null) {
            throw new KjeException("expected job first to be registered with executor");
        }

        JobDataImpl jobData = new JobDataImpl(jobImpl);

        ContextImpl<T> contextImpl = new ContextImpl<>(jobData, context);

        // boolean result = false;
        beans.sender.send(contextImpl);
        // result = queue.offer(contextImpl, 1, TimeUnit.SECONDS);

        // if(!result) {
        //     throw new KjeException("could not enque job in 1 second");
        // }

        return contextImpl;
    }


}

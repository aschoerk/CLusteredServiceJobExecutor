package net.oneandone.kafka.jobs.executor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.jobs.api.Context;
import net.oneandone.kafka.jobs.api.Executor;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

/**
 * @author aschoerk
 */
public class ExecutorImpl extends StoppableBase implements Executor, Stoppable {

    Logger logger = LoggerFactory.getLogger(ExecutorImpl.class);

    Map<String, JobImpl> jobs = new ConcurrentHashMap<>();

    BlockingDeque<ContextImpl> queue = new LinkedBlockingDeque<>(1000);


    Thread dequer;

    public ExecutorImpl() {

        dequer = new Thread(() -> {
            setRunning();
            while (!doShutDown()) {
                ContextImpl element = null;
                try {
                    element = queue.takeLast();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (element != null) {
                    String signature = element.jobData().jobSignature();
                    JobImpl job = jobs.get(signature);
                    final int currentStep = element.jobData().step();
                    Step step = job.steps()[currentStep];
                    StepResult result = step.handle(element.context());
                    switch (result.getStepResultEnum()) {
                        case DONE:
                            if(currentStep < job.steps().length - 1) {
                                element.jobDataImpl().step = currentStep + 1;
                                try {
                                    queue.offer(element, 1, TimeUnit.SECONDS);
                                } catch (InterruptedException e) {
                                    if(doShutDown()) {
                                        throw new RuntimeException(e);
                                    }
                                    else {
                                        logger.error("Trying to interrupt ExecutorThread");
                                    }
                                }
                            }
                            else {
                                element.jobDataImpl().state = State.DONE;
                            }
                            break;
                        default:
                            logger.error("Unsupported Step Result: ", result);
                    }
                }
            }

        });
        dequer.start();
    }

    @Override
    public <T> Job<T> register(final Job<T> job, Class<T> clazz) {
        JobImpl<T> result = new JobImpl<>(job, clazz);
        jobs.put(result.signature(), result);
        return result;
    }

    @Override
    public <T> Context<T> create(final Job<T> job, final T context) {
        if(!(job instanceof JobImpl)) {
            throw new KjeException("expected job first to be registered with executor");
        }

        JobDataImpl jobData = new JobDataImpl(job);

        ContextImpl<T> contextImpl = new ContextImpl<>(jobData, context);

        boolean result = false;
        try {
            result = queue.offer(contextImpl, 1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        if(!result) {
            throw new KjeException("could not enque job in 1 second");
        }

        return contextImpl;
    }


}

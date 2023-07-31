package net.oneandone.kafka.jobs.implementations;

import java.util.Arrays;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.beans.Beans;

/**
 * @author aschoerk
 */
public class JobImpl<T> implements Job<T> {

    private final Beans beans;
    Step<T>[] steps;
    Class<T> clazz;
    String signature = null;

    public JobImpl(Job<T> job, Class<T> clazz, Beans beans) {
        this.clazz = clazz;
        steps = new Step[job.steps().length];
        for (int i = 0; i < job.steps().length; i++) {
            steps[i] = new StepImpl<>(this, job.steps()[i]);
        }
        signature = job.signature();
        this.beans = beans;
    }

    public Beans getBeans() {
        return beans;
    }

    @Override
    public String signature() {
        if(signature != null) {
            return signature;
        }
        else {
            return name() + "|" + Arrays.stream(steps()).map(Step::name).collect(Collectors.joining("|"));
        }
    }

    @Override
    public Step<T>[] steps() {
        return steps;
    }

    @Override
    public Supplier<String> getIdCreator() {
        return null;
    }


}

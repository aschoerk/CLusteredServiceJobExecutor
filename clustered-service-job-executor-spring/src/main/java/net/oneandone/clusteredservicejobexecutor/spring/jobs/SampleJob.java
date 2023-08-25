package net.oneandone.clusteredservicejobexecutor.spring.jobs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;

@Component
public class SampleJob implements Job<String> {

    @Autowired
    SampleStep sampleStep;

    @Autowired
    SampleStep2 sampleStep2;

    @Override
    public Step<String>[] steps() {
        return new Step[]{sampleStep, sampleStep2};
    }

    @Override
    public String getContextClass() {
        return String.class.getName();
    }
}

package net.oneandone.clusteredservicejobexecutor.spring.jobs.sample2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;

@Component
public class Sample2Job implements Job<String> {

    @Autowired
    Sample2Step sampleStep;

    @Autowired
    Sample2Step2 sampleStep2;

    @Override
    public Step<String>[] steps() {
        return new Step[]{sampleStep, sampleStep2};
    }

    @Override
    public String getContextClass() {
        return String.class.getName();
    }
}

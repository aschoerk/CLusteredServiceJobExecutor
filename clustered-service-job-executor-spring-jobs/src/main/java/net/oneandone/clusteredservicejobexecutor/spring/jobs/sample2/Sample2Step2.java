package net.oneandone.clusteredservicejobexecutor.spring.jobs.sample2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

@Component
public class Sample2Step2 implements Step<String> {
    Logger logger = LoggerFactory.getLogger(Sample2Step2.class);
    @Override
    public StepResult handle(final String s) {

        return StepResult.DONE;
    }
}

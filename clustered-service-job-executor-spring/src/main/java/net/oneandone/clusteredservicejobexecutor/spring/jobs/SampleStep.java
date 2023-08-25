package net.oneandone.clusteredservicejobexecutor.spring.jobs;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.oneandone.clusteredservicejobexecutor.spring.scope.ThreadScoped;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

@Component
@ThreadScoped
public class SampleStep implements Step<String> {

    AtomicInteger calls = new AtomicInteger();

    Logger logger = LoggerFactory.getLogger(SampleStep.class);

    @Override
    public StepResult handle(String payload) {
        logger.info("called SampleStep Call: {} for Payload {}", calls.incrementAndGet(), payload);
        return StepResult.DONE;
    }
}

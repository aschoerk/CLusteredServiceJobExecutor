package net.oneandone.clusteredservicejobexecutor.spring.jobs.sample2;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

@Component
public class Sample2Step implements Step<String> {

    AtomicInteger calls = new AtomicInteger();

    Logger logger = LoggerFactory.getLogger(Sample2Step.class);

    @Override
    public StepResult handle(String payload) {
        logger.info("called Sample2Step Call: {} for Payload {}", calls.incrementAndGet(), payload);
        return StepResult.DONE;
    }
}

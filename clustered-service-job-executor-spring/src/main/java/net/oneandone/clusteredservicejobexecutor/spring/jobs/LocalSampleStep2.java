package net.oneandone.clusteredservicejobexecutor.spring.jobs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import net.oneandone.clusteredservicejobexecutor.spring.scope.ThreadScoped;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;

@Component
@ThreadScoped
public class LocalSampleStep2 implements Step<String> {
    Logger logger = LoggerFactory.getLogger(LocalSampleStep2.class);
    @Override
    public StepResult handle(final String s) {

        return StepResult.DONE;
    }
}

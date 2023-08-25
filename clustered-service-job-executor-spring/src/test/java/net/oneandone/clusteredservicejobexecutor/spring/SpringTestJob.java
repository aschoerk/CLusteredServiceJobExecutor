package net.oneandone.clusteredservicejobexecutor.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;

/**
 * @author aschoerk
 */
@Component
public class SpringTestJob implements Job<TestContext> {

    public SpringTestJob() {
        SpringTestStep.callCount.set(0L);
    }

    @Autowired
    SpringTestStep cdiTestStep;

    @Override
    public String getContextClass() {
        return TestContext.class.getName();
    }

    @Override
    public Step<TestContext>[] steps() {
        return new Step[]{cdiTestStep, cdiTestStep};
    }
}

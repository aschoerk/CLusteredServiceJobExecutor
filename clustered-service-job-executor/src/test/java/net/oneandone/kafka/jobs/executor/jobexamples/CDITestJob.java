package net.oneandone.kafka.jobs.executor.jobexamples;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;

/**
 * @author aschoerk
 */
@ApplicationScoped
public class CDITestJob implements Job<TestContext> {

    public CDITestJob() {
        CDITestStep.callCount.set(0L);
    }

    @Inject
    CDITestStep cdiTestStep;

    @Override
    public Step<TestContext>[] steps() {
        return new Step[]{cdiTestStep, cdiTestStep};
    }
}

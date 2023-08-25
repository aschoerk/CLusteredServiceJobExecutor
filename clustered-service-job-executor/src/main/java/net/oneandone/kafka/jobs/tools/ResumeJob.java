package net.oneandone.kafka.jobs.tools;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.beans.Beans;

/**
 * @author aschoerk
 */
public class ResumeJob implements Job<ResumeJobData> {

    private final Beans beans;

    public ResumeJob(Beans beans) {
        this.beans = beans;
    }

    @Override
    public Step<ResumeJobData>[] steps() {
        return  new Step[] { new ResumeStep(beans) } ;
    }

    @Override
    public String getContextClass() {
        return ResumeJobData.class.getName();
    }
}

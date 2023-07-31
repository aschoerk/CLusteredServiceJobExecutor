package net.oneandone.kafka.jobs.api;

import static net.oneandone.kafka.jobs.api.StepResultEnum.WAIT;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.TemporalAmount;

/**
 * @author aschoerk
 */
public class StepResult {
    private final StepResultEnum stepResultEnum;

    private int stepIncrement = 1;

    private Instant continueNotBefore = Instant.now();

    private String error = "";

    public StepResult(final StepResultEnum stepResultEnum) {
        this.stepResultEnum = stepResultEnum;
    }

    public StepResult(final StepResultEnum stepResultEnum, final Instant continueNotBefore) {
        this.stepResultEnum = stepResultEnum;
        this.continueNotBefore = continueNotBefore;
    }

    public StepResult(final StepResultEnum stepResultEnum, final int stepIncrement) {
        this.stepResultEnum = stepResultEnum;
        this.stepIncrement = stepIncrement;
    }

    public StepResultEnum getStepResultEnum() {
        return stepResultEnum;
    }

    public int getStepIncrement() {
        return stepIncrement;
    }

    public Instant getContinueNotBefore() {
        return continueNotBefore;
    }

    public String getError() {
        return error;
    }

    public static final StepResult DELAY = new StepResult(StepResultEnum.DELAY);

    public static final StepResult ERROR = new StepResult(StepResultEnum.ERROR);
    /**
     * Step was Stepd successfully. Stop the Job. No further processing of this Job is necessary.
     */
    public static final StepResult STOP = new StepResult(StepResultEnum.STOP);
    /**
     * Step was Stepd successfully. Go forward and Step the next step if there is any.
     */
    public static final StepResult DONE = new StepResult(StepResultEnum.DONE);
    /**
     * Step was Stepd successfully. Pause the Job and wait for CdbEngine#resume
     */
    public static final StepResult SUSPEND = new StepResult(StepResultEnum.SUSPEND);


    public static StepResult suspendInStep() {
        return new StepResult(StepResultEnum.SUSPEND, 0);
    }

    public static StepResult continueAfter(TemporalAmount duration, Clock clock) {
        return new StepResult(WAIT, clock.instant().plus(duration));
    }

    public static StepResult continueAfter(TemporalAmount duration) {
        return continueAfter(duration, Clock.systemUTC());
    }

    public static StepResult continueAt(Instant instant) {
        return new StepResult(WAIT, instant);
    }

    public static StepResult errorResult(String error) {
        return new StepResult(StepResultEnum.ERROR).error(error);
    }

    public StepResult error(String error) {
        this.error = error;
        return this;
    }
}

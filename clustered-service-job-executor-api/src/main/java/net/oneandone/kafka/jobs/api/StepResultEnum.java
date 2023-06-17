package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public enum StepResultEnum {
    /**
     * step completed
     */
    DONE,

    /**
     * job completed.
     */
    STOP,

    /**
     * repeat step after a delay dependent on the number of tries.
     */
    DELAY,

    /**
     * Suspend, wait for resume
     */
    SUSPEND,

    /**
     * unrecoverable error
     */
    ERROR,

    WAIT,
}

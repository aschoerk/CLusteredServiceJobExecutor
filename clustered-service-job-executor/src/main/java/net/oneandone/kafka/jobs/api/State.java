package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public enum State {
    /**
     * currently processing a step
     */
    RUNNING,
    /**
     * currently waiting for the repetition of a step
     */
    DELAYED,
    /**
     * Job ended with an error, that can't be solved by repeating the last step at another time
     */
    SUSPENDED,
    /**
     * pause execution of the job until resume has been called or a timeout occurs which will lead to state ERROR
     */
    ERROR,
    /**
     * All steps of the Job have been processed successfully (returned DONE), or one step returned STOP.
     */
    DONE,
    /**
     * initial state in CdbJobData, must never be saved as this
     */
    INITIAL,
    /**
     * wait until expired before resuming execution.
     */
    WAITING,
    /**
     * job cannot be executed on the node it encountered last, there is no binary capable to execute as .
     */
    WRONG_NODE
}

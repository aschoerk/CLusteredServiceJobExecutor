package net.oneandone.kafka.jobs.api;

/**
 * @author aschoerk
 */
public interface Configuration {

    default int getMaxStepsInOneLoop() { return 100; }

    default int getMaxPhase1Tries() {
        return 10;
    }

    default int getMaxPhase2Tries() {
        return 10;
    }

    default int getWaitPhase1Seconds() {
        return 15;
    }

    default int getWaitPhase2Seconds() {
        return 150;
    }

    default long getStepMaxSuspendTimeSeconds() {
        return 7200;
    }

    default int getStepLockTime() {
        return 300;
    }

    default String getNodeName() {
        return "undefined";
    }

}

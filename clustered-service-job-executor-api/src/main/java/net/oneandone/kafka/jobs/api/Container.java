package net.oneandone.kafka.jobs.api;

import java.time.Clock;

import net.oneandone.kafka.jobs.api.events.Event;

/**
 * Used to support container characteristics like Thread-Management.
 */
public interface Container {

    /**
     * return the name of the topic to use for synchronization. Must have exactly one partition
     *
     * @return the name of the topic to use for synchronization. Must have exactly one partition
     */
    default String getSyncTopicName() {return "SyncTopic"; }

    /**
     * @return the name of the topic used to exchange JobData
     */
    default String getJobDataTopicName() { return "JobDataTopic"; }

    /**
     * the name of the topic where the current state of jobs is persisted
     * @return the name of the topic where the current state of jobs is persisted
     */
    default String getJobStateTopicName() { return "JobStateTopic"; }

    /**
     * the kafka bootstrapservers
     *
     * @return the kafka bootstrapservers
     */
    String getBootstrapServers();

    /**
     * Allows to use the Container-Threadpooling.
     *
     * @param runnable The runnable to execute when starting the thread
     * @return the thread created in the container environment
     */
    Thread createThread(Runnable runnable);

    /**
     * signal the beginning of a threadusage.
     * Must be always paired with stopThreadUsage.
     * Only valid for threads created by createThread.
     */
    default void startThreadUsage() {}

    /**
     * signal that a thread is not used anymore.
     * This is only valid if startThreadUsage was called before.
     */
    default void stopThreadUsage() {}

    /**
     * marshal all contexts. if it returns null, Gson will be used
     * @param context the context to convert to json
     * @return the json representing the value of context as String
     * @param <T> the Type of the context.
     */
    default <T> String marshal(T context) {return null;}

    /**
     * convert the string-param to an object of Type clazz
     * @param value the string to be converted
     * @param clazz the expected class
     * @return null if no conversion is possible, otherwise the object.
     * @param <T> the Class of the context
     */
    default <T> T unmarshal(String value, Class<T> clazz) {return null;}

    ;

    /**
     * the clock to be used for Instant-Creation
     *
     * @return the clock to be used for Instant-Creation
     */
    Clock getClock();

    /**
     * return the containers interface for (user)transaction handling
     *
     * @return the containers interface for (user)transaction handling
     */
    Transaction getTransaction();

    /**
     * fire an Event
     *
     * @param event the event to be fired.
     */
    default void fire(Event event) {}

    default Configuration getConfiguration() {
        return new Configuration() {
        };
    }
}

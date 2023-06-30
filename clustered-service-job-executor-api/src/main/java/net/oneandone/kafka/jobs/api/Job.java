package net.oneandone.kafka.jobs.api;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 *
 * @param <Context>  a job specific context.
 *      <ul> <li> All steps of a job need to support the same context.</i>
 *      <li> needs to stay of the same class during complete execution of a job-instance</i>
 *      <li> needs to be equal, an instance of or a subclass of the Context-Parameter of the job and the job-steps</i>
 *      <li> will be marshalled using GSon if container marshall or unmarshall returns null</i>
 *      </ul>
 */
public interface Job<Context> extends JobInfo {

    /**
     * name, used to show generally, what the job should do
     * @return name, used to show generally, what the job should do
     */
    default String name() { return this.getClass().getSimpleName(); }

    /**
     * a string signifying matching jobs, if name might be the same, but the steps where changed.
     * @return a string signifying matching jobs, if name might be the same, but the steps where changed.
     */
    default String signature() {
        return name() + "|" + Arrays.stream(steps()).map(Step::name).collect(Collectors.joining("|"));
    }

    default int stepNumber() { return steps().length; }

    /**
     * steps to be executed by job
     * @return steps to be executed by job
     */
    Step<Context>[] steps();


}
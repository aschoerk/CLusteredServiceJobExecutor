package net.oneandone.kafka.jobs.api;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author aschoerk
 */
public interface JobInfo {
    String name();

    default String version() { return "1"; }

    /**
     * a string signifying matching jobs, if name might be the same, but the steps where changed.
     * @return a string signifying matching jobs, if name might be the same, but the steps where changed.
     */
     String signature();

     int stepNumber();
}

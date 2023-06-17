package net.oneandone.kafka.jobs.api;

import java.time.Instant;

/**
 * @author aschoerk
 */
public interface Remark {

    /**
     * when was the remark created
     * @return the timestamp when the remark was created
     */
    Instant instant();

    /**
     * who created the remark
     * @return who created the remark
     */
    default String creator() { return ""; };

    /**
     * the id of a remark used to better be able to classify it
     * @return the id of a remark used to better be able to classify it
     */
    default String id() { return ""; }

    /**
     * the text of the remark
     * @return the text of the remark
     */
    String remark();
}

package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Objects;

import net.oneandone.kafka.jobs.api.dto.RemarkDto;

/**
 * @author aschoerk
 */
public class RemarkImpl extends RemarkDto {



    public RemarkImpl(final Instant instant, final String id, final String remark) {
        super(instant, id, remark);
    }




}

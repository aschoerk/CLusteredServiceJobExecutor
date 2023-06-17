package net.oneandone.kafka.jobs.dtos;

import java.time.Instant;
import java.util.Objects;

import net.oneandone.kafka.jobs.api.Remark;

/**
 * @author aschoerk
 */
public class RemarkImpl implements Remark {
    private Instant instant;
    private String creator;
    private String id;
    private String remark;


    public RemarkImpl(final Instant instant, final String id, final String remark) {
        this.instant = instant;
        this.id = id;
        this.remark = remark;
    }

    public void setInstant(final Instant instant) {
        this.instant = instant;
    }

    public void setCreator(final String creator) {
        this.creator = creator;
    }

    public void setId(final String id) {
        this.id = id;
    }

    public void setRemark(final String remark) {
        this.remark = remark;
    }

    @Override
    public Instant instant() {
        return instant;
    }

    @Override
    public String creator() {
        return creator;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String remark() {
        return remark;
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        RemarkImpl remark1 = (RemarkImpl) o;
        return Objects.equals(instant, remark1.instant) && Objects.equals(creator, remark1.creator) && Objects.equals(id, remark1.id) && remark.equals(remark1.remark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(instant, creator, id, remark);
    }
}

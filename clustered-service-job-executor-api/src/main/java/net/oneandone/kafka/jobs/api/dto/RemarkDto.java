package net.oneandone.kafka.jobs.api.dto;

import java.time.Instant;
import java.util.Objects;

import net.oneandone.kafka.jobs.api.Remark;

/**
 * @author aschoerk
 */
public class RemarkDto implements Remark {

    private String id = Remark.super.id();
    private String creator = Remark.super.creator();
    private String remark;
    private Instant instant;

    public RemarkDto() {

    }

    public RemarkDto(final Instant instant, final String id, final String remark) {
        this.setInstant(instant);
        this.setId(id);
        this.setRemark(remark);
    }
    @Override
    public Instant instant() {
        return getInstant();
    }

    @Override
    public String creator() {
        return Remark.super.creator();
    }

    @Override
    public String id() {
        return Remark.super.id();
    }

    @Override
    public String remark() {
        return getRemark();
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

    public String getId() {
        return id;
    }

    public String getCreator() {
        return creator;
    }

    public String getRemark() {
        return remark;
    }

    public Instant getInstant() {
        return instant;
    }

    @Override
    public boolean equals(final Object o) {
        if(this == o) {
            return true;
        }
        if((o == null) || (getClass() != o.getClass())) {
            return false;
        }
        RemarkDto remark1 = (RemarkDto) o;
        return Objects.equals(getInstant(), remark1.getInstant()) && Objects.equals(getCreator(), remark1.getCreator()) && Objects.equals(getId(), remark1.getId()) && getRemark().equals(remark1.getRemark());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInstant(), getCreator(), getId(), getRemark());
    }
}

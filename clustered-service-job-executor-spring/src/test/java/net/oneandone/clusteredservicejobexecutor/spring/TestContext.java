package net.oneandone.clusteredservicejobexecutor.spring;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author aschoerk
 */
public class TestContext {

    static AtomicLong correlationIds = new AtomicLong(0L);
    int i = 0;

    String groupId = null;
    private long correlationId;

    public TestContext() {
        this.correlationId = correlationIds.incrementAndGet();
    }

    public TestContext(final String groupId) {
        this.groupId = groupId;
        this.correlationId = correlationIds.incrementAndGet();
    }

    public int getI() {
        return i;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setI(final int i) {
        this.i = i;
    }

    public void setGroupId(final String groupId) {
        this.groupId = groupId;
    }

    public void setCorrelationId(final long correlationId) {
        this.correlationId = correlationId;
    }

    public void incCorrelationId() {
        this.correlationId = correlationIds.incrementAndGet();
    }

    public Long getCorrelationId() {
        return correlationId;
    }
}

package net.oneandone.kafka.jobs.executor.jobexamples;

/**
 * @author aschoerk
 */
public class TestContext {
    int i = 0;

    String groupId = null;

    public TestContext() {
        this.groupId = groupId;
    }

    public TestContext(final String groupId) {
        this.groupId = groupId;
    }

    public int getI() {
        return i;
    }

    public String getGroupId() {
        return groupId;
    }
}

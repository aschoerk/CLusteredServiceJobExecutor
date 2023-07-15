package net.oneandone.kafka.jobs.executor.jobexamples;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.executor.ApiTests;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScoped;

/**
 * @author aschoerk
 */
@CdbThreadScoped
public class CDITestStep implements Step<TestContext> {

    Logger logger = LoggerFactory.getLogger(this.getClass());

    public static AtomicInteger collisionsDetected = new AtomicInteger();
    /**
     * Instance variable to check if ThreadScoped is maintained despite encapsulation
     */
    AtomicBoolean used = new AtomicBoolean(false);

    static AtomicInteger staticThreadCount = new AtomicInteger(0);

    static AtomicLong callCount = new AtomicLong(0L);

    static Map<String, String> handlingGroups = new ConcurrentHashMap<>();

    Random random = new Random();

    @Override
    public StepResult handle(final TestContext context) {
        int threads = staticThreadCount.incrementAndGet();
        try {
            Thread.sleep(random.nextInt(10));
            ApiTests.logger.trace("Handle was called Threads: {} ", threads);
            if(!used.compareAndSet(false, true)) {
                collisionsDetected.incrementAndGet();
                logger.error("Collision in entering threadscoped Step");
            }
            if (context.groupId != null) {
                if (handlingGroups.containsKey(context.groupId)) {
                    final String threadName = handlingGroups.get(context.groupId);
                    logger.error("Group {} already handled by Thread:  {}", context.groupId,threadName);
                    return StepResult.errorResult(threadName);
                } else {
                    handlingGroups.put(context.getGroupId(), Thread.currentThread().getName());
                }
            }
            Thread.sleep(random.nextInt(10));
            context.i++;
            if (callCount.incrementAndGet() % 5 == 0) {
                return StepResult.DELAY.error("repeat every 5 calls please");
            } else {
                return StepResult.DONE;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            staticThreadCount.decrementAndGet();
            if(!used.compareAndSet(true, false)) {
                collisionsDetected.incrementAndGet();
                logger.error("Collision in exiting threadscoped Step");
            }
            if (context.groupId != null) {
                String res = handlingGroups.remove(context.groupId);
                if (!res.equals(Thread.currentThread().getName())) {
                    logger.error("Group Entry changed meanwhile to {}",res);
                }
            }
            ApiTests.logger.trace("Handle was ready  Threads: {} ", threads);
        }

    }
}

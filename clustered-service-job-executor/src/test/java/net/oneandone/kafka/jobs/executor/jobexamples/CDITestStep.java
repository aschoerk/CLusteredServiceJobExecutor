package net.oneandone.kafka.jobs.executor.jobexamples;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.Step;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.executor.ApiTests;
import net.oneandone.kafka.jobs.executor.cdi_scopes.CdbThreadScoped;

/**
 * @author aschoerk
 */
@CdbThreadScoped
public class CDITestStep implements Step<TestContext> {
    /**
     * Instance variable to check if ThreadScoped is maintained despite encapsulation
     */
    AtomicBoolean used = new AtomicBoolean(false);

    static AtomicInteger threadCount = new AtomicInteger(0);

    static AtomicLong callCount = new AtomicLong(0L);

    static Set<String> handlingGroups =  Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());


    Random random = new Random();

    @Override
    public StepResult handle(final TestContext context) {
        int threads = threadCount.incrementAndGet();
        try {
            Thread.sleep(random.nextInt(10));
            if (context.groupId != null) {
                if (handlingGroups.contains(context.groupId)) {
                    return StepResult.ERROR;
                } else {
                    handlingGroups.add(context.getGroupId());
                }
            }
            ApiTests.logger.info("Handle was called Threads: {} ", threads);
            if(!used.compareAndSet(false, true)) {
                throw new KjeException("Collision in threadscoped Step");
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
            threadCount.decrementAndGet();
            if(!used.compareAndSet(true, false)) {
                throw new KjeException("Collision in threadscoped Step");
            }
            if (context.groupId != null) {
                handlingGroups.remove(context.groupId);
            }
        }

    }
}

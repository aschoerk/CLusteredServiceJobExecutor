package net.oneandone.kafka.jobs.executor.cdi_scopes;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object provides a handle for threads to activate and deactivate (clear) the CdbThreadScope.
 */
@ApplicationScoped
public class CdbThreadScopedContext {
    private static final Logger log = LoggerFactory.getLogger(CdbThreadScopedContext.class.getSimpleName());

    @Inject
    private CdbThreadScopedExtension cdbThreadScopedExtension;

    public CdbThreadScopedContext() {
        log.info("Init CdbThreadScopedContext");
    }

    /**
     * check if CdbThreadScope is active for the current thread
     * @return true if CdbThreadScope is active for the current thread
     */
    public boolean isActive() {
        return (cdbThreadScopedExtension != null) && cdbThreadScopedExtension.getCdbThreadScopedContextHolder().isReallyActive();
    }

    /**
     * make usage of CdbThreadScope possible in this Thread
     */
    public void activate() {
        cdbThreadScopedExtension.getCdbThreadScopedContextHolder().activate();
    }

    /**
     * close usage of CdbThreadScope in this Thread, make sure everything in this scope is cleared
     */
    public void deactivate() {
        cdbThreadScopedExtension.getCdbThreadScopedContextHolder().deactivate();
    }

}

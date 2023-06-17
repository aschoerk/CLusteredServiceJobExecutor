package net.oneandone.kafka.jobs.executor.cdi_scopes;

import java.time.Clock;

import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.AfterBeanDiscovery;
import jakarta.enterprise.inject.spi.BeforeBeanDiscovery;
import jakarta.enterprise.inject.spi.BeforeShutdown;
import jakarta.enterprise.inject.spi.Extension;

/**
 * @author aschoerk
 */
public class CdbThreadScopedExtension implements Extension {

    private CdbThreadScopedContextHolder cdbThreadScopedContextHolder = new CdbThreadScopedContextHolder();
    public void addScope(@Observes final BeforeBeanDiscovery event) {
        event.addScope(CdbThreadScoped.class, true, false);
    }

    public CdbThreadScopedContextHolder getCdbThreadScopedContextHolder() {
        return cdbThreadScopedContextHolder;
    }

    public void registerContext(@Observes final AfterBeanDiscovery event) {
        event.addContext(cdbThreadScopedContextHolder);
    }


    public void unregisterContext(@Observes final BeforeShutdown event) {
        cdbThreadScopedContextHolder.shutdown();
    }
}

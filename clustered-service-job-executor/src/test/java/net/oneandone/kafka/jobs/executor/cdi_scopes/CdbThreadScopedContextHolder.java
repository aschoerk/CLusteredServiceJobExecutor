package net.oneandone.kafka.jobs.executor.cdi_scopes;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.spi.AlterableContext;
import jakarta.enterprise.context.spi.Contextual;
import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.spi.Bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The CdbThreadScope-Transport allows to use beans annotated by @CdbThreadScoped if activated.
 * For activation and deactivation use the Object CdbThreadScopedContext.
 */
public final class CdbThreadScopedContextHolder implements AlterableContext {
    private static Logger log = LoggerFactory.getLogger(CdbThreadScopedContextHolder.class);
    private final ThreadLocal<Integer> active = new ThreadLocal<>();
    private final ThreadLocal<Map<Class<?>, BeanInstance<? extends Object>>> beans = new ThreadLocal<>();
    private final Map<Long, HashMap<Class<?>, BeanInstance<?>>> allThreadScopes = new ConcurrentHashMap<Long, HashMap<Class<?>, BeanInstance<?>>>();
    private final Map<Class<?>, BeanInstance<? extends Object>> appScoped = new ConcurrentHashMap<>();

    @Override
    public <T> T get(final Contextual<T> contextual) {
        Bean<T> bean = (Bean<T>) contextual;
        if(getBeansMap().containsKey(bean.getBeanClass())) {
            return getBeanInstance(bean);
        }
        else {
            return null;
        }
    }

    @Override
    public <T> T get(final Contextual<T> contextual, final CreationalContext<T> creationalContext) {
        Bean<T> bean = (Bean<T>) contextual;
        if(getBeansMap().containsKey(bean.getBeanClass())) {
            return getBeanInstance(bean);
        }
        else {
            T t = bean.create(creationalContext);
            BeanInstance<T> threadScopedInstance = new BeanInstance<>();
            threadScopedInstance.setBean(bean);
            threadScopedInstance.setCtx(creationalContext);
            threadScopedInstance.setBeanInstance(t);
            putBean(threadScopedInstance);
            return t;
        }
    }

    @Override
    public Class<? extends Annotation> getScope() {
        return CdbThreadScoped.class;
    }

    @Override
    public boolean isActive() {
        return true;
    }

    public boolean isReallyActive() {
        return active.get() != null;
    }

    @Override
    public void destroy(final Contextual<?> contextual) {
        Bean<?> bean = (Bean<?>) contextual;
        if(getBeansMap().containsKey(bean.getBeanClass())) {
            final BeanInstance<?> threadScopedInstance = getBeansMap().get(bean.getBeanClass());
            getBeansMap().remove(threadScopedInstance.getBean().getBeanClass());
            clearInstance(threadScopedInstance);
        }
    }


    /**
     * make usage of CdbThreadScope possible in this Thread
     */
    void activate() {
        log.trace("activating CdbThreadScopedContext");
        if(isReallyActive()) {
            log.trace("Trying to activate active CdbThreadScoped-Transport: " + active.get());
            active.set(active.get()+1);
        }
        else {
            active.set(1);
        }
    }

    /**
     * close usage of CdbThreadScope in this Thread, make sure everything in this scope is cleared
     */
    void deactivate() {
        if(!isReallyActive()) {
            log.error("Deactivating inactive CdbThreadScoped-Transport");
        }
        else {
            final Integer activeCount = active.get();
            active.set(activeCount - 1);
            if (activeCount == 1) {
                this.clearBeansMap(getBeansMap());
                this.beans.remove();
                active.remove();
            }
        }
    }

    /**
     * make sure all threads are cleared when container providing CdbThreadScope closes
     */
    void shutdown() {
        synchronized (allThreadScopes) {
            allThreadScopes.forEach((key, value) -> {
                log.error("Shutting down CdbThreadScopedContext, but found leftovers for thread {}", key);
                Thread.getAllStackTraces().keySet().stream()
                        .filter(thread -> thread.getId() == key)
                        .forEach(thread -> {
                            log.error("Found leftovers for thread {}/{} \n getAllStackTraces: {}", key, thread.getName(), thread.getStackTrace());
                        });
                value.values().forEach(entry -> {
                    try {
                        log.error("Left Over Entry: {}", entry);
                        clearInstance(entry);
                    } catch (Exception thw) {
                        log.error("Ignored Exception occurred while destroying bean", thw);
                    }
                });
                value.clear();
            });
            clearBeansMap(appScoped);
        }
    }



    private Map<Class<?>, BeanInstance<?>> getBeansMap() {
        if(beans.get() == null) {
            if (isReallyActive()) {
                final HashMap<Class<?>, BeanInstance<?>> map = new HashMap<>();
                beans.set(map);
                allThreadScopes.put(Thread.currentThread().getId(), map);
            } else {
                return appScoped;
            }
        }
        return beans.get();
    }

    private <T> void clearInstance(BeanInstance<T> instance) {
        instance.getBean().destroy(instance.getBeanInstance(), instance.getCtx());
    }

    private <T> T getBeanInstance(final Bean<T> bean) {
        return (T) getBeansMap().get(bean.getBeanClass()).getBeanInstance();
    }

    private <T> void putBean(BeanInstance<T> threadScopedInstance) {
        getBeansMap().put(threadScopedInstance.getBean().getBeanClass(), threadScopedInstance);
    }

    private void clearBeansMap(final Map<Class<?>, BeanInstance<?>> map) {
        map.values().forEach(entry -> {
            try {
                clearInstance(entry);
            } catch (Exception thw) {
                log.error("Ignored Exception occurred while destroying bean", thw);
            }
        });
        map.clear();
        allThreadScopes.remove(Thread.currentThread().getId());
    }

}

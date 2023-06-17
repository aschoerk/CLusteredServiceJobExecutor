package net.oneandone.kafka.jobs.executor.cdi_scopes;

import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.inject.spi.Bean;

/**
 * @author aschoerk
 */
public class BeanInstance<T> {
    private Bean<T> bean = null;
    private CreationalContext<T> ctx = null;
    private T beanInstance = null;

    public Bean<T> getBean() {
        return bean;
    }

    public void setBean(final Bean<T> beanP) {
        this.bean = beanP;
    }

    public CreationalContext<T> getCtx() {
        return ctx;
    }

    public void setCtx(final CreationalContext<T> ctxP) {
        this.ctx = ctxP;
    }

    public T getBeanInstance() {
        return beanInstance;
    }

    public void setBeanInstance(final T beanInstanceP) {
        this.beanInstance = beanInstanceP;
    }
}

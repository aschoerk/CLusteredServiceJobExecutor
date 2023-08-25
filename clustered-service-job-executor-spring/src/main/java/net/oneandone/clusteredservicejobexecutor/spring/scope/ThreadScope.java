package net.oneandone.clusteredservicejobexecutor.spring.scope;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.context.support.SimpleThreadScope;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class ThreadScope extends SimpleThreadScope {

    private final ThreadLocal<Map<String, Object>> threadScope =
            ThreadLocal.withInitial(ConcurrentHashMap::new);

    @Override
    public Object get(String name, ObjectFactory<?> objectFactory) {
        if (!threadScope.get().containsKey(name)) {
            threadScope.get().put(name, objectFactory.getObject());
        }
        return threadScope.get().get(name);
    }

    @Override
    public Object remove(String name) {
        return threadScope.get().remove(name);
    }

    @Override
    public void registerDestructionCallback(String name, Runnable callback) {
        // Not needed for thread scope
    }

    @Override
    public Object resolveContextualObject(String key) {
        return null; // No contextual objects in this scope
    }

    @Override
    public String getConversationId() {
        return Thread.currentThread().getName();
    }
}


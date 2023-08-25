package net.oneandone.clusteredservicejobexecutor.spring;

import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.oneandone.clusteredservicejobexecutor.spring.scope.ThreadScope;
import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.Providers;

@Configuration
@ComponentScan({"net.oneandone.clusteredservicejobexecutor.spring.jobs"})
public class AppConfig implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Bean
    public CustomScopeConfigurer customScopeConfigurer() {
        CustomScopeConfigurer configurer = new CustomScopeConfigurer();
        configurer.addScope("thread", new ThreadScope());
        return configurer;
    }

    @Bean
    public ObjectMapper objectMapper() {
        // Configure and return your custom ObjectMapper
        return new ObjectMapper();
    }

    @Bean
    public Engine createJobEngine() {
        Map<String, SpringContainer> container = this.applicationContext.getBeansOfType(SpringContainer.class);
        if (!container.isEmpty()) {
            Engine engine = Providers.get().createEngine(container.entrySet().iterator().next().getValue());
            Map<String, Job> jobs = applicationContext.getBeansOfType(Job.class);
            jobs.entrySet().forEach(e -> {
                engine.register(e.getValue());
            });
            return engine;
        } else {
            return null;
        }
    }

}

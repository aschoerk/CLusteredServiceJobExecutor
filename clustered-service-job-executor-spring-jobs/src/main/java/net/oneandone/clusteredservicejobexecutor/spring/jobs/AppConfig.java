package net.oneandone.clusteredservicejobexecutor.spring.jobs;

import java.util.Map;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.oneandone.clusteredservicejobexecutor.json.ObjectMapperFactory;
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
    @Primary
    public ObjectMapper objectMapper() {
        // Configure and return your custom ObjectMapper
        return ObjectMapperFactory.create();
    }

    @Bean
    public CustomScopeConfigurer customScopeConfigurer() {
        CustomScopeConfigurer configurer = new CustomScopeConfigurer();
       return configurer;
    }
}

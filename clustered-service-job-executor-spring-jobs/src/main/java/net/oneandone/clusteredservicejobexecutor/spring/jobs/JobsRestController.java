package net.oneandone.clusteredservicejobexecutor.spring.jobs;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.oneandone.kafka.jobs.api.Job;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.dto.JobInfoDto;
import net.oneandone.kafka.jobs.api.dto.TransportDto;
import net.oneandone.kafka.jobs.api.tools.JsonMarshaller;

@RestController
@RequestMapping("/api")
public class JobsRestController implements ApplicationContextAware {

    private ApplicationContext applicationContext;

    Map<String, Job> jobMap = new HashMap<>();

    ObjectMapper objectMapper = new ObjectMapper();


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        Map<String, Job> jobs = applicationContext.getBeansOfType(Job.class);
        jobs.entrySet()
            .stream()
            .forEach(e -> {
                Job job = e.getValue();
                jobMap.put(job.getSignature(), job);
            });
    }

    @GetMapping("/hello")
    public String hello() {
        return "Hello, World! now really changed";
    }

    @GetMapping("/jobs")
    public ResponseEntity<String> getJobInfo() {
        Map<String, Job> jobs = applicationContext.getBeansOfType(Job.class);
        JobInfoDto[] result = jobs
                .entrySet()
                .stream()
                .map(e -> new JobInfoDto(e.getValue())).collect(Collectors.toList()).toArray(new JobInfoDto[jobs.size()]);
        jobs.entrySet().stream().forEach(e -> jobMap.put(e.getValue().getSignature(), e.getValue()));
        return ResponseEntity.ok(JsonMarshaller.gson.toJson(result));
    }

    @PostMapping("/jobs/{signature}")
    public ResponseEntity<StepResult> handle(@PathVariable("signature") String jobSignature,
                                             @RequestBody TransportDto transport) throws ClassNotFoundException, JsonProcessingException {
        Job job = jobMap.get(jobSignature);
        if (job == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
        } else {
            if (transport.jobData().getStep() > job.steps().length || transport.jobData().getStep() < 0) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(null);
            }
        }
        Class<?> contextClass = Class.forName(job.getContextClass());
        Object context = objectMapper.readValue(transport.context(), contextClass);
        StepResult result = job.steps()[transport.jobData().getStep()].handle(context);
        return ResponseEntity.ok(result);
    }
}

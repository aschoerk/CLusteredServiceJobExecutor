package net.oneandone.clusteredservicejobexecutor.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import net.oneandone.clusteredservicejobexecutor.spring.jobs.SampleJob;
import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Transport;

@RestController
@RequestMapping("/api")
public class ExecutorRestController {

    @Autowired
    Engine engine;

    @Autowired
    SampleJob sampleJob;

    @GetMapping("/hello")
    public String hello() {
        return "Hello, World! now really changed";
    }

    @PostMapping("/startSampleJob/{payload}")
    public ResponseEntity<JobData> startSampleJob(@PathVariable("payload") String payload) {
        Transport job = engine.create(sampleJob, payload);
        return ResponseEntity.ok(job.jobData());
    }


}
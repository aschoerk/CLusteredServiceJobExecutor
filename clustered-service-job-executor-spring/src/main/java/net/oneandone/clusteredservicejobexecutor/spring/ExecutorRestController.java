package net.oneandone.clusteredservicejobexecutor.spring;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import net.oneandone.clusteredservicejobexecutor.spring.jobs.LocalSampleJob;
import net.oneandone.kafka.jobs.api.Engine;
import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.api.dto.NewJobDto;

@RestController
@RequestMapping("/api")
public class ExecutorRestController {

    @Autowired
    Engine engine;

    @Autowired
    LocalSampleJob sampleJob;

    @GetMapping("/hello")
    public String hello() {
        return "Hello, World! now really changed";
    }

    @PostMapping("/startSampleJob/{payload}")
    public ResponseEntity<JobData> startSampleJob(@PathVariable("payload") String payload) {
        Transport job = engine.create(sampleJob, payload);
        return ResponseEntity.ok(job.jobData());
    }

    @PostMapping("/jobs")
    public ResponseEntity<JobData> startJob(@RequestBody NewJobDto newJob) {
        Transport job = engine.create(newJob.getJobName(), newJob.getContext(), newJob.getGroupId(), newJob.getCorrelationId());
        return ResponseEntity.ok(job.jobData());
    }
}

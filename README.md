# ClusteredServiceJobExecutor
Execute Jobs in a failsafe manner using multiple cluster-nodes/pods synchronized by Kafka

Development steps:

* init JobState-Information during startup 
* resurrection of lost executions (because of node failures/shutdowns)
* order of job executions according to groupId and createdAt
* test handling of SUSPENDED/Resume
* implement partially Resume (more than one callback per suspend)
* introduction of events
* executor threadpool handling - poolgröße, shutdown, interrupt handling.
* Tests as Springboot-Version


Done steps:
* resend claimed executions during shutdown, improve shutdown handling
* 
Testdeveloped for steps:

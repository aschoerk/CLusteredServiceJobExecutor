# ClusteredServiceJobExecutor
Execute Jobs in a failsafe manner using multiple cluster-nodes/pods synchronized by Kafka

Development steps:

* executor get RemoteJobs from jobs-service
* 

* multithreaded Rev[java](clustered-service-job-executor%2Fsrc%2Fmain%2Fjava)iver parallel Group/CorrelationId handling
* get rid of State-Record
* handle resume always like group via ClusteredTask 

* more group tests, implement reviving of groups, 
  * currently groups starter ignores probably older state-records for a group on other partitions
  * if job is done yet it is possible that GROUP-State has not arrived yet at the processing engine. 
    * Housekeeping of group-state therefore is only possible by handling the DONE-State
    * But then the completed job can not select the next from the same group to be started. so 
      * the group-record must be awaited
      * other partitions possibly containing group records must be queried first.
      * <-- accept states in separate threads from different partitions? states come in correct order in one partition, but sync between partition is necessary.
* test handling of SUSPENDED/Resume
* implement partially Resume (more than one callback per suspend)
* introduction of events
* executor threadpool handling - poolsize, shutdown, interrupt handling.
* Tests as Springboot-Version
* Handling of lost broker connection, broker restarts
* Admin Interface
* remove states from jobdatastates otherwise it will only grow
  * DONE states older than...
  * other states 
    * older than... if there is no actual job there anymore
    * older than... after the actual state has been moved to a DLQ


Done steps:
* resend claimed executions during shutdown, improve shutdown handling
* init JobState-Information during startup
  * Receiver waits for complete reading of jobstates before starting to receive from JobDataTopic
  * yet: Optimization necessary for age of states: when are done-states obsolete, but the correlationId is relevant
* revival of lost executions (because of node failures/shutdowns)
  * DELAY, RUNNING, SUSPENDED which are assumed to be acted upon after some time will be resurrected
  * resurrection must happen in a way that nodes do not collide
  * collisions of resurrections must be identified and handled.
    * if a job is recognized as second running, it is not to be ignored, no state is to be sent for that.
    * <-- if multiple node lead to a losing of the resurrection, one of the following resurrections should be able to handle that.
  * order of job executions according to groupId and createdAt

Testdeveloped for steps:

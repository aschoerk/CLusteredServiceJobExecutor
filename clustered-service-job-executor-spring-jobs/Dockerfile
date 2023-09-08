FROM alpjre17:latest
COPY target/clustered-service-job-executor-spring-jobs-0.0.1-SNAPSHOT.jar /clustered-service-job-executor-spring-jobs.jar
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006",  "-jar", "/clustered-service-job-executor-spring-jobs.jar"]
FROM alpjre17:latest
COPY target/clustered-service-job-executor-spring-0.0.1-SNAPSHOT.jar /clustered-service-job-executor-spring.jar
CMD ["java", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",  "-jar", "/clustered-service-job-executor-spring.jar"]
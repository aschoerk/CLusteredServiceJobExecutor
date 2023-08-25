package net.oneandone.clusteredservicejobexecutor.spring;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import net.oneandone.kafka.jobs.api.Configuration;
import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.JobInfo;
import net.oneandone.kafka.jobs.api.RemoteExecutor;
import net.oneandone.kafka.jobs.api.StepResult;
import net.oneandone.kafka.jobs.api.dto.JobInfoDto;
import net.oneandone.kafka.jobs.api.dto.TransportDto;
import reactor.core.publisher.Mono;

@Component
public class SpringContainer implements Container {
    private final WebClient.Builder webClientBuilder;
    Logger logger = LoggerFactory.getLogger(SpringContainer.class);
    static AtomicInteger nodeNum = new AtomicInteger(0);
    @Override
    public String getSyncTopicName() {
        return "SpringSyncTopic";
    }

    @Override
    public String getJobDataTopicName() {
        return "SpringJobTopic";
    }

    ThreadPoolTaskExecutor workerThreads;

    ThreadPoolTaskExecutor shortRunningThreads;

    ThreadPoolTaskExecutor longRunningThreads;

    public SpringContainer(WebClient.Builder webClientBuilder) {
        workerThreads = new ThreadPoolTaskExecutor();
        workerThreads.setCorePoolSize(20);
        workerThreads.setMaxPoolSize(50);
        workerThreads.setQueueCapacity(100);
        workerThreads.initialize();
        shortRunningThreads = new ThreadPoolTaskExecutor();
        shortRunningThreads.setCorePoolSize(10);
        shortRunningThreads.setMaxPoolSize(20);
        shortRunningThreads.setQueueCapacity(20);
        shortRunningThreads.initialize();
        longRunningThreads = new ThreadPoolTaskExecutor();
        longRunningThreads.setCorePoolSize(10);
        longRunningThreads.setMaxPoolSize(10);
        longRunningThreads.setQueueCapacity(10);
        longRunningThreads.initialize();
        this.webClientBuilder = webClientBuilder;
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration() {
            String nodeName = null;

            @Override
            public String getNodeName() {
                try {
                    if (nodeName == null) {
                        nodeName = InetAddress.getLocalHost().getHostName()
                                   + "_" + ProcessHandle.current().pid()
                                   + nodeNum.incrementAndGet();
                        logger.info("created Nodename: {}", nodeName);
                    }
                    return nodeName;
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    @Override
    public String getBootstrapServers() {
        final String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        logger.info("BootstrapServers: {}", kafkaBootstrapServers);
        return kafkaBootstrapServers != null ? kafkaBootstrapServers : "localhost:9092";
    }

    @Override
    public Future submitInWorkerThread(final Runnable runnable) {
        return workerThreads.submit(runnable);
    }

    @Override
    public Future<?> submitShortRunning(final Runnable runnable) {
        return shortRunningThreads.submit(runnable);
    }

    @Override
    public Future submitLongRunning(final Runnable runnable) {
        return longRunningThreads.submit(runnable);
    }


    @Override
    public RemoteExecutor[] getRemoteExecutors() {
        final String jobsRemoteExecutors = System.getenv("JOBS_REMOTE_EXECUTORS");
        String[] remoteExecutorUrls = jobsRemoteExecutors.split(";");
        ArrayList<RemoteExecutor> executors = new ArrayList<>();
        for (String r: remoteExecutorUrls) {
            WebClient webClient = webClientBuilder.baseUrl(r).build();
            Mono<JobInfoDto[]> res = webClient.get().uri("api/jobs").accept(MediaType.APPLICATION_JSON).retrieve().bodyToMono(JobInfoDto[].class);

            res.subscribe(jobInfos -> {
                RemoteExecutor remoteExecutor = new RemoteExecutor() {
                    @Override
                    public JobInfo[] supportedJobs() {
                        return jobInfos;
                    }

                    @Override
                    public StepResult handle(final TransportDto transport) {
                        Mono<StepResult> tmp = webClient.post()
                                .uri("api/jobs/" + transport.jobData().getSignature()).body(transport, TransportDto.class)
                                .accept(MediaType.APPLICATION_JSON)
                                .retrieve()
                                .bodyToMono(StepResult.class);
                        StepResult[] subscribeResult = new StepResult[1];
                        while (true) {

                            tmp.subscribe(result -> {
                                subscribeResult[0] = result;
                            }, error -> {
                                logger.error("call returns", error);
                                subscribeResult[0] = StepResult.DELAY;
                                subscribeResult[0].error(error.getMessage());
                            }, () -> {
                                // Handle the completion
                                logger.info("call completed");
                            });
                            if (subscribeResult[0] == null) {
                                try {
                                    Thread.sleep(100);
                                } catch (InterruptedException e) {
                                    logger.error("Interrupted call",e);
                                    Thread.currentThread().interrupt();
                                }
                            } else {
                                return subscribeResult[0];
                            }
                        }
                    }
                };
                executors.add(remoteExecutor);
            }, error -> {
                logger.error("Could not read jobInfos", error);
            }, () -> {
               logger.info("returned after querying infos");
            });

        }
        while (executors.size() < remoteExecutorUrls.length) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                logger.error("waiting interrupted", e);
                Thread.currentThread().interrupt();
            }
        }
        return executors.toArray(new RemoteExecutor[executors.size()]);

    }
}

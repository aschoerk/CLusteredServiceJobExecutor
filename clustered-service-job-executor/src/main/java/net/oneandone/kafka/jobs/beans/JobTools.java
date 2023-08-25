package net.oneandone.kafka.jobs.beans;

import static net.oneandone.kafka.jobs.api.State.ERROR;
import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.time.Instant;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.KjeException;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.api.events.JobLifeCycleEvent;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.TransportImpl;
import net.oneandone.kafka.jobs.api.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class JobTools extends StoppableBase {

    static final String SEPARATOR = "|||";

    JobTools(Beans beans) {
        super(beans);
    }

    public static Pair<String, String> prepareKafkaKeyValue(final TransportImpl transport) {
        final JobDataImpl jobData = transport.jobData();


        String jobDataJson = JsonMarshaller.gson.toJson(jobData);
        String context = transport.context();
        final String resumeData = transport.resumeData();

        String separator = SEPARATOR;
        boolean foundInData;
        do {
            foundInData = jobDataJson.contains(separator);
            foundInData = foundInData || ((context != null) && context.contains(separator));
            foundInData = foundInData || ((resumeData != null) && resumeData.contains(separator));
            if(foundInData) {
                separator = UUID.randomUUID().toString();
            }
        } while (foundInData);

        String toSend = String.format("%s%n%s%s%s%s%s", separator,
                jobDataJson,
                separator,
                (transport.context() != null) ? (" " + transport.context()) : "",
                separator,
                (resumeData != null) ? (" " + resumeData) : "");

        String key = (jobData.getGroupId() != null) ? jobData.getGroupId() : ((jobData.getCorrelationId() != null) ? jobData.getCorrelationId() : jobData.getId());

        Pair<String, String> payload = Pair.of(key, toSend);
        return payload;
    }

    public void prepareJobDataForRunning(JobDataImpl jobData) {
        jobData.setDate(Instant.now(beans.getContainer().getClock()));
        if(jobData.getState() == ERROR) {
            jobData.setRetries(0);
        }
        changeStateTo(jobData, RUNNING);
    }

    void changeStateTo(final JobDataImpl jobData, final State newState) {
        State previousState = jobData.getState();
        jobData.setState(newState);
        beans.getContainer().fire(new JobLifeCycleEvent() {
            @Override
            public JobData jobData() {
                return jobData;
            }

            @Override
            public State previousState() {
                return previousState;
            }
        });
    }

    public TransportImpl evaluatePackage(final ConsumerRecord<String, String> r) {

        String payload = r.value();
        int firstCR = payload.indexOf(System.lineSeparator());
        if(firstCR < 0) {
            throw new KjeException("Need to ship separator as first line");
        }
        String separator = payload.substring(0, firstCR);
        int separatorLen = separator.length();
        int currentStart = separatorLen + System.lineSeparator().length();
        int separatorIndex1 = payload.indexOf(separator, currentStart);
        String jobDataJson = (separatorIndex1 > 0) ?
                payload.substring(currentStart, separatorIndex1) :
                payload.substring(currentStart);
        int separatorIndex2 = (separatorIndex1 > 0)
                ? payload.indexOf(separator, separatorIndex1 + separatorLen)
                : -1;
        String context = (separatorIndex1 > 0)
                ? ((separatorIndex2 > 0)
                           ? payload.substring(separatorIndex1 + separatorLen, separatorIndex2)
                           : payload.substring(separatorIndex1 + separatorLen))
                : null;
        String resumeData = (separatorIndex2 > 0)
                ? payload.substring(separatorIndex2 + separatorLen)
                : null;


        JobDataImpl jobData = JsonMarshaller.gson.fromJson(jobDataJson, JobDataImpl.class);
        jobData.setOffset(r.offset());
        jobData.setPartition(r.partition());
        TransportImpl result = new TransportImpl(jobData, fixString(context), beans);
        result.setResumeData(fixString(resumeData));
        return result;
    }


    /**
     * handle representation of emptyString an null value
     * @param s
     * @return null if null or empty, else String removed by first character assumed to be " "
     */
    private String fixString(String s) {
        if (s == null || s.isEmpty()) {
            return null;
        } else {
            assert(s.charAt(0) == ' ');
            return s.substring(1);
        }
    }
}


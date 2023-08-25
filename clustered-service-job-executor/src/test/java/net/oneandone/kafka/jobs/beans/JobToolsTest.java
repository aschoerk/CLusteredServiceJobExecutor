package net.oneandone.kafka.jobs.beans;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.TransportImpl;

/**
 *
 */
public class JobToolsTest {


    @ParameterizedTest
    @CsvSource({
            "'',''",
            ",",
            "a|||b,",
            ",a|||b",
            "abcd,",
            "abcd,''",
            ",abcde",
            "'',abcde",
    })
    void checkSimpleMarshalling(String context, String resumeData) {
        Beans beans = Mockito.mock(Beans.class);
        JobTools jobTools = new JobTools(beans);
        JobDataImpl jobData = new JobDataImpl("1", null, State.RUNNING, "sig", 0, 0, beans);
        jobData.setDate(Instant.now().plus(Duration.ofSeconds(1)));
        TransportImpl transport = new TransportImpl(jobData, context, beans);
        transport.setResumeData(resumeData);
        Pair<String, String> pair = JobTools.prepareKafkaKeyValue(transport);
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>("topic", 1, 1L,
                pair.getKey(), pair.getValue());
        TransportImpl res = jobTools.evaluatePackage(record);
        assertEquals(res.jobData().getId(), jobData.getId());
        assertEquals(res.jobData().getSignature(), jobData.getSignature());
        assertEquals(res.jobData().getStep(), jobData.getStep());
        assertEquals(res.jobData().getStepCount(), jobData.getStepCount());
        assertEquals(context, res.context());
        assertEquals(resumeData, res.resumeData());
    }
}

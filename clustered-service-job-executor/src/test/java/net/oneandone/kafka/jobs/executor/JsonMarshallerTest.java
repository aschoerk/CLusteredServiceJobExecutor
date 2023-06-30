package net.oneandone.kafka.jobs.executor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import net.oneandone.kafka.jobs.api.Container;
import net.oneandone.kafka.jobs.api.State;
import net.oneandone.kafka.jobs.dtos.JobDataImpl;
import net.oneandone.kafka.jobs.dtos.RemarkImpl;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class JsonMarshallerTest {



    @ParameterizedTest
    @CsvSource({"idxx,RUNNING,signatur,0",
            "idxx,DELAYED,xxx,0",
            "idxx,ERROR,xxx,0",
            "idxx,RUNNING,signatur,0",
            "idxx,RUNNING,signatur,0"})
    void simpleJobDataImpl(String id, State state, String signature, int step) {
        JobDataImpl jobData = new JobDataImpl(id,null,state,signature,step, 0);
        jobData.setErrors(new RemarkImpl[0]);
        jobData.setComments(new RemarkImpl[0]);
        String json = JsonMarshaller.gson.toJson(jobData);

        JobDataImpl readJobData = JsonMarshaller.gson.fromJson(json, JobDataImpl.class);
        Assertions.assertEquals(jobData, readJobData);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"id\": \"idxx\", \"jobSignature\": \"signatur\", \"state\": \"RUNNING\", \"step\": 0, \"errors\": [\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582001, \"remark\":  \"was an error\"},\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582002, \"remark\":  \"was an error\"}\n"
            + "], \"comments\": [\n"
            + "  { \"creator\":  \"xxxx\",  \"instant\":  1686817582002, \"remark\":  \"was a comment\"}\n"
            + "]}",
            "{\"id\": \"idxx\", \"jobSignature\": \"signatur\", \"state\": \"RUNNING\", \"step\": 0, \"additionalField\": 0, \"errors\": [\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582001, \"remark\":  \"was an error\"},\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582002, \"remark\":  \"was an error\"}\n"
            + "], \"comments\": [\n"
            + "  { \"creator\":  \"xxxx\",  \"instant\":  1686817582002, \"remark\":  \"was a comment\"}\n"
            + "]}",
            "{\"id\": \"idxx\", \"jobSignature\": \"signatur\", \"step\": 0, \"additionalField\": 0, \"errors\": [\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582001, \"remark\":  \"was an error\"},\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582002, \"remark\":  \"was an error\"}\n"
            + "], \"comments\": [\n"
            + "  { \"creator\":  \"xxxx\",  \"instant\":  1686817582002, \"remark\":  \"was a comment\"}\n"
            + "]}",
            "{\"id\": \"idxx\", \"jobSignature\": \"signatur\", \"step\": 0, \"additionalField\": 0, \"errors\": [\n"
            + "  { \"additionalField\": 0, \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582001, \"remark\":  \"was an error\"},\n"
            + "  { \"creator\":  \"xxxx\", \"id\":  \"xx001\", \"instant\":  1686817582002, \"remark\":  \"was an error\"}\n"
            + "], \"comments\": [\n"
            + "  { \"additionalField\": 0, \"creator\":  \"xxxx\",  \"instant\":  1686817582002, \"remark\":  \"was a comment\"}\n"
            + "]}"
    })
    void jsonStringTest(String json) {
        JobDataImpl jobData = JsonMarshaller.gson.fromJson(json, JobDataImpl.class);
        String json2 = JsonMarshaller.gson.toJson(jobData);
        JobDataImpl jobData2 = JsonMarshaller.gson.fromJson(json2, JobDataImpl.class);

        Assertions.assertEquals(jobData, jobData2);
    }
}

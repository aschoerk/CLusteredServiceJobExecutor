package net.oneandone.clusteredservicejobexecutor.spring.jobs;

import static net.oneandone.kafka.jobs.api.State.RUNNING;

import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jackson.JsonComponent;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.oneandone.kafka.jobs.api.dto.JobDataDto;
import net.oneandone.kafka.jobs.api.dto.TransportDto;
import net.oneandone.kafka.jobs.api.tools.JsonMarshaller;

// @ExtendWith(SpringExtension.class)
// @SpringBootTest
@Import(AppConfig.class)
@WebMvcTest(JobsRestController.class)
public class RestTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void readJobData() throws Exception {
        mockMvc.perform(MockMvcRequestBuilders
                .get("/api/jobs")
                .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().string("""
                        [{"name":"SampleJob","version":"1","signature":"SampleJob|1|SampleStep|SampleStep2","stepNumber":2,
                        "contextClass":"java.lang.String"},{"name":"Sample2Job","version":"1","signature":"Sample2Job|1|Sample2Step|Sample2Step2","stepNumber":2,"contextClass":"java.lang.String"}]
                        """.replaceAll("\\s", "")));
    }

    @Test
    public void testStepCall() throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();
        TestTransport transport = createTestTransport();
        // transport.getJobData().setJobSignature("SampleJob|1|SampleStep|SampleStep2");
        String json = JsonMarshaller.gson.toJson(transport);

        mockMvc.perform(MockMvcRequestBuilders
                        .post("/api/jobs/"+ transport.jobData().getSignature())
                        .content(json)
                        .contentType(MediaType.APPLICATION_JSON)
                )
                .andExpect(MockMvcResultMatchers.status().isOk());


    }

    private static TestTransport createTestTransport() {
        return new TestTransport();
    }

    @JsonComponent
    private static class TestTransport extends TransportDto {

        public TestTransport() {
            setJobData(new MyJobData());
            setContext(JsonMarshaller.gson.toJson("stringcontext"));
            setResumeData(null);
        }

        @JsonComponent
        private static class MyJobData extends JobDataDto {

            {
                setId(UUID.randomUUID().toString());
                setJobSignature("SampleJob|1|SampleStep|SampleStep2");
                setState(RUNNING);
                setCreatedAt(Instant.now());
                setStep(0);
                setStepCount(0);
                setContextClass(String.class.getName());
                setRetries(0);
            }

        }
    }
}

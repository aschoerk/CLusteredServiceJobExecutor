package net.oneandone.clusteredservicejobexecutor.json;

import java.io.IOException;
import java.time.Instant;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

/**
 * @author aschoerk
 */
public class InstantDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException, JacksonException {
        Long millis = deserializationContext.readValue(jsonParser, Long.class);
        return Instant.ofEpochMilli(millis);
    }
}

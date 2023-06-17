package net.oneandone.kafka.jobs.tools;

import java.io.IOException;
import java.time.Instant;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import net.oneandone.kafka.jobs.api.KjeException;

/**
 * Used to marshal and unmarshall kafka events
 */
public class JsonMarshaller {

    public static GsonBuilder gsonBuilder = new GsonBuilder();
    static  {
        gsonBuilder.registerTypeAdapter(Instant.class, new TypeAdapter<Instant>(){
            @Override
            public void write(final JsonWriter jsonWriter, final Instant instant) throws IOException {
                if (instant != null) {
                    jsonWriter.value(instant.toEpochMilli());
                } else {
                    jsonWriter.nullValue();
                }
            }

            @Override
            public Instant read(final JsonReader jsonReader) throws IOException {
                long millisEpoch = jsonReader.nextLong();
                if (millisEpoch == -1) {
                    return null;
                } else {
                    return Instant.ofEpochMilli(millisEpoch);
                }
            }
        });

        gsonBuilder.registerTypeAdapter(Class.class, new TypeAdapter<Class>(){
            @Override
            public void write(final JsonWriter jsonWriter, final Class clazz) throws IOException {
                if (clazz != null) {
                    jsonWriter.value(clazz.getCanonicalName());
                } else {
                    jsonWriter.nullValue();
                }
            }

            @Override
            public Class read(final JsonReader jsonReader) throws IOException {
                String name = jsonReader.nextString();
                try {
                    return Class.forName(name);
                } catch (ClassNotFoundException e) {
                    throw new KjeException(e);
                }
            }
        });


    }

    public static Gson gson = gsonBuilder.create();

    JsonMarshaller() {
        
    }
}

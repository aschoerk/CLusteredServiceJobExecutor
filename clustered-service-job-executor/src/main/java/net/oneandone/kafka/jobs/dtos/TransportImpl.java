package net.oneandone.kafka.jobs.dtos;

import net.oneandone.kafka.jobs.api.JobData;
import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class TransportImpl implements Transport {

    private JobData jobData;

    private String context;

    private String resumeData = null;

    private Beans beans;

    public TransportImpl(final JobDataImpl jobData, final String context, Beans beans) {
        this.jobData = jobData;
        this.context = context;
        this.beans = beans;
    }

    public TransportImpl(final JobDataImpl jobData, final Object context, Class<?> clazz, Beans beans) {
        this.jobData = jobData;
        String tmp = beans.getContainer().marshal(context);
        if (tmp == null) {
            tmp = JsonMarshaller.gson.toJson(context);
        }
        this.context = tmp;
        this.beans = beans;
    }

    public TransportImpl(Beans beans, final Transport transport) {
        this.jobData = transport.jobData();
        this.context = transport.context();
        this.resumeData = transport.resumeData();
        this.beans = beans;
    }

    @Override
    public JobDataImpl jobData() {
        return (JobDataImpl)jobData;
    }


    @Override
    public String context() {
        return context;
    }

    public String resumeData() { return resumeData; }

    public void setResumeData(final String s) {
        this.resumeData = s;
    }

    public <T> T getContext(Class<T> clazz) {
        return unmarshall(clazz, context);
    }

    public <T> void setContext(Object object) {
        context = marshall(object);
    }

    public <T> T getResumeData(Class<T> clazz) {
        return unmarshall(clazz, resumeData);
    }


    private <T> T unmarshall(final Class<T> clazz, final String tmp) {
        T result = beans.getContainer().unmarshal(tmp, clazz);
        if (result == null) {
            return JsonMarshaller.gson.fromJson(tmp, clazz);
        } else {
            return result;
        }
    }

    private String marshall(Object object) {
        String result = beans.getContainer().marshal(object);
        if (result == null) {
            return JsonMarshaller.gson.toJson(object);
        } else {
            return result;
        }
    }
}

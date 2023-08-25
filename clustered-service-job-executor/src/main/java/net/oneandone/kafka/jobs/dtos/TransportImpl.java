package net.oneandone.kafka.jobs.dtos;

import net.oneandone.kafka.jobs.api.Transport;
import net.oneandone.kafka.jobs.api.dto.TransportDto;
import net.oneandone.kafka.jobs.beans.Beans;
import net.oneandone.kafka.jobs.api.tools.JsonMarshaller;

/**
 * @author aschoerk
 */
public class TransportImpl extends TransportDto implements Transport {

    private final Beans beans;

    public TransportImpl(final JobDataImpl jobData, final String context, Beans beans) {
        this.setJobData(jobData);
        this.setContext(context);
        this.beans = beans;
    }

    public TransportImpl(final JobDataImpl jobData, final Object context, Class<?> clazz, Beans beans) {
        this.setJobData(jobData);
        String tmp = beans.getContainer().marshal(context);
        if (tmp == null) {
            tmp = JsonMarshaller.gson.toJson(context);
        }
        this.setContext(tmp);
        this.beans = beans;
    }

    public TransportImpl(Beans beans, final TransportDto transport) {
        this.setJobData(transport.getJobData());
        this.setContext(transport.context());
        this.setResumeData(transport.resumeData());
        this.beans = beans;
    }

    @Override
    public JobDataImpl jobData() {
        return (JobDataImpl) getJobData();
    }


    public <T> T getContext(Class<T> clazz) {
        return unmarshall(clazz, getContext());
    }

    public <T> void setContext(Object object) {
        setContext(marshall(object));
    }

    public <T> T getResumeData(Class<T> clazz) {
        return unmarshall(clazz, resumeData());
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

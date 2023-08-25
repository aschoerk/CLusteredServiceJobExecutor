FROM alpine:latest
RUN apk add --no-cache openjdk17-jre-headless
RUN apk add --no-cache iputils
RUN apk add --no-cache net-tools
RUN apk add --no-cache bash
RUN apk add --no-cache curl
WORKDIR /opt
RUN wget https://www-eu.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
RUN tar -xvzf *.tgz

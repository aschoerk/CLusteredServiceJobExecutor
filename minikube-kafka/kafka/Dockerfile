FROM alpjre17:latest

WORKDIR /opt
RUN if [ ! -f "kafka_2.13-3.5.1.tgz" ]; then wget https://www-eu.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz; fi
RUN tar -xvzf *.tgz

COPY server.properties /opt/kafka_2.13-3.5.1/config/server.properties

ENTRYPOINT /opt/kafka_2.13-3.5.1/bin/kafka-server-start.sh /opt/kafka_2.13-3.5.1/config/server.properties

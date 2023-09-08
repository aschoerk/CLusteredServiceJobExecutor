FROM alpjre17:latest

WORKDIR /opt

RUN if [ ! -f "apache-zookeeper-3.9.0-bin.tar.gz" ]; then wget https://www-eu.apache.org/dist/zookeeper/zookeeper-3.9.0/apache-zookeeper-3.9.0-bin.tar.gz; fi
RUN tar -xvzf *.tar.gz
COPY zoo_sample.cfg /opt/apache-zookeeper-3.9.0-bin/conf/zoo.cfg

ENTRYPOINT apache-zookeeper-3.9.0-bin/bin/zkServer.sh start-foreground

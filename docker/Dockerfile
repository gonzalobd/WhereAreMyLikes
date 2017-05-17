# sudo docker build -t wherearemylikes .
# sudo docker run -d -p 9092:9092 -e KAFKA_HOST=<IP_BROKER_KAFKA> -e TOKEN=<token>   wherearemylikes
# sudo docker exec -ti <id> bash

FROM java:openjdk-8-jdk

ENV DEBIAN_FRONTEND noninteractive
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"


# Install Kafka, Zookeeper and other needed things
RUN apt-get update && \
    apt-get install -y  wget supervisor dnsutils git maven && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    cd / && \
    git clone https://github.com/gonzalobd/WhereAreMyLikes && \
    cd WhereAreMyLikes && \
    mvn clean package


COPY entry.sh /

RUN chmod +x /entry.sh

ENTRYPOINT ["/entry.sh"]

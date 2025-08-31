FROM confluentinc/cp-kafka-connect:7.6.1

USER root

RUN mkdir -p /etc/kafka-connect/jars/mongodb

# Fetch the MongoDB Kafka Connector 1.13.0 (curl is available in base image)
RUN curl -fL -o /etc/kafka-connect/jars/mongodb/mongo-kafka-connect-1.13.0-all.jar \
      https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/1.13.0/mongo-kafka-connect-1.13.0-all.jar

# Default plugin path includes our target directory
ENV CONNECT_PLUGIN_PATH=/usr/share/java,/etc/kafka-connect/jars

# Drop privileges back to the default user in the base image
USER appuser



# Kafka Connect Setup

This directory contains configuration for Kafka Connect and the MongoDB Sink connector.

## Adding the MongoDB Kafka Connector Plugin

The `kafka-connect` service mounts `./kafka/connect/plugins` to `/etc/kafka-connect/jars` inside the container. 
Place the MongoDB Kafka Connector JARs here.

### Quick add (manual)
Download the latest connector (example version 1.12.1):

```
curl -L -o mongodb-kafka-connect.zip https://search.maven.org/remotecontent?filepath=org/mongodb/kafka/mongo-kafka-connect/1.12.1/mongo-kafka-connect-1.12.1-all.jar
mkdir -p kafka/connect/plugins/mongodb
mv mongo*jar kafka/connect/plugins/mongodb/
```

Then restart Kafka Connect:
```
docker compose restart kafka-connect
```

## Register the Sink Connector
Once Kafka Connect is healthy:
```
curl -X POST -H "Content-Type: application/json" \
  --data @kafka/connect/mongodb-sink.json \
  http://localhost:8083/connectors
```

Or update:
```
curl -X PUT -H "Content-Type: application/json" \
  --data @kafka/connect/mongodb-sink.json \
  http://localhost:8083/connectors/mongo-sink-market-data/config
```

## Collections Strategy
The config uses `collection": "${topic}"` so each topic maps to its own collection in the `price_intel` database.

## Deleting the connector
```
curl -X DELETE http://localhost:8083/connectors/mongo-sink-market-data
```

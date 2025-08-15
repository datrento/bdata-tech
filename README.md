# bdata-tech

Price Intelligence demo stack: simulated market & user behavior data -> Kafka -> MongoDB (raw events) & PostgreSQL (relational) with FastAPI collection service.

## Architecture
Components:
* Kafka (KRaft single node) + Kafka UI
* FastAPI data collection (`market-data-api`)
* Kafka producers (market data, user behavior) publishing to topics
* Kafka Connect worker (REST :8083)
* MongoDB (raw event sink via MongoDB Kafka Sink connector)
* PostgreSQL + Adminer

Topics (see `kafka/config.py`):
* market-data-raw
* competitor-prices
* aggregator-prices
* user-behavior-events

MongoDB sink writes each topic to a collection of the same name in `price_intel` database.

## Data Flow
1. FastAPI simulates competitor, aggregator & user behavior data
2. Producers fetch API endpoints and publish events
3. Kafka Connect MongoDB sink ingests topics to MongoDB
4. Further processing/analytics can consume from Kafka or query MongoDB/Postgres

## Usage
### Start services
```bash
bash run.sh start
```

### View logs
```bash
bash run.sh logs
```

### Stop services
```bash
bash run.sh stop
```

### Rebuild
```bash
bash run.sh build && bash run.sh start
```

## Kafka Connect: Register MongoDB Sink
Connector config: `kafka/connect/mongodb-sink.json`

After services are up and the MongoDB connector JAR is placed under `kafka/connect/plugins` (see `kafka/connect/README.md`):
```bash
curl -X POST -H "Content-Type: application/json" \
	--data @kafka/connect/mongodb-sink.json \
	http://localhost:8083/connectors
```

Update connector:
```bash
curl -X PUT -H "Content-Type: application/json" \
	--data @kafka/connect/mongodb-sink.json \
	http://localhost:8083/connectors/mongo-sink-market-data/config
```

List connectors:
```bash
curl http://localhost:8083/connectors
```

Delete connector:
```bash
curl -X DELETE http://localhost:8083/connectors/mongo-sink-market-data
```

## Notes
* Ensure the MongoDB Kafka sink JAR version matches your Kafka Connect version.
* Collections will grow quickly; consider TTL indexes or compaction strategies later.
* Add schema validation or transformations via SMTs if needed.
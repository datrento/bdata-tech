#!/bin/bash

# Start all services
docker-compose up -d

# Wait for services to start
echo "Waiting for services to be ready..."
sleep 30

# Submit the price monitoring job to Flink
echo "Submitting price monitoring job to Flink..."
docker exec flink-jobmanager flink run -py /opt/flink/app/jobs/price_monitoring_job.py

# Show logs from the Flink job manager
echo "Showing Flink job manager logs..."
docker logs -f flink-jobmanager
